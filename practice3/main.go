package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

const SnapshotDateFormat = "02-Jan-2006-15_04_05"

const (
	Insert Action = iota
	Replace
	Delete
	Select
	Snapshot
)

var PostTransactions = []Action{Insert, Delete, Insert}

type Action int

type Router struct {
	// Поля
}

type Message struct {
	err  error
	body []byte
}

type Storage struct {
	name     string
	leader   bool
	replicas []string

	engine *StorageEngine

	featuresPrimInd map[string]*geojson.Feature
	featuresRTree   rtree.RTreeG[*geojson.Feature]

	mtx sync.Mutex

	transactionCh chan Transaction
	ResponseCh    chan Message

	ctx    context.Context
	cancel context.CancelFunc
}

type StorageEngine struct {
	lsnCounter uint64

	vclock           map[string]uint64 // {<Узел 1>: <LSN узла 1>, ..., <Узел N>: <LSN узла N>}
	replicasRegistry map[string]*websocket.Conn

	snapshotsDir string
}

type Transaction struct {
	Lsn    uint64 `json:"lsn"`
	Name   string `json:"name"` // Имя узла, на котором применяется транзакция
	Action Action `json:"action"`

	Feature *geojson.Feature `json:"feature"`
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Отключить проверку происхождения для тестирования
	},
}

const snapshotsDirectory = "snapshots/"

func (s *Storage) logFilename() string {
	return s.name + ".wal"
}

func cleanFile(filename string) error {
	return os.Truncate(filename, 0)
}

func (s *Storage) leaderCheck() error {
	if !s.leader {
		return errors.New("the provided storage is not a leader")
	} else {
		return nil
	}
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		panic(err.Error())
	}
}

func getLastFileFilenameInDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var newestFile string
	var newestTime int64 = 0

	for _, snapshot := range entries {
		fileInfo, err := os.Stat(dir + snapshot.Name())
		if err != nil {
			return "", err
		}
		currTime := fileInfo.ModTime().Unix()
		if currTime > newestTime {
			newestTime = currTime
			newestFile = snapshot.Name()
		}
	}
	return newestFile, nil
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))

	mux.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		// Редиректим на рандомный узел нашего единственного недо-шарда
		http.Redirect(w, r, "/"+nodes[0][rand.Intn(len(nodes[0]))]+"/select", http.StatusTemporaryRedirect)
	})

	mux.Handle("/insert", http.RedirectHandler("/"+nodes[0][0]+"/insert", http.StatusTemporaryRedirect))
	mux.Handle("/replace", http.RedirectHandler("/"+nodes[0][0]+"/replace", http.StatusTemporaryRedirect))
	mux.Handle("/delete", http.RedirectHandler("/"+nodes[0][0]+"/delete", http.StatusTemporaryRedirect))
	mux.Handle("/checkpoint", http.RedirectHandler("/"+nodes[0][0]+"/checkpoint", http.StatusTemporaryRedirect))

	return &result
}

func (r *Router) Run() {
	slog.Info("Starting router...")
}

func (r *Router) Stop() {
	slog.Info("Stopping router...")
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	engine := StorageEngine{
		0, make(map[string]uint64), make(map[string]*websocket.Conn), snapshotsDirectory}

	storage := Storage{name, leader, replicas, &engine,
		make(map[string]*geojson.Feature), rtree.RTreeG[*geojson.Feature]{}, sync.Mutex{}, make(chan Transaction), make(chan Message), ctx, cancel}

	storage.setupHandlers(mux)
	return &storage
}

func (s *Storage) setupHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/"+s.name+"/checkpoint", func(w http.ResponseWriter, _ *http.Request) {
		slog.Info("SAVING SNAPSHOT...")
		s.handleGetRequest(w, Snapshot)
	})

	// TODO: надо переделать под новое тз
	mux.HandleFunc("/"+s.name+"/select", func(w http.ResponseWriter, _ *http.Request) {
		slog.Info("SELECT QUERY...")
		s.handleGetRequest(w, Select)
	})

	// Создавать новые транзакции может только `Storage` у которого `leader == true`.
	if s.leader {
		mux.HandleFunc("/"+s.name+"/insert", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("INSERT QUERY")
			s.handlePostRequest(w, r, Insert)
		})

		mux.HandleFunc("/"+s.name+"/replace", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("REPLACE QUERY")
			s.handlePostRequest(w, r, Replace)
		})

		mux.HandleFunc("/"+s.name+"/delete", func(w http.ResponseWriter, r *http.Request) {
			slog.Info("DELETE QUERY")
			s.handlePostRequest(w, r, Delete)
		})
	} else { // Ну иначе мы имеем дело с репликацией (предполагаем, что она уже подключена и находится в нашем регистре)
		mux.HandleFunc("/"+s.name+"/replication", func(w http.ResponseWriter, r *http.Request) {

			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err.Error())
			}
			defer conn.Close()

			// Слушаем, что пришло от мастера
			for {
				_, message, err := conn.ReadMessage()

				if err != nil {
					// Если пришло сообщение на завершение, и текущее хранилище - реплика, то завершаемся. Иначе если мастер, то можем спокойно продолжать работу
					if err, status := err.(*websocket.CloseError); status && (err.Code == websocket.CloseNormalClosure) {
						/* if s.leader {
							// TODO: опять надо подумать, что делать, если реплика - мастер
							// Если запрос на завершение пришел, значит тот адрес завершил свое существование
							// Предлагается удалять из регистра адрес, с которого пришел запрос

							//continue
						} */
						return
					}
					panic(err.Error())
				}

				var tmpTr Transaction
				if err = json.Unmarshal(message, &tmpTr); err != nil {
					panic(err.Error())
				}

				s.transactionCh <- tmpTr
				<-s.ResponseCh
			}
		})
	}
}

func (s *Storage) handlePostRequest(w http.ResponseWriter, r *http.Request, action Action) {
	var feature geojson.Feature
	err := json.NewDecoder(r.Body).Decode(&feature)

	if err != nil {
		writeError(w, err)
		return
	}

	s.transactionCh <- Transaction{Name: s.name, Action: action, Feature: &feature}
	engineResp := <-s.ResponseCh

	if engineResp.err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		writeError(w, engineResp.err)
	}
}

func (s *Storage) handleGetRequest(w http.ResponseWriter, action Action) {
	s.transactionCh <- Transaction{Name: s.name, Action: action}
	engineResp := <-s.ResponseCh
	if engineResp.err == nil {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(engineResp.body)
		if err != nil {
			panic(err.Error())
		}
	} else {
		writeError(w, engineResp.err)
	}
}

// TODO: внимательнее с leader

func (s *Storage) handleTransaction(tr Transaction) {

	s.mtx.Lock()
	defer s.mtx.Unlock()

	tr.Lsn = s.engine.lsnCounter
	msg, err := s.runTransaction(tr, false)

	if err != nil {
		s.ResponseCh <- Message{err: err}
	} else {
		if s.leader {
			logFd, err := os.OpenFile(s.logFilename(), os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				panic(err.Error())
			}
			if err = logTransaction(logFd, tr, true); err != nil {
				panic(err.Error())
			}
			if err = logFd.Close(); err != nil {
				panic(err.Error())
			}
			if slices.Contains(PostTransactions, tr.Action) {
				s.engine.lsnCounter++ // Т.к. считаем только post-транзакции, а на slave'е их не может быть
			}
		}
		s.ResponseCh <- Message{err: err, body: msg}
	}
}

// Инициализирует журнал логирования и папку со снапшотами, если таких нет; запускает транзакции с последнего снапшота
func (s *Storage) Init() error {

	if err := initFiles(s.logFilename(), s.engine.snapshotsDir); err != nil {
		return err
	}

	snapshot, err := getLastFileFilenameInDir(s.engine.snapshotsDir)
	if err != nil {
		return err
	}

	// Пытаемся считать последний snapshot
	if err = s.runTransactionsFromFile(s.engine.snapshotsDir + snapshot); err != nil && !os.IsNotExist(err) {
		return err
	}

	err = s.runTransactionsFromFile(s.logFilename())
	if err != nil {
		return err
	}

	if _, err := s.saveSnapshot(true); err != nil {
		return err
	}

	return nil
}

func initFiles(logFilename string, snapshotsDir string) error {
	// Создадим директорию со snapshot'ами, если ее нет
	if err := os.Mkdir(snapshotsDir, 0755); err != nil && !os.IsExist(err) {
		return err
	}

	// Создадим журнал, если его нет
	file, err := os.OpenFile(logFilename, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	return nil
}

func (s *Storage) Run() {
	slog.Info("Starting storage '" + s.name + "'...")

	go func() {

		if s.leader {
			if err := s.Init(); err != nil {
				panic(err.Error())
			}
			s.connectToReplicas()
		}

		// Слушаем
		for {
			select {
			case <-s.ctx.Done():
				return

			case tr := <-s.transactionCh:
				log.Print(s.name)
				s.handleTransaction(tr)
			}
		}
	}()
}

func (s *Storage) Stop() {
	slog.Info("Stopping storage '" + s.name + "'...")

	s.cancel()
	// Проходимся по всем подключениям реплик в регистре и посылаем closeMessage
	// С другой стороны, если реплика получит такое сообщение, то горутина завершится (см. handle /replication)
	// После чего закрываем подключение
	/* for _, conn := range s.engine.replicasRegistry {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	} */
}

// Сохраняет snapshot, чистит журнал (если надо) и возвращает пару: (название snapshot'а, ошибка | nil)
func (s *Storage) saveSnapshot(cleanLogAfterSnapshotFlag bool) (string, error) {
	snapshotFilename := s.engine.snapshotsDir + "snapshot-" + time.Now().Format(SnapshotDateFormat) + ".ckp"
	file, err := os.OpenFile(snapshotFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}

	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	for _, feature := range s.featuresPrimInd {
		// Сохраняю типа транзакции
		if err = logTransaction(file, Transaction{Action: Insert, Feature: feature}, true); err != nil {
			return "", err
		}
	}

	if cleanLogAfterSnapshotFlag {
		if err = cleanFile(s.logFilename()); err != nil {
			return "", err
		}
	}

	return snapshotFilename, nil
}

// 1 transaction - 1 line
func logTransaction(file *os.File, transaction Transaction, onlyPostTransaction bool) error {
	if onlyPostTransaction && !slices.Contains(PostTransactions, transaction.Action) {
		return nil // Если выставлен флаг onlyPostTransaction, а сама транзакция - не post, то просто выйдем
	}
	marshal, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	_, err = file.WriteString(string(marshal) + "\n")
	if err != nil {
		return err
	}
	return nil
}

// Выполняет транзакцию и уведомляет реплики, если это транзакция на изменение
func (s *Storage) runTransaction(transaction Transaction, vclockDependent bool) ([]byte, error) {
	// Если транзакция зависит от vclock (например, она не зависит от vclock, когда мы подгружаем транзакции из файла)
	if vclockDependent {
		// Проверяем, применяли ли мы уже эту транзакцию
		lastLSN, exists := s.engine.vclock[transaction.Name]
		if exists && transaction.Lsn <= lastLSN {
			return nil, nil // Уже применили эту или более новую транзакцию от этого узла, но по факту это же не ошибка
		}
		// Обновляем vclock
		s.engine.vclock[transaction.Name] = transaction.Lsn
		// ----
	}

	switch transaction.Action {
	case Insert:
		s.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature
		s.featuresRTree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)

		if s.leader {
			s.notifyReplicas(&transaction)
		}
	case Replace:
		s.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature
		s.featuresRTree.Replace(
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, s.featuresPrimInd[transaction.Feature.ID.(string)],
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature,
		)

		if s.leader {
			s.notifyReplicas(&transaction)
		}
	case Delete:
		delete(s.featuresPrimInd, transaction.Feature.ID.(string))
		s.featuresRTree.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)

		if s.leader {
			s.notifyReplicas(&transaction)
		}
	case Snapshot:
		snapshotFilename, err := s.saveSnapshot(true)
		return []byte("Snapshot '" + snapshotFilename + "' has been saved!"), err
	case Select:
		featureCollection := geojson.NewFeatureCollection()
		s.featuresRTree.Scan(
			func(_, _ [2]float64, data *geojson.Feature) bool {
				featureCollection.Append(data)
				return true
			},
		)
		marshal, err := json.Marshal(featureCollection)
		if err != nil {
			return nil, err
		}
		return marshal, nil
	default:
		return nil, errors.ErrUnsupported
	}
	return nil, nil
}

func (s *Storage) runTransactionsFromFile(filename string) error {
	file, err := os.Open(filename)
	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tmpTr Transaction
		if err = json.Unmarshal(scanner.Bytes(), &tmpTr); err != nil {
			return err
		}

		if _, err = s.runTransaction(tmpTr, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) connectToReplicas() {
	if !s.leader {
		return
	}
	for _, replica := range s.replicas {
		go func(replica string) {
			// Установить соединение с репликой по WebSocket
			conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/"+replica+"/replication", nil)
			if err != nil {
				log.Printf("Ошибка подключения к реплике %s: %v", replica, err)
				return
			}
			s.engine.replicasRegistry[replica] = conn
		}(replica)
	}
}

// Передаем транзакцию репликам
func (s *Storage) notifyReplicas(tr *Transaction) {
	if err := s.leaderCheck(); err != nil {
		panic(err.Error())
	}
	json, err := json.Marshal(tr)
	if err != nil {
		panic(err.Error())
	}

	for replica, conn := range s.engine.replicasRegistry {
		err = conn.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			log.Fatal("Can't send transaction to replica: '" + replica + "'.")
			conn.Close()
			delete(s.engine.replicasRegistry, replica)
		}
	}
}

func signalHandler(server *http.Server) {
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan
		slog.Info("Got signal:", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := server.Shutdown(ctx)
		if err != nil {
			panic(err.Error())
		}
	}()
}

func main() {

	mux := http.ServeMux{}

	router := NewRouter(&mux, [][]string{{"master", "slave1"}})
	m := NewStorage(&mux, "master", []string{"slave"}, true)
	s := NewStorage(&mux, "slave", make([]string, 0), false)

	server := http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: &mux,
	}
	router.Run()
	defer router.Stop()
	s.Run()
	defer s.Stop()
	m.Run()
	defer m.Stop()

	signalHandler(&server)

	slog.Info("Listen http://" + server.Addr)
	err := server.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}

	defer slog.Info("Shutting down...")
}
