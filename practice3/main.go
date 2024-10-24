package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	mux      *http.ServeMux

	featuresPrimInd map[string]*geojson.Feature
	featuresRTree   rtree.RTreeG[*geojson.Feature]

	ctx    context.Context
	cancel context.CancelFunc
}

type DBEngine struct {
	lsnCounter uint64

	vclock           map[string]uint64 // {<Узел 1>: <LSN узла 1>, ..., <Узел N>: <LSN узла N>}
	replicasRegistry map[string]*websocket.Conn

	storage *Storage

	transactionCh chan Transaction
	engineRespCh  chan Message

	snapshotsDir string
	logFilename  string
}

type Transaction struct {
	Lsn    uint64 `json:"lsn"`
	Name   string `json:"name"` // Имя узла, на котором применяется транзакция
	Action Action `json:"action"`

	Feature *geojson.Feature `json:"feature"`
}

const snapshotsDirectory = "snapshots/"

var upgrader = websocket.Upgrader{}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))

	for nodeRow := range nodes {
		for node := range nodes[nodeRow] {
			currentNode := nodes[nodeRow][node]
			mux.Handle("/select", http.RedirectHandler("/"+currentNode+"/select", http.StatusTemporaryRedirect))
			mux.Handle("/insert", http.RedirectHandler("/"+currentNode+"/insert", http.StatusTemporaryRedirect))
			mux.Handle("/replace", http.RedirectHandler("/"+currentNode+"/replace", http.StatusTemporaryRedirect))
			mux.Handle("/delete", http.RedirectHandler("/"+currentNode+"/delete", http.StatusTemporaryRedirect))
			mux.Handle("/checkpoint", http.RedirectHandler("/"+currentNode+"/checkpoint", http.StatusTemporaryRedirect))
		}
	}

	return &result
}

func (r *Router) Run() {
	slog.Info("Starting router...")
}

func (r *Router) Stop() {
	slog.Info("Stopping router...")
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		panic(err.Error())
	}
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	storage := Storage{name, leader, make([]string, 0), mux, make(map[string]*geojson.Feature), rtree.RTreeG[*geojson.Feature]{}, ctx, cancel}
	return &storage
}

func (e *DBEngine) handlePostRequest(w http.ResponseWriter, r *http.Request, storage *Storage, name string, action Action) {
	var feature geojson.Feature
	err := json.NewDecoder(r.Body).Decode(&feature)

	if err != nil {
		writeError(w, err)
		return
	}

	e.transactionCh <- Transaction{Name: name, Action: action, Feature: &feature}
	engineResp := <-e.engineRespCh

	if engineResp.err == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		writeError(w, engineResp.err)
	}
}

func (e *DBEngine) handleGetRequest(w http.ResponseWriter, storage *Storage, name string, action Action) {
	e.transactionCh <- Transaction{Name: name, Action: action}
	engineResp := <-e.engineRespCh
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

func (s *Storage) Run() {
	slog.Info("Starting storage '" + s.name + "'...")

	engine := DBEngine{
		0, make(map[string]uint64), make(map[string]*websocket.Conn), s,
		make(chan Transaction), make(chan Message), snapshotsDirectory, s.name + ".ldf"}
	engine.Run()
}

func (s *Storage) Stop() {
	slog.Info("Stopping storage '" + s.name + "'...")
	s.cancel()
}

/*
Предлагаю после создания snapshot'а чистить журнал транзакций. Тогда, если во время запуска Engine в журнале будут
какие-то данные, мы точно будем знать, что они были записаны после последнего snapshot'а

Следовательно, предлагается добиться персистентности данных следующим образом: считаем snapshot в переменную features,
последовательно применим действия из журнала к features, очистим журнал, сделаем snapshot
*/

// TODO: возможно нужен будет чек стореджа на мастера
func (e *DBEngine) Run() {
	go func() {

		err := e.Init()
		if err != nil {
			panic(err.Error())
		}

		if _, err = e.saveSnapshot(); err != nil {
			panic(err.Error())
		}

		e.setupHandlers()
		// e.connectToReplicas()

		// Слушаем
		for {
			select {
			case <-e.storage.ctx.Done():
				return

			case tr := <-e.transactionCh:
				tr.Lsn = e.lsnCounter
				msg, err := e.runTransaction(tr)
				if err != nil {
					e.engineRespCh <- Message{err: err}
				} else {
					logFd, err := os.OpenFile(e.logFilename, os.O_WRONLY|os.O_APPEND, 0666)
					if err != nil {
						panic(err.Error())
					}
					e.engineRespCh <- Message{err: writeTransactionToFile(logFd, tr), body: msg}
					if err = logFd.Close(); err != nil {
						panic(err.Error())
					}
				}
				e.lsnCounter++
			}
		}

	}()
}

func (e *DBEngine) setupHandlers() {
	// POST запросы
	e.storage.mux.HandleFunc("/"+e.storage.name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("INSERT QUERY")
		e.handlePostRequest(w, r, e.storage, e.storage.name, Insert)
	})

	e.storage.mux.HandleFunc("/"+e.storage.name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("REPLACE QUERY")
		e.handlePostRequest(w, r, e.storage, e.storage.name, Replace)
	})

	e.storage.mux.HandleFunc("/"+e.storage.name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("DELETE QUERY")
		e.handlePostRequest(w, r, e.storage, e.storage.name, Delete)
	})

	// GET запросы
	e.storage.mux.HandleFunc("/"+e.storage.name+"/checkpoint", func(w http.ResponseWriter, _ *http.Request) {
		slog.Info("SAVING SNAPSHOT...")
		e.handleGetRequest(w, e.storage, e.storage.name, Snapshot)
	})

	// TODO: надо переделать под новое тз
	e.storage.mux.HandleFunc("/"+e.storage.name+"/select", func(w http.ResponseWriter, _ *http.Request) {
		slog.Info("SELECT QUERY...")
		e.handleGetRequest(w, e.storage, e.storage.name, Select)
	})
}

func (e *DBEngine) connectToReplicas() {

}

func (e *DBEngine) Init() error {
	if err := initEngineFiles(e.logFilename, e.snapshotsDir); err != nil {
		return err
	}

	snapshot, err := getLastSnapshotFilename(e.snapshotsDir)
	if err != nil {
		return err
	}

	// Пытаемся считать последний snapshot
	if err = e.runTransactionsFromFile(e.snapshotsDir + snapshot); err != nil && !os.IsNotExist(err) {
		return err
	}

	err = e.runTransactionsFromFile(e.logFilename)
	if err != nil {
		return err
	}

	return nil
}

func initEngineFiles(logFilename string, snapshotsDir string) error {
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

// Сохраняет snapshot, чистит журнал и возвращает пару: (название snapshot'а, ошибка | nil)
func (e *DBEngine) saveSnapshot() (string, error) {
	snapshotFilename := e.snapshotsDir + "snapshot-" + time.Now().Format(SnapshotDateFormat) + ".ckp"
	file, err := os.OpenFile(snapshotFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}

	defer func(file *os.File) {
		if err = file.Close(); err != nil {
			return
		}
	}(file)

	for _, feature := range e.storage.featuresPrimInd {
		// Сохраняю типа транзакции
		if err = writeTransactionToFile(file, Transaction{Action: Insert, Feature: feature}); err != nil {
			return "", err
		}
	}

	// Чистим журнал после создания snapshot'а
	if err = cleanLog(e.logFilename); err != nil {
		return "", err
	}

	return snapshotFilename, nil
}

func getLastSnapshotFilename(snapshotsDir string) (string, error) {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return "", err
	}
	var newestFile string
	var newestTime int64 = 0

	for _, snapshot := range entries {
		fileInfo, err := os.Stat(snapshotsDir + snapshot.Name())
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

func cleanLog(filename string) error {
	return os.Truncate(filename, 0)
}

func (e *DBEngine) runTransaction(transaction Transaction) ([]byte, error) {
	switch transaction.Action {
	case Insert:
		e.storage.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature
		e.storage.featuresRTree.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Replace:
		e.storage.featuresPrimInd[transaction.Feature.ID.(string)] = transaction.Feature

		e.storage.featuresRTree.Replace(
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, e.storage.featuresPrimInd[transaction.Feature.ID.(string)],
			transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature,
		)
	case Delete:
		delete(e.storage.featuresPrimInd, transaction.Feature.ID.(string))
		e.storage.featuresRTree.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Snapshot:
		snapshotFilename, err := e.saveSnapshot()
		return []byte("Snapshot '" + snapshotFilename + "' has been saved!"), err
	case Select:
		featureCollection := geojson.NewFeatureCollection()
		e.storage.featuresRTree.Scan(
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

func (e *DBEngine) runTransactionsFromFile(filename string) error {
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

		if _, err = e.runTransaction(tmpTr); err != nil {
			return err
		}
	}
	return nil
}

// 1 transaction - 1 line
func writeTransactionToFile(file *os.File, transaction Transaction) error {
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

func main() {

	mux := http.ServeMux{}

	router := NewRouter(&mux, [][]string{{"test"}})
	router.Run()

	storage := NewStorage(&mux, "test", []string{}, true)
	storage.Run()

	server := http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: &mux,
	}

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

	defer slog.Info("Shutting down...")

	slog.Info("Listen http://" + server.Addr)
	err := server.ListenAndServe() // http event loop
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}

	router.Stop()
	storage.Stop()
}
