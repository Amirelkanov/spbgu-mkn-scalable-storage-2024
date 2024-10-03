package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const SnapshotDateFormat = "02-Jan-2006-15_04_05"

type Action int

const (
	Insert Action = iota
	Replace
	Delete
	Select
	Snapshot
)

type Router struct {
	// Поля
}

// TODO: Message сделай и канал chan Message

type Storage struct {
	name          string
	transactionCh chan Transaction
	engineRespCh  chan error

	ctx    context.Context
	cancel context.CancelFunc
}

type DBEngine struct {
	features map[string]*geojson.Feature
	rt       rtree.RTree

	lsnCounter uint64

	snapshotsDir string
	logFilename  string
}

type Transaction struct {
	Lsn    uint64 `json:"lsn"`
	Name   string `json:"name"`
	Action Action `json:"action"`

	Feature *geojson.Feature `json:"feature"`
}

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
	storage := Storage{name, make(chan Transaction), make(chan error), ctx, cancel}

	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("INSERT QUERY")
		handlePostRequest(w, r, storage, name, Insert)
	})

	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("REPLACE QUERY")
		handlePostRequest(w, r, storage, name, Replace)
	})

	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("DELETE QUERY")
		handlePostRequest(w, r, storage, name, Delete)
	})

	// GET запросы
	mux.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("SAVING SNAPSHOT...")
		handleGetRequest(w, r, storage, name, Snapshot)
	})

	// TODO: НЕ ЗАБУДЬ ЗАКРЫТЬ файлы!
	// TODO: сделать (просят использовать rtree)
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("SELECT QUERY...")
		handleGetRequest(w, r, storage, name, Snapshot)
	})

	return &storage
}

func handlePostRequest(w http.ResponseWriter, r *http.Request, storage Storage, name string, action Action) {
	var feature geojson.Feature
	err := json.NewDecoder(r.Body).Decode(&feature)

	if err != nil {
		writeError(w, err)
		return
	}

	storage.transactionCh <- Transaction{Name: name, Action: action, Feature: &feature}
	engineResp := <-storage.engineRespCh

	if engineResp == nil {
		w.WriteHeader(http.StatusOK)
	} else {
		writeError(w, engineResp)
	}
}

func handleGetRequest(w http.ResponseWriter, r *http.Request, storage Storage, name string, action Action) {
	storage.transactionCh <- Transaction{Name: name, Action: action}
	engineResp := <-storage.engineRespCh

	if engineResp == nil {
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprintf(w, "OK")
		if err != nil {
			panic(err.Error())
		}
	} else {
		writeError(w, engineResp)
	}
}

func (s *Storage) Run() {
	slog.Info("Starting storage '" + s.name + "'...")
	runEngine(s)
}

func (s *Storage) Stop() {
	slog.Info("Stopping storage '" + s.name + "'...")
	s.cancel()
}

/*
Предлагаю после создания snapshot'а чистить журнал транзакций. Тогда, если во время запуска Engine в журнале будут
какие-то данные, мы точно будем знать, что они были записаны после последнего snapshot'а

Следовательно, предлагается добиться персистентности данных, считав snapshot в переменную features,
а затем последовательно применив действия из журнала к ней же, после чего сделать snapshot
Замечание: Применяя действия с журнала, мы опять заполним его теми же транзакциями, но при этом текущая БД уже будет
иметь отличные от ласт (на момент запуска runEngine) snapshot'а данные. Поэтому после считывания всех данных мы будем делать сразу же snapshot
*/
func runEngine(s *Storage) {
	go func() {
		Engine := DBEngine{map[string]*geojson.Feature{}, rtree.RTree{}, 0, "snapshots/", s.name + ".ldf"}

		err := initEngine(&Engine)
		if err != nil {
			panic(err.Error())
		}

		if err = saveSnapshot(&Engine); err != nil {
			panic(err.Error())
		}

		logFd, err := os.OpenFile(Engine.logFilename, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err.Error())
		}
		defer logFd.Close()

		// Слушаем
		for {
			select {
			case <-s.ctx.Done():
				return

			case tr := <-s.transactionCh:

				tr.Lsn = Engine.lsnCounter
				err = runTransaction(&Engine, tr)
				if err != nil {
					s.engineRespCh <- err
				} else {
					s.engineRespCh <- writeTransactionToFile(logFd, tr)
				}
				Engine.lsnCounter++
			}
		}

	}()
}

func initEngine(e *DBEngine) error {
	if err := initEngineFiles(e.logFilename, e.snapshotsDir); err != nil {
		return err
	}

	snapshot, err := getLastSnapshotFilename(e.snapshotsDir)
	if err != nil {
		return err
	}

	if err = runTransactionsFromFile(e, e.snapshotsDir+snapshot); err != nil && !os.IsNotExist(err) { // Пытаемся считать последний snapshot
		return err
	}

	err = runTransactionsFromFile(e, e.logFilename)
	if err != nil {
		return err
	}

	return nil
}

func initEngineFiles(logFilename string, snapshotsDir string) error {
	// Создадим директорию со снапшотами, если ее нет
	if err := os.Mkdir(snapshotsDir, 0755); err != nil && !os.IsExist(err) {
		return err
	}

	// Создадим журнал, если его нет
	if _, err := os.Stat(logFilename); err != nil {
		if _, err = os.Create(logFilename); err != nil {
			return err
		}
	}

	return nil
}

func saveSnapshot(e *DBEngine) error {
	file, err := os.OpenFile(
		e.snapshotsDir+"snapshot-"+time.Now().Format(SnapshotDateFormat)+".ckp",
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666,
	)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, feature := range e.features {
		// Сохраняю типа транзакции
		if err = writeTransactionToFile(file, Transaction{Action: Insert, Feature: feature}); err != nil {
			return err
		}
	}

	// Чистим журнал после создания snapshot'а
	if err = cleanLog(e.logFilename); err != nil {
		return err
	}

	return nil
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

// TODO: сюда еще Rtree научиться бы поддерживать + select
func runTransaction(e *DBEngine, transaction Transaction) error {
	switch transaction.Action {
	case Insert:
		e.features[transaction.Feature.ID.(string)] = transaction.Feature
		e.rt.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Replace:
		e.features[transaction.Feature.ID.(string)] = transaction.Feature

		e.rt.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
		e.rt.Insert(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Delete:
		delete(e.features, transaction.Feature.ID.(string))
		e.rt.Delete(transaction.Feature.Geometry.Bound().Min, transaction.Feature.Geometry.Bound().Max, transaction.Feature)
	case Snapshot:
		return saveSnapshot(e)
	case Select:
		return nil
	default:
		return errors.ErrUnsupported
	}
	return nil
}

func runTransactionsFromFile(e *DBEngine, filename string) error {
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tmpTr Transaction
		if err = json.Unmarshal(scanner.Bytes(), &tmpTr); err != nil {
			return err
		}

		if err = runTransaction(e, tmpTr); err != nil {
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
