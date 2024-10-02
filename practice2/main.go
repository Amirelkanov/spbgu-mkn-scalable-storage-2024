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

type Action int

const SnapshotDateFormat = "02-Jan-2006-15_04_05"

const (
	Insert Action = iota
	Replace
	Delete
)

type Router struct {
	// Поля
}

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
	lsn      uint64 // TODO При инициализации 0 - подумвй про мьютекс здесь, в остальных частях все ок

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
		panic(err)
	}
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	storage := Storage{name, make(chan Transaction), make(chan error), ctx, cancel}

	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("INSERT QUERY")

		var feature geojson.Feature
		err := json.NewDecoder(r.Body).Decode(&feature)
		if err != nil {
			writeError(w, err)
			return
		}
		storage.transactionCh <- Transaction{Name: name, Action: Insert, Feature: &feature}
		engineResp := <-storage.engineRespCh
		if engineResp == nil {
			w.WriteHeader(http.StatusOK)
		} else {
			writeError(w, engineResp)
		}
	})

	return &storage
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
			panic(err)
		}

		logFd, err := os.OpenFile(Engine.logFilename, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err.Error())
		}

		// Слушаем
		for {
			select {
			case <-s.ctx.Done():
				return

			case tr := <-s.transactionCh:
				runTransaction(&Engine, tr)
				s.engineRespCh <- writeTransactionToLog(logFd, tr)
			}
		}

	}()
}

func initEngine(e *DBEngine) error {
	if err := initEngineFiles(e.logFilename, e.snapshotsDir); err != nil {
		return err
	}

	snapshot, err := getLastSnapshot(e.snapshotsDir)
	if err != nil {
		return err
	}

	if err = readSnapshot(snapshot, e); err != nil && !os.IsNotExist(err) { // Пытаемся считать последний snapshot
		return err
	}

	err = runTransactionsFromLog(e)
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
		if _, err := os.Create(logFilename); err != nil {
			return err
		}
	}

	return nil
}

// Сохраняет данные из e.features
func saveSnapshot(e *DBEngine) error {
	// Надеюсь, не обидишься, если я не буду в снапшоте хранить транзакции честно...
	featureCollection := geojson.NewFeatureCollection()

	for _, feature := range e.features {
		featureCollection.Append(feature)
	}
	body, err := json.Marshal(featureCollection)
	if err != nil {
		return err
	}

	if err = os.WriteFile(e.snapshotsDir+"snapshot-"+time.Now().Format(SnapshotDateFormat)+".ckp", body, 0644); err != nil {
		return err
	}

	if err = cleanLog(e.logFilename); err != nil {
		return err
	}

	return nil
}

// Читает snapshot в e.features
func readSnapshot(snapshotFilename string, e *DBEngine) error {

	body, err := os.ReadFile(e.snapshotsDir + snapshotFilename)
	if err != nil {
		return err
	}

	tmpFeatureCollection := geojson.NewFeatureCollection()
	err = json.Unmarshal(body, tmpFeatureCollection)
	if err != nil {
		return err
	}
	for _, feature := range tmpFeatureCollection.Features {
		// Делаю "недотранзакцию" во избежание копипаст
		runTransaction(e, Transaction{Action: Insert, Feature: feature})
	}
	fmt.Println(e.features)
	return nil
}

func getLastSnapshot(snapshotsDir string) (string, error) {
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

func runTransactionsFromLog(e *DBEngine) error {
	file, err := os.Open(e.logFilename)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var tmpTr Transaction
		if err = json.Unmarshal(scanner.Bytes(), &tmpTr); err != nil {
			return err
		}
		runTransaction(e, tmpTr)
	}
	return nil
}

// TODO: сюда еще Rtree научиться бы поддерживать + select
func runTransaction(e *DBEngine, transaction Transaction) {
	switch transaction.Action {
	case Insert:
		e.features[transaction.Feature.ID.(string)] = transaction.Feature
	case Replace:
		e.features[transaction.Feature.ID.(string)] = transaction.Feature
	case Delete:
		delete(e.features, transaction.Feature.ID.(string))
	}
}

// 1 transaction - 1 line
func writeTransactionToLog(file *os.File, transaction Transaction) error {
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
			panic(err)
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
