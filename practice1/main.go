package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/paulmach/orb/geojson"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Router struct {
	// Поля
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

type Storage struct {
	name string
	dir  string
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	storage := Storage{name, wd + "/" + name}

	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("INSERT QUERY")

		var feature geojson.Feature
		err := json.NewDecoder(r.Body).Decode(&feature)
		if err != nil {
			writeError(w, err)
			return
		}

		body, err := json.Marshal(feature)
		if err != nil {
			writeError(w, err)
			return
		}

		err = os.WriteFile(storage.dir+"/"+feature.ID.(string)+".json", body, 0644)
		if err != nil {
			writeError(w, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("REPLACE QUERY")

		var feature geojson.Feature
		err := json.NewDecoder(r.Body).Decode(&feature)
		if err != nil {
			writeError(w, err)
			return
		}

		body, err := json.Marshal(feature)
		if err != nil {
			writeError(w, err)
			return
		}

		err = os.WriteFile(storage.dir+"/"+feature.ID.(string)+".json", body, 0644)
		if err != nil {
			writeError(w, err)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("SELECT QUERY")

		featureCollection := geojson.NewFeatureCollection()

		dirEntries, err := os.ReadDir(storage.dir)
		if err != nil {
			writeError(w, err)
			return
		}

		for _, dirEntry := range dirEntries { // Пробежимся по всем json-ам и сохраним body
			body, err := os.ReadFile(storage.dir + "/" + dirEntry.Name())
			if err != nil {
				writeError(w, err)
				return
			}

			var currFeature geojson.Feature
			err = json.Unmarshal(body, &currFeature)
			if err != nil {
				writeError(w, err)
				return
			}
			featureCollection.Append(&currFeature)
		}

		body, err := json.Marshal(featureCollection)
		if err != nil {
			writeError(w, err)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, err = w.Write(body)
		if err != nil {
			writeError(w, err)
			return
		}
	})

	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("DELETE QUERY")

		var feature geojson.Feature
		err := json.NewDecoder(r.Body).Decode(&feature)
		if err != nil {
			writeError(w, err)
			return
		}

		err = os.Remove(storage.dir + "/" + feature.ID.(string) + ".json")
		if err != nil {
			writeError(w, err)
		}

		w.WriteHeader(http.StatusOK)
	})

	return &storage
}

// В тз я не увидел, нужно ли после перезагрузки оставлять файлы или не нужно,
// поэтому я сделал более чистенький вариант - 2

func (s *Storage) Run() {
	slog.Info("Starting storage '" + s.name + "'...")
	err := os.MkdirAll(s.dir, 0755)

	if err != nil {
		panic(err)
	}
}

func (s *Storage) Stop() {
	slog.Info("Stopping storage '" + s.name + "'...")
	err := os.RemoveAll(s.dir)

	if err != nil {
		panic(err)
	}
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
