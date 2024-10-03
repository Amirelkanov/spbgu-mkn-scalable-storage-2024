package main

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb/geojson"
)

// TODO: сделать сообсна)
func postTest(t *testing.T, mux *http.ServeMux, url string, body []byte) {

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code == http.StatusTemporaryRedirect {
		req, err := http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
		}
	} else if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func insertTest(t *testing.T, mux *http.ServeMux, storage *Storage) {

	point := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point.ID = uuid.New().String()

	body, err := point.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	postTest(t, mux, "/"+storage.name+"/insert", body)
}

func replaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.Float64(), rand.Float64()}, orb.Point{rand.Float64(), rand.Float64()}})
	line.ID = uuid.New().String()

	body, err := line.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	postTest(t, mux, "/"+storage.name+"/replace", body)
}

func deleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.Float64(), rand.Float64()}, orb.Point{rand.Float64(), rand.Float64()}})
	line.ID = uuid.New().String()

	body, err := line.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	postTest(t, mux, "/"+storage.name+"/delete", body)
}

func checkpointTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	postTest(t, mux, "/"+storage.name+"/checkpoint", []byte{})
}

func TestComplex(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true)
	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)

	insertTest(t, mux, s)
	replaceTest(t, mux, s)
	deleteTest(t, mux, s)
	checkpointTest(t, mux, s)
}
