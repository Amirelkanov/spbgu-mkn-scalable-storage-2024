package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/stretchr/testify/assert"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/paulmach/orb/geojson"
)

func postTest(t *testing.T, mux *http.ServeMux, url string, feature *geojson.Feature) {

	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
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

func checkJSONContents(t *testing.T, feature *geojson.Feature, storage *Storage) {
	fileBody, err := os.ReadFile(storage.dir + "/" + feature.ID.(string) + ".json")
	assert.True(t, err == nil)

	featureBody, err := json.Marshal(feature)
	assert.True(t, err == nil)

	assert.Equal(t, fileBody, featureBody)
}

func insertTest(t *testing.T, mux *http.ServeMux, storage *Storage) *geojson.Feature {
	point := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point.ID = uuid.New().String()

	postTest(t, mux, "/"+storage.name+"/insert", point)
	checkJSONContents(t, point, storage)

	return point // Здесь я возвращаю *geojson.Feature для удобства тестирования deleteTest
}

func replaceTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	line := geojson.NewFeature(orb.LineString{orb.Point{rand.Float64(), rand.Float64()}, orb.Point{rand.Float64(), rand.Float64()}})
	line.ID = uuid.New().String()

	postTest(t, mux, "/"+storage.name+"/replace", line)
	checkJSONContents(t, line, storage)
}

func deleteTest(t *testing.T, mux *http.ServeMux, storage *Storage) {
	feature := insertTest(t, mux, storage)
	postTest(t, mux, "/"+storage.name+"/delete", feature)

	_, err := os.ReadFile(storage.dir + "/" + feature.ID.(string) + ".json")
	assert.True(t, os.IsNotExist(err))
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
}
