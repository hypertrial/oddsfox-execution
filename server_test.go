package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSubscriptionsAPI(t *testing.T) {
	hub := NewHub(nil)
	server := NewAPIServer(hub, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v0/subscriptions", bytes.NewBufferString(`{"asset_ids":["b","a","a"]}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		AssetIDs []string `json:"asset_ids"`
		Added    []string `json:"added"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if got := stringsJoin(body.AssetIDs); got != "a,b" {
		t.Fatalf("asset ids = %s", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v0/graph/snapshot", nil)
	rec = httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("snapshot status %d", rec.Code)
	}
}

func stringsJoin(values []string) string {
	if len(values) == 0 {
		return ""
	}
	out := values[0]
	for _, value := range values[1:] {
		out += "," + value
	}
	return out
}
