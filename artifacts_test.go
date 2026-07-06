package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestGraphArtifactAPI(t *testing.T) {
	dir := t.TempDir()
	graphPath := filepath.Join(dir, "graph_snapshot.json")
	writeTestGraphArtifact(t, graphPath, "edge-a")
	hub := NewHub(nil)
	store := NewArtifactStore(hub, NewSportsState(), "", graphPath, "", "")
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	server := NewAPIServer(hub, nil).WithArtifactStore(store)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/graph/snapshot", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got GraphSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 || len(got.Edges) != 1 || got.Edges[0].Source != "edge-a" {
		t.Fatalf("unexpected graph snapshot: %+v", got)
	}
}

func TestMissingGraphArtifactWarns(t *testing.T) {
	store := NewArtifactStore(NewHub(nil), NewSportsState(), "", filepath.Join(t.TempDir(), "missing.json"), "", "")
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	snapshot := store.GraphSnapshot(NewHub(nil).Snapshot())
	if len(snapshot.Warnings) != 1 || len(snapshot.Edges) != 0 {
		t.Fatalf("unexpected missing-artifact snapshot: %+v", snapshot)
	}
}

func TestArtifactReloadSwapsGraph(t *testing.T) {
	dir := t.TempDir()
	graphPath := filepath.Join(dir, "graph_snapshot.json")
	writeTestGraphArtifact(t, graphPath, "edge-a")
	store := NewArtifactStore(NewHub(nil), NewSportsState(), "", graphPath, "", "")
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	writeTestGraphArtifact(t, graphPath, "edge-b")
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	snapshot := store.GraphSnapshot(NewHub(nil).Snapshot())
	if len(snapshot.Edges) != 1 || snapshot.Edges[0].Source != "edge-b" {
		t.Fatalf("reload did not swap graph: %+v", snapshot.Edges)
	}
}

func TestArtifactReloadMissingGraphClearsPreviousSnapshot(t *testing.T) {
	dir := t.TempDir()
	graphPath := filepath.Join(dir, "graph_snapshot.json")
	writeTestGraphArtifact(t, graphPath, "edge-a")
	store := NewArtifactStore(NewHub(nil), NewSportsState(), "", graphPath, "", "")
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(graphPath); err != nil {
		t.Fatal(err)
	}
	if err := store.Reload(); err != nil {
		t.Fatal(err)
	}

	snapshot := store.GraphSnapshot(NewHub(nil).Snapshot())
	if len(snapshot.Edges) != 0 || len(snapshot.Warnings) != 1 {
		t.Fatalf("missing graph did not clear snapshot: %+v", snapshot)
	}
}

func writeTestGraphArtifact(t *testing.T, path string, edgeSource string) {
	t.Helper()
	data := []byte(`{
		"version":"v0.1.0",
		"built_at":"2026-07-03T00:00:00Z",
		"source_manifest":"build_manifest.json",
		"counts":{"nodes":1,"logic_edges":1,"conditionals":0,"violations":1},
		"nodes":[{"node_id":"node-a","market_id":"m1","question":"Will Alpha win?","outcome_label":"Yes","canonical_proposition":"Will Alpha win?","team":"Alpha","stage_key":"winner","current_price":0.4,"current_price_devig":0.4}],
		"logic_edges":[{"source":"` + edgeSource + `","target":"node-b","type":"implies","basis":"stage_progression_rule","confidence":1,"current_p_src":0.4,"current_p_dst":0.5}],
		"conditionals":[],
		"violations":[{"id":"v1","type":"logic","severity":"low","description":"test violation"}]
	}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
