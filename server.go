package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type APIServer struct {
	hub      *Hub
	client   *MarketClient
	knockout *KnockoutService
}

func NewAPIServer(hub *Hub, client *MarketClient) *APIServer {
	return &APIServer{hub: hub, client: client}
}

func (s *APIServer) WithKnockout(knockout *KnockoutService) *APIServer {
	s.knockout = knockout
	return s
}

func (s *APIServer) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/health", s.handleHealth)
	mux.HandleFunc("/api/v0/subscriptions", s.handleSubscriptions)
	mux.HandleFunc("/api/v0/graph/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/v0/knockout/snapshot", s.handleKnockoutSnapshot)
	mux.HandleFunc("/api/v0/stream", s.handleStream)
	mux.HandleFunc("/api/v0/replay/events", s.handleReplay)
	return withCORS(mux)
}

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSONResponse(w, s.hub.Health())
}

func (s *APIServer) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSONResponse(w, map[string]any{"asset_ids": s.hub.Subscriptions()})
	case http.MethodPost:
		var req struct {
			AssetIDs []string `json:"asset_ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		added := s.hub.AddSubscriptions(req.AssetIDs)
		if s.client != nil {
			_ = s.client.SubscribeAssets(context.Background(), added)
		}
		writeJSONResponse(w, map[string]any{"asset_ids": s.hub.Subscriptions(), "added": added})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *APIServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	writeJSONResponse(w, s.hub.Snapshot())
}

func (s *APIServer) handleKnockoutSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.knockout == nil {
		http.Error(w, "knockout artifact not configured", http.StatusNotFound)
		return
	}
	writeJSONResponse(w, s.knockout.Snapshot())
}

func (s *APIServer) handleReplay(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var events []LiveEvent
	if s.hub.replay != nil {
		events = ReadReplayEvents(s.hub.replay.dir, limit)
	}
	if len(events) == 0 {
		events = s.hub.RecentEvents(limit)
	}
	writeJSONResponse(w, map[string]any{"events": events})
}

func (s *APIServer) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	ch, cancel := s.hub.SubscribeSSE()
	defer cancel()
	writeSSE(w, "snapshot", s.hub.Snapshot())
	flusher.Flush()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			writeSSE(w, "event", event)
			if s.knockout != nil && (event.Type == "sports_update" || event.AssetID != "") {
				writeSSE(w, "knockout_snapshot", s.knockout.Snapshot())
			}
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func writeJSONResponse(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeSSE(w http.ResponseWriter, eventType string, value any) {
	data, _ := json.Marshal(value)
	fmt.Fprintf(w, "event: %s\n", eventType)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
