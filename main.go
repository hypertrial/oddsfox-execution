package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const defaultPolymarketMarketWS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "HTTP listen address")
	wsURL := flag.String("ws", defaultPolymarketMarketWS, "Polymarket market WebSocket URL")
	sportsWS := flag.String("sports-ws", defaultPolymarketSportsWS, "Polymarket sports WebSocket URL")
	replayDir := flag.String("replay-dir", "replay", "JSONL replay log directory")
	assets := flag.String("assets", "", "comma-separated Polymarket asset/token IDs")
	artifactDir := flag.String("artifact-dir", getenv("ODDSFOX_ARTIFACT_DIR", ""), "artifact directory containing current/*.json")
	graphArtifact := flag.String("graph-artifact", getenv("ODDSFOX_GRAPH_ARTIFACT", ""), "oddsfox-graph graph_snapshot.json path")
	knockoutArtifact := flag.String("knockout-artifact", getenv("ODDSFOX_KNOCKOUT_ARTIFACT", ""), "oddsfox-graph knockout_artifacts.json path")
	reloadInterval := flag.Duration("artifact-reload-interval", envDuration("ODDSFOX_ARTIFACT_RELOAD_INTERVAL", 60*time.Second), "artifact reload interval")
	resultsOverride := flag.String("results-override", "", "local results override JSON path")
	flag.Parse()

	replay := NewReplayLog(*replayDir)
	hub := NewHub(replay)
	hub.AddSubscriptions(splitCSV(*assets))
	sportsState := NewSportsState()
	var artifacts *ArtifactStore
	if *artifactDir != "" || *graphArtifact != "" || *knockoutArtifact != "" {
		artifacts = NewArtifactStore(hub, sportsState, *artifactDir, *graphArtifact, *knockoutArtifact, *resultsOverride)
		if err := artifacts.Reload(); err != nil {
			log.Fatal(err)
		}
		go artifacts.Run(context.Background(), *reloadInterval)
		go NewSportsClient(*sportsWS, sportsState, hub.Publish).Run(context.Background())
	}

	client := NewMarketClient(*wsURL, hub.Subscriptions, hub.HandleRaw, hub.SetConnectionState)
	go client.Run(context.Background())

	server := NewAPIServer(hub, client).WithArtifactStore(artifacts)
	log.Printf("oddsfox-live listening on http://%s", *addr)
	if err := http.ListenAndServe(*addr, server.Routes()); err != nil {
		log.Fatal(err)
	}
}

func getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
