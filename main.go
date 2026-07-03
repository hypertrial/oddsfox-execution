package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strings"
)

const defaultPolymarketMarketWS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

func main() {
	addr := flag.String("addr", "127.0.0.1:8787", "HTTP listen address")
	wsURL := flag.String("ws", defaultPolymarketMarketWS, "Polymarket market WebSocket URL")
	sportsWS := flag.String("sports-ws", defaultPolymarketSportsWS, "Polymarket sports WebSocket URL")
	replayDir := flag.String("replay-dir", "replay", "JSONL replay log directory")
	assets := flag.String("assets", "", "comma-separated Polymarket asset/token IDs")
	knockoutArtifact := flag.String("knockout-artifact", "", "oddsfox-graph knockout_artifacts.json path")
	resultsOverride := flag.String("results-override", "", "local results override JSON path")
	flag.Parse()

	replay := NewReplayLog(*replayDir)
	hub := NewHub(replay)
	hub.AddSubscriptions(splitCSV(*assets))
	sportsState := NewSportsState()
	var knockout *KnockoutService
	if *knockoutArtifact != "" {
		artifact, err := LoadKnockoutArtifact(*knockoutArtifact)
		if err != nil {
			log.Fatal(err)
		}
		knockout = NewKnockoutService(*artifact, hub, sportsState, *resultsOverride)
		hub.AddSubscriptions(knockout.AssetIDs())
		go NewSportsClient(*sportsWS, sportsState, hub.Publish).Run(context.Background())
	}

	client := NewMarketClient(*wsURL, hub.Subscriptions, hub.HandleRaw, hub.SetConnectionState)
	go client.Run(context.Background())

	server := NewAPIServer(hub, client).WithKnockout(knockout)
	log.Printf("oddsfox-live listening on http://%s", *addr)
	if err := http.ListenAndServe(*addr, server.Routes()); err != nil {
		log.Fatal(err)
	}
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
