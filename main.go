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
	replayDir := flag.String("replay-dir", "replay", "JSONL replay log directory")
	assets := flag.String("assets", "", "comma-separated Polymarket asset/token IDs")
	flag.Parse()

	replay := NewReplayLog(*replayDir)
	hub := NewHub(replay)
	hub.AddSubscriptions(splitCSV(*assets))

	client := NewMarketClient(*wsURL, hub.Subscriptions, hub.HandleRaw, hub.SetConnectionState)
	go client.Run(context.Background())

	server := NewAPIServer(hub, client)
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
