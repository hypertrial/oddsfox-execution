package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const defaultPolymarketSportsWS = "wss://sports-api.polymarket.com/ws"

type SportsState struct {
	mu      sync.RWMutex
	results map[string]MatchResult
}

func NewSportsState() *SportsState {
	return &SportsState{results: map[string]MatchResult{}}
}

func (s *SportsState) HandleRaw(data []byte) bool {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "ping" || trimmed == "pong" {
		return false
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		return false
	}
	slug := firstNonEmpty(
		stringField(obj, "sports_slug"),
		stringField(obj, "slug"),
		stringField(obj, "event_slug"),
		stringField(obj, "game_id"),
	)
	if slug == "" {
		return false
	}
	result := MatchResult{
		SportsSlug:   slug,
		Status:       firstNonEmpty(stringField(obj, "status"), stringField(obj, "state"), stringField(obj, "event_type")),
		WinnerTeamID: stringField(obj, "winner_team_id"),
		TeamIDs:      stringListField(obj, "team_ids"),
		Score:        firstNonEmpty(stringField(obj, "score"), scoreString(obj)),
		Source:       "sports_ws",
		UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	s.mu.Lock()
	s.results[slug] = result
	s.mu.Unlock()
	return true
}

func (s *SportsState) Results() []MatchResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]MatchResult, 0, len(s.results))
	for _, result := range s.results {
		out = append(out, result)
	}
	return out
}

type SportsClient struct {
	endpoint string
	dial     wsDial
	state    *SportsState
	publish  func(LiveEvent)
	retryMin time.Duration
	retryMax time.Duration
}

func NewSportsClient(endpoint string, state *SportsState, publish func(LiveEvent)) *SportsClient {
	return &SportsClient{
		endpoint: endpoint,
		dial:     defaultDial,
		state:    state,
		publish:  publish,
		retryMin: time.Second,
		retryMax: 30 * time.Second,
	}
}

func (c *SportsClient) Run(ctx context.Context) {
	backoff := c.retryMin
	for ctx.Err() == nil {
		if err := c.runOnce(ctx); err != nil && ctx.Err() == nil {
			sleep(ctx, backoff)
			backoff *= 2
			if backoff > c.retryMax {
				backoff = c.retryMax
			}
			continue
		}
		backoff = c.retryMin
	}
}

func (c *SportsClient) runOnce(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, err := c.dial(dialCtx, c.endpoint)
	cancel()
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if strings.TrimSpace(string(data)) == "ping" {
			if err := conn.Write(ctx, websocket.MessageText, []byte("pong")); err != nil {
				return err
			}
			continue
		}
		if c.state.HandleRaw(data) && c.publish != nil {
			payload := append([]byte(nil), data...)
			c.publish(LiveEvent{
				Type:    "sports_update",
				Payload: payload,
			})
		}
	}
}

func scoreString(obj map[string]any) string {
	home := stringField(obj, "home_score")
	away := stringField(obj, "away_score")
	if home == "" || away == "" {
		return ""
	}
	return home + "-" + away
}
