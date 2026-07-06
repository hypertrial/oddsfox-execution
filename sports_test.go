package main

import (
	"context"
	"errors"
	"testing"

	"github.com/coder/websocket"
)

var errScriptDone = errors.New("done")

type scriptedConn struct {
	reads  [][]byte
	writes [][]byte
}

func (s *scriptedConn) Read(context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) == 0 {
		return websocket.MessageText, nil, errScriptDone
	}
	data := s.reads[0]
	s.reads = s.reads[1:]
	return websocket.MessageText, data, nil
}

func (s *scriptedConn) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	s.writes = append(s.writes, append([]byte(nil), data...))
	return nil
}

func (s *scriptedConn) Close(websocket.StatusCode, string) error { return nil }
func (s *scriptedConn) CloseNow() error                          { return nil }
func (s *scriptedConn) SetReadLimit(int64)                       {}

func TestSportsStateParsesResult(t *testing.T) {
	state := NewSportsState()
	if !state.HandleRaw([]byte(`{"sports_slug":"match-1","status":"final","home_score":2,"away_score":1}`)) {
		t.Fatal("expected update")
	}
	results := state.Results()
	if len(results) != 1 || results[0].SportsSlug != "match-1" || results[0].Score != "2-1" {
		t.Fatalf("unexpected results: %+v", results)
	}
}

func TestSportsClientPongAndPublish(t *testing.T) {
	conn := &scriptedConn{reads: [][]byte{
		[]byte("ping"),
		[]byte(`{"sports_slug":"match-1","status":"live"}`),
	}}
	state := NewSportsState()
	published := 0
	client := NewSportsClient("ws://sports", state, func(LiveEvent) { published++ })
	client.dial = func(context.Context, string) (wsConn, error) { return conn, nil }
	err := client.runOnce(context.Background())
	if !errors.Is(err, errScriptDone) {
		t.Fatalf("expected fake close, got %v", err)
	}
	if string(conn.writes[0]) != "pong" {
		t.Fatalf("expected pong, got %q", conn.writes[0])
	}
	if published != 1 {
		t.Fatalf("published %d updates", published)
	}
}
