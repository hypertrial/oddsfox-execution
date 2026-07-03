package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type fakeConn struct {
	writes chan []byte
	errs   chan error
}

func newFakeConn() *fakeConn {
	return &fakeConn{writes: make(chan []byte, 4), errs: make(chan error, 1)}
}

func (f *fakeConn) Read(context.Context) (websocket.MessageType, []byte, error) {
	err := <-f.errs
	return websocket.MessageText, nil, err
}

func (f *fakeConn) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	f.writes <- append([]byte(nil), data...)
	return nil
}

func (f *fakeConn) Close(websocket.StatusCode, string) error { return nil }
func (f *fakeConn) CloseNow() error                          { return nil }

func TestMarketClientSubscribesAndReconnects(t *testing.T) {
	first := newFakeConn()
	second := newFakeConn()
	dials := make(chan wsConn, 2)
	dials <- first
	dials <- second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewMarketClient("ws://example", func() []string { return []string{"asset-a"} }, func([]byte) {}, func(bool, string) {})
	client.retryMin = 10 * time.Millisecond
	client.retryMax = 10 * time.Millisecond
	client.pingEvery = time.Hour
	client.dial = func(context.Context, string) (wsConn, error) { return <-dials, nil }

	go client.Run(ctx)
	assertWriteContains(t, first.writes, `"type":"market"`)
	first.errs <- errors.New("drop")
	assertWriteContains(t, second.writes, `"type":"market"`)
}

func assertWriteContains(t *testing.T, writes <-chan []byte, want string) {
	t.Helper()
	select {
	case data := <-writes:
		if !strings.Contains(string(data), want) {
			t.Fatalf("write %s does not contain %s", data, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for write containing %s", want)
	}
}
