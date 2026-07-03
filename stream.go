package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type wsConn interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Close(websocket.StatusCode, string) error
	CloseNow() error
}

type wsDial func(context.Context, string) (wsConn, error)

type MarketClient struct {
	endpoint  string
	dial      wsDial
	assets    func() []string
	handle    func([]byte)
	status    func(bool, string)
	pingEvery time.Duration
	retryMin  time.Duration
	retryMax  time.Duration
	connMu    sync.Mutex
	conn      wsConn
}

func NewMarketClient(endpoint string, assets func() []string, handle func([]byte), status func(bool, string)) *MarketClient {
	return &MarketClient{
		endpoint:  endpoint,
		dial:      defaultDial,
		assets:    assets,
		handle:    handle,
		status:    status,
		pingEvery: 10 * time.Second,
		retryMin:  time.Second,
		retryMax:  30 * time.Second,
	}
}

func defaultDial(ctx context.Context, url string) (wsConn, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *MarketClient) Run(ctx context.Context) {
	backoff := c.retryMin
	for ctx.Err() == nil {
		assets := c.assets()
		if len(assets) == 0 {
			sleep(ctx, time.Second)
			continue
		}
		if err := c.runOnce(ctx, assets); err != nil && ctx.Err() == nil {
			c.status(false, err.Error())
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

func (c *MarketClient) SubscribeAssets(ctx context.Context, assetIDs []string) error {
	if len(assetIDs) == 0 {
		return nil
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return nil
	}
	return writeJSON(ctx, conn, map[string]any{
		"operation":              "subscribe",
		"assets_ids":             assetIDs,
		"custom_feature_enabled": true,
	})
}

func (c *MarketClient) runOnce(ctx context.Context, assetIDs []string) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, err := c.dial(dialCtx, c.endpoint)
	cancel()
	if err != nil {
		return err
	}
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	defer func() {
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()
		_ = conn.CloseNow()
	}()

	if err := writeJSON(ctx, conn, map[string]any{
		"assets_ids":             assetIDs,
		"type":                   "market",
		"custom_feature_enabled": true,
	}); err != nil {
		return err
	}
	c.status(true, "")

	pingCtx, pingCancel := context.WithCancel(ctx)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(c.pingEvery)
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				_ = conn.Write(pingCtx, websocket.MessageText, []byte("PING"))
			}
		}
	}()
	defer func() {
		pingCancel()
		<-pingDone
	}()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		c.handle(data)
	}
}

func writeJSON(ctx context.Context, conn wsConn, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
