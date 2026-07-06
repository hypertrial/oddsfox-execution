package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const apiVersion = "v0.1.0"

type AssetState struct {
	AssetID        string    `json:"asset_id"`
	Market         string    `json:"market,omitempty"`
	Question       string    `json:"question,omitempty"`
	Slug           string    `json:"slug,omitempty"`
	BestBid        string    `json:"best_bid,omitempty"`
	BestAsk        string    `json:"best_ask,omitempty"`
	Spread         string    `json:"spread,omitempty"`
	LastPrice      string    `json:"last_price,omitempty"`
	LastTradeSide  string    `json:"last_trade_side,omitempty"`
	TickSize       string    `json:"tick_size,omitempty"`
	Resolved       bool      `json:"resolved"`
	WinningAssetID string    `json:"winning_asset_id,omitempty"`
	WinningOutcome string    `json:"winning_outcome,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type GraphEdge struct {
	Source      string   `json:"source"`
	Target      string   `json:"target"`
	Type        string   `json:"type"`
	Basis       string   `json:"basis,omitempty"`
	Confidence  *float64 `json:"confidence,omitempty"`
	CurrentPSrc *float64 `json:"current_p_src,omitempty"`
	CurrentPDst *float64 `json:"current_p_dst,omitempty"`
}

type Violation struct {
	ID          string `json:"id"`
	Type        string `json:"type,omitempty"`
	Severity    string `json:"severity,omitempty"`
	Message     string `json:"message"`
	Description string `json:"description,omitempty"`
	SrcNodeID   string `json:"src_node_id,omitempty"`
	DstNodeID   string `json:"dst_node_id,omitempty"`
	MarketIDSrc string `json:"market_id_src,omitempty"`
	MarketIDDst string `json:"market_id_dst,omitempty"`
}

type GraphNode struct {
	NodeID               string   `json:"node_id"`
	MarketID             string   `json:"market_id"`
	Question             string   `json:"question"`
	OutcomeLabel         string   `json:"outcome_label"`
	CanonicalProposition string   `json:"canonical_proposition"`
	Team                 string   `json:"team,omitempty"`
	StageKey             string   `json:"stage_key,omitempty"`
	CurrentPrice         *float64 `json:"current_price,omitempty"`
	CurrentPriceDevig    *float64 `json:"current_price_devig,omitempty"`
}

type ConditionalRow struct {
	ANodeID    string   `json:"a_node_id"`
	BNodeID    string   `json:"b_node_id"`
	PAGivenB   *float64 `json:"p_a_given_b,omitempty"`
	LowerBound *float64 `json:"lower_bound,omitempty"`
	UpperBound *float64 `json:"upper_bound,omitempty"`
	Method     string   `json:"method"`
	Confidence *float64 `json:"confidence,omitempty"`
}

type GraphMetadata struct {
	Version        string         `json:"version,omitempty"`
	BuiltAt        string         `json:"built_at,omitempty"`
	SourceManifest string         `json:"source_manifest,omitempty"`
	Counts         map[string]int `json:"counts,omitempty"`
}

type GraphSnapshot struct {
	Version      string           `json:"version"`
	UpdatedAt    time.Time        `json:"updated_at"`
	Assets       []AssetState     `json:"assets"`
	Metadata     GraphMetadata    `json:"metadata"`
	Nodes        []GraphNode      `json:"nodes"`
	Edges        []GraphEdge      `json:"edges"`
	Conditionals []ConditionalRow `json:"conditionals"`
	Violations   []Violation      `json:"violations"`
	Warnings     []string         `json:"warnings"`
}

type LiveEvent struct {
	ID         string          `json:"id"`
	ReceivedAt time.Time       `json:"received_at"`
	Type       string          `json:"type"`
	AssetID    string          `json:"asset_id,omitempty"`
	Market     string          `json:"market,omitempty"`
	Payload    json.RawMessage `json:"payload"`
}

type Hub struct {
	mu        sync.RWMutex
	assets    map[string]*AssetState
	subs      map[string]struct{}
	events    []LiveEvent
	sinks     map[chan LiveEvent]struct{}
	replay    *ReplayLog
	connected bool
	lastError string
	seq       int64
}

func NewHub(replay *ReplayLog) *Hub {
	return &Hub{
		assets: make(map[string]*AssetState),
		subs:   make(map[string]struct{}),
		sinks:  make(map[chan LiveEvent]struct{}),
		replay: replay,
	}
}

func (h *Hub) AddSubscriptions(ids []string) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	added := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := h.subs[id]; ok {
			continue
		}
		h.subs[id] = struct{}{}
		h.ensureAssetLocked(id).UpdatedAt = time.Now().UTC()
		added = append(added, id)
	}
	return added
}

func (h *Hub) Subscriptions() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return sortedKeys(h.subs)
}

func (h *Hub) SetConnectionState(connected bool, lastError string) {
	h.mu.Lock()
	h.connected = connected
	h.lastError = lastError
	h.mu.Unlock()
}

func (h *Hub) Health() map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return map[string]any{
		"status":        "ok",
		"version":       apiVersion,
		"connected":     h.connected,
		"last_error":    h.lastError,
		"subscriptions": len(h.subs),
		"assets":        len(h.assets),
	}
}

func (h *Hub) Snapshot() GraphSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	assets := make([]AssetState, 0, len(h.assets))
	for _, asset := range h.assets {
		assets = append(assets, *asset)
	}
	sort.Slice(assets, func(i, j int) bool { return assets[i].AssetID < assets[j].AssetID })
	return GraphSnapshot{
		Version:      apiVersion,
		UpdatedAt:    time.Now().UTC(),
		Assets:       assets,
		Nodes:        []GraphNode{},
		Edges:        []GraphEdge{},
		Conditionals: []ConditionalRow{},
		Violations:   []Violation{},
		Warnings:     []string{},
	}
}

func (h *Hub) AssetStates() map[string]AssetState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]AssetState, len(h.assets))
	for id, asset := range h.assets {
		out[id] = *asset
	}
	return out
}

func (h *Hub) RecentEvents(limit int) []LiveEvent {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if limit <= 0 || limit > len(h.events) {
		limit = len(h.events)
	}
	out := append([]LiveEvent(nil), h.events[len(h.events)-limit:]...)
	return out
}

func (h *Hub) SubscribeSSE() (<-chan LiveEvent, func()) {
	ch := make(chan LiveEvent, 32)
	h.mu.Lock()
	h.sinks[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.sinks, ch)
		h.mu.Unlock()
	}
}

func (h *Hub) HandleRaw(data []byte) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "PONG" || trimmed == "{}" {
		return
	}
	var value any
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return
	}
	switch msg := value.(type) {
	case []any:
		for _, item := range msg {
			if obj, ok := item.(map[string]any); ok {
				h.handleObject(obj)
			}
		}
	case map[string]any:
		h.handleObject(msg)
	}
}

func (h *Hub) handleObject(obj map[string]any) {
	eventType := stringField(obj, "event_type")
	if eventType == "" {
		return
	}
	now := time.Now().UTC()
	payload, _ := json.Marshal(obj)
	event := LiveEvent{
		ReceivedAt: now,
		Type:       eventType,
		AssetID:    stringField(obj, "asset_id"),
		Market:     stringField(obj, "market"),
		Payload:    payload,
	}

	h.mu.Lock()
	h.applyObjectLocked(now, eventType, obj)
	h.mu.Unlock()
	h.Publish(event)
}

func (h *Hub) Publish(event LiveEvent) {
	h.mu.Lock()
	h.seq++
	event.ID = fmt.Sprintf("%d", h.seq)
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now().UTC()
	}
	h.events = append(h.events, event)
	if len(h.events) > 1000 {
		h.events = h.events[len(h.events)-1000:]
	}
	sinks := make([]chan LiveEvent, 0, len(h.sinks))
	for sink := range h.sinks {
		sinks = append(sinks, sink)
	}
	h.mu.Unlock()

	if h.replay != nil {
		_ = h.replay.Append(event)
	}
	for _, sink := range sinks {
		select {
		case sink <- event:
		default:
		}
	}
}

func (h *Hub) applyObjectLocked(now time.Time, eventType string, obj map[string]any) {
	switch eventType {
	case "book":
		asset := h.ensureAssetLocked(stringField(obj, "asset_id"))
		asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
		asset.BestBid = bestPrice(obj["bids"], true)
		asset.BestAsk = bestPrice(obj["asks"], false)
		asset.Spread = spread(asset.BestBid, asset.BestAsk)
		asset.UpdatedAt = now
	case "best_bid_ask":
		asset := h.ensureAssetLocked(stringField(obj, "asset_id"))
		asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
		asset.BestBid = firstNonEmpty(stringField(obj, "best_bid"), asset.BestBid)
		asset.BestAsk = firstNonEmpty(stringField(obj, "best_ask"), asset.BestAsk)
		asset.Spread = firstNonEmpty(stringField(obj, "spread"), spread(asset.BestBid, asset.BestAsk))
		asset.UpdatedAt = now
	case "last_trade_price":
		asset := h.ensureAssetLocked(stringField(obj, "asset_id"))
		asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
		asset.LastPrice = firstNonEmpty(stringField(obj, "price"), asset.LastPrice)
		asset.LastTradeSide = firstNonEmpty(stringField(obj, "side"), asset.LastTradeSide)
		asset.UpdatedAt = now
	case "tick_size_change":
		asset := h.ensureAssetLocked(stringField(obj, "asset_id"))
		asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
		asset.TickSize = firstNonEmpty(stringField(obj, "new_tick_size"), asset.TickSize)
		asset.UpdatedAt = now
	case "price_change":
		h.applyPriceChangesLocked(now, obj)
	case "new_market":
		for _, id := range stringListField(obj, "assets_ids", "clob_token_ids") {
			asset := h.ensureAssetLocked(id)
			asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
			asset.Question = firstNonEmpty(stringField(obj, "question"), asset.Question)
			asset.Slug = firstNonEmpty(stringField(obj, "slug"), asset.Slug)
			asset.UpdatedAt = now
		}
	case "market_resolved":
		for _, id := range stringListField(obj, "assets_ids", "clob_token_ids") {
			asset := h.ensureAssetLocked(id)
			asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
			asset.Resolved = true
			asset.WinningAssetID = firstNonEmpty(stringField(obj, "winning_asset_id"), asset.WinningAssetID)
			asset.WinningOutcome = firstNonEmpty(stringField(obj, "winning_outcome"), asset.WinningOutcome)
			asset.UpdatedAt = now
		}
	}
}

func (h *Hub) applyPriceChangesLocked(now time.Time, obj map[string]any) {
	changes, _ := obj["price_changes"].([]any)
	for _, item := range changes {
		change, ok := item.(map[string]any)
		if !ok {
			continue
		}
		asset := h.ensureAssetLocked(stringField(change, "asset_id"))
		asset.Market = firstNonEmpty(stringField(obj, "market"), asset.Market)
		asset.BestBid = firstNonEmpty(stringField(change, "best_bid"), asset.BestBid)
		asset.BestAsk = firstNonEmpty(stringField(change, "best_ask"), asset.BestAsk)
		asset.Spread = spread(asset.BestBid, asset.BestAsk)
		asset.UpdatedAt = now
	}
}

func (h *Hub) ensureAssetLocked(id string) *AssetState {
	if id == "" {
		id = "unknown"
	}
	if asset, ok := h.assets[id]; ok {
		return asset
	}
	asset := &AssetState{AssetID: id, UpdatedAt: time.Now().UTC()}
	h.assets[id] = asset
	return asset
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func stringField(obj map[string]any, key string) string {
	switch value := obj[key].(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(value)
	default:
		return ""
	}
}

func stringListField(obj map[string]any, keys ...string) []string {
	for _, key := range keys {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		items, ok := raw.([]any)
		if !ok {
			continue
		}
		out := make([]string, 0, len(items))
		for _, item := range items {
			switch value := item.(type) {
			case string:
				out = append(out, value)
			case json.Number:
				out = append(out, value.String())
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

func bestPrice(raw any, high bool) string {
	levels, _ := raw.([]any)
	best := 0.0
	bestText := ""
	for _, item := range levels {
		level, ok := item.(map[string]any)
		if !ok {
			continue
		}
		text := stringField(level, "price")
		value, err := strconv.ParseFloat(normalizePrice(text), 64)
		if err != nil {
			continue
		}
		if bestText == "" || (high && value > best) || (!high && value < best) {
			best = value
			bestText = text
		}
	}
	return bestText
}

func spread(bid string, ask string) string {
	b, errB := strconv.ParseFloat(normalizePrice(bid), 64)
	a, errA := strconv.ParseFloat(normalizePrice(ask), 64)
	if errB != nil || errA != nil || ask == "" || bid == "" {
		return ""
	}
	return strconv.FormatFloat(a-b, 'f', -1, 64)
}

func normalizePrice(value string) string {
	if strings.HasPrefix(value, ".") {
		return "0" + value
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
