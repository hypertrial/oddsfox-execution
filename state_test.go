package main

import "testing"

func TestHubHandlesMarketMessages(t *testing.T) {
	hub := NewHub(nil)
	hub.HandleRaw([]byte(`{
		"event_type":"book",
		"asset_id":"asset-a",
		"market":"0xmarket",
		"bids":[{"price":"0.48","size":"30"},{"price":"0.51","size":"5"}],
		"asks":[{"price":"0.55","size":"10"},{"price":"0.53","size":"20"}]
	}`))
	hub.HandleRaw([]byte(`{
		"event_type":"last_trade_price",
		"asset_id":"asset-a",
		"market":"0xmarket",
		"price":"0.52",
		"side":"BUY"
	}`))
	snapshot := hub.Snapshot()
	if len(snapshot.Assets) != 1 {
		t.Fatalf("got %d assets", len(snapshot.Assets))
	}
	asset := snapshot.Assets[0]
	if asset.AssetID != "asset-a" || asset.BestBid != "0.51" || asset.BestAsk != "0.53" {
		t.Fatalf("unexpected asset state: %+v", asset)
	}
	if asset.LastPrice != "0.52" || asset.LastTradeSide != "BUY" {
		t.Fatalf("missing trade state: %+v", asset)
	}
}

func TestHubHandlesPriceChanges(t *testing.T) {
	hub := NewHub(nil)
	hub.HandleRaw([]byte(`{
		"event_type":"price_change",
		"market":"0xmarket",
		"price_changes":[{"asset_id":"asset-a","best_bid":"0.4","best_ask":"0.6"}]
	}`))
	asset := hub.Snapshot().Assets[0]
	if asset.Spread != "0.19999999999999996" {
		t.Fatalf("unexpected spread %q", asset.Spread)
	}
}

func TestBestPriceHandlesLeadingDotPrices(t *testing.T) {
	hub := NewHub(nil)
	hub.HandleRaw([]byte(`{
		"event_type":"book",
		"asset_id":"asset-a",
		"bids":[{"price":".48","size":"30"},{"price":".5","size":"5"}],
		"asks":[{"price":".55","size":"10"},{"price":".53","size":"20"}]
	}`))
	asset := hub.Snapshot().Assets[0]
	if asset.BestBid != ".5" || asset.BestAsk != ".53" {
		t.Fatalf("unexpected best prices: %+v", asset)
	}
}
