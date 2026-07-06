package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestKnockoutSnapshotUsesResultOverride(t *testing.T) {
	hub := NewHub(nil)
	artifact := testKnockoutArtifact()
	hub.AddSubscriptions(artifact.AssetIDs)
	overridePath := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(
		overridePath,
		[]byte(`{"slots":{"final-1":{"team_ids":["alpha","beta"],"winner_team_id":"alpha","status":"final"}}}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	service := NewKnockoutService(artifact, hub, NewSportsState(), overridePath)
	snapshot := service.Snapshot()

	if len(snapshot.MatchResults) != 1 || snapshot.MatchResults[0].Source != "override" {
		t.Fatalf("missing override result: %+v", snapshot.MatchResults)
	}
	got := map[string]TeamProbability{}
	for _, probability := range snapshot.TeamProbabilities {
		got[probability.TeamID+"|"+probability.StageKey] = probability
	}
	if got["alpha|winner"].Probability == nil || *got["alpha|winner"].Probability != 1 {
		t.Fatalf("alpha winner override not applied: %+v", got["alpha|winner"])
	}
	if got["beta|winner"].Probability == nil || *got["beta|winner"].Probability != 0 {
		t.Fatalf("beta winner override not applied: %+v", got["beta|winner"])
	}
	if got["alpha|final"].Probability == nil || *got["alpha|final"].Probability != 1 {
		t.Fatalf("alpha final result not applied: %+v", got["alpha|final"])
	}
	if got["beta|final"].Probability == nil || *got["beta|final"].Probability != 1 {
		t.Fatalf("beta final result not applied: %+v", got["beta|final"])
	}
}

func TestKnockoutSnapshotUsesLiveMidpoint(t *testing.T) {
	hub := NewHub(nil)
	artifact := testKnockoutArtifact()
	hub.AddSubscriptions(artifact.AssetIDs)
	hub.HandleRaw([]byte(`{
		"event_type":"best_bid_ask",
		"asset_id":"asset-alpha-win",
		"best_bid":"0.40",
		"best_ask":"0.50"
	}`))
	snapshot := NewKnockoutService(artifact, hub, NewSportsState(), "").Snapshot()
	for _, probability := range snapshot.TeamProbabilities {
		if probability.TeamID == "alpha" && probability.StageKey == "winner" {
			if probability.Probability == nil || *probability.Probability != 0.45 || probability.Source != "live_midpoint" {
				t.Fatalf("live midpoint not applied: %+v", probability)
			}
			return
		}
	}
	t.Fatal("missing alpha winner probability")
}

func TestKnockoutSnapshotUsesProgressionAssetForNoToken(t *testing.T) {
	hub := NewHub(nil)
	value := 0.1
	artifact := KnockoutArtifact{
		Competition: "wc2026",
		Stages:      []KnockoutStage{{Key: "round_of_16", Rank: 1, Label: "Round of 16"}},
		Teams:       []KnockoutTeam{{TeamID: "alpha", Name: "Alpha"}},
		TeamStageMarkets: []TeamStageMarket{{
			TeamID:              "alpha",
			Team:                "Alpha",
			StageKey:            "round_of_16",
			AssetID:             "alpha-no",
			ProgressionAssetID:  "alpha-no",
			OppositeAssetID:     "alpha-yes",
			YesAssetID:          "alpha-yes",
			NoAssetID:           "alpha-no",
			BaselineProbability: &value,
		}},
		AssetIDs: []string{"alpha-yes", "alpha-no"},
	}
	hub.AddSubscriptions(artifact.AssetIDs)
	hub.HandleRaw([]byte(`{"event_type":"last_trade_price","asset_id":"alpha-no","price":"0.70"}`))
	hub.HandleRaw([]byte(`{"event_type":"last_trade_price","asset_id":"alpha-yes","price":"0.30"}`))

	snapshot := NewKnockoutService(artifact, hub, NewSportsState(), "").Snapshot()

	if len(snapshot.TeamProbabilities) != 1 {
		t.Fatalf("unexpected probabilities: %+v", snapshot.TeamProbabilities)
	}
	got := snapshot.TeamProbabilities[0]
	if got.Probability == nil || *got.Probability != 0.7 || got.Source != "live_devig_last_trade" {
		t.Fatalf("No-token progression not used: %+v", got)
	}
}

func TestLoadKnockoutArtifact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knockout_artifacts.json")
	data, err := json.Marshal(testKnockoutArtifact())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := LoadKnockoutArtifact(path)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Competition != "wc2026" || len(artifact.AssetIDs) != 8 {
		t.Fatalf("unexpected artifact: %+v", artifact)
	}
}

func TestKnockoutTimeseriesFiltersPinnedTeamAndForcesResult(t *testing.T) {
	artifact := testKnockoutArtifact()
	overridePath := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(
		overridePath,
		[]byte(`{"slots":{"final-1":{"team_ids":["alpha","beta"],"winner_team_id":"alpha","status":"final","updated_at":"2026-07-03T02:30:00Z"}}}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	service := NewKnockoutService(artifact, NewHub(nil), NewSportsState(), overridePath)
	got := service.Timeseries(mapQuery("stage=winner&team_id=beta"))

	if len(got.Series) != 1 || got.Series[0].TeamID != "beta" {
		t.Fatalf("unexpected series: %+v", got.Series)
	}
	last := got.Series[0].Points[len(got.Series[0].Points)-1]
	if last.Probability == nil || *last.Probability != 0 || !last.ResultForced {
		t.Fatalf("result forcing missing: %+v", last)
	}
	if len(got.ResultMarkers) != 1 || got.ResultMarkers[0].HourUTC != "2026-07-03T02:00:00Z" {
		t.Fatalf("unexpected markers: %+v", got.ResultMarkers)
	}
}

func TestKnockoutTimeseriesEndpoint(t *testing.T) {
	server := NewAPIServer(NewHub(nil), nil).WithKnockout(NewKnockoutService(testKnockoutArtifact(), NewHub(nil), NewSportsState(), ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v0/knockout/timeseries?stage=winner&limit_teams=1", nil)
	rec := httptest.NewRecorder()

	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var got KnockoutTimeseries
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.StageKey != "winner" || len(got.Series) != 1 {
		t.Fatalf("unexpected timeseries: %+v", got)
	}
}

func TestKnockoutOverrideInfersStageFromSlotID(t *testing.T) {
	artifact := testKnockoutArtifact()
	artifact.BracketSlots = nil
	overridePath := filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(
		overridePath,
		[]byte(`{"slots":{"final-1":{"team_ids":["alpha","beta"],"winner_team_id":"alpha"}}}`),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	snapshot := NewKnockoutService(artifact, NewHub(nil), NewSportsState(), overridePath).Snapshot()
	for _, probability := range snapshot.TeamProbabilities {
		if probability.TeamID == "alpha" && probability.StageKey == "winner" {
			if probability.Probability == nil || *probability.Probability != 1 {
				t.Fatalf("slot_id fallback not applied: %+v", probability)
			}
			return
		}
	}
	t.Fatal("missing alpha winner probability")
}

func testKnockoutArtifact() KnockoutArtifact {
	alphaWin := 0.25
	alphaFinal := 0.60
	betaWin := 0.20
	betaFinal := 0.50
	return KnockoutArtifact{
		Competition:    "wc2026",
		BuiltAt:        "2026-07-03T00:00:00Z",
		SourceManifest: "build_manifest.json",
		Stages: []KnockoutStage{
			{Key: "final", Rank: 4, Label: "Final", SlotCount: 1},
			{Key: "winner", Rank: 5, Label: "Winner", SlotCount: 1},
		},
		Teams: []KnockoutTeam{
			{TeamID: "alpha", Name: "Alpha"},
			{TeamID: "beta", Name: "Beta"},
		},
		TeamStageMarkets: []TeamStageMarket{
			{TeamID: "alpha", Team: "Alpha", StageKey: "winner", AssetID: "asset-alpha-win", YesAssetID: "asset-alpha-win", NoAssetID: "asset-alpha-win-no", BaselineProbability: &alphaWin},
			{TeamID: "alpha", Team: "Alpha", StageKey: "final", AssetID: "asset-alpha-final", YesAssetID: "asset-alpha-final", NoAssetID: "asset-alpha-final-no", BaselineProbability: &alphaFinal},
			{TeamID: "beta", Team: "Beta", StageKey: "winner", AssetID: "asset-beta-win", YesAssetID: "asset-beta-win", NoAssetID: "asset-beta-win-no", BaselineProbability: &betaWin},
			{TeamID: "beta", Team: "Beta", StageKey: "final", AssetID: "asset-beta-final", YesAssetID: "asset-beta-final", NoAssetID: "asset-beta-final-no", BaselineProbability: &betaFinal},
		},
		TeamStageHourly: []TeamStageHourly{
			{TeamID: "alpha", Team: "Alpha", StageKey: "winner", HourUTC: "2026-07-03T00:00:00Z", HourEpoch: 1783036800, Probability: floatPtr(0.20), Source: "hourly_close", StaleAgeHours: intPtr(0)},
			{TeamID: "alpha", Team: "Alpha", StageKey: "winner", HourUTC: "2026-07-03T01:00:00Z", HourEpoch: 1783040400, Probability: floatPtr(0.25), Source: "hourly_close", StaleAgeHours: intPtr(0)},
			{TeamID: "alpha", Team: "Alpha", StageKey: "winner", HourUTC: "2026-07-03T02:00:00Z", HourEpoch: 1783044000, Probability: floatPtr(0.30), Source: "hourly_close", StaleAgeHours: intPtr(0)},
			{TeamID: "beta", Team: "Beta", StageKey: "winner", HourUTC: "2026-07-03T00:00:00Z", HourEpoch: 1783036800, Probability: floatPtr(0.15), Source: "hourly_close", StaleAgeHours: intPtr(0)},
			{TeamID: "beta", Team: "Beta", StageKey: "winner", HourUTC: "2026-07-03T01:00:00Z", HourEpoch: 1783040400, Probability: floatPtr(0.20), Source: "hourly_close", StaleAgeHours: intPtr(0)},
			{TeamID: "beta", Team: "Beta", StageKey: "winner", HourUTC: "2026-07-03T02:00:00Z", HourEpoch: 1783044000, Probability: floatPtr(0.22), Source: "hourly_close", StaleAgeHours: intPtr(0)},
		},
		BracketSlots: []BracketSlot{
			{SlotID: "final-1", StageKey: "final", SlotIndex: 1, Label: "Final 1", TeamIDs: []string{"alpha", "beta"}},
		},
		AssetIDs: []string{
			"asset-alpha-win", "asset-alpha-win-no",
			"asset-alpha-final", "asset-alpha-final-no",
			"asset-beta-win", "asset-beta-win-no",
			"asset-beta-final", "asset-beta-final-no",
		},
	}
}

func floatPtr(value float64) *float64 {
	return &value
}

func intPtr(value int) *int {
	return &value
}

func mapQuery(raw string) map[string][]string {
	req := httptest.NewRequest(http.MethodGet, "/?"+raw, nil)
	return req.URL.Query()
}
