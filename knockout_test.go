package main

import (
	"encoding/json"
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
	if artifact.Competition != "wc2026" || len(artifact.AssetIDs) != 4 {
		t.Fatalf("unexpected artifact: %+v", artifact)
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
			{TeamID: "alpha", Team: "Alpha", StageKey: "winner", AssetID: "asset-alpha-win", BaselineProbability: &alphaWin},
			{TeamID: "alpha", Team: "Alpha", StageKey: "final", AssetID: "asset-alpha-final", BaselineProbability: &alphaFinal},
			{TeamID: "beta", Team: "Beta", StageKey: "winner", AssetID: "asset-beta-win", BaselineProbability: &betaWin},
			{TeamID: "beta", Team: "Beta", StageKey: "final", AssetID: "asset-beta-final", BaselineProbability: &betaFinal},
		},
		BracketSlots: []BracketSlot{
			{SlotID: "final-1", StageKey: "final", SlotIndex: 1, Label: "Final 1", TeamIDs: []string{"alpha", "beta"}},
		},
		AssetIDs: []string{"asset-alpha-win", "asset-alpha-final", "asset-beta-win", "asset-beta-final"},
	}
}
