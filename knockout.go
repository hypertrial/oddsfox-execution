package main

import (
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KnockoutArtifact struct {
	Competition              string                   `json:"competition"`
	BuiltAt                  string                   `json:"built_at"`
	SourceManifest           string                   `json:"source_manifest"`
	Stages                   []KnockoutStage          `json:"stages"`
	Teams                    []KnockoutTeam           `json:"teams"`
	TeamStageMarkets         []TeamStageMarket        `json:"team_stage_markets"`
	ConditionalProbabilities []ConditionalProbability `json:"conditional_probabilities"`
	BracketSlots             []BracketSlot            `json:"bracket_slots"`
	AssetIDs                 []string                 `json:"asset_ids"`
}

type KnockoutStage struct {
	Key       string `json:"key"`
	Rank      int    `json:"rank"`
	Label     string `json:"label"`
	SlotCount int    `json:"slot_count"`
}

type KnockoutTeam struct {
	TeamID string `json:"team_id"`
	Name   string `json:"name"`
}

type TeamStageMarket struct {
	TeamID              string   `json:"team_id"`
	Team                string   `json:"team"`
	StageKey            string   `json:"stage_key"`
	StageRank           int      `json:"stage_rank"`
	NodeID              string   `json:"node_id"`
	AssetID             string   `json:"asset_id"`
	MarketID            string   `json:"market_id"`
	Question            string   `json:"question"`
	EventSlug           string   `json:"event_slug"`
	BaselineProbability *float64 `json:"baseline_probability"`
	ProbabilitySource   string   `json:"probability_source"`
	IsActive            bool     `json:"is_active"`
	IsClosed            bool     `json:"is_closed"`
}

type ConditionalProbability struct {
	TeamID      string   `json:"team_id"`
	FromStage   string   `json:"from_stage"`
	ToStage     string   `json:"to_stage"`
	Probability *float64 `json:"probability"`
	Method      string   `json:"method"`
}

type BracketSlot struct {
	SlotID     string   `json:"slot_id"`
	StageKey   string   `json:"stage_key"`
	SlotIndex  int      `json:"slot_index"`
	Label      string   `json:"label"`
	SportsSlug string   `json:"sports_slug"`
	TeamIDs    []string `json:"team_ids"`
}

type MatchResult struct {
	SlotID       string   `json:"slot_id,omitempty"`
	SportsSlug   string   `json:"sports_slug,omitempty"`
	Status       string   `json:"status,omitempty"`
	WinnerTeamID string   `json:"winner_team_id,omitempty"`
	TeamIDs      []string `json:"team_ids,omitempty"`
	Score        string   `json:"score,omitempty"`
	Source       string   `json:"source,omitempty"`
	UpdatedAt    string   `json:"updated_at,omitempty"`
}

type KnockoutSnapshot struct {
	Version           string            `json:"version"`
	UpdatedAt         time.Time         `json:"updated_at"`
	Competition       string            `json:"competition"`
	Stages            []KnockoutStage   `json:"stages"`
	Slots             []BracketSlot     `json:"slots"`
	Teams             []KnockoutTeam    `json:"teams"`
	TeamProbabilities []TeamProbability `json:"team_probabilities"`
	MatchResults      []MatchResult     `json:"match_results"`
	Sources           map[string]string `json:"sources"`
	Warnings          []string          `json:"warnings"`
}

type TeamProbability struct {
	TeamID      string   `json:"team_id"`
	Team        string   `json:"team"`
	StageKey    string   `json:"stage_key"`
	Probability *float64 `json:"probability"`
	Source      string   `json:"source"`
	AssetID     string   `json:"asset_id"`
	MarketID    string   `json:"market_id"`
	UpdatedAt   string   `json:"updated_at,omitempty"`
}

type ResultsOverride struct {
	Slots       map[string]MatchResult `json:"slots"`
	SportsSlugs map[string]MatchResult `json:"sports_slugs"`
}

type KnockoutService struct {
	artifact     KnockoutArtifact
	hub          *Hub
	sports       *SportsState
	overridePath string
	mu           sync.Mutex
	overrideMod  time.Time
	override     ResultsOverride
}

func LoadKnockoutArtifact(path string) (*KnockoutArtifact, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var artifact KnockoutArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, err
	}
	return &artifact, nil
}

func NewKnockoutService(artifact KnockoutArtifact, hub *Hub, sports *SportsState, overridePath string) *KnockoutService {
	return &KnockoutService{artifact: artifact, hub: hub, sports: sports, overridePath: overridePath}
}

func (s *KnockoutService) AssetIDs() []string {
	return append([]string(nil), s.artifact.AssetIDs...)
}

func (s *KnockoutService) Snapshot() KnockoutSnapshot {
	warnings := s.loadOverrides()
	assets := s.hub.AssetStates()
	results := s.results()
	resultByTeamStage := s.resultProbabilities(results)
	probs := make([]TeamProbability, 0, len(s.artifact.TeamStageMarkets))
	for _, market := range s.artifact.TeamStageMarkets {
		probability, source, updatedAt := s.marketProbability(market, assets)
		if override, ok := resultByTeamStage[market.TeamID+"|"+market.StageKey]; ok {
			probability = &override
			source = "result"
		}
		if probability == nil {
			warnings = append(warnings, "missing probability for "+market.TeamID+" "+market.StageKey)
		}
		probs = append(probs, TeamProbability{
			TeamID:      market.TeamID,
			Team:        market.Team,
			StageKey:    market.StageKey,
			Probability: probability,
			Source:      source,
			AssetID:     market.AssetID,
			MarketID:    market.MarketID,
			UpdatedAt:   updatedAt,
		})
	}
	sort.Slice(probs, func(i, j int) bool {
		if probs[i].StageKey == probs[j].StageKey {
			return probs[i].Team < probs[j].Team
		}
		return stageRank(s.artifact.Stages, probs[i].StageKey) < stageRank(s.artifact.Stages, probs[j].StageKey)
	})
	return KnockoutSnapshot{
		Version:           apiVersion,
		UpdatedAt:         time.Now().UTC(),
		Competition:       s.artifact.Competition,
		Stages:            s.artifact.Stages,
		Slots:             s.artifact.BracketSlots,
		Teams:             s.artifact.Teams,
		TeamProbabilities: probs,
		MatchResults:      results,
		Sources: map[string]string{
			"artifact_built_at": s.artifact.BuiltAt,
			"artifact_manifest": s.artifact.SourceManifest,
		},
		Warnings: dedupe(warnings),
	}
}

func (s *KnockoutService) marketProbability(market TeamStageMarket, assets map[string]AssetState) (*float64, string, string) {
	if asset, ok := assets[market.AssetID]; ok {
		if value, ok := midpoint(asset.BestBid, asset.BestAsk); ok {
			return &value, "live_midpoint", asset.UpdatedAt.Format(time.RFC3339)
		}
		if value, ok := parseProbability(asset.LastPrice); ok {
			return &value, "last_trade_price", asset.UpdatedAt.Format(time.RFC3339)
		}
	}
	if market.BaselineProbability != nil {
		value := clamp(*market.BaselineProbability)
		return &value, "artifact_" + market.ProbabilitySource, ""
	}
	return nil, "missing", ""
}

func (s *KnockoutService) loadOverrides() []string {
	if s.overridePath == "" {
		return nil
	}
	info, err := os.Stat(s.overridePath)
	if err != nil {
		return []string{"results override unavailable: " + err.Error()}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !info.ModTime().After(s.overrideMod) {
		return nil
	}
	data, err := os.ReadFile(s.overridePath)
	if err != nil {
		return []string{"results override unreadable: " + err.Error()}
	}
	var override ResultsOverride
	if err := json.Unmarshal(data, &override); err != nil {
		return []string{"results override invalid: " + err.Error()}
	}
	s.override = override
	s.overrideMod = info.ModTime()
	return nil
}

func (s *KnockoutService) results() []MatchResult {
	merged := map[string]MatchResult{}
	if s.sports != nil {
		for _, result := range s.sports.Results() {
			merged[resultKey(result)] = result
		}
	}
	s.mu.Lock()
	for key, result := range s.override.Slots {
		result.SlotID = firstNonEmpty(result.SlotID, key)
		result.Source = "override"
		merged["slot:"+result.SlotID] = result
	}
	for key, result := range s.override.SportsSlugs {
		result.SportsSlug = firstNonEmpty(result.SportsSlug, key)
		result.Source = "override"
		merged["sports:"+result.SportsSlug] = result
	}
	s.mu.Unlock()
	out := make([]MatchResult, 0, len(merged))
	for _, result := range merged {
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool { return resultKey(out[i]) < resultKey(out[j]) })
	return out
}

func (s *KnockoutService) resultProbabilities(results []MatchResult) map[string]float64 {
	out := map[string]float64{}
	for _, result := range results {
		if result.WinnerTeamID == "" || len(result.TeamIDs) == 0 {
			continue
		}
		current := slotStage(s.artifact.BracketSlots, result)
		if current == "" {
			continue
		}
		next := nextStage(s.artifact.Stages, current)
		nextRank := stageRank(s.artifact.Stages, next)
		for _, teamID := range result.TeamIDs {
			out[teamID+"|"+current] = 1.0
			if next == "" {
				continue
			}
			if teamID == result.WinnerTeamID {
				out[teamID+"|"+next] = 1.0
				continue
			}
			out[teamID+"|"+next] = 0.0
			for _, stage := range s.artifact.Stages {
				if stage.Rank > nextRank {
					out[teamID+"|"+stage.Key] = 0.0
				}
			}
		}
	}
	return out
}

func midpoint(bid, ask string) (float64, bool) {
	b, okB := parseProbability(bid)
	a, okA := parseProbability(ask)
	if !okB || !okA {
		return 0, false
	}
	return clamp((b + a) / 2), true
}

func parseProbability(value string) (float64, bool) {
	if value == "" {
		return 0, false
	}
	n, err := strconv.ParseFloat(normalizePrice(value), 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, false
	}
	return clamp(n), true
}

func clamp(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func stageRank(stages []KnockoutStage, key string) int {
	for _, stage := range stages {
		if stage.Key == key {
			return stage.Rank
		}
	}
	return 999
}

func slotStage(slots []BracketSlot, result MatchResult) string {
	for _, slot := range slots {
		if (result.SlotID != "" && slot.SlotID == result.SlotID) || (result.SportsSlug != "" && slot.SportsSlug == result.SportsSlug) {
			return slot.StageKey
		}
	}
	if result.SlotID != "" {
		for _, stage := range []string{"round_of_32", "round_of_16", "quarterfinal", "semifinal", "final", "winner"} {
			if strings.HasPrefix(result.SlotID, stage+"-") || result.SlotID == stage {
				return stage
			}
		}
	}
	return ""
}

func nextStage(stages []KnockoutStage, key string) string {
	rank := stageRank(stages, key)
	for _, stage := range stages {
		if stage.Rank == rank+1 {
			return stage.Key
		}
	}
	return ""
}

func resultKey(result MatchResult) string {
	if result.SlotID != "" {
		return "slot:" + result.SlotID
	}
	return "sports:" + result.SportsSlug
}

func dedupe(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
