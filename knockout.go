package main

import (
	"encoding/json"
	"math"
	"net/url"
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
	TeamStageHourly          []TeamStageHourly        `json:"team_stage_probabilities_hourly"`
	ConditionalHourly        []ConditionalHourly      `json:"conditional_probabilities_hourly"`
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
	ProgressionAssetID  string   `json:"progression_asset_id"`
	OppositeAssetID     string   `json:"opposite_asset_id"`
	YesAssetID          string   `json:"yes_asset_id"`
	NoAssetID           string   `json:"no_asset_id"`
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

type TeamStageHourly struct {
	TeamID        string   `json:"team_id"`
	Team          string   `json:"team"`
	StageKey      string   `json:"stage_key"`
	HourUTC       string   `json:"hour_utc"`
	HourEpoch     int64    `json:"hour_epoch"`
	Probability   *float64 `json:"probability"`
	Source        string   `json:"source"`
	StaleAgeHours *int     `json:"stale_age_hours"`
}

type ConditionalHourly struct {
	TeamID        string   `json:"team_id"`
	Team          string   `json:"team"`
	FromStage     string   `json:"from_stage"`
	ToStage       string   `json:"to_stage"`
	HourUTC       string   `json:"hour_utc"`
	HourEpoch     int64    `json:"hour_epoch"`
	Probability   *float64 `json:"probability"`
	Method        string   `json:"method"`
	StaleAgeHours *int     `json:"stale_age_hours"`
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

type KnockoutTimeseries struct {
	Version       string             `json:"version"`
	UpdatedAt     time.Time          `json:"updated_at"`
	Competition   string             `json:"competition"`
	StageKey      string             `json:"stage_key"`
	Metric        string             `json:"metric"`
	FromStage     string             `json:"from_stage,omitempty"`
	Hours         []string           `json:"hours"`
	Series        []TimeseriesSeries `json:"series"`
	ResultMarkers []ResultMarker     `json:"result_markers"`
	Sources       map[string]string  `json:"sources"`
	Warnings      []string           `json:"warnings"`
}

type TimeseriesSeries struct {
	TeamID string            `json:"team_id"`
	Team   string            `json:"team"`
	Points []TimeseriesPoint `json:"points"`
}

type TimeseriesPoint struct {
	HourUTC       string   `json:"hour_utc"`
	Probability   *float64 `json:"probability"`
	Source        string   `json:"source"`
	StaleAgeHours *int     `json:"stale_age_hours"`
	ResultForced  bool     `json:"result_forced"`
}

type ResultMarker struct {
	HourUTC      string `json:"hour_utc"`
	StageKey     string `json:"stage_key"`
	WinnerTeamID string `json:"winner_team_id,omitempty"`
	Status       string `json:"status,omitempty"`
	Score        string `json:"score,omitempty"`
	Source       string `json:"source,omitempty"`
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

func (s *KnockoutService) Timeseries(query url.Values) KnockoutTimeseries {
	warnings := s.loadOverrides()
	stage := query.Get("stage")
	if stage == "" {
		stage = "winner"
	}
	if !hasStage(s.artifact.Stages, stage) {
		warnings = append(warnings, "unknown stage "+stage+", using winner")
		stage = "winner"
	}
	metric := query.Get("metric")
	if metric == "" {
		metric = "stage_probability"
	}
	if metric != "stage_probability" && metric != "conditional_probability" {
		warnings = append(warnings, "unknown metric "+metric+", using stage_probability")
		metric = "stage_probability"
	}
	fromStage := query.Get("from_stage")
	if metric == "conditional_probability" && fromStage == "" {
		fromStage = previousStage(s.artifact.Stages, stage)
	}
	limit := parseLimit(query.Get("limit_teams"), 12)
	pinned := splitQueryIDs(query.Get("team_id"))
	results := s.results()
	points := s.timeseriesPoints(stage, metric, fromStage)
	points = s.overlayCurrentHour(points, stage, metric, fromStage)
	points = applyResultForcing(points, stage, metric, fromStage, s.resultForces(results))
	series := selectTimeseries(points, s.artifact.Teams, pinned, limit)
	return KnockoutTimeseries{
		Version:       apiVersion,
		UpdatedAt:     time.Now().UTC(),
		Competition:   s.artifact.Competition,
		StageKey:      stage,
		Metric:        metric,
		FromStage:     fromStage,
		Hours:         seriesHours(series),
		Series:        series,
		ResultMarkers: s.resultMarkers(results),
		Sources: map[string]string{
			"artifact_built_at": s.artifact.BuiltAt,
			"artifact_manifest": s.artifact.SourceManifest,
		},
		Warnings: dedupe(warnings),
	}
}

func (s *KnockoutService) timeseriesPoints(stage, metric, fromStage string) map[string][]TimeseriesPoint {
	out := map[string][]TimeseriesPoint{}
	if metric == "conditional_probability" {
		for _, row := range s.artifact.ConditionalHourly {
			if row.ToStage != stage || row.FromStage != fromStage {
				continue
			}
			out[row.TeamID] = append(out[row.TeamID], TimeseriesPoint{
				HourUTC:       row.HourUTC,
				Probability:   cloneFloat(row.Probability),
				Source:        firstNonEmpty(row.Method, "market_ratio"),
				StaleAgeHours: cloneInt(row.StaleAgeHours),
			})
		}
		return sortPointMap(out)
	}
	for _, row := range s.artifact.TeamStageHourly {
		if row.StageKey != stage {
			continue
		}
		out[row.TeamID] = append(out[row.TeamID], TimeseriesPoint{
			HourUTC:       row.HourUTC,
			Probability:   cloneFloat(row.Probability),
			Source:        row.Source,
			StaleAgeHours: cloneInt(row.StaleAgeHours),
		})
	}
	return sortPointMap(out)
}

func (s *KnockoutService) overlayCurrentHour(points map[string][]TimeseriesPoint, stage, metric, fromStage string) map[string][]TimeseriesPoint {
	hour := time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)
	if metric == "conditional_probability" {
		for _, team := range s.artifact.Teams {
			toProb, toSource := s.currentStageProbability(team.TeamID, stage)
			fromProb, fromSource := s.currentStageProbability(team.TeamID, fromStage)
			probability := ratioPtr(toProb, fromProb)
			if probability == nil || (!isLiveSource(toSource) && !isLiveSource(fromSource)) {
				continue
			}
			points[team.TeamID] = upsertPoint(points[team.TeamID], TimeseriesPoint{
				HourUTC:      hour,
				Probability:  probability,
				Source:       "live_ratio",
				ResultForced: false,
			})
		}
		return sortPointMap(points)
	}
	for _, market := range s.artifact.TeamStageMarkets {
		if market.StageKey != stage {
			continue
		}
		probability, source, _ := s.marketProbability(market, s.hub.AssetStates())
		if probability == nil || !isLiveSource(source) {
			continue
		}
		points[market.TeamID] = upsertPoint(points[market.TeamID], TimeseriesPoint{
			HourUTC:      hour,
			Probability:  cloneFloat(probability),
			Source:       source,
			ResultForced: false,
		})
	}
	return sortPointMap(points)
}

func (s *KnockoutService) currentStageProbability(teamID, stage string) (*float64, string) {
	if stage == "" {
		return nil, "missing"
	}
	assets := s.hub.AssetStates()
	for _, market := range s.artifact.TeamStageMarkets {
		if market.TeamID == teamID && market.StageKey == stage {
			probability, source, _ := s.marketProbability(market, assets)
			return probability, source
		}
	}
	return nil, "missing"
}

func (s *KnockoutService) marketProbability(market TeamStageMarket, assets map[string]AssetState) (*float64, string, string) {
	progressID := firstNonEmpty(market.ProgressionAssetID, market.AssetID, market.YesAssetID)
	oppositeID := firstNonEmpty(market.OppositeAssetID, market.NoAssetID)
	progress, ok := assets[progressID]
	if ok {
		if value, updatedAt, ok := liveDevigMidpoint(progress, assets[oppositeID]); ok {
			return &value, "live_devig_midpoint", updatedAt
		}
		if value, ok := midpoint(progress.BestBid, progress.BestAsk); ok {
			return &value, "live_midpoint", progress.UpdatedAt.Format(time.RFC3339)
		}
		if value, updatedAt, ok := liveDevigLastTrade(progress, assets[oppositeID]); ok {
			return &value, "live_devig_last_trade", updatedAt
		}
		if value, ok := parseProbability(progress.LastPrice); ok {
			return &value, "last_trade_price", progress.UpdatedAt.Format(time.RFC3339)
		}
	}
	if market.BaselineProbability != nil {
		value := clamp(*market.BaselineProbability)
		return &value, "artifact_" + market.ProbabilitySource, ""
	}
	return nil, "missing", ""
}

func liveDevigMidpoint(yes AssetState, no AssetState) (float64, string, bool) {
	yesMid, okYes := midpoint(yes.BestBid, yes.BestAsk)
	noMid, okNo := midpoint(no.BestBid, no.BestAsk)
	if !okYes || !okNo || yesMid+noMid <= 0 {
		return 0, "", false
	}
	return clamp(yesMid / (yesMid + noMid)), latestTimestamp(yes.UpdatedAt, no.UpdatedAt), true
}

func liveDevigLastTrade(yes AssetState, no AssetState) (float64, string, bool) {
	yesPrice, okYes := parseProbability(yes.LastPrice)
	noPrice, okNo := parseProbability(no.LastPrice)
	if !okYes || !okNo || yesPrice+noPrice <= 0 {
		return 0, "", false
	}
	return clamp(yesPrice / (yesPrice + noPrice)), latestTimestamp(yes.UpdatedAt, no.UpdatedAt), true
}

func latestTimestamp(a, b time.Time) string {
	if b.After(a) {
		return b.Format(time.RFC3339)
	}
	return a.Format(time.RFC3339)
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

type resultForce struct {
	TeamID  string
	Stage   string
	HourUTC string
	Value   float64
}

func (s *KnockoutService) resultForces(results []MatchResult) []resultForce {
	var out []resultForce
	for _, result := range results {
		if result.WinnerTeamID == "" || len(result.TeamIDs) == 0 {
			continue
		}
		current := slotStage(s.artifact.BracketSlots, result)
		if current == "" {
			continue
		}
		hour := resultHour(result)
		next := nextStage(s.artifact.Stages, current)
		nextRank := stageRank(s.artifact.Stages, next)
		for _, teamID := range result.TeamIDs {
			out = append(out, resultForce{TeamID: teamID, Stage: current, HourUTC: hour, Value: 1})
			if next == "" {
				continue
			}
			if teamID == result.WinnerTeamID {
				out = append(out, resultForce{TeamID: teamID, Stage: next, HourUTC: hour, Value: 1})
				continue
			}
			out = append(out, resultForce{TeamID: teamID, Stage: next, HourUTC: hour, Value: 0})
			for _, stage := range s.artifact.Stages {
				if stage.Rank > nextRank {
					out = append(out, resultForce{TeamID: teamID, Stage: stage.Key, HourUTC: hour, Value: 0})
				}
			}
		}
	}
	return out
}

func (s *KnockoutService) resultMarkers(results []MatchResult) []ResultMarker {
	markers := make([]ResultMarker, 0, len(results))
	for _, result := range results {
		stage := slotStage(s.artifact.BracketSlots, result)
		if stage == "" {
			continue
		}
		markers = append(markers, ResultMarker{
			HourUTC:      resultHour(result),
			StageKey:     stage,
			WinnerTeamID: result.WinnerTeamID,
			Status:       result.Status,
			Score:        result.Score,
			Source:       result.Source,
		})
	}
	sort.Slice(markers, func(i, j int) bool { return markers[i].HourUTC < markers[j].HourUTC })
	return markers
}

func applyResultForcing(points map[string][]TimeseriesPoint, stage, metric, fromStage string, forces []resultForce) map[string][]TimeseriesPoint {
	for _, force := range forces {
		if force.Stage != stage && !(metric == "conditional_probability" && force.Stage == fromStage) {
			continue
		}
		rows := points[force.TeamID]
		for i := range rows {
			if rows[i].HourUTC < force.HourUTC {
				continue
			}
			value := force.Value
			if metric == "conditional_probability" && force.Stage == fromStage {
				if force.Value == 0 {
					rows[i].Probability = nil
					rows[i].Source = "result"
					rows[i].ResultForced = true
				}
				continue
			}
			rows[i].Probability = &value
			rows[i].Source = "result"
			rows[i].ResultForced = true
		}
		points[force.TeamID] = rows
	}
	return points
}

func selectTimeseries(points map[string][]TimeseriesPoint, teams []KnockoutTeam, pinned []string, limit int) []TimeseriesSeries {
	names := map[string]string{}
	for _, team := range teams {
		names[team.TeamID] = team.Name
	}
	ids := pinned
	if len(ids) == 0 {
		ids = topTeamIDs(points, limit)
	}
	out := make([]TimeseriesSeries, 0, len(ids))
	for _, teamID := range ids {
		rows := points[teamID]
		if len(rows) == 0 {
			continue
		}
		out = append(out, TimeseriesSeries{TeamID: teamID, Team: names[teamID], Points: rows})
	}
	return out
}

func topTeamIDs(points map[string][]TimeseriesPoint, limit int) []string {
	type rank struct {
		teamID string
		value  float64
	}
	ranks := []rank{}
	for teamID, rows := range points {
		value := -1.0
		for i := len(rows) - 1; i >= 0; i-- {
			if rows[i].Probability != nil {
				value = *rows[i].Probability
				break
			}
		}
		ranks = append(ranks, rank{teamID: teamID, value: value})
	}
	sort.Slice(ranks, func(i, j int) bool {
		if ranks[i].value == ranks[j].value {
			return ranks[i].teamID < ranks[j].teamID
		}
		return ranks[i].value > ranks[j].value
	})
	if limit > len(ranks) {
		limit = len(ranks)
	}
	out := make([]string, 0, limit)
	for _, row := range ranks[:limit] {
		out = append(out, row.teamID)
	}
	return out
}

func seriesHours(series []TimeseriesSeries) []string {
	seen := map[string]struct{}{}
	for _, item := range series {
		for _, point := range item.Points {
			seen[point.HourUTC] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for hour := range seen {
		out = append(out, hour)
	}
	sort.Strings(out)
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

func previousStage(stages []KnockoutStage, key string) string {
	rank := stageRank(stages, key)
	prev := ""
	prevRank := -1
	for _, stage := range stages {
		if stage.Rank < rank && stage.Rank > prevRank {
			prev = stage.Key
			prevRank = stage.Rank
		}
	}
	return prev
}

func hasStage(stages []KnockoutStage, key string) bool {
	return stageRank(stages, key) != 999
}

func resultKey(result MatchResult) string {
	if result.SlotID != "" {
		return "slot:" + result.SlotID
	}
	return "sports:" + result.SportsSlug
}

func resultHour(result MatchResult) string {
	if result.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, result.UpdatedAt); err == nil {
			return parsed.UTC().Truncate(time.Hour).Format(time.RFC3339)
		}
	}
	return time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)
}

func parseLimit(value string, fallback int) int {
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > 48 {
		return 48
	}
	return limit
}

func splitQueryIDs(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func sortPointMap(points map[string][]TimeseriesPoint) map[string][]TimeseriesPoint {
	for teamID := range points {
		sort.Slice(points[teamID], func(i, j int) bool {
			return points[teamID][i].HourUTC < points[teamID][j].HourUTC
		})
	}
	return points
}

func upsertPoint(points []TimeseriesPoint, point TimeseriesPoint) []TimeseriesPoint {
	for i := range points {
		if points[i].HourUTC == point.HourUTC {
			points[i] = point
			return points
		}
	}
	return append(points, point)
}

func isLiveSource(source string) bool {
	return strings.HasPrefix(source, "live_") || source == "last_trade_price"
}

func ratioPtr(later, base *float64) *float64 {
	if later == nil || base == nil || *base <= 0 {
		return nil
	}
	value := clamp(*later / *base)
	return &value
}

func cloneFloat(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
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
