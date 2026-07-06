package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	graphArtifactName    = "graph_snapshot.json"
	knockoutArtifactName = "knockout_artifacts.json"
)

type GraphArtifact struct {
	Version        string           `json:"version"`
	BuiltAt        string           `json:"built_at"`
	SourceManifest string           `json:"source_manifest"`
	Counts         map[string]int   `json:"counts"`
	Nodes          []GraphNode      `json:"nodes"`
	LogicEdges     []GraphEdge      `json:"logic_edges"`
	Conditionals   []ConditionalRow `json:"conditionals"`
	Violations     []Violation      `json:"violations"`
}

type ArtifactStore struct {
	mu                   sync.RWMutex
	hub                  *Hub
	sports               *SportsState
	resultsOverride      string
	artifactDir          string
	graphPath            string
	knockoutPath         string
	explicitKnockoutPath bool
	graph                *GraphArtifact
	knockout             *KnockoutService
	warnings             []string
	lastGraphModTime     time.Time
	lastKnockoutModTime  time.Time
}

func NewArtifactStore(hub *Hub, sports *SportsState, artifactDir, graphPath, knockoutPath, resultsOverride string) *ArtifactStore {
	return &ArtifactStore{
		hub:                  hub,
		sports:               sports,
		resultsOverride:      resultsOverride,
		artifactDir:          artifactDir,
		graphPath:            graphPath,
		knockoutPath:         knockoutPath,
		explicitKnockoutPath: knockoutPath != "",
	}
}

func (s *ArtifactStore) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.Reload()
		}
	}
}

func (s *ArtifactStore) Reload() error {
	var warnings []string
	graph, graphMod, graphWarning, err := loadGraphMaybe(s.resolvedGraphPath())
	if err != nil {
		return err
	}
	if graphWarning != "" {
		warnings = append(warnings, graphWarning)
	}

	knockout, knockoutMod, knockoutWarning, err := loadKnockoutMaybe(s.resolvedKnockoutPath(), s.explicitKnockoutPath)
	if err != nil {
		return err
	}
	if knockoutWarning != "" {
		warnings = append(warnings, knockoutWarning)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.graph = graph
	s.lastGraphModTime = graphMod
	if knockout != nil {
		s.knockout = NewKnockoutService(*knockout, s.hub, s.sports, s.resultsOverride)
		s.hub.AddSubscriptions(knockout.AssetIDs)
		s.lastKnockoutModTime = knockoutMod
	} else if s.resolvedKnockoutPath() != "" {
		s.knockout = nil
		s.lastKnockoutModTime = time.Time{}
	}
	s.warnings = warnings
	return nil
}

func (s *ArtifactStore) GraphSnapshot(base GraphSnapshot) GraphSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	base.Warnings = append(base.Warnings, s.warnings...)
	if s.graph == nil {
		return base
	}
	base.Metadata = GraphMetadata{
		Version:        s.graph.Version,
		BuiltAt:        s.graph.BuiltAt,
		SourceManifest: s.graph.SourceManifest,
		Counts:         s.graph.Counts,
	}
	base.Nodes = append(make([]GraphNode, 0, len(s.graph.Nodes)), s.graph.Nodes...)
	base.Edges = append(make([]GraphEdge, 0, len(s.graph.LogicEdges)), s.graph.LogicEdges...)
	base.Conditionals = append(make([]ConditionalRow, 0, len(s.graph.Conditionals)), s.graph.Conditionals...)
	base.Violations = normalizeViolations(s.graph.Violations)
	return base
}

func (s *ArtifactStore) Knockout() *KnockoutService {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.knockout
}

func (s *ArtifactStore) resolvedGraphPath() string {
	if s.graphPath != "" {
		return s.graphPath
	}
	if s.artifactDir == "" {
		return ""
	}
	return filepath.Join(s.artifactDir, "current", graphArtifactName)
}

func (s *ArtifactStore) resolvedKnockoutPath() string {
	if s.knockoutPath != "" {
		return s.knockoutPath
	}
	if s.artifactDir == "" {
		return ""
	}
	return filepath.Join(s.artifactDir, "current", knockoutArtifactName)
}

func loadGraphMaybe(path string) (*GraphArtifact, time.Time, string, error) {
	if path == "" {
		return nil, time.Time{}, "", nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, time.Time{}, "graph artifact not found: " + path, nil
		}
		return nil, time.Time{}, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, "", err
	}
	var artifact GraphArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, time.Time{}, "", err
	}
	return &artifact, info.ModTime(), "", nil
}

func loadKnockoutMaybe(path string, required bool) (*KnockoutArtifact, time.Time, string, error) {
	if path == "" {
		return nil, time.Time{}, "", nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !required {
			return nil, time.Time{}, "knockout artifact not found: " + path, nil
		}
		return nil, time.Time{}, "", err
	}
	artifact, err := LoadKnockoutArtifact(path)
	if err != nil {
		return nil, time.Time{}, "", err
	}
	return artifact, info.ModTime(), "", nil
}

func normalizeViolations(values []Violation) []Violation {
	out := append(make([]Violation, 0, len(values)), values...)
	for i := range out {
		if out[i].Message == "" {
			out[i].Message = out[i].Description
		}
	}
	return out
}
