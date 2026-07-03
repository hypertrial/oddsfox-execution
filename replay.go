package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type ReplayLog struct {
	dir string
	mu  sync.Mutex
}

func NewReplayLog(dir string) *ReplayLog {
	return &ReplayLog{dir: dir}
}

func (r *ReplayLog) Append(event LiveEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(r.dir, event.ReceivedAt.Format("2006-01-02")+".jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}

func ReadReplayEvents(dir string, limit int) []LiveEvent {
	if limit <= 0 {
		limit = 100
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && strings.HasSuffix(name, ".jsonl") {
			names = append(names, filepath.Join(dir, name))
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	out := make([]LiveEvent, 0, limit)
	for _, name := range names {
		file, err := os.Open(name)
		if err != nil {
			continue
		}
		var lines []string
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		file.Close()
		for i := len(lines) - 1; i >= 0 && len(out) < limit; i-- {
			var event LiveEvent
			if json.Unmarshal([]byte(lines[i]), &event) == nil && !event.ReceivedAt.IsZero() {
				out = append(out, event)
			}
		}
		if len(out) >= limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReceivedAt.Before(out[j].ReceivedAt)
	})
	return out
}
