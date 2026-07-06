package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestAPIDocsCoverRoutesAndSSEEvents(t *testing.T) {
	docs := readTestFile(t, "docs/api.md")
	server := readTestFile(t, "server.go")

	for _, route := range regexp.MustCompile(`mux\.HandleFunc\("([^"]+)"`).FindAllStringSubmatch(server, -1) {
		if !strings.Contains(docs, "`"+route[1]+"`") {
			t.Fatalf("docs/api.md missing route %s", route[1])
		}
	}

	for _, event := range regexp.MustCompile(`writeSSE\(w, "([^"]+)"`).FindAllStringSubmatch(server, -1) {
		if !strings.Contains(docs, "`"+event[1]+"`") {
			t.Fatalf("docs/api.md missing SSE event %s", event[1])
		}
	}

	if strings.Contains(server, `: ping\n\n`) && !strings.Contains(docs, "`: ping`") {
		t.Fatal("docs/api.md missing SSE ping heartbeat")
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
