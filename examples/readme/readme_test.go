// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package readme

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestREADMESnippets checks that every Go snippet in the repository
// README appears in a file of this package: each run of non-blank
// lines, in order, with whitespace runs collapsed. These files compile,
// so the README's code does too.
func TestREADMESnippets(t *testing.T) {
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	var sources []string
	err = filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			sources = append(sources, string(normalize(b)))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, snippet := range goSnippets(readme) {
		found := false
		for _, src := range sources {
			if containsChunks(src, chunks(snippet)) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("README Go snippet %d is in no file of examples/readme:\n%s", i+1, snippet)
		}
	}
}

func goSnippets(readme []byte) []string {
	var out []string
	var cur []string
	in := false
	s := bufio.NewScanner(bytes.NewReader(readme))
	for s.Scan() {
		line := s.Text()
		switch {
		case !in && strings.TrimSpace(line) == "```go":
			in, cur = true, nil
		case in && strings.TrimSpace(line) == "```":
			in = false
			out = append(out, strings.Join(cur, "\n"))
		case in:
			cur = append(cur, line)
		}
	}
	return out
}

// chunks splits a snippet at blank lines.
func chunks(snippet string) []string {
	var out []string
	for _, c := range strings.Split(string(normalize([]byte(snippet))), "\n\n") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}

// normalize collapses whitespace runs within lines and keeps blank lines
// as separators.
func normalize(b []byte) []byte {
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		out = append(out, strings.Join(strings.Fields(line), " "))
	}
	return []byte(strings.Join(out, "\n"))
}

// containsChunks reports whether src holds every chunk, each as whole
// lines, in order.
func containsChunks(src string, cs []string) bool {
	rest := "\n" + src + "\n"
	for _, c := range cs {
		i := strings.Index(rest, "\n"+c+"\n")
		if i < 0 {
			return false
		}
		rest = rest[i+len(c)+1:]
	}
	return true
}
