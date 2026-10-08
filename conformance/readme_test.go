// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package conformance

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

const (
	testsBegin = "<!-- tests:begin -->\n"
	testsEnd   = "<!-- tests:end -->"
)

// TestREADMEListsEveryTest keeps the README's list of tests generated
// from the suite: one row per Test function with the first sentence of
// its doc comment. It runs without the conformance tag or a broker.
func TestREADMEListsEveryTest(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	want, err := testTable()
	if err != nil {
		t.Fatal(err)
	}
	s := string(readme)
	i, j := strings.Index(s, testsBegin), strings.Index(s, testsEnd)
	if i < 0 || j < i {
		t.Fatalf("README.md has no %q … %q block", strings.TrimSpace(testsBegin), testsEnd)
	}
	if got := s[i+len(testsBegin) : j]; got != want {
		t.Errorf("README.md's test list is stale; replace the block with:\n%s%s%s", testsBegin, want, testsEnd)
	}
}

// testTable renders the Markdown table of every Test function in this
// directory, by file and then source order.
func testTable() (string, error) {
	entries, err := os.ReadDir(".")
	if err != nil {
		return "", err
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "_test.go") && e.Name() != "readme_test.go" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	var b strings.Builder
	b.WriteString("| Test | File | Checks |\n|---|---|---|\n")
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			return "", err
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			if fn.Doc == nil {
				return "", fmt.Errorf("%s: %s has no doc comment", name, fn.Name.Name)
			}
			fmt.Fprintf(&b, "| `%s` | `%s` | %s |\n", fn.Name.Name, name, firstSentence(fn.Name.Name, fn.Doc.Text()))
		}
	}
	return b.String(), nil
}

// firstSentence returns doc's first sentence on one line, dropping a
// leading reference to the test itself ("TestX verifies …").
func firstSentence(name, doc string) string {
	s := strings.Join(strings.Fields(doc), " ")
	if rest, ok := strings.CutPrefix(s, name+" "); ok && rest != "" {
		s = strings.ToUpper(rest[:1]) + rest[1:]
	}
	if i := strings.Index(s, ". "); i >= 0 {
		s = s[:i+1]
	}
	return strings.ReplaceAll(s, "|", `\|`)
}
