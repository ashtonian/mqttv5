// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"
)

// The README's option tables are the configuration reference: every
// exported option constructor must appear in one.
func TestREADMEDocumentsEveryOption(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	optionTypes := map[string]bool{"Option": true, "SubscribeOption": true, "QueueOption": true, "ClientGroupOption": true}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || !fn.Name.IsExported() || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 {
				continue
			}
			if id, ok := fn.Type.Results.List[0].Type.(*ast.Ident); ok && optionTypes[id.Name] {
				if !strings.Contains(string(readme), "`"+fn.Name.Name+"(") {
					t.Errorf("README.md documents no %s", fn.Name.Name)
				}
			}
		}
	}
}

// Every citation of the MQTT v5.0 standard in the README links to it:
// each §N is written as the reference [§N], and each reference has a
// definition.
func TestREADMELinksEveryCitation(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\[(§[\d.]+)\]: `).FindAllStringSubmatch(string(readme), -1) {
		defined[m[1]] = true
	}
	section := regexp.MustCompile(`§\d+(\.\d+)*`)
	code := regexp.MustCompile("`[^`]*`")
	inFence := false
	for n, line := range strings.Split(string(readme), "\n") {
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
		}
		if inFence || strings.HasPrefix(line, "[§") && strings.Contains(line, "]: ") {
			continue
		}
		line = code.ReplaceAllString(line, "")
		for _, loc := range section.FindAllStringIndex(line, -1) {
			cite := line[loc[0]:loc[1]]
			linked := loc[0] > 0 && line[loc[0]-1] == '[' && loc[1] < len(line) && line[loc[1]] == ']'
			switch {
			case !linked:
				t.Errorf("README.md:%d: %s is not written as the link [%s]", n+1, cite, cite)
			case !defined[cite]:
				t.Errorf("README.md:%d: [%s] has no definition", n+1, cite)
			}
		}
	}
}
