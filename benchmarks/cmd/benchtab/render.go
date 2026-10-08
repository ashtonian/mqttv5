// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchmath"
	"golang.org/x/perf/benchproc"
	"golang.org/x/perf/benchunit"
)

// confidence is the level of the intervals shown in every cell.
const confidence = 0.95

var (
	blockRE = regexp.MustCompile(`(?s)(<!-- benchtab ([^\n]*?)-->\n)(.*?)(<!-- /benchtab -->)`)
	attrRE  = regexp.MustCompile(`(\w+)=(?:"([^"]*)"|(\S+))`)
)

// fenceRE matches a fenced code block, where a benchtab marker is an
// example, not a table.
var fenceRE = regexp.MustCompile("(?ms)^```.*?^```[^\n]*$")

// Render returns src with every benchtab table regenerated, leaving
// fenced code blocks alone. Raw result files are resolved relative to
// mdPath's directory.
func Render(src []byte, mdPath string) ([]byte, error) {
	var out []byte
	var errs []error
	last := 0
	for _, f := range fenceRE.FindAllIndex(src, -1) {
		rendered, err := renderTables(src[last:f[0]], mdPath)
		out = append(append(out, rendered...), src[f[0]:f[1]]...)
		errs = append(errs, err)
		last = f[1]
	}
	rendered, err := renderTables(src[last:], mdPath)
	out = append(out, rendered...)
	return out, errors.Join(append(errs, err)...)
}

func renderTables(src []byte, mdPath string) ([]byte, error) {
	var errs []error
	out := blockRE.ReplaceAllFunc(src, func(block []byte) []byte {
		m := blockRE.FindSubmatch(block)
		s, err := parseSpec(string(m[2]))
		if err == nil {
			var table string
			table, err = s.render(filepath.Dir(mdPath))
			if err == nil {
				return bytes.Join([][]byte{m[1], []byte(table), m[4]}, nil)
			}
		}
		errs = append(errs, fmt.Errorf("table %q: %w", strings.TrimSpace(string(m[2])), err))
		return block
	})
	return out, errors.Join(errs...)
}

// spec is one table's declaration.
type spec struct {
	file, filter, unit, col string
	rows                    []string
	compare                 [][2]string
	// meta lists configuration keys of the raw file to show instead of
	// results.
	meta []string
}

func parseSpec(attrs string) (spec, error) {
	var s spec
	for _, m := range attrRE.FindAllStringSubmatch(attrs, -1) {
		v := m[2] + m[3]
		switch m[1] {
		case "file":
			s.file = v
		case "filter":
			s.filter = v
		case "rows":
			s.rows = strings.Split(v, ",")
		case "cols":
			s.col = v
		case "unit":
			s.unit = v
		case "meta":
			s.meta = strings.Split(v, ",")
		case "compare":
			for _, pair := range strings.Split(v, ",") {
				a, b, ok := strings.Cut(pair, ":")
				if !ok {
					return s, fmt.Errorf("compare %q: want a:b", pair)
				}
				s.compare = append(s.compare, [2]string{a, b})
			}
		default:
			return s, fmt.Errorf("unknown attribute %q", m[1])
		}
	}
	switch {
	case s.file == "":
		return s, errors.New("missing file")
	case len(s.meta) > 0:
		return s, nil
	case len(s.rows) == 0:
		return s, errors.New("missing rows")
	case s.col == "":
		return s, errors.New("missing cols")
	case s.unit == "":
		return s, errors.New("missing unit")
	}
	if s.filter == "" {
		s.filter = "*"
	}
	return s, nil
}

// grid holds the samples of one table, rows and columns in the order
// they first appear in the raw file.
type grid struct {
	rows, cols []string
	rowLabels  map[string][]string
	cells      map[string]map[string][]float64
}

func (s spec) load(dir string) (*grid, error) {
	filter, err := benchproc.NewFilter(s.filter)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}
	f, err := os.Open(filepath.Join(dir, s.file))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	g := &grid{rowLabels: map[string][]string{}, cells: map[string]map[string][]float64{}}
	seenCol := map[string]bool{}
	r := benchfmt.NewReader(f, s.file)
	for r.Scan() {
		res, ok := r.Result().(*benchfmt.Result)
		if !ok {
			continue
		}
		if match, err := filter.Apply(res); err != nil {
			return nil, err
		} else if !match {
			continue
		}
		v, ok := res.Value(s.unit)
		if !ok {
			continue
		}
		keys := nameKeys(res.Name)
		label := make([]string, len(s.rows))
		for i, k := range s.rows {
			if label[i], ok = keys[k]; !ok {
				return nil, fmt.Errorf("%s has no key %q", res.Name, k)
			}
		}
		col, ok := keys[s.col]
		if !ok {
			return nil, fmt.Errorf("%s has no key %q", res.Name, s.col)
		}
		row := strings.Join(label, "\x00")
		if g.cells[row] == nil {
			g.rows = append(g.rows, row)
			g.rowLabels[row] = label
			g.cells[row] = map[string][]float64{}
		}
		if !seenCol[col] {
			seenCol[col] = true
			g.cols = append(g.cols, col)
		}
		g.cells[row][col] = append(g.cells[row][col], v)
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	if len(g.rows) == 0 {
		return nil, fmt.Errorf("no %s results match", s.unit)
	}
	return g, nil
}

// nameKeys returns a benchmark's key=value sub-name parts.
func nameKeys(name benchfmt.Name) map[string]string {
	_, parts := name.Parts()
	keys := map[string]string{}
	for _, p := range parts {
		if len(p) == 0 || p[0] != '/' {
			continue // the -GOMAXPROCS suffix
		}
		if k, v, ok := strings.Cut(string(p[1:]), "="); ok {
			keys[k] = v
		}
	}
	return keys
}

// config returns the raw file's configuration lines, the first value of
// each key.
func (s spec) config(dir string) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join(dir, s.file))
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if m := configRE.FindStringSubmatch(line); m != nil {
			if _, seen := values[m[1]]; !seen {
				values[m[1]] = m[2]
			}
		}
	}
	return values, nil
}

// renderMeta shows the raw file's configuration lines for s.meta.
func (s spec) renderMeta(dir string) (string, error) {
	values, err := s.config(dir)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("\n| | |\n|---|---|\n")
	for _, k := range s.meta {
		v, ok := values[k]
		if !ok {
			return "", fmt.Errorf("%s has no %q line", s.file, k)
		}
		fmt.Fprintf(&b, "| %s | %s |\n", k, strings.ReplaceAll(v, "|", "\\|"))
	}
	return b.String(), nil
}

// recorded describes where the raw results came from — "recorded
// 2026-10-09 at 1a2b3c4d5e6f" — from the date and commit lines
// scripts/run.sh writes, or "" when the file has neither. The raw files
// are not kept in the repository, so this is what ties a table to them.
func recorded(values map[string]string) string {
	var parts []string
	if d := values["date"]; d != "" {
		day, _, _ := strings.Cut(d, "T")
		parts = append(parts, day)
	}
	if c := values["commit"]; c != "" {
		sha, suffix, _ := strings.Cut(c, "+")
		if len(sha) > 12 {
			sha = sha[:12]
		}
		if suffix != "" {
			sha += "+" + suffix
		}
		parts = append(parts, "at "+sha)
	}
	if len(parts) == 0 {
		return ""
	}
	return "recorded " + strings.Join(parts, " ")
}

// configRE matches a benchfmt configuration line.
var configRE = regexp.MustCompile(`^([a-z][^\s:]*): (.*)$`)

func (s spec) render(dir string) (string, error) {
	if len(s.meta) > 0 {
		return s.renderMeta(dir)
	}
	g, err := s.load(dir)
	if err != nil {
		return "", err
	}
	values, err := s.config(dir)
	if err != nil {
		return "", err
	}
	thresholds := benchmath.DefaultThresholds
	sample := func(row, col string) *benchmath.Sample {
		if v := g.cells[row][col]; len(v) > 0 {
			return benchmath.NewSample(v, &thresholds)
		}
		return nil
	}

	var b strings.Builder
	b.WriteString("\n|")
	for _, k := range s.rows {
		b.WriteString(" " + k + " |")
	}
	for _, c := range g.cols {
		b.WriteString(" " + c + " |")
	}
	for _, p := range s.compare {
		b.WriteString(" " + p[0] + " vs " + p[1] + " |")
	}
	b.WriteString("\n|")
	for range s.rows {
		b.WriteString("---|")
	}
	for range len(g.cols) + len(s.compare) {
		b.WriteString("---:|")
	}
	b.WriteString("\n")

	minN, maxN := math.MaxInt, 0
	for _, row := range g.rows {
		b.WriteString("|")
		for _, l := range g.rowLabels[row] {
			b.WriteString(" " + l + " |")
		}
		for _, c := range g.cols {
			smp := sample(row, c)
			if smp == nil {
				b.WriteString(" — |")
				continue
			}
			minN, maxN = min(minN, len(smp.Values)), max(maxN, len(smp.Values))
			sum := benchmath.AssumeNothing.Summary(smp, confidence)
			b.WriteString(" " + formatValue(sum.Center, s.unit))
			if ci := sum.PctRangeString(); ci != "0%" {
				b.WriteString(" ±" + ci)
			}
			b.WriteString(" |")
		}
		for _, p := range s.compare {
			b.WriteString(" " + formatChange(sample(row, p[0]), sample(row, p[1])) + " |")
		}
		b.WriteString("\n")
	}

	n := fmt.Sprint(minN)
	if minN != maxN {
		n = fmt.Sprintf("%d–%d", minN, maxN)
	}
	fmt.Fprintf(&b, "\n<sub>%s: median of %s runs ±95%% CI", s.unit, n)
	if len(s.compare) > 0 {
		b.WriteString("; changes where p < 0.05 (Mann-Whitney U), ~ otherwise")
	}
	if r := recorded(values); r != "" {
		b.WriteString("; " + r)
	}
	b.WriteString(".</sub>\n")
	return b.String(), nil
}

// formatChange is a's change relative to b, or ~ when the samples do
// not differ significantly.
func formatChange(a, b *benchmath.Sample) string {
	if a == nil || b == nil {
		return "—"
	}
	cmp := benchmath.AssumeNothing.Compare(b, a)
	ca := benchmath.AssumeNothing.Summary(a, confidence).Center
	cb := benchmath.AssumeNothing.Summary(b, confidence).Center
	return fmt.Sprintf("%s (%s)", cmp.FormatDelta(cb, ca), cmp)
}

// formatValue scales v with an SI or binary prefix and the unit's
// dimension: "142.3µs", "1.01MiB", "6". Percentages keep four
// significant digits and no prefix: "0.03250%", not "32.50m%".
func formatValue(v float64, unit string) string {
	cls := benchunit.ClassOf(unit)
	suffix := ""
	switch {
	case strings.Contains(unit, "sec"):
		suffix = "s"
	case cls == benchunit.Binary:
		suffix = "B"
	case strings.HasSuffix(unit, "%"):
		suffix = "%"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1000 {
		return fmt.Sprintf("%d%s", int64(v), suffix)
	}
	if suffix == "%" {
		decimals := max(0, 3-int(math.Floor(math.Log10(math.Abs(v)))))
		return strconv.FormatFloat(v, 'f', decimals, 64) + suffix
	}
	return benchunit.Scale(v, cls) + suffix
}
