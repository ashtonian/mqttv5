// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rawResults writes n runs of two libraries' results: fast takes
// 100–109 µs per op, slow 200–209 µs; allocations are constant.
func rawResults(t *testing.T, dir string, n int) {
	t.Helper()
	var b strings.Builder
	b.WriteString("date: 2026-10-09T01:02:03Z\ncommit: 0123456789abcdef0123+dirty\nsource-hash: 5eed\nbroker: mosquitto | 2.1\ngoos: darwin\ngoarch: arm64\npkg: example\n")
	for i := range n {
		for _, size := range []string{"64B", "1KiB"} {
			fmt.Fprintf(&b, "BenchmarkPub/lib=fast/size=%s-12 \t 1000 \t %d ns/op \t 6 allocs/op\n", size, 100000+1000*i)
			fmt.Fprintf(&b, "BenchmarkPub/lib=slow/size=%s-12 \t 1000 \t %d ns/op \t 53 allocs/op\n", size, 200000+1000*i)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "raw.txt"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRender(t *testing.T) {
	dir := t.TempDir()
	rawResults(t, dir, 10)
	md := "# Results\n\n" +
		`<!-- benchtab file=raw.txt filter=".name:Pub" rows=size cols=lib unit=sec/op compare=fast:slow -->` + "\nstale\n<!-- /benchtab -->\n\n" +
		`<!-- benchtab file=raw.txt rows=size cols=lib unit=allocs/op -->` + "\n<!-- /benchtab -->\n"
	out, err := Render([]byte(md), filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := "# Results\n\n" +
		`<!-- benchtab file=raw.txt filter=".name:Pub" rows=size cols=lib unit=sec/op compare=fast:slow -->` + "\n" +
		"\n| size | fast | slow | fast vs slow |\n|---|---:|---:|---:|\n" +
		"| 64B | 104.5µs ±3% | 204.5µs ±2% | -48.90% (p=0.000 n=10) |\n" +
		"| 1KiB | 104.5µs ±3% | 204.5µs ±2% | -48.90% (p=0.000 n=10) |\n" +
		"\n<sub>sec/op: median of 10 runs ±95% CI; changes where p < 0.05 (Mann-Whitney U), ~ otherwise; recorded 2026-10-09 at 0123456789ab+dirty (source 5eed).</sub>\n" +
		"<!-- /benchtab -->\n\n" +
		`<!-- benchtab file=raw.txt rows=size cols=lib unit=allocs/op -->` + "\n" +
		"\n| size | fast | slow |\n|---|---:|---:|\n| 64B | 6 | 53 |\n| 1KiB | 6 | 53 |\n" +
		"\n<sub>allocs/op: median of 10 runs ±95% CI; recorded 2026-10-09 at 0123456789ab+dirty (source 5eed).</sub>\n" +
		"<!-- /benchtab -->\n"
	if string(out) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out, want)
	}
	again, err := Render(out, filepath.Join(dir, "README.md"))
	if err != nil || string(again) != string(out) {
		t.Fatalf("rendering twice changed the output (%v)", err)
	}
}

// Too few runs for a significant difference show "~".
func TestRenderInsignificant(t *testing.T) {
	dir := t.TempDir()
	rawResults(t, dir, 3)
	md := `<!-- benchtab file=raw.txt rows=size cols=lib unit=sec/op compare=fast:slow -->` + "\n<!-- /benchtab -->\n"
	out, err := Render([]byte(md), filepath.Join(dir, "x.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "| ~ (p=0.100 n=3) |") {
		t.Fatalf("expected an insignificant change:\n%s", out)
	}
}

func TestRenderErrors(t *testing.T) {
	dir := t.TempDir()
	rawResults(t, dir, 1)
	for name, attrs := range map[string]string{
		"missing file": `rows=size cols=lib unit=sec/op`,
		"no such key":  `file=raw.txt rows=qos cols=lib unit=sec/op`,
		"no results":   `file=raw.txt rows=size cols=lib unit=p99-sec`,
		"bad compare":  `file=raw.txt rows=size cols=lib unit=sec/op compare=fast`,
		"unknown attr": `file=raw.txt rows=size cols=lib unit=sec/op colour=red`,
		"missing raw":  `file=nope.txt rows=size cols=lib unit=sec/op`,
	} {
		t.Run(name, func(t *testing.T) {
			md := "<!-- benchtab " + attrs + " -->\nkept\n<!-- /benchtab -->\n"
			out, err := Render([]byte(md), filepath.Join(dir, "x.md"))
			if err == nil {
				t.Fatal("no error")
			}
			if string(out) != md {
				t.Fatalf("a failed table was rewritten:\n%s", out)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	for _, tt := range []struct {
		v    float64
		unit string
		want string
	}{
		{0.0001423, "sec/op", "142.3µs"},
		{6, "allocs/op", "6"},
		{1058520, "B/op", "1.009MiB"},
		{512, "B/op", "512B"},
		{99.5, "delivered-%", "99.50%"},
		{0.0325, "delivered-%", "0.03250%"},
		{32.77, "delivered-%", "32.77%"},
		{2.5e-3, "p99-sec", "2.500ms"},
	} {
		if got := formatValue(tt.v, tt.unit); got != tt.want {
			t.Errorf("formatValue(%v, %q) = %q, want %q", tt.v, tt.unit, got, tt.want)
		}
	}
}

func TestRenderMeta(t *testing.T) {
	dir := t.TempDir()
	rawResults(t, dir, 1)
	md := "<!-- benchtab file=raw.txt meta=commit,broker,goos -->\n<!-- /benchtab -->\n"
	out, err := Render([]byte(md), filepath.Join(dir, "x.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := "<!-- benchtab file=raw.txt meta=commit,broker,goos -->\n" +
		"\n| | |\n|---|---|\n| commit | 0123456789abcdef0123+dirty |\n| broker | mosquitto \\| 2.1 |\n| goos | darwin |\n" +
		"<!-- /benchtab -->\n"
	if string(out) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", out, want)
	}
	if _, err := Render([]byte("<!-- benchtab file=raw.txt meta=nope -->\n<!-- /benchtab -->\n"), filepath.Join(dir, "x.md")); err == nil {
		t.Fatal("a missing key rendered")
	}
}

// A marker inside a fenced code block is documentation, not a table.
func TestRenderSkipsCodeFences(t *testing.T) {
	dir := t.TempDir()
	rawResults(t, dir, 10)
	md := "```text\n<!-- benchtab file=nope.txt rows=size cols=lib unit=sec/op -->\n<!-- /benchtab -->\n```\n" +
		"<!-- benchtab file=raw.txt rows=size cols=lib unit=allocs/op -->\n<!-- /benchtab -->\n"
	out, err := Render([]byte(md), filepath.Join(dir, "x.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), "```text\n<!-- benchtab file=nope.txt rows=size cols=lib unit=sec/op -->\n<!-- /benchtab -->\n```\n") {
		t.Fatalf("the fenced example changed:\n%s", out)
	}
	if !strings.Contains(string(out), "| 64B | 6 | 53 |") {
		t.Fatalf("the table after the fence was not rendered:\n%s", out)
	}
}
