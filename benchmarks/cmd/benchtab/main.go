// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Command benchtab renders the benchmark tables in Markdown files from
// raw `go test -bench` output recorded by scripts/run.sh, so every
// published number comes from a recording; each table's footnote names
// the recording's date and commit. The raw files are not kept in the
// repository (the bench workflow uploads them as artifacts).
//
// A table is declared by a pair of HTML comments; benchtab replaces
// whatever lies between them:
//
//	<!-- benchtab file=results/2026-10-08-e2e/e2e.txt filter=".name:E2E_Publish /qos:1" rows=size cols=lib unit=sec/op compare=mqttv5:autopaho -->
//	<!-- /benchtab -->
//
// Attributes:
//
//	file     raw results, relative to the Markdown file (required)
//	filter   golang.org/x/perf/benchproc filter (default: everything)
//	rows     comma-separated sub-benchmark keys labelling rows (required)
//	cols     sub-benchmark key whose values become columns (required)
//	unit     the metric, tidied as benchstat does: sec/op, B/op,
//	         allocs/op, cpu-sec/op, p99-sec, ... (required)
//	compare  comma-separated a:b column pairs; each adds a column with
//	         a's change relative to b and its p-value
//	meta     comma-separated configuration keys of the raw file (the
//	         "key: value" lines scripts/run.sh writes) to show as a
//	         table instead of results; rows, cols and unit are then not
//	         used
//
// Sub-benchmark names must be key=value pairs (lib=mqttv5/size=64B).
// Each cell is the median with its 95% confidence interval; a change is
// shown only when a Mann-Whitney U test gives p < 0.05, as benchstat
// does.
//
// Usage:
//
//	benchtab [-check] file.md...
//
// Without -check the files are rewritten in place. With -check nothing
// is written and the exit status is 1 if any table differs from what the
// raw results give; run it where the raw files are.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
)

func main() {
	check := flag.Bool("check", false, "report stale tables instead of rewriting them")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: benchtab [-check] file.md...")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	stale := false
	for _, path := range flag.Args() {
		src, err := os.ReadFile(path)
		if err != nil {
			fatal(err)
		}
		out, err := Render(src, path)
		if err != nil {
			fatal(fmt.Errorf("%s: %w", path, err))
		}
		if bytes.Equal(src, out) {
			continue
		}
		if *check {
			fmt.Fprintf(os.Stderr, "%s: tables differ from the raw results; run benchtab %s\n", path, path)
			stale = true
			continue
		}
		if err := os.WriteFile(path, out, 0o644); err != nil {
			fatal(err)
		}
	}
	if stale {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "benchtab:", err)
	os.Exit(1)
}
