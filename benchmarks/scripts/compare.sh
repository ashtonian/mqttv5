#!/usr/bin/env bash
# Compare two Go benchmark output files with benchstat.
#
# Typical workflow:
#   1. Dispatch the `bench` workflow on main:
#        gh workflow run bench.yml --ref main
#   2. Wait for it, download the artifact:
#        gh run download <run-id> -n e2e-bench-<run-id> -D /tmp/base
#   3. Dispatch on your PR branch, download:
#        gh run download <run-id> -n e2e-bench-<run-id> -D /tmp/cur
#   4. Run:
#        benchmarks/scripts/compare.sh /tmp/base/e2e_results.txt /tmp/cur/e2e_results.txt
#
# Output: benchstat's standard table with geomean, delta vs baseline,
# and significance per metric.
set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <baseline.txt> <current.txt>" >&2
    exit 2
fi

baseline=$1
current=$2

for f in "$baseline" "$current"; do
    if [ ! -f "$f" ]; then
        echo "error: $f not found" >&2
        exit 1
    fi
done

if ! command -v benchstat >/dev/null 2>&1; then
    echo "installing benchstat..." >&2
    go install golang.org/x/perf/cmd/benchstat@latest
fi

benchstat "$baseline" "$current"
