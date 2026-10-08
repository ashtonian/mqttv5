#!/usr/bin/env bash
# Run a benchmark suite and keep its raw output together with the
# conditions it ran under, as benchfmt configuration lines at the top of
# the file (benchstat and benchtab read them; they carry no results).
#
#   scripts/run.sh <name> <package-dir> [go test flags...]
#
# Writes results/<YYYY-MM-DD>-<name>/<name>.txt relative to this
# benchmarks directory, the full `go test` output to <name>.log, and the
# load average at the start of each run to <name>.load. <package-dir> is
# where `go test` runs: "." for this module, ".." for the core package's
# in-process benchmarks.
#
# The source is recorded as the commit, or refused when the working tree
# has changes: set ALLOW_DIRTY=1 to record one anyway, which records the
# commit plus "+dirty" and a source-hash of the changes (tracked and
# untracked), so a result can be matched to the exact tree it came from.
#
# DATE (default today, YYYY-MM-DD) names the results directory, so
# recordings that belong together share one date even when a long run
# crosses midnight.
#
# LOAD_MAX, when set, holds each run until the 1-minute load average is
# below it (checked every 30 s, for at most LOAD_WAIT seconds, default
# 3600), so a busy shared host does not land in the middle of a
# recording. If the load stays up that long the recording is abandoned:
# what was written so far is kept as <name>.txt.invalid and the script
# exits 1.
#
# RUNS (default 10) repeats the whole sweep that many times with
# -count 1, instead of -count N running each benchmark N times in a row:
# a load spike on a shared host then lands on every library in turn
# rather than on whichever was running. Do not pass -count.
#
# Examples:
#   scripts/run.sh codec . -run '^$' -bench '^Benchmark(Decode|Encode)Publish' -benchmem
#   scripts/run.sh e2e . -tags e2e -run '^$' -bench '^BenchmarkE2E_(Publish|Receive)$' -benchmem -timeout 2h
set -euo pipefail

if [ "$#" -lt 2 ]; then
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0" >&2
    exit 2
fi
runs=${RUNS:-10}
for arg in "$@"; do
    case "$arg" in
    -count | -count=* | -test.count*)
        echo "run.sh: set RUNS instead of -count" >&2
        exit 2
        ;;
    esac
done
name=$1
pkg=$2
shift 2

bench=$(cd "$(dirname "$0")/.." && pwd)
dir="$bench/results/${DATE:-$(date +%Y-%m-%d)}-$name"
out="$dir/$name.txt"
mkdir -p "$dir"

loadavg() { uptime | sed -E 's/.*load averages?: //; s/,//g'; }

# quiet waits until the 1-minute load average is below LOAD_MAX, and
# fails once it has waited LOAD_WAIT seconds.
quiet() {
    [ -n "${LOAD_MAX:-}" ] || return 0
    local waited=0
    while awk -v l="$(loadavg | cut -d' ' -f1)" -v m="$LOAD_MAX" 'BEGIN { exit !(l >= m) }'; do
        if [ "$waited" -ge "${LOAD_WAIT:-3600}" ]; then
            echo "load still $(loadavg | cut -d' ' -f1) after ${waited}s" >&2
            return 1
        fi
        sleep 30
        waited=$((waited + 30))
    done
}

cpu_model() {
    case "$(uname -s)" in
    Darwin) sysctl -n machdep.cpu.brand_string ;;
    Linux) sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1 ;;
    *) echo unknown ;;
    esac
}

top=$(git -C "$bench" rev-parse --show-toplevel)

# Results are not source: recording one must not make the next dirty.
not_results=':(exclude)benchmarks/results'

# dirty reports whether the working tree differs from HEAD, untracked
# files included.
dirty() { [ -n "$(git -C "$top" status --porcelain -- . "$not_results")" ]; }

# source_hash is a SHA-256 over the changes to HEAD: the diff of tracked
# files and the names and contents of untracked ones.
source_hash() {
    {
        git -C "$top" diff --binary HEAD -- . "$not_results"
        git -C "$top" ls-files --others --exclude-standard -z -- . "$not_results" |
            while IFS= read -r -d '' f; do
                printf '%s\0' "$f"
                cat "$top/$f"
            done
    } | shasum -a 256 | cut -c1-16
}

if dirty && [ "${ALLOW_DIRTY:-}" != 1 ]; then
    echo "run.sh: the working tree has changes; commit them, or set ALLOW_DIRTY=1 to record a source-hash" >&2
    exit 2
fi

broker() {
    local c=mqttv5-bench-mosquitto
    if [ -n "${MQTT_BROKER:-}" ]; then
        echo "$MQTT_BROKER"
    elif docker inspect "$c" >/dev/null 2>&1; then
        echo "$(docker inspect -f '{{.Config.Image}}' "$c") image $(docker inspect -f '{{.Image}}' "$c" | cut -c1-19)"
    else
        echo none
    fi
}

cpus=$(getconf _NPROCESSORS_ONLN)
{
    echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if dirty; then
        echo "commit: $(git -C "$top" rev-parse HEAD)+dirty"
        echo "source-hash: $(source_hash)"
    else
        echo "commit: $(git -C "$top" rev-parse HEAD)"
    fi
    echo "toolchain: $(go env GOVERSION)"
    echo "host-os: $(uname -sr)"
    echo "host-cpu: $(cpu_model)"
    echo "host-cpus: $cpus"
    echo "gomaxprocs: ${GOMAXPROCS:-$cpus}"
    echo "broker: $(broker)"
    echo "load-before: $(loadavg)"
    echo "runs: $runs"
    if [ -n "${LOAD_MAX:-}" ]; then
        echo "load-max: $LOAD_MAX"
    fi
    echo "flags: $*"
} >"$out"

echo "writing $out" >&2
: >"$dir/$name.log"
printf 'run\tload-1m\tload-5m\tload-15m\n' >"$dir/$name.load"

# abandon marks the recording invalid and stops.
abandon() {
    echo "invalid: $1" >>"$out"
    mv "$out" "$out.invalid"
    echo "run.sh: $1; partial results in $out.invalid" >&2
    exit 1
}

peak=0
for i in $(seq 1 "$runs"); do
    quiet || abandon "run $i of $runs did not start within LOAD_WAIT=${LOAD_WAIT:-3600}s under LOAD_MAX=$LOAD_MAX"
    load=$(loadavg)
    printf '%s\t%s\n' "$i" "$(echo "$load" | tr ' ' '\t')" >>"$dir/$name.load"
    peak=$(awk -v a="$peak" -v b="${load%% *}" 'BEGIN { print (b > a ? b : a) }')
    echo "run $i of $runs (load ${load%% *})" >&2
    set +e
    (cd "$bench/$pkg" && go test -count 1 "$@") 2>&1 | tee -a "$dir/$name.log" | grep -E '^(Benchmark|goos|goarch|pkg|cpu)' >>"$out"
    status=("${PIPESTATUS[@]}")
    set -e
    [ "${status[0]}" -eq 0 ] || abandon "go test failed in run $i of $runs (see $name.log)"
    [ "${status[2]}" -eq 0 ] || abandon "run $i of $runs produced no benchmark results"
done
# After the results, so they configure none of them.
echo "load-after: $(loadavg)" >>"$out"
echo "load-peak: $peak" >>"$out"
