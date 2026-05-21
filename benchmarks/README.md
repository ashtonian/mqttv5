# benchmarks

Head-to-head benchmarks comparing `github.com/ashtonian/mqttv5`
against `github.com/eclipse/paho.golang`.

Two tiers, gated by build tag:

| Tier | What it measures | Build tag | Requires broker |
|---|---|---|---|
| Codec micro | Decode / encode of MQTT packets in isolation | none | no |
| End-to-end (e2e) | Real Publish / Subscribe / round-trip against a live broker | `e2e` | yes (mosquitto via docker) |

## Independence note

This is a **separate Go module** (`github.com/ashtonian/mqttv5/benchmarks`)
that is published with `mqttv5` but is **not** required by anything in
the core library. The core `mqttv5` module has **no dependency** on
`eclipse/paho.golang`. The benchmark code is original (Apache 2.0,
same as the rest of the project) and only consumes eclipse paho's
public API to feed fixtures through it. No eclipse paho source is
copied or redistributed by this module.

The eclipse paho dependency is pinned to an upstream tag in this
module's `go.mod` (currently `v0.23.0`) and fetched from the module
proxy like any other dependency — no local clone is required.

## Running

### Codec micro benchmarks (no broker needed)

```bash
go -C benchmarks test -bench=. -run=^$ -benchmem -count=3 -benchtime=2s
```

### End-to-end benchmarks (requires mosquitto)

```bash
docker compose -f ../conformance/docker-compose.yml up -d mosquitto
go -C benchmarks test -tags e2e -bench='^BenchmarkE2E_' -run=^$ -benchmem -benchtime=2s -count=2 -timeout 10m
docker compose -f ../conformance/docker-compose.yml down
```

Use `MQTT_BROKER=mqtt://host:port` to point at a different broker.

## CI tracking

Two GitHub Actions workflows track benchmark health:

### Codec micro benchmarks — automatic, on every push/PR

Workflow: [`.github/workflows/bench-codec.yml`](../.github/workflows/bench-codec.yml).
Runs on every push to `main` and on PRs that touch `wire/`,
`benchmarks/decode_*`, or `benchmarks/encode_*`.

Results land in two places via
[`benchmark-action/github-action-benchmark`](https://github.com/benchmark-action/github-action-benchmark):

- **Live dashboard** (Chart.js, one line per benchmark, full history):
  `https://<owner>.github.io/mqttv5/dev/bench/`
  *(One-time setup: enable GitHub Pages on the `gh-pages` branch in
  the repo's Settings → Pages. The first push to `main` after this
  workflow lands creates the branch automatically.)*
- **PR comments** — every PR run posts a benchstat-style diff vs the
  latest entry on `main`. Regressions ≥ 1.5× trigger a flagged
  comment but do not fail the build.

### End-to-end benchmarks vs autopaho — manual, on demand

Workflow: [`.github/workflows/bench.yml`](../.github/workflows/bench.yml).
Dispatch via `gh workflow run bench.yml --ref <branch>` (or the
"Run workflow" button in the GitHub UI). Runs `BenchmarkE2E_*`
against mosquitto in docker for ~5-10 minutes, uploads the raw
output as an artifact.

E2E benches are too slow + too noisy on shared CI runners for
automated tracking — compare them locally with `benchstat`:

```bash
# Dispatch against main, then your branch:
gh workflow run bench.yml --ref main
gh workflow run bench.yml --ref <your-branch>

# Once each completes, download the artifacts:
gh run download <main-run-id>   -n e2e-bench-<main-run-id>   -D /tmp/base
gh run download <branch-run-id> -n e2e-bench-<branch-run-id> -D /tmp/cur

# Compare:
benchmarks/scripts/compare.sh /tmp/base/e2e_results.txt /tmp/cur/e2e_results.txt
```

`compare.sh` installs `benchstat` on first use and prints the
geomean / delta / significance table per metric.

## E2E results

Against eclipse-mosquitto:2.0 on loopback, Apple M2 Pro (12-core),
median of 5 runs at 3 s benchtime each. Full raw output in
`e2e_results.txt`.

Numbers below are **per-call** ns/op and **per-call** allocations,
unless noted (concurrent runs report aggregate). Loopback runs are
noisy; treat ~10% deltas as in-the-noise unless they repeat across
alloc and MB/s columns too.

### Single-publisher publish QoS 0, default `PublishFireAndForget`

`Publish` QoS 0 — no PUBACK. Tests the encode + writer hand-off cost
only. mqttv5 runs in its **default** `PublishFireAndForget` mode:
the call pre-encodes the packet into a pooled buffer and hands it to
the writer goroutine, which does one `conn.Write`. autopaho's QoS 0
always waits for its writer mutex — that's its only QoS 0 path.

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 (default) ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  | 4,476 | 15 / 600 | 4,427 | **0 / 0** |
| 256 B | 4,509 | 15 / 600 | 4,651 | **0 / 0** |
| 1 KiB | 5,216 | 15 / 600 | 4,420 | **0 / 0** |

**Zero allocations per call** at every payload size — every byte goes
through the pooled-buffer wire encoder, no closure capture, no slice
literals that escape. Ns/op is essentially tied with autopaho on 64 B
and 256 B and ~15 % faster on 1 KiB.

### Explicit wait-for-flush QoS 0 (`WithPublishMode(PublishWaitForFlush)`)

Use this mode when you want a transport-level write error to surface
as the `Publish` call's return value. `Publish` blocks until the
writer goroutine has called `conn.Write` — apples-to-apples with
autopaho (whose QoS 0 always behaves this way). Same zero-alloc
encode path as fire-and-forget; the only extra cost is the
ack-channel round-trip with the writer.

### Single-publisher publish QoS 1 (waits for PUBACK)

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  | 148,159 | 53 / 4,413 | **142,120** | **11 / 687** |
| 256 B | 351,678 | 53 / 4,572 | **146,249** | **11 / 848** |
| 1 KiB | 307,343 | 53 / 5,437 | **209,554** | **11 / 1,714** |

mqttv5 consistently faster and **~5× fewer allocations / ~3-7× fewer
bytes**. autopaho's 256 B and 1 KiB rows include one slow run each
(504 k and 439 k ns/op) — even ignoring those, mqttv5 lines up at or
above autopaho's best.

### Single-publisher publish QoS 2 (full handshake)

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  | 296,962 | 92 / 7,364 | 305,076 | **15 / 781** |
| 256 B | 285,941 | 92 / 7,524 | 299,729 | **15 / 942** |
| 1 KiB | 301,370 | 92 / 8,388 | 297,835 | **15 / 1,808** |

Tied on ns/op (each library spends most of its time in two broker
round-trips). **~6× fewer allocations, ~4-10× fewer bytes** for
mqttv5.

### Concurrent publish — 8 goroutines, QoS 1

```
PublishConcurrentQoS1: 8 goroutines all calling Publish() on the
same client. autopaho serialises writes via a single mutex; mqttv5
funnels into an MPSC channel consumed by a dedicated writer goroutine.
```

| Payload | autopaho ns/op | autopaho MB/s | mqttv5 ns/op | mqttv5 MB/s | Speedup |
|---|---:|---:|---:|---:|---:|
| 64 B  | 38,472 |  1.7 | **12,128** |  **5.3** | **~3.2×** |
| 256 B | 22,934 | 11.2 | **16,948** | **15.1** | **~1.35×** |
| 1 KiB | 27,654 | 37.1 | **16,785** | **64.9** | **~1.65×** |

Per-call allocation gap: **11 allocs / ~690-1,730 B for mqttv5** vs
**56 allocs / 4,640-5,664 B for autopaho** — the writer queue is the
whole point of the design and it pays off here.

### Round-trip (publisher → broker → subscriber, QoS 1)

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  | 178,352 | 95 / 10,140 |  202,397 | **22 / 1,109** |
| 256 B | 199,714 | 95 / 10,684 | **185,304** | **22 / 1,269** |
| 1 KiB | 207,221 | 97 / 14,653 | **186,511** | **22 / 2,140** |

Tied to slight mqttv5 win on ns/op. **~4× fewer allocations,
~7-8× fewer bytes** for mqttv5.

### Subscribe throughput (QoS 1 publish + manual-ack receive)

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  | 201,005 | 95 / 10,140 | **171,361** | **22 / 1,108** |
| 256 B | 194,637 | 95 / 10,684 |  193,605 | **22 / 1,270** |
| 1 KiB | 209,762 | 97 / 14,652 |  202,902 | **22 / 2,136** |

Tied to slight mqttv5 win, same ~4× allocs / ~7-8× bytes story.

### Subscribe firehose (no PUBACK gating, callback subscriber)

Publisher fires QoS 0 without per-message acks (mqttv5's default
`Publish` in `PublishFireAndForget` mode; autopaho regular QoS 0
Publish). Subscriber increments an atomic counter directly from the
inbound dispatch path — no channel buffer to overflow. Unlike the
QoS 1 `Subscribe` benchmark above, the publisher rate is **not**
capped by PUBACK round-trips.

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  |  5,951 | 42 / 5,379 |  **5,435** | **5 / 286** |
| 256 B |  9,875 | 42 / 5,763 |    9,907 | **5 / 286** |
| 1 KiB | 24,560 | 44 / 8,867 | **23,898** | **5 / 286** |

mqttv5 wins or ties at every size. **~8× fewer objects, ~16-31×
fewer bytes** at every size — at a sustained 100k msg/sec that's
the difference between ~880 MB/s of garbage (autopaho) and ~29 MB/s
(mqttv5).

## Takeaways

1. **Hot paths are zero-alloc.** Publish is 0 / 0; inbound dispatch
   is 5 / 286 B. Compared to autopaho's 15-95 allocs / 600-14 600 B
   per call, the GC pressure simply isn't there.
2. **Single-publisher QoS 0 is tied to slightly faster.** mqttv5 is
   within ±3 % of autopaho at 64 B and 256 B and ~15 % faster at 1 KiB.
   `PublishMode` is meaningful for callers who need transport-level
   error visibility (`PublishWaitForFlush`), at the cost of one
   channel round-trip with the writer.
3. **Concurrency is the decisive win.** With 8 publisher goroutines
   on one client, mqttv5 is **1.3-3.2× faster** because the writer
   queue eliminates the mutex contention autopaho serialises through.
4. **QoS 1 single-publisher: mqttv5 wins.** Faster *and* fewer allocs
   at every payload size.
5. **Round-trip-dominated workloads (QoS 2, RoundTrip, Subscribe)
   are roughly tied on ns/op** — both libraries spend most of their
   time waiting on the broker. The allocation gap remains.

## Codec micro benchmarks (no broker)

See `decode_bench_test.go` / `encode_bench_test.go` and the
`baseline.txt` snapshot for the codec-only numbers (eclipse `packets`
vs mqttv5 `wire`). Headline:

- **Decode PUBLISH: 22-115× faster, zero steady-state allocations
  (vs 22-103 for eclipse)**
- **Encode PUBLISH: 2.5-4.5× faster, 3 allocs vs 9-12**
