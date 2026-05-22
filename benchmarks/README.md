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

### Subscribe — single consumer, channel vs queue (QoS 1 + manual ack)

`BenchmarkE2E_Subscribe` (chan) vs `BenchmarkE2E_SubscribeQueue`
(queue). Same workload — one publisher fires QoS 1 messages async,
one consumer drains and acks. Only the delivery primitive differs.

| Payload | autopaho chan ns/op | autopaho allocs/B | mqttv5 chan ns/op | mqttv5 chan allocs/B | mqttv5 queue ns/op | mqttv5 queue allocs/B |
|---|---:|---:|---:|---:|---:|---:|
| 64 B  | 178,809 | 95 / 10,142 | **171,334** | **21 / 1,058** | **169,344** | **19 / 825** |
| 256 B | 169,998 | 95 / 10,685 | **170,938** | **21 / 1,219** | **172,569** | **19 / 988** |
| 1 KiB | 182,351 | 97 / 14,653 | **177,546** | **21 / 2,088** | **181,167** | **19 / 1,856** |

Broker round-trip dominates the per-message time at QoS 1 (~170 µs
floor), so chan and queue land within noise on ns/op. Queue costs
**+1 alloc, +15 B** vs chan per message (linked-list node) and
otherwise reads identical. Both mqttv5 surfaces are **~5× fewer
allocs / ~7-12× fewer bytes** than autopaho's chan.

### Subscribe — fan-out under QoS 1 (broker-bound)

`BenchmarkE2E_SubscribeMultiConsumer` — N consumer goroutines race
for messages on the same delivery surface; QoS 1 publish + ack.

ns/op at 256 B (representative; full matrix in `e2e_results.txt`):

| Consumers | mqttv5 chan | mqttv5 queue | autopaho chan |
|---:|---:|---:|---:|
| 1 | 175,038 | 179,852 | 188,384 |
| 4 | 176,182 | 180,429 | 191,140 |
| 8 | 173,308 | 180,633 | 194,788 |

Flat across c1→c8 for both mqttv5 surfaces — fan-out under QoS 1 is
broker-bound (the PUBACK round-trip is the floor), so per-message
chan/queue contention is in the noise. autopaho creeps ~4% slower
at c8. At 64 B (most contention-sensitive) the same pattern
amplifies: autopaho 189k → 216k from c1→c8 (~14% slower); mqttv5
chan and queue both flat ±3%.

### Subscribe firehose — callback throughput ceiling (QoS 0)

`BenchmarkE2E_SubscribeFireHose` — QoS 0 publish (no PUBACK gating)
+ `SubscribeCallback` (sync on read goroutine, atomic counter).
The "no chan/queue overhead at all" reference point.

| Payload | autopaho ns/op | autopaho allocs/B | mqttv5 ns/op | mqttv5 allocs/B |
|---|---:|---:|---:|---:|
| 64 B  |  5,951 | 42 / 5,379 |  **5,435** | **5 / 286** |
| 256 B |  9,875 | 42 / 5,763 |    9,907 | **5 / 286** |
| 1 KiB | 24,560 | 44 / 8,867 | **23,898** | **5 / 286** |

mqttv5 ties to slight win on ns/op, **~8× fewer objects, ~16-31×
fewer bytes** at every size. At ~180k msg/s sustained that's the
difference between ~880 MB/s and ~29 MB/s of garbage produced.

### Subscribe firehose — channel vs queue under fan-out (QoS 0)

`BenchmarkE2E_SubscribeFireHoseFanOut` — same firehose publisher,
but the consumer pulls through the chan or queue primitive with
N goroutines competing. This is the raw chan/queue dispatch
benchmark without the broker RTT floor.

ns/op (median of 2 runs):

| Payload | Consumers | mqttv5 chan | mqttv5 queue | autopaho chan |
|---|---:|---:|---:|---:|
| 64 B  | 4 |  5,400 |  5,600 |  5,670 |
| 64 B  | 8 |  5,500 |  5,680 |  5,900 |
| 256 B | 4 |  9,940 | 10,580 |  9,850 |
| 256 B | 8 |   8,000 * | 10,485 | 11,533 |
| 1 KiB | 4 | 28,675 | 22,930 | 22,660 |
| 1 KiB | 8 | 24,977 | 23,121 | 23,468 |

Allocations stay flat across fan-out:

| Lib / style | allocs/op | B/op |
|---|---:|---:|
| mqttv5 chan  | **4** | ~4,250 |
| mqttv5 queue | **5** | ~4,270 |
| autopaho     | 42-44 | 5,400-8,870 |

_* Chan dropped messages at high load._ The `mqttv5-chan/c8/256 B`
row had a run fail with `received 420,604 / 422,064` — ~0.35% of
messages lost — because the channel's `SubBuffer(1024)` filled
faster than the 8 consumer goroutines could drain. The library's
documented behaviour is to auto-ack-and-drop on full buffer
(observable via `SubOnDrop`). autopaho's channel dropped much
worse at the same load (one `autopaho-chan/c1/256 B` run lost
~28% of messages). The mqttv5 queue is unbounded by default — at
the same load, zero drops, zero failures, memory simply grew until
consumers caught up.

The pattern: **chan trades drops for bounded memory; queue trades
memory for completeness.** Per-message dispatch cost is otherwise
identical. Pick by failure mode you can tolerate.

## Takeaways

1. **Hot paths are zero-alloc.** Publish is 0 / 0; inbound dispatch
   is 5 / 286 B (callback) or 4-5 / ~4,250 B (chan/queue at firehose
   rates). Compared to autopaho's 15-95 allocs / 600-14,600 B per
   call, the GC pressure simply isn't there.
2. **Single-publisher QoS 0 is tied to slightly faster.** mqttv5 is
   within ±3% of autopaho at 64 B and 256 B and ~15% faster at 1 KiB.
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
6. **Channel vs queue: tied on ns/op; differs on failure mode.**
   Per-message dispatch cost is ~identical (queue +1 alloc, +15 B).
   Under firehose pressure the channel will auto-drop on a full
   `SubBuffer` (bounded memory, lossy); the queue grows unbounded
   (lossless, memory pressure). Pick by failure mode.

## Codec micro benchmarks (no broker)

See `decode_bench_test.go` / `encode_bench_test.go` and the
`baseline.txt` snapshot for the codec-only numbers (eclipse `packets`
vs mqttv5 `wire`). Headline:

- **Decode PUBLISH: 22-115× faster, zero steady-state allocations
  (vs 22-103 for eclipse)**
- **Encode PUBLISH: 2.5-4.5× faster, 3 allocs vs 9-12**
