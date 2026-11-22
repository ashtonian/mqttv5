# Contributing

[Documentation](docs/README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](examples/README.md)

- [Build and test](#build-and-test)
- [Continuous integration](#continuous-integration)
- [Documentation checks](#documentation-checks)
- [Independence](#independence)

Changes should include tests for the behavior they affect. Keep public
API changes, the configuration reference, and the compiled examples in
sync. See [the architecture guide](docs/architecture.md) for the code
map. Run the commands below from the repository root.

## Build and test

```bash
# Core — no broker required.
go test -race ./...

# Every module go.work uses (each has its own go.mod): tidy, vet with and
# without the test build tags, and race tests, exactly as CI runs them.
scripts/each-module.sh "$PWD/scripts/test-module.sh"

# Conformance against mosquitto and EMQX (add `--profile hivemq` for
# HiveMQ CE; see conformance/README.md), then every example run against
# the same brokers.
docker compose -f conformance/docker-compose.yml up -d --wait
go -C conformance test -tags conformance -race ./...
go -C examples test -tags smoke -v -run TestExamplesRun .

# Benchmarks: see benchmarks/README.md.
docker compose -f benchmarks/docker-compose.yml up -d --wait
benchmarks/scripts/run.sh e2e . -tags e2e -run '^$' -bench '^BenchmarkE2E_' -benchmem -timeout 3h
go -C benchmarks run ./cmd/benchtab README.md ../README.md
```

## Continuous integration

| Workflow | Runs on | What it does |
|---|---|---|
| `ci.yml` | every pull request and push to `master` | Three jobs over every module go.work uses (`scripts/each-module.sh`): golangci-lint; tidy, vet and race tests (`scripts/test-module.sh`); build and govulncheck. A fourth calls `conformance.yml`. |
| `conformance.yml` | called by `ci.yml` and `release.yml` | The broker suite against mosquitto, then every example. |
| `bench-codec.yml` | pull requests and pushes that touch `wire/` or the codec benchmarks | One short sample per benchmark on a pull request; the full sweep, recorded in the `gh-pages` history, on a push. See [benchmarks/README.md](benchmarks/README.md#in-ci). |
| `nightly.yml` | 02:41 UTC, and by hand | The model-based tests with new seeds every night; the full broker suite and soak unless they already passed on the same commit. See [conformance/README.md](conformance/README.md). |
| `release.yml` | a `v*` tag | The `ci.yml` module checks and `conformance.yml` on the tag, a proxy install check, then the GitHub release. |

A pull request's `ci` run ends with `record validation`, which stores a
receipt of the tested merge commit's tree
([go-ci-actions acceptance](https://github.com/ashtonian/go-ci-actions#push-acceptance)).
The push that merges it starts with `reuse pull request validation`: when
the merge commit has that tree and the pull request run passed within
six hours, the lint, test, build and conformance jobs are skipped, and the
job summary links the run they passed in. A squash or rebase merge, a
direct push, a re-run or a stale pull request run executes them all; the
job's `reason` output says which. Jobs pin the Go minor (`1.27.x`). Lint
and tests take the newest patch the runner image carries; the build and
govulncheck job resolves the newest released patch, because govulncheck
reports the standard library of the Go that runs it.

## Documentation checks

The README and `docs/*.md` guides share compiled snippets in
`examples/readme/`. Update a snippet and its matching Go source together.
The configuration reference must list every exported option constructor,
and each guide defines its own MQTT specification citations.

```bash
go test -run 'Test(ReferenceDocumentsEveryOption|DocumentationLinksEveryCitation)$' .
go -C examples test ./readme/...
```

## Independence

Independent, clean-room implementation written from the
[MQTT v5.0 OASIS specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
Not a fork of any existing Go MQTT client.

The `benchmarks/` submodule imports `eclipse/paho.golang` for
head-to-head comparison only — it is not redistributed.
