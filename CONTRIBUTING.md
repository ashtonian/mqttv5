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

# Only some modules and packages, as CI runs a change's scope.
GO_PACKAGES='{"codec/json": ["."], ".": ["./wire"]}' scripts/each-module.sh "$PWD/scripts/test-module.sh"

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
| `ci.yml` | every pull request and push to `master`; 04:23 UTC; by hand | A scope job, then the jobs the change needs: golangci-lint; tidy, vet and race tests (`scripts/test-module.sh`); build and govulncheck (`scripts/build-module.sh`), each over the affected packages of the modules go.work uses (`scripts/each-module.sh`); and `conformance.yml`. The schedule and a manual run check everything. |
| `conformance.yml` | called by `ci.yml` and `release.yml` | The broker suite against mosquitto, then every example. |
| `bench-codec.yml` | pull requests and pushes that touch `wire/` or the codec benchmarks | One short sample per benchmark on a pull request; the full sweep, recorded in the `gh-pages` history, on a push. See [benchmarks/README.md](benchmarks/README.md#in-ci). |
| `nightly.yml` | 02:41 UTC, and by hand | The model-based tests with new seeds every night; the full broker suite and soak unless they already passed on the same commit. See [conformance/README.md](conformance/README.md). |
| `release.yml` | a `v*` tag | The `ci.yml` module checks over every package and `conformance.yml` on the tag, a proxy install check, then the GitHub release. |

### What a change runs

`ci.yml`'s `scope` job
([go-ci-actions scope](https://github.com/ashtonian/go-ci-actions#change-scope))
decides which jobs a change needs and which packages they check.

- **Packages.** The affected packages are the ones holding a changed file plus
  every package whose build or tests import one, across all modules, from
  `go list -e -test` with the `conformance e2e smoke` tags. Lint, test and
  build run on those packages only; conformance runs whenever a package of
  the conformance or examples module is affected.
- **Measured from.** A pull request is measured from the base it merges into.
  A push to `master` is measured from the last green `ci` run on `master`, so
  a cancelled or failed run's changes are checked again.
- **Everything.** These run every job over every package: a change to
  `ci.yml`, `conformance.yml`, `scripts/each-module.sh`, any go.mod, go.sum,
  go.work or go.work.sum, a file no package owns, or a pull request labelled
  `full-ci`. So do the 04:23 UTC schedule (skipped when `master` has not
  moved since the last green scheduled run) and `gh workflow run ci.yml`. A
  change to `.golangci.yml` or a job's own script reruns that job over every
  package.
- **Reuse on merge.** A pull request's run ends with `record validation`,
  which stores a receipt of the tested merge commit's tree and the jobs that
  passed
  ([go-ci-actions acceptance](https://github.com/ashtonian/go-ci-actions#push-acceptance)).
  The push that merges it skips every job when the merge commit has that tree,
  the pull request run passed within six hours, and every job the push's own
  scope selects passed in it. A squash or rebase merge, a direct push, a
  re-run or a stale pull request run runs its scoped jobs instead. The scope
  job's log names the reason for each decision.

The scope's configuration lives in `ci.yml`, in the `scope` step's inputs.
Tests read `docs/*.md`, `README.md` and `CONTRIBUTING.md` from the root
package and `examples/readme` at run time, and the conformance suite reads
its broker and PKI files from `conformance/`. `go-owners` declares those.
When a test starts reading a file outside its own directory, add the file to
`go-owners`. Otherwise a change to that file alone runs every package or,
when the file is in `go-ignore`, skips the test that reads it. Files no
`ci.yml` job reads belong in `go-ignore`: other workflows, release scripts and
the benchmark broker setup.

Jobs pin the Go minor (`1.27.x`). Lint and tests take the newest patch the
runner image carries. The build and govulncheck job resolves the newest
released patch, because govulncheck reports the standard library of the Go
that runs it.

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
