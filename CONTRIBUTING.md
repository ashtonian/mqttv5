# Contributing

[Documentation](docs/README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](examples/README.md)

- [Build and test](#build-and-test)
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

# Every module (each has its own go.mod).
for m in . codec/json codec/msgpack store/file queue/file internal/filedb transport/ws examples benchmarks; do
    (cd $m && go vet ./... && go test -race ./...)
done

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
