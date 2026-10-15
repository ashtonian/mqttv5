# Documentation

[Project overview and quick start](../README.md) · [Go API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Runnable examples](../examples/README.md)

The guides explain usage and behavior; the configuration reference lists
every option, default and error. Commands are run from the repository
root unless a guide says otherwise.

## Getting started

- [Quick start](../README.md#quick-start): connect, subscribe, publish and acknowledge.
- [Why mqttv5](comparison.md): differences from Paho and the supported MQTT v5 features.
- [Modules and dependencies](modules.md): core, codecs, file stores and WebSocket transport.
- [Runnable examples](../examples/README.md): programs and broker configuration.

## Using a client

- [Publishing](publishing.md): completion semantics, the write path and broker limits.
- [Subscribing](subscribing.md): channels, queues, callbacks, backpressure and durable consumers.
- [Typed payloads](typed-payloads.md): encodings, typed delivery and decode errors.
- [Request / response](request-response.md): Response Topic and Correlation Data.
- [Durable publishing](durable-publishing.md): outbound queues and crash recovery.
- [Authentication](authentication.md): fixed and rotating credentials, enhanced authentication and re-authentication.
- [WebSocket](websocket.md): ws:// and wss:// transport.
- [Disconnecting](disconnecting.md): graceful shutdown and DISCONNECT properties.

## Multiple connections and brokers

- [Multiple brokers](multi-broker.md): failover, parallel sessions and how the patterns compose.
- [Publish pools](publisher-pool.md): additional publish-only connections and their guarantees.
- [Recipes](recipes.md): central services, bridges, regional HA, quorum writes, sharding, outboxes, service discovery and shared subscriptions.

## Reference and operations

- [Configuration and errors](reference.md): every option, default, error and API stability policy.
- [Reliability semantics](reliability.md): connection lifecycle, sessions, delivery, subscriptions and protocol behavior.
- [Operations](operations.md): health, metrics, logs, troubleshooting and store failures.
- [Security](security.md): TLS, credentials, untrusted input, persistence and reporting.
- [Performance](../benchmarks/README.md): complete results, conditions and reproduction commands.

## Contributing

- [Contributing](../CONTRIBUTING.md): build, test and documentation checks.
- [Architecture and code map](architecture.md): read and write paths, session engine and repository layout.
- [Conformance suite](../conformance/README.md): tests against real brokers.
