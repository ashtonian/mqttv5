# Why mqttv5

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

Compared with [eclipse/paho.golang](https://github.com/eclipse-paho/paho.golang)
and its [autopaho](https://github.com/eclipse-paho/paho.golang/tree/master/autopaho)
supervisor:

- **One client, supervisor built in.** Reconnect, in-flight replay and
  re-subscription are always on; there is no second package to wire up.
- **A Go-shaped API.** Channels and queues for delivery as well as
  callbacks, `context.Context` on every call, sentinel errors for
  `errors.Is`, and functional options instead of a large options struct.
- **A low cost per message.** Decoding a PUBLISH of up to 64 KiB
  allocates nothing once pools are warm; a delivered message costs two
  allocations at QoS 0 ([Performance](../benchmarks/README.md#allocation-counts)).
- **Messages you can keep.** A delivered `*Message` owns its topic,
  payload and properties, so it is safe to retain, and `Ack` may be
  called more than once. `SubZeroCopy()` skips the copy on hot paths.
- **Backpressure per subscription.** A consumer that falls behind loses
  QoS 0 messages, never QoS 1 or 2: the broker's flow control holds
  those back, unless the subscription opts out with `SubAutoAck` or a
  `DropOldest` queue. A dropped message is acknowledged so the broker
  stops resending it.
- **Several brokers, two distinct patterns.** Failover (`WithBrokers`)
  and parallel sessions (`ClientGroup`), each its own API, and they
  compose ([Multiple brokers](multi-broker.md#multiple-brokers)).
- **Publishing apart from receiving.** `WithPublisherPool` adds
  publish-only connections to the same broker, each with a QoS 1/2
  window of its own; subscriptions stay on the main connection, so a
  slow `SubscribeCallback` handler holds up no publish a member carries
  ([Publish pool](publisher-pool.md#publish-pool-withpublisherpool)).
- **Durable publishing.** `QueuePublisher` with `queue/file` and
  `store/file` keeps publishing through outages and survives a process
  crash without publishing an in-flight message twice.
- **Typed payloads.** One `Encoding` on the client, the payload type per
  call: `cli.SubscribeAs[Reading](…)` delivers decoded values and
  `cli.PublishValue(…)` encodes any value and declares the MQTT 5
  Content Type. JSON and msgpack Encodings ship as submodules, so the
  core stays standard library only.
- **The MQTT v5 feature set, tested against real brokers.** Shared
  subscriptions, Subscription Identifiers, topic aliases in both
  directions, session expiry and resumption, retained messages, the Will
  and its properties, server redirects, enhanced authentication (CONNECT
  and mid-session, [§4.12]), and the broker's CONNACK limits enforced
  before a packet is sent. The [conformance suite](../conformance) runs
  against mosquitto and EMQX on every change, and nightly against HiveMQ
  CE, including one that announces tight CONNACK limits.
- **WebSocket without a core dependency.** `transport/ws` plugs in
  through `WithDialFunc` ([WebSocket](websocket.md#websocket)).

<!-- MQTT v5.0 citations -->

[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
