# Request / response

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

MQTT 5 carries the reply address in the request: a Response Topic and
Correlation Data, which the responder copies into its reply.

```go
// Responder: reply on each request's Response Topic, with its
// Correlation Data. A shared subscription spreads requests over instances.
reqs, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "$share/pricing/requests/price", QoS: 1}})
for m := range reqs {
    if rt := m.Properties.ResponseTopic(); rt != "" {
        _ = cli.Publish(ctx, mqttv5.PublishOptions{
            Topic:           rt,
            CorrelationData: m.Properties.CorrelationData(),
            QoS:             1,
            Payload:         price(m.Payload),
        })
    }
    _ = m.Ack()
}
```

```go
// Requester: one reply subscription serves every request in flight.
replyTo := "replies/" + cli.ClientID()
replies, _, _ := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: replyTo, QoS: 1}})
_ = cli.Publish(ctx, mqttv5.PublishOptions{
    Topic:           "requests/price",
    QoS:             1,
    Payload:         []byte("A-100"),
    ResponseTopic:   replyTo,
    CorrelationData: []byte("req-1"), // matches the reply to this call
})
```

A requester with several requests in flight matches each reply to its
call by Correlation Data; [`examples/request`](../examples/request) keeps
the pending calls in a map, each with its own timeout. Brokers that
assign reply topics announce a prefix in `ConnackInfo.ResponseInformation`
when asked with `WithRequestResponseInformation(true)`.
