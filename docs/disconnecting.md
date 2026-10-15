# Disconnecting

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

`Disconnect(ctx)` ends the client gracefully:

1. it sends DISCONNECT (Normal disconnection) after any acknowledgements
   already made;
2. it waits for the connection's goroutines (the read loop runs
   `SubscribeCallback` handlers) and closes every subscription's channel
   or queue;
3. it waits, bounded by ctx, for session store writes;
4. with a [publish pool](publisher-pool.md#publish-pool-withpublisherpool), it refuses
   new publishes with `ErrClosed`, waits, bounded by ctx, for those in
   flight on pool members, and disconnects the members.

`Disconnect` returns ctx's error, joined with the pool members'
disconnect errors, if ctx ended before steps 3 or 4 completed. Unfinished
QoS 1/2 exchanges of the main connection stay in the session for the
next `Connect`; a pool member's session ends with its
connection, which is why step 4 waits for them.
`Disconnect` is idempotent and does not fire `OnConnectionDown`: the
call itself is the "going down" signal. A lifecycle callback may call
it; a `SubscribeCallback` handler must not (see
[Lifecycle callbacks](reference.md#lifecycle-callbacks)).

`DisconnectWith(ctx, opts)` sets the reason code and properties:

```go
expiry := uint32(0)
_ = cli.DisconnectWith(ctx, mqttv5.DisconnectOptions{
    ReasonCode:            mqttv5.ReasonAdministrativeAction,
    ReasonString:          "planned shutdown",
    SessionExpiryInterval: &expiry, // override to drop the session immediately
})
```

See [`examples/disconnect`](../examples/disconnect).
