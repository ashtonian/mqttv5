# Authentication

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- [Credentials per connection attempt](#credentials-per-connection-attempt)
- [Re-authentication without reconnecting](#re-authentication-without-reconnecting)
- [Mutual authentication](#mutual-authentication)

`WithCredentials(user, pass)` sends a fixed username and password.
For credentials that change, rotate them per connection attempt or
re-authenticate the live connection; `WithAuthenticator` adds MQTT v5
enhanced authentication (challenge/response such as SCRAM).

## Credentials per connection attempt

`WithConnectPacketBuilder(fn)` runs immediately before each CONNECT is
serialised. Use it to refresh an OAuth token, fetch a SigV4-signed
CONNECT credential, or rotate any other per-attempt secret. The context
is bounded by `ConnectTimeout`.

```go
mqttv5.WithConnectPacketBuilder(func(ctx context.Context, opts *mqttv5.ConnectOptions) error {
    tok, err := oauth.FetchToken(ctx)
    if err != nil {
        return err // fails this attempt; supervisor retries after backoff
    }
    opts.Username = "service-account"
    opts.Password = []byte(tok)
    return nil
}),
```

`WithOnConnectError` reports each failed attempt with its error. See
[`examples/oauth`](../examples/oauth).

## Re-authentication without reconnecting

`Client.Reauthenticate(ctx)` drives MQTT v5 re-authentication ([§4.12]) on
the *live* connection — no reconnect, no QoS-state churn:

1. Sends an AUTH `0x19` carrying a fresh `Authenticator.Begin(ctx)` payload.
2. Services any broker challenges via `Continue`.
3. Returns when the broker concludes with `0x00` Success.

Use it on a long-lived connection whose bearer token (OAuth, JWT)
expires: start a timer from the token's lifetime and call
`Reauthenticate` ahead of expiry.

```go
// e.g. 30s before the JWT `exp`:
if err := cli.Reauthenticate(ctx); err != nil {
    // ErrReauthRejected → broker refused the new credential;
    // the supervisor is already reconnecting with a fresh CONNECT.
}
```

Behaviour:

- `ctx` bounds the whole operation, including the token fetch in `Begin`.
- Calls are single-flighted per connection.
- A broker rejection returns `ErrReauthRejected` and tears the
  connection down, so the supervisor reconnects through the normal
  CONNECT path.
- `WithOnReauthenticated` observes every successful refresh in one
  place.
- With a [publish pool](publisher-pool.md#publish-pool-withpublisherpool), every
  connected member re-authenticates at the same time; a member's
  failure is joined into the returned error, and a member that is not
  connected presents a fresh credential when it connects.

## Mutual authentication

For mechanisms with mutual authentication (e.g. SCRAM), an
`Authenticator` may also implement the optional `ServerFinalVerifier`
interface (`VerifyServerFinal([]byte) error`).

The client hands it the server's concluding `AuthenticationData` — the
CONNACK on connect, the AUTH `0x00` on re-auth — so it can verify the
server proved knowledge of the credential.

A verification failure aborts the connect, or fails `Reauthenticate`
and tears the connection down.

<!-- MQTT v5.0 citations -->

[§4.12]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256 "4.12 Enhanced authentication"
