# Security

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- **TLS.** `mqtts://`, `ssl://`, `tls://` and `wss://` verify the
  broker against the system roots by default; `WithTLSConfig` supplies
  CAs, client certificates or a server name. Never set
  `InsecureSkipVerify` outside tests. See
  [Certificate management](#certificate-management).
- **Credentials.** `WithCredentials` and `WithConnectPacketBuilder`
  (per-attempt rotation) keep secrets in memory only; the client never
  logs them. Enhanced authentication (`WithAuthenticator`) supports
  SCRAM-style challenge/response and re-authentication
  (`Reauthenticate`).
- **Untrusted input.** Every packet from the broker is validated
  before it is acted on, and a violation closes the connection (see
  [Reliability semantics](reliability.md#reliability-semantics)). Memory a broker can
  make the client hold is bounded: per packet by `WithMaximumPacketSize`
  (unset: the decoder holds at most eight times the bytes actually
  received until a packet is complete), per subscription by `SubBuffer`
  / `SubMaxQueueSize` for QoS 0 messages and by `WithReceiveMaximum`
  for QoS 1/2 ones.
- **Persistence.** `store/file` and `queue/file` write message payloads
  to disk unencrypted, with 0600 permissions by default under a 0700
  directory; encrypt the volume if payloads are sensitive.
- **Reporting.** Report vulnerabilities privately through GitHub
  security advisories on this repository.

## Certificate management

**What verification checks.** The handshake fails unless the broker's
certificate chains to a trusted root (`RootCAs`, or the system roots
when it is nil), names the host being dialed (its DNS or IP subject
alternative names match `ServerName`, or the URL host when
`ServerName` is empty), is within its validity window, and allows
server authentication. A broker that fails any of these never receives
CONNECT, so credentials are not sent to it; `Connect` and
`WithOnConnectError` return the `crypto/x509` error, which
`errors.As` resolves (`x509.UnknownAuthorityError`,
`x509.HostnameError`, `x509.CertificateInvalidError`).

**What it does not check.** Revocation. Go's `crypto/x509` consults
neither CRLs nor OCSP, and the client adds no check of its own.

**Private CA.** Put the CA in `RootCAs`. Setting `RootCAs` replaces the
system roots rather than adding to them.

```go
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caPEM)
mqttv5.WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
```

**Client certificates (mutual TLS).** `Certificates` presents a fixed
certificate. `GetClientCertificate` is called on every handshake in
which the broker asks for a client certificate, and each reconnect is
a new handshake, so returning the current certificate from it rotates
the client's certificate without restarting the client:

```go
var current atomic.Pointer[tls.Certificate] // replaced on renewal
mqttv5.WithTLSConfig(&tls.Config{
    RootCAs: pool,
    GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
        return current.Load(), nil
    },
})
```

**Rotating trusted roots, per-broker trust.** `WithTLSConfig` gives
every dial the same config. `WithTLSConfigFunc` instead asks for the
config on each TLS dial, before the TCP dial and within the connect
timeout, with the broker's URL: return the current roots to rotate
them without a new client, or a different config per broker of
`WithBrokers`. An error from it fails that attempt, which is retried
with backoff.

```go
var roots atomic.Pointer[x509.CertPool] // replaced when the CA bundle changes
mqttv5.WithTLSConfigFunc(func(ctx context.Context, broker *url.URL) (*tls.Config, error) {
    return &tls.Config{RootCAs: roots.Load(), MinVersion: tls.VersionTLS13}, nil
})
```

`WithTLSConfig`, `WithTLSConfigFunc` and `WithDialer` configure the
built-in dial, so `New` rejects them beside `WithDialFunc`; a custom
dial configures its own TLS (`ws.DialOpts` has `TLSConfig` and
`TLSConfigFunc` of its own).

**Session resumption.** The client keeps TLS sessions in an in-memory
cache of its own, so a reconnect to the same broker resumes the session
instead of repeating the full handshake: the broker sends no
certificate chain and signs nothing, which saves bandwidth and CPU on
both ends during reconnect storms. It applies to the config of
`WithTLSConfig` or `WithTLSConfigFunc` when that config sets no
`ClientSessionCache` and presents no client certificate; a config
returned fresh on each dial resumes too. `ws.DialFunc` does the same
for `wss://`.

- **Client certificates.** A resumed session does not present the
  client certificate again ([RFC 8446, section
  4.3.2](https://www.rfc-editor.org/rfc/rfc8446#section-4.3.2)), so a
  certificate rotated through `GetClientCertificate` would not reach the
  broker until it stopped honouring the session ticket, up to seven days
  in TLS 1.3. A config presenting a client certificate therefore gets no
  cache from the client; set `ClientSessionCache` on it to resume anyway.
- **Verification.** Before resuming, Go checks the cached certificate's
  validity window and host name and verifies its chain against the
  `RootCAs` of the config at hand, so removing a root ends resumption
  with it: the next connection is a full handshake and is refused.
  `tls.Config.VerifyConnection` runs on resumed connections too, given
  the chain and stapled OCSP response of the handshake that created the
  session.
- **Your own cache.** Set `ClientSessionCache`, for example to resume
  across restarts (`tls.ClientSessionState.ResumptionState` and
  `tls.NewResumptionState` convert a session to and from
  `tls.SessionState`, whose `Bytes` and `tls.ParseSessionState`
  serialise it). A persistent cache must be confidential and
  integrity-protected: a stored session holds its resumption secret, and
  a resumed TLS 1.3 broker proves itself with that secret rather than
  its certificate, so whoever can read or write the store can
  impersonate the broker.
- **Turning it off.** `SessionTicketsDisabled: true` makes every
  connection a full handshake.

**Protocol version.** Go's client accepts TLS 1.2 and later by default.
Set `MinVersion: tls.VersionTLS13` when every broker supports it.
