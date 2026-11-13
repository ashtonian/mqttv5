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

**Rotating trusted roots.** `RootCAs` is read from the config given to
`WithTLSConfig` at `New` and used for every dial. Replacing it needs a
dial path that builds its config per dial: `WithDialFunc` around
`transport.DialTLS`.

**Protocol version.** Go's client accepts TLS 1.2 and later by default.
Set `MinVersion: tls.VersionTLS13` when every broker supports it.

**Inspecting the connection.** `ConnackInfo.TLS`, in
`WithOnConnectionUp` and `ServerInfo()`, is the handshake's
`tls.ConnectionState`: the broker's certificate chain as presented
(`PeerCertificates`) and as verified (`VerifiedChains`), the negotiated
version, cipher suite and ALPN protocol, and any stapled OCSP response.
It is nil without TLS. Use it to record which certificate a broker
presented, or to warn before it expires:

```go
mqttv5.WithOnConnectionUp(func(info mqttv5.ConnackInfo) {
    if info.TLS == nil {
        return
    }
    leaf := info.TLS.PeerCertificates[0]
    slog.Info("broker certificate", "broker", info.Broker,
        "subject", leaf.Subject.String(), "not_after", leaf.NotAfter)
})
```

A custom `WithDialFunc` transport reports its TLS state by implementing
`transport.ConnectionStater` on the connection it returns.
