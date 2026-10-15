# Security

[Documentation](README.md) · [API reference](https://pkg.go.dev/github.com/ashtonian/mqttv5) · [Examples](../examples/README.md)

- **TLS.** `mqtts://`, `ssl://`, `tls://` and `wss://` verify the
  broker against the system roots by default; `WithTLSConfig` supplies
  CAs, client certificates or a server name. Never set
  `InsecureSkipVerify` outside tests.
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
