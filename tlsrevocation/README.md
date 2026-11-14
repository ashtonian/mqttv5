# tlsrevocation

Certificate revocation checking for
[`github.com/ashtonian/mqttv5`](../README.md), and for any Go TLS client:
stapled OCSP responses, OCSP responders and CRLs. It is a separate module
so the core stays standard-library only.

```go
import "github.com/ashtonian/mqttv5/tlsrevocation"

checker, err := tlsrevocation.New()
if err != nil {
    return err
}
cli, err := mqttv5.New(
    mqttv5.WithBroker("mqtts://broker.example.com:8883"),
    mqttv5.WithTLSVerifier(checker.Verify),
)
```

| | |
|---|---|
| Module | `github.com/ashtonian/mqttv5/tlsrevocation` |
| Depends on | `github.com/ashtonian/mqttv5` (tests only), [`golang.org/x/crypto`](https://pkg.go.dev/golang.org/x/crypto/ocsp) v0.59.0 (`ocsp`) |
| Standards | RFC 5280 (CRLs), RFC 6960 and RFC 5019 (OCSP), RFC 6066 §8 (stapling), RFC 7633 (must-staple) |

## Overview

Go's `crypto/x509` verifies a certificate's chain, name and validity
window, but never asks whether the certificate has been revoked. A
`Checker` does, after the handshake: `mqttv5.WithTLSVerifier` runs
`Checker.Verify` before CONNECT, under the connection attempt's context,
so credentials never reach a broker whose certificate is revoked and every
lookup is bounded by the connect timeout. `Checker.VerifyConnection` fits
`tls.Config.VerifyConnection` for other clients.

Every certificate of the verified chain except the root is checked (the
leaf alone with `WithScope(LeafOnly)`). For each certificate:

1. **Local evidence.** The stapled OCSP response (leaf only) and the CRL
   from `WithCRLProvider`. A revocation reported by either fails the
   check; otherwise either one vouching for the certificate ends the
   check with no network traffic.
2. **Must-staple.** A leaf carrying the RFC 7633 TLS Feature extension
   with `status_request` fails without a valid staple, under any policy.
3. **OCSP responders** named in the certificate's Authority Information
   Access, in order: GET when the encoded request fits in 255 bytes,
   POST otherwise.
4. **CRL distribution points** named in the certificate, in order.

The first definitive answer (good or revoked) decides. When no source
answers, the status is unknown and `WithUnknownPolicy` decides: by default
the check fails.

When the handshake produced several verified chains (cross-signed
intermediates), the check passes if any chain passes, and returns the
first chain's error otherwise.

Validation beyond what the underlying parsers do:

- OCSP: `ocsp.ParseResponseForCert` checks signatures only. A response
  signed by a delegated responder is accepted only when the responder's
  certificate carries `id-kp-OCSPSigning` and is within its validity
  window (RFC 6960 §4.2.2.2), and every response must be current
  (`ThisUpdate` not in the future, `NextUpdate` not past, with
  `WithClockSkew` tolerance; without `NextUpdate`, at most `WithMaxAge`
  old).
- CRLs: `crypto/x509` parses CRL extensions without rejecting unknown
  critical ones. A CRL is used only when it is issued and signed by the
  certificate's issuer, is current, and is complete and direct: delta
  CRLs, indirect CRLs, CRLs partitioned by reason, and CRLs with an
  unknown critical extension (or an entry with a critical extension) are
  rejected. An Issuing Distribution Point restricting the CRL to CA or
  end-entity certificates is honoured, and one that names distribution
  points must name the URL the CRL was fetched from.

## Configuration

Options to `New`; every one has a default.

| Option | Type | Default | Description |
|---|---|---|---|
| `WithUnknownPolicy` | `Policy` | `FailClosed` | What an unknown status does: `FailClosed` fails the check; `FailOpen` allows it, logs a warning and counts `Stats.AllowedUnknown`. Revoked certificates and missing must-staple staples fail under either. |
| `WithScope` | `Scope` | `FullChain` | `FullChain` checks every certificate but the root; `LeafOnly` checks the broker's certificate. |
| `WithCRLProvider` | `CRLProvider` | none | Supplies CRLs out of band, per issuer, for a private CA without distribution points. Called on every check and not cached, so a pushed revocation applies at once; return `nil, nil` for an issuer it has no CRL for. |
| `WithHTTPClient` | `*http.Client` | see below | The client that fetches OCSP responses and CRLs. |
| `WithFetchTimeout` | `time.Duration` | `DefaultFetchTimeout` (5 s) | Bound on one fetch. Also the default HTTP client's dial, TLS handshake and response-header timeouts. |
| `WithVerifyTimeout` | `time.Duration` | `DefaultVerifyTimeout` (10 s) | Bound on one `VerifyConnection` call, which has no context of its own. `Verify` is bounded by its context. |
| `WithClockSkew` | `time.Duration` | `DefaultClockSkew` (5 min) | Tolerance on OCSP response, CRL and delegated responder validity windows. |
| `WithMaxAge` | `time.Duration` | `DefaultMaxAge` (24 h) | How long a response or CRL without `NextUpdate` is accepted after its `ThisUpdate`. |
| `WithFailureBackoff` | `time.Duration` | `DefaultFailureBackoff` (10 s) | How long a URL whose fetch failed is skipped; zero retries every time. |
| `WithMaxOCSPResponseBytes` | `int64` | `DefaultMaxOCSPResponseBytes` (64 KiB) | Cap on a fetched OCSP response. |
| `WithMaxCRLBytes` | `int64` | `DefaultMaxCRLBytes` (16 MiB) | Cap on a fetched CRL. |
| `WithCacheEntries` | `int, int` | `DefaultOCSPCacheEntries` (1024), `DefaultCRLCacheEntries` (8) | Bounds on cached OCSP answers (per certificate) and CRLs (per issuer and URL). Entries expire at their `NextUpdate`; least recently used are evicted first. Zero disables a cache. |
| `WithClock` | `func() time.Time` | `time.Now` | Time source for validity checks and cache expiry. |
| `WithLogger` | `*slog.Logger` | `slog.Default()` | Where the checker logs. |

The default HTTP client honours `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY`,
times out a request after `WithFetchTimeout`, keeps at most 16 idle
connections (2 per host) for 90 s, and follows at most 3 redirects, to
`http` or `https` URLs only. OCSP and CRL URLs with any other scheme
(`ldap`, `file`) are skipped.

## External dependencies

| Module | Version | Used for |
|---|---|---|
| `golang.org/x/crypto` | v0.59.0 | `ocsp`: parsing responses and building requests. The standard library has no OCSP support. |
| `github.com/ashtonian/mqttv5` | same release | Tests only: the client, test broker and test PKI. |

At runtime a check may reach the OCSP responders and CRL distribution
points named in the broker's certificates, over HTTP: allow egress to
them, or supply CRLs through `WithCRLProvider` and rely on stapling.

## API

| Symbol | Description |
|---|---|
| `New(opts ...Option) (*Checker, error)` | Builds a checker; rejects invalid options. |
| `(*Checker).Verify(ctx, *url.URL, tls.ConnectionState) error` | Fits `mqttv5.WithTLSVerifier`. The URL is used for logging and may be nil. When ctx ends, fails with its error under any policy. |
| `(*Checker).VerifyConnection(tls.ConnectionState) error` | Fits `tls.Config.VerifyConnection`. |
| `(*Checker).Stats() Stats` | Counters since `New`: certificates checked, good, revoked, unknown, allowed unknown, answers by source, cache hits and misses, fetch failures. |
| `Policy`: `FailClosed`, `FailOpen` | See `WithUnknownPolicy`. |
| `Scope`: `FullChain`, `LeafOnly` | See `WithScope`. |
| `CRLProvider` | `func(ctx, issuer *x509.Certificate) (*x509.RevocationList, error)`. |
| `Source` | Where an answer came from: staple, CRL provider, OCSP responder, CRL distribution point. |
| `*RevocationError` | A failed certificate: subject, serial, source, revocation time and reason, and `Err`. |
| `ErrRevoked`, `ErrRevocationUnknown`, `ErrStapleRequired` | What `RevocationError.Err` matches with `errors.Is`, joined with each source's failure for an unknown status. |
| `ErrNoVerifiedChain` | The connection has no verified chain to check (for example, `InsecureSkipVerify`). |

Share one `Checker` between clients: they then share its cache and
in-flight fetches. It is safe for concurrent use and needs no shutdown;
a fetch runs on its own goroutine for at most `WithFetchTimeout`, so a
caller whose context ends does not fail other callers waiting on the
same fetch.

## Build, run, test

```bash
go -C tlsrevocation test -race ./...
```

The tests run against an in-process PKI (`internal/testpki`), an OCSP
responder and CRL servers on `httptest`, and a fake clock. They cover
every source and its fall-through, delegated responders, must-staple,
revoked intermediates, cross-signed chains, CRL scope and unsupported
CRLs, caching, failure backoff, single-flight fetches, cancellation, and,
through `mqttv5.WithTLSVerifier`, a revoked broker receiving no CONNECT.

## Operations

`Stats()` returns counters to export to your metrics system:
`Revoked` and `Unknown` rising, or `FetchFailures` rising with `Unknown`,
are the signals to alert on. The checker logs a warning for every
revocation, every allowed unknown status and every failed fetch, with the
broker URL, certificate subject and serial; cache hits and decisions log
at debug level.

| Symptom | Check | Then |
|---|---|---|
| `Connect` fails with `ErrRevoked` | `RevocationError`: subject, serial, source, reason | The broker serves a revoked certificate: have its operator replace it. |
| `ErrRevocationUnknown`, joined errors name `HTTP 5xx`, timeouts or `failed recently` | egress to the OCSP and CRL URLs in the joined errors; `Stats().FetchFailures` | Restore access, raise `WithFetchTimeout`, or supply CRLs with `WithCRLProvider`. `FailOpen` trades the check for availability. |
| `ErrRevocationUnknown`: `names no OCSP responder or CRL distribution point` | the CA's certificate profile | Use `WithCRLProvider` for a private CA, enable stapling on the broker, or scope to `LeafOnly` when only intermediates lack sources. |
| `ErrRevocationUnknown`: `stale since` / `not valid until` | the host clock; the CA's CRL and OCSP publication | Fix the clock, or have the CA publish on schedule. `WithClockSkew` tolerates small drift. |
| `ErrRevocationUnknown`: `response exceeds the size limit` | the CRL's size | Raise `WithMaxCRLBytes`, or prefer OCSP. |
| `ErrRevocationUnknown`: `delta CRL`, `indirect CRLs`, `partitioned by reason`, `unsupported critical extension` | the CA's CRL profile | These CRLs are not supported; the CA must publish a complete, direct CRL. |
| `ErrStapleRequired` | whether the broker staples (`openssl s_client -status`) | The broker's certificate is must-staple: enable OCSP stapling on the broker. |
| `ErrNoVerifiedChain` | `InsecureSkipVerify`, or a custom verifier that leaves `VerifiedChains` empty | Let crypto/tls verify the chain; revocation needs the issuers it finds. |

## Security

- Revocation checking runs after crypto/tls has verified the chain; it
  never accepts a certificate crypto/tls refused.
- `FailClosed` is the default: an attacker who can block OCSP and CRL
  traffic cannot turn a revoked certificate into an accepted one.
  `FailOpen` gives that up; use it only where availability outweighs it.
- Stapled and fetched responses are authenticated (issuer or authorised
  delegate) and must be current, so an old "good" response cannot be
  replayed past its `NextUpdate`, or past `WithMaxAge` when it has none.
- The revocation status of a delegated OCSP responder's own certificate is
  not checked; RFC 6960 §4.2.2.2.1 lets CAs mark such certificates
  `id-pkix-ocsp-nocheck` for this reason.
- OCSP nonces (RFC 8954) are not sent: most responders serve
  pre-signed responses and ignore them. Freshness comes from the
  response's validity window instead.
- Fetched bodies are capped (`WithMaxOCSPResponseBytes`,
  `WithMaxCRLBytes`), redirects are limited to three, to `http` and
  `https` only.
- Revocation of client certificates is the broker's concern; this module
  checks the broker's chain.

## Code map

| File | What |
|---|---|
| `checker.go` | `Checker`, `New`, options, `Verify`, `VerifyConnection`, the per-certificate decision, errors |
| `ocsp.go` | stapled and fetched OCSP responses, the HTTP client, must-staple |
| `crl.go` | CRL validation, Issuing Distribution Point scope, the provider and distribution points |
| `cache.go` | the expiring LRU cache, single-flight fetches, `Stats` |
| `pki_test.go` | test OCSP responder, CRL server, fixture |
| `checker_test.go` | the source matrix, provider, caching, single-flight, cancellation, cross-signed chains |
| `client_test.go` | through `mqttv5.WithTLSVerifier` and `tls.Config.VerifyConnection` |
