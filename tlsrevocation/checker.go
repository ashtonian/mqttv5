// Package tlsrevocation checks the certificates of a verified TLS chain
// for revocation, which crypto/tls does not: a stapled OCSP response,
// CRLs supplied by the caller, OCSP responders, and CRL distribution
// points.
//
// A [Checker] plugs into the MQTT client through
// mqttv5.WithTLSVerifier, which runs it after the TLS handshake and
// before CONNECT, under the connection attempt's context:
//
//	checker, err := tlsrevocation.New()
//	// ...
//	cli, err := mqttv5.New(
//	    mqttv5.WithBroker("mqtts://broker.example:8883"),
//	    mqttv5.WithTLSVerifier(checker.Verify),
//	)
//
// [Checker.VerifyConnection] fits tls.Config.VerifyConnection for other
// TLS clients.
//
// Every certificate of the verified chain except the root is checked
// (see [WithScope]). For each, local evidence comes first: the stapled
// OCSP response (leaf only) and the [CRLProvider]'s CRL. Either one
// reporting the certificate revoked fails the check; either one
// vouching for it ends the check. Otherwise the certificate's OCSP
// responders and then its CRL distribution points are consulted in
// order, and the first definitive answer decides. When nothing answers,
// the status is unknown and [WithUnknownPolicy] decides; by default the
// check fails.
package tlsrevocation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"time"
)

// Defaults for the options of the same names.
const (
	DefaultFetchTimeout         = 5 * time.Second
	DefaultVerifyTimeout        = 10 * time.Second
	DefaultClockSkew            = 5 * time.Minute
	DefaultMaxAge               = 24 * time.Hour
	DefaultFailureBackoff       = 10 * time.Second
	DefaultMaxOCSPResponseBytes = 64 << 10
	DefaultMaxCRLBytes          = 16 << 20
	DefaultOCSPCacheEntries     = 1024
	DefaultCRLCacheEntries      = 8
)

var (
	// ErrRevoked reports a revoked certificate.
	ErrRevoked = errors.New("tlsrevocation: certificate revoked")

	// ErrRevocationUnknown reports that no source could establish a
	// certificate's status.
	ErrRevocationUnknown = errors.New("tlsrevocation: revocation status unknown")

	// ErrStapleRequired reports a certificate that demands a stapled
	// OCSP response (RFC 7633 must-staple) served without a valid one.
	ErrStapleRequired = errors.New("tlsrevocation: certificate requires a stapled OCSP response")

	// ErrNoVerifiedChain reports a connection with no verified chain to
	// check, as with InsecureSkipVerify: issuers cannot be identified.
	ErrNoVerifiedChain = errors.New("tlsrevocation: the connection has no verified certificate chain")
)

// Source is where a certificate's status came from.
type Source int

// Sources, in the order they are consulted.
const (
	SourceNone Source = iota
	SourceStaple
	SourceCRLProvider
	SourceOCSP
	SourceCRL
)

func (s Source) String() string {
	switch s {
	case SourceStaple:
		return "stapled OCSP response"
	case SourceCRLProvider:
		return "CRL provider"
	case SourceOCSP:
		return "OCSP responder"
	case SourceCRL:
		return "CRL distribution point"
	default:
		return "none"
	}
}

// RevocationError reports a certificate that failed the check.
type RevocationError struct {
	// Subject and Serial identify the certificate.
	Subject string
	Serial  *big.Int

	// Source decided a revocation; SourceNone when the status is unknown.
	Source Source

	// RevokedAt and Reason (an RFC 5280 CRLReason code) describe a
	// revocation.
	RevokedAt time.Time
	Reason    int

	// Err is ErrRevoked, ErrRevocationUnknown or ErrStapleRequired,
	// joined with what each source reported.
	Err error
}

func (e *RevocationError) Error() string {
	cert := fmt.Sprintf("certificate %q (serial %s)", e.Subject, serialText(e.Serial))
	if errors.Is(e.Err, ErrRevoked) {
		return fmt.Sprintf("tlsrevocation: %s revoked at %s (reason %d), per the %s",
			cert, e.RevokedAt.UTC().Format(time.RFC3339), e.Reason, e.Source)
	}
	return fmt.Sprintf("tlsrevocation: %s: %v", cert, e.Err)
}

func (e *RevocationError) Unwrap() error { return e.Err }

// Policy decides a certificate whose status is unknown.
type Policy int

const (
	// FailClosed fails the check (the default).
	FailClosed Policy = iota
	// FailOpen allows the certificate, logs a warning and counts it in
	// [Stats].AllowedUnknown.
	FailOpen
)

// Scope selects the certificates checked.
type Scope int

const (
	// FullChain checks every certificate of the verified chain except
	// the root (the default).
	FullChain Scope = iota
	// LeafOnly checks the broker's own certificate.
	LeafOnly
)

// CRLProvider returns the current CRL issued by issuer, supplied out of
// band (a file, a secret store, a configuration push). A nil list with a
// nil error means it has none for issuer. Its CRLs are not cached: it is
// called on every check, so a newly pushed revocation applies at once.
type CRLProvider func(ctx context.Context, issuer *x509.Certificate) (*x509.RevocationList, error)

type config struct {
	unknown          Policy
	scope            Scope
	provider         CRLProvider
	httpClient       *http.Client
	fetchTimeout     time.Duration
	verifyTimeout    time.Duration
	skew             time.Duration
	maxAge           time.Duration
	failureBackoff   time.Duration
	maxOCSPBytes     int64
	maxCRLBytes      int64
	ocspCacheEntries int
	crlCacheEntries  int
	now              func() time.Time
	logger           *slog.Logger
}

// Option configures a [Checker].
type Option func(*config) error

// WithUnknownPolicy decides certificates whose status no source could
// establish. Default [FailClosed].
func WithUnknownPolicy(p Policy) Option {
	return func(c *config) error {
		if p != FailClosed && p != FailOpen {
			return fmt.Errorf("tlsrevocation: invalid Policy %d", p)
		}
		c.unknown = p
		return nil
	}
}

// WithScope selects the certificates checked. Default [FullChain].
func WithScope(s Scope) Option {
	return func(c *config) error {
		if s != FullChain && s != LeafOnly {
			return fmt.Errorf("tlsrevocation: invalid Scope %d", s)
		}
		c.scope = s
		return nil
	}
}

// WithCRLProvider supplies CRLs out of band, for a private CA that
// publishes no distribution point. Nil rejected.
func WithCRLProvider(p CRLProvider) Option {
	return func(c *config) error {
		if p == nil {
			return errors.New("tlsrevocation: WithCRLProvider(nil) is invalid")
		}
		c.provider = p
		return nil
	}
}

// WithHTTPClient replaces the client that fetches OCSP responses and
// CRLs. Each fetch is still bounded by [WithFetchTimeout]. Nil rejected.
func WithHTTPClient(h *http.Client) Option {
	return func(c *config) error {
		if h == nil {
			return errors.New("tlsrevocation: WithHTTPClient(nil) is invalid")
		}
		c.httpClient = h
		return nil
	}
}

// WithFetchTimeout bounds one OCSP or CRL fetch. Default
// [DefaultFetchTimeout].
func WithFetchTimeout(d time.Duration) Option {
	return positive("WithFetchTimeout", d, func(c *config) { c.fetchTimeout = d })
}

// WithVerifyTimeout bounds one [Checker.VerifyConnection] call, which
// has no context of its own. Default [DefaultVerifyTimeout].
func WithVerifyTimeout(d time.Duration) Option {
	return positive("WithVerifyTimeout", d, func(c *config) { c.verifyTimeout = d })
}

// WithClockSkew is the tolerance applied to the validity windows of
// OCSP responses, CRLs and delegated OCSP responder certificates.
// Default [DefaultClockSkew].
func WithClockSkew(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("tlsrevocation: WithClockSkew(%v): negative", d)
		}
		c.skew = d
		return nil
	}
}

// WithMaxAge is how long an OCSP response or CRL without a NextUpdate
// is accepted after its ThisUpdate. Default [DefaultMaxAge].
func WithMaxAge(d time.Duration) Option {
	return positive("WithMaxAge", d, func(c *config) { c.maxAge = d })
}

// WithFailureBackoff is how long a URL whose fetch failed is skipped
// before it is tried again; zero retries on every check. Default
// [DefaultFailureBackoff].
func WithFailureBackoff(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("tlsrevocation: WithFailureBackoff(%v): negative", d)
		}
		c.failureBackoff = d
		return nil
	}
}

// WithMaxOCSPResponseBytes caps a fetched OCSP response; a larger one
// is not an answer. Default [DefaultMaxOCSPResponseBytes].
func WithMaxOCSPResponseBytes(n int64) Option {
	return positive("WithMaxOCSPResponseBytes", n, func(c *config) { c.maxOCSPBytes = n })
}

// WithMaxCRLBytes caps a fetched CRL; a larger one is not an answer.
// Default [DefaultMaxCRLBytes].
func WithMaxCRLBytes(n int64) Option {
	return positive("WithMaxCRLBytes", n, func(c *config) { c.maxCRLBytes = n })
}

// WithCacheEntries bounds the cached OCSP answers (one per certificate)
// and fetched CRLs (one per issuer and URL). Entries expire at their
// NextUpdate; the least recently used is evicted first. Zero disables
// that cache. Defaults [DefaultOCSPCacheEntries] and
// [DefaultCRLCacheEntries].
func WithCacheEntries(ocsp, crls int) Option {
	return func(c *config) error {
		if ocsp < 0 || crls < 0 {
			return fmt.Errorf("tlsrevocation: WithCacheEntries(%d, %d): negative", ocsp, crls)
		}
		c.ocspCacheEntries, c.crlCacheEntries = ocsp, crls
		return nil
	}
}

// WithClock replaces the time source used for validity checks and
// cache expiry. Default time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return errors.New("tlsrevocation: WithClock(nil) is invalid")
		}
		c.now = now
		return nil
	}
}

// WithLogger sets the logger. Default slog.Default.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		if l == nil {
			return errors.New("tlsrevocation: WithLogger(nil) is invalid")
		}
		c.logger = l
		return nil
	}
}

func positive[T int64 | time.Duration](name string, v T, set func(*config)) Option {
	return func(c *config) error {
		if v <= 0 {
			return fmt.Errorf("tlsrevocation: %s(%v): must be positive", name, v)
		}
		set(c)
		return nil
	}
}

// Checker checks certificate chains for revocation. It is safe for
// concurrent use; share one across clients so they share its cache.
type Checker struct {
	cfg        config
	httpClient *http.Client

	ocspAnswers *cache[answer]
	crls        *cache[*crlEntry]
	failures    *cache[error]
	ocspFetches flight[answer]
	crlFetches  flight[*crlEntry]

	stats counters
}

// New builds a Checker.
func New(opts ...Option) (*Checker, error) {
	cfg := config{
		fetchTimeout:     DefaultFetchTimeout,
		verifyTimeout:    DefaultVerifyTimeout,
		skew:             DefaultClockSkew,
		maxAge:           DefaultMaxAge,
		failureBackoff:   DefaultFailureBackoff,
		maxOCSPBytes:     DefaultMaxOCSPResponseBytes,
		maxCRLBytes:      DefaultMaxCRLBytes,
		ocspCacheEntries: DefaultOCSPCacheEntries,
		crlCacheEntries:  DefaultCRLCacheEntries,
		now:              time.Now,
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	hc := cfg.httpClient
	if hc == nil {
		hc = defaultHTTPClient(cfg.fetchTimeout)
	}
	return &Checker{
		cfg:         cfg,
		httpClient:  hc,
		ocspAnswers: newCache[answer](cfg.ocspCacheEntries),
		crls:        newCache[*crlEntry](cfg.crlCacheEntries),
		failures:    newCache[error](failureEntries),
	}, nil
}

// failureEntries bounds the URLs remembered as recently failed.
const failureEntries = 256

// Verify checks the verified chains of state; it fits
// mqttv5.WithTLSVerifier. The check passes when every checked
// certificate of one verified chain is not revoked; chains are tried in
// order, and the first chain's failure is returned when none passes.
// brokerURL is used for logging and may be nil. ctx bounds every fetch;
// when it ends the check fails with its error, whatever the policy.
func (c *Checker) Verify(ctx context.Context, brokerURL *url.URL, state tls.ConnectionState) error {
	if len(state.VerifiedChains) == 0 {
		return ErrNoVerifiedChain
	}
	broker := ""
	if brokerURL != nil {
		broker = brokerURL.Redacted()
	}
	var first error
	for _, chain := range state.VerifiedChains {
		err := c.verifyChain(ctx, broker, chain, state.OCSPResponse)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if first == nil {
			first = err
		}
	}
	return first
}

// VerifyConnection is [Checker.Verify] for tls.Config.VerifyConnection,
// bounded by [WithVerifyTimeout].
func (c *Checker) VerifyConnection(state tls.ConnectionState) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.verifyTimeout)
	defer cancel()
	return c.Verify(ctx, nil, state)
}

func (c *Checker) verifyChain(ctx context.Context, broker string, chain []*x509.Certificate, staple []byte) error {
	// The last certificate is the trust anchor: trusted, not checked.
	checked := len(chain) - 1
	if c.cfg.scope == LeafOnly {
		checked = min(checked, 1)
	}
	for i := range checked {
		var leafStaple []byte
		if i == 0 {
			leafStaple = staple
		}
		if err := c.checkCert(ctx, broker, chain[i], chain[i+1], i == 0, leafStaple); err != nil {
			return err
		}
	}
	return nil
}

// checkCert establishes cert's status, issued by issuer.
func (c *Checker) checkCert(ctx context.Context, broker string, cert, issuer *x509.Certificate, leaf bool, staple []byte) error {
	c.stats.checked.Add(1)
	log := c.cfg.logger.With(
		slog.String("broker", broker),
		slog.String("subject", cert.Subject.String()),
		slog.String("serial", serialText(cert.SerialNumber)),
	)
	var failures []error

	// Local evidence: free to consult, so both sources are, and a
	// revocation either reports wins over the other's vouching.
	var local []answer
	stapleValid := false
	if leaf && len(staple) > 0 {
		a, err := c.parseOCSP(staple, cert, issuer)
		switch {
		case err != nil:
			failures = append(failures, fmt.Errorf("%s: %w", SourceStaple, err))
		case a.status == statusUnknown:
			failures = append(failures, fmt.Errorf("%s: the responder does not know the certificate", SourceStaple))
		default:
			a.source = SourceStaple
			local = append(local, a)
			stapleValid = true
		}
	}
	if c.cfg.provider != nil {
		a, err := c.providerCRL(ctx, cert, issuer)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", SourceCRLProvider, err))
		} else if a.status != statusUnknown {
			local = append(local, a)
		}
	}
	for _, a := range local {
		if a.status == statusRevoked {
			return c.revoked(log, cert, a)
		}
	}
	if leaf && !stapleValid && mustStaple(cert) {
		c.stats.unknown.Add(1)
		return &RevocationError{
			Subject: cert.Subject.String(),
			Serial:  cert.SerialNumber,
			Err:     errors.Join(append([]error{ErrStapleRequired}, failures...)...),
		}
	}
	if len(local) > 0 {
		return c.good(log, local[0])
	}

	for _, server := range cert.OCSPServer {
		if err := ctx.Err(); err != nil {
			return err
		}
		a, err := c.fetchedOCSP(ctx, log, server, cert, issuer)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s %s: %w", SourceOCSP, server, err))
			continue
		}
		if a.status == statusUnknown {
			failures = append(failures, fmt.Errorf("%s %s: the responder does not know the certificate", SourceOCSP, server))
			continue
		}
		return c.decide(log, cert, a)
	}
	for _, point := range cert.CRLDistributionPoints {
		if err := ctx.Err(); err != nil {
			return err
		}
		a, err := c.fetchedCRL(ctx, log, point, cert, issuer)
		if err != nil {
			failures = append(failures, fmt.Errorf("%s %s: %w", SourceCRL, point, err))
			continue
		}
		return c.decide(log, cert, a)
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	c.stats.unknown.Add(1)
	if len(failures) == 0 {
		failures = append(failures, errors.New("the certificate names no OCSP responder or CRL distribution point"))
	}
	reasons := errors.Join(failures...)
	if c.cfg.unknown == FailOpen {
		c.stats.allowedUnknown.Add(1)
		log.Warn("tlsrevocation: revocation status unknown; allowed by FailOpen", slog.Any("error", reasons))
		return nil
	}
	return &RevocationError{
		Subject: cert.Subject.String(),
		Serial:  cert.SerialNumber,
		Err:     errors.Join(ErrRevocationUnknown, reasons),
	}
}

func (c *Checker) decide(log *slog.Logger, cert *x509.Certificate, a answer) error {
	if a.status == statusRevoked {
		return c.revoked(log, cert, a)
	}
	return c.good(log, a)
}

func (c *Checker) good(log *slog.Logger, a answer) error {
	c.stats.good.Add(1)
	c.stats.bySource(a.source)
	log.Debug("tlsrevocation: not revoked", slog.String("source", a.source.String()))
	return nil
}

func (c *Checker) revoked(log *slog.Logger, cert *x509.Certificate, a answer) error {
	c.stats.revoked.Add(1)
	c.stats.bySource(a.source)
	err := &RevocationError{
		Subject:   cert.Subject.String(),
		Serial:    cert.SerialNumber,
		Source:    a.source,
		RevokedAt: a.revokedAt,
		Reason:    a.reason,
		Err:       ErrRevoked,
	}
	log.Warn("tlsrevocation: certificate revoked",
		slog.String("source", a.source.String()),
		slog.Time("revoked_at", a.revokedAt),
		slog.Int("reason", a.reason))
	return err
}

// status is a certificate's revocation status per one source.
type status int

const (
	statusUnknown status = iota
	statusGood
	statusRevoked
)

// answer is one source's verdict on one certificate.
type answer struct {
	status    status
	source    Source
	revokedAt time.Time
	reason    int
	// expires is when the evidence behind the answer goes stale.
	expires time.Time
}

// fresh checks a thisUpdate/nextUpdate window against the clock, and
// returns when the evidence goes stale.
func (c *Checker) fresh(thisUpdate, nextUpdate time.Time) (time.Time, error) {
	now := c.cfg.now()
	if thisUpdate.After(now.Add(c.cfg.skew)) {
		return time.Time{}, fmt.Errorf("not valid until %s", thisUpdate.UTC().Format(time.RFC3339))
	}
	expires := nextUpdate
	if expires.IsZero() {
		expires = thisUpdate.Add(c.cfg.maxAge)
	}
	if now.Add(-c.cfg.skew).After(expires) {
		return time.Time{}, fmt.Errorf("stale since %s", expires.UTC().Format(time.RFC3339))
	}
	return expires, nil
}

// backingOff reports the recent failure of url, while its backoff lasts.
func (c *Checker) backingOff(url string) error {
	err, ok := c.failures.get(url, c.cfg.now())
	if !ok {
		return nil
	}
	return fmt.Errorf("failed recently, retrying after backoff: %w", err)
}

func (c *Checker) failed(log *slog.Logger, url string, err error) {
	c.stats.fetchFailures.Add(1)
	log.Warn("tlsrevocation: fetch failed", slog.String("url", url), slog.Any("error", err))
	if c.cfg.failureBackoff > 0 {
		c.failures.put(url, err, c.cfg.now().Add(c.cfg.failureBackoff))
	}
}

func serialText(n *big.Int) string {
	if n == nil {
		return "<nil>"
	}
	return n.Text(16)
}
