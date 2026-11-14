package tlsrevocation

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/ashtonian/mqttv5/internal/testpki"
)

func idpExtension(t testing.TB, idp issuingDistributionPoint) pkix.Extension {
	t.Helper()
	value, err := asn1.Marshal(idp)
	if err != nil {
		t.Fatal(err)
	}
	return pkix.Extension{Id: oidIssuingDistPoint, Critical: true, Value: value}
}

// idpNaming is an Issuing Distribution Point naming uri as the CRL's
// distribution point.
func idpNaming(t testing.TB, uri string) issuingDistributionPoint {
	t.Helper()
	general, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: tagURI, Bytes: []byte(uri)})
	if err != nil {
		t.Fatal(err)
	}
	return issuingDistributionPoint{DistributionPoint: asn1.RawValue{
		Class: asn1.ClassContextSpecific, Tag: tagFullName, IsCompound: true, Bytes: general,
	}}
}

func withExtension(ext pkix.Extension) func(*x509.RevocationList) {
	return func(l *x509.RevocationList) { l.ExtraExtensions = append(l.ExtraExtensions, ext) }
}

func TestVerify(t *testing.T) {
	keyCompromise := 1
	tests := []struct {
		name     string
		leafOpts []testpki.Option
		// setup prepares the fixture's sources and returns the staple.
		setup func(t *testing.T, f *fixture) []byte
		opts  []Option
		// wantErr is the sentinel the error matches; nil means the
		// chain passes.
		wantErr    error
		wantSource Source
		// check makes further assertions.
		check func(t *testing.T, f *fixture, c *Checker)
	}{
		{
			name:  "good staple",
			setup: func(t *testing.T, f *fixture) []byte { return f.staple(t, ocsp.Good) },
			opts:  []Option{WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if n := f.ocsp.requests.Load() + f.leafCRL.requests.Load(); n != 0 {
					t.Errorf("%d fetches, want none after a good staple", n)
				}
				if s := c.Stats(); s.FromStaple != 1 || s.Good != 1 {
					t.Errorf("stats = %+v, want one good answer from the staple", s)
				}
			},
		},
		{
			name:       "revoked staple",
			setup:      func(t *testing.T, f *fixture) []byte { return f.staple(t, ocsp.Revoked) },
			wantErr:    ErrRevoked,
			wantSource: SourceStaple,
		},
		{
			name: "expired staple falls through to the responder",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.set(f.leaf.Leaf, ocsp.Good)
				return f.staple(t, ocsp.Revoked, func(r *ocsp.Response) {
					r.ThisUpdate = testpki.Now.Add(-48 * time.Hour)
					r.NextUpdate = testpki.Now.Add(-24 * time.Hour)
				})
			},
			opts: []Option{WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if n := f.ocsp.requests.Load(); n != 1 {
					t.Errorf("responder asked %d times, want 1", n)
				}
			},
		},
		{
			name: "staple signed by an unrelated CA falls through",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.set(f.leaf.Leaf, ocsp.Good)
				other := testpki.NewRoot(t, "other")
				return ocspResponse(t, other, nil, f.leaf.Leaf.SerialNumber, ocsp.Revoked)
			},
			opts: []Option{WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if s := c.Stats(); s.FromOCSP != 1 {
					t.Errorf("stats = %+v, want the responder's answer", s)
				}
			},
		},
		{
			// A broker can staple the issuer's genuine response about
			// another of its certificates; EMQX 6.3.1 does when two
			// stapling listeners' certificates share a signature
			// algorithm.
			name: "staple about another certificate falls through",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.set(f.leaf.Leaf, ocsp.Good)
				other := f.intermediate.Leaf(t, "another broker")
				return ocspResponse(t, f.intermediate, nil, other.Leaf.SerialNumber, ocsp.Revoked)
			},
			opts: []Option{WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if s := c.Stats(); s.FromOCSP != 1 || s.FromStaple != 0 {
					t.Errorf("stats = %+v, want the responder's answer", s)
				}
			},
		},
		{
			name:     "must-staple leaf with a staple about another certificate",
			leafOpts: []testpki.Option{testpki.WithMustStaple()},
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.set(f.leaf.Leaf, ocsp.Good)
				other := f.intermediate.Leaf(t, "another broker")
				return ocspResponse(t, f.intermediate, nil, other.Leaf.SerialNumber, ocsp.Good)
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrStapleRequired,
		},
		{
			name: "staple from a delegated responder without id-kp-OCSPSigning falls through",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.set(f.leaf.Leaf, ocsp.Good)
				delegate := f.intermediate.Leaf(t, "responder")
				return ocspResponse(t, f.intermediate, &delegate, f.leaf.Leaf.SerialNumber, ocsp.Revoked)
			},
			opts: []Option{WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if s := c.Stats(); s.FromOCSP != 1 || s.FromStaple != 0 {
					t.Errorf("stats = %+v, want the staple ignored and the responder's answer used", s)
				}
			},
		},
		{
			name: "staple from an authorised delegated responder",
			setup: func(t *testing.T, f *fixture) []byte {
				delegate := f.intermediate.Leaf(t, "responder", testpki.WithExtKeyUsage(x509.ExtKeyUsageOCSPSigning))
				return ocspResponse(t, f.intermediate, &delegate, f.leaf.Leaf.SerialNumber, ocsp.Revoked)
			},
			wantErr:    ErrRevoked,
			wantSource: SourceStaple,
		},
		{
			name:  "no staple, responder good",
			setup: func(t *testing.T, f *fixture) []byte { f.ocsp.set(f.leaf.Leaf, ocsp.Good); return nil },
			check: func(t *testing.T, f *fixture, c *Checker) {
				if s := c.Stats(); s.FromOCSP != 1 || s.FromCRL != 1 {
					t.Errorf("stats = %+v, want the leaf from OCSP and the intermediate from the root's CRL", s)
				}
			},
		},
		{
			name:       "no staple, responder revoked",
			setup:      func(t *testing.T, f *fixture) []byte { f.ocsp.set(f.leaf.Leaf, ocsp.Revoked); return nil },
			wantErr:    ErrRevoked,
			wantSource: SourceOCSP,
		},
		{
			name: "responder down, CRL lists the serial",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.serve(crl(t, f.intermediate, []*x509.Certificate{f.leaf.Leaf}))
				return nil
			},
			wantErr:    ErrRevoked,
			wantSource: SourceCRL,
		},
		{
			name: "every source down fails closed",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.down.Store(true)
				return nil
			},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "every source down, FailOpen allows",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.down.Store(true)
				return nil
			},
			opts: []Option{WithUnknownPolicy(FailOpen), WithScope(LeafOnly)},
			check: func(t *testing.T, f *fixture, c *Checker) {
				if s := c.Stats(); s.AllowedUnknown != 1 || s.FetchFailures != 2 {
					t.Errorf("stats = %+v, want one allowed unknown after two failed fetches", s)
				}
			},
		},
		{
			name:     "must-staple leaf without a staple",
			leafOpts: []testpki.Option{testpki.WithMustStaple()},
			setup:    func(t *testing.T, f *fixture) []byte { f.ocsp.set(f.leaf.Leaf, ocsp.Good); return nil },
			opts:     []Option{WithUnknownPolicy(FailOpen)},
			wantErr:  ErrStapleRequired,
		},
		{
			name:     "must-staple leaf with a good staple",
			leafOpts: []testpki.Option{testpki.WithMustStaple()},
			setup:    func(t *testing.T, f *fixture) []byte { return f.staple(t, ocsp.Good) },
			opts:     []Option{WithScope(LeafOnly)},
		},
		{
			name: "intermediate revoked in the root's CRL",
			setup: func(t *testing.T, f *fixture) []byte {
				f.rootCRL.serve(crl(t, f.root, []*x509.Certificate{f.intermediate.Cert}))
				return f.staple(t, ocsp.Good)
			},
			wantErr:    ErrRevoked,
			wantSource: SourceCRL,
		},
		{
			name: "intermediate revoked, leaf only",
			setup: func(t *testing.T, f *fixture) []byte {
				f.rootCRL.serve(crl(t, f.root, []*x509.Certificate{f.intermediate.Cert}))
				return f.staple(t, ocsp.Good)
			},
			opts: []Option{WithScope(LeafOnly)},
		},
		{
			name: "CRL from another CA at the leaf's distribution point",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.serve(crl(t, f.root, nil))
				return nil
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "delta CRL is not complete",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				delta, _ := asn1.Marshal(1)
				f.leafCRL.serve(crl(t, f.intermediate, nil, withExtension(pkix.Extension{Id: oidDeltaCRLIndicator, Critical: true, Value: delta})))
				return nil
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "CRL with an unknown critical extension",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				value, _ := asn1.Marshal(true)
				f.leafCRL.serve(crl(t, f.intermediate, nil, withExtension(pkix.Extension{Id: asn1.ObjectIdentifier{1, 2, 3, 4}, Critical: true, Value: value})))
				return nil
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "CRL scoped to CA certificates does not vouch for the leaf",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.serve(crl(t, f.intermediate, nil, withExtension(idpExtension(t, issuingDistributionPoint{OnlyContainsCACerts: true}))))
				return nil
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "CRL naming another distribution point",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.serve(crl(t, f.intermediate, nil, withExtension(idpExtension(t, idpNaming(t, "http://elsewhere.example/ca.crl")))))
				return nil
			},
			opts:    []Option{WithScope(LeafOnly)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name: "CRL naming the distribution point it came from",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				f.leafCRL.serve(crl(t, f.intermediate, []*x509.Certificate{f.leaf.Leaf}, withExtension(idpExtension(t, idpNaming(t, f.leafCRL.URL())))))
				return nil
			},
			opts:       []Option{WithScope(LeafOnly)},
			wantErr:    ErrRevoked,
			wantSource: SourceCRL,
		},
		{
			name: "oversize CRL",
			setup: func(t *testing.T, f *fixture) []byte {
				f.ocsp.down.Store(true)
				return nil
			},
			opts:    []Option{WithScope(LeafOnly), WithMaxCRLBytes(16)},
			wantErr: ErrRevocationUnknown,
		},
		{
			name:     "certificate without revocation sources",
			leafOpts: []testpki.Option{testpki.WithOCSPServer(), testpki.WithCRLDistributionPoints()},
			setup:    func(*testing.T, *fixture) []byte { return nil },
			opts:     []Option{WithScope(LeafOnly)},
			wantErr:  ErrRevocationUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, tt.leafOpts...)
			staple := tt.setup(t, f)
			c := f.checker(t, tt.opts...)
			err := c.Verify(context.Background(), nil, f.state(staple))
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Verify: %v", err)
				}
			} else {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Verify error = %v, want %v", err, tt.wantErr)
				}
				var rerr *RevocationError
				if !errors.As(err, &rerr) {
					t.Fatalf("Verify error %T is not a *RevocationError", err)
				}
				if rerr.Source != tt.wantSource {
					t.Fatalf("Source = %v, want %v", rerr.Source, tt.wantSource)
				}
				if tt.wantErr == ErrRevoked && rerr.Reason != keyCompromise {
					t.Fatalf("Reason = %d, want keyCompromise (%d)", rerr.Reason, keyCompromise)
				}
			}
			if tt.check != nil {
				tt.check(t, f, c)
			}
		})
	}
}

// A CRL from the provider decides without a fetch, and its revocation
// wins over a good staple.
func TestVerifyCRLProvider(t *testing.T) {
	for _, tt := range []struct {
		name    string
		revoked bool
		staple  bool
		wantErr error
	}{
		{name: "vouches", wantErr: nil},
		{name: "lists the serial", revoked: true, wantErr: ErrRevoked},
		{name: "lists the serial despite a good staple", revoked: true, staple: true, wantErr: ErrRevoked},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, testpki.WithOCSPServer(), testpki.WithCRLDistributionPoints())
			var listed []*x509.Certificate
			if tt.revoked {
				listed = append(listed, f.leaf.Leaf)
			}
			list, err := x509.ParseRevocationList(crl(t, f.intermediate, listed))
			if err != nil {
				t.Fatal(err)
			}
			provider := func(_ context.Context, issuer *x509.Certificate) (*x509.RevocationList, error) {
				if issuer.Equal(f.intermediate.Cert) {
					return list, nil
				}
				return nil, nil
			}
			var staple []byte
			if tt.staple {
				staple = f.staple(t, ocsp.Good)
			}
			c := f.checker(t, WithCRLProvider(provider), WithScope(LeafOnly))
			err = c.Verify(context.Background(), nil, f.state(staple))
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("Verify error = %v, want %v", err, tt.wantErr)
			}
			if n := f.ocsp.requests.Load() + f.leafCRL.requests.Load(); n != 0 {
				t.Fatalf("%d fetches, want none", n)
			}
		})
	}
}

// Answers are cached until their NextUpdate; a failed URL is skipped
// while its backoff lasts.
func TestVerifyCaching(t *testing.T) {
	f := newFixture(t)
	f.ocsp.set(f.leaf.Leaf, ocsp.Good)
	c := f.checker(t, WithScope(LeafOnly))
	state := f.state(nil)
	for range 3 {
		if err := c.Verify(context.Background(), nil, state); err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
	if n := f.ocsp.requests.Load(); n != 1 {
		t.Fatalf("responder asked %d times before NextUpdate, want 1", n)
	}
	f.clock.advance(time.Hour + DefaultClockSkew + time.Second)
	f.ocsp.down.Store(true)
	f.leafCRL.down.Store(true)
	for range 2 {
		if err := c.Verify(context.Background(), nil, state); !errors.Is(err, ErrRevocationUnknown) {
			t.Fatalf("Verify after NextUpdate with every source down = %v, want ErrRevocationUnknown", err)
		}
	}
	if n := f.ocsp.requests.Load(); n != 2 {
		t.Fatalf("responder asked %d times, want 2: one refetch after NextUpdate, then backoff", n)
	}
	if s := c.Stats(); s.CacheHits != 2 {
		t.Fatalf("CacheHits = %d, want 2", s.CacheHits)
	}
}

// Concurrent checks of one certificate share one fetch.
func TestVerifySingleFlight(t *testing.T) {
	f := newFixture(t)
	f.ocsp.set(f.leaf.Leaf, ocsp.Good)
	gate := make(chan struct{})
	f.ocsp.gate.Store(&gate)
	c := f.checker(t, WithScope(LeafOnly))
	state := f.state(nil)

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Go(func() { errs <- c.Verify(context.Background(), nil, state) })
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.ocsp.requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
	if n := f.ocsp.requests.Load(); n != 1 {
		t.Fatalf("responder asked %d times by %d concurrent checks, want 1", n, callers)
	}
}

// A check whose context ends stops waiting and fails with the
// context's error, even under FailOpen.
func TestVerifyContextCancelled(t *testing.T) {
	f := newFixture(t)
	gate := make(chan struct{})
	f.ocsp.gate.Store(&gate)
	t.Cleanup(func() { close(gate) })
	c := f.checker(t, WithUnknownPolicy(FailOpen))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Verify(ctx, nil, f.state(nil)) }()
	deadline := time.Now().Add(3 * time.Second)
	for f.ocsp.requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Verify error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Verify did not return after its context was cancelled")
	}
}

// A chain through a revoked intermediate fails, but the connection
// passes when another verified chain, through a cross-signed copy of
// that intermediate, is not revoked.
func TestVerifyTriesEveryChain(t *testing.T) {
	f := newFixture(t)
	f.rootCRL.serve(crl(t, f.root, []*x509.Certificate{f.intermediate.Cert}))
	otherRoot := testpki.NewRoot(t, "other root")
	otherCRL := newCRLServer(t)
	otherCRL.serve(crl(t, otherRoot, nil))
	crossSigned := f.intermediate.CrossSigned(t, otherRoot, testpki.WithCRLDistributionPoints(otherCRL.URL()))
	staple := f.staple(t, ocsp.Good)
	revokedChain := []*x509.Certificate{f.leaf.Leaf, f.intermediate.Cert, f.root.Cert}
	goodChain := []*x509.Certificate{f.leaf.Leaf, crossSigned.Cert, otherRoot.Cert}
	c := f.checker(t)

	state := tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{revokedChain}, OCSPResponse: staple}
	if err := c.Verify(context.Background(), nil, state); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Verify with only the revoked chain = %v, want ErrRevoked", err)
	}
	state.VerifiedChains = [][]*x509.Certificate{revokedChain, goodChain}
	if err := c.Verify(context.Background(), nil, state); err != nil {
		t.Fatalf("Verify with a good second chain: %v", err)
	}
}

func TestVerifyWithoutVerifiedChain(t *testing.T) {
	f := newFixture(t)
	c := f.checker(t, WithUnknownPolicy(FailOpen))
	state := f.state(nil)
	state.VerifiedChains = nil
	if err := c.Verify(context.Background(), nil, state); !errors.Is(err, ErrNoVerifiedChain) {
		t.Fatalf("Verify = %v, want ErrNoVerifiedChain", err)
	}
}

func TestNewRejectsInvalidOptions(t *testing.T) {
	for name, opt := range map[string]Option{
		"policy":        WithUnknownPolicy(Policy(9)),
		"scope":         WithScope(Scope(9)),
		"nil provider":  WithCRLProvider(nil),
		"nil client":    WithHTTPClient(nil),
		"fetch timeout": WithFetchTimeout(0),
		"verify":        WithVerifyTimeout(-time.Second),
		"skew":          WithClockSkew(-time.Second),
		"max age":       WithMaxAge(0),
		"backoff":       WithFailureBackoff(-time.Second),
		"ocsp bytes":    WithMaxOCSPResponseBytes(0),
		"crl bytes":     WithMaxCRLBytes(-1),
		"cache":         WithCacheEntries(-1, 0),
		"store":         WithStore(nil),
		"clock":         WithClock(nil),
		"logger":        WithLogger(nil),
	} {
		if _, err := New(opt); err == nil {
			t.Errorf("%s: New accepted an invalid option", name)
		}
	}
}
