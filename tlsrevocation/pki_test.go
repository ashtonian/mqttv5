package tlsrevocation

import (
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/ashtonian/mqttv5/internal/testpki"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// responder is an OCSP responder for the certificates one CA issued.
// Certificates it has no status for are answered unknown.
type responder struct {
	t      testing.TB
	issuer *testpki.Authority
	srv    *httptest.Server

	requests atomic.Int32
	down     atomic.Bool
	// gate, when set, holds every request until it is closed.
	gate atomic.Pointer[chan struct{}]

	mu       sync.Mutex
	statuses map[string]int
}

func newResponder(t testing.TB) *responder {
	r := &responder{t: t, statuses: make(map[string]int)}
	r.srv = httptest.NewServer(r)
	t.Cleanup(r.srv.Close)
	return r
}

func (r *responder) URL() string { return r.srv.URL }

func (r *responder) set(cert *x509.Certificate, status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses[cert.SerialNumber.String()] = status
}

func (r *responder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.requests.Add(1)
	if gate := r.gate.Load(); gate != nil {
		<-*gate
	}
	if r.down.Load() {
		http.Error(w, "down", http.StatusInternalServerError)
		return
	}
	var der []byte
	var err error
	if req.Method == http.MethodGet {
		var encoded string
		if encoded, err = url.PathUnescape(strings.TrimPrefix(req.URL.EscapedPath(), "/")); err == nil {
			der, err = base64.StdEncoding.DecodeString(encoded)
		}
	} else {
		der, err = io.ReadAll(req.Body)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ocspReq, err := ocsp.ParseRequest(der)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	status, ok := r.statuses[ocspReq.SerialNumber.String()]
	r.mu.Unlock()
	if !ok {
		status = ocsp.Unknown
	}
	w.Header().Set("Content-Type", "application/ocsp-response")
	_, _ = w.Write(ocspResponse(r.t, r.issuer, nil, ocspReq.SerialNumber, status))
}

// ocspResponse is a response from issuer, or from signer when set,
// about serial, valid an hour either side of testpki.Now unless mods
// change it.
func ocspResponse(t testing.TB, issuer *testpki.Authority, signer *tls.Certificate, serial *big.Int, status int, mods ...func(*ocsp.Response)) []byte {
	t.Helper()
	tmpl := ocsp.Response{
		Status:       status,
		SerialNumber: serial,
		ThisUpdate:   testpki.Now.Add(-time.Hour),
		NextUpdate:   testpki.Now.Add(time.Hour),
	}
	if status == ocsp.Revoked {
		tmpl.RevokedAt = testpki.Now.Add(-2 * time.Hour)
		tmpl.RevocationReason = ocsp.KeyCompromise
	}
	for _, mod := range mods {
		mod(&tmpl)
	}
	responderCert, key := issuer.Cert, crypto.Signer(issuer.Key)
	if signer != nil {
		responderCert, key = signer.Leaf, signer.PrivateKey.(crypto.Signer)
		tmpl.Certificate = signer.Leaf // embedded, so the response verifies through it
	}
	der, err := ocsp.CreateResponse(issuer.Cert, responderCert, tmpl, key)
	if err != nil {
		t.Fatalf("ocsp.CreateResponse: %v", err)
	}
	return der
}

// crlServer serves one CRL.
type crlServer struct {
	srv      *httptest.Server
	requests atomic.Int32
	down     atomic.Bool
	body     atomic.Pointer[[]byte]
}

func newCRLServer(t testing.TB) *crlServer {
	s := &crlServer{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		body := s.body.Load()
		if s.down.Load() || body == nil {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/pkix-crl")
		_, _ = w.Write(*body)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *crlServer) URL() string { return s.srv.URL + "/ca.crl" }

func (s *crlServer) serve(der []byte) { s.body.Store(&der) }

// crl is a CRL by issuer listing revoked, valid an hour either side of
// testpki.Now unless mods change it.
func crl(t testing.TB, issuer *testpki.Authority, revoked []*x509.Certificate, mods ...func(*x509.RevocationList)) []byte {
	t.Helper()
	tmpl := &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: testpki.Now.Add(-time.Hour),
		NextUpdate: testpki.Now.Add(time.Hour),
	}
	for _, cert := range revoked {
		tmpl.RevokedCertificateEntries = append(tmpl.RevokedCertificateEntries, x509.RevocationListEntry{
			SerialNumber:   cert.SerialNumber,
			RevocationTime: testpki.Now.Add(-2 * time.Hour),
			ReasonCode:     1, // keyCompromise
		})
	}
	for _, mod := range mods {
		mod(tmpl)
	}
	der, err := x509.CreateRevocationList(rand.Reader, tmpl, issuer.Cert, issuer.Key)
	if err != nil {
		t.Fatalf("x509.CreateRevocationList: %v", err)
	}
	return der
}

// fixture is a root, an intermediate and a broker leaf. The leaf names
// the intermediate's OCSP responder and CRL; the intermediate names the
// root's CRL. Both CRLs start out empty and the responder knows nothing.
type fixture struct {
	root, intermediate *testpki.Authority
	leaf               tls.Certificate
	ocsp               *responder
	leafCRL, rootCRL   *crlServer
	clock              *fakeClock
}

func newFixture(t testing.TB, leafOpts ...testpki.Option) *fixture {
	t.Helper()
	f := &fixture{
		ocsp:    newResponder(t),
		leafCRL: newCRLServer(t),
		rootCRL: newCRLServer(t),
		clock:   &fakeClock{t: testpki.Now},
	}
	f.root = testpki.NewRoot(t, "root")
	f.intermediate = f.root.NewIntermediate(t, "intermediate", testpki.WithCRLDistributionPoints(f.rootCRL.URL()))
	f.ocsp.issuer = f.intermediate
	opts := append([]testpki.Option{
		testpki.WithOCSPServer(f.ocsp.URL()),
		testpki.WithCRLDistributionPoints(f.leafCRL.URL()),
	}, leafOpts...)
	f.leaf = f.intermediate.Server(t, opts...)
	f.leafCRL.serve(crl(t, f.intermediate, nil))
	f.rootCRL.serve(crl(t, f.root, nil))
	return f
}

func (f *fixture) state(staple []byte) tls.ConnectionState {
	return tls.ConnectionState{
		HandshakeComplete: true,
		PeerCertificates:  []*x509.Certificate{f.leaf.Leaf, f.intermediate.Cert},
		VerifiedChains:    [][]*x509.Certificate{{f.leaf.Leaf, f.intermediate.Cert, f.root.Cert}},
		OCSPResponse:      staple,
	}
}

// staple is a response from the intermediate about the leaf.
func (f *fixture) staple(t testing.TB, status int, mods ...func(*ocsp.Response)) []byte {
	t.Helper()
	return ocspResponse(t, f.intermediate, nil, f.leaf.Leaf.SerialNumber, status, mods...)
}

func (f *fixture) checker(t testing.TB, opts ...Option) *Checker {
	t.Helper()
	base := []Option{WithClock(f.clock.now), WithLogger(slog.New(slog.DiscardHandler))}
	c, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}
