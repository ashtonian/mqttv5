// Package testpki issues X.509 certificates in memory for TLS tests: a
// root CA, intermediates under it, and server and client leaves.
//
// Every certificate is valid at the fixed instant [Now]. Tests set
// tls.Config.Time to [Clock] on both peers, so verification never
// depends on the wall clock and an expired certificate is one whose
// validity window ends before [Now].
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Now is the instant every certificate is issued against.
var Now = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// Clock returns [Now]. Use it as tls.Config.Time.
func Clock() time.Time { return Now }

// serials makes every certificate's serial number unique within a test
// binary, so a test can tell two certificates apart by serial.
var serials atomic.Int64

// Authority is a certificate authority that issues certificates.
type Authority struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey

	// chain is the DER of this authority and every intermediate above
	// it, leaf-side first, excluding the root: what a peer presents
	// after its leaf.
	chain [][]byte
}

// NewRoot creates a self-signed root CA named cn.
func NewRoot(t testing.TB, cn string) *Authority {
	t.Helper()
	key := newKey(t)
	tmpl := caTemplate(cn)
	cert := create(t, tmpl, tmpl, &key.PublicKey, key)
	return &Authority{Cert: cert, Key: key}
}

// NewIntermediate creates an intermediate CA named cn, signed by a.
func (a *Authority) NewIntermediate(t testing.TB, cn string) *Authority {
	t.Helper()
	key := newKey(t)
	cert := create(t, caTemplate(cn), a.Cert, &key.PublicKey, a.Key)
	chain := append([][]byte{cert.Raw}, a.chain...)
	return &Authority{Cert: cert, Key: key, chain: chain}
}

// Pool returns a pool holding only a's certificate: trust a as a root.
func (a *Authority) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(a.Cert)
	return p
}

// Option adjusts a leaf certificate before it is signed.
type Option func(*x509.Certificate)

// WithValidity sets the leaf's validity window.
func WithValidity(notBefore, notAfter time.Time) Option {
	return func(c *x509.Certificate) {
		c.NotBefore = notBefore
		c.NotAfter = notAfter
	}
}

// WithDNSNames replaces the leaf's DNS subject alternative names.
func WithDNSNames(names ...string) Option {
	return func(c *x509.Certificate) { c.DNSNames = names }
}

// WithIPs replaces the leaf's IP subject alternative names.
func WithIPs(ips ...net.IP) Option {
	return func(c *x509.Certificate) { c.IPAddresses = ips }
}

// Server issues a server certificate for 127.0.0.1 and localhost.
func (a *Authority) Server(t testing.TB, opts ...Option) tls.Certificate {
	t.Helper()
	tmpl := leafTemplate("server", x509.ExtKeyUsageServerAuth)
	tmpl.DNSNames = []string{"localhost"}
	tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	return a.issue(t, tmpl, opts)
}

// Client issues a client certificate with common name cn.
func (a *Authority) Client(t testing.TB, cn string, opts ...Option) tls.Certificate {
	t.Helper()
	return a.issue(t, leafTemplate(cn, x509.ExtKeyUsageClientAuth), opts)
}

func (a *Authority) issue(t testing.TB, tmpl *x509.Certificate, opts []Option) tls.Certificate {
	t.Helper()
	for _, opt := range opts {
		opt(tmpl)
	}
	key := newKey(t)
	cert := create(t, tmpl, a.Cert, &key.PublicKey, a.Key)
	chain := append([][]byte{cert.Raw}, a.chain...)
	return tls.Certificate{Certificate: chain, PrivateKey: key, Leaf: cert}
}

func caTemplate(cn string) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             Now.Add(-24 * time.Hour),
		NotAfter:              Now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
}

func leafTemplate(cn string, eku x509.ExtKeyUsage) *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: nextSerial(),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    Now.Add(-time.Hour),
		NotAfter:     Now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{eku},
	}
}

func nextSerial() *big.Int { return big.NewInt(serials.Add(1)) }

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("testpki: generate key: %v", err)
	}
	return key
}

func create(t testing.TB, tmpl, parent *x509.Certificate, pub *ecdsa.PublicKey, signer *ecdsa.PrivateKey) *x509.Certificate {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
	if err != nil {
		t.Fatalf("testpki: create %q: %v", tmpl.Subject.CommonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("testpki: parse %q: %v", tmpl.Subject.CommonName, err)
	}
	return cert
}

// VerificationCase is one certificate a broker presents and the outcome
// a client that trusts the matrix's root must reach.
type VerificationCase struct {
	Name string
	Cert tls.Certificate
	// ServerName is the client's tls.Config.ServerName; empty means the
	// URL host, 127.0.0.1.
	ServerName string
	// WantErr reports whether a handshake error is the expected one;
	// nil means the handshake succeeds.
	WantErr func(error) bool
}

// VerificationCases is the verification matrix every TLS transport runs.
// Clients dial 127.0.0.1 trusting only root, with tls.Config.Time set to
// [Clock].
func VerificationCases(t testing.TB) (root *Authority, cases []VerificationCase) {
	t.Helper()
	root = NewRoot(t, "root")
	intermediate := root.NewIntermediate(t, "intermediate")
	untrusted := NewRoot(t, "untrusted root")
	elsewhere := []Option{WithDNSNames("broker.example"), WithIPs()}
	return root, []VerificationCase{
		{
			Name: "chain through an intermediate to a trusted root",
			Cert: intermediate.Server(t),
		},
		{
			Name:    "untrusted root",
			Cert:    untrusted.Server(t),
			WantErr: isUnknownAuthority,
		},
		{
			Name:    "no SAN matches the URL host",
			Cert:    intermediate.Server(t, elsewhere...),
			WantErr: isHostnameMismatch,
		},
		{
			Name:    "expired",
			Cert:    intermediate.Server(t, WithValidity(Now.Add(-48*time.Hour), Now.Add(-24*time.Hour))),
			WantErr: isExpired,
		},
		{
			Name:       "ServerName overrides the URL host",
			Cert:       intermediate.Server(t, elsewhere...),
			ServerName: "broker.example",
		},
	}
}

func isUnknownAuthority(err error) bool {
	var target x509.UnknownAuthorityError
	return errors.As(err, &target)
}

func isHostnameMismatch(err error) bool {
	var target x509.HostnameError
	return errors.As(err, &target)
}

func isExpired(err error) bool {
	var target x509.CertificateInvalidError
	return errors.As(err, &target) && target.Reason == x509.Expired
}
