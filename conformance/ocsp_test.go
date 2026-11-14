//go:build conformance

package conformance

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/tlsrevocation"
)

// ocspPKI is where the ocsp-responder service writes its PKI.
const ocspPKI = "ocsp/pki"

// keyCompromise is the RFC 5280 CRLReason the rig revokes with.
const keyCompromise = 1

// ocspRoots returns the rig's root, and skips the test when the rig has
// not run.
func ocspRoots(t *testing.T) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(filepath.Join(ocspPKI, "root.pem"))
	if err != nil {
		t.Skipf("conformance: no OCSP PKI (%v) — start the ocsp-responder and emqx-ocsp services first", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatalf("%s/root.pem holds no certificate", ocspPKI)
	}
	return pool
}

// TestOCSP_StaplingAndResponders checks tlsrevocation, through
// WithTLSVerifier, against EMQX's OCSP stapling and OpenSSL's OCSP
// responder: a stapled good response satisfies a must-staple
// certificate, a stapled revocation is refused, good and revoked
// answers fetched from the responder (signed by a delegated responder,
// and the CA's own by the root) decide, and a must-staple certificate
// served without a staple is refused.
func TestOCSP_StaplingAndResponders(t *testing.T) {
	roots := ocspRoots(t)
	tests := []struct {
		name string
		port int
		// wantErr is the sentinel Connect's error matches; nil connects.
		wantErr error
		// wantSource is where a revocation came from.
		wantSource tlsrevocation.Source
		// wantStaple and wantOCSP count answers by source: the broker's
		// certificate, then the CA's.
		wantStaple, wantOCSP uint64
	}{
		{name: "stapled good response satisfies must-staple", port: 8883, wantStaple: 1, wantOCSP: 1},
		{name: "stapled revocation", port: 8884, wantErr: tlsrevocation.ErrRevoked, wantSource: tlsrevocation.SourceStaple, wantStaple: 1},
		{name: "fetched good responses", port: 8885, wantOCSP: 2},
		{name: "fetched revocation", port: 8886, wantErr: tlsrevocation.ErrRevoked, wantSource: tlsrevocation.SourceOCSP, wantOCSP: 1},
		{name: "must-staple certificate without a staple", port: 8887, wantErr: tlsrevocation.ErrStapleRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			broker := fmt.Sprintf("mqtts://localhost:%d", tt.port)
			requireBroker(t, broker)
			checker, err := tlsrevocation.New(tlsrevocation.WithLogger(slog.New(slog.DiscardHandler)))
			if err != nil {
				t.Fatalf("tlsrevocation.New: %v", err)
			}
			cli, err := mqttv5.New(
				mqttv5.WithBroker(broker),
				mqttv5.WithClientID(strings.ReplaceAll(t.Name(), "/", "-")+"-"+randSuffix()),
				mqttv5.WithTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}),
				mqttv5.WithTLSVerifier(checker.Verify),
				mqttv5.WithConnectTimeout(10*time.Second),
			)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err = cli.Connect(ctx)
			if err == nil {
				t.Cleanup(func() {
					shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
					defer c()
					_ = cli.Disconnect(shutdown)
				})
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Connect = %v, want %v", err, tt.wantErr)
			}
			if errors.Is(err, tlsrevocation.ErrRevoked) {
				var rerr *tlsrevocation.RevocationError
				if !errors.As(err, &rerr) {
					t.Fatalf("Connect error %v is not a *RevocationError", err)
				}
				if rerr.Source != tt.wantSource || rerr.Reason != keyCompromise {
					t.Fatalf("revoked per %v, reason %d; want per %v, reason %d", rerr.Source, rerr.Reason, tt.wantSource, keyCompromise)
				}
			}
			if s := checker.Stats(); s.FromStaple != tt.wantStaple || s.FromOCSP != tt.wantOCSP || s.FetchFailures != 0 {
				t.Fatalf("Stats = %+v, want %d answers from the staple and %d from the responder, no fetch failures",
					s, tt.wantStaple, tt.wantOCSP)
			}
		})
	}
}
