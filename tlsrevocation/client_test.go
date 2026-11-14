package tlsrevocation

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/internal/testpki"
)

// Through mqttv5.WithTLSVerifier, a broker whose stapled response says
// its certificate is revoked receives no CONNECT, and the client says
// why; a good staple connects.
func TestClientRefusesRevokedBroker(t *testing.T) {
	for _, tt := range []struct {
		name    string
		status  int
		wantErr error
	}{
		{name: "good", status: ocsp.Good},
		{name: "revoked", status: ocsp.Revoked, wantErr: ErrRevoked},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, testpki.WithOCSPServer(), testpki.WithCRLDistributionPoints())
			served := f.leaf
			served.OCSPStaple = f.staple(t, tt.status)
			b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{served}})
			checker := f.checker(t, WithScope(LeafOnly))
			cli, err := mqttv5.New(
				mqttv5.WithBroker(b.URL()),
				mqttv5.WithClientID("tlsrevocation-"+tt.name),
				mqttv5.WithCredentials("user", []byte("secret")),
				mqttv5.WithTLSConfig(&tls.Config{RootCAs: f.root.Pool(), Time: testpki.Clock}),
				mqttv5.WithTLSVerifier(checker.Verify),
			)
			if err != nil {
				t.Fatalf("mqttv5.New: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err = cli.Connect(ctx)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Connect: %v", err)
				}
				_ = cli.Disconnect(ctx)
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Connect error = %v, want %v", err, tt.wantErr)
			}
			conn := b.Conn(0, time.Second)
			if conn == nil {
				t.Fatal("broker accepted no connection")
			}
			<-conn.Gone()
			if log := conn.Log(); len(log) != 0 {
				t.Fatalf("revoked broker received %d packets", len(log))
			}
		})
	}
}

// VerifyConnection runs the same check from tls.Config, without mqttv5.
func TestVerifyConnection(t *testing.T) {
	f := newFixture(t, testpki.WithOCSPServer(), testpki.WithCRLDistributionPoints())
	served := f.leaf
	served.OCSPStaple = f.staple(t, ocsp.Revoked)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{served}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()
	checker := f.checker(t, WithScope(LeafOnly))
	_, err = tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		RootCAs:          f.root.Pool(),
		Time:             testpki.Clock,
		VerifyConnection: checker.VerifyConnection,
	})
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("tls.Dial error = %v, want ErrRevoked", err)
	}
}
