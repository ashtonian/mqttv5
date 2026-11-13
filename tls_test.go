package mqttv5

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/internal/testpki"
	"github.com/ashtonian/mqttv5/wire"
)

func peerSerial(t *testing.T, c *testbroker.Conn) *big.Int {
	t.Helper()
	state, ok := c.TLS()
	if !ok || len(state.PeerCertificates) == 0 {
		t.Fatalf("connection %d has no client certificate", c.Index)
	}
	return state.PeerCertificates[0].SerialNumber
}

// GetClientCertificate runs on every handshake, so a reconnect presents
// the certificate it returns at that moment: rotating a client
// certificate needs nothing from the client.
func TestTLSClientCertificateRotatesOnReconnect(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	deviceCA := testpki.NewRoot(t, "device CA")
	b := testbroker.NewTLS(t, &tls.Config{
		Certificates: []tls.Certificate{brokerCA.Server(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    deviceCA.Pool(),
		Time:         testpki.Clock,
	})
	first, second := deviceCA.Client(t, "device"), deviceCA.Client(t, "device")
	var current atomic.Pointer[tls.Certificate]
	current.Store(&first)
	var calls atomic.Int32
	tbClient(t, b, WithTLSConfig(&tls.Config{
		RootCAs: brokerCA.Pool(),
		Time:    testpki.Clock,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			calls.Add(1)
			return current.Load(), nil
		},
	}))

	if got := peerSerial(t, b.Conn(0, 0)); got.Cmp(first.Leaf.SerialNumber) != 0 {
		t.Fatalf("first connection presented serial %v, want %v", got, first.Leaf.SerialNumber)
	}

	current.Store(&second)
	b.DropAll()
	reconnected := b.Conn(1, 3*time.Second)
	if reconnected == nil {
		t.Fatal("client did not reconnect")
	}
	waitLog(t, reconnected, wire.CONNECT, 1)
	if got := peerSerial(t, reconnected); got.Cmp(second.Leaf.SerialNumber) != 0 {
		t.Fatalf("reconnection presented serial %v, want the rotated %v", got, second.Leaf.SerialNumber)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("GetClientCertificate called %d times, want once per connection (2)", n)
	}
}

// A broker whose certificate does not verify never receives CONNECT,
// so credentials never reach it, and the caller can tell why.
func TestTLSUntrustedBrokerReceivesNoConnect(t *testing.T) {
	brokerCA := testpki.NewRoot(t, "broker CA")
	otherCA := testpki.NewRoot(t, "other CA")
	b := testbroker.NewTLS(t, &tls.Config{Certificates: []tls.Certificate{brokerCA.Server(t)}})

	reported := make(chan error, 1)
	c, err := New(
		WithBroker(b.URL()),
		WithClientID("tb-"+t.Name()),
		WithCredentials("user", []byte("secret")),
		WithLogger(quietLogger()),
		WithTLSConfig(&tls.Config{RootCAs: otherCA.Pool(), Time: testpki.Clock}),
		WithOnConnectError(func(err error) {
			select {
			case reported <- err:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err = c.Connect(ctx)

	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("Connect error = %v, want x509.UnknownAuthorityError", err)
	}
	select {
	case got := <-reported:
		if !errors.As(got, &unknown) {
			t.Fatalf("OnConnectError got %v, want x509.UnknownAuthorityError", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnectError did not fire")
	}
	conn := b.Conn(0, time.Second)
	if conn == nil {
		t.Fatal("broker accepted no connection")
	}
	<-conn.Gone()
	if log := conn.Log(); len(log) != 0 {
		t.Fatalf("broker received %d packets from a client that does not trust it", len(log))
	}
}
