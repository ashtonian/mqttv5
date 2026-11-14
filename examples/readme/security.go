package readme

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"sync/atomic"
)

import (
	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/tlsrevocation"
)

var caPEM []byte

func privateCA() {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	mqttv5.WithTLSConfig(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
}

func rotatingClientCertificate(pool *x509.CertPool) {
	var current atomic.Pointer[tls.Certificate] // replaced on renewal
	mqttv5.WithTLSConfig(&tls.Config{
		RootCAs: pool,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return current.Load(), nil
		},
	})
}

func brokerCertificate() {
	mqttv5.WithOnConnectionUp(func(info mqttv5.ConnackInfo) {
		if info.TLS == nil {
			return
		}
		leaf := info.TLS.PeerCertificates[0]
		slog.Info("broker certificate", "broker", info.Broker,
			"subject", leaf.Subject.String(), "not_after", leaf.NotAfter)
	})
}

func revocation() error {
	checker, err := tlsrevocation.New()
	if err != nil {
		return err
	}
	cli, err := mqttv5.New(
		mqttv5.WithBroker("mqtts://broker.example.com:8883"),
		mqttv5.WithTLSVerifier(checker.Verify),
	)
	_ = cli
	return err
}
