package readme

import (
	"crypto/tls"
	"crypto/x509"
	"sync/atomic"
)

import (
	"github.com/ashtonian/mqttv5"
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
