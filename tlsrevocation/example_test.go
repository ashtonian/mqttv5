package tlsrevocation_test

import (
	"context"
	"crypto/x509"
	"log"
	"os"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/tlsrevocation"
)

// Check the broker's chain for revocation before every CONNECT.
func ExampleChecker_Verify() {
	checker, err := tlsrevocation.New()
	if err != nil {
		log.Fatal(err)
	}
	cli, err := mqttv5.New(
		mqttv5.WithBroker("mqtts://broker.example.com:8883"),
		mqttv5.WithTLSVerifier(checker.Verify),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = cli
}

// A private CA with no distribution point publishes its CRL out of
// band; the provider hands it to the checker.
func ExampleWithCRLProvider() {
	provider := func(_ context.Context, issuer *x509.Certificate) (*x509.RevocationList, error) {
		der, err := os.ReadFile("/etc/mqtt/ca.crl")
		if err != nil {
			return nil, err
		}
		return x509.ParseRevocationList(der)
	}
	checker, err := tlsrevocation.New(tlsrevocation.WithCRLProvider(provider))
	if err != nil {
		log.Fatal(err)
	}
	_ = checker
}
