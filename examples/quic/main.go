// QUIC example: connect over mqtt-quic:// by composing
// mqttv5.WithDialFunc with a transport/quic Dialer, publish a message to
// itself, then wait for an interrupt. The core mqttv5 module stays
// stdlib-only; quic-go lives only in the transport/quic submodule,
// imported only here.
//
// Run a broker with an MQTT-over-QUIC listener (EMQX's quic:default, in
// conformance/docker-compose.yml on 14567/udp) then:
//
//	MQTT_CA_FILE=../conformance/quic/pki/root.pem go -C examples run ./quic
//
// MQTT_CA_FILE is resolved from the examples directory; empty verifies
// the broker against the system roots. MQTT_FALLBACK, an mqtts:// URL,
// falls back to TLS when QUIC cannot reach the broker.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport/quic"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	interrupted := ctx.Err() != nil
	stop()
	// An interrupt cancels whatever run was doing: that is a clean stop.
	if interrupted && errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "quic:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "mqtt-quic://127.0.0.1:14567")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tlsCfg, err := tlsConfig(os.Getenv("MQTT_CA_FILE"))
	if err != nil {
		return err
	}
	opts := quic.DialOpts{TLSConfig: tlsCfg, Logger: logger}
	if fb := os.Getenv("MQTT_FALLBACK"); fb != "" {
		opts.Fallback = &quic.Fallback{URL: fb}
	}
	// A Dialer of our own, rather than quic.DialFunc, so shutdown can
	// wait for the graceful close that runs after Disconnect returns.
	dialer := quic.NewDialer(opts)

	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithDialFunc(dialer.Dial),
		mqttv5.WithClientID("mqttv5-quic-example"),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return err
	}
	if err = cli.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cli.Disconnect(shutdown)
		if waitErr := dialer.Wait(shutdown); waitErr != nil {
			logger.Warn("graceful QUIC close did not finish", "error", waitErr)
		}
	}()
	info, _ := cli.ServerInfo()
	logger.Info("connected", "broker", broker, "tls_alpn", alpn(info.TLS))

	topic := "mqttv5/examples/quic/" + time.Now().Format("150405.000")
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(1))
	if err != nil {
		return err
	}
	if err = cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: []byte("over QUIC")}); err != nil {
		return err
	}
	select {
	case m := <-msgs:
		logger.Info("round trip", "topic", m.Topic, "payload", string(m.Payload))
		_ = m.Ack()
	case <-time.After(5 * time.Second):
		return errors.New("the published message did not come back")
	case <-ctx.Done():
		return ctx.Err()
	}

	<-ctx.Done()
	return nil
}

// tlsConfig trusts the PEM CA bundle at caFile, or the system roots when
// caFile is empty.
func tlsConfig(caFile string) (*tls.Config, error) {
	if caFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("MQTT_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("MQTT_CA_FILE %s holds no PEM certificate", caFile)
	}
	return &tls.Config{RootCAs: pool}, nil
}

func alpn(state *tls.ConnectionState) string {
	if state == nil {
		return ""
	}
	return state.NegotiatedProtocol
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
