// WebSocket example: connect over ws:// or wss:// by composing
// mqttv5.WithDialFunc with transport/ws, publish a message to itself,
// then wait for an interrupt. The core mqttv5 module stays stdlib-only;
// the gobwas/ws dependency lives only in the transport/ws submodule,
// imported only here.
//
// Run a broker with a WebSocket listener (EMQX's ws:default on 8083 and
// wss:default on 8084, in conformance/docker-compose.yml) then:
//
//	go -C examples run ./ws
//	MQTT_BROKER=wss://127.0.0.1:8084/mqtt MQTT_CA_FILE=../conformance/tls/pki/root.pem go -C examples run ./ws
//
// MQTT_CA_FILE is resolved from the examples directory; empty verifies a
// wss:// broker against the system roots.
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
	"github.com/ashtonian/mqttv5/transport/ws"
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
		fmt.Fprintln(os.Stderr, "ws:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	broker := envOr("MQTT_BROKER", "ws://127.0.0.1:8083/mqtt")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	tlsCfg, err := tlsConfig(os.Getenv("MQTT_CA_FILE"))
	if err != nil {
		return err
	}
	cli, err := mqttv5.New(
		mqttv5.WithBroker(broker),
		mqttv5.WithDialFunc(ws.DialFunc(ws.DialOpts{TLSConfig: tlsCfg})),
		mqttv5.WithClientID("mqttv5-ws-example"),
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
	}()
	logger.Info("connected over websocket", "broker", broker)

	topic := "mqttv5/examples/ws/" + time.Now().Format("150405.000")
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(1))
	if err != nil {
		return err
	}
	if err = cli.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: []byte("over WebSocket")}); err != nil {
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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
