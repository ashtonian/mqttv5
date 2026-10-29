// Bridge example: copy plant/# from a source broker to a destination
// broker as site-3/plant/#, at least once. Each message is acked to the
// source only after the destination acknowledged its copy, and the
// source keeps the bridge's session, so a message is lost neither while
// the destination is down nor when the bridge itself restarts.
//
//	docker run -d --name src -p 1883:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	docker run -d --name dst -p 1884:1883 eclipse-mosquitto:2.1.2-alpine mosquitto -c /mosquitto-no-auth.conf
//	go -C examples run ./bridge
//
// It also runs a producer on the source and a consumer on the
// destination, so the flow shows. Stop dst for a while, or the bridge
// itself, and no tick goes missing.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "bridge:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	srcURL := envOr("MQTT_SOURCE", "mqtt://127.0.0.1:1883")
	dstURL := envOr("MQTT_DESTINATION", "mqtt://127.0.0.1:1884")
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	src, err := mqttv5.New(
		mqttv5.WithBroker(srcURL),
		mqttv5.WithClientID("mqttv5-bridge"),
		mqttv5.WithCleanStart(false),     // resume the source's session after a restart
		mqttv5.WithSessionExpiry(60*60),  // the source holds messages while the bridge is down
		mqttv5.WithRetryInitialConnect(), // start while a broker is down
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return err
	}
	dst, err := mqttv5.New(
		mqttv5.WithBroker(dstURL),
		mqttv5.WithClientID("mqttv5-bridge"),
		mqttv5.WithRetryInitialConnect(),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return err
	}

	// Subscribe before Connect, so the messages the source held for the
	// bridge while it was down reach it.
	msgs, _, err := src.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "plant/#", QoS: 1}})
	if err != nil {
		return err
	}
	for _, c := range []*mqttv5.Client{dst, src} {
		if err := c.Connect(ctx); err != nil {
			return err
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = c.Disconnect(shutdown)
		}()
	}

	go produce(ctx, srcURL, logger)
	go consume(ctx, dstURL, logger)

	for {
		var m *mqttv5.Message
		select {
		case <-ctx.Done():
			return nil
		case m = <-msgs:
		}
		if err := forward(ctx, dst, m); err != nil {
			return err // m stays unacked: the source delivers it again
		}
		_ = m.Ack()
	}
}

// forward publishes m on dst under "site-3/", waiting out dst's
// outages. It returns nil once dst has the message or refused it for
// good, and an error when ctx ends or dst is closed.
func forward(ctx context.Context, dst *mqttv5.Client, m *mqttv5.Message) error {
	opts := m.PublishOptions() // payload and properties as they arrived
	opts.Topic = "site-3/" + m.Topic
	for {
		err := dst.Publish(ctx, opts)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, mqttv5.ErrNotConnected):
			if err = dst.AwaitConnection(ctx); err != nil {
				return err
			}
		case ctx.Err() != nil, errors.Is(err, mqttv5.ErrClosed):
			return err
		default:
			fmt.Printf("DROP  destination refused %s: %v\n", opts.Topic, err)
			return nil // drop it rather than hold up every message behind it
		}
	}
}

// produce publishes a numbered tick to the source every half second.
func produce(ctx context.Context, url string, logger *slog.Logger) {
	cli, err := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithRetryInitialConnect(), mqttv5.WithLogger(logger))
	if err != nil || cli.Connect(ctx) != nil {
		return
	}
	defer cli.Disconnect(context.WithoutCancel(ctx))
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		err := cli.Publish(ctx, mqttv5.PublishOptions{
			Topic: "plant/line-1", QoS: 1, Payload: fmt.Appendf(nil, "tick %d", i), ContentType: "text/plain",
		})
		if err != nil {
			fmt.Printf("PROD  tick %d not published: %v\n", i, err)
		}
	}
}

// consume prints what reaches the destination.
func consume(ctx context.Context, url string, logger *slog.Logger) {
	cli, err := mqttv5.New(mqttv5.WithBroker(url), mqttv5.WithRetryInitialConnect(), mqttv5.WithLogger(logger))
	if err != nil {
		return
	}
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "site-3/#", QoS: 1}})
	if err != nil || cli.Connect(ctx) != nil {
		return
	}
	// Disconnect closes msgs, which ends the loop.
	go func() {
		<-ctx.Done()
		_ = cli.Disconnect(context.WithoutCancel(ctx))
	}()
	for m := range msgs {
		fmt.Printf("DEST  %s %s (%s)\n", m.Topic, m.Payload, m.Properties.ContentType())
		_ = m.Ack()
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
