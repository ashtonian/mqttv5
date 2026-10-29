// Durable example: a QueuePublisher on queue/file with the session in
// store/file. Publish returns once a message is on disk, so the program
// keeps publishing while no broker is up, and a restarted program
// continues where the last one stopped instead of publishing its
// in-flight messages again.
//
//	go -C examples run ./durable          # with or without a broker running
//	docker stop mq; sleep 5; docker start mq
//
// Messages published while the broker was down arrive, in order, once it
// is back. Stop the program mid-run and start it again: nothing is lost
// or published twice.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	qfile "github.com/ashtonian/mqttv5/queue/file"
	sfile "github.com/ashtonian/mqttv5/store/file"
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
		fmt.Fprintln(os.Stderr, "durable:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	brokers := strings.Split(envOr("MQTT_BROKERS", envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")), ",")
	dir := envOr("MQTT_DATA_DIR", filepath.Join(os.TempDir(), "mqttv5-durable"))
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	st, err := sfile.Open(filepath.Join(dir, "session"))
	if err != nil {
		return fmt.Errorf("open session store: %w", err)
	}
	defer func() { _ = st.Close() }()
	q, err := qfile.Open(filepath.Join(dir, "outbound"))
	if err != nil {
		return fmt.Errorf("open queue: %w", err)
	}
	defer func() { _ = q.Close() }()

	cli, err := mqttv5.New(
		mqttv5.WithBrokers(brokers...),
		mqttv5.WithClientID("mqttv5-durable"),
		mqttv5.WithStore(st),
		mqttv5.WithRetryInitialConnect(),
		mqttv5.WithReconnectBackoff(mqttv5.ExponentialBackoff{Initial: 250 * time.Millisecond, Max: 5 * time.Second, Jitter: 0.5}),
		mqttv5.WithOnConnectionUp(func(info mqttv5.ConnackInfo) {
			fmt.Printf("UP    %s session_present=%t\n", info.Broker, info.SessionPresent)
		}),
		mqttv5.WithOnConnectionDown(func() bool {
			fmt.Println("DOWN  queuing on disk until a broker is back")
			return true
		}),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return err
	}
	pub, err := mqttv5.NewQueuePublisher(cli, q,
		mqttv5.WithQueueTTL(time.Hour),
		mqttv5.WithQueueIdempotencyKey(),
		mqttv5.WithDeadLetter(func(e mqttv5.QueueEntry, err error) {
			fmt.Printf("DEAD  %s %s: %v\n", e.Publish.Topic, e.Publish.Payload, err)
		}),
	)
	if err != nil {
		return err
	}
	if err := cli.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pub.Close(shutdown)
		_ = cli.Disconnect(shutdown)
	}()
	go consume(ctx, brokers, logger)

	start := time.Now().Format("15:04:05")
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		payload := fmt.Sprintf("run %s #%d", start, i)
		if err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: "durable/demo", QoS: 1, Payload: []byte(payload)}); err != nil {
			return fmt.Errorf("store %q: %w", payload, err)
		}
		fmt.Printf("STORE %s connected=%t\n", payload, cli.Connected())
	}
}

// consume prints what reaches the broker, with the idempotency key a
// consumer would deduplicate on.
func consume(ctx context.Context, brokers []string, logger *slog.Logger) {
	cli, err := mqttv5.New(
		mqttv5.WithBrokers(brokers...),
		mqttv5.WithClientID("mqttv5-durable-consumer"),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(60*60),
		mqttv5.WithRetryInitialConnect(),
		mqttv5.WithLogger(logger),
	)
	if err != nil {
		return
	}
	msgs, _, err := cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: "durable/#", QoS: 1}})
	if err != nil || cli.Connect(ctx) != nil {
		return
	}
	// Disconnect closes msgs, which ends the loop.
	go func() {
		<-ctx.Done()
		_ = cli.Disconnect(context.WithoutCancel(ctx))
	}()
	for m := range msgs {
		key, _ := m.Properties.UserProperty("mqttv5-msg-id")
		fmt.Printf("RECV  %s (id %s)\n", m.Payload, key)
		_ = m.Ack()
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
