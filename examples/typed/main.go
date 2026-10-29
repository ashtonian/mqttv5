// Typed example: one client, one JSON Encoding, two payload types.
// Readings and commands each arrive decoded into their own struct;
// PublishValue encodes any value and declares its Content Type. A
// payload that does not decode is reported and counted, not delivered.
//
// Run a local broker (e.g., `docker run -p 1883:1883 eclipse-mosquitto mosquitto -c /mosquitto-no-auth.conf`)
// then:
//
//	MQTT_BROKER=mqtt://127.0.0.1:1883 go -C examples run ./typed
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashtonian/mqttv5"
	jsoncodec "github.com/ashtonian/mqttv5/codec/json"
)

type Reading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
}

type Command struct {
	Op   string `json:"op"`
	Args []int  `json:"args,omitempty"`
}

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
		fmt.Fprintln(os.Stderr, "typed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cli, err := mqttv5.New(
		mqttv5.WithBroker(envOr("MQTT_BROKER", "mqtt://127.0.0.1:1883")),
		mqttv5.WithClientID("mqttv5-typed"),
		mqttv5.WithEncoding(jsoncodec.Encoding{}),
		mqttv5.WithStats(),
	)
	if err != nil {
		return err
	}
	if err = cli.Connect(ctx); err != nil {
		return err
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	}()

	reportBadPayload := mqttv5.SubOnDecodeError(func(m *mqttv5.Message, err error) {
		fmt.Printf("skip  topic=%s: %v\n", m.Topic, err)
	})
	readings, _, err := cli.SubscribeAs[Reading](ctx,
		[]mqttv5.TopicFilter{{Topic: "sensors/#", QoS: 1}}, reportBadPayload)
	if err != nil {
		return err
	}
	commands, _, err := cli.SubscribeAs[Command](ctx,
		[]mqttv5.TopicFilter{{Topic: "devices/+/cmd", QoS: 1}}, reportBadPayload)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case m, ok := <-readings:
				if !ok {
					return
				}
				fmt.Printf("recv  topic=%s reading=%+v content-type=%s\n", m.Topic, m.Value, m.Properties.ContentType())
				_ = m.Ack()
			case m, ok := <-commands:
				if !ok {
					return
				}
				fmt.Printf("recv  topic=%s command=%+v\n", m.Topic, m.Value)
				_ = m.Ack()
			}
		}
	}()

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for i := 0; ; i++ {
		select {
		case <-t.C:
			device := fmt.Sprintf("sensor-%03d", i%4)
			var err error
			switch {
			case i%5 == 4:
				// Not JSON: SubOnDecodeError reports it, Stats counts it.
				err = cli.Publish(ctx, mqttv5.PublishOptions{Topic: "sensors/" + device, QoS: 1, Payload: []byte("22.5")})
			case i%3 == 2:
				err = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "devices/" + device + "/cmd", QoS: 1},
					Command{Op: "calibrate", Args: []int{i}})
			default:
				err = cli.PublishValue(ctx, mqttv5.PublishOptions{Topic: "sensors/" + device, QoS: 1},
					Reading{DeviceID: device, Temp: 20.0 + float64(i%10)})
			}
			if err != nil {
				return err
			}
		case <-ctx.Done():
			fmt.Printf("decode failures: %d\n", cli.Stats().DecodeFailures)
			return nil
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
