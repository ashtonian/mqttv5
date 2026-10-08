// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// TestReceiveMaximumHonoured publishes far more QoS 1 messages at once
// than the broker's Receive Maximum (mosquitto advertises 20, EMQX 32,
// HiveMQ 10, the strict HiveMQ 4) and checks the client never had more
// in flight than the broker allows and that every publish completes.
func TestReceiveMaximumHonoured(t *testing.T) {
	for _, url := range []string{brokerURL(), secondaryBrokerURL(), strictBrokerURL()} {
		t.Run(url, func(t *testing.T) {
			requireBroker(t, url)
			cli := connectTo(t, url, mqttv5.WithStats())
			info, ok := cli.ServerInfo()
			if !ok {
				t.Fatal("not connected")
			}
			t.Logf("broker Receive Maximum %d", info.ReceiveMaximum)
			limit := int(info.ReceiveMaximum)

			var maxInFlight atomic.Int64
			stop := make(chan struct{})
			var sampler sync.WaitGroup
			sampler.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}
					if n := int64(limit) - int64(cli.Stats().SendQuota); n > maxInFlight.Load() {
						maxInFlight.Store(n)
					}
					time.Sleep(50 * time.Microsecond)
				}
			})

			const n = 400
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Go(func() {
					if err := cli.Publish(ctx, mqttv5.PublishOptions{Topic: "conformance/rm/" + randSuffix(), Payload: []byte{byte(i)}, QoS: 1}); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			close(stop)
			sampler.Wait()
			if got := maxInFlight.Load(); got > int64(limit) || got < 1 {
				t.Fatalf("max in flight %d, Receive Maximum %d", got, limit)
			}
			if got := cli.Stats().PublishesAcked; got != n {
				t.Fatalf("%d of %d publishes acknowledged", got, n)
			}
		})
	}
}

// connectTo is connect against a specific broker.
func connectTo(t *testing.T, url string, opts ...mqttv5.Option) *mqttv5.Client {
	t.Helper()
	all := append([]mqttv5.Option{
		mqttv5.WithBroker(url),
		mqttv5.WithClientID(t.Name() + "-" + randSuffix()),
		mqttv5.WithConnectTimeout(5 * time.Second),
	}, opts...)
	cli, err := mqttv5.New(all...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cli.Disconnect(ctx)
	})
	return cli
}
