// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
)

// A pipelined QueuePublisher delivers every queued message once, in
// order, through a real broker at QoS 1 and 2.
func TestQueuePublisher_DrainsInOrderOnce(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint("qos", qos), func(t *testing.T) {
			sub := connect(t)
			pub := connect(t)
			topic := "conformance/queue/" + randSuffix()
			ctx := context.Background()
			msgs, _, err := sub.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: topic, QoS: 2}}, mqttv5.SubBuffer(512))
			if err != nil {
				t.Fatal(err)
			}
			q := mqttv5.NewMemoryPublisherQueue()
			qp, err := mqttv5.NewQueuePublisher(pub, q, mqttv5.WithQueueWindow(16))
			if err != nil {
				t.Fatal(err)
			}
			const n = 200
			for i := range n {
				if err := qp.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: qos, Payload: fmt.Appendf(nil, "%03d", i)}); err != nil {
					t.Fatal(err)
				}
			}
			for i := range n {
				select {
				case m := <-msgs:
					if want := fmt.Sprintf("%03d", i); string(m.Payload) != want {
						t.Fatalf("message %d is %q, want %q", i, m.Payload, want)
					}
					_ = m.Ack()
				case <-time.After(10 * time.Second):
					t.Fatalf("received %d of %d messages", i, n)
				}
			}
			select {
			case m := <-msgs:
				t.Fatalf("extra message %q", m.Payload)
			case <-time.After(500 * time.Millisecond):
			}
			closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := qp.Close(closeCtx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
