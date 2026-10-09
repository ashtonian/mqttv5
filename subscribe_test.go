// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// TestSubscribeChanDropOldestRejected verifies that an explicit
// SubDropPolicy(DropOldest) on the channel-based Subscribe returns
// ErrChanDropOldestUnsupported. DropOldest requires head-eviction,
// which would race a consumer-owned channel.
func TestSubscribeChanDropOldestRejected(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err = cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "x/y", QoS: 1}},
		SubDropPolicy(DropOldest),
	)
	if !errors.Is(err, ErrChanDropOldestUnsupported) {
		t.Fatalf("Subscribe err = %v, want ErrChanDropOldestUnsupported", err)
	}
}

// TestSubscribeChanIgnoresClientLevelDropOldestDefault verifies the
// asymmetric handling: a client-level WithDropPolicy(DropOldest)
// propagates to chan Subscribe but is silently downgraded to
// DropNewest (chan can't peek-and-pop). Only an explicit per-call
// SubDropPolicy(DropOldest) errors.
func TestSubscribeChanIgnoresClientLevelDropOldestDefault(t *testing.T) {
	subAcked := make(chan struct{}, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			sub, ok := pkt.(*wire.Subscribe)
			if !ok {
				pkt.Release()
				continue
			}
			id := sub.PacketID
			pkt.Release()
			_, _ = wire.WriteSuback(c, wire.SubackOpts{
				PacketID:    id,
				ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
			})
			select {
			case subAcked <- struct{}{}:
			default:
			}
		}
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithDropPolicy(DropOldest), // client-level default
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// Channel Subscribe with no explicit SubDropPolicy must succeed.
	_, _, err = cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "x/y", QoS: 1}},
	)
	if err != nil {
		t.Fatalf("Subscribe (chan) with client-level DropOldest: %v", err)
	}
}

// A consumer that falls behind loses QoS 0 messages but no QoS 1 or 2
// one: those have room beyond the buffer or cap for as many as the
// broker may send before the consumer acks one. Auto-ack gives up the
// broker's flow control, and with it that room.
func TestSlowConsumerKeepsQoS1(t *testing.T) {
	const recvMax = 8
	// Two QoS 0 messages fill the buffer; recvMax QoS 1 messages and two
	// more QoS 0 ones follow.
	sent := []string{"s/q0/a", "s/q0/b"}
	for i := 1; i <= recvMax; i++ {
		sent = append(sent, fmt.Sprintf("s/q1/%d", i))
	}
	sent = append(sent, "s/q0/c", "s/q0/d")
	qos1 := sent[2 : 2+recvMax]

	filters := []TopicFilter{{Topic: "s/#", QoS: 1}}
	viaChannel := func(t *testing.T, cli *Client, opts []SubscribeOption) func() []string {
		ch, _, err := cli.Subscribe(context.Background(), filters, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return func() []string {
			var got []string
			for {
				select {
				case m := <-ch:
					got = append(got, m.Topic)
					_ = m.Ack()
				default:
					return got
				}
			}
		}
	}
	viaQueue := func(t *testing.T, cli *Client, opts []SubscribeOption) func() []string {
		q, _, err := cli.SubscribeQueue(context.Background(), filters, opts...)
		if err != nil {
			t.Fatal(err)
		}
		return func() []string {
			var got []string
			for {
				m, ok := q.TryDequeue()
				if !ok {
					return got
				}
				got = append(got, m.Topic)
				_ = m.Ack()
			}
		}
	}

	tests := []struct {
		name      string
		subscribe func(*testing.T, *Client, []SubscribeOption) func() []string
		opts      []SubscribeOption
		delivered []string
		dropped   []string
	}{
		{"channel", viaChannel, nil, sent[:2+recvMax], sent[2+recvMax:]},
		{"queue", viaQueue, nil, sent[:2+recvMax], sent[2+recvMax:]},
		{"channel with auto-ack", viaChannel, []SubscribeOption{SubAutoAck()}, sent[:2], append(slices.Clone(qos1), sent[2+recvMax:]...)},
		{"queue with auto-ack", viaQueue, []SubscribeOption{SubAutoAck()}, sent[:2], append(slices.Clone(qos1), sent[2+recvMax:]...)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ready := make(chan struct{})
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{})
				c.ServeSubscribe(-1)
				<-ready
				var id uint16
				for _, topic := range sent {
					opts := wire.PublishOpts{Topic: topic}
					if strings.HasPrefix(topic, "s/q1/") {
						id++
						opts.QoS, opts.PacketID = 1, id
					}
					c.Publish(opts)
				}
				c.Hold(0)
			})
			cli := tbClient(t, b, WithReceiveMaximum(recvMax))
			var mu sync.Mutex
			var dropped []string
			opts := append([]SubscribeOption{SubBuffer(2), SubMaxQueueSize(2), SubOnDrop(func(m *Message) {
				mu.Lock()
				dropped = append(dropped, m.Topic)
				mu.Unlock()
			})}, tt.opts...)
			drain := tt.subscribe(t, cli, opts)
			close(ready)
			// The read loop places messages in order, so once the last
			// drop is seen every message has been placed.
			deadline := time.Now().Add(3 * time.Second)
			for {
				mu.Lock()
				n := len(dropped)
				mu.Unlock()
				if n >= len(tt.dropped) || time.Now().After(deadline) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			got := drain()
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(got, tt.delivered) {
				t.Errorf("delivered %v, want %v", got, tt.delivered)
			}
			if !slices.Equal(dropped, tt.dropped) {
				t.Errorf("dropped %v, want %v", dropped, tt.dropped)
			}
		})
	}
}
