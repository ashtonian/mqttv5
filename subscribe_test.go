// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

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

// TestSubscribeAutoAck verifies the SubAutoAck opt-in: the broker
// PUBACK lands before the consumer touches the message (so the lib
// drove the ack on the dispatcher) and the consumer can read
// Topic / Payload safely past its own Ack call (detached copies).
func TestSubscribeAutoAck(t *testing.T) {
	pubackSeen := make(chan struct{}, 1)
	connCh := make(chan net.Conn, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		connCh <- c

		// Wait for SUBSCRIBE, reply SUBACK, then PUBLISH QoS 1.
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			switch p := pkt.(type) {
			case *wire.Subscribe:
				id := p.PacketID
				pkt.Release()
				_, _ = wire.WriteSuback(c, wire.SubackOpts{
					PacketID:    id,
					ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
				})
				_, _ = wire.WritePublish(c, wire.PublishOpts{
					Topic:    "auto/ack/topic",
					Payload:  []byte("hello"),
					QoS:      1,
					PacketID: 42,
				})
			case *wire.PubResp:
				if p.Type() == wire.PUBACK {
					select {
					case pubackSeen <- struct{}{}:
					default:
					}
				}
				pkt.Release()
			default:
				pkt.Release()
			}
		}
	})

	cli, err := New(WithBroker(fb.URL()), WithClientID("autoack-test"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	msgs, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "auto/ack/+", QoS: 1}},
		SubAutoAck(),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// PUBACK must reach the broker without the test pulling from msgs.
	select {
	case <-pubackSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("broker never received PUBACK — auto-ack didn't fire on dispatcher")
	}

	// Now receive the (already-acked) message.
	var m *Message
	select {
	case m = <-msgs:
	case <-time.After(2 * time.Second):
		t.Fatal("no message delivered after auto-ack")
	}

	if m.Topic != "auto/ack/topic" {
		t.Errorf("Topic = %q, want %q", m.Topic, "auto/ack/topic")
	}
	if string(m.Payload) != "hello" {
		t.Errorf("Payload = %q, want %q", string(m.Payload), "hello")
	}

	// Caller Ack is documented as a no-op for detached messages.
	if err := m.Ack(); err != nil {
		t.Fatalf("Ack on detached: %v", err)
	}
	// Repeated Ack must still be a no-op (no panic, no double release).
	_ = m.Ack()

	// Topic / Payload must still be valid after Ack — detached copies.
	if m.Topic != "auto/ack/topic" || string(m.Payload) != "hello" {
		t.Errorf("post-Ack Topic / Payload corrupted: topic=%q payload=%q",
			m.Topic, string(m.Payload))
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
