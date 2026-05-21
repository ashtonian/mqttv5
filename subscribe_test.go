// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"net"
	"testing"

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
