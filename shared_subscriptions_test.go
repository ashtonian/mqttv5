// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

func TestParseShareFilter(t *testing.T) {
	for _, tc := range []struct {
		filter     string
		wantGroup  string
		wantUnder  string
		wantShared bool
	}{
		{"$share/g1/foo/bar", "g1", "foo/bar", true},
		{"$share/group/+/+", "group", "+/+", true},
		{"$share/g/#", "g", "#", true},
		{"foo/bar", "", "foo/bar", false},                 // plain filter
		{"$share/", "", "$share/", false},                 // missing group + filter
		{"$share/onlyName", "", "$share/onlyName", false}, // missing filter
	} {
		t.Run(tc.filter, func(t *testing.T) {
			g, u, s := parseShareFilter(tc.filter)
			if g != tc.wantGroup || u != tc.wantUnder || s != tc.wantShared {
				t.Errorf("parseShareFilter(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tc.filter, g, u, s, tc.wantGroup, tc.wantUnder, tc.wantShared)
			}
		})
	}
}

func TestSubscribe_SharedFilter_TrieMatchesUnderlying(t *testing.T) {
	// The broker delivers inbound PUBLISHes with the original
	// (non-share) topic. The trie must match against the underlying
	// filter — otherwise nothing dispatches.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			pkt.Release()
			return
		}
		// Validate the broker received the FULL $share/... filter.
		if len(sub.Filters) != 1 || sub.Filters[0].Topic != "$share/workers/sensor/+" {
			t.Errorf("broker saw filter %v, want [$share/workers/sensor/+]", sub.Filters)
		}
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    sub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		pkt.Release()

		// Push a PUBLISH using the UNDERLYING topic (no $share prefix).
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic:    "sensor/temp",
			Payload:  []byte("shared-msg"),
			QoS:      1,
			PacketID: 100,
		})
		// Drain PUBACK.
		if pkt, err := dec.ReadPacket(); err == nil {
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ch, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "$share/workers/sensor/+", QoS: 1}}, SubBuffer(4))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case m := <-ch:
		if m.Topic != "sensor/temp" || string(m.Payload) != "shared-msg" {
			t.Errorf("got topic=%q payload=%q", m.Topic, m.Payload)
		}
		_ = m.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("shared subscription did not dispatch the inbound publish")
	}
}
