// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// TestSubscribe_NoLocal_SuppressesOwnPublishes verifies the MQTT v5
// §3.8.3.1 No-Local subscription option: when a client subscribes with
// NoLocal=true, the broker must NOT deliver that client's own matching
// PUBLISHes back to it. A second client subscribing to the same topic
// without No-Local still receives them.
//
// No-Local is a per-connection behavior — it only suppresses delivery
// when the same connection both publishes and subscribes. This test
// proves the broker honors the option byte we encode (the wire round
// trip is covered separately in wire/subscribe.go).
func TestSubscribe_NoLocal_SuppressesOwnPublishes(t *testing.T) {
	requireBroker(t, brokerURL())

	topic := "conformance/nolocal/" + randSuffix()

	// Client C subscribes WITH No-Local and is also the publisher: it
	// must not get its own messages echoed back. Use Subscribe directly
	// so we can set NoLocal on the filter (withSubscriber doesn't).
	c := connect(t)
	chC, _, err := c.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, NoLocal: true}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("C subscribe %s (NoLocal): %v", topic, err)
	}

	// Client D subscribes to the SAME topic WITHOUT No-Local — it is a
	// separate connection, so it must still receive C's publish.
	d := connect(t)
	chD, _, err := d.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, NoLocal: false}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatalf("D subscribe %s: %v", topic, err)
	}

	// Brief pause so both subscriptions are installed broker-side
	// before the publish races them.
	time.Sleep(50 * time.Millisecond)

	want := []byte("nolocal-payload")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Publish(ctx, wire.PublishOpts{
		Topic: topic, Payload: want, QoS: 1,
	}); err != nil {
		t.Fatalf("C publish: %v", err)
	}

	// D (separate connection, normal subscription) must receive it,
	// byte-equal and at the negotiated QoS.
	m := expectMessage(t, chD, 3*time.Second)
	if !bytes.Equal(m.Payload, want) || m.QoS != 1 {
		t.Errorf("D got QoS=%d payload=%q, want QoS=1 payload=%q", m.QoS, m.Payload, want)
	}
	if m.Topic != topic {
		t.Errorf("D topic = %q, want %q", m.Topic, topic)
	}
	_ = m.Ack()

	// C subscribed with No-Local, so its own publish must NOT come back.
	// Wait well past D's delivery to be sure the broker had time to
	// (wrongly) echo it if No-Local were broken.
	expectNoMessage(t, chC, 1500*time.Millisecond)
}
