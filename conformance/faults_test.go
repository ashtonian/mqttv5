// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

// When the broker has lost the session by the time the client
// reconnects (Session Present 0), an unacknowledged QoS 1 publish
// follows the session-loss policy: republished as a new message by
// default, or failed with ErrSessionLost.
func TestSession_LostWhileDisconnected(t *testing.T) {
	for _, tt := range []struct {
		name   string
		policy mqttv5.SessionLossPolicy
	}{
		{"republish", mqttv5.SessionLossRepublish},
		{"fail", mqttv5.SessionLossFail},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requireBroker(t, brokerURL())
			topic := "conformance/session-lost/" + randSuffix()
			ch, cleanup := withSubscriber(t, topic, 4)
			defer cleanup()

			px := newFaultProxy(t, stripScheme(brokerURL()), true)
			pub := connect(t,
				mqttv5.WithBroker(px.url()),
				mqttv5.WithSessionExpiry(1),
				mqttv5.WithSessionLossPolicy(tt.policy),
				mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(50*time.Millisecond)),
			)

			// The PUBLISH leaves the client but never reaches the broker.
			px.dropFromClient(func(pt wire.PacketType) bool { return pt == wire.PUBLISH })
			done := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				done <- pub.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 1, Payload: []byte("across-loss")})
			}()
			px.awaitDropped(t, 1)
			px.dropFromClient(nil)
			// Keep the client away until its one-second session expires.
			px.refuse(2500 * time.Millisecond)
			px.cut()

			err := <-done
			sessions := px.trace.sessions()
			if len(sessions) < 2 || !strings.Contains(sessions[len(sessions)-1], "sp=false") {
				t.Fatalf("the reconnect resumed the session: %v", sessions)
			}
			switch tt.policy {
			case mqttv5.SessionLossRepublish:
				if err != nil {
					t.Fatalf("Publish: %v", err)
				}
				m := expectMessage(t, ch, 3*time.Second)
				if string(m.Payload) != "across-loss" {
					t.Errorf("payload %q", m.Payload)
				}
				_ = m.Ack()
				expectNoMessage(t, ch, 500*time.Millisecond)
			case mqttv5.SessionLossFail:
				if !errors.Is(err, mqttv5.ErrSessionLost) {
					t.Fatalf("Publish: %v, want ErrSessionLost", err)
				}
				expectNoMessage(t, ch, 500*time.Millisecond)
			}
		})
	}
}

// A QoS 2 exchange cut after PUBREC resumes with PUBREL on the next
// connection (§4.4): the Publish succeeds and the message arrives once.
func TestQoS2_ResumesAfterPUBREC(t *testing.T) {
	requireBroker(t, brokerURL())
	topic := "conformance/qos2-resume/" + randSuffix()
	sub := connect(t)
	ch, _, err := sub.Subscribe(context.Background(), []mqttv5.TopicFilter{{Topic: topic, QoS: 2}}, mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	px := newFaultProxy(t, stripScheme(brokerURL()), true)
	pub := connect(t,
		mqttv5.WithBroker(px.url()),
		mqttv5.WithSessionExpiry(300),
		mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(20*time.Millisecond)),
	)
	// The broker gets the PUBLISH and answers PUBREC; the client's
	// PUBREL never arrives.
	px.dropFromClient(func(pt wire.PacketType) bool { return pt == wire.PUBREL })
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		done <- pub.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: 2, Payload: []byte("resumed")})
	}()
	px.awaitDropped(t, 1)
	px.dropFromClient(nil)
	px.cut()

	if err := <-done; err != nil {
		t.Fatalf("Publish: %v", err)
	}
	m := expectMessage(t, ch, 3*time.Second)
	if string(m.Payload) != "resumed" || m.QoS != 2 {
		t.Errorf("delivered %q at QoS %d", m.Payload, m.QoS)
	}
	_ = m.Ack()
	expectNoMessage(t, ch, time.Second)

	var resent, pubrels int
	for _, e := range px.trace.history("resumed") {
		if strings.Contains(e, "PUBLISH") && strings.Contains(e, "dup=true") {
			resent++
		}
		if strings.Contains(e, "PUBREL") {
			pubrels++
		}
	}
	if resent != 0 || pubrels != 1 {
		t.Errorf("after the cut the client resent %d PUBLISH and %d PUBREL, want 0 and 1: %v",
			resent, pubrels, px.trace.history("resumed"))
	}
}

// A filter a broker will not take is reported, and the client carries
// on. The filter is valid MQTT but deeper than some brokers allow:
// EMQX refuses it in its SUBACK (0x8F, beyond 128 levels), mosquitto
// disconnects instead (0x81, beyond 200), which the client gives up on
// after a few attempts; brokers without such a limit grant it and the
// subtest skips.
func TestSubscribe_RefusedFilterReported(t *testing.T) {
	deep := "conformance/deep/" + strings.Repeat("l/", 199) + "l"
	for _, url := range []string{brokerURL(), secondaryBrokerURL()} {
		t.Run(url, func(t *testing.T) {
			requireBroker(t, url)
			cli := connectTo(t, url, mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(10*time.Millisecond)))
			pub := connectTo(t, url)
			ok := "conformance/allowed/" + randSuffix()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ch, tok, err := cli.Subscribe(ctx,
				[]mqttv5.TopicFilter{{Topic: deep, QoS: 1}, {Topic: ok, QoS: 1}}, mqttv5.SubBuffer(4))
			var serr *mqttv5.SubscribeError
			var rej *mqttv5.RejectedByDisconnectError
			switch {
			case err == nil:
				t.Skip("the broker accepts a filter of 202 levels")
			case errors.As(err, &serr):
				r := tok.Results()
				if len(r) != 2 || r[0].Granted() || !r[1].Granted() {
					t.Fatalf("results %+v", r)
				}
				t.Logf("refused in SUBACK with 0x%02X", byte(r[0].Reason))
			case errors.As(err, &rej):
				t.Logf("refused by disconnecting: %v", err)
				if err := cli.AwaitConnection(ctx); err != nil {
					t.Fatal(err)
				}
				// The rejected SUBSCRIBE carried both filters.
				if ch, _, err = cli.Subscribe(ctx, []mqttv5.TopicFilter{{Topic: ok, QoS: 1}}, mqttv5.SubBuffer(4)); err != nil {
					t.Fatalf("subscribe after the rejection: %v", err)
				}
			default:
				t.Fatalf("Subscribe: %v", err)
			}
			if err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: ok, QoS: 1, Payload: []byte("granted")}); err != nil {
				t.Fatal(err)
			}
			m := expectMessage(t, ch, 3*time.Second)
			if string(m.Payload) != "granted" {
				t.Errorf("payload %q", m.Payload)
			}
			_ = m.Ack()
		})
	}
}
