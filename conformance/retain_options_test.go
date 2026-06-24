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

// ---------------- RetainAsPublished (§3.8.3.1) ----------------

// TestSubscribe_RetainAsPublished pins the discriminating behavior of
// the RetainAsPublished subscription option, which only manifests on a
// LIVE forward of a PUBLISH whose own RETAIN bit is set:
//
//   - RetainAsPublished=true: the broker forwards the publisher's RETAIN
//     flag verbatim. A live publish with RETAIN=1 is delivered RETAIN=1.
//   - RetainAsPublished=false (the spec default): the broker clears the
//     RETAIN flag on forwarded PUBLISHes. The same live RETAIN=1 publish
//     is delivered RETAIN=0.
//
// Both legs install the subscription FIRST on a fresh topic (so the
// later publish is a live forward, not a subscribe-time retained
// replay) and then publish an identical RETAIN=1 message. The legs
// differ ONLY in the RetainAsPublished option and produce OPPOSITE
// delivered RETAIN flags — that is what makes the option load-bearing.
//
// Per §3.3.1.3 a retained replay sent because a subscription was just
// established always carries RETAIN=1 regardless of RetainAsPublished,
// so the replay path cannot discriminate the option and is left to
// TestSubscribe_RetainHandling. Each leg clears its retained message
// afterward so it can't leak across the shared broker.
func TestSubscribe_RetainAsPublished(t *testing.T) {
	requireBroker(t, brokerURL())

	pub := connect(t)

	// --- RAP=true: live RETAIN=1 publish forwarded with RETAIN=1. ---
	trueTopic := "conformance/rap-true/" + randSuffix()
	subTrue := connect(t)
	chTrue, _, err := subTrue.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: trueTopic, QoS: 1, RetainAsPublished: true}},
		mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	// Let the broker install the subscription so the publish below is a
	// live forward against an existing subscription, not a replay.
	time.Sleep(50 * time.Millisecond)

	want := []byte("rap-live-true")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: trueTopic, Payload: want, QoS: 1, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pub.Publish(context.Background(), wire.PublishOpts{
			Topic: trueTopic, QoS: 1, Retain: true, Payload: nil,
		})
	})

	mTrue := expectMessage(t, chTrue, 3*time.Second)
	if !mTrue.Retain {
		t.Error("RetainAsPublished=true: live RETAIN=1 publish delivered with RETAIN=0, want RETAIN=1")
	}
	if !bytes.Equal(mTrue.Payload, want) {
		t.Errorf("RetainAsPublished=true: payload = %q, want %q", mTrue.Payload, want)
	}
	_ = mTrue.Ack()

	// --- RAP=false: same live RETAIN=1 publish forwarded RETAIN cleared. ---
	falseTopic := "conformance/rap-false/" + randSuffix()
	subFalse := connect(t)
	chFalse, _, err := subFalse.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: falseTopic, QoS: 1, RetainAsPublished: false}},
		mqttv5.SubBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	live := []byte("rap-live-false")
	if err := pub.Publish(context.Background(), wire.PublishOpts{
		Topic: falseTopic, Payload: live, QoS: 1, Retain: true,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pub.Publish(context.Background(), wire.PublishOpts{
			Topic: falseTopic, QoS: 1, Retain: true, Payload: nil,
		})
	})

	mFalse := expectMessage(t, chFalse, 3*time.Second)
	if mFalse.Retain {
		t.Error("RetainAsPublished=false: live RETAIN=1 publish delivered with RETAIN=1, want RETAIN=0")
	}
	if !bytes.Equal(mFalse.Payload, live) {
		t.Errorf("RetainAsPublished=false: payload = %q, want %q", mFalse.Payload, live)
	}
	_ = mFalse.Ack()
}

// ---------------- RetainHandling (§3.8.3.1) ----------------

// TestSubscribe_RetainHandling pins the send-retained-on-subscribe
// semantics of the RetainHandling option against a stored retained
// message:
//
//   - mode 0 (send retained at subscribe time): a brand-new subscription
//     receives the retained message immediately. Asserted on RETAIN=1
//     replay + payload.
//   - mode 2 (do not send retained at subscribe time): no retained
//     replay at all. Asserted via expectNoMessage.
//
// mode 1 (send retained only if the subscription did not already exist)
// is established-state dependent — see broker_caveats; it is exercised
// here only as the "new subscription -> retained delivered" case, which
// for a fresh distinct filter behaves like mode 0.
//
// Each leg uses its own topic + freshly connected client so a prior
// leg's installed subscription can't satisfy the next leg's "is this
// subscription new?" decision.
func TestSubscribe_RetainHandling(t *testing.T) {
	requireBroker(t, brokerURL())

	pub := connect(t)
	want := []byte("rh-retained")

	// storeRetained publishes a retained message on a fresh topic and
	// returns it; the caller subscribes afterward. A cleanup clears the
	// retained message so it can't leak across the shared broker.
	storeRetained := func(t *testing.T) string {
		t.Helper()
		topic := "conformance/rh/" + randSuffix()
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic: topic, Payload: want, QoS: 1, Retain: true,
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = pub.Publish(context.Background(), wire.PublishOpts{
				Topic: topic, QoS: 1, Retain: true, Payload: nil,
			})
		})
		// Let the broker store the retained message before any
		// subscribe races it.
		time.Sleep(100 * time.Millisecond)
		return topic
	}

	t.Run("mode0_sendsRetainedAtSubscribe", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 0}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !m.Retain {
			t.Error("RetainHandling=0: replay delivered with RETAIN=0, want RETAIN=1")
		}
		if !bytes.Equal(m.Payload, want) {
			t.Errorf("RetainHandling=0: payload = %q, want %q", m.Payload, want)
		}
		_ = m.Ack()
	})

	t.Run("mode1_sendsRetainedOnNewSubscription", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		// Fresh client + fresh filter => the subscription is new, so
		// mode 1 must deliver the retained message (same observable
		// behavior as mode 0 for a first-time subscription).
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 1}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !m.Retain {
			t.Error("RetainHandling=1 (new sub): replay delivered with RETAIN=0, want RETAIN=1")
		}
		if !bytes.Equal(m.Payload, want) {
			t.Errorf("RetainHandling=1 (new sub): payload = %q, want %q", m.Payload, want)
		}
		_ = m.Ack()
	})

	t.Run("mode2_suppressesRetainedAtSubscribe", func(t *testing.T) {
		topic := storeRetained(t)
		sub := connect(t)
		ch, _, err := sub.Subscribe(context.Background(),
			[]mqttv5.TopicFilter{{Topic: topic, QoS: 1, RetainHandling: 2}},
			mqttv5.SubBuffer(4))
		if err != nil {
			t.Fatal(err)
		}
		// Mode 2 must NOT replay the retained message at subscribe time.
		expectNoMessage(t, ch, 1*time.Second)

		// Sanity: the subscription itself is live — a subsequent live
		// publish still flows. This guards against a false pass where
		// the subscribe silently failed rather than suppressing the
		// retained replay.
		live := []byte("rh-live-after-mode2")
		if err := pub.Publish(context.Background(), wire.PublishOpts{
			Topic: topic, Payload: live, QoS: 1, Retain: false,
		}); err != nil {
			t.Fatal(err)
		}
		m := expectMessage(t, ch, 3*time.Second)
		if !bytes.Equal(m.Payload, live) {
			t.Errorf("RetainHandling=2: live payload = %q, want %q", m.Payload, live)
		}
		if m.Retain {
			t.Error("RetainHandling=2: live forward delivered with RETAIN=1, want RETAIN=0")
		}
		_ = m.Ack()
	})
}
