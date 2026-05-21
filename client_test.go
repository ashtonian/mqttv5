// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// ---------------- Connect / Disconnect ----------------

func TestClientConnect(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Hold the connection open so the read loop is happy.
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithClientID("test-client"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after Connect")
	}
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if cli.Connected() {
		t.Fatal("Connected() = true after Disconnect")
	}
}

func TestClientConnectRefused(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pkt.Release()
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode: wire.ReasonBanned,
		})
	})

	cli, _ := New(WithBroker(fb.URL()))
	err := cli.Connect(context.Background())
	if !errors.Is(err, ErrConnectRefused) {
		t.Fatalf("got %v, want ErrConnectRefused", err)
	}
}

func TestClientDoubleConnect(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	err := cli.Connect(context.Background())
	if !errors.Is(err, ErrAlreadyConnected) {
		t.Fatalf("got %v, want ErrAlreadyConnected", err)
	}
}

// ---------------- Publish ----------------

func TestPublishQoS0(t *testing.T) {
	received := make(chan *wire.Publish, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		if pub, ok := pkt.(*wire.Publish); ok {
			// Copy the bits we want to inspect before Release.
			received <- &wire.Publish{
				Topic:   string([]byte(pub.Topic)),
				Payload: append([]byte(nil), pub.Payload...),
				QoS:     pub.QoS,
			}
		}
		pkt.Release()
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Publish(context.Background(), wire.PublishOpts{
		Topic:   "sensor/temp",
		Payload: []byte("42.7"),
		QoS:     0,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if got.Topic != "sensor/temp" || string(got.Payload) != "42.7" || got.QoS != 0 {
			t.Errorf("got %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for broker to see publish")
	}
}

func TestPublishQoS1(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pub, ok := pkt.(*wire.Publish)
		if !ok {
			pkt.Release()
			return
		}
		id := pub.PacketID
		pkt.Release()
		_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: id})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, wire.PublishOpts{
		Topic:   "events",
		Payload: []byte("hi"),
		QoS:     1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestPublishQoS2(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// PUBLISH → PUBREC
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		pub, ok := pkt.(*wire.Publish)
		if !ok {
			pkt.Release()
			return
		}
		id := pub.PacketID
		pkt.Release()
		_, _ = wire.WritePubrec(c, wire.PubRespOpts{PacketID: id})

		// PUBREL → PUBCOMP
		pkt, err = dec.ReadPacket()
		if err != nil {
			return
		}
		if rel, ok := pkt.(*wire.PubResp); !ok || rel.Type() != wire.PUBREL {
			t.Errorf("expected PUBREL, got %T %s", pkt, pkt.Type())
		}
		pkt.Release()
		_, _ = wire.WritePubcomp(c, wire.PubRespOpts{PacketID: id})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, wire.PublishOpts{
		Topic:   "events/important",
		Payload: []byte("once-and-only-once"),
		QoS:     2,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// ---------------- Subscribe + handler dispatch ----------------

func TestSubscribeAndReceive(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// SUBSCRIBE → SUBACK
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			pkt.Release()
			return
		}
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    sub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		pkt.Release()

		// Push a PUBLISH the client should route to its handler.
		_, _ = wire.WritePublish(c, wire.PublishOpts{
			Topic:    "sensor/temp",
			Payload:  []byte("42.7"),
			QoS:      1,
			PacketID: 100,
		})

		// Receive PUBACK from client.
		pkt, err = dec.ReadPacket()
		if err == nil {
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	got := make(chan string, 1)
	_, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}},
		func(m *Message) {
			got <- m.Topic + ":" + string(m.Payload)
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-got:
		if msg != "sensor/temp:42.7" {
			t.Errorf("got %q, want sensor/temp:42.7", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestUnsubscribe(t *testing.T) {
	subSeen := atomic.Int32{}
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// SUBSCRIBE → SUBACK
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			pkt.Release()
			return
		}
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    sub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		pkt.Release()
		subSeen.Add(1)

		// UNSUBSCRIBE → UNSUBACK
		pkt, err = dec.ReadPacket()
		if err != nil {
			return
		}
		unsub, ok := pkt.(*wire.Unsubscribe)
		if !ok {
			pkt.Release()
			return
		}
		_, _ = wire.WriteUnsuback(c, wire.UnsubackOpts{
			PacketID:    unsub.PacketID,
			ReasonCodes: []wire.ReasonCode{wire.ReasonSuccess},
		})
		pkt.Release()
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	token, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "topic/x", QoS: 1}}, func(*Message) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if subSeen.Load() != 1 {
		t.Errorf("subSeen = %d, want 1", subSeen.Load())
	}

	if err := cli.Unsubscribe(context.Background(), token); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
}

// ---------------- Not-connected guards ----------------

func TestPublishWithoutConnect(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"))
	err := cli.Publish(context.Background(), wire.PublishOpts{Topic: "x", QoS: 0})
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("got %v, want ErrNotConnected", err)
	}
}

func TestDialFailure(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://127.0.0.1:1"), WithConnectTimeout(200*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := cli.Connect(ctx); err == nil {
		t.Fatal("expected dial failure")
	}
}

// brokerCapabilityHandler returns a fakeBroker handler that completes
// the CONNECT with a CONNACK carrying the supplied capability flags
// (nil means "absent", which per spec defaults to supported). It then
// holds the connection open so Subscribe-time validations can run.
func brokerCapabilityHandler(t *testing.T, shared, wildcard, subID *byte) func(*fakeBroker, net.Conn) {
	t.Helper()
	return func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("CONNECT read: %v", err)
			return
		}
		pkt.Release()
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode:                      wire.ReasonSuccess,
			SharedSubscriptionAvailable:     shared,
			WildcardSubscriptionAvailable:   wildcard,
			SubscriptionIdentifierAvailable: subID,
		}); err != nil {
			t.Errorf("CONNACK write: %v", err)
			return
		}
		<-fb.Done()
	}
}

func TestSubscribeBlockedBySharedSubsUnsupported(t *testing.T) {
	disabled := byte(0)
	fb := newFakeBroker(t, brokerCapabilityHandler(t, &disabled, nil, nil))

	cli, _ := New(WithBroker(fb.URL()), WithClientID("shared-sub-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "$share/g/sport/+", QoS: 1}}, SubBuffer(16))
	if !errors.Is(err, ErrSharedSubsUnsupported) {
		t.Fatalf("Subscribe shared: got %v, want ErrSharedSubsUnsupported", err)
	}
}

func TestSubscribeBlockedByWildcardSubsUnsupported(t *testing.T) {
	disabled := byte(0)
	fb := newFakeBroker(t, brokerCapabilityHandler(t, nil, &disabled, nil))

	cli, _ := New(WithBroker(fb.URL()), WithClientID("wildcard-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sport/+", QoS: 1}}, SubBuffer(16))
	if !errors.Is(err, ErrWildcardSubsUnsupported) {
		t.Fatalf("Subscribe wildcard: got %v, want ErrWildcardSubsUnsupported", err)
	}
}

func TestSubscribeAllowedWhenBrokerSupports(t *testing.T) {
	// Default behaviour: broker omits the properties => supported.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Accept the SUBSCRIBE so the test path completes.
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub, ok := pkt.(*wire.Subscribe)
		if !ok {
			t.Errorf("expected SUBSCRIBE, got %s", pkt.Type())
			pkt.Release()
			return
		}
		id := sub.PacketID
		pkt.Release()
		_, _ = wire.WriteSuback(c, wire.SubackOpts{
			PacketID:    id,
			ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
		})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()), WithClientID("default-caps-test"))
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	_, _, err := cli.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sport/+", QoS: 1}}, SubBuffer(16))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
}

func TestOnServerDisconnect(t *testing.T) {
	// Broker accepts the CONNECT, then immediately sends a
	// DISCONNECT with ServerMoved + ServerReference so the test can
	// verify both the callback firing and the ability to redirect
	// via SetBrokers.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		_, _ = wire.WriteDisconnect(c, wire.DisconnectOpts{
			ReasonCode:      wire.ReasonServerMoved,
			ServerReference: "mqtt://elsewhere.example:1883",
			ReasonString:    "moved",
		})
		<-fb.Done()
	})

	var (
		got      atomic.Pointer[wire.Disconnect]
		fired    atomic.Int32
		callDone = make(chan struct{}, 1)
	)
	cli, err := New(
		WithBroker(fb.URL()),
		WithClientID("server-disco-test"),
		WithOnServerDisconnect(func(d *wire.Disconnect) {
			got.Store(d)
			fired.Add(1)
			select {
			case callDone <- struct{}{}:
			default:
			}
		}),
		// Tight backoff so the supervisor races to reconnect, which
		// we don't want — fail any further dial since we redirected
		// to a bogus address. The test only cares about the first
		// callback firing.
		WithReconnectBackoff(ConstantBackoff(50*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-callDone:
	case <-time.After(2 * time.Second):
		t.Fatal("OnServerDisconnect callback not fired")
	}

	d := got.Load()
	if d == nil {
		t.Fatal("callback received nil")
	}
	if d.ReasonCode != wire.ReasonServerMoved {
		t.Fatalf("ReasonCode = 0x%02X, want ReasonServerMoved", byte(d.ReasonCode))
	}
	ref, ok := d.Properties.String(wire.PropServerReference)
	if !ok || ref != "mqtt://elsewhere.example:1883" {
		t.Fatalf("ServerReference = %q (ok=%v), want elsewhere", ref, ok)
	}
	if fired.Load() != 1 {
		t.Fatalf("callback fired %d times, want 1", fired.Load())
	}
}

func TestSetBrokers(t *testing.T) {
	cli, _ := New(WithBroker("mqtt://a:1883"))
	// brokerIdx is randomised at New() (§3 multi-broker rotation),
	// so pin a known value to assert specific URL selection.
	cli.brokerIdx.Store(0)

	if err := cli.SetBrokers("mqtt://b:1883", "mqtt://c:1883"); err != nil {
		t.Fatal(err)
	}
	if got := cli.currentBrokerURL(); got != "mqtt://b:1883" {
		t.Fatalf("currentBrokerURL = %q, want mqtt://b:1883", got)
	}
	cli.brokerIdx.Store(1)
	if got := cli.currentBrokerURL(); got != "mqtt://c:1883" {
		t.Fatalf("brokerIdx=1: currentBrokerURL = %q, want mqtt://c:1883", got)
	}

	if err := cli.SetBrokers(); !errors.Is(err, ErrMissingBroker) {
		t.Fatalf("empty SetBrokers: got %v, want ErrMissingBroker", err)
	}
	if err := cli.SetBrokers("mqtt://d:1883", ""); err == nil {
		t.Fatal("expected error for empty URL in list")
	}
}

func TestWithDialFunc(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	var called atomic.Int32
	var seenScheme string
	dial := func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		called.Add(1)
		seenScheme = u.Scheme
		// The "custom" scheme proves DialFunc replaces the built-in
		// scheme dispatch; we ignore the URL and dial the fake
		// broker's actual TCP address.
		return net.Dial("tcp", fb.addr)
	}

	cli, err := New(
		WithBroker("custom://ignored"),
		WithDialFunc(dial),
		WithClientID("dialfunc-test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if called.Load() != 1 {
		t.Fatalf("DialFunc called %d times, want 1", called.Load())
	}
	if seenScheme != "custom" {
		t.Fatalf("DialFunc received scheme %q, want %q", seenScheme, "custom")
	}
}

// silence "imported and not used" for io if we trim a test later
var _ = io.EOF
