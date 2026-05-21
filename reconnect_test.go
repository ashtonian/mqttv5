// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// TestReconnectAfterDrop simulates a broker that drops the connection
// after the handshake on the first attempt and accepts the second.
// Verifies the supervisor reconnects and Connected() returns true.
// TestMultiBrokerFailover verifies the supervisor rotates through the
// BrokerURLs list on reconnect. The first broker is dialled, drops the
// connection, the supervisor advances to the second broker for the
// next attempt.
func TestMultiBrokerFailover(t *testing.T) {
	connsA := atomic.Int32{}
	connsB := atomic.Int32{}
	acceptedOnB := make(chan struct{}, 1)

	fbA := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		connsA.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		// Drop right after the first CONNACK so the supervisor
		// rotates to broker B for the next attempt.
	})
	fbB := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		connsB.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		select {
		case acceptedOnB <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	cli, err := New(
		WithBrokers(fbA.URL(), fbB.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	// Force initial pick to be A. brokerIdx.Load() % 2 must equal 0.
	// Set to a known value then verify currentBrokerURL.
	cli.brokerIdx.Store(0)
	if cli.currentBrokerURL() != fbA.URL() {
		t.Fatalf("currentBrokerURL = %q, want %q", cli.currentBrokerURL(), fbA.URL())
	}

	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-acceptedOnB:
	case <-time.After(2 * time.Second):
		t.Fatalf("did not failover to broker B within 2s (connsA=%d connsB=%d)",
			connsA.Load(), connsB.Load())
	}

	if cli.currentBrokerURL() != fbB.URL() {
		t.Fatalf("after failover, currentBrokerURL = %q, want %q",
			cli.currentBrokerURL(), fbB.URL())
	}
}

func TestReconnectAfterDrop(t *testing.T) {
	connNum := atomic.Int32{}
	secondAccepted := make(chan struct{}, 1)

	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		if n == 1 {
			// Drop right after CONNACK.
			return
		}
		select {
		case secondAccepted <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case <-secondAccepted:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not reconnect within 2s")
	}

	// Give the runConnection a moment to update Connected().
	deadline := time.Now().Add(time.Second)
	for !cli.Connected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after reconnect")
	}
}

// TestResubscribeOnReconnect verifies the supervisor re-issues
// SUBSCRIBE after a reconnect so prior subscriptions remain active.
func TestResubscribeOnReconnect(t *testing.T) {
	subsSeen := make(chan uint16, 8)
	connNum := atomic.Int32{}

	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if sub, ok := pkt.(*wire.Subscribe); ok {
				subsSeen <- sub.PacketID
				_, _ = wire.WriteSuback(c, wire.SubackOpts{
					PacketID:    sub.PacketID,
					ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
				})
				pkt.Release()
				if n == 1 {
					return // drop after first SUBSCRIBE
				}
				continue
			}
			pkt.Release()
		}
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if _, err := cli.SubscribeCallback(context.Background(),
		[]TopicFilter{{Topic: "topic/x", QoS: 1}},
		func(*Message) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// First subscribe.
	select {
	case <-subsSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not see initial SUBSCRIBE")
	}

	// After the drop + reconnect, resubscribeAll should re-issue the SUBSCRIBE.
	select {
	case <-subsSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not resubscribe after reconnect")
	}
}

// TestReplayQoS1OnReconnect verifies that a QoS 1 publish in-flight
// when the broker drops gets replayed (with DUP) after reconnect and
// the original Publish() call completes successfully.
func TestReplayQoS1OnReconnect(t *testing.T) {
	connNum := atomic.Int32{}
	publishesSeen := make(chan *publishSeen, 8)

	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		n := connNum.Add(1)
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			if pub, ok := pkt.(*wire.Publish); ok {
				publishesSeen <- &publishSeen{
					connNum: int(n),
					id:      pub.PacketID,
					dup:     pub.Dup,
					topic:   string([]byte(pub.Topic)),
				}
				id := pub.PacketID
				pkt.Release()
				if n == 1 {
					// First connection: read PUBLISH but DROP before PUBACK.
					return
				}
				// Second connection: ACK the replay.
				_, _ = wire.WritePuback(c, wire.PubRespOpts{PacketID: id})
				continue
			}
			pkt.Release()
		}
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(20*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	pubDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		pubDone <- cli.Publish(ctx, wire.PublishOpts{
			Topic:   "events/important",
			Payload: []byte("hello"),
			QoS:     1,
		})
	}()

	// First broker connection should see the PUBLISH (DUP=false).
	first, err := waitForPublish(publishesSeen, 2*time.Second)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if first.connNum != 1 || first.dup {
		t.Errorf("first publish: connNum=%d dup=%v (want 1, false)", first.connNum, first.dup)
	}

	// After reconnect, supervisor replays — DUP must be set.
	second, err := waitForPublish(publishesSeen, 3*time.Second)
	if err != nil {
		t.Fatalf("replay publish: %v", err)
	}
	if second.connNum < 2 || !second.dup {
		t.Errorf("replay: connNum=%d dup=%v (want >=2, true)", second.connNum, second.dup)
	}
	if second.id != first.id {
		t.Errorf("replay packet id changed: first=%d replay=%d", first.id, second.id)
	}

	// The original Publish() call must complete with no error.
	select {
	case err := <-pubDone:
		if err != nil {
			t.Errorf("Publish() returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish() did not complete after replay+PUBACK")
	}
}

// publishSeen is a per-connection record of a PUBLISH the broker saw.
type publishSeen struct {
	connNum int
	id      uint16
	dup     bool
	topic   string
}

func waitForPublish(ch <-chan *publishSeen, d time.Duration) (*publishSeen, error) {
	select {
	case p := <-ch:
		return p, nil
	case <-time.After(d):
		return nil, errors.New("timeout waiting for publish")
	}
}

// silence unused-import linter warnings on future test additions
var _ = io.EOF
