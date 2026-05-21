// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// TestNextPingWait exercises the deadline math without spinning up a
// connection. It's the unit-test part of the idle-PINGREQ scheduler.
func TestNextPingWait(t *testing.T) {
	keep := time.Second
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		lastWrite time.Time
		lastRead  time.Time
		now       time.Time
		wantMin   time.Duration
		wantMax   time.Duration
	}{
		{
			name:      "both fresh - wait full keepalive",
			lastWrite: base,
			lastRead:  base,
			now:       base,
			wantMin:   900 * time.Millisecond,
			wantMax:   keep,
		},
		{
			name:      "outbound stale, inbound fresh - fire soon",
			lastWrite: base.Add(-2 * keep),
			lastRead:  base,
			now:       base,
			wantMin:   1,
			wantMax:   10 * time.Millisecond,
		},
		{
			name:      "inbound stale, outbound fresh - fire soon",
			lastWrite: base,
			lastRead:  base.Add(-2 * keep),
			now:       base,
			wantMin:   1,
			wantMax:   10 * time.Millisecond,
		},
		{
			name:      "both stale - fire immediately (clamped)",
			lastWrite: base.Add(-3 * keep),
			lastRead:  base.Add(-3 * keep),
			now:       base,
			wantMin:   1,
			wantMax:   10 * time.Millisecond,
		},
		{
			name:      "earlier deadline dominates",
			lastWrite: base.Add(-500 * time.Millisecond),
			lastRead:  base,
			now:       base,
			wantMin:   450 * time.Millisecond,
			wantMax:   550 * time.Millisecond,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := &connState{}
			cs.lastWriteUnixNano.Store(tc.lastWrite.UnixNano())
			cs.lastReadUnixNano.Store(tc.lastRead.UnixNano())
			got := nextPingWait(cs, keep, tc.now)
			if got < tc.wantMin || got > tc.wantMax {
				t.Fatalf("nextPingWait = %v, want in [%v, %v]", got, tc.wantMin, tc.wantMax)
			}
		})
	}
}

// pingCounterBroker is a fake broker handler that counts inbound
// PINGREQ packets. Replies with PINGRESP unless dropPing is true.
type pingCounterBroker struct {
	pings    atomic.Int32
	dropPing atomic.Bool
}

func (p *pingCounterBroker) serve(t *testing.T, fb *fakeBroker, c net.Conn) {
	t.Helper()
	defer c.Close()
	dec := wire.NewDecoder(c)
	acceptConnect(t, c, dec)
	for {
		select {
		case <-fb.Done():
			return
		default:
		}
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		switch pkt.Type() {
		case wire.PINGREQ:
			p.pings.Add(1)
			pkt.Release()
			if p.dropPing.Load() {
				continue
			}
			if _, err := wire.WritePingresp(c); err != nil {
				return
			}
		case wire.PUBLISH:
			pub := pkt.(*wire.Publish)
			// Auto-ack QoS 1 publishes so the client's read window
			// stays fresh — this is the codepath the idle scheduler
			// is designed to take advantage of.
			if pub.QoS == 1 {
				_, _ = wire.WritePuback(c, wire.PubRespOpts{
					PacketID:   pub.PacketID,
					ReasonCode: wire.ReasonSuccess,
				})
			}
			pkt.Release()
		case wire.DISCONNECT:
			pkt.Release()
			return
		default:
			pkt.Release()
		}
	}
}

// TestIdlePING_BusyConnectionSkipsPINGREQ verifies that a connection
// publishing QoS 1 (and therefore receiving PUBACKs back) emits zero
// PINGREQs over several KeepAlive windows. This is the central
// optimisation: outbound + inbound traffic together cover both halves
// of §3.1.2.10.
func TestIdlePING_BusyConnectionSkipsPINGREQ(t *testing.T) {
	t.Parallel()
	pb := &pingCounterBroker{}
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		pb.serve(t, fb, c)
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithKeepAlive(1), // 1 s keepalive
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// Run busy traffic for ~2.5 keepalive windows.
	stop := time.After(2500 * time.Millisecond)
	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-tick.C:
			err := cli.Publish(context.Background(), wire.PublishOpts{
				Topic:   "busy/topic",
				Payload: []byte("x"),
				QoS:     1,
			})
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
	}

	if got := pb.pings.Load(); got != 0 {
		t.Fatalf("PINGREQ count = %d, want 0 (busy connection should skip)", got)
	}
}

// TestIdlePING_IdleConnectionStillSendsPINGREQ verifies the fallback:
// a connection with no application traffic still fires PINGREQ on
// schedule so the broker doesn't disconnect us.
func TestIdlePING_IdleConnectionStillSendsPINGREQ(t *testing.T) {
	t.Parallel()
	pb := &pingCounterBroker{}
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		pb.serve(t, fb, c)
	})

	cli, err := New(
		WithBroker(fb.URL()),
		WithKeepAlive(1),                      // 1 s keepalive
		WithPingTimeout(200*time.Millisecond), // override 10 s default so cycles complete in test window
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// No traffic at all — expect PINGREQ to fire on the keepalive
	// cadence. Wait for ~2.5 windows then check.
	time.Sleep(2500 * time.Millisecond)

	if got := pb.pings.Load(); got < 2 {
		t.Fatalf("PINGREQ count = %d over 2.5 KeepAlive windows, want >= 2", got)
	}
}

// TestIdlePING_TimeoutStillDetectsHalfOpen verifies that the PINGRESP
// timeout path still works after the rewrite: if the broker silently
// drops PINGREQs, the connection is torn down within PingTimeout.
func TestIdlePING_TimeoutStillDetectsHalfOpen(t *testing.T) {
	t.Parallel()
	pb := &pingCounterBroker{}
	pb.dropPing.Store(true)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		pb.serve(t, fb, c)
	})

	downSignaled := make(chan struct{}, 1)
	cli, err := New(
		WithBroker(fb.URL()),
		WithKeepAlive(1),
		WithPingTimeout(300*time.Millisecond),
		WithReconnectBackoff(ConstantBackoff(time.Hour)), // don't reconnect during the test
		WithOnConnectionDown(func() bool {
			select {
			case downSignaled <- struct{}{}:
			default:
			}
			return true
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	// Wait for one keepalive + ping timeout window, plus generous slack.
	select {
	case <-downSignaled:
	case <-time.After(3 * time.Second):
		t.Fatalf("OnConnectionDown not fired within 3s after broker dropped PINGRESP")
	}
}
