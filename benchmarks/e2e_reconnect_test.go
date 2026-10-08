// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// replayWindow is how many QoS 1 publishes are in flight when the
// connection is cut: the benchmark broker's Receive Maximum.
const replayWindow = 20

// BenchmarkE2E_Reconnect measures session resumption. Each iteration
// lets replayWindow QoS 1 publishes leave the client and vanish (a proxy
// between client and broker swallows them), then cuts the connection.
// ns/op is the time from the cut until a subscriber has received every
// one of them: detecting the loss, reconnecting with the session
// (CleanStart=0) and resending the publishes with DUP set.
// publish-errors/op counts Publish calls that reported an error although
// the message was delivered after the reconnect.
func BenchmarkE2E_Reconnect(b *testing.B) {
	requireBroker(b)
	for _, l := range libs {
		if !l.v5 {
			continue // the MQTT 3.1.1 client's session resumption is not compared
		}
		b.Run(fmt.Sprintf("lib=%s/inflight=%d/size=%s", l.name, replayWindow, size256B.name), func(b *testing.B) {
			runReconnect(b, l)
		})
	}
}

func runReconnect(b *testing.B, l lib) {
	topic := "bench/reconnect/" + uniqueID("")
	frame, err := wire.EncodePublish(wire.PublishOpts{Topic: topic, QoS: 1, PacketID: 1, Payload: Payload(size256B.bytes)})
	if err != nil {
		b.Fatal(err)
	}
	frameLen := len(*frame)
	wire.ReleaseBuf(frame)

	// Each payload carries its sequence number, so the sink can tell a
	// redelivery, which QoS 1 allows after the reconnect, from a first
	// delivery.
	total := b.N * replayWindow
	payloads := make([][]byte, total)
	for i := range payloads {
		payloads[i] = Payload(size256B.bytes)
		stampSeq(payloads[i], uint64(i))
	}
	s := newSink(size256B.bytes, total)
	s.allowDups = true
	roundDone := make(chan struct{}, 1)
	sub := dialRaw(b, uniqueID("raw-sub"))
	sub.subscribe(topic, 0, func(p *wire.Publish) {
		if s.accept(p.Payload) && s.got.Load()%replayWindow == 0 {
			roundDone <- struct{}{}
		}
	})

	px := newProxy(b, brokerAddr(b))
	p := l.connect(b, clientConfig{id: uniqueID(l.name + "-resume"), addr: px.addr(), session: true})
	ctx := context.Background()
	waitLive(b, s, func() error { return p.publish(ctx, topic, 0, nil) })

	var publishErrors atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.StopTimer()
	for round := range b.N {
		px.swallow()
		var wg sync.WaitGroup
		for k := range replayWindow {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := p.publish(ctx, topic, 1, payloads[round*replayWindow+k]); err != nil {
					publishErrors.Add(1)
				}
			}()
		}
		px.awaitSwallowed(b, replayWindow*frameLen)

		b.StartTimer()
		px.cut()
		select {
		case <-roundDone:
		case <-s.invalid:
			b.Fatalf("round %d: invalid delivery; %s", round, s)
		case <-time.After(30 * time.Second):
			b.Fatalf("round %d: want %d messages; %s", round, (round+1)*replayWindow, s)
		}
		b.StopTimer()
		wg.Wait()
	}
	// The reader stops before the counts are read.
	sub.close()
	if err := s.verify(noDrops); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(publishErrors.Load())/float64(b.N), "publish-errors/op")
	b.ReportMetric(float64(s.dup.Load())/float64(b.N), "duplicates/op")
}

// proxy relays a client's connections to the broker. swallow makes the
// current connection discard what the client sends, so publishes leave
// the client but never reach the broker; cut closes it.
type proxy struct {
	b        *testing.B
	ln       net.Listener
	upstream string

	mu    sync.Mutex
	links []*link
}

// link is one relayed connection pair.
type link struct {
	client, broker net.Conn
	swallowing     atomic.Bool
	swallowed      atomic.Int64
}

func newProxy(b *testing.B, upstream string) *proxy {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	px := &proxy{b: b, ln: ln, upstream: upstream}
	go px.accept()
	b.Cleanup(func() {
		_ = ln.Close()
		px.cut()
	})
	return px
}

func (px *proxy) addr() string { return px.ln.Addr().String() }

func (px *proxy) accept() {
	for {
		client, err := px.ln.Accept()
		if err != nil {
			return
		}
		broker, err := net.Dial("tcp", px.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		lk := &link{client: client, broker: broker}
		px.mu.Lock()
		px.links = append(px.links, lk)
		px.mu.Unlock()
		go lk.pump(client, broker, nil)
		go lk.pump(broker, client, lk)
	}
}

// pump copies src to dst. Traffic from the client (swallow != nil) is
// discarded while the link swallows.
func (lk *link) pump(dst, src net.Conn, swallow *link) {
	defer dst.Close()
	buf := make([]byte, 64<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if swallow != nil && swallow.swallowing.Load() {
				swallow.swallowed.Add(int64(n))
			} else if _, err := dst.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (px *proxy) current() *link {
	px.mu.Lock()
	defer px.mu.Unlock()
	if len(px.links) == 0 {
		px.b.Fatal("proxy: no connection")
	}
	return px.links[len(px.links)-1]
}

func (px *proxy) swallow() { px.current().swallowing.Store(true) }

// awaitSwallowed waits until the current link discarded n bytes.
func (px *proxy) awaitSwallowed(b *testing.B, n int) {
	b.Helper()
	lk := px.current()
	deadline := time.Now().Add(10 * time.Second)
	for lk.swallowed.Load() < int64(n) {
		if time.Now().After(deadline) {
			b.Fatalf("the client sent %d of %d bytes", lk.swallowed.Load(), n)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// cut closes every relayed connection.
func (px *proxy) cut() {
	px.mu.Lock()
	links := px.links
	px.links = nil
	px.mu.Unlock()
	for _, lk := range links {
		_ = lk.client.Close()
		_ = lk.broker.Close()
	}
}
