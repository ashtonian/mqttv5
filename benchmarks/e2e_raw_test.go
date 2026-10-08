// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// rawClient is a minimal MQTT v5 client written directly on the wire
// package, used as the load generator and observer in scenarios that
// measure one side of a library. It is the same for every library under
// test and costs little per message: publishes are pre-encoded, stamped
// with their sequence number (see stampSeq) and written in batches,
// without allocating. It runs in the benchmark process, so its CPU time
// is part of cpu-ns/op. A rawClient either publishes or subscribes.
type rawClient struct {
	b    *testing.B
	conn net.Conn
	dec  *wire.Decoder

	// window is the broker's Receive Maximum; credits holds one token
	// per QoS 1 publish the broker may still accept, and stall bounds
	// the wait for one.
	window  int
	credits chan struct{}
	stall   *time.Timer

	// onPublish is called on the reader goroutine for each PUBLISH a
	// subscribing rawClient receives.
	onPublish func(p *wire.Publish)
	subacked  chan struct{}

	readErr atomic.Pointer[error]
	closing atomic.Bool
	done    chan struct{}
}

func dialRaw(b *testing.B, id string) *rawClient {
	b.Helper()
	conn, err := net.DialTimeout("tcp", brokerAddr(b), 5*time.Second)
	if err != nil {
		b.Fatalf("raw dial: %v", err)
	}
	if _, err := wire.WriteConnect(conn, wire.ConnectOpts{ClientID: id, CleanStart: true}); err != nil {
		b.Fatalf("raw CONNECT: %v", err)
	}
	dec := wire.NewDecoderSize(conn, 64<<10)
	pkt, err := dec.ReadPacket()
	if err != nil {
		b.Fatalf("raw CONNACK: %v", err)
	}
	ack, ok := pkt.(*wire.Connack)
	if !ok || ack.ReasonCode != wire.ReasonSuccess {
		b.Fatalf("raw CONNECT refused: %v", pkt)
	}
	window := 65535
	if v, ok := ack.Properties.Uint16(wire.PropReceiveMaximum); ok {
		window = int(v)
	}
	pkt.Release()
	c := &rawClient{
		b: b, conn: conn, dec: dec, window: window,
		credits:  make(chan struct{}, window),
		stall:    stoppedTimer(),
		subacked: make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	for range window {
		c.credits <- struct{}{}
	}
	go c.read()
	b.Cleanup(c.close)
	return c
}

func (c *rawClient) read() {
	defer close(c.done)
	for {
		pkt, err := c.dec.ReadPacket()
		if err != nil {
			if !c.closing.Load() {
				c.readErr.Store(&err)
			}
			return
		}
		switch p := pkt.(type) {
		case *wire.PubResp:
			if p.Type() == wire.PUBACK {
				c.credits <- struct{}{}
			}
		case *wire.Publish:
			if c.onPublish != nil {
				c.onPublish(p)
			}
			if p.QoS == 1 {
				_, err = wire.WritePuback(c.conn, wire.PubRespOpts{PacketID: p.PacketID})
			}
		case *wire.Suback:
			c.subacked <- struct{}{}
		}
		pkt.Release()
		if err != nil {
			c.readErr.Store(&err)
			return
		}
	}
}

// err reports why the reader stopped, if it did.
func (c *rawClient) err() error {
	if p := c.readErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (c *rawClient) close() {
	c.closing.Store(true)
	_, _ = wire.WriteDisconnect(c.conn, wire.DisconnectOpts{})
	_ = c.conn.Close()
	<-c.done
}

// subscribe subscribes to filter at qos and calls fn for every PUBLISH
// received, on the reader goroutine.
func (c *rawClient) subscribe(filter string, qos byte, fn func(p *wire.Publish)) {
	c.b.Helper()
	c.onPublish = fn
	if _, err := wire.WriteSubscribe(c.conn, wire.SubscribeOpts{
		PacketID: 1, Filters: []wire.SubscribeFilter{{Topic: filter, QoS: qos}},
	}); err != nil {
		c.b.Fatalf("raw SUBSCRIBE: %v", err)
	}
	select {
	case <-c.subacked:
	case <-time.After(5 * time.Second):
		c.b.Fatalf("raw SUBSCRIBE: no SUBACK (%v)", c.err())
	}
}

// rawBatch bounds how many publishes one write carries.
const rawBatch = 64

// publish sends n messages of payload to topic at qos as fast as the
// broker accepts them, keeping at most the broker's Receive Maximum of
// QoS 1 messages unacknowledged. Message i carries sequence number i. It
// returns once every message has been written; QoS 1 acknowledgements
// may still be outstanding.
func (c *rawClient) publish(topic string, qos byte, payload []byte, n int) error {
	ids := 1
	if qos > 0 {
		// Cycle through more packet identifiers than the window, so an
		// identifier is acknowledged long before it is used again.
		ids = min(4*c.window, 65535)
	}
	frames := make([][]byte, ids)
	for i := range frames {
		o := wire.PublishOpts{Topic: topic, QoS: qos, Payload: payload}
		if qos > 0 {
			o.PacketID = uint16(i + 1)
		}
		bp, err := wire.EncodePublish(o)
		if err != nil {
			return err
		}
		frames[i] = append([]byte(nil), *bp...)
		wire.ReleaseBuf(bp)
	}
	body := len(frames[0]) - len(payload)
	w := bufio.NewWriterSize(c.conn, rawBatch*len(frames[0]))
	next := 0
	for sent := 0; sent < n; {
		k := min(rawBatch, n-sent)
		if qos > 0 {
			k = takeCredits(c.credits, c.stall, k)
			if k == 0 {
				return fmt.Errorf("raw publisher stalled after %d of %d: %v", sent, n, c.err())
			}
		}
		for i := range k {
			stampSeq(frames[next][body:], uint64(sent+i))
			if _, err := w.Write(frames[next]); err != nil {
				return err
			}
			next = (next + 1) % ids
		}
		if err := w.Flush(); err != nil {
			return err
		}
		sent += k
	}
	return nil
}

// creditStall is how long a source waits for a send credit before it
// gives up.
const creditStall = 30 * time.Second

func stoppedTimer() *time.Timer {
	t := time.NewTimer(creditStall)
	t.Stop()
	return t
}

// takeCredits blocks for one send credit and then takes up to max-1
// more without blocking. It returns 0 if no credit arrives within
// creditStall, which it measures with stall, a stopped timer, to avoid
// allocating one per wait.
func takeCredits(credits chan struct{}, stall *time.Timer, max int) int {
	select {
	case <-credits:
	default:
		stall.Reset(creditStall)
		select {
		case <-credits:
			stall.Stop()
		case <-stall.C:
			return 0
		}
	}
	got := 1
	for got < max {
		select {
		case <-credits:
			got++
		default:
			return got
		}
	}
	return got
}

// publishAsync runs publish on its own goroutine; wait returns its error.
func (c *rawClient) publishAsync(topic string, qos byte, payload []byte, n int) (wait func() error) {
	var wg sync.WaitGroup
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = c.publish(topic, qos, payload, n)
	}()
	return func() error {
		wg.Wait()
		return err
	}
}
