// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build e2e

package benchmarks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	paho3 "github.com/eclipse/paho.mqtt.golang"

	"github.com/ashtonian/mqttv5"
)

// publisher is one connected client of a library under test.
type publisher interface {
	publish(ctx context.Context, topic string, qos byte, payload []byte) error
}

type clientConfig struct {
	id string
	// addr is the broker's host:port; empty means brokerAddr.
	addr string
	// written makes a QoS 0 publish return once the packet is written
	// to the connection. mqttv5 otherwise returns when the packet is
	// queued for its writer; the other libraries always wait.
	written bool
	// session keeps the session across reconnects (Session Expiry
	// 300 s) and reconnects after 1 ms. Without it every library
	// connects with Clean Start and Session Expiry 0.
	session bool
	// receiveMaximum is advertised by MQTT 5 clients; 0 leaves the
	// library's default.
	receiveMaximum uint16
}

func (cfg clientConfig) host(b *testing.B) string {
	if cfg.addr != "" {
		return cfg.addr
	}
	return brokerAddr(b)
}

// mode is how a subscriber receives messages: a handler the library
// calls (callback), the library's channel (chan), or its queue (queue).
type mode string

const (
	modeCallback mode = "callback"
	modeChan     mode = "chan"
	modeQueue    mode = "queue"
)

// lib is one client library under test.
type lib struct {
	name string
	// v5 is false for the MQTT 3.1.1 library.
	v5 bool
	// modes are the delivery modes subscribe supports.
	modes   []mode
	connect func(b *testing.B, cfg clientConfig) publisher
	// subscribe connects a subscriber to filter and passes each
	// message's payload to onMsg through mode — from consumers
	// goroutines for chan and queue. onMsg may block. A QoS 1/2 message
	// is acknowledged when a consumer takes it, before onMsg, in every
	// library and mode. The returned function counts messages the
	// library discarded for want of room. The consumers end with the
	// benchmark.
	subscribe func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) (dropped func() int64)
}

var libs = []lib{
	{name: "mqttv5", v5: true, modes: []mode{modeCallback, modeChan, modeQueue}, connect: connectMqttv5, subscribe: subscribeMqttv5},
	{name: "autopaho", v5: true, modes: []mode{modeCallback, modeChan}, connect: connectAutopaho, subscribe: subscribeAutopaho},
	{name: "paho3", v5: false, modes: []mode{modeCallback}, connect: connectPaho3, subscribe: subscribePaho3},
}

func (l lib) supports(m mode) bool {
	for _, x := range l.modes {
		if x == m {
			return true
		}
	}
	return false
}

// ---------------- mqttv5 ----------------

type mqttv5Publisher struct{ c *mqttv5.Client }

func (p mqttv5Publisher) publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	return p.c.Publish(ctx, mqttv5.PublishOptions{Topic: topic, QoS: qos, Payload: payload})
}

func newMqttv5(b *testing.B, cfg clientConfig) *mqttv5.Client {
	b.Helper()
	opts := []mqttv5.Option{
		mqttv5.WithBroker("mqtt://" + cfg.host(b)),
		mqttv5.WithClientID(cfg.id),
		mqttv5.WithKeepAlive(60),
		mqttv5.WithConnectTimeout(5 * time.Second),
		mqttv5.WithLogger(quietLogger),
	}
	if cfg.written {
		opts = append(opts, mqttv5.WithPublishMode(mqttv5.PublishWaitForFlush))
	}
	if cfg.session {
		opts = append(opts, mqttv5.WithSessionExpiry(300),
			mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(time.Millisecond)))
	} else {
		// mqttv5 keeps sessions for DefaultSessionExpiry; the other
		// libraries end them with the connection.
		opts = append(opts, mqttv5.WithSessionExpiry(0))
	}
	if cfg.receiveMaximum > 0 {
		opts = append(opts, mqttv5.WithReceiveMaximum(cfg.receiveMaximum))
	}
	cli, err := mqttv5.New(opts...)
	if err != nil {
		b.Fatalf("mqttv5 New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		b.Fatalf("mqttv5 Connect: %v", err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cli.Disconnect(ctx)
	})
	return cli
}

func connectMqttv5(b *testing.B, cfg clientConfig) publisher {
	return mqttv5Publisher{newMqttv5(b, cfg)}
}

func subscribeMqttv5(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) func() int64 {
	b.Helper()
	// Registered first, so it runs after the client's Disconnect, which
	// ends the consumers.
	var wg sync.WaitGroup
	b.Cleanup(wg.Wait)
	cli := newMqttv5(b, cfg)
	var dropped atomic.Int64
	filters := []mqttv5.TopicFilter{{Topic: filter, QoS: qos}}
	onDrop := mqttv5.SubOnDrop(func(*mqttv5.Message) { dropped.Add(1) })
	ctx := context.Background()
	switch m {
	case modeCallback:
		if _, err := cli.SubscribeCallback(ctx, filters, func(msg *mqttv5.Message) { onMsg(msg.Payload) }); err != nil {
			b.Fatalf("mqttv5 SubscribeCallback: %v", err)
		}
	case modeChan:
		ch, _, err := cli.Subscribe(ctx, filters, onDrop)
		if err != nil {
			b.Fatalf("mqttv5 Subscribe: %v", err)
		}
		for range consumers {
			wg.Go(func() {
				for msg := range ch {
					_ = msg.Ack()
					onMsg(msg.Payload)
				}
			})
		}
	case modeQueue:
		q, _, err := cli.SubscribeQueue(ctx, filters, onDrop)
		if err != nil {
			b.Fatalf("mqttv5 SubscribeQueue: %v", err)
		}
		for range consumers {
			wg.Go(func() {
				for {
					msg, ok := q.Dequeue(ctx)
					if !ok {
						return
					}
					_ = msg.Ack()
					onMsg(msg.Payload)
				}
			})
		}
	default:
		b.Fatalf("mqttv5: no mode %q", m)
	}
	return dropped.Load
}

// ---------------- eclipse/paho.golang autopaho ----------------

type autopahoPublisher struct{ cm *autopaho.ConnectionManager }

func (p autopahoPublisher) publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	_, err := p.cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Payload: payload})
	return err
}

func newAutopaho(b *testing.B, cfg clientConfig, onPublish func(paho.PublishReceived) (bool, error)) *autopaho.ConnectionManager {
	b.Helper()
	u, err := url.Parse("mqtt://" + cfg.host(b))
	if err != nil {
		b.Fatal(err)
	}
	cc := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{u},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: true,
		ConnectTimeout:                5 * time.Second,
		ClientConfig:                  paho.ClientConfig{ClientID: cfg.id},
	}
	if cfg.session {
		cc.SessionExpiryInterval = 300
		cc.ReconnectBackoff = func(int) time.Duration { return time.Millisecond }
	}
	if cfg.receiveMaximum > 0 {
		rm := cfg.receiveMaximum
		cc.ConnectPacketBuilder = func(c *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if c.Properties == nil {
				c.Properties = &paho.ConnectProperties{}
			}
			c.Properties.ReceiveMaximum = &rm
			return c, nil
		}
	}
	if onPublish != nil {
		cc.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPublish}
	}
	// The connection manager lives until its context is cancelled.
	life, cancel := context.WithCancel(context.Background())
	cm, err := autopaho.NewConnection(life, cc)
	if err != nil {
		cancel()
		b.Fatalf("autopaho NewConnection: %v", err)
	}
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	if err := cm.AwaitConnection(ctx); err != nil {
		cancel()
		b.Fatalf("autopaho AwaitConnection: %v", err)
	}
	b.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = cm.Disconnect(ctx)
		cancel()
	})
	return cm
}

func connectAutopaho(b *testing.B, cfg clientConfig) publisher {
	return autopahoPublisher{newAutopaho(b, cfg, nil)}
}

func subscribeAutopaho(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) func() int64 {
	b.Helper()
	var handler func(paho.PublishReceived) (bool, error)
	var stop func()
	switch m {
	case modeCallback:
		handler = func(pr paho.PublishReceived) (bool, error) {
			onMsg(pr.Packet.Payload)
			return true, nil
		}
	case modeChan:
		// autopaho has no channel API; this is the forwarding an
		// application writes, matched to mqttv5's channel. autopaho
		// acknowledges a message when the handler returns, so at QoS 1/2
		// the handler waits for a consumer to take it, as mqttv5's
		// consumers acknowledge on taking a message; at QoS 0 it buffers
		// as many messages as mqttv5's channel does. A full channel
		// holds the handler back, as a library channel with
		// backpressure would.
		buffer := mqttv5.DefaultSubscribeBuffer
		if qos > 0 {
			buffer = 0
		}
		ch := make(chan *paho.Publish, buffer)
		done := make(chan struct{})
		handler = func(pr paho.PublishReceived) (bool, error) {
			select {
			case ch <- pr.Packet:
			case <-done:
			}
			return true, nil
		}
		var wg sync.WaitGroup
		for range consumers {
			wg.Go(func() {
				for {
					select {
					case p := <-ch:
						onMsg(p.Payload)
					case <-done:
						return
					}
				}
			})
		}
		stop = func() {
			close(done)
			wg.Wait()
		}
	default:
		b.Fatalf("autopaho: no mode %q", m)
	}
	cm := newAutopaho(b, cfg, handler)
	if stop != nil {
		// Runs before the connection's Disconnect, so a handler blocked
		// on the channel lets it finish.
		b.Cleanup(stop)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}},
	}); err != nil {
		b.Fatalf("autopaho Subscribe: %v", err)
	}
	return func() int64 { return 0 }
}

// ---------------- eclipse/paho.mqtt.golang (MQTT 3.1.1) ----------------

type paho3Publisher struct{ c paho3.Client }

var errPaho3Timeout = errors.New("paho3: no acknowledgement within 30s")

func (p paho3Publisher) publish(_ context.Context, topic string, qos byte, payload []byte) error {
	t := p.c.Publish(topic, qos, false, payload)
	if !t.WaitTimeout(30 * time.Second) {
		return errPaho3Timeout
	}
	return t.Error()
}

func newPaho3(b *testing.B, cfg clientConfig) paho3.Client {
	b.Helper()
	if cfg.session {
		b.Fatal("paho3: session resumption is not benchmarked")
	}
	o := paho3.NewClientOptions().
		AddBroker("tcp://" + cfg.host(b)).
		SetClientID(cfg.id).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetKeepAlive(60 * time.Second).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false).
		SetOrderMatters(true)
	c := paho3.NewClient(o)
	if t := c.Connect(); !t.WaitTimeout(10*time.Second) || t.Error() != nil {
		b.Fatalf("paho3 Connect: %v", t.Error())
	}
	b.Cleanup(func() { c.Disconnect(250) })
	return c
}

func connectPaho3(b *testing.B, cfg clientConfig) publisher {
	return paho3Publisher{newPaho3(b, cfg)}
}

func subscribePaho3(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, _ int, onMsg func([]byte)) func() int64 {
	b.Helper()
	if m != modeCallback {
		b.Fatalf("paho3: no mode %q", m)
	}
	c := newPaho3(b, cfg)
	t := c.Subscribe(filter, qos, func(_ paho3.Client, msg paho3.Message) { onMsg(msg.Payload()) })
	if !t.WaitTimeout(5*time.Second) || t.Error() != nil {
		b.Fatalf("paho3 Subscribe: %v", t.Error())
	}
	return func() int64 { return 0 }
}

// sink checks what a subscriber receives against what the raw sources
// sent: target messages of size bytes, Payload(size) each stamped with
// its sequence number at both ends (see stampSeq). It counts each
// sequence number once; a duplicate, a payload that differs anywhere
// from what was sent, or a message missing at the end fails the
// benchmark. A zero-length payload is a probe: it only shows that the
// subscription is live.
type sink struct {
	size    int
	target  int64
	pattern []byte          // the payload between the stamps
	seen    []atomic.Uint64 // one bit per sequence number
	got     atomic.Int64    // distinct messages
	dup     atomic.Int64
	wrong   atomic.Int64 // wrong size or content, unknown or torn sequence numbers
	all     atomic.Int64 // every delivery but probes
	ready   chan struct{}
	once    sync.Once
	done    chan struct{}
}

func newSink(size, target int) *sink {
	s := &sink{
		size: size, target: int64(target), seen: make([]atomic.Uint64, (target+63)/64),
		ready: make(chan struct{}), done: make(chan struct{}),
	}
	if size >= 2*seqBytes {
		s.pattern = Payload(size)[seqBytes : size-seqBytes]
	}
	return s
}

func (s *sink) onMsg(p []byte) {
	if len(p) == 0 {
		s.once.Do(func() { close(s.ready) })
		return
	}
	s.all.Add(1)
	seq, ok := readSeq(p)
	if len(p) != s.size || !ok || seq >= uint64(s.target) || !bytes.Equal(p[seqBytes:len(p)-seqBytes], s.pattern) {
		s.wrong.Add(1)
		return
	}
	bit := uint64(1) << (seq % 64)
	if s.seen[seq/64].Or(bit)&bit != 0 {
		s.dup.Add(1)
		return
	}
	if s.got.Add(1) == s.target {
		close(s.done)
	}
}

func (s *sink) String() string {
	return fmt.Sprintf("received %d of %d (%d duplicates, %d of the wrong size or content)",
		s.got.Load(), s.target, s.dup.Load(), s.wrong.Load())
}

// check fails the benchmark unless every message arrived exactly once
// and intact (see verify).
func (s *sink) check(b *testing.B, dropped func() int64) {
	b.Helper()
	if err := s.verify(dropped); err != nil {
		b.Fatal(err)
	}
}

// verify waits until deliveries have stopped — no new one for 50 ms,
// within a second — so one that trails the last expected message is
// counted too, then reports anything but every message once and intact.
func (s *sink) verify(dropped func() int64) error {
	last := s.all.Load()
	quiet := time.Now()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline) && time.Since(quiet) < 50*time.Millisecond; {
		time.Sleep(5 * time.Millisecond)
		if n := s.all.Load(); n != last {
			last, quiet = n, time.Now()
		}
	}
	if n := dropped(); n > 0 {
		return fmt.Errorf("subscriber dropped %d messages; a run with drops measures nothing", n)
	}
	if s.wrong.Load() > 0 || s.dup.Load() > 0 || s.got.Load() != s.target {
		return errors.New(s.String())
	}
	return nil
}

// seqBytes is the size of the sequence number stamped at each end of a
// payload; payloads under 2×seqBytes carry none.
const seqBytes = 8

// stampSeq writes seq into the first and last seqBytes of payload.
func stampSeq(payload []byte, seq uint64) {
	if len(payload) < 2*seqBytes {
		return
	}
	binary.BigEndian.PutUint64(payload, seq)
	binary.BigEndian.PutUint64(payload[len(payload)-seqBytes:], seq)
}

// readSeq returns the sequence number stampSeq wrote; false when the two
// ends disagree, as in a payload put together from two messages.
func readSeq(p []byte) (uint64, bool) {
	if len(p) < 2*seqBytes {
		return 0, false
	}
	head := binary.BigEndian.Uint64(p)
	return head, head == binary.BigEndian.Uint64(p[len(p)-seqBytes:])
}

// waitLive publishes zero-length probes until one reaches s, so the
// broker has installed the subscription before the measurement starts.
func waitLive(b *testing.B, s *sink, probe func() error) {
	b.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := probe(); err != nil {
			b.Fatalf("probe publish: %v", err)
		}
		select {
		case <-s.ready:
			return
		case <-tick.C:
		case <-deadline:
			b.Fatal("the subscription never received a probe")
		}
	}
}
