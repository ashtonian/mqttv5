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
	// is acknowledged when onMsg returns in callback mode, and when a
	// consumer takes it, before onMsg, in chan and queue mode, in every
	// library. Benchmarks call it through the package's subscribe.
	subscribe func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber
}

// subscriber is a library's subscriber as its adapter returns it.
type subscriber struct {
	// dropped counts messages the library discarded for want of room.
	dropped func() int64
	// stop disconnects the subscriber and returns once the library will
	// deliver nothing more, or with an error when it cannot confirm that
	// within ctx.
	stop func(ctx context.Context) error
}

func noDrops() int64 { return 0 }

// subscribe connects l's subscriber to filter, passing each delivery to
// onMsg through the subscription's gate. The subscription stops when the
// benchmark ends, if not before.
func subscribe(b *testing.B, l lib, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) *subscription {
	b.Helper()
	s := newSubscription()
	s.subscriber = l.subscribe(b, cfg, filter, qos, m, consumers, s.through(onMsg))
	b.Cleanup(func() {
		if b.Failed() {
			_ = s.Stop(stopBound)
			return
		}
		if err := s.Stop(stopBound); err != nil {
			b.Errorf("stop the %s subscriber: %v", l.name, err)
		} else if n := s.late.Load(); n > 0 {
			b.Errorf("the %s subscriber delivered %d messages after its library stopped", l.name, n)
		}
	})
	return s
}

// cleanupWithin registers stop as cleanup, giving it bound: a library
// that does not stop in time fails the benchmark instead of holding up
// the run.
func cleanupWithin(b *testing.B, what string, bound time.Duration, stop func(ctx context.Context) error) {
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), bound)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- stop(ctx) }()
		var err error
		select {
		case err = <-done:
		case <-ctx.Done():
			err = fmt.Errorf("did not return within %v", bound)
		}
		if err != nil && !b.Failed() {
			b.Errorf("%s: %v", what, err)
		}
	})
}

// subscription is a subscriber under test. Its deliveries reach onMsg
// through a gate, which Stop closes after the library's stop: it waits
// for the deliveries in progress and counts any that arrive later.
type subscription struct {
	subscriber
	gate *gate
	// late counts messages that reached the closed gate: delivered after
	// the library's stop returned.
	late atomic.Int64

	once     sync.Once
	bound    time.Duration
	deadline time.Time
	stopped  chan struct{}
	err      error
}

func newSubscription() *subscription {
	return &subscription{gate: newGate(), stopped: make(chan struct{})}
}

// through returns onMsg behind the subscription's gate.
func (s *subscription) through(onMsg func([]byte)) func([]byte) {
	return func(p []byte) {
		if !s.gate.enter() {
			// Zero-length probes may trail the run.
			if len(p) > 0 {
				s.late.Add(1)
			}
			return
		}
		defer s.gate.exit()
		onMsg(p)
	}
}

// Stop ends delivery: it stops the library's subscriber, then closes the
// gate and waits for the onMsg calls in progress, so a check after it
// sees every delivery of the run. It fails when the library's stop does,
// or when stopping takes longer than bound. The bound runs from the first
// call: a later one, such as the benchmark's cleanup after a check that
// timed out, waits only for what is left of it.
func (s *subscription) Stop(bound time.Duration) error {
	s.once.Do(func() {
		s.bound = bound
		s.deadline = time.Now().Add(bound)
		go func() {
			ctx, cancel := context.WithDeadline(context.Background(), s.deadline)
			defer cancel()
			err := s.stop(ctx)
			s.gate.close()
			s.err = err
			close(s.stopped)
		}()
	})
	t := time.NewTimer(time.Until(s.deadline))
	defer t.Stop()
	select {
	case <-s.stopped:
		return s.err
	case <-t.C:
		return fmt.Errorf("not stopped within %v", s.bound)
	}
}

// gate passes calls until it is closed; close waits for the calls in
// progress. One word holds both: gateClosed, plus the number of calls in
// progress.
type gate struct {
	state atomic.Int64
	once  sync.Once
	idle  chan struct{} // closed once the gate is closed and no call is in progress
}

const gateClosed = 1 << 62

func newGate() *gate { return &gate{idle: make(chan struct{})} }

// enter reports whether a call may proceed; one that does calls exit
// when done.
func (g *gate) enter() bool {
	if g.state.Add(1)&gateClosed != 0 {
		g.exit()
		return false
	}
	return true
}

func (g *gate) exit() {
	if g.state.Add(-1) == gateClosed {
		g.once.Do(func() { close(g.idle) })
	}
}

// close refuses calls from now on and waits for those in progress. Call
// it once.
func (g *gate) close() {
	if g.state.Add(gateClosed) == gateClosed {
		g.once.Do(func() { close(g.idle) })
	}
	<-g.idle
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
	cleanupWithin(b, "mqttv5 Disconnect", stopBound, cli.Disconnect)
	return cli
}

func connectMqttv5(b *testing.B, cfg clientConfig) publisher {
	return mqttv5Publisher{newMqttv5(b, cfg)}
}

func subscribeMqttv5(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
	b.Helper()
	var wg sync.WaitGroup
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
	// Disconnect closes the channel or queue the consumers drain and
	// returns once the read loop, which runs callbacks, has exited.
	return subscriber{dropped: dropped.Load, stop: func(ctx context.Context) error {
		err := cli.Disconnect(ctx)
		wg.Wait()
		return err
	}}
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
	cleanupWithin(b, "autopaho Disconnect", stopBound, func(ctx context.Context) error {
		defer cancel()
		return cm.Disconnect(ctx)
	})
	return cm
}

func connectAutopaho(b *testing.B, cfg clientConfig) publisher {
	return autopahoPublisher{newAutopaho(b, cfg, nil)}
}

func subscribeAutopaho(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
	b.Helper()
	var handler func(paho.PublishReceived) (bool, error)
	var stopConsumers func()
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
						// What the library handed over before it stopped.
						for {
							select {
							case p := <-ch:
								onMsg(p.Payload)
							default:
								return
							}
						}
					}
				}
			})
		}
		stopConsumers = func() {
			close(done)
			wg.Wait()
		}
	default:
		b.Fatalf("autopaho: no mode %q", m)
	}
	cm := newAutopaho(b, cfg, handler)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}},
	}); err != nil {
		b.Fatalf("autopaho Subscribe: %v", err)
	}
	// Disconnect returns once the client's workers, which run the
	// handlers, have exited. It goes first: consumers keep taking what a
	// handler is blocked handing over, so the handler returns.
	return subscriber{dropped: noDrops, stop: func(ctx context.Context) error {
		err := cm.Disconnect(ctx)
		if stopConsumers != nil {
			stopConsumers()
		}
		if err != nil {
			return fmt.Errorf("autopaho Disconnect: %w", err)
		}
		return nil
	}}
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
	cleanupWithin(b, "paho3 Disconnect", stopBound, func(ctx context.Context) error { return disconnectPaho3(ctx, c) })
	return c
}

// disconnectPaho3 disconnects c within ctx. Disconnect returns once the
// client's workers, its router and so its callbacks included, have
// exited, or when its quiesce period ends: only a return before the
// period ends confirms the client stopped.
func disconnectPaho3(ctx context.Context, c paho3.Client) error {
	quiesce := 250 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		quiesce = time.Until(deadline).Truncate(time.Millisecond)
	}
	begin := time.Now()
	c.Disconnect(uint(quiesce.Milliseconds()))
	if time.Since(begin) >= quiesce {
		return fmt.Errorf("paho3: Disconnect gave up after %v with its router still running", quiesce)
	}
	return nil
}

func connectPaho3(b *testing.B, cfg clientConfig) publisher {
	return paho3Publisher{newPaho3(b, cfg)}
}

func subscribePaho3(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, _ int, onMsg func([]byte)) subscriber {
	b.Helper()
	if m != modeCallback {
		b.Fatalf("paho3: no mode %q", m)
	}
	c := newPaho3(b, cfg)
	t := c.Subscribe(filter, qos, func(_ paho3.Client, msg paho3.Message) { onMsg(msg.Payload()) })
	if !t.WaitTimeout(5*time.Second) || t.Error() != nil {
		b.Fatalf("paho3 Subscribe: %v", t.Error())
	}
	return subscriber{dropped: noDrops, stop: func(ctx context.Context) error { return disconnectPaho3(ctx, c) }}
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
	ready   chan struct{}
	once    sync.Once
	done    chan struct{}
	// invalid is closed by the first delivery that fails the run.
	invalid     chan struct{}
	invalidOnce sync.Once

	// allowDrops accepts a message the library dropped, and counted, in
	// place of its delivery; allowDups accepts redeliveries, still
	// counted in dup.
	allowDrops, allowDups bool
}

func newSink(size, target int) *sink {
	s := &sink{
		size: size, target: int64(target), seen: make([]atomic.Uint64, (target+63)/64),
		ready: make(chan struct{}), done: make(chan struct{}), invalid: make(chan struct{}),
	}
	if size >= 2*seqBytes {
		s.pattern = Payload(size)[seqBytes : size-seqBytes]
	}
	return s
}

func (s *sink) onMsg(p []byte) { s.accept(p) }

// accept records p and reports whether it is a message's first intact
// delivery.
func (s *sink) accept(p []byte) bool {
	if len(p) == 0 {
		s.once.Do(func() { close(s.ready) })
		return false
	}
	seq, ok := readSeq(p)
	if len(p) != s.size || !ok || seq >= uint64(s.target) || !bytes.Equal(p[seqBytes:len(p)-seqBytes], s.pattern) {
		s.wrong.Add(1)
		s.fail()
		return false
	}
	bit := uint64(1) << (seq % 64)
	if s.seen[seq/64].Or(bit)&bit != 0 {
		s.dup.Add(1)
		if !s.allowDups {
			s.fail()
		}
		return false
	}
	if s.got.Add(1) == s.target {
		close(s.done)
	}
	return true
}

func (s *sink) fail() { s.invalidOnce.Do(func() { close(s.invalid) }) }

// wait waits up to d for every message, failing b at once when a
// delivery has made the run invalid.
func (s *sink) wait(b *testing.B, d time.Duration) {
	b.Helper()
	select {
	case <-s.done:
	case <-s.invalid:
		b.Fatalf("invalid delivery: %s", s)
	case <-time.After(d):
		b.Fatalf("timed out after %v: %s", d, s)
	}
}

func (s *sink) String() string {
	return fmt.Sprintf("received %d of %d (%d duplicates, %d of the wrong size or content)",
		s.got.Load(), s.target, s.dup.Load(), s.wrong.Load())
}

// check stops sub, so no delivery can follow, and fails the benchmark
// unless every message arrived exactly once and intact. Stopping it
// within stopBound is part of the check: a subscriber that cannot be
// stopped cannot be shown to have finished.
func (s *sink) check(b *testing.B, sub *subscription) {
	b.Helper()
	if err := s.finish(sub, stopBound); err != nil {
		b.Fatal(err)
	}
}

// finish stops sub, within bound, and verifies what arrived.
func (s *sink) finish(sub *subscription, bound time.Duration) error {
	if err := sub.Stop(bound); err != nil {
		return fmt.Errorf("the subscriber did not stop: %w; %s", err, s)
	}
	if n := sub.late.Load(); n > 0 {
		return fmt.Errorf("the subscriber delivered %d messages after its library stopped; %s", n, s)
	}
	return s.verify(sub.dropped)
}

// stopBound is how long a subscriber may take to stop.
const stopBound = 10 * time.Second

// verify reports anything but every message once and intact. Call it
// once delivery has ended.
func (s *sink) verify(dropped func() int64) error {
	n := dropped()
	if n > 0 && !s.allowDrops {
		return fmt.Errorf("subscriber dropped %d messages; a run with drops measures nothing", n)
	}
	if s.wrong.Load() > 0 || (s.dup.Load() > 0 && !s.allowDups) || s.got.Load()+n != s.target {
		return fmt.Errorf("%s, %d dropped", s, n)
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
