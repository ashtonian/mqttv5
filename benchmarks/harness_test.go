//go:build e2e

package benchmarks

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

var quietLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

// brokerURL is the broker under test: MQTT_BROKER, or the benchmark
// broker from docker-compose.yml.
func brokerURL() string {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1893"
}

func brokerAddr(b *testing.B) string {
	b.Helper()
	u, err := url.Parse(brokerURL())
	if err != nil {
		b.Fatalf("MQTT_BROKER: %v", err)
	}
	return u.Host
}

// brokerHost is brokerURL's host:port, empty when it does not parse.
func brokerHost() string {
	u, err := url.Parse(brokerURL())
	if err != nil {
		return ""
	}
	return u.Host
}

// requireBroker skips the benchmark when no broker is listening.
func requireBroker(b *testing.B) {
	b.Helper()
	c, err := net.DialTimeout("tcp", brokerAddr(b), 500*time.Millisecond)
	if err != nil {
		b.Skipf("broker %s unreachable (%v): docker compose -f benchmarks/docker-compose.yml up -d", brokerURL(), err)
	}
	_ = c.Close()
}

// uniqueID returns a fresh client ID or topic segment: a reused client
// ID would take over another benchmark's session.
func uniqueID(prefix string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

type size struct {
	name  string
	bytes int
}

var (
	size64B  = size{"64B", 64}
	size256B = size{"256B", 256}
	size1KiB = size{"1KiB", 1 << 10}
	size1MiB = size{"1MiB", 1 << 20}
)

// cpuTime is the CPU time this process has used, user plus system.
func cpuTime(b *testing.B) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		b.Fatalf("getrusage: %v", err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// cpuMeter reports the process CPU time per operation as cpu-ns/op:
// what the client costs per message regardless of whether the broker or
// the client limited the rate. Start it where the timer starts.
type cpuMeter struct {
	b     *testing.B
	start time.Duration
}

func startCPU(b *testing.B) *cpuMeter { return &cpuMeter{b: b, start: cpuTime(b)} }

func (m *cpuMeter) stop() {
	used := cpuTime(m.b) - m.start
	m.b.ReportMetric(float64(used.Nanoseconds())/float64(m.b.N), "cpu-ns/op")
}

// await waits for done or fails the benchmark after d with what.
func await(b *testing.B, done <-chan struct{}, d time.Duration, what func() string) {
	b.Helper()
	select {
	case <-done:
	case <-time.After(d):
		b.Fatalf("timed out after %v: %s", d, what())
	}
}

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

func stamped(seq uint64) []byte {
	p := Payload(64)
	stampSeq(p, seq)
	return p
}

// The sink accepts each message once, intact, and nothing else.
func TestSinkChecksEveryDelivery(t *testing.T) {
	msg := stamped
	for _, tc := range []struct {
		name    string
		deliver func(s *sink)
		ok      bool
	}{
		{"every message once", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)) }, true},
		{"a duplicate", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(1)) }, false},
		{"one missing", func(s *sink) { s.onMsg(msg(0)) }, false},
		{"an interior byte changed", func(s *sink) {
			p := msg(1)
			p[32] ^= 0xff
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"two messages' halves", func(s *sink) {
			p := msg(1)
			copy(p[32:], msg(0)[32:])
			s.onMsg(msg(0))
			s.onMsg(p)
		}, false},
		{"a sequence number never sent", func(s *sink) { s.onMsg(msg(0)); s.onMsg(msg(1)); s.onMsg(msg(7)) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink(64, 2)
			tc.deliver(s)
			err := s.verify(func() int64 { return 0 })
			if (err == nil) != tc.ok {
				t.Fatalf("verify: %v, want ok %v", err, tc.ok)
			}
		})
	}
}

// A sink that allows drops counts each message its library dropped in
// place of a delivery, and one that allows duplicates counts them
// without failing; neither accepts a damaged or missing message.
func TestSinkPolicies(t *testing.T) {
	for _, tc := range []struct {
		name             string
		allowDrops, dups bool
		deliver          []uint64
		dropped          int64
		damaged          bool
		ok               bool
	}{
		{name: "drops counted in place of deliveries", allowDrops: true, deliver: []uint64{0}, dropped: 1, ok: true},
		{name: "drops that leave a message missing", allowDrops: true, deliver: []uint64{0}, ok: false},
		{name: "drops refused by default", deliver: []uint64{0}, dropped: 1, ok: false},
		{name: "a duplicate where drops are allowed", allowDrops: true, deliver: []uint64{0, 0}, dropped: 1, ok: false},
		{name: "duplicates allowed", dups: true, deliver: []uint64{0, 1, 1}, ok: true},
		{name: "duplicates allowed, one missing", dups: true, deliver: []uint64{0, 0}, ok: false},
		{name: "damage where duplicates are allowed", dups: true, deliver: []uint64{0, 1}, damaged: true, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSink(64, 2)
			s.allowDrops, s.allowDups = tc.allowDrops, tc.dups
			for i, seq := range tc.deliver {
				p := stamped(seq)
				if tc.damaged && i == len(tc.deliver)-1 {
					p[32] ^= 0xff
				}
				s.onMsg(p)
			}
			err := s.verify(func() int64 { return tc.dropped })
			if (err == nil) != tc.ok {
				t.Fatalf("verify: %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func fakeSubscription(stop func(ctx context.Context) error) *subscription {
	s := newSubscription()
	s.subscriber = subscriber{dropped: noDrops, stop: stop}
	return s
}

// A delivery in progress when the library's stop returns, as paho3's
// does after its quiesce period, is part of the run: the check waits for
// it, so a duplicate in a consumer's hands fails the run.
func TestCheckWaitsForADeliveryInProgress(t *testing.T) {
	s := newSink(64, 1)
	sub := fakeSubscription(func(context.Context) error { return nil })
	release := make(chan struct{})
	entered := make(chan struct{})
	deliver := sub.through(func(p []byte) {
		if s.got.Load() == 1 {
			close(entered)
			<-release
		}
		s.onMsg(p)
	})
	deliver(stamped(0))
	go deliver(stamped(0))
	<-entered
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	if err := s.finish(sub, 5*time.Second); err == nil {
		t.Fatal("a duplicate delivered while the subscriber stopped passed the check")
	}
}

// Nothing reaches onMsg once Stop has returned, and a message the library
// delivers after its stop returned fails the check.
func TestDeliveryAfterStopFailsTheCheck(t *testing.T) {
	s := newSink(64, 1)
	sub := fakeSubscription(func(context.Context) error { return nil })
	deliver := sub.through(s.onMsg)
	deliver(stamped(0))
	if err := sub.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
	deliver(stamped(0))
	deliver(nil) // a trailing probe is not a delivery
	if s.dup.Load() != 0 {
		t.Fatal("a delivery after Stop reached onMsg")
	}
	if err := s.finish(sub, time.Second); err == nil {
		t.Fatal("a delivery after the library stopped passed the check")
	}
}

// A subscriber that does not stop within the bound fails the check, a
// failed library stop fails it too, and the benchmark's cleanup, which
// stops the subscription again, does not wait a second bound.
func TestStopIsBounded(t *testing.T) {
	stuck := fakeSubscription(func(context.Context) error { select {} })
	if err := newSink(64, 0).finish(stuck, 10*time.Millisecond); err == nil {
		t.Fatal("a subscriber that never stops passed the check")
	}
	again := make(chan error, 1)
	go func() { again <- stuck.Stop(stopBound) }()
	select {
	case err := <-again:
		if err == nil {
			t.Fatal("a second Stop of a stuck subscriber succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("a second Stop waited again for a subscriber that already timed out")
	}

	failing := fakeSubscription(func(context.Context) error { return errors.New("disconnect failed") })
	if err := newSink(64, 0).finish(failing, time.Second); err == nil {
		t.Fatal("a failed library stop passed the check")
	}
}

// adapterGoroutines counts goroutines running in this package's
// subscribe adapters.
func adapterGoroutines() int {
	var b bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&b, 2)
	n := 0
	for _, g := range strings.Split(b.String(), "\n\n") {
		if strings.Contains(g, "benchmarks.subscribe") {
			n++
		}
	}
	return n
}

// Every adapter's consumers end with the benchmark that started them,
// so repeated runs do not accumulate goroutines that skew later ones.
func TestAdapterConsumersEndWithTheBenchmark(t *testing.T) {
	for _, l := range append(libs, floorLib) {
		for _, m := range l.modes {
			for _, qos := range []byte{0, 1} {
				t.Run(fmt.Sprintf("lib=%s/mode=%s/qos=%d", l.name, m, qos), func(t *testing.T) {
					before := adapterGoroutines()
					res := testing.Benchmark(func(b *testing.B) {
						srv := newRawServer(b, l.v5)
						s := newSink(64, 1)
						subscribe(b, l, clientConfig{id: uniqueID("adapter"), addr: srv.addr(), receiveMaximum: receiveWindow},
							"adapter/t", qos, m, 4, s.onMsg)
						srv.awaitSubscribed(b)
						if err := srv.publish("adapter/t", qos, Payload(64), 1); err != nil {
							b.Fatal(err)
						}
						s.wait(b, 5*time.Second)
					})
					if res.N == 0 {
						t.Fatal("the benchmark failed")
					}
					deadline := time.Now().Add(5 * time.Second)
					for adapterGoroutines() > before {
						if time.Now().After(deadline) {
							t.Fatalf("adapter goroutines after the benchmark: %d, before: %d", adapterGoroutines(), before)
						}
						time.Sleep(10 * time.Millisecond)
					}
				})
			}
		}
	}
}

// Stopping a real adapter's subscription while a duplicate is still
// being delivered waits for that delivery, so the check counts it. The
// delivery is held inside the subscription's gate, which waits for it,
// or before it, where only the library's own stop can: a library whose
// stop returned with its callback still running would let the
// duplicate reach a closed gate after the check.
func TestAdapterStopWaitsForADeliveryInProgress(t *testing.T) {
	for _, held := range []string{"inside-gate", "before-gate"} {
		for _, l := range append(libs, floorLib) {
			t.Run(fmt.Sprintf("held=%s/lib=%s", held, l.name), func(t *testing.T) {
				var checked error
				res := testing.Benchmark(func(b *testing.B) {
					entered, release := make(chan struct{}), make(chan struct{})
					var calls atomic.Int64
					hold := func(onMsg func([]byte)) func([]byte) {
						return func(p []byte) {
							if calls.Add(1) == 2 {
								close(entered)
								<-release
							}
							onMsg(p)
						}
					}
					s := newSink(64, 1)
					onMsg := s.onMsg
					if held == "inside-gate" {
						onMsg = hold(onMsg)
					} else {
						adapter := l.subscribe
						l.subscribe = func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
							return adapter(b, cfg, filter, qos, m, consumers, hold(onMsg))
						}
					}
					srv := newRawServer(b, l.v5)
					sub := subscribe(b, l, clientConfig{id: uniqueID("adapter-stop"), addr: srv.addr(), receiveMaximum: receiveWindow},
						"adapter/t", 0, modeCallback, 1, onMsg)
					srv.awaitSubscribed(b)
					for range 2 {
						if err := srv.publish("adapter/t", 0, Payload(64), 1); err != nil {
							b.Fatal(err)
						}
					}
					await(b, entered, 5*time.Second, s.String)
					// Held past paho3's default 250 ms quiesce.
					go func() {
						time.Sleep(time.Second)
						close(release)
					}()
					checked = s.finish(sub, stopBound)
				})
				if res.N == 0 {
					t.Fatal("the benchmark failed")
				}
				if checked == nil || !strings.Contains(checked.Error(), "1 duplicates") {
					t.Fatalf("check = %v, want the duplicate counted", checked)
				}
			})
		}
	}
}

// A client cleanup that does not return within its bound fails the
// benchmark rather than holding it up.
func TestCleanupIsBounded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ended := make(chan testing.BenchmarkResult, 1)
	go func() {
		ended <- testing.Benchmark(func(b *testing.B) {
			cleanupWithin(b, "stuck", 50*time.Millisecond, func(context.Context) error {
				<-release
				return nil
			})
		})
	}()
	select {
	case res := <-ended:
		if res.N != 0 {
			t.Fatal("a cleanup that did not return passed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the benchmark waited for a stuck cleanup")
	}
}

// publishFunc is a publisher made of a function.
type publishFunc func(ctx context.Context, topic string, qos byte, payload []byte) error

func (f publishFunc) publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	return f(ctx, topic, qos, payload)
}

// loopbackLib is a library double whose publisher hands each payload to
// its subscriber through deliver, one at a time, without a broker: tests
// use it to make a scenario see deliveries a real library would not.
func loopbackLib(deliver func(onMsg func([]byte), p []byte)) lib {
	var mu sync.Mutex
	var onMsg func([]byte)
	return lib{name: "loopback", v5: true, modes: []mode{modeCallback},
		subscribe: func(_ *testing.B, _ clientConfig, _ string, _ byte, _ mode, _ int, f func([]byte)) subscriber {
			mu.Lock()
			onMsg = f
			mu.Unlock()
			return subscriber{dropped: noDrops, stop: func(context.Context) error { return nil }}
		},
		connect: func(*testing.B, clientConfig) publisher {
			return publishFunc(func(_ context.Context, _ string, _ byte, p []byte) error {
				mu.Lock()
				defer mu.Unlock()
				deliver(onMsg, p)
				return nil
			})
		},
	}
}

func corrupted(p []byte) []byte {
	c := bytes.Clone(p)
	c[len(c)/2] ^= 0xff
	return c
}

var faults = []struct {
	name    string
	deliver func(onMsg func([]byte), p []byte)
	ok      bool
}{
	{"intact", func(onMsg func([]byte), p []byte) { onMsg(p) }, true},
	{"corrupted", func(onMsg func([]byte), p []byte) {
		if len(p) > 0 {
			p = corrupted(p)
		}
		onMsg(p)
	}, false},
	{"duplicated", func(onMsg func([]byte), p []byte) {
		onMsg(p)
		if len(p) > 0 {
			onMsg(p)
		}
	}, false},
}

// The round-trip and latency scenarios accept a run only when every
// message arrived once and intact.
func TestScenariosCheckEveryDelivery(t *testing.T) {
	for _, f := range faults {
		t.Run("RoundTrip/"+f.name, func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) { runRoundTrip(b, loopbackLib(f.deliver), 1, 64) })
			if (res.N > 0) != f.ok {
				t.Fatalf("run accepted: %v, want %v", res.N > 0, f.ok)
			}
		})
		t.Run("Latency/"+f.name, func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) { runLatency(b, loopbackLib(f.deliver), 1, 10000, 256) })
			if (res.N > 0) != f.ok {
				t.Fatalf("run accepted: %v, want %v", res.N > 0, f.ok)
			}
		})
	}
}

// requireBrokerT skips t when the benchmark broker is unreachable.
func requireBrokerT(t *testing.T) {
	t.Helper()
	c, err := net.DialTimeout("tcp", brokerHost(), 500*time.Millisecond)
	if err != nil {
		t.Skipf("broker %s unreachable (%v)", brokerURL(), err)
	}
	_ = c.Close()
}

// The slow-consumer and reconnect scenarios, which go through the
// broker, reject a corrupted delivery.
func TestBrokerScenariosRejectCorruption(t *testing.T) {
	requireBrokerT(t)
	l := libs[0]
	t.Run("SlowConsumer", func(t *testing.T) {
		corrupting := l
		corrupting.subscribe = func(b *testing.B, cfg clientConfig, filter string, qos byte, m mode, consumers int, onMsg func([]byte)) subscriber {
			return l.subscribe(b, cfg, filter, qos, m, consumers, func(p []byte) {
				if len(p) > 0 {
					p = corrupted(p)
				}
				onMsg(p)
			})
		}
		res := testing.Benchmark(func(b *testing.B) { runSlowConsumer(b, corrupting, modeCallback, 0, 1024) })
		if res.N > 0 {
			t.Fatal("a run with corrupted deliveries was accepted")
		}
	})
	t.Run("Reconnect", func(t *testing.T) {
		corrupting := l
		corrupting.connect = func(b *testing.B, cfg clientConfig) publisher {
			p := l.connect(b, cfg)
			return publishFunc(func(ctx context.Context, topic string, qos byte, payload []byte) error {
				if len(payload) > 0 {
					payload = corrupted(payload)
				}
				return p.publish(ctx, topic, qos, payload)
			})
		}
		res := testing.Benchmark(func(b *testing.B) { runReconnect(b, corrupting) })
		if res.N > 0 {
			t.Fatal("a run with corrupted deliveries was accepted")
		}
	})
}
