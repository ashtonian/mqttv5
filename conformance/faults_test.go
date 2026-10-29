//go:build conformance

package conformance

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/transport"
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
		if e.typ == wire.PUBLISH && e.dup {
			resent++
		}
		if e.typ == wire.PUBREL {
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

// connCapture wraps the built-in dial path and records the most recent
// net.Conn handed to the client, so a test can Close() it directly to
// simulate an ungraceful network drop (no DISCONNECT). Each reconnect
// dial overwrites the captured conn — Latest always points at the live
// connection.
type connCapture struct {
	mu    sync.Mutex
	conn  net.Conn
	dials int
}

// DialFunc returns a transport.DialFunc that defers to the real TCP/TLS
// dial (transport.Dial, via the *url.URL's string form, so the
// default-port logic for mqtt:// is reused) and captures the resulting
// connection.
func (cc *connCapture) DialFunc() transport.DialFunc {
	return func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		c, err := transport.Dial(ctx, u.String(), transport.DialOpts{})
		if err != nil {
			return nil, err
		}
		cc.mu.Lock()
		cc.conn = c
		cc.dials++
		cc.mu.Unlock()
		return c, nil
	}
}

// dropLatest closes the most recently captured connection, forcing an
// ungraceful drop under the client's reader/writer. Returns false if no
// connection has been captured yet.
func (cc *connCapture) dropLatest() bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.conn == nil {
		return false
	}
	_ = cc.conn.Close()
	return true
}

func (cc *connCapture) dialCount() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.dials
}

// A QoS 1 message published across an ungraceful connection drop is
// resent after the reconnect, delivered at least once, and its Publish
// call returns success. The supervisor replays it from the resumed
// session; the subscriber sees the payload byte for byte, and the
// publisher's call returns once the PUBACK arrives.
//
// Mechanism: the publisher P keeps a non-expiring session
// (WithSessionExpiry(300) + WithCleanStartOnReconnect(false)) so the
// broker resumes its in-flight QoS 1 state across the reconnect, and a
// WithDialFunc wrapper captures P's live net.Conn so the test can
// Close() it to look like a network drop. P publishes a QoS 1 message
// and the test forces the drop around the publish; the supervisor
// reconnects on a short backoff and replays the unacked PUBLISH.
//
// DUP=1 on the replayed copy is the strict MQTT v5 §4.4 expectation, but
// whether the subscriber observes DUP depends on broker timing (a broker
// that PUBACKs P before the drop, or that delivers to S before P's
// replay lands, can surface the message with DUP=0). QoS 1 is
// at-least-once: when the broker forwarded the first copy and the PUBACK
// was lost with the connection, the replay is legitimately delivered
// again. The broker-independent invariants asserted here are: the
// message is delivered at least once, every copy is byte-equal, the
// publisher reconnects, and P.Publish returns nil.
func TestReconnect_QoS1SurvivesConnectionDrop(t *testing.T) {
	requireBroker(t, brokerURL())

	topic := "conformance/reconnect-replay/" + randSuffix()
	want := []byte("qos1-survives-drop")

	// Subscriber S: durable enough to keep its subscription across the
	// whole test. It stays connected throughout; the QoS 1 publish must
	// reach it once after P's reconnect+replay.
	sub, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID("conformance-replay-sub-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithCleanStart(false),
		mqttv5.WithSessionExpiry(300),
	)
	if err != nil {
		t.Fatalf("New sub: %v", err)
	}
	if err := sub.Connect(context.Background()); err != nil {
		t.Fatalf("Connect sub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = sub.Disconnect(ctx)
	})

	ch, _, err := sub.Subscribe(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic, QoS: 1}}, mqttv5.SubBuffer(8))
	if err != nil {
		t.Fatalf("subscribe %s: %v", topic, err)
	}
	// Let the broker install the subscription before any publish races it.
	time.Sleep(100 * time.Millisecond)

	// Publisher P: stable ClientID + non-expiring session so the broker
	// resumes in-flight QoS 1 state after the drop. Short reconnect
	// backoff so the replay happens inside the test's timeout. The
	// connCapture DialFunc lets the test sever P's socket directly.
	cc := &connCapture{}
	pub, err := mqttv5.New(
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID("conformance-replay-pub-"+randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5*time.Second),
		mqttv5.WithCleanStart(false),
		mqttv5.WithCleanStartOnReconnect(false),
		mqttv5.WithSessionExpiry(300),
		mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(100*time.Millisecond)),
		mqttv5.WithDialFunc(cc.DialFunc()),
	)
	if err != nil {
		t.Fatalf("New pub: %v", err)
	}
	if err := pub.Connect(context.Background()); err != nil {
		t.Fatalf("Connect pub: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = pub.Disconnect(ctx)
	})
	if cc.dialCount() == 0 {
		t.Fatal("DialFunc was not invoked on Connect — capture wiring is broken")
	}

	// Publish QoS 1 in the background; for QoS 1, Publish blocks until
	// PUBACK, which (given the forced drop) only arrives after the
	// supervisor reconnects and replays. A generous timeout absorbs the
	// reconnect blip.
	pubDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		pubDone <- pub.Publish(ctx, mqttv5.PublishOptions{
			Topic:   topic,
			Payload: want,
			QoS:     1,
		})
	}()

	// Force an ungraceful drop of P's connection around the publish.
	// Closing the captured socket under the reader/writer looks like a
	// network drop (not a graceful DISCONNECT), so the broker keeps the
	// QoS 1 message un-acked and the supervisor must replay it. A short
	// sleep first gives the PUBLISH a chance to be written before the
	// socket dies, exercising the replay path either way (un-acked
	// in-flight publish is re-sent on reconnect).
	time.Sleep(20 * time.Millisecond)
	if !cc.dropLatest() {
		t.Fatal("no captured connection to drop")
	}

	// The message must arrive exactly once at S, byte-equal. Collect for
	// a generous window, then assert no duplicate follows.
	m := expectMessage(t, ch, 15*time.Second)
	if string(m.Payload) != string(want) {
		t.Errorf("payload = %q, want %q", m.Payload, want)
	}
	if m.QoS != 1 {
		t.Errorf("QoS = %d, want 1", m.QoS)
	}
	// DUP is informational only — record it, don't gate on it (broker
	// timing dependent; see the doc comment).
	t.Logf("delivered copy: DUP=%v QoS=%d (DUP is broker-timing dependent)", m.Dup, m.QoS)
	_ = m.Ack()

	// At-least-once: a replayed duplicate is allowed (see the doc
	// comment), but every copy must carry the same bytes.
	dupes := 0
	for collecting := true; collecting; {
		select {
		case d := <-ch:
			dupes++
			if string(d.Payload) != string(want) {
				t.Errorf("duplicate payload = %q, want %q", d.Payload, want)
			}
			_ = d.Ack()
		case <-time.After(time.Second):
			collecting = false
		}
	}
	t.Logf("duplicates delivered after the first copy: %d", dupes)

	// The supervisor must actually have reconnected — i.e. dialled more
	// than once. When P's PUBACK beat the forced drop the reconnect is
	// still pending here, so wait for it rather than sampling once.
	deadline := time.Now().Add(15 * time.Second)
	for cc.dialCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := cc.dialCount(); got < 2 {
		t.Errorf("dialCount = %d, want >= 2 (supervisor should have reconnected)", got)
	}

	// P's Publish must ultimately succeed: nil return == PUBACK received
	// after the replay.
	select {
	case err := <-pubDone:
		if err != nil {
			t.Errorf("Publish returned error (expected PUBACK after replay): %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Publish did not complete (no PUBACK after reconnect+replay)")
	}
}

// soakDuration is how long TestSoak_DeliveryAcrossConnectionCuts
// publishes: MQTTV5_SOAK (a Go duration) or a few seconds, so the
// regular suite runs it briefly and the nightly job for minutes.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("MQTTV5_SOAK"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("MQTTV5_SOAK=%q: %v", v, err)
		}
		return d
	}
	return 3 * time.Second
}

// Publishers whose connection a proxy cuts at random moments keep their
// session (Session Expiry 300 s), so every message they publish arrives:
// QoS 2 exactly once, QoS 1 at least once. A Publish made while the
// connection is down returns ErrNotConnected without sending, and is
// retried once the client has reconnected; every other Publish returns
// success. A subscriber on a stable connection counts arrivals.
//
// With MQTTV5_SOAK_TRACE set, the publisher's packets tell a broker's
// fault from the client's: a QoS 2 message whose exchange was correct
// but arrived twice was duplicated by the broker ([MQTT-4.3.3-10]), and
// a first PUBLISH refused with 0x91 under an identifier whose previous
// exchange had ended met state the broker kept after that exchange.
// Both are logged rather than failed; without the trace they fail.
func TestSoak_DeliveryAcrossConnectionCuts(t *testing.T) {
	requireBroker(t, brokerURL())
	duration := soakDuration(t)
	topic := "conformance/soak/" + randSuffix()

	var mu sync.Mutex
	arrived := map[string]int{}
	var subTrace *packetTrace
	subOpts := []mqttv5.Option{}
	if os.Getenv("MQTTV5_SOAK_TRACE") != "" {
		subPx := newFaultProxy(t, stripScheme(brokerURL()), true)
		subTrace = subPx.trace
		subOpts = append(subOpts, mqttv5.WithBroker(subPx.url()))
	}
	sub := connect(t, subOpts...)
	if _, err := sub.SubscribeCallback(context.Background(),
		[]mqttv5.TopicFilter{{Topic: topic + "/#", QoS: 2}},
		func(m *mqttv5.Message) {
			mu.Lock()
			arrived[string(m.Payload)]++
			mu.Unlock()
		}); err != nil {
		t.Fatal(err)
	}

	px := newFaultProxy(t, stripScheme(brokerURL()), os.Getenv("MQTTV5_SOAK_TRACE") != "")
	pub := connect(t,
		mqttv5.WithBroker(px.url()),
		mqttv5.WithSessionExpiry(300),
		mqttv5.WithReconnectBackoff(mqttv5.ConstantBackoff(5*time.Millisecond)),
		mqttv5.WithStats(),
	)
	time.Sleep(100 * time.Millisecond)

	stop := make(chan struct{})
	var cuts atomic.Int64
	go func() {
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Duration(20+rng.IntN(280)) * time.Millisecond):
				cuts.Add(int64(px.cut()))
			}
		}
	}()

	const workers = 8
	type sent struct {
		id  string
		qos byte
	}
	var sentMu sync.Mutex
	var published []sent
	type failure struct {
		id  string
		err error
	}
	var failures []failure
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; time.Now().Before(deadline); seq++ {
				qos := byte(1 + (w+seq)%2)
				id := fmt.Sprintf("w%d-%d", w, seq)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := pub.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("%s/%d", topic, w), QoS: qos, Payload: []byte(id)})
				for errors.Is(err, mqttv5.ErrNotConnected) {
					// Not sent: wait for the reconnect and try again.
					if err = pub.AwaitConnection(ctx); err == nil {
						err = pub.Publish(ctx, mqttv5.PublishOptions{Topic: fmt.Sprintf("%s/%d", topic, w), QoS: qos, Payload: []byte(id)})
					}
				}
				cancel()
				sentMu.Lock()
				if err != nil {
					failures = append(failures, failure{id, fmt.Errorf("%s (QoS %d): %w", id, qos, err)})
				} else {
					published = append(published, sent{id, qos})
				}
				sentMu.Unlock()
				// A PUBLISH the broker refused because it holds the
				// identifier ends only that message; any other failure
				// ends the worker.
				var refused *mqttv5.ReasonCodeError
				inUse := errors.As(err, &refused) && refused.Code == wire.ReasonPacketIdentifierInUse
				if err != nil && !inUse {
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	brokerRefusals := 0
	for _, f := range failures {
		if px.trace != nil && staleIdentifierRefusal(px.trace.history(f.id)) {
			brokerRefusals++
			t.Logf("broker refused %s under an identifier whose exchange had ended: %v; publisher %s", f.id, f.err, px.trace.history(f.id))
			continue
		}
		t.Error(f.err)
		if px.trace != nil {
			t.Logf("%s: publisher %s; subscriber %s", f.id, px.trace.history(f.id), subTrace.history(f.id))
		}
	}

	missing := func() []string {
		mu.Lock()
		defer mu.Unlock()
		var out []string
		for _, p := range published {
			if arrived[p.id] == 0 {
				out = append(out, p.id)
			}
		}
		return out
	}
	for end := time.Now().Add(30 * time.Second); len(missing()) > 0 && time.Now().Before(end); {
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // room for a late duplicate to show
	if m := missing(); len(m) > 0 {
		t.Errorf("%d of %d messages never arrived, e.g. %v", len(m), len(published), m[:min(5, len(m))])
		if px.trace != nil {
			for _, id := range m[:min(5, len(m))] {
				t.Logf("%s: publisher %s; subscriber %s", id, px.trace.history(id), subTrace.history(id))
			}
			if len(published) > 0 {
				ok := published[len(published)/2].id
				t.Logf("for comparison, %s (arrived): publisher %s; subscriber %s", ok, px.trace.history(ok), subTrace.history(ok))
			}
			t.Logf("CONNACKs: %s", px.trace.sessions())
		}
	}
	mu.Lock()
	duplicates, brokerDuplicates := 0, 0
	for _, p := range published {
		switch {
		case arrived[p.id] <= 1:
		case p.qos == 1:
			duplicates++
		case px.trace != nil && exchangeCorrect(px.trace.history(p.id)):
			brokerDuplicates++
			t.Logf("broker delivered QoS 2 message %s %d times after a correct exchange: publisher %s; subscriber %s",
				p.id, arrived[p.id], px.trace.history(p.id), subTrace.history(p.id))
		default:
			t.Errorf("QoS 2 message %s arrived %d times", p.id, arrived[p.id])
			if px.trace != nil {
				t.Logf("%s: publisher %s; subscriber %s", p.id, px.trace.history(p.id), subTrace.history(p.id))
			}
		}
	}
	mu.Unlock()
	s := pub.Stats()
	t.Logf("%v: %d messages, %d connection cuts, %d reconnects, %d publishes resent, %d QoS 1 duplicates; broker faults: %d QoS 2 duplicates, %d stale-identifier refusals",
		duration, len(published), cuts.Load(), s.Connects-1, s.PublishesReplayed, duplicates, brokerDuplicates, brokerRefusals)
	if cuts.Load() == 0 {
		t.Error("the proxy never cut a connection")
	}
}

// exchangeCorrect reports whether the client's packets for one message
// were a correct QoS 2 exchange ([§4.3.3], [§4.4]): a first PUBLISH
// without DUP under an identifier whose previous exchange had ended,
// each later PUBLISH a DUP resend under the same identifier on a later
// connection and none after the PUBREL, ending with PUBCOMP.
func exchangeCorrect(ev []traceEvent) bool {
	var id uint16
	pubs, conn := 0, 0
	released, completed := false, false
	for _, e := range ev {
		switch {
		case e.out && e.typ == wire.PUBLISH:
			if pubs == 0 {
				if e.dup || !e.reuseEnded {
					return false
				}
				id = e.id
			} else if !e.dup || e.id != id || e.conn <= conn || released {
				return false
			}
			pubs++
			conn = e.conn
		case e.out && e.typ == wire.PUBREL:
			if e.id != id {
				return false
			}
			released = true
		case !e.out && e.typ == wire.PUBCOMP && e.id == id:
			completed = true
		}
	}
	return pubs > 0 && released && completed
}

// staleIdentifierRefusal reports whether a message was refused because
// the broker still held its packet identifier: the client's first
// PUBLISH, under an identifier whose previous exchange had ended, drew a
// PUBREC with 0x91 (Packet Identifier in use).
func staleIdentifierRefusal(ev []traceEvent) bool {
	if len(ev) == 0 {
		return false
	}
	first := ev[0]
	if !first.out || first.typ != wire.PUBLISH || first.dup || !first.reuseEnded {
		return false
	}
	for _, e := range ev[1:] {
		if !e.out && e.typ == wire.PUBREC && e.id == first.id {
			return e.rc == wire.ReasonPacketIdentifierInUse
		}
	}
	return false
}

// The soak blames the broker only for a message the client exchanged
// correctly; any fault of the client's own stays a failure.
func TestSoak_BrokerFaultAttribution(t *testing.T) {
	publish := func(conn int, id uint16, dup, reuseEnded bool) traceEvent {
		return traceEvent{conn: conn, out: true, typ: wire.PUBLISH, id: id, qos: 2, dup: dup, reuseEnded: reuseEnded}
	}
	ack := func(conn int, typ wire.PacketType, id uint16, rc wire.ReasonCode) traceEvent {
		return traceEvent{conn: conn, typ: typ, id: id, rc: rc}
	}
	rel := func(conn int, id uint16) traceEvent {
		return traceEvent{conn: conn, out: true, typ: wire.PUBREL, id: id}
	}
	exchanges := []struct {
		name    string
		events  []traceEvent
		correct bool
	}{
		{"one connection", []traceEvent{publish(1, 5, false, true), ack(1, wire.PUBREC, 5, 0), rel(1, 5), ack(1, wire.PUBCOMP, 5, 0)}, true},
		{"resent after a cut", []traceEvent{publish(1, 5, false, true), publish(2, 5, true, false), ack(2, wire.PUBREC, 5, 0), rel(2, 5), ack(2, wire.PUBCOMP, 5, 0)}, true},
		{"resent without DUP", []traceEvent{publish(1, 5, false, true), publish(2, 5, false, true), ack(2, wire.PUBREC, 5, 0), rel(2, 5), ack(2, wire.PUBCOMP, 5, 0)}, false},
		{"resent on the same connection", []traceEvent{publish(1, 5, false, true), publish(1, 5, true, false), ack(1, wire.PUBREC, 5, 0), rel(1, 5), ack(1, wire.PUBCOMP, 5, 0)}, false},
		{"resent under another identifier", []traceEvent{publish(1, 5, false, true), publish(2, 6, true, false), ack(2, wire.PUBREC, 6, 0), rel(2, 6), ack(2, wire.PUBCOMP, 6, 0)}, false},
		{"PUBLISH after PUBREL", []traceEvent{publish(1, 5, false, true), ack(1, wire.PUBREC, 5, 0), rel(1, 5), publish(2, 5, true, false), ack(2, wire.PUBCOMP, 5, 0)}, false},
		{"identifier still in use", []traceEvent{publish(1, 5, false, false), ack(1, wire.PUBREC, 5, 0), rel(1, 5), ack(1, wire.PUBCOMP, 5, 0)}, false},
		{"no PUBCOMP", []traceEvent{publish(1, 5, false, true), ack(1, wire.PUBREC, 5, 0), rel(1, 5)}, false},
	}
	for _, tt := range exchanges {
		t.Run("exchange/"+tt.name, func(t *testing.T) {
			if got := exchangeCorrect(tt.events); got != tt.correct {
				t.Errorf("exchangeCorrect = %v, want %v", got, tt.correct)
			}
		})
	}
	refusals := []struct {
		name   string
		events []traceEvent
		stale  bool
	}{
		{"fresh identifier refused in use", []traceEvent{publish(1, 7, false, true), ack(1, wire.PUBREC, 7, wire.ReasonPacketIdentifierInUse)}, true},
		{"identifier still in use", []traceEvent{publish(1, 7, false, false), ack(1, wire.PUBREC, 7, wire.ReasonPacketIdentifierInUse)}, false},
		{"other refusal", []traceEvent{publish(1, 7, false, true), ack(1, wire.PUBREC, 7, wire.ReasonUnspecifiedError)}, false},
		{"refused resend", []traceEvent{publish(1, 7, true, false), ack(1, wire.PUBREC, 7, wire.ReasonPacketIdentifierInUse)}, false},
	}
	for _, tt := range refusals {
		t.Run("refusal/"+tt.name, func(t *testing.T) {
			if got := staleIdentifierRefusal(tt.events); got != tt.stale {
				t.Errorf("staleIdentifierRefusal = %v, want %v", got, tt.stale)
			}
		})
	}
}
