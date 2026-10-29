package mqttv5

import (
	"context"
	"errors"
	"maps"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

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
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
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
		return net.Dial("tcp", fb.Addr())
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

// pubackBroker accepts CONNECT and PUBACKs every QoS>0 PUBLISH.
func pubackBroker(t *testing.T) *testbroker.Broker {
	t.Helper()
	return testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		for {
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			pub, ok := pkt.(*wire.Publish)
			if !ok {
				pkt.Release()
				continue
			}
			id := pub.PacketID
			pkt.Release()
			if id != 0 {
				_, _ = wire.WritePuback(c, wire.PubRespOpts{
					PacketID:   id,
					ReasonCode: wire.ReasonSuccess,
				})
			}
		}
	})
}

func TestStatsCountersTickOnPublish(t *testing.T) {
	fb := pubackBroker(t)

	cli, err := New(WithBroker(fb.URL()), WithStats())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if got := cli.Stats().Connects; got != 1 {
		t.Errorf("Stats.Connects after Connect = %d, want 1", got)
	}

	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), PublishOptions{
			Topic: "stats/test", QoS: 1, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := cli.Stats()
		if s.PublishesSent >= 3 && s.PublishesAcked >= 3 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s := cli.Stats()
	t.Fatalf("after 3 Publishes: PublishesSent=%d PublishesAcked=%d (want both >= 3)",
		s.PublishesSent, s.PublishesAcked)
}

// TestStatsDisabledReturnsZero verifies that WithStats() is required
// to populate counters. Without it, every Stats() field stays at the
// zero value even after activity that would otherwise tick counters.
// (PublishesInflight and SubscriptionsActive are point-in-time gauges
// and stay zero only because we haven't subscribed or pre-loaded any
// outbound entries.)
func TestStatsDisabledReturnsZero(t *testing.T) {
	fb := pubackBroker(t)

	cli, err := New(WithBroker(fb.URL())) // no WithStats
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	for i := 0; i < 3; i++ {
		if err := cli.Publish(context.Background(), PublishOptions{
			Topic: "stats/disabled", QoS: 1, Payload: []byte("x"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}
	// Give the broker a beat to ack so the publish path completes.
	time.Sleep(100 * time.Millisecond)

	s := cli.Stats()
	if s.Connects != 0 || s.PublishesSent != 0 || s.PublishesAcked != 0 {
		t.Errorf("Stats without WithStats should be zero, got %+v", s)
	}
}

// Publishes the broker's CONNACK rules out fail before the wire.
func TestPublishRespectsConnackLimits(t *testing.T) {
	maxQoS, noRetain, maxSize := byte(1), byte(0), uint32(64)
	limited := wire.ConnackOpts{MaximumQoS: &maxQoS, RetainAvailable: &noRetain, MaximumPacketSize: &maxSize}
	tests := []struct {
		name string
		opts PublishOptions
		err  error
	}{
		{"qos above maximum", PublishOptions{Topic: "t", QoS: 2}, ErrQoSNotSupported},
		{"retain unavailable", PublishOptions{Topic: "t", Retain: true}, ErrRetainNotSupported},
		{"qos0 too large", PublishOptions{Topic: "t", Payload: make([]byte, 100)}, ErrPacketTooLarge},
		{"qos1 too large", PublishOptions{Topic: "t", Payload: make([]byte, 100), QoS: 1}, ErrPacketTooLarge},
	}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(limited)
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := cli.Publish(ctx, tt.opts); !errors.Is(err, tt.err) {
				t.Fatalf("Publish = %v, want %v", err, tt.err)
			}
		})
	}
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 1}); err != nil {
		t.Fatalf("publish within limits: %v", err)
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH && (p.QoS == 2 || p.Retain || len(p.Payload) > 0) {
			t.Fatalf("a refused publish reached the broker: %+v", p)
		}
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: strings.Repeat("f", 80)}}); !errors.Is(err, ErrPacketTooLarge) {
		t.Fatalf("oversized SUBSCRIBE = %v", err)
	}
}

// WithQoSDowngrade sends at the broker's maximum instead.
func TestQoSDowngrade(t *testing.T) {
	maxQoS := byte(1)
	got := make(chan byte, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{MaximumQoS: &maxQoS})
		p := c.Expect(wire.PUBLISH, 0)
		got <- p.QoS
		c.Puback(p.PacketID, wire.ReasonSuccess)
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithQoSDowngrade())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Publish(ctx, PublishOptions{Topic: "t", QoS: 2}); err != nil {
		t.Fatal(err)
	}
	if q := <-got; q != 1 {
		t.Fatalf("broker received QoS %d, want 1", q)
	}
}

// ServerInfo reports the effective limits with spec defaults.
func TestServerInfo(t *testing.T) {
	ka, rm := uint16(7), uint16(9)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{ServerKeepAlive: &ka, ReceiveMaximum: &rm, ResponseInformation: "resp/"})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	info, ok := cli.ServerInfo()
	if !ok {
		t.Fatal("not connected")
	}
	if info.KeepAlive != 7 || info.ReceiveMaximum != 9 || info.MaximumQoS != 2 || !info.RetainAvailable ||
		info.ResponseInformation != "resp/" || info.SessionExpiry != DefaultSessionExpiry {
		t.Fatalf("ServerInfo = %+v", info)
	}
}

// A packet above the client's advertised Maximum Packet Size closes
// the connection with DISCONNECT 0x95.
func TestInboundMaximumPacketSizeEnforced(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		ci := c.AcceptConnect(wire.ConnackOpts{})
		if v, ok := ci.Properties().Uint32(wire.PropMaximumPacketSize); !ok || v != 256 {
			c.T.Errorf("CONNECT Maximum Packet Size = %d, %v", v, ok)
		}
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "big", Payload: make([]byte, 1024)})
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonPacketTooLarge {
			c.T.Errorf("DISCONNECT reason = %#x, want 0x95", byte(p.Reason))
		}
	})
	cli := tbClient(t, b, WithMaximumPacketSize(256))
	ch, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "big"}})
	if err != nil {
		t.Fatal(err)
	}
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	select {
	case m := <-ch:
		t.Fatalf("oversized message delivered (%d bytes)", len(m.Payload))
	default:
	}
}

// Opting out of problem information sends the property as 0; the
// default leaves it absent (meaning 1).
func TestRequestProblemInformation(t *testing.T) {
	for _, tt := range []struct {
		opts    []Option
		present bool
		value   byte
	}{{nil, false, 0}, {[]Option{WithRequestProblemInformation(false)}, true, 0}} {
		got := make(chan *testbroker.ConnectInfo, 1)
		b := testbroker.New(t, func(c *testbroker.Conn) {
			got <- c.AcceptConnect(wire.ConnackOpts{})
			c.ServeAuto()
		})
		tbClient(t, b, tt.opts...)
		v, ok := (<-got).Properties().Byte(wire.PropRequestProblemInfo)
		if ok != tt.present || v != tt.value {
			t.Fatalf("Request Problem Information = %d, %v; want %d, %v", v, ok, tt.value, tt.present)
		}
	}
}

// DISCONNECT may not set a non-zero Session Expiry when CONNECT
// sent 0 [MQTT-3.14.2-2].
func TestDisconnectCannotRaiseZeroSessionExpiry(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithSessionExpiry(0))
	ten := uint32(10)
	err := cli.DisconnectWith(context.Background(), DisconnectOptions{SessionExpiryInterval: &ten})
	if !errors.Is(err, ErrInvalidSessionExpiry) {
		t.Fatalf("DisconnectWith = %v", err)
	}
	if !cli.Connected() {
		t.Fatal("refused DisconnectWith still disconnected")
	}
}

// Invalid publishes and subscriptions fail at the call and
// never reach the broker.
func TestInvalidRequestsNeverSent(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	cli := tbClient(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, tt := range []struct {
		name string
		opts PublishOptions
		want error
	}{
		{"wildcard topic qos0", PublishOptions{Topic: "a/#"}, ErrInvalidTopic},
		{"wildcard topic qos1", PublishOptions{Topic: "a/+", QoS: 1}, ErrInvalidTopic},
		{"NUL in topic", PublishOptions{Topic: "a\x00"}, ErrInvalidTopic},
		{"alias without topic at QoS 1", PublishOptions{TopicAlias: 1, QoS: 1}, ErrTopicAliasInvalid},
	} {
		if err := cli.Publish(ctx, tt.opts); !errors.Is(err, tt.want) {
			t.Errorf("%s: Publish = %v, want %v", tt.name, err, tt.want)
		}
	}
	for _, f := range []TopicFilter{{Topic: "$share/g/a", NoLocal: true}, {Topic: "a/#/b"}, {Topic: "$share//x"}} {
		if _, _, err := cli.Subscribe(ctx, []TopicFilter{f}); err == nil {
			t.Errorf("Subscribe(%+v) succeeded", f)
		}
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.PUBLISH || p.Type == wire.SUBSCRIBE {
			t.Fatalf("invalid request reached the broker: %+v", p)
		}
	}
}

// A malformed packet from the broker closes the connection with the
// spec's reason code and is counted.
func TestMalformedBrokerPacketDisconnects(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0x30, 4, 0, 1, 0xff, 0}) // PUBLISH with invalid UTF-8 topic
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonMalformedPacket {
			c.T.Errorf("DISCONNECT reason %#x, want 0x81", byte(p.Reason))
		}
	})
	cli := tbClient(t, b, WithStats())
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	if got := cli.Stats().ProtocolErrors; got != 1 {
		t.Fatalf("ProtocolErrors = %d", got)
	}
}

// A packet type only clients send is a protocol error from a broker.
func TestServerOnlyPacketFromBrokerDisconnects(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0xc0, 0}) // PINGREQ
		if p := c.Expect(wire.DISCONNECT, 0); p.Reason != wire.ReasonProtocolError {
			c.T.Errorf("DISCONNECT reason %#x, want 0x82", byte(p.Reason))
		}
	})
	tbClient(t, b)
	waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
}

// Lenient decoding tolerates a non-minimal Remaining Length.
func TestLenientDecodingOption(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Raw([]byte{0xd0, 0x80, 0x00}) // PINGRESP, Remaining Length in two bytes
		c.Hold(0)
	})
	cli := tbClient(t, b, WithLenientDecoding())
	cs := cli.cur.Load()
	for deadline := time.Now().Add(3 * time.Second); cs.reads.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("client never read the PINGRESP")
		}
	}
	if !cli.Connected() {
		t.Fatal("lenient client disconnected on a tolerated violation")
	}
	for _, p := range b.Conn(0, 0).Log() {
		if p.Type == wire.DISCONNECT {
			t.Fatal("lenient client sent DISCONNECT")
		}
	}
}

// New rejects options that could only fail later.
func TestNewValidatesOptions(t *testing.T) {
	bad := "a\x00b"
	for _, tt := range []struct {
		name string
		opts []Option
	}{
		{"ws without a dial function", []Option{WithBroker("ws://broker:80/mqtt")}},
		{"wss without a dial function", []Option{WithBroker("wss://broker:443/mqtt")}},
		{"negative write queue", []Option{WithWriteQueueSize(-1)}},
		{"negative pool size", []Option{WithPublisherPool(-2)}},
		{"negative connect timeout", []Option{WithConnectTimeout(-time.Second)}},
		{"negative disconnect flush timeout", []Option{WithDisconnectFlushTimeout(-time.Second)}},
		{"invalid will topic", []Option{WithWill(&WillOptions{Topic: "w/#"})}},
		{"NUL in client ID", []Option{WithClientID(bad)}},
		{"invalid user property", []Option{WithConnectUserProperties([]UserProperty{{Key: bad, Value: "v"}})}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]Option{WithBroker("mqtt://127.0.0.1:1883")}, tt.opts...)
			if _, err := New(opts...); err == nil {
				t.Fatal("New accepted it")
			}
		})
	}
	if _, err := New(WithBroker("ws://broker:80/mqtt"), WithDialFunc(func(context.Context, *url.URL) (transport.Conn, error) { return nil, nil })); err != nil {
		t.Fatalf("ws with a dial function: %v", err)
	}
}

// SetBrokers gives the pool's connections the new list too, so a
// reconnect never takes a pool member back to a broker the client left.
func TestSetBrokersMovesThePool(t *testing.T) {
	a := testbroker.New(t)
	b := testbroker.New(t)
	cli := tbClient(t, a, WithPublisherPool(2))
	waitForPoolReady(t, cli, 2, 3*time.Second)

	if err := cli.SetBrokers(b.URL()); err != nil {
		t.Fatal(err)
	}
	a.Close() // every connection drops, and a accepts no more

	missing := map[string]bool{cli.ClientID(): true, cli.ClientID() + "-pub-1": true, cli.ClientID() + "-pub-2": true}
	deadline := time.Now().Add(3 * time.Second)
	for len(missing) > 0 && time.Now().Before(deadline) {
		for i := range b.Accepted() {
			if c := b.Conn(i, time.Second); c != nil {
				if log := c.Log(); len(log) > 0 && log[0].Connect != nil {
					delete(missing, log[0].Connect.ClientID)
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(missing) > 0 {
		t.Fatalf("never reached the new broker: %v", slices.Sorted(maps.Keys(missing)))
	}
}

// Stats.add must sum every field, or a field added later would leave
// the pool members' share out of Client.Stats.
func TestStatsAddSumsEveryField(t *testing.T) {
	var one Stats
	v := reflect.ValueOf(&one).Elem()
	for i := range v.NumField() {
		v.Field(i).SetUint(1)
	}
	sum := one
	sum.add(one)
	sv := reflect.ValueOf(sum)
	for i := range sv.NumField() {
		if sv.Field(i).Uint() != 2 {
			t.Errorf("Stats.add leaves out %s", sv.Type().Field(i).Name)
		}
	}
}

// Concurrent SetBrokers calls leave the main connection and every pool
// member on the same list.
func TestConcurrentSetBrokersKeepsThePoolTogether(t *testing.T) {
	cli, err := New(WithBroker("mqtt://initial:1883"), WithPublisherPool(8), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	for round := range 2000 {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, url := range []string{"mqtt://a:1883", "mqtt://b:1883"} {
			wg.Go(func() {
				<-start
				if err := cli.SetBrokers(url); err != nil {
					t.Error(err)
				}
			})
		}
		close(start)
		wg.Wait()
		main := *cli.brokerURLs.Load()
		for i, m := range cli.pool.members {
			if got := *m.brokerURLs.Load(); !slices.Equal(got, main) {
				t.Fatalf("round %d: main %v, member %d %v", round, main, i+1, got)
			}
		}
	}
}
