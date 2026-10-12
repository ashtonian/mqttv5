// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// groupPublishRecorder records (memberName, topic) pairs for tests
// that exercise GroupPublishPolicy dispatch. The memberName comes
// from the CONNECT Username field — tests configure each member
// with WithCredentials so the broker can identify which connection
// is publishing without needing to thread ClientID parsing through
// the recorder.
type groupPublishRecorder struct {
	mu   sync.Mutex
	pubs []struct{ member, topic string }
}

func (r *groupPublishRecorder) record(member, topic string) {
	r.mu.Lock()
	r.pubs = append(r.pubs, struct{ member, topic string }{member, topic})
	r.mu.Unlock()
}

func (r *groupPublishRecorder) snapshot() []struct{ member, topic string } {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]struct{ member, topic string }, len(r.pubs))
	copy(out, r.pubs)
	return out
}

// groupBrokerHandler returns a raw testbroker handler that records each
// PUBLISH (member-name + topic) using the CONNECT Username as the
// member identifier. PUBACK every QoS 1 so reliable publishes
// don't block.
func groupBrokerHandler(t *testing.T, rec *groupPublishRecorder) func(*testbroker.Broker, net.Conn) {
	t.Helper()
	return func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, err := dec.ReadPacket()
		if err != nil {
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			pkt.Release()
			return
		}
		// conn.Username aliases the frame; clone before Release so
		// the captured identifier survives subsequent frame reuse.
		member := strings.Clone(conn.Username)
		pkt.Release()
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}); err != nil {
			return
		}
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
			topic := strings.Clone(pub.Topic)
			id := pub.PacketID
			pkt.Release()
			rec.record(member, topic)
			if id != 0 {
				_, _ = wire.WritePuback(c, wire.PubRespOpts{
					PacketID: id, ReasonCode: wire.ReasonSuccess,
				})
			}
		}
	}
}

// waitForGroupConnected blocks until every group member's Connected()
// is true.
func waitForGroupConnected(t *testing.T, g *ClientGroup, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := 0
		for _, m := range g.Members() {
			if m.Connected() {
				ready++
			}
		}
		if ready == len(g.Members()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("group not fully connected within %v", timeout)
}

// TestClientGroupPerMemberAuth verifies that per-member Opts override
// shared Opts — typical use is per-broker credentials.
func TestClientGroupPerMemberAuth(t *testing.T) {
	rec := &groupPublishRecorder{}
	fb1 := testbroker.NewRaw(t, groupBrokerHandler(t, rec))
	fb2 := testbroker.NewRaw(t, groupBrokerHandler(t, rec))

	g, err := NewClientGroup(
		[]GroupMember{
			{
				Brokers: []string{fb1.URL()},
				Name:    "primary",
				Opts:    []Option{WithCredentials("primary-user", []byte("pw1"))},
			},
			{
				Brokers: []string{fb2.URL()},
				Name:    "secondary",
				Opts:    []Option{WithCredentials("secondary-user", []byte("pw2"))},
			},
		},
	)
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer g.Disconnect(context.Background())

	waitForGroupConnected(t, g, 2*time.Second)
	if _, err := g.Publish(context.Background(), PublishOptions{
		Topic: "t", Payload: []byte("p"), QoS: 1,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pubs := rec.snapshot()
	if len(pubs) != 2 {
		t.Fatalf("recorded %d publishes, want 2", len(pubs))
	}
	members := map[string]bool{}
	for _, p := range pubs {
		members[p.member] = true
	}
	if !members["primary-user"] || !members["secondary-user"] {
		t.Errorf("expected both per-member usernames; got %v", members)
	}
}

// TestClientGroupPublishRoundRobin verifies that GroupPublishRoundRobin
// distributes publishes across members.
func TestClientGroupPublishRoundRobin(t *testing.T) {
	rec := &groupPublishRecorder{}
	const memberCount = 3
	brokers := make([]*testbroker.Broker, memberCount)
	members := make([]GroupMember, memberCount)
	for i := range memberCount {
		brokers[i] = testbroker.NewRaw(t, groupBrokerHandler(t, rec))
		members[i] = GroupMember{
			Brokers: []string{brokers[i].URL()},
			Name:    fmt.Sprintf("m%d", i+1),
			Opts:    []Option{WithCredentials(fmt.Sprintf("m%d", i+1), nil)},
		}
	}

	g, err := NewClientGroup(members, WithGroupPublishPolicy(GroupPublishRoundRobin))
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer g.Disconnect(context.Background())

	waitForGroupConnected(t, g, 2*time.Second)

	// 9 publishes to the same topic — every member must take at
	// least one.
	for range 9 {
		if _, err := g.Publish(context.Background(), PublishOptions{
			Topic: "rr", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 9 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pubs := rec.snapshot()
	if len(pubs) != 9 {
		t.Fatalf("recorded %d publishes, want 9", len(pubs))
	}
	hit := map[string]int{}
	for _, p := range pubs {
		hit[p.member]++
	}
	if len(hit) < memberCount {
		t.Errorf("round-robin reached %d of %d members: %v", len(hit), memberCount, hit)
	}
}

// TestClientGroupPublishHashByTopic verifies that the same topic
// always lands on the same member.
func TestClientGroupPublishHashByTopic(t *testing.T) {
	rec := &groupPublishRecorder{}
	const memberCount = 3
	brokers := make([]*testbroker.Broker, memberCount)
	members := make([]GroupMember, memberCount)
	for i := range memberCount {
		brokers[i] = testbroker.NewRaw(t, groupBrokerHandler(t, rec))
		members[i] = GroupMember{
			Brokers: []string{brokers[i].URL()},
			Name:    fmt.Sprintf("m%d", i+1),
			Opts:    []Option{WithCredentials(fmt.Sprintf("m%d", i+1), nil)},
		}
	}

	g, err := NewClientGroup(members, WithGroupPublishPolicy(GroupPublishHashByTopic))
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer g.Disconnect(context.Background())

	waitForGroupConnected(t, g, 2*time.Second)

	for range 5 {
		if _, err := g.Publish(context.Background(), PublishOptions{
			Topic: "alpha", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish alpha: %v", err)
		}
		if _, err := g.Publish(context.Background(), PublishOptions{
			Topic: "beta", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish beta: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) >= 10 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	pubs := rec.snapshot()
	if len(pubs) != 10 {
		t.Fatalf("recorded %d publishes, want 10", len(pubs))
	}

	alpha := map[string]bool{}
	beta := map[string]bool{}
	for _, p := range pubs {
		switch p.topic {
		case "alpha":
			alpha[p.member] = true
		case "beta":
			beta[p.member] = true
		}
	}
	if len(alpha) != 1 {
		t.Errorf("topic alpha hit %d distinct members (want 1): %v", len(alpha), alpha)
	}
	if len(beta) != 1 {
		t.Errorf("topic beta hit %d distinct members (want 1): %v", len(beta), beta)
	}
}

// TestClientGroupMemberByName verifies that GroupMember.Name keys
// into Member() and that the Subscribe token map is name-keyed.
func TestClientGroupMemberByName(t *testing.T) {
	fb1 := testbroker.NewRaw(t, groupBrokerHandler(t, &groupPublishRecorder{}))
	fb2 := testbroker.NewRaw(t, groupBrokerHandler(t, &groupPublishRecorder{}))

	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{fb1.URL()}, Name: "primary"},
		{Brokers: []string{fb2.URL()}, Name: "secondary"},
	})
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if g.Member("primary") == nil || g.Member("secondary") == nil {
		t.Fatal("Member lookup returned nil for known names")
	}
	if g.Member("nope") != nil {
		t.Fatal("Member returned non-nil for unknown name")
	}

	gotNames := g.Names()
	if len(gotNames) != 2 || gotNames[0] != "primary" || gotNames[1] != "secondary" {
		t.Errorf("Names() = %v, want [primary secondary]", gotNames)
	}
}

// TestClientGroupParallelConnect verifies that Connect across N
// members completes in less than N * single-connect-time. We use
// connect-context-blocked brokers to make sequential N*T visible.
func TestClientGroupParallelConnect(t *testing.T) {
	const memberCount = 4
	const slack = 200 * time.Millisecond

	// Broker that delays the CONNACK reply.
	makeSlowBroker := func() *testbroker.Broker {
		return testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
			defer c.Close()
			dec := wire.NewDecoder(c)
			pkt, _ := dec.ReadPacket()
			if pkt != nil {
				pkt.Release()
			}
			time.Sleep(slack)
			_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
			<-fb.Done()
		})
	}
	members := make([]GroupMember, memberCount)
	for i := range memberCount {
		members[i] = GroupMember{Brokers: []string{makeSlowBroker().URL()}}
	}

	g, err := NewClientGroup(members)
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	defer g.Disconnect(context.Background())

	start := time.Now()
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	elapsed := time.Since(start)

	// Sequential would be memberCount * slack. Parallel should be
	// roughly one slack window plus scheduling jitter.
	maxParallel := slack + 200*time.Millisecond
	if elapsed > maxParallel {
		t.Errorf("Connect took %v, want < %v (parallel should be ~%v)",
			elapsed, maxParallel, slack)
	}
}

// TestClientGroupNamedTokens verifies that Subscribe returns a
// name-keyed token map and UnsubscribeAll tears them down.
func TestClientGroupNamedTokens(t *testing.T) {
	makeBroker := func() *testbroker.Broker {
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
				switch p := pkt.(type) {
				case *wire.Subscribe:
					_, _ = wire.WriteSuback(c, wire.SubackOpts{
						PacketID:    p.PacketID,
						ReasonCodes: []wire.ReasonCode{wire.ReasonGrantedQoS1},
					})
				case *wire.Unsubscribe:
					_, _ = wire.WriteUnsuback(c, wire.UnsubackOpts{
						PacketID:    p.PacketID,
						ReasonCodes: []wire.ReasonCode{wire.ReasonSuccess},
					})
				}
				pkt.Release()
			}
		})
	}
	fb1 := makeBroker()
	fb2 := makeBroker()

	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{fb1.URL()}, Name: "alpha"},
		{Brokers: []string{fb2.URL()}, Name: "beta"},
	})
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer g.Disconnect(context.Background())

	_, tokens, err := g.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "t/x", QoS: 1}})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, ok := tokens["alpha"]; !ok {
		t.Errorf("tokens missing alpha key: %v", tokens)
	}
	if _, ok := tokens["beta"]; !ok {
		t.Errorf("tokens missing beta key: %v", tokens)
	}
	if err := g.UnsubscribeAll(context.Background(), tokens); err != nil {
		t.Errorf("UnsubscribeAll: %v", err)
	}
}

// TestClientGroupDuplicateMemberName rejects two members with the
// same Name — the token map and Member lookup require uniqueness.
func TestClientGroupDuplicateMemberName(t *testing.T) {
	_, err := NewClientGroup([]GroupMember{
		{Brokers: []string{"mqtt://127.0.0.1:1"}, Name: "same"},
		{Brokers: []string{"mqtt://127.0.0.1:2"}, Name: "same"},
	})
	if err == nil {
		t.Fatal("NewClientGroup accepted duplicate Name; want error")
	}
}

// A member's brokers come only from GroupMember.Brokers.
func TestClientGroupMemberBrokers(t *testing.T) {
	tests := []struct {
		name   string
		member GroupMember
		shared []Option
		want   string
	}{
		{"none", GroupMember{}, nil, "Brokers is required"},
		{"in member opts", GroupMember{
			Brokers: []string{"mqtt://127.0.0.1:1"},
			Opts:    []Option{WithBrokers("mqtt://127.0.0.1:2")},
		}, nil, "set brokers in GroupMember.Brokers"},
		{"in shared opts", GroupMember{Brokers: []string{"mqtt://127.0.0.1:1"}},
			[]Option{WithBroker("mqtt://127.0.0.1:2")}, "set brokers in GroupMember.Brokers"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClientGroup([]GroupMember{tt.member}, WithGroupSharedOpts(tt.shared...))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("NewClientGroup = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// Members with the same ClientID get a suffix; members without one
// leave it to their brokers.
func TestClientGroupMemberClientIDs(t *testing.T) {
	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{"mqtt://127.0.0.1:1"}, Opts: []Option{WithClientID("svc")}},
		{Brokers: []string{"mqtt://127.0.0.1:2"}, Opts: []Option{WithClientID("svc")}},
		{Brokers: []string{"mqtt://127.0.0.1:3"}},
		{Brokers: []string{"mqtt://127.0.0.1:4"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, m := range g.Members() {
		ids = append(ids, m.ClientID())
	}
	if want := []string{"svc", "svc-2", "", ""}; !slices.Equal(ids, want) {
		t.Fatalf("member ClientIDs = %q, want %q", ids, want)
	}
}

// A member with several brokers connects through whichever accepts it.
func TestClientGroupMemberFailover(t *testing.T) {
	fb := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		acceptConnect(t, c, wire.NewDecoder(c))
		<-fb.Done()
	})
	g, err := NewClientGroup([]GroupMember{
		{Name: "region", Brokers: []string{"mqtt://127.0.0.1:1", fb.URL()}},
	}, WithGroupSharedOpts(WithLogger(quietLogger())))
	if err != nil {
		t.Fatal(err)
	}
	m := g.Member("region")
	m.brokerIdx.Store(0)
	if err := g.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer g.Disconnect(context.Background())
	if info, _ := m.ServerInfo(); info.Broker != fb.URL() {
		t.Fatalf("member connected to %q, want %q", info.Broker, fb.URL())
	}
}

// TestClientGroupEmptyMembers rejects construction with no members.
func TestClientGroupEmptyMembers(t *testing.T) {
	_, err := NewClientGroup(nil)
	if err == nil {
		t.Fatal("NewClientGroup accepted empty member list; want error")
	}
	if !strings.Contains(err.Error(), "at least one member") {
		t.Errorf("err = %v, want mention of 'at least one member'", err)
	}
}

// TestClientGroupConnectPartialFailure verifies that a partial
// Connect failure surfaces the failing member in the joined error
// while the group is still usable for the healthy members.
func TestClientGroupConnectPartialFailure(t *testing.T) {
	ok := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})
	bad := testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonNotAuthorized})
	})

	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{ok.URL()}, Name: "good"},
		{Brokers: []string{bad.URL()}, Name: "evil"},
	})
	if err != nil {
		t.Fatalf("NewClientGroup: %v", err)
	}
	defer g.Disconnect(context.Background())

	err = g.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded; want partial-failure error")
	}
	if !strings.Contains(err.Error(), "evil") {
		t.Errorf("err = %v, want mention of failing member 'evil'", err)
	}
	if !g.Member("good").Connected() {
		t.Fatal("'good' member not connected after partial-failure Connect")
	}
	if !errors.Is(err, ErrConnectRefused) {
		// Joined error should wrap the per-member ErrConnectRefused.
		t.Logf("note: joined Connect error doesn't wrap ErrConnectRefused: %v", err)
	}
}

func TestClientGroupPublishFanOut(t *testing.T) {
	publishesSeen := atomic.Int32{}

	makeBroker := func() *testbroker.Broker {
		return testbroker.NewRaw(t, func(fb *testbroker.Broker, c net.Conn) {
			defer c.Close()
			dec := wire.NewDecoder(c)
			acceptConnect(t, c, dec)
			for {
				pkt, err := dec.ReadPacket()
				if err != nil {
					return
				}
				if _, ok := pkt.(*wire.Publish); ok {
					publishesSeen.Add(1)
				}
				pkt.Release()
			}
		})
	}
	fb1 := makeBroker()
	fb2 := makeBroker()

	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{fb1.URL()}},
		{Brokers: []string{fb2.URL()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())

	if _, err := g.Publish(context.Background(), PublishOptions{
		Topic:   "alpha",
		Payload: []byte("ping"),
		QoS:     0,
	}); err != nil {
		t.Fatalf("group Publish: %v", err)
	}

	// Allow both brokers' read goroutines to dispatch.
	deadline := time.Now().Add(2 * time.Second)
	for publishesSeen.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := publishesSeen.Load(); got != 2 {
		t.Errorf("publishesSeen = %d, want 2 (both brokers should receive)", got)
	}
}

func TestClientGroupSubscribeFanIn(t *testing.T) {
	makeBroker := func(payload []byte) *testbroker.Broker {
		return testbroker.NewRaw(t, pushPublishBroker(t, payload))
	}
	fb1 := makeBroker([]byte("from-broker-1"))
	fb2 := makeBroker([]byte("from-broker-2"))

	g, err := NewClientGroup([]GroupMember{
		{Brokers: []string{fb1.URL()}},
		{Brokers: []string{fb2.URL()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())

	ch, _, err := g.Subscribe(context.Background(),
		[]TopicFilter{{Topic: "sensor/+", QoS: 1}}, SubBuffer(8))
	if err != nil {
		t.Fatalf("group Subscribe: %v", err)
	}

	got := map[string]bool{}
	deadline := time.NewTimer(3 * time.Second)
	for len(got) < 2 {
		select {
		case msg := <-ch:
			got[string(msg.Payload)] = true
			_ = msg.Ack()
		case <-deadline.C:
			t.Fatalf("only got %d messages: %v", len(got), got)
		}
	}
	if !got["from-broker-1"] || !got["from-broker-2"] {
		t.Errorf("missing payloads: %v", got)
	}
}

// groupBroker serves one member: it answers SUBSCRIBE with subscribe
// (or not at all when holdSubscribe), PUBLISH with publish, and counts
// PUBLISHes and UNSUBSCRIBEs.
type groupBroker struct {
	b             *testbroker.Broker
	subscribe     wire.ReasonCode
	holdSubscribe bool
	publish       wire.ReasonCode
	publishes     atomic.Int32
	unsubscribes  atomic.Int32
	conn          atomic.Pointer[testbroker.Conn]
}

func newGroupBroker(t *testing.T, subscribe, publish wire.ReasonCode) *groupBroker {
	gb := &groupBroker{subscribe: subscribe, publish: publish}
	gb.b = testbroker.New(t)
	gb.b.SetFallback(func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		gb.conn.Store(c)
		for {
			p, ok, err := c.Next(time.Minute)
			if !ok {
				if errors.Is(err, testbroker.ErrTimeout) {
					continue
				}
				return
			}
			switch p.Type {
			case wire.SUBSCRIBE:
				if gb.holdSubscribe {
					continue
				}
				codes := make([]wire.ReasonCode, len(p.Filters))
				for i := range codes {
					codes[i] = gb.subscribe
				}
				c.Suback(p.PacketID, codes...)
			case wire.UNSUBSCRIBE:
				gb.unsubscribes.Add(1)
				codes := make([]wire.ReasonCode, len(p.Topics))
				c.Write(func(w io.Writer) (int64, error) {
					return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
				})
			case wire.PUBLISH:
				gb.publishes.Add(1)
				if p.QoS == 1 {
					c.Puback(p.PacketID, gb.publish)
				}
			case wire.PINGREQ:
				c.Write(wire.WritePingresp)
			case wire.DISCONNECT:
				return
			}
		}
	})
	return gb
}

func newTestGroup(t *testing.T, brokers []*groupBroker, opts ...ClientGroupOption) *ClientGroup {
	t.Helper()
	members := make([]GroupMember, len(brokers))
	for i, gb := range brokers {
		members[i] = GroupMember{Brokers: []string{gb.b.URL()}}
	}
	opts = append([]ClientGroupOption{WithGroupSharedOpts(WithLogger(quietLogger()), WithClientID(t.Name()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))}, opts...)
	g, err := NewClientGroup(members, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Disconnect(ctx)
	})
	return g
}

func waitCount(t *testing.T, n *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for n.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("count %d, want %d", n.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// The merged channel closes when every member's subscription
// ends, by UnsubscribeAll or by Disconnect.
func TestClientGroupMergedOutputsClose(t *testing.T) {
	t.Run("UnsubscribeAll", func(t *testing.T) {
		g := newTestGroup(t, []*groupBroker{newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)})
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil || len(tokens) != 2 {
			t.Fatalf("Subscribe = %v, %v", tokens, err)
		}
		if err := g.UnsubscribeAll(context.Background(), tokens); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, ch)
	})
	t.Run("Disconnect queue", func(t *testing.T) {
		g := newTestGroup(t, []*groupBroker{newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)})
		q, _, err := g.SubscribeQueue(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Disconnect(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, ok := q.Dequeue(ctx); ok || ctx.Err() != nil {
			t.Fatal("merged queue not closed by Disconnect")
		}
		if _, err := g.Publish(context.Background(), PublishOptions{Topic: "t"}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Publish after Disconnect = %v, want ErrClosed", err)
		}
	})
}

// A group subscribe that does not meet its success policy, or whose ctx
// ends, takes back the members that subscribed.
func TestClientGroupSubscribeUnwinds(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, wire.ReasonNotAuthorized, 0)
		g := newTestGroup(t, []*groupBroker{ok, refusing})
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		var gerr *GroupError
		if !errors.As(err, &gerr) || !errors.Is(err, ErrNotAuthorized) || ch != nil || tokens != nil {
			t.Fatalf("Subscribe = %v, %v, %v; want a GroupError", ch, tokens, err)
		}
		waitCount(t, &ok.unsubscribes, 1)
	})
	t.Run("any is enough", func(t *testing.T) {
		ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, wire.ReasonNotAuthorized, 0)
		g := newTestGroup(t, []*groupBroker{ok, refusing}, WithGroupSuccess(GroupSuccessAny))
		ch, tokens, err := g.Subscribe(context.Background(), []TopicFilter{{Topic: "g/#"}})
		if err != nil || ch == nil || len(tokens) != 1 || tokens["member-1"].sub == nil {
			t.Fatalf("Subscribe = %v, %v, %v", ch, tokens, err)
		}
	})
	t.Run("ctx", func(t *testing.T) {
		ok, slow := newGroupBroker(t, 0, 0), newGroupBroker(t, 0, 0)
		slow.holdSubscribe = true
		g := newTestGroup(t, []*groupBroker{ok, slow})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if _, _, err := g.Subscribe(ctx, []TopicFilter{{Topic: "g/#"}}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Subscribe = %v", err)
		}
		waitCount(t, &ok.unsubscribes, 1)
	})
}

// A broadcast publish reports every member; the success policy
// decides the error. No policy duplicates a refused message.
func TestClientGroupPublishResults(t *testing.T) {
	for _, tt := range []struct {
		name    string
		success GroupSuccess
		fail    bool
	}{{"all", GroupSuccessAll, true}, {"any", GroupSuccessAny, false}} {
		t.Run(tt.name, func(t *testing.T) {
			ok, refusing := newGroupBroker(t, 0, 0), newGroupBroker(t, 0, wire.ReasonNotAuthorized)
			g := newTestGroup(t, []*groupBroker{ok, refusing}, WithGroupSuccess(tt.success))
			results, err := g.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1})
			if (err != nil) != tt.fail || len(results) != 2 || results[0].Err != nil || !errors.Is(results[1].Err, ErrNotAuthorized) {
				t.Fatalf("Publish = %+v, %v", results, err)
			}
			if ok.publishes.Load() != 1 || refusing.publishes.Load() != 1 {
				t.Fatalf("PUBLISHes %d and %d, want one each", ok.publishes.Load(), refusing.publishes.Load())
			}
		})
	}
	t.Run("round robin", func(t *testing.T) {
		a, b := newGroupBroker(t, 0, wire.ReasonQuotaExceeded), newGroupBroker(t, 0, wire.ReasonQuotaExceeded)
		g := newTestGroup(t, []*groupBroker{a, b}, WithGroupPublishPolicy(GroupPublishRoundRobin))
		_, err := g.Publish(context.Background(), PublishOptions{Topic: "t", QoS: 1})
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("Publish = %v", err)
		}
		if n := a.publishes.Load() + b.publishes.Load(); n != 1 {
			t.Fatalf("one refused Publish became %d PUBLISHes", n)
		}
	})
}

// A typed subscription needs no goroutine of its own, so one whose
// consumer stopped reading leaves nothing behind once it ends.
func TestTypedSubscriptionLeavesNothingBehind(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		for i := range 8 {
			c.Publish(wire.PublishOpts{Topic: "t", Payload: []byte(`"` + string(rune('a'+i)) + `"`)})
		}
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithStats())
	typed := NewTyped(cli, testJSONCodec[string]{})
	before := runtime.NumGoroutine()
	ch, tok, err := typed.Subscribe(context.Background(), []TopicFilter{{Topic: "t"}}, SubBuffer(2))
	if err != nil {
		t.Fatal(err)
	}
	// The broker's 8 messages fill the 2-slot buffer; the rest drop.
	deadline := time.Now().Add(3 * time.Second)
	for cli.Stats().InboundDropped < 6 {
		if time.Now().After(deadline) {
			t.Fatalf("%d dropped, want 6", cli.Stats().InboundDropped)
		}
		time.Sleep(time.Millisecond)
	}
	if err := cli.Unsubscribe(context.Background(), tok); err != nil {
		t.Fatal(err)
	}
	got := 0
	for range ch {
		got++
	}
	if got != 2 {
		t.Fatalf("%d messages through a 2-slot buffer the consumer never read", got)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("%d goroutines before the subscription, %d after it ended", before, after)
	}
}
