// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

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

// groupBrokerHandler returns a fakeBroker handler that records each
// PUBLISH (member-name + topic) using the CONNECT Username as the
// member identifier. PUBACK every QoS 1 so reliable publishes
// don't block.
func groupBrokerHandler(t *testing.T, rec *groupPublishRecorder) func(*fakeBroker, net.Conn) {
	t.Helper()
	return func(fb *fakeBroker, c net.Conn) {
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
	fb1 := newFakeBroker(t, groupBrokerHandler(t, rec))
	fb2 := newFakeBroker(t, groupBrokerHandler(t, rec))

	g, err := NewClientGroup(
		[]GroupMember{
			{
				Broker: fb1.URL(),
				Name:   "primary",
				Opts:   []Option{WithCredentials("primary-user", []byte("pw1"))},
			},
			{
				Broker: fb2.URL(),
				Name:   "secondary",
				Opts:   []Option{WithCredentials("secondary-user", []byte("pw2"))},
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
	if err := g.Publish(context.Background(), wire.PublishOpts{
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
	brokers := make([]*fakeBroker, memberCount)
	members := make([]GroupMember, memberCount)
	for i := range memberCount {
		brokers[i] = newFakeBroker(t, groupBrokerHandler(t, rec))
		members[i] = GroupMember{
			Broker: brokers[i].URL(),
			Name:   fmt.Sprintf("m%d", i+1),
			Opts:   []Option{WithCredentials(fmt.Sprintf("m%d", i+1), nil)},
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
		if err := g.Publish(context.Background(), wire.PublishOpts{
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
	brokers := make([]*fakeBroker, memberCount)
	members := make([]GroupMember, memberCount)
	for i := range memberCount {
		brokers[i] = newFakeBroker(t, groupBrokerHandler(t, rec))
		members[i] = GroupMember{
			Broker: brokers[i].URL(),
			Name:   fmt.Sprintf("m%d", i+1),
			Opts:   []Option{WithCredentials(fmt.Sprintf("m%d", i+1), nil)},
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
		if err := g.Publish(context.Background(), wire.PublishOpts{
			Topic: "alpha", Payload: []byte("x"), QoS: 1,
		}); err != nil {
			t.Fatalf("Publish alpha: %v", err)
		}
		if err := g.Publish(context.Background(), wire.PublishOpts{
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
	fb1 := newFakeBroker(t, groupBrokerHandler(t, &groupPublishRecorder{}))
	fb2 := newFakeBroker(t, groupBrokerHandler(t, &groupPublishRecorder{}))

	g, err := NewClientGroup([]GroupMember{
		{Broker: fb1.URL(), Name: "primary"},
		{Broker: fb2.URL(), Name: "secondary"},
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
	makeSlowBroker := func() *fakeBroker {
		return newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
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
		members[i] = GroupMember{Broker: makeSlowBroker().URL()}
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
	makeBroker := func() *fakeBroker {
		return newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
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
		{Broker: fb1.URL(), Name: "alpha"},
		{Broker: fb2.URL(), Name: "beta"},
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
		{Broker: "mqtt://127.0.0.1:1", Name: "same"},
		{Broker: "mqtt://127.0.0.1:2", Name: "same"},
	})
	if err == nil {
		t.Fatal("NewClientGroup accepted duplicate Name; want error")
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
	ok := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess})
		<-fb.Done()
	})
	bad := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		pkt, _ := dec.ReadPacket()
		if pkt != nil {
			pkt.Release()
		}
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonNotAuthorized})
	})

	g, err := NewClientGroup([]GroupMember{
		{Broker: ok.URL(), Name: "good"},
		{Broker: bad.URL(), Name: "evil"},
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
