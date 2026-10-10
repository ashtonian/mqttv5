// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/soak"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// modelBroker keeps subscriptions the way §3.8.4 says a broker must: one
// per exact filter, replaced by a later SUBSCRIBE for the same filter and
// tagged with that SUBSCRIBE's identifier. Its session survives a
// reconnect when the next CONNACK says so.
type modelBroker struct {
	t *testing.T

	mu         sync.Mutex
	subs       map[string]uint32
	keep       bool // next CONNACK reports Session Present when CleanStart allows
	singleCopy bool // one PUBLISH tagged with every matching subscription, else one per subscription
	conn       *testbroker.Conn
}

func (m *modelBroker) serve(c *testbroker.Conn) {
	p, ok := c.Await(wire.CONNECT, 0)
	if !ok {
		return
	}
	avail := byte(1)
	m.mu.Lock()
	sp := m.keep && !p.Connect.CleanStart
	if !sp {
		m.subs = map[string]uint32{}
	}
	m.conn = c
	m.mu.Unlock()
	if err := c.Write(func(w io.Writer) (int64, error) {
		return wire.WriteConnack(w, wire.ConnackOpts{SessionPresent: sp, SubscriptionIdentifierAvailable: &avail})
	}); err != nil {
		return
	}
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
			codes := make([]wire.ReasonCode, len(p.Filters))
			m.mu.Lock()
			for _, f := range p.Filters {
				m.subs[f.Topic] = subIDOf(p)
			}
			m.mu.Unlock()
			c.Suback(p.PacketID, codes...)
		case wire.UNSUBSCRIBE:
			codes := make([]wire.ReasonCode, len(p.Topics))
			m.mu.Lock()
			for i, f := range p.Topics {
				if _, ok := m.subs[f]; !ok {
					codes[i] = wire.ReasonNoSubscriptionExisted
				}
				delete(m.subs, f)
			}
			m.mu.Unlock()
			c.Write(func(w io.Writer) (int64, error) {
				return wire.WriteUnsuback(w, wire.UnsubackOpts{PacketID: p.PacketID, ReasonCodes: codes})
			})
		case wire.PINGREQ:
			c.Write(wire.WritePingresp)
		case wire.DISCONNECT:
			return
		}
	}
}

func (m *modelBroker) filters() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var fs []string
	for f := range m.subs {
		fs = append(fs, f)
	}
	slices.Sort(fs)
	return fs
}

// publish sends topic to every matching subscription.
func (m *modelBroker) publish(topic, payload string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []uint32
	for f, id := range m.subs {
		if filterMatches(f, topic) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	opts := wire.PublishOpts{Topic: topic, Payload: []byte(payload)}
	if m.singleCopy && len(ids) > 0 {
		m.conn.PublishTagged(opts, ids...)
		return
	}
	for _, id := range ids {
		m.conn.PublishTagged(opts, id)
	}
}

func (m *modelBroker) subsSnapshot() map[string]uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.subs)
}

func (c *Client) subsDump() string {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	var b strings.Builder
	for f, bs := range c.brokerSubs {
		fmt.Fprintf(&b, "%s ids=%v holders=%d view=%v; ", f, bs.ids, len(bs.holders), bs.view.Load().ids)
	}
	for id, sub := range c.activeSubs {
		fmt.Fprintf(&b, "\nsub %d state %d filters %v", id, sub.state, sub.filters)
	}
	return b.String()
}

// live reports whether the broker has a connection the test has not
// dropped; once the client is connected, it is connected to that one.
func (m *modelBroker) live() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn != nil
}

func (m *modelBroker) drop(keep bool) {
	m.mu.Lock()
	m.keep = keep
	c := m.conn
	m.conn = nil
	m.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// filterMatches is §4.7 matching, written independently of the trie.
func filterMatches(filter, topic string) bool {
	if strings.HasPrefix(filter, "$share/") {
		_, filter, _ = strings.Cut(strings.TrimPrefix(filter, "$share/"), "/")
	}
	f, tp := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i, level := range f {
		if level == "#" {
			return true
		}
		if i >= len(tp) || (level != "+" && level != tp[i]) {
			return false
		}
	}
	return len(f) == len(tp)
}

type modelSub struct {
	filter string
	ch     <-chan *Message
	token  SubscriptionToken
}

// TestSubscriptionModel runs random rounds of concurrent Subscribe,
// Unsubscribe and abandoned Subscribe calls, with connection drops that
// keep or lose the session, against modelBroker. After each round the
// broker must hold exactly the filters of the open subscriptions, and a
// message to each probe topic must reach every open subscription whose
// filter matches it exactly once and no other.
func TestSubscriptionModel(t *testing.T) {
	const rounds = 25
	base, n := soak.Seeds(t, 20, 4)
	for seed := base; seed < base+n; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			runSubscriptionModel(t, seed, rounds)
		})
	}
}

func runSubscriptionModel(t *testing.T, seed uint64, rounds int) {
	rng := rand.New(rand.NewPCG(seed, 0x5eed))
	// Every filter matches the final probe, a/b, so each round ends with
	// one known message per open subscription.
	filterPool := []string{"a/b", "a/+", "a/#", "#", "+/b", "$share/g/a/b"}
	probes := []string{"a/c", "b/b", "a/b/c", "a/b"}

	m := &modelBroker{t: t, singleCopy: seed%2 == 1}
	b := testbroker.New(t)
	b.SetFallback(m.serve)
	cli := tbClient(t, b)

	var open []*modelSub
	for round := range rounds {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			added   []*modelSub
			removed []*modelSub
		)
		for range 1 + rng.IntN(4) {
			switch op := rng.IntN(10); {
			case op < 4: // subscribe
				filter := filterPool[rng.IntN(len(filterPool))]
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: filter}}, SubBuffer(64))
					if errors.Is(err, ErrNotConnected) {
						return
					}
					if err != nil {
						t.Errorf("round %d: Subscribe(%s): %v", round, filter, err)
						return
					}
					mu.Lock()
					added = append(added, &modelSub{filter: filter, ch: ch, token: tok})
					mu.Unlock()
				})
			case op < 6: // subscribe, then give up at a random point
				filter := filterPool[rng.IntN(len(filterPool))]
				wait := time.Duration(rng.IntN(300)) * time.Microsecond
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), wait)
					defer cancel()
					ch, tok, err := cli.Subscribe(ctx, []TopicFilter{{Topic: filter}}, SubBuffer(64))
					if err != nil {
						return
					}
					mu.Lock()
					added = append(added, &modelSub{filter: filter, ch: ch, token: tok})
					mu.Unlock()
				})
			case op < 9: // unsubscribe one opened in an earlier round
				if len(open) == 0 {
					continue
				}
				i := rng.IntN(len(open))
				s := open[i]
				open = slices.Delete(open, i, i+1)
				removed = append(removed, s)
				wg.Go(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := cli.Unsubscribe(ctx, s.token); err != nil {
						t.Errorf("round %d: Unsubscribe(%s): %v", round, s.filter, err)
					}
				})
			default:
				m.drop(rng.IntN(2) == 0)
			}
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		open = append(open, added...)
		for _, s := range removed {
			for msg := range s.ch {
				t.Fatalf("round %d: unsubscribed %s still held %q", round, s.filter, msg.Payload)
			}
		}

		want := make([]string, 0, len(open))
		for _, s := range open {
			if !slices.Contains(want, s.filter) {
				want = append(want, s.filter)
			}
		}
		slices.Sort(want)
		deadline := time.Now().Add(5 * time.Second)
		for !slices.Equal(m.filters(), want) || !cli.ctrlIdle() || !m.live() || cli.cur.Load() == nil {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: broker holds %q, open subscriptions %q", round, m.filters(), want)
			}
			time.Sleep(time.Millisecond)
		}

		for _, p := range probes {
			m.publish(p, fmt.Sprintf("%d:%s", round, p))
		}
		for _, s := range open {
			var expect []string
			for _, p := range probes {
				if filterMatches(s.filter, p) {
					expect = append(expect, fmt.Sprintf("%d:%s", round, p))
				}
			}
			for _, e := range expect {
				var msg *Message
				select {
				case msg = <-s.ch:
				case <-time.After(5 * time.Second):
					var tracked []uint64
					for _, o := range open {
						tracked = append(tracked, o.token.sub.id)
					}
					t.Fatalf("round %d: %s (sub %d) did not receive %q\nbroker %v\nclient %s\ntracked %v", round, s.filter, s.token.sub.id, e, m.subsSnapshot(), cli.subsDump(), tracked)
				}
				if msg == nil {
					t.Fatalf("round %d: %s closed before %q", round, s.filter, e)
				}
				if string(msg.Payload) != e {
					t.Fatalf("round %d: %s received %q, want %q (expected %q)", round, s.filter, msg.Payload, e, expect)
				}
			}
		}
	}
	for _, s := range open {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := cli.Unsubscribe(ctx, s.token)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		for msg := range s.ch {
			t.Fatalf("%s received an extra %q", s.filter, msg.Payload)
		}
	}
}

// ctrlIdle reports whether no SUBSCRIBE or UNSUBSCRIBE awaits an answer.
func (c *Client) ctrlIdle() bool {
	c.ctrlMu.Lock()
	defer c.ctrlMu.Unlock()
	return len(c.ctrlQueue) == 0
}
