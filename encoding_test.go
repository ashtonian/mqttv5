// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// testJSON is an inline JSON Encoding: the shipping one lives in the
// codec/json submodule, which the core's tests do not import.
type testJSON struct{}

func (testJSON) ContentType() string             { return "application/json" }
func (testJSON) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (testJSON) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// testText encodes strings as their bytes, to tell encodings apart.
type testText struct{}

func (testText) ContentType() string { return "text/plain" }

func (testText) Marshal(v any) ([]byte, error) {
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("testText: %T is not a string", v)
	}
	return []byte(s), nil
}

func (testText) Unmarshal(b []byte, v any) error {
	p, ok := v.(*string)
	if !ok {
		return fmt.Errorf("testText: %T is not a *string", v)
	}
	*p = string(b)
	return nil
}

type sensorReading struct {
	DeviceID string  `json:"device_id"`
	Temp     float64 `json:"temp"`
}

type deviceCommand struct {
	Op   string `json:"op"`
	Args []int  `json:"args"`
}

func recvTyped[T any](t *testing.T, ch <-chan *TypedMessage[T]) *TypedMessage[T] {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("typed channel closed")
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("no typed message")
		return nil
	}
}

// One client decodes each subscription into its own type; no wrapper or
// codec per type.
func TestSubscribeAsTwoPayloadTypes(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "sensors/a1/temp", Payload: []byte(`{"device_id":"a1","temp":22.5}`)})
		c.Publish(wire.PublishOpts{Topic: "devices/a1/cmd", Payload: []byte(`{"op":"reboot","args":[3]}`)})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithEncoding(testJSON{}))
	ctx := context.Background()
	readings, _, err := cli.SubscribeAs[sensorReading](ctx, []TopicFilter{{Topic: "sensors/+/temp"}})
	if err != nil {
		t.Fatal(err)
	}
	cmds, _, err := cli.SubscribeAs[deviceCommand](ctx, []TopicFilter{{Topic: "devices/+/cmd"}})
	if err != nil {
		t.Fatal(err)
	}
	if r := recvTyped(t, readings); r.Value != (sensorReading{"a1", 22.5}) || r.Topic != "sensors/a1/temp" {
		t.Errorf("reading = %+v on %q", r.Value, r.Topic)
	}
	if c := recvTyped(t, cmds); c.Value.Op != "reboot" || len(c.Value.Args) != 1 || c.Value.Args[0] != 3 {
		t.Errorf("command = %+v", c.Value)
	}
}

// PublishValue encodes with the client's Encoding and declares its
// Content Type, unless the caller set one.
func TestPublishValueContentType(t *testing.T) {
	b := testbroker.New(t)
	cli := tbClient(t, b, WithEncoding(testJSON{}))
	ctx := context.Background()
	if err := cli.PublishValue(ctx, PublishOptions{Topic: "a", QoS: 1}, sensorReading{"a1", 22.5}); err != nil {
		t.Fatal(err)
	}
	if err := cli.PublishValue(ctx, PublishOptions{Topic: "b", QoS: 1, ContentType: "application/vnd.reading+json"}, sensorReading{"b2", 19}); err != nil {
		t.Fatal(err)
	}
	want := map[string][2]string{
		"a": {`{"device_id":"a1","temp":22.5}`, "application/json"},
		"b": {`{"device_id":"b2","temp":19}`, "application/vnd.reading+json"},
	}
	pubs := publishesSeen(b)
	if len(pubs) != 2 {
		t.Fatalf("broker saw %d PUBLISHes, want 2", len(pubs))
	}
	for _, p := range pubs {
		ct, _ := p.Properties().String(wire.PropContentType)
		if w := want[p.Topic]; string(p.Payload) != w[0] || ct != w[1] {
			t.Errorf("PUBLISH %s: payload %s, content type %q; want %s, %q", p.Topic, p.Payload, ct, w[0], w[1])
		}
	}
}

// publishesSeen returns the PUBLISH packets the broker's first
// connection received, read from its log: the auto-responder consumes
// them as it acks.
func publishesSeen(b *testbroker.Broker) []testbroker.Packet {
	var out []testbroker.Packet
	for _, p := range b.Conn(0, time.Second).Log() {
		if p.Type == wire.PUBLISH {
			out = append(out, p)
		}
	}
	return out
}

// A payload the encoding rejects is counted, reported to the hook and
// acknowledged, and the subscription carries on.
func TestSubscribeAsDecodeFailure(t *testing.T) {
	acked := make(chan uint16, 1)
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "s", QoS: 1, PacketID: 7, Payload: []byte(`not json`)})
		acked <- c.Expect(wire.PUBACK, 0).PacketID
		c.Publish(wire.PublishOpts{Topic: "s", Payload: []byte(`{"device_id":"ok"}`)})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithEncoding(testJSON{}), WithStats())
	var mu sync.Mutex
	var failed []string
	ch, _, err := cli.SubscribeAs[sensorReading](context.Background(), []TopicFilter{{Topic: "s", QoS: 1}},
		SubOnDecodeError(func(m *Message, err error) {
			mu.Lock()
			failed = append(failed, string(m.Payload)+": "+err.Error())
			mu.Unlock()
		}))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-acked:
		if id != 7 {
			t.Errorf("PUBACK for %d, want 7", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the undecodable message was never acknowledged")
	}
	if m := recvTyped(t, ch); m.Value.DeviceID != "ok" {
		t.Errorf("delivered %+v, want the decodable message", m.Value)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(failed) != 1 || !strings.HasPrefix(failed[0], "not json: ") {
		t.Errorf("decode errors reported = %q, want one for the bad payload", failed)
	}
	if n := cli.Stats().DecodeFailures; n != 1 {
		t.Errorf("Stats().DecodeFailures = %d, want 1", n)
	}
}

// SubEncoding decodes one subscription with an encoding other than the
// client's.
func TestSubEncodingOverridesTheClients(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "log", Payload: []byte(`plain words`)})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithEncoding(testJSON{}))
	ch, _, err := cli.SubscribeAs[string](context.Background(), []TopicFilter{{Topic: "log"}}, SubEncoding(testText{}))
	if err != nil {
		t.Fatal(err)
	}
	if m := recvTyped(t, ch); m.Value != "plain words" {
		t.Errorf("value %q", m.Value)
	}
}

// Without an Encoding the typed calls fail at once, before anything is
// sent.
func TestTypedCallsNeedAnEncoding(t *testing.T) {
	b := testbroker.New(t)
	cli := tbClient(t, b)
	ctx := context.Background()
	if err := cli.PublishValue(ctx, PublishOptions{Topic: "t"}, 1); !errors.Is(err, ErrNoEncoding) {
		t.Errorf("PublishValue = %v, want ErrNoEncoding", err)
	}
	if _, _, err := cli.SubscribeAs[int](ctx, []TopicFilter{{Topic: "t"}}); !errors.Is(err, ErrNoEncoding) {
		t.Errorf("SubscribeAs = %v, want ErrNoEncoding", err)
	}
	if _, _, err := cli.SubscribeQueueAs[int](ctx, []TopicFilter{{Topic: "t"}}); !errors.Is(err, ErrNoEncoding) {
		t.Errorf("SubscribeQueueAs = %v, want ErrNoEncoding", err)
	}
	if _, err := cli.SubscribeCallbackAs(ctx, []TopicFilter{{Topic: "t"}}, func(*TypedMessage[int]) {}); !errors.Is(err, ErrNoEncoding) {
		t.Errorf("SubscribeCallbackAs = %v, want ErrNoEncoding", err)
	}
	b.Conn(0, time.Second).ExpectNone(wire.SUBSCRIBE, 100*time.Millisecond)
}

// The queue and callback variants decode as the channel one does.
func TestSubscribeQueueAndCallbackAs(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		c.ServeSubscribe(-1)
		c.Publish(wire.PublishOpts{Topic: "q", Payload: []byte(`{"device_id":"q1"}`)})
		c.Publish(wire.PublishOpts{Topic: "cb", Payload: []byte(`{"op":"ping"}`)})
		c.ServeAuto()
	})
	cli := tbClient(t, b, WithEncoding(testJSON{}))
	ctx := context.Background()
	q, _, err := cli.SubscribeQueueAs[sensorReading](ctx, []TopicFilter{{Topic: "q"}})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan deviceCommand, 1)
	if _, err := cli.SubscribeCallbackAs(ctx, []TopicFilter{{Topic: "cb"}}, func(m *TypedMessage[deviceCommand]) {
		got <- m.Value
	}); err != nil {
		t.Fatal(err)
	}
	dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	m, ok := q.Dequeue(dctx)
	if !ok || m.Value.DeviceID != "q1" {
		t.Errorf("queue gave %+v, %v", m, ok)
	}
	select {
	case c := <-got:
		if c.Op != "ping" {
			t.Errorf("callback got %+v", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback never ran")
	}
}

// Each group member encodes and decodes with its own Encoding, so a group
// can bridge brokers whose payloads differ.
func TestGroupTypedPerMemberEncoding(t *testing.T) {
	serve := func(payload string) (*testbroker.Broker, chan testbroker.Packet) {
		pubs := make(chan testbroker.Packet, 1)
		b := testbroker.New(t, func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			c.Publish(wire.PublishOpts{Topic: "in", Payload: []byte(payload)})
			c.ServeAutoThen(func(p testbroker.Packet) error {
				if p.Type == wire.PUBLISH {
					pubs <- p
				}
				return nil
			})
		})
		return b, pubs
	}
	jb, jsonPubs := serve(`"from json"`)
	tb, textPubs := serve(`from text`)
	g, err := NewClientGroup([]GroupMember{
		{Name: "json", Brokers: []string{jb.URL()}, Opts: []Option{WithEncoding(testJSON{})}},
		{Name: "text", Brokers: []string{tb.URL()}, Opts: []Option{WithEncoding(testText{})}},
	}, WithGroupSharedOpts(WithClientID("typed-group"), WithLogger(quietLogger())))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := g.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(ctx)

	ch, _, err := g.SubscribeAs[string](ctx, []TopicFilter{{Topic: "in"}})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{recvTyped(t, ch).Value: true, recvTyped(t, ch).Value: true}
	if !seen["from json"] || !seen["from text"] {
		t.Errorf("decoded %v, want one value from each member", seen)
	}

	if _, err := g.PublishValue(ctx, PublishOptions{Topic: "out", QoS: 1}, "hello"); err != nil {
		t.Fatal(err)
	}
	for name, pubs := range map[string]chan testbroker.Packet{"json": jsonPubs, "text": textPubs} {
		select {
		case p := <-pubs:
			ct, _ := p.Properties().String(wire.PropContentType)
			want := map[string][2]string{"json": {`"hello"`, "application/json"}, "text": {"hello", "text/plain"}}[name]
			if string(p.Payload) != want[0] || ct != want[1] {
				t.Errorf("%s published %s as %q, want %s as %q", name, p.Payload, ct, want[0], want[1])
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s never published", name)
		}
	}
}

// QueuePublisher.PublishValue encodes before the message is queued.
func TestQueuePublisherPublishValue(t *testing.T) {
	b := testbroker.New(t)
	cli := tbClient(t, b, WithEncoding(testJSON{}))
	qp, err := NewQueuePublisher(cli, NewMemoryPublisherQueue())
	if err != nil {
		t.Fatal(err)
	}
	defer qp.Close(context.Background())
	if err := qp.PublishValue(context.Background(), PublishOptions{Topic: "q", QoS: 1}, sensorReading{"q1", 1}); err != nil {
		t.Fatal(err)
	}
	var pubs []testbroker.Packet
	deadline := time.Now().Add(2 * time.Second)
	for len(pubs) == 0 && time.Now().Before(deadline) {
		pubs = publishesSeen(b)
		time.Sleep(10 * time.Millisecond)
	}
	if len(pubs) != 1 {
		t.Fatalf("broker saw %d PUBLISHes, want 1", len(pubs))
	}
	ct, _ := pubs[0].Properties().String(wire.PropContentType)
	if string(pubs[0].Payload) != `{"device_id":"q1","temp":1}` || ct != "application/json" {
		t.Errorf("queued PUBLISH %s as %q", pubs[0].Payload, ct)
	}
}

// A group publishes a value only when every member can encode it, so
// ErrNoEncoding means nothing was sent.
func TestGroupPublishValueWithoutAnEncodingSendsNothing(t *testing.T) {
	b := testbroker.New(t)
	g, err := NewClientGroup([]GroupMember{
		{Name: "encoded", Brokers: []string{b.URL()}, Opts: []Option{WithEncoding(testJSON{})}},
		{Name: "missing", Brokers: []string{b.URL()}},
	}, WithGroupSharedOpts(WithLogger(quietLogger())))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := g.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer g.Disconnect(context.Background())
	if _, err := g.PublishValue(ctx, PublishOptions{Topic: "t", QoS: 1}, 42); !errors.Is(err, ErrNoEncoding) {
		t.Fatalf("PublishValue = %v, want ErrNoEncoding", err)
	}
	for i := range b.Accepted() {
		for _, p := range b.Conn(i, time.Second).Log() {
			if p.Type == wire.PUBLISH {
				t.Fatalf("ErrNoEncoding returned, yet %q was published", p.Payload)
			}
		}
	}
}
