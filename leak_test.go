// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
)

// noLeaks records how many goroutines run now. The returned check
// fails t if more are running once exiting ones have had time to
// finish, and prints every goroutine's stack. Take the baseline before
// starting anything the test owns, and check after tearing it down.
func noLeaks(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for runtime.NumGoroutine() > before {
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				t.Fatalf("%d goroutines before, %d after teardown:\n%s", before, runtime.NumGoroutine(), buf[:n])
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// A client used every way there is — every subscription kind, every
// QoS, a publisher pool, a QueuePublisher, Typed, a reconnect, and a
// second Connect — leaves no goroutine behind once it is disconnected.
func TestLifecycleLeavesNoGoroutines(t *testing.T) {
	check := noLeaks(t)
	b := testbroker.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, err := New(WithBroker(b.URL()), WithClientID("leak"), WithLogger(quietLogger()),
		WithPublisherPool(2), WithStats(), WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	for cycle := range 2 {
		if err := cli.Connect(ctx); err != nil {
			t.Fatalf("cycle %d: Connect: %v", cycle, err)
		}
		ch, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "leak/chan", QoS: 1}})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for m := range ch {
				_ = m.Ack()
			}
		}()
		if _, _, err := cli.SubscribeQueue(ctx, []TopicFilter{{Topic: "leak/queue", QoS: 2}}); err != nil {
			t.Fatal(err)
		}
		if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "leak/cb"}}, func(*Message) {}); err != nil {
			t.Fatal(err)
		}
		typed := NewTyped(cli, testJSONCodec[string]{})
		if _, _, err := typed.Subscribe(ctx, []TopicFilter{{Topic: "leak/typed"}}); err != nil {
			t.Fatal(err)
		}
		for qos := byte(0); qos <= 2; qos++ {
			if err := cli.Publish(ctx, PublishOptions{Topic: "leak/pub", QoS: qos, Payload: []byte("x")}); err != nil {
				t.Fatalf("publish QoS %d: %v", qos, err)
			}
		}
		qp, err := NewQueuePublisher(cli, NewMemoryPublisherQueue())
		if err != nil {
			t.Fatal(err)
		}
		if err := qp.Publish(ctx, PublishOptions{Topic: "leak/queued", QoS: 1}); err != nil {
			t.Fatal(err)
		}
		if err := qp.Close(ctx); err != nil {
			t.Fatal(err)
		}
		// Drop the connections; the supervisors reconnect.
		connects := cli.Stats().Connects
		b.DropAll()
		for cli.Stats().Connects == connects {
			if ctx.Err() != nil {
				t.Fatal("no reconnect after the drop")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if err := cli.Disconnect(ctx); err != nil {
			t.Fatalf("cycle %d: Disconnect: %v", cycle, err)
		}
	}
	b.Close()
	check()
}

// A client that never reaches its broker, and one whose supervisor
// stops on OnConnectionDown, leave nothing behind either.
func TestStoppedSupervisorsLeaveNoGoroutines(t *testing.T) {
	t.Run("retrying initial connect", func(t *testing.T) {
		check := noLeaks(t)
		b := testbroker.New(t)
		b.SetFallback(func(c *testbroker.Conn) { c.Close() })
		cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()), WithRetryInitialConnect(),
			WithReconnectBackoff(ConstantBackoff(5*time.Millisecond)))
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // a few failed attempts
		if err := cli.Disconnect(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := cli.AwaitConnection(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("AwaitConnection after Disconnect: %v", err)
		}
		b.Close()
		check()
	})
	t.Run("OnConnectionDown returns false", func(t *testing.T) {
		check := noLeaks(t)
		b := testbroker.New(t)
		down := make(chan struct{})
		cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()),
			WithOnConnectionDown(func() bool { close(down); return false }))
		if err != nil {
			t.Fatal(err)
		}
		if err := cli.Connect(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "t"}}); err != nil {
			t.Fatal(err)
		}
		b.DropAll()
		<-down
		b.Close()
		check()
	})
}
