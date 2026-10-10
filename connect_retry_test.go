// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

// With WithRetryInitialConnect a client starts while the
// broker refuses it and connects once the broker accepts.
func TestRetryInitialConnect(t *testing.T) {
	refuse := func(c *testbroker.Conn) { c.Close() }
	b := testbroker.New(t, refuse, refuse, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	})
	var failures int
	cli, err := New(WithBroker(b.URL()), WithClientID("retry"), WithLogger(quietLogger()),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)), WithRetryInitialConnect(),
		WithOnConnectError(func(error) { failures++ }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect with the broker refusing: %v", err)
	}
	t.Cleanup(func() { _ = cli.Disconnect(context.Background()) })
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatalf("AwaitConnection: %v", err)
	}
	if failures != 2 {
		t.Fatalf("%d connect errors reported, want 2", failures)
	}
	if _, _, err := cli.Subscribe(ctx, []TopicFilter{{Topic: "up"}}); err != nil {
		t.Fatalf("Subscribe once connected: %v", err)
	}
}

// Without the option Connect fails fast, as before.
func TestInitialConnectFailsFast(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) { c.Close() })
	cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded against a refusing broker")
	}
	if cli.Connected() {
		t.Fatal("client reports connected")
	}
}

// AwaitConnection ends with ErrClosed when the client is disconnected
// while it waits, and with ctx.
func TestAwaitConnectionEnds(t *testing.T) {
	b := testbroker.New(t)
	b.SetFallback(func(c *testbroker.Conn) { c.Close() })
	cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()), WithRetryInitialConnect(),
		WithReconnectBackoff(ConstantBackoff(10*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cli.AwaitConnection(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AwaitConnection = %v, want the deadline", err)
	}
	done := make(chan error, 1)
	go func() { done <- cli.AwaitConnection(context.Background()) }()
	if err := cli.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("AwaitConnection = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitConnection still waiting after Disconnect")
	}
}
