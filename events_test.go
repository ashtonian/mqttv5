// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/session"
	"github.com/ashtonian/mqttv5/wire"
)

// Callbacks run one at a time in the order posted, and flush waits for
// those posted before it unless stop closes first.
func TestEventsRunInOrder(t *testing.T) {
	var q events
	var got []int
	held := make(chan struct{})
	q.post(func() { <-held })
	for i := range 100 {
		q.post(func() { got = append(got, i) })
	}
	stop := make(chan struct{})
	close(stop)
	if q.flush(stop) {
		t.Fatal("flush reported the queue flushed while a callback was held")
	}
	close(held)
	if !q.flush(nil) {
		t.Fatal("flush did not complete")
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("callback %d ran as %d", v, i)
		}
	}
	if len(got) != 100 {
		t.Fatalf("%d of 100 callbacks ran", len(got))
	}
}

// Every lifecycle callback may call Disconnect: it returns, and the
// client ends disconnected.
func TestDisconnectFromALifecycleCallback(t *testing.T) {
	refuse := func(rc wire.ReasonCode, ref string) testbroker.Script {
		return func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{ReasonCode: rc, ServerReference: ref})
		}
	}
	held := func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeAuto()
	}
	drop := func(_ *testing.T, _ *Client, b *testbroker.Broker) { b.Conn(0, time.Second).Close() }
	for _, tc := range []struct {
		name     string
		first    testbroker.Script // nil: the broker's default
		fallback testbroker.Script
		opts     func(stop func()) []Option
		trigger  func(*testing.T, *Client, *testbroker.Broker)
	}{
		{name: "OnConnectionUp", opts: func(stop func()) []Option {
			return []Option{WithOnConnectionUp(func(ConnackInfo) { stop() })}
		}},
		{name: "OnConnectionUp after a reconnect", first: held, trigger: drop, opts: func(stop func()) []Option {
			var ups int
			return []Option{WithOnConnectionUp(func(ConnackInfo) {
				if ups++; ups == 2 {
					stop()
				}
			})}
		}},
		{name: "OnConnectionDown", first: held, trigger: drop, opts: func(stop func()) []Option {
			return []Option{WithOnConnectionDown(func() bool { stop(); return true })}
		}},
		{name: "OnServerDisconnect", first: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			_ = c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonServerShuttingDown})
		}, opts: func(stop func()) []Option {
			return []Option{WithOnServerDisconnect(func(DisconnectInfo) { stop() })}
		}},
		{name: "OnReconnectAttempt", first: held, trigger: drop, opts: func(stop func()) []Option {
			return []Option{WithOnReconnectAttempt(func(int, string) { stop() })}
		}},
		{name: "OnConnectError", fallback: refuse(wire.ReasonNotAuthorized, ""), opts: func(stop func()) []Option {
			return []Option{WithRetryInitialConnect(), WithOnConnectError(func(error) { stop() })}
		}},
		{name: "OnServerRedirect", fallback: refuse(wire.ReasonServerMoved, "127.0.0.1:1"), opts: func(stop func()) []Option {
			return []Option{WithRetryInitialConnect(), WithOnServerRedirect(func(ServerRedirect) { stop() })}
		}},
		{name: "OnResubscribeError", first: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			c.ServeSubscribe(-1)
			<-c.Gone()
		}, fallback: func(c *testbroker.Conn) {
			c.AcceptConnect(wire.ConnackOpts{})
			p := c.Expect(wire.SUBSCRIBE, 0)
			_ = c.Suback(p.PacketID, wire.ReasonNotAuthorized)
			c.ServeAuto()
		}, trigger: func(t *testing.T, cli *Client, b *testbroker.Broker) {
			if _, _, err := cli.Subscribe(context.Background(), []TopicFilter{{Topic: "acl/#"}}); err != nil {
				t.Fatal(err)
			}
			b.Conn(0, time.Second).Close()
		}, opts: func(stop func()) []Option {
			return []Option{WithOnResubscribeError(func(SubscriptionToken, error) { stop() })}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scripts []testbroker.Script
			if tc.first != nil {
				scripts = append(scripts, tc.first)
			}
			b := testbroker.New(t, scripts...)
			if tc.fallback != nil {
				b.SetFallback(tc.fallback)
			}
			var cli *Client
			var once sync.Once
			stopped := make(chan error, 1)
			stop := func() {
				once.Do(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					stopped <- cli.Disconnect(ctx)
				})
			}
			opts := append([]Option{WithBroker(b.URL()), WithClientID("callback-" + t.Name()), WithoutKeepAlive(),
				WithLogger(quietLogger()), WithReconnectBackoff(ConstantBackoff(10 * time.Millisecond))}, tc.opts(stop)...)
			var err error
			cli, err = New(opts...)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := cli.Connect(ctx); err != nil && !errors.Is(err, ErrClosed) {
				t.Fatalf("Connect: %v", err)
			}
			if tc.trigger != nil {
				tc.trigger(t, cli, b)
			}
			select {
			case err := <-stopped:
				if err != nil {
					t.Fatalf("Disconnect: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("Disconnect called from the callback did not return")
			}
			if cli.Connected() {
				t.Fatal("connected after Disconnect")
			}
		})
	}
}

// OnStoreFailure may start the client again from what the store holds.
func TestConnectFromOnStoreFailure(t *testing.T) {
	st := &phaseFailStore{MemoryStore: session.NewMemoryStore(), phase: session.AwaitPubcomp}
	st.fail.Store(true)
	b := testbroker.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var cli *Client
	restarted := make(chan error, 1)
	cli = tbClient(t, b, WithStore(st), WithOnStoreFailure(func(error) {
		st.fail.Store(false)
		restarted <- cli.Connect(ctx)
	}))
	if err := cli.Publish(ctx, PublishOptions{Topic: "s", QoS: 2, Payload: []byte("x")}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Publish returned %v, want the store failure", err)
	}
	select {
	case err := <-restarted:
		if err != nil {
			t.Fatalf("Connect from OnStoreFailure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Connect from OnStoreFailure did not return")
	}
	if err := cli.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
}

// recordingLog keeps the messages logged at Warn and above.
type recordingLog struct {
	slog.Handler
	mu   sync.Mutex
	msgs []string
}

func (h *recordingLog) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *recordingLog) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *recordingLog) logged(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if strings.HasPrefix(m, prefix) {
			return true
		}
	}
	return false
}

// A teardown held up by a SubscribeCallback handler says so in the log,
// and completes once the handler returns.
func TestTeardownReportsAHandlerThatDoesNotReturn(t *testing.T) {
	clk := clock.NewFake(time.Now())
	log := &recordingLog{Handler: slog.Default().Handler()}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.ServeSubscribe(-1)
		_ = c.Publish(wire.PublishOpts{Topic: "t", Payload: []byte("held")})
		c.Hold(0)
	})
	cli, err := New(WithBroker(b.URL()), WithClientID("stall"), WithoutKeepAlive(), WithLogger(slog.New(log)), withClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	entered, held := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(held) })
	t.Cleanup(release)
	if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "t"}}, func(*Message) {
		close(entered)
		<-held
	}); err != nil {
		t.Fatal(err)
	}
	<-entered
	timers := clk.Pending()
	stopped := make(chan error, 1)
	go func() { stopped <- cli.Disconnect(ctx) }()
	if !clk.WaitPending(timers+1, 5*time.Second) {
		t.Fatal("the teardown did not start waiting for the handler")
	}
	clk.Advance(joinStallWarning)
	const warning = "mqttv5: disconnect is waiting for a SubscribeCallback handler"
	for deadline := time.Now().Add(5 * time.Second); !log.logged(warning); {
		if time.Now().After(deadline) {
			t.Fatal("no warning about the handler holding the teardown")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-stopped:
		t.Fatal("Disconnect returned while the handler was running")
	default:
	}
	release()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}
