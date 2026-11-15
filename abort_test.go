package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// seamConn is a TCP connection that implements the optional transport
// interfaces and records, in order, how the client ended it.
type seamConn struct {
	transport.Conn
	mu     sync.Mutex
	events []string
	ended  chan struct{}
	once   sync.Once
}

func (c *seamConn) record(event string, ends bool) {
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	if ends {
		c.once.Do(func() { close(c.ended) })
	}
}

func (c *seamConn) Abort(reason byte) error {
	c.record(event("abort", wire.ReasonCode(reason)), true)
	return c.Conn.Close()
}

func (c *seamConn) ObserveDisconnect(reason byte) {
	c.record(event("observe", wire.ReasonCode(reason)), false)
}

func (c *seamConn) Close() error {
	c.record("close", true)
	return c.Conn.Close()
}

func event(name string, reason wire.ReasonCode) string {
	return fmt.Sprintf("%s %#x", name, byte(reason))
}

// endedWith waits for the client to end c and returns what it called.
func (c *seamConn) endedWith(t *testing.T) []string {
	t.Helper()
	select {
	case <-c.ended:
	case <-time.After(3 * time.Second):
		t.Fatal("the client never ended the connection")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

// seamDial dials the broker over TCP and hands each connection, wrapped
// in a seamConn, to conns.
func seamDial(conns chan<- *seamConn) Option {
	return WithDialFunc(func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		var d net.Dialer
		nc, err := d.DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return nil, err
		}
		c := &seamConn{Conn: nc, ended: make(chan struct{})}
		select {
		case conns <- c:
		default:
		}
		return c, nil
	})
}

// A protocol error in the broker's packets ends the connection through
// Abort with the DISCONNECT's reason code, after that DISCONNECT, so a
// transport like QUIC can carry the reason in its own close.
func TestProtocolViolationAborts(t *testing.T) {
	for _, tt := range []struct {
		name   string
		packet []byte
		reason wire.ReasonCode
	}{
		{"malformed packet", []byte{0x30, 4, 0, 1, 0xff, 0}, wire.ReasonMalformedPacket}, // invalid UTF-8 topic
		{"protocol error", []byte{0xc0, 0}, wire.ReasonProtocolError},                    // PINGREQ from a broker
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.AcceptConnect(wire.ConnackOpts{})
				_ = c.Raw(tt.packet)
				if p := c.Expect(wire.DISCONNECT, 0); p.Reason != tt.reason {
					c.T.Errorf("DISCONNECT reason %v, want %v", p.Reason, tt.reason)
				}
			})
			conns := make(chan *seamConn, 1)
			tbClient(t, b, seamDial(conns))
			if got, want := (<-conns).endedWith(t), []string{event("abort", tt.reason)}; !slices.Equal(got, want) {
				t.Fatalf("the client ended the connection with %v, want %v", got, want)
			}
		})
	}
}

// A protocol error in the CONNACK aborts the same way.
func TestHandshakeViolationAborts(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{SessionPresent: true})
	})
	conns := make(chan *seamConn, 1)
	cli, err := New(WithBroker(b.URL()), WithLogger(quietLogger()), seamDial(conns))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var perr *ProtocolError
	if err := cli.Connect(ctx); !errors.As(err, &perr) || perr.Reason != wire.ReasonProtocolError {
		t.Fatalf("Connect = %v, want a Protocol Error", err)
	}
	if got, want := (<-conns).endedWith(t), []string{event("abort", wire.ReasonProtocolError)}; !slices.Equal(got, want) {
		t.Fatalf("the client ended the connection with %v, want %v", got, want)
	}
}

// The broker's DISCONNECT reaches the transport before the client closes
// it, so the transport can act on the reason (QUIC waits before
// reconnecting after Session Taken Over).
func TestBrokerDisconnectIsObserved(t *testing.T) {
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		_ = c.Disconnect(wire.DisconnectOpts{ReasonCode: wire.ReasonSessionTakenOver})
	}, func(c *testbroker.Conn) {
		c.AcceptConnect(wire.ConnackOpts{})
		c.Hold(0)
	})
	conns := make(chan *seamConn, 1)
	tbClient(t, b, seamDial(conns))
	want := []string{event("observe", wire.ReasonSessionTakenOver), "close"}
	if got := (<-conns).endedWith(t); !slices.Equal(got, want) {
		t.Fatalf("the client ended the connection with %v, want %v", got, want)
	}
}
