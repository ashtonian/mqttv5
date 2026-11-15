package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"io"
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

// writeLog is a connection without vectored writes, as TLS, WebSocket
// and QUIC connections are, that records each Write.
type writeLog struct {
	transport.Conn
	mu     sync.Mutex
	writes [][]byte
	err    error
	// gate, when set, holds the next Write until it is closed; entered
	// is closed once that Write is held.
	gate, entered chan struct{}
}

func (c *writeLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	gate, entered := c.gate, c.entered
	c.gate, c.entered = nil, nil
	c.mu.Unlock()
	if gate != nil {
		close(entered)
		<-gate
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return 0, c.err
	}
	c.writes = append(c.writes, bytes.Clone(p))
	if c.Conn == nil {
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *writeLog) take() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.writes
	c.writes = nil
	return w
}

func filled(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }

func TestPacketWriterGathers(t *testing.T) {
	small, other := filled(100, 'a'), filled(200, 'b')
	half := filled(gatherLimit/2+1, 'c')
	big := filled(gatherLimit, 'd')
	tests := []struct {
		name string
		bufs [][]byte
		want [][]byte
	}{
		{"one buffer", [][]byte{small}, [][]byte{small}},
		{"a header and its body", [][]byte{small[:2], other}, [][]byte{slices.Concat(small[:2], other)}},
		{"a batch", [][]byte{small, other, small}, [][]byte{slices.Concat(small, other, small)}},
		{"past the limit", [][]byte{half, half}, [][]byte{half, half}},
		{"a large buffer on its own", [][]byte{small, big, other}, [][]byte{small, big, other}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &writeLog{}
			w := newPacketWriter(conn)
			bufs := net.Buffers(slices.Clone(tt.bufs))
			n, err := w.WriteBuffers(&bufs)
			if err != nil {
				t.Fatal(err)
			}
			if want := len(slices.Concat(tt.bufs...)); n != int64(want) {
				t.Errorf("wrote %d bytes, want %d", n, want)
			}
			if len(bufs) != 0 {
				t.Errorf("%d buffers left unconsumed", len(bufs))
			}
			if got := conn.take(); !slices.EqualFunc(got, tt.want, bytes.Equal) {
				t.Errorf("writes of %v bytes, want %v", lens(got), lens(tt.want))
			}
		})
	}
}

func lens(bufs [][]byte) []int {
	n := make([]int, len(bufs))
	for i, b := range bufs {
		n[i] = len(b)
	}
	return n
}

// An encoder's buffers reach the connection as one Write; one that
// fails writes nothing, and leaves nothing behind for the next packet.
func TestPacketWriterWritePacket(t *testing.T) {
	conn := &writeLog{}
	w := newPacketWriter(conn)
	failed := errors.New("encode failed")
	if _, err := w.WritePacket(func(w io.Writer) (int64, error) {
		_, _ = w.Write([]byte("half"))
		return 4, failed
	}); !errors.Is(err, failed) {
		t.Fatalf("WritePacket = %v, want %v", err, failed)
	}
	if _, err := w.WritePacket(wire.WritePingreq); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	_, _ = wire.WritePingreq(&want)
	if got := conn.take(); len(got) != 1 || !bytes.Equal(got[0], want.Bytes()) {
		t.Fatalf("writes %q, want one PINGREQ %q", got, want.Bytes())
	}

	if _, err := w.WritePacket(func(w io.Writer) (int64, error) {
		return wire.WriteDisconnect(w, wire.DisconnectOpts{ReasonCode: wire.ReasonServerBusy, ReasonString: "draining"})
	}); err != nil {
		t.Fatal(err)
	}
	if got := conn.take(); len(got) != 1 {
		t.Fatalf("DISCONNECT in %d writes, want 1", len(got))
	}
}

func TestPacketWriterReportsWriteErrors(t *testing.T) {
	broken := errors.New("broken pipe")
	conn := &writeLog{err: broken}
	w := newPacketWriter(conn)
	bufs := net.Buffers{[]byte("ab"), []byte("cd")}
	if _, err := w.WriteBuffers(&bufs); !errors.Is(err, broken) {
		t.Fatalf("WriteBuffers = %v, want %v", err, broken)
	}
	if _, err := w.WritePacket(wire.WritePingreq); !errors.Is(err, broken) {
		t.Fatalf("WritePacket = %v, want %v", err, broken)
	}
}

// Over a connection without vectored writes, the CONNECT reaches it in
// one Write, and packets the writer finds queued go out together.
func TestPacketsGatheredWithoutVectoredWrites(t *testing.T) {
	b := testbroker.New(t)
	var conn *writeLog
	dial := func(ctx context.Context, u *url.URL) (transport.Conn, error) {
		nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return nil, err
		}
		conn = &writeLog{Conn: nc}
		return conn, nil
	}
	cli := tbClient(t, b, WithDialFunc(dial), WithWriteBatch(8))
	if got := conn.take(); len(got) != 1 || wire.PacketType(got[0][0]>>4) != wire.CONNECT {
		t.Fatalf("CONNECT in %d writes, want 1", len(got))
	}

	// The first publish holds the writer in its Write while the rest
	// queue behind it.
	gate, entered := make(chan struct{}), make(chan struct{})
	conn.mu.Lock()
	conn.gate, conn.entered = gate, entered
	conn.mu.Unlock()
	const n = 5
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	publish := func() {
		if err := cli.Publish(ctx, PublishOptions{Topic: "t", Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	publish()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("the first publish was not written")
	}
	cs := cli.cur.Load()
	for range n - 1 {
		publish()
	}
	for cs.queued.Load() != n {
		if ctx.Err() != nil {
			t.Fatalf("%d writes queued, want %d", cs.queued.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)
	waitLog(t, b.Conn(0, time.Second), wire.PUBLISH, n)
	if got := lens(conn.take()); len(got) != 2 {
		t.Fatalf("%d publishes in writes of %v bytes, want the first alone and the rest in one", n, got)
	}
}
