package quic

import (
	"bytes"
	"io"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"

	quicgo "github.com/quic-go/quic-go"
)

// Only the first MQTT packet, and only when it is a CONNECT, goes out as
// early data, however the writes split it.
func TestEarlyAllowance(t *testing.T) {
	connect := connectPacket(10)
	long := connectPacket(300) // two-byte Remaining Length
	tests := []struct {
		name   string
		writes [][]byte
		want   []int
	}{
		{name: "CONNECT then more in one write", writes: [][]byte{append(bytes.Clone(connect), 0x82, 0x01)}, want: []int{len(connect)}},
		{name: "CONNECT, then another packet", writes: [][]byte{connect, {0x82, 0x01}}, want: []int{len(connect), 0}},
		{name: "fixed header split", writes: [][]byte{{0x10}, connect[1:]}, want: []int{1, len(connect) - 1}},
		{name: "two-byte Remaining Length split", writes: [][]byte{long[:2], long[2:], {0xC0, 0x00}}, want: []int{2, len(long) - 2, 0}},
		{name: "not a CONNECT", writes: [][]byte{{0x82, 0x01, 0x00}}, want: []int{0}},
		{name: "malformed Remaining Length", writes: [][]byte{{0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}}, want: []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &earlyData{need: -1}
			for i, p := range tt.writes {
				n := e.allow(p)
				if n != tt.want[i] {
					t.Fatalf("write %d: allow = %d, want %d", i, n, tt.want[i])
				}
				e.sent = append(e.sent, p[:n]...)
			}
		})
	}
}

// primeTicket makes a first connection with d to u, which srv serves,
// so that d holds a session ticket allowing 0-RTT.
func primeTicket(t *testing.T, d *Dialer, srv *testServer, u *url.URL) {
	t.Helper()
	conn, err := d.Dial(testContext(t), u)
	if err != nil {
		t.Fatal(err)
	}
	write(t, conn, connectPacket(1))
	sc := srv.next(t)
	readFull(t, sc.stream, connectPacket(1))
	// A round trip lets the client take in the session ticket, which the
	// server sends after the handshake.
	write(t, sc.stream, []byte{0x20})
	readFull(t, conn, []byte{0x20})
	conn.Close()
	_ = sc.stream.Close()
}

func earlyDialer(srv *testServer) *Dialer {
	opts := srv.opts()
	opts.Allow0RTT = true
	return NewDialer(opts)
}

// With a session ticket, the CONNECT goes out as 0-RTT data before the
// handshake completes; what follows it waits for the handshake.
func Test0RTTSendsOnlyTheConnect(t *testing.T) {
	srv := startServer(t)
	rel := startRelay(t, srv.ln.Addr())
	d := earlyDialer(srv)
	primeTicket(t, d, srv, rel.url())

	// Hold the server's handshake replies back: the client can send
	// 0-RTT data but not complete the handshake.
	rel.hold()
	conn, err := d.Dial(testContext(t), rel.url())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if !isEarly(conn.(*Conn).qc) {
		t.Fatal("dial waited for the handshake despite a 0-RTT ticket")
	}
	connect := connectPacket(32)
	write(t, conn, connect)
	sc := srv.next(t)
	readFull(t, sc.stream, connect)

	subscribe := []byte{0x82, 0x01, 0x00}
	var released atomic.Bool
	wrote := make(chan error, 1)
	go func() {
		_, err := conn.Write(subscribe)
		wrote <- err
	}()
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, len(subscribe))
		_, err := io.ReadFull(sc.stream, buf)
		if err == nil && !released.Load() {
			t.Error("a packet after the CONNECT reached the server before the handshake completed")
		}
		got <- err
	}()
	// Release once the write is under way and waiting for the handshake.
	c := conn.(*Conn)
	for c.wmu.TryLock() {
		c.wmu.Unlock()
		runtime.Gosched()
	}
	select {
	case err := <-wrote:
		t.Fatalf("Write after the CONNECT returned (%v) before the handshake completed", err)
	default:
	}
	released.Store(true)
	rel.release()
	if err := <-wrote; err != nil {
		t.Fatalf("Write after the CONNECT: %v", err)
	}
	if err := <-got; err != nil {
		t.Fatalf("server read: %v", err)
	}
	if !conn.(*Conn).qc.ConnectionState().Used0RTT || !sc.qc.ConnectionState().Used0RTT {
		t.Fatal("connection did not use 0-RTT")
	}
}

// A broker that rejects the early data gets the CONNECT again, once, on
// a new stream after the handshake (draft §5.5.3).
func Test0RTTRejectedResendsTheConnect(t *testing.T) {
	var conns atomic.Int32
	srv := startServer(t, func(c *serverConfig) {
		accepting := c.quic
		rejecting := accepting.Clone()
		rejecting.Allow0RTT = false
		accepting.GetConfigForClient = func(*quicgo.ClientInfo) (*quicgo.Config, error) {
			if conns.Add(1) == 1 {
				return accepting, nil
			}
			return rejecting, nil
		}
	})
	d := earlyDialer(srv)
	primeTicket(t, d, srv, srv.url)
	conn, err := d.Dial(testContext(t), srv.url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	connect := connectPacket(32)
	write(t, conn, connect)

	reply := make(chan error, 1)
	go func() {
		sc := srv.next(t)
		buf := make([]byte, len(connect))
		if _, err := io.ReadFull(sc.stream, buf); err != nil || !bytes.Equal(buf, connect) {
			reply <- err
			return
		}
		_, err := sc.stream.Write([]byte{0x20})
		reply <- err
	}()
	readFull(t, conn, []byte{0x20})
	if err := <-reply; err != nil {
		t.Fatalf("server: %v", err)
	}
	if conn.(*Conn).qc.ConnectionState().Used0RTT {
		t.Fatal("the broker accepted the early data")
	}
}
