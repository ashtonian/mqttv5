package ws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"

	"github.com/ashtonian/mqttv5/internal/clock"
)

const testTimeout = 5 * time.Second

// startWS runs serve on the server side of each upgraded connection and
// returns a connected client.
func startWS(t *testing.T, serve func(conn net.Conn)) *Conn {
	t.Helper()
	return startWSWith(t, DialOpts{}, serve)
}

// startWSWith is startWS dialing with opts.
func startWSWith(t *testing.T, opts DialOpts, serve func(conn net.Conn)) *Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := upgradeMQTT(r, w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		serve(conn)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(strings.Replace(srv.URL, "http://", "ws://", 1))
	c, err := Dial(context.Background(), u, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	return c.(*Conn)
}

// readClientFrame reads one frame the client sent and unmasks it.
func readClientFrame(conn net.Conn) (gws.Header, []byte, error) {
	h, err := gws.ReadHeader(conn)
	if err != nil {
		return h, nil, err
	}
	payload := make([]byte, h.Length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return h, nil, err
	}
	if h.Masked {
		gws.Cipher(payload, h.Mask, 0)
	}
	return h, payload, nil
}

// RFC 6455 §5.5.2: a Ping between messages gets a Pong carrying its
// payload, and reading carries on.
func TestPingIsAnswered(t *testing.T) {
	pong := make(chan []byte, 1)
	c := startWS(t, func(conn net.Conn) {
		_ = wsutil.WriteServerMessage(conn, gws.OpPing, []byte("hb"))
		_ = wsutil.WriteServerBinary(conn, []byte("data"))
		h, payload, err := readClientFrame(conn)
		if err != nil || h.OpCode != gws.OpPong {
			t.Errorf("client answered the ping with %v (%v)", h.OpCode, err)
		}
		pong <- payload
		_, _, _ = readClientFrame(conn)
	})
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "data" {
		t.Fatalf("read %q, %v", got, err)
	}
	select {
	case p := <-pong:
		if string(p) != "hb" {
			t.Fatalf("pong payload %q, want %q", p, "hb")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no pong")
	}
}

// closeFrame is a Close frame the client sent: its status, or
// StatusNoStatusRcvd for none, and its reason.
type closeFrame struct {
	code   gws.StatusCode
	reason string
}

// readClientClose reads the client's next frame, which must be a Close.
func readClientClose(conn net.Conn) (closeFrame, error) {
	h, p, err := readClientFrame(conn)
	if err != nil {
		return closeFrame{}, err
	}
	if h.OpCode != gws.OpClose {
		return closeFrame{}, fmt.Errorf("client sent %v, want Close", h.OpCode)
	}
	if len(p) == 0 {
		return closeFrame{code: gws.StatusNoStatusRcvd}, nil
	}
	code, reason := gws.ParseCloseFrameData(p)
	return closeFrame{code: code, reason: reason}, nil
}

// waitClientEOF reads until the client closes the TCP connection, failing
// on a frame or a timeout.
func waitClientEOF(conn net.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(testTimeout))
	if n, err := conn.Read(make([]byte, 64)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("read %d bytes, %v; want the client to close the connection", n, err)
	}
	return nil
}

// [MQTT-6.0.0-1]: MQTT travels only in binary frames; the client closes
// with Unsupported Data.
func TestTextFrameIsAnError(t *testing.T) {
	sent := make(chan closeFrame, 1)
	c := startWS(t, func(conn net.Conn) {
		_ = wsutil.WriteServerText(conn, []byte("{}"))
		f, _ := readClientClose(conn)
		sent <- f
	})
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, ErrTextFrame) {
		t.Fatalf("Read after a text frame: %v", err)
	}
	if f := <-sent; f.code != gws.StatusUnsupportedData {
		t.Fatalf("client closed with %d, want %d", f.code, gws.StatusUnsupportedData)
	}
}

// A frame that breaks RFC 6455 gets a Close with Protocol Error.
func TestBrokerProtocolErrorIsClosed(t *testing.T) {
	sent := make(chan closeFrame, 1)
	c := startWS(t, func(conn net.Conn) {
		// A server never masks its frames (RFC 6455 §5.1).
		f := gws.MaskFrame(gws.NewBinaryFrame([]byte("x")))
		_ = gws.WriteFrame(conn, f)
		cf, _ := readClientClose(conn)
		sent <- cf
	})
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, gws.ErrProtocolMaskUnexpected) {
		t.Fatalf("Read after a masked frame: %v", err)
	}
	if f := <-sent; f.code != gws.StatusProtocolError {
		t.Fatalf("client closed with %d, want %d", f.code, gws.StatusProtocolError)
	}
}

// A Close from the broker is answered with its status and ends the
// stream: io.EOF for Normal Closure or none, a CloseError otherwise.
func TestBrokerClose(t *testing.T) {
	tests := []struct {
		name     string
		body     []byte
		wantErr  error
		wantEcho gws.StatusCode
	}{
		{name: "normal", body: gws.NewCloseFrameBody(gws.StatusNormalClosure, "bye"), wantErr: io.EOF, wantEcho: gws.StatusNormalClosure},
		{name: "no status", wantErr: io.EOF, wantEcho: gws.StatusNoStatusRcvd},
		{name: "policy violation", body: gws.NewCloseFrameBody(gws.StatusPolicyViolation, "not authorized"),
			wantErr: &CloseError{Code: 1008, Reason: "not authorized"}, wantEcho: gws.StatusPolicyViolation},
		{name: "status out of range", body: gws.NewCloseFrameBody(999, ""), wantErr: gws.ErrProtocolStatusCodeNotInUse, wantEcho: gws.StatusProtocolError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			echo := make(chan closeFrame, 1)
			c := startWS(t, func(conn net.Conn) {
				_ = wsutil.WriteServerMessage(conn, gws.OpClose, tt.body)
				f, _ := readClientClose(conn)
				echo <- f
			})
			_, err := c.Read(make([]byte, 8))
			var ce *CloseError
			if want, ok := tt.wantErr.(*CloseError); ok {
				if !errors.As(err, &ce) || *ce != *want {
					t.Fatalf("Read = %v, want %v", err, want)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Read = %v, want %v", err, tt.wantErr)
			}
			if f := <-echo; f.code != tt.wantEcho {
				t.Fatalf("client answered with %d, want %d", f.code, tt.wantEcho)
			}
		})
	}
}

// Pongs sent while Read handles pings never land inside a data frame
// another goroutine is writing.
func TestPongsKeepFramesWhole(t *testing.T) {
	const n = 500
	payload := func(i int) []byte {
		p := bytes.Repeat([]byte{byte(i)}, 1000)
		binary.BigEndian.PutUint32(p, uint32(i))
		return p
	}
	result := make(chan error, 1)
	c := startWS(t, func(conn net.Conn) {
		go func() {
			for i := range n {
				if wsutil.WriteServerMessage(conn, gws.OpPing, []byte{byte(i)}) != nil {
					return
				}
			}
		}()
		data, pongs := 0, 0
		for data < n || pongs < n {
			h, p, err := readClientFrame(conn)
			if err != nil {
				result <- fmt.Errorf("after %d data frames and %d pongs: %w", data, pongs, err)
				return
			}
			switch h.OpCode {
			case gws.OpBinary:
				if !bytes.Equal(p, payload(data)) {
					result <- fmt.Errorf("data frame %d corrupted", data)
					return
				}
				data++
			case gws.OpPong:
				pongs++
			default:
				result <- fmt.Errorf("unexpected frame %v", h.OpCode)
				return
			}
		}
		result <- nil
	})
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()
	for i := range n {
		if _, err := c.Write(payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not receive every frame")
	}
}

// Close never waits behind a write stalled on a peer that stopped
// reading: it closes the socket, which ends the write.
func TestCloseEndsAStalledWrite(t *testing.T) {
	release := make(chan struct{})
	c := startWS(t, func(net.Conn) { <-release })
	defer close(release)
	_ = c.SetDeadline(time.Time{})
	// Larger than the socket buffers of a peer that never read, so the
	// write blocks.
	packet := make([]byte, 8<<20)
	wrote := make(chan error, 1)
	go func() {
		_, err := c.Write(packet)
		wrote <- err
	}()
	// Wait until the write holds the lock, blocked on the socket. Masking
	// the frame first takes a while under the race detector.
	for deadline := time.Now().Add(30 * time.Second); c.wmu.TryLock(); time.Sleep(time.Millisecond) {
		c.wmu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the write never started")
		}
	}
	closed := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked behind the stalled write")
	}
	select {
	case err := <-wrote:
		if err == nil {
			t.Fatal("the stalled write succeeded after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stalled write did not end with the connection")
	}
}

// Close sends Normal Closure and leaves the TCP connection open until
// the broker answers, then closes it (RFC 6455 §7.1.1, §7.1.2).
func TestCloseHandshake(t *testing.T) {
	answer := make(chan struct{})
	result := make(chan error, 1)
	opts, fake := withClock(DialOpts{})
	c := startWSWith(t, opts, func(conn net.Conn) {
		result <- func() error {
			f, err := readClientClose(conn)
			if err != nil {
				return err
			}
			if f.code != gws.StatusNormalClosure {
				return fmt.Errorf("client closed with %d, want %d", f.code, gws.StatusNormalClosure)
			}
			<-answer
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				return fmt.Errorf("before the broker's Close: %v, want the connection open", err)
			}
			_ = wsutil.WriteServerMessage(conn, gws.OpClose, gws.NewCloseFrameBody(gws.StatusNormalClosure, ""))
			return waitClientEOF(conn)
		}()
	})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	close(answer)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if fake.Pending() != 0 {
		t.Fatal("the close timer outlived the handshake")
	}
}

// A broker that never answers the Close has the TCP connection closed on
// it at CloseFrameTimeout.
func TestCloseTimesOut(t *testing.T) {
	result := make(chan error, 1)
	opts, fake := withClock(DialOpts{CloseFrameTimeout: 3 * time.Second})
	c := startWSWith(t, opts, func(conn net.Conn) {
		result <- func() error {
			if _, err := readClientClose(conn); err != nil {
				return err
			}
			return waitClientEOF(conn)
		}()
	})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if !fake.WaitPending(1, testTimeout) {
		t.Fatal("no close timer armed")
	}
	fake.Advance(3 * time.Second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// After Close nothing more is sent: not data, not a Pong to the
// broker's Ping (RFC 6455 §5.5.1), and Read and Write report the
// connection closed.
func TestNothingFollowsClose(t *testing.T) {
	result := make(chan error, 1)
	opts, _ := withClock(DialOpts{})
	c := startWSWith(t, opts, func(conn net.Conn) {
		result <- func() error {
			if _, err := readClientClose(conn); err != nil {
				return err
			}
			_ = wsutil.WriteServerMessage(conn, gws.OpPing, []byte("hb"))
			_ = wsutil.WriteServerMessage(conn, gws.OpClose, gws.NewCloseFrameBody(gws.StatusNormalClosure, ""))
			return waitClientEOF(conn)
		}()
	})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte{0xC0, 0}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Close = %v, want net.ErrClosed", err)
	}
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read after Close = %v, want net.ErrClosed", err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("second Close = %v, want net.ErrClosed", err)
	}
}

// Abort closes with Protocol Error naming the MQTT reason code, and
// closes the TCP connection without waiting for the broker.
func TestAbortCarriesReason(t *testing.T) {
	result := make(chan error, 1)
	c := startWS(t, func(conn net.Conn) {
		result <- func() error {
			f, err := readClientClose(conn)
			if err != nil {
				return err
			}
			if f.code != gws.StatusProtocolError || !strings.Contains(f.reason, "0x82") {
				return fmt.Errorf("client closed with %d %q, want %d naming 0x82", f.code, f.reason, gws.StatusProtocolError)
			}
			return waitClientEOF(conn)
		}()
	})
	if err := c.Abort(0x82); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

// A broker that selects no subprotocol is not an MQTT broker
// [MQTT-6.0.0-4].
func TestNoSubprotocol(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err == nil {
			_ = conn.Close()
		}
	}))
	defer srv.Close()
	u, _ := url.Parse(strings.Replace(srv.URL, "http://", "ws://", 1))
	if _, err := Dial(context.Background(), u, DialOpts{}); !errors.Is(err, ErrNoSubprotocol) {
		t.Fatalf("Dial = %v, want ErrNoSubprotocol", err)
	}
}

// Reading a stream of small frames takes far fewer reads from the socket
// than frames: frame headers come out of a buffer, not a read each.
func TestReadsAreBuffered(t *testing.T) {
	const frames = 1000
	var stream []byte
	for range frames {
		b, _ := gws.CompileFrame(gws.NewBinaryFrame([]byte("0123456789abcdef0123456789")))
		stream = append(stream, b...)
	}
	var reads atomic.Int64
	opts := DialOpts{NetDial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, network, addr)
		return countingConn{Conn: conn, reads: &reads}, err
	}}
	c := startWSWith(t, opts, func(conn net.Conn) {
		_, _ = conn.Write(stream)
		_, _, _ = readClientFrame(conn)
	})
	before := reads.Load()
	want := frames * 26
	if _, err := io.ReadFull(c, make([]byte, want)); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load() - before; n > frames/10 {
		t.Fatalf("%d frames took %d socket reads", frames, n)
	}
}

type countingConn struct {
	net.Conn
	reads *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(p)
}

// withClock returns opts paced by a fake clock.
func withClock(opts DialOpts) (DialOpts, *clock.Fake) {
	fake := clock.NewFake(time.Unix(1_000_000, 0))
	opts.clock = fake
	return opts, fake
}
