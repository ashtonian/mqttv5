package quic

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/ashtonian/mqttv5/wire"
)

// closeCause waits for qc to close and returns why.
func closeCause(t *testing.T, qc *quicgo.Conn) error {
	t.Helper()
	select {
	case <-qc.Context().Done():
		return context.Cause(qc.Context())
	case <-time.After(testTimeout):
		t.Fatal("connection did not close")
		return nil
	}
}

// wantAppClose fails unless err is the peer closing the connection with
// application error code.
func wantAppClose(t *testing.T, err error, code quicgo.ApplicationErrorCode) {
	t.Helper()
	ae, ok := errors.AsType[*quicgo.ApplicationError](err)
	if !ok || !ae.Remote || ae.ErrorCode != code {
		t.Fatalf("connection closed with %v, want the client's application error %#x", err, code)
	}
}

// wantStreamError fails unless err is the client cancelling the stream
// with code.
func wantStreamError(t *testing.T, err error, code quicgo.StreamErrorCode) {
	t.Helper()
	se, ok := errors.AsType[*quicgo.StreamError](err)
	if !ok || !se.Remote || se.ErrorCode != code {
		t.Fatalf("stream error %v, want the client's code %#x", err, code)
	}
}

// The broker's FIN is the end of the MQTT byte stream.
func TestBrokerFINIsEOF(t *testing.T) {
	srv := startServer(t)
	conn := dial(t, srv.url, srv.opts())
	defer conn.Close()
	write(t, conn, connectPacket(1))
	sc := srv.next(t)
	write(t, sc.stream, []byte("bye"))
	_ = sc.stream.Close()

	readFull(t, conn, []byte("bye"))
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read after the broker's FIN = %v, want io.EOF", err)
	}
}

// Close sends a FIN after what was written, waits for the broker's FIN
// and then closes the QUIC connection with no error (draft §6.3.1).
func TestCloseIsGraceful(t *testing.T) {
	srv := startServer(t)
	d := NewDialer(srv.opts())
	conn, err := d.Dial(testContext(t), srv.url)
	if err != nil {
		t.Fatal(err)
	}
	disconnect := []byte{0xE0, 0x00}
	write(t, conn, connectPacket(4))
	write(t, conn, disconnect)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read after Close = %v, want net.ErrClosed", err)
	}
	if _, err := conn.Write(disconnect); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Close = %v, want net.ErrClosed", err)
	}

	sc := srv.next(t)
	got, err := io.ReadAll(sc.stream)
	if err != nil {
		t.Fatalf("broker read up to the FIN: %v", err)
	}
	if want := append(connectPacket(4), disconnect...); !bytes.Equal(got, want) {
		t.Fatalf("broker read %x, want %x", got, want)
	}
	// The client waits for the broker's FIN before closing the connection.
	select {
	case <-sc.qc.Context().Done():
		t.Fatal("client closed the connection before the broker's FIN")
	default:
	}
	_ = sc.stream.Close()
	wantAppClose(t, closeCause(t, sc.qc), 0)
	if err := d.Wait(testContext(t)); err != nil {
		t.Fatalf("Dialer.Wait: %v", err)
	}
}

// Without the broker's FIN, the close timeout resets the stream and
// closes the connection.
func TestCloseTimesOut(t *testing.T) {
	srv := startServer(t)
	opts, fake := withClock(srv.opts())
	conn := dial(t, srv.url, opts)
	write(t, conn, connectPacket(1))
	sc := srv.next(t)
	conn.Close()

	advanceWhenPending(t, fake, DefaultCloseTimeout)
	wantAppClose(t, closeCause(t, sc.qc), 0)
}

// A write stalled behind flow control does not hold Close up, and the
// stream is reset rather than cut short by a FIN.
func TestCloseDuringStalledWrite(t *testing.T) {
	srv := startServer(t)
	conn := dial(t, srv.url, srv.opts())
	write(t, conn, connectPacket(1))
	sc := srv.next(t)

	stalled := make(chan error, 1)
	go func() {
		// Far past the broker's flow-control window, which it never
		// reads to open.
		_, err := conn.Write(make([]byte, 64<<20))
		stalled <- err
	}()
	// Close once the write holds the connection's write lock.
	for conn.wmu.TryLock() {
		conn.wmu.Unlock()
		runtime.Gosched()
	}
	conn.Close()
	select {
	case err := <-stalled:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("stalled Write = %v, want net.ErrClosed", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("Close did not end the stalled write")
	}
	// The reset and the connection close race to the broker; either is
	// an abrupt end, where a FIN would pass off a cut-off packet as the
	// end of the stream.
	_, err := io.Copy(io.Discard, sc.stream)
	se, reset := errors.AsType[*quicgo.StreamError](err)
	ae, closed := errors.AsType[*quicgo.ApplicationError](err)
	clientReset := reset && se.Remote && se.ErrorCode == 0
	clientClosed := closed && ae.Remote && ae.ErrorCode == 0
	if !clientReset && !clientClosed {
		t.Fatalf("broker read ended with %v, want the client's reset or close", err)
	}
}

// Abort resets both directions with 0x300 + reason after the DISCONNECT
// written before it, and closes the connection with the same code at the
// close timeout (draft §6.7.2).
func TestAbortCarriesReason(t *testing.T) {
	srv := startServer(t)
	opts, fake := withClock(srv.opts())
	conn := dial(t, srv.url, opts)
	write(t, conn, connectPacket(1))
	sc := srv.next(t)
	readFull(t, sc.stream, connectPacket(1))

	disconnect := []byte{0xE0, 0x01, byte(wire.ReasonProtocolError)}
	write(t, conn, disconnect)
	if err := conn.Abort(byte(wire.ReasonProtocolError)); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Read after Abort = %v, want net.ErrClosed", err)
	}

	// RESET_STREAM_AT delivers the DISCONNECT before the reset.
	got, err := io.ReadAll(sc.stream)
	if !bytes.Equal(got, disconnect) {
		t.Fatalf("broker read %x before the reset, want the DISCONNECT %x", got, disconnect)
	}
	wantStreamError(t, err, 0x382)
	// STOP_SENDING carries the same code.
	deadline := time.Now().Add(testTimeout)
	for {
		_, err = sc.stream.Write([]byte{0})
		if err != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	wantStreamError(t, err, 0x382)

	advanceWhenPending(t, fake, DefaultCloseTimeout)
	wantAppClose(t, closeCause(t, sc.qc), 0x382)
}

// A reset with a mapped code from the broker is a PeerError; Session
// Taken Over makes the Dialer wait before its next dial (draft §6.3.3).
func TestPeerTakeover(t *testing.T) {
	srv := startServer(t)
	opts, fake := withClock(srv.opts())
	d := NewDialer(opts)
	ctx := testContext(t)
	conn, err := d.Dial(ctx, srv.url)
	if err != nil {
		t.Fatal(err)
	}
	write(t, conn, connectPacket(1))
	srv.next(t).stream.CancelWrite(0x38E)

	_, err = conn.Read(make([]byte, 64))
	pe, ok := errors.AsType[*PeerError](err)
	if !ok || pe.Reason != wire.ReasonSessionTakenOver {
		t.Fatalf("Read = %v, want a PeerError with Session Taken Over", err)
	}
	if !errors.As(err, new(*quicgo.StreamError)) {
		t.Fatalf("PeerError %v does not unwrap to the broker's stream error", err)
	}
	conn.Close()
	// The close ends at once on the broker's reset; then the only timer
	// left to arm is the takeover wait.
	if err := d.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	dialed := make(chan error, 1)
	go func() {
		c, err := d.Dial(ctx, srv.url)
		if err == nil {
			c.Close()
		}
		dialed <- err
	}()
	advanceWhenPending(t, fake, DefaultTakeoverBackoff)
	if err := <-dialed; err != nil {
		t.Fatalf("dial after the takeover backoff: %v", err)
	}
}

// A Session Taken Over DISCONNECT, reported by the client, makes the
// Dialer wait as well; other reasons do not.
func TestObserveDisconnect(t *testing.T) {
	srv := startServer(t)
	srv.echo()
	opts, fake := withClock(srv.opts())
	d := NewDialer(opts)
	ctx := testContext(t)

	conn, err := d.Dial(ctx, srv.url)
	if err != nil {
		t.Fatal(err)
	}
	conn.(*Conn).ObserveDisconnect(byte(wire.ReasonServerShuttingDown))
	conn.Close()
	// The fake clock never advances here: a wait would fail the dial at
	// ctx's deadline.
	conn, err = d.Dial(ctx, srv.url)
	if err != nil {
		t.Fatalf("dial after Server Shutting Down: %v", err)
	}

	conn.(*Conn).ObserveDisconnect(byte(wire.ReasonSessionTakenOver))
	conn.Close()
	// Both closes finish on the broker's FIN; then the only timer left
	// to arm is the takeover wait.
	if err := d.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	dialed := make(chan error, 1)
	go func() {
		_, err := d.Dial(ctx, srv.url)
		dialed <- err
	}()
	advanceWhenPending(t, fake, DefaultTakeoverBackoff)
	if err := <-dialed; err != nil {
		t.Fatalf("dial after the takeover backoff: %v", err)
	}
}

func TestDeadlines(t *testing.T) {
	srv := startServer(t)
	srv.echo()
	conn := dial(t, srv.url, srv.opts())
	past := time.Now().Add(-time.Second)
	if err := conn.SetReadDeadline(past); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(past); err != nil {
		t.Fatal(err)
	}
	var ne net.Error
	if _, err := conn.Read(make([]byte, 1)); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Read past the deadline = %v, want a timeout", err)
	}
	if _, err := conn.Write([]byte{0xC0, 0x00}); !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("Write past the deadline = %v, want a timeout", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	write(t, conn, connectPacket(1))
	readFull(t, conn, connectPacket(1))

	conn.Close()
	if err := conn.SetDeadline(time.Time{}); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetDeadline after Close = %v, want net.ErrClosed", err)
	}
}
