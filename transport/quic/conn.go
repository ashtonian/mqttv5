package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// reasonSessionTakenOver is the MQTT reason code Session Taken Over.
const reasonSessionTakenOver = byte(wire.ReasonSessionTakenOver)

// PeerError reports that the broker aborted the connection with a QUIC
// application error code of 0x300 plus an MQTT reason code (draft
// §6.3.3): 0x381 Malformed Packet, 0x382 Protocol Error, 0x38E Session
// Taken Over. Read and Write return it; match it with errors.As.
type PeerError struct {
	// Reason is the MQTT reason code the QUIC error code carries.
	Reason wire.ReasonCode
	// Err is the quic-go stream or connection error it came in.
	Err error
}

func (e *PeerError) Error() string {
	return fmt.Sprintf("mqttv5/transport/quic: the broker aborted the connection with MQTT reason code %#02x: %v", byte(e.Reason), e.Err)
}

func (e *PeerError) Unwrap() error { return e.Err }

// peerReason extracts the MQTT reason code from a QUIC error the broker
// sent, when its code is 0x300 plus one.
func peerReason(err error) (byte, bool) {
	var code uint64
	if se, ok := errors.AsType[*quicgo.StreamError](err); ok && se.Remote {
		code = uint64(se.ErrorCode)
	} else if ae, ok := errors.AsType[*quicgo.ApplicationError](err); ok && ae.Remote {
		code = uint64(ae.ErrorCode)
	} else {
		return 0, false
	}
	if code < codeBase || code > codeBase+0xFF {
		return 0, false
	}
	return byte(code - codeBase), true
}

// Conn is one MQTT Network Connection over QUIC: the stream that carries
// the MQTT bytes and the QUIC connection around it, which closes with it.
// Dial returns it as a transport.Conn; treat it as opaque.
type Conn struct {
	qc     *quicgo.Conn
	dialer *Dialer

	// mu guards stream, which a rejected 0-RTT attempt replaces, the
	// deadlines carried over to its replacement, and the closed flag's
	// ordering against them.
	mu            sync.Mutex
	stream        *quicgo.Stream
	readDeadline  time.Time
	writeDeadline time.Time

	// rmu admits one reader: the caller's Read, or the drain that waits
	// for the broker's FIN after Close.
	rmu sync.Mutex

	// wmu keeps writes whole: Close sends the FIN only between them. It
	// also guards early.
	wmu sync.Mutex
	// early holds the CONNECT sent as 0-RTT data until the handshake
	// settles it; nil when there is none.
	early *earlyData

	closeOnce sync.Once
	closed    atomic.Bool
	// closing is closed by Close and Abort, ending waits for the
	// handshake.
	closing chan struct{}
}

var (
	_ transport.Conn               = (*Conn)(nil)
	_ transport.ConnectionStater   = (*Conn)(nil)
	_ transport.Aborter            = (*Conn)(nil)
	_ transport.DisconnectObserver = (*Conn)(nil)
)

// newConn wraps qc and its MQTT stream s. release runs once qc has
// closed, after its CONNECTION_CLOSE has been written.
func newConn(d *Dialer, qc *quicgo.Conn, s *quicgo.Stream, early bool, release func()) *Conn {
	c := &Conn{qc: qc, dialer: d, stream: s, closing: make(chan struct{})}
	if early {
		c.early = &earlyData{need: -1}
	}
	d.closing.add()
	go func() {
		defer d.closing.done()
		<-qc.Context().Done()
		release()
	}()
	return c
}

func (c *Conn) currentStream() *quicgo.Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream
}

// Read reads the broker's MQTT byte stream. The broker's FIN is io.EOF;
// a reset with a mapped error code is a *PeerError.
func (c *Conn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for {
		if c.closed.Load() {
			return 0, net.ErrClosed
		}
		s := c.currentStream()
		n, err := s.Read(p)
		if err == nil {
			return n, nil
		}
		if n == 0 && errors.Is(err, quicgo.Err0RTTRejected) {
			if rerr := c.resendEarly(c.deadlines().read); rerr != nil {
				return 0, c.ioError(rerr)
			}
			if c.currentStream() == s {
				return 0, err
			}
			continue
		}
		return n, c.ioError(err)
	}
}

// Write writes p to the stream. Before the handshake completes on a
// 0-RTT connection, only the CONNECT goes out as early data; anything
// after it waits for the handshake.
func (c *Conn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closed.Load() {
		return 0, net.ErrClosed
	}
	written := 0
	if c.early != nil {
		n, err := c.writeEarly(p)
		written += n
		p = p[n:]
		if err != nil {
			return written, c.ioError(err)
		}
		if len(p) == 0 {
			return written, nil
		}
		if err := c.settleEarly(c.deadlines().write); err != nil {
			return written, c.ioError(err)
		}
	}
	n, err := c.currentStream().Write(p)
	written += n
	if err != nil {
		return written, c.ioError(err)
	}
	return written, nil
}

// ioError reports an error from the stream: net.ErrClosed once the
// connection is closing, a *PeerError for a mapped code from the broker.
func (c *Conn) ioError(err error) error {
	if c.closed.Load() {
		return net.ErrClosed
	}
	reason, ok := peerReason(err)
	if !ok {
		return err
	}
	if reason == reasonSessionTakenOver {
		c.dialer.noteTakeover()
	}
	return &PeerError{Reason: wire.ReasonCode(reason), Err: err}
}

// ObserveDisconnect records a Session Taken Over DISCONNECT from the
// broker, so the Dialer waits before dialing again (draft §6.3.3).
func (c *Conn) ObserveDisconnect(reason byte) {
	if reason == reasonSessionTakenOver {
		c.dialer.noteTakeover()
	}
}

// Close ends the connection gracefully (draft §6.3.1): a FIN after the
// last packet written, normally the client's DISCONNECT, then, in the
// background, a wait of up to DialOpts.CloseTimeout for the broker's FIN
// before the QUIC connection is closed; at the timeout the stream is
// reset first. Read and Write return net.ErrClosed at once. A write in
// progress, which may be stalled behind flow control, is not waited for:
// the stream is reset instead, since a FIN would cut its packet short.
// Closing a closed connection reports net.ErrClosed.
func (c *Conn) Close() error {
	err := net.ErrClosed
	c.closeOnce.Do(func() {
		err = nil
		c.markClosed()
		s := c.currentStream()
		c.dialer.closing.add()
		if !c.wmu.TryLock() {
			s.CancelWrite(0)
			s.CancelRead(0)
			go func() {
				defer c.dialer.closing.done()
				_ = c.qc.CloseWithError(0, "")
			}()
			return
		}
		_ = s.Close()
		c.wmu.Unlock()
		go c.drain(s)
	})
	return err
}

// markClosed fails Read and Write from now on and ends the caller's Read
// in progress; the drain then takes over reading.
func (c *Conn) markClosed() {
	c.mu.Lock()
	c.closed.Store(true)
	_ = c.stream.SetReadDeadline(time.Now())
	c.mu.Unlock()
	close(c.closing)
}

// drain waits for the broker's FIN, discarding what it still sends, and
// closes the QUIC connection: at once on the FIN or a failure, after
// resetting the stream at the close timeout.
func (c *Conn) drain(s *quicgo.Stream) {
	defer c.dialer.closing.done()
	c.rmu.Lock()
	defer c.rmu.Unlock()
	_ = s.SetReadDeadline(time.Time{})

	timer := c.dialer.clock.NewTimer(c.dialer.closeTimeout())
	defer timer.Stop()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		buf := make([]byte, 1024)
		for {
			if _, err := s.Read(buf); err != nil {
				return
			}
		}
	}()
	select {
	case <-drained:
		_ = c.qc.CloseWithError(0, "")
	case <-timer.C():
		c.dialer.logger.Warn("mqttv5/transport/quic: the broker did not end the stream within the close timeout; closing the connection",
			slog.String("broker", c.qc.RemoteAddr().String()),
			slog.Duration("close_timeout", c.dialer.closeTimeout()))
		s.CancelWrite(0)
		_ = c.qc.CloseWithError(0, "graceful close timed out")
		// Closing the connection ends the read.
		<-drained
	}
}

// Abort ends the connection over a protocol error the client detected in
// the broker's packets (draft §6.7.2): RESET_STREAM and STOP_SENDING
// with 0x300 + reason, after the DISCONNECT the client already wrote,
// which a broker supporting RESET_STREAM_AT still receives. The QUIC
// connection is closed with the same code once the broker has closed it,
// or at DialOpts.CloseTimeout. Aborting a closed connection reports
// net.ErrClosed.
func (c *Conn) Abort(reason byte) error {
	err := net.ErrClosed
	c.closeOnce.Do(func() {
		err = nil
		c.markClosed()
		code := codeBase + uint64(reason)
		s := c.currentStream()
		if c.wmu.TryLock() {
			s.SetReliableBoundary()
			c.wmu.Unlock()
		}
		s.CancelWrite(quicgo.StreamErrorCode(code))
		s.CancelRead(quicgo.StreamErrorCode(code))
		c.dialer.closing.add()
		go c.linger(quicgo.ApplicationErrorCode(code))
	})
	return err
}

// linger closes the QUIC connection with code unless the broker closes
// it first, giving the reset frames time to reach it.
func (c *Conn) linger(code quicgo.ApplicationErrorCode) {
	defer c.dialer.closing.done()
	timer := c.dialer.clock.NewTimer(c.dialer.closeTimeout())
	defer timer.Stop()
	select {
	case <-c.qc.Context().Done():
	case <-timer.C():
		_ = c.qc.CloseWithError(code, "")
	}
}

// ConnectionState returns the TLS state of the handshake, waiting for it
// to complete on a 0-RTT connection, so a TLS check before CONNECT sees
// the verified chain (and the CONNECT then waits for the handshake).
func (c *Conn) ConnectionState() tls.ConnectionState {
	if isEarly(c.qc) {
		select {
		case <-c.qc.HandshakeComplete():
		case <-c.qc.Context().Done():
		}
	}
	return c.qc.ConnectionState().TLS
}

type deadlines struct{ read, write time.Time }

func (c *Conn) deadlines() deadlines {
	c.mu.Lock()
	defer c.mu.Unlock()
	return deadlines{read: c.readDeadline, write: c.writeDeadline}
}

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	return c.setDeadlines(t, t, true, true)
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.setDeadlines(t, time.Time{}, true, false)
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.setDeadlines(time.Time{}, t, false, true)
}

func (c *Conn) setDeadlines(read, write time.Time, setRead, setWrite bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return net.ErrClosed
	}
	if setRead {
		c.readDeadline = read
		if err := c.stream.SetReadDeadline(read); err != nil {
			return err
		}
	}
	if setWrite {
		c.writeDeadline = write
		if err := c.stream.SetWriteDeadline(write); err != nil {
			return err
		}
	}
	return nil
}

// LocalAddr is the UDP socket's address.
func (c *Conn) LocalAddr() net.Addr { return c.qc.LocalAddr() }

// RemoteAddr is the broker's UDP address.
func (c *Conn) RemoteAddr() net.Addr { return c.qc.RemoteAddr() }

// earlyData tracks the CONNECT a 0-RTT connection sends before its
// handshake completes, so it can be sent again when the broker rejects
// the early data (draft §5.5.3).
type earlyData struct {
	// sent is what went out as early data.
	sent []byte
	// need is the CONNECT's length once its fixed header has been
	// written; -1 before.
	need int
}

// allow returns how many leading bytes of p belong to the first MQTT
// packet and may go out as early data. That packet must be a CONNECT:
// replaying any other packet could repeat its effect, so it waits for
// the handshake.
func (e *earlyData) allow(p []byte) int {
	if e.need < 0 {
		head := make([]byte, 0, 5)
		head = append(head, e.sent[:min(len(e.sent), 5)]...)
		head = append(head, p[:min(len(p), 5-len(head))]...)
		need, known := packetLength(head)
		if !known {
			// Every byte so far is the CONNECT's fixed header.
			return len(p)
		}
		e.need = need
	}
	return max(0, min(len(p), e.need-len(e.sent)))
}

// packetLength returns the length of the MQTT packet whose fixed header
// head starts, or 0 when it is not a CONNECT or its header is malformed;
// known is false while head is too short to tell.
func packetLength(head []byte) (length int, known bool) {
	if len(head) == 0 {
		return 0, false
	}
	if head[0] != byte(wire.CONNECT)<<4 {
		return 0, true
	}
	remaining, shift := 0, 0
	for i := 1; i < len(head); i++ {
		b := head[i]
		remaining |= int(b&0x7F) << shift
		if b&0x80 == 0 {
			return i + 1 + remaining, true
		}
		if i == 4 {
			return 0, true
		}
		shift += 7
	}
	return 0, false
}

// writeEarly writes the part of p that may go out as early data; the
// caller holds wmu. A rejection of the early data settles it, and the
// rest of p goes out on the replacement stream.
func (c *Conn) writeEarly(p []byte) (int, error) {
	n := c.early.allow(p)
	if n == 0 {
		return 0, nil
	}
	k, err := c.currentStream().Write(p[:n])
	c.early.sent = append(c.early.sent, p[:k]...)
	if errors.Is(err, quicgo.Err0RTTRejected) {
		return k, c.settleEarly(c.deadlines().write)
	}
	return k, err
}

// resendEarly settles the early data from the read side.
func (c *Conn) resendEarly(deadline time.Time) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.settleEarly(deadline)
}

// settleEarly waits for the handshake, by deadline, and, when the broker
// rejected the early data, sends the CONNECT again on a new stream; the
// caller holds wmu.
func (c *Conn) settleEarly(deadline time.Time) error {
	if c.early == nil {
		return nil
	}
	ctx, cancel := c.waitContext(deadline)
	defer cancel()
	select {
	case <-c.qc.HandshakeComplete():
	case <-c.qc.Context().Done():
		return context.Cause(c.qc.Context())
	case <-ctx.Done():
		return c.waitError(ctx)
	}
	sent := c.early.sent
	c.early = nil
	if c.qc.ConnectionState().Used0RTT {
		return nil
	}
	if _, err := c.qc.NextConnection(ctx); err != nil {
		return err
	}
	s, err := c.qc.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.stream = s
	_ = s.SetReadDeadline(c.readDeadline)
	_ = s.SetWriteDeadline(c.writeDeadline)
	c.mu.Unlock()
	if len(sent) == 0 {
		return nil
	}
	_, err = s.Write(sent)
	return err
}

// waitContext ends at deadline, if set, or when the connection starts
// closing.
func (c *Conn) waitContext(deadline time.Time) (context.Context, context.CancelFunc) {
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline.IsZero() {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), deadline)
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-c.closing:
			cancel()
		case <-stop:
		}
	}()
	return ctx, func() {
		close(stop)
		cancel()
	}
}

func (c *Conn) waitError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return os.ErrDeadlineExceeded
	}
	return net.ErrClosed
}
