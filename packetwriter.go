package mqttv5

import (
	"io"
	"net"

	"github.com/ashtonian/mqttv5/transport"
)

// gatherLimit bounds the bytes a packetWriter gathers into one Write:
// the largest TLS record plaintext (RFC 8446, section 5.1), so a
// gathered write is at most one record. A buffer this size or larger
// goes out on its own, uncopied.
const gatherLimit = 16 << 10

// packetWriter writes encoded packets to a connection in as few writes
// as the connection allows. A TCP or Unix socket takes a net.Buffers in
// one writev. Other connections (TLS, WebSocket, QUIC, a custom
// WithDialFunc transport) have no vectored write, and each Write is a
// TLS record, a WebSocket frame or a stream write of its own with a
// syscall behind it; for those, packetWriter copies the buffers of a
// packet, or of a batch of packets, into one Write.
//
// It is not safe for concurrent use: the handshake uses it before the
// connection's goroutines start, and they use it under connState.wmu.
type packetWriter struct {
	conn transport.Conn
	// vectored is set when conn writes a net.Buffers in one writev.
	vectored bool
	// buf holds bytes gathered and not yet written. It grows to the
	// largest write gathered, at most gatherLimit, and is kept for the
	// connection.
	buf []byte
}

func newPacketWriter(conn transport.Conn) *packetWriter {
	return &packetWriter{conn: conn, vectored: writesVectored(conn)}
}

// writesVectored reports whether net.Buffers.WriteTo writes to conn in
// one writev, as it does for the net package's own sockets.
func writesVectored(conn transport.Conn) bool {
	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		return true
	}
	return false
}

// WritePacket runs encode, which writes one or more packets, and writes
// what it wrote. Encoders write a packet as several buffers (a fixed
// header, then the body); on a connection without vectored writes they
// are gathered into one Write.
func (w *packetWriter) WritePacket(encode writeFn) (int64, error) {
	if w.vectored {
		return encode(w.conn)
	}
	n, err := encode(w)
	if err != nil {
		w.buf = w.buf[:0]
		return n, err
	}
	return n, w.Flush()
}

// WriteBuffers writes bufs, consuming them as net.Buffers.WriteTo does:
// one writev on a vectored connection, otherwise gathered into as few
// Writes as gatherLimit allows.
func (w *packetWriter) WriteBuffers(bufs *net.Buffers) (int64, error) {
	if w.vectored {
		return bufs.WriteTo(w.conn)
	}
	if len(*bufs) == 1 && len(w.buf) == 0 {
		n, err := w.conn.Write((*bufs)[0])
		*bufs = (*bufs)[1:]
		return int64(n), err
	}
	var n int64
	for len(*bufs) > 0 {
		m, err := w.Write((*bufs)[0])
		n += int64(m)
		if err != nil {
			w.buf = w.buf[:0]
			return n, err
		}
		*bufs = (*bufs)[1:]
	}
	return n, w.Flush()
}

// Write gathers p behind what was gathered before, writing that first
// when p would take it past gatherLimit; a p of gatherLimit or more is
// then written on its own. Bytes gathered reach the connection on
// Flush. On a vectored connection Write writes p at once.
func (w *packetWriter) Write(p []byte) (int, error) {
	if w.vectored {
		return w.conn.Write(p)
	}
	if len(w.buf)+len(p) > gatherLimit {
		if err := w.Flush(); err != nil {
			return 0, err
		}
		if len(p) >= gatherLimit {
			return w.conn.Write(p)
		}
	}
	if need := len(w.buf) + len(p); need > cap(w.buf) {
		grown := make([]byte, len(w.buf), min(gatherLimit, max(need, 2*cap(w.buf))))
		copy(grown, w.buf)
		w.buf = grown
	}
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// Flush writes the gathered bytes.
func (w *packetWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.conn.Write(w.buf)
	w.buf = w.buf[:0]
	return err
}

var _ io.Writer = (*packetWriter)(nil)
