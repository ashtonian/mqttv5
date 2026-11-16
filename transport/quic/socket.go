package quic

import (
	"fmt"
	"net"
	"reflect"
	"sync"

	quicgo "github.com/quic-go/quic-go"
)

// sockets demultiplexes each caller-supplied PacketConn through one
// quic-go Transport, shared by every dial on it from any Dialer or Dial:
// quic-go allows one Transport per socket, and two would split its
// datagrams between them. The Transport lives while a connection uses
// the socket, then hands the socket back to the caller, open.
var sockets = socketTable{inUse: map[net.PacketConn]*sharedSocket{}}

type socketTable struct {
	mu    sync.Mutex
	inUse map[net.PacketConn]*sharedSocket
}

type sharedSocket struct {
	tr    *quicgo.Transport
	conns int
}

// acquire returns the Transport for pc and a release to call once the
// connection dialed over it has closed, or the dial has failed.
func (t *socketTable) acquire(pc net.PacketConn) (*quicgo.Transport, func(), error) {
	if !reflect.TypeOf(pc).Comparable() {
		return nil, nil, fmt.Errorf("mqttv5/transport/quic: DialOpts.PacketConn of type %T is not comparable; pass a pointer", pc)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.inUse[pc]
	if s == nil {
		s = &sharedSocket{tr: &quicgo.Transport{Conn: pc}}
		t.inUse[pc] = s
	}
	s.conns++
	var once sync.Once
	return s.tr, func() { once.Do(func() { t.release(pc, s) }) }, nil
}

func (t *socketTable) release(pc net.PacketConn, s *sharedSocket) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s.conns--
	if s.conns > 0 {
		return
	}
	delete(t.inUse, pc)
	// Under the lock: a Transport acquired for pc next must not start
	// reading until this one has stopped. Close leaves pc open.
	_ = s.tr.Close()
}
