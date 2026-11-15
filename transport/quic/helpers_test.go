package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/internal/testpki"
)

// testTimeout bounds a test's waits on the network, as a backstop.
const testTimeout = 5 * time.Second

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// testServer is an in-process MQTT over QUIC endpoint. Each connection
// is handed over once the client has written to its stream, which is
// when the server learns of the stream.
type testServer struct {
	url   *url.URL
	root  *testpki.Authority
	ln    *quicgo.EarlyListener
	conns chan *serverConn

	mu       sync.Mutex
	accepted []*quicgo.Conn
}

type serverConn struct {
	qc     *quicgo.Conn
	stream *quicgo.Stream
}

type serverConfig struct {
	root       *testpki.Authority
	cert       *tls.Certificate
	nextProtos []string
	quic       *quicgo.Config
}

func startServer(t *testing.T, mods ...func(*serverConfig)) *testServer {
	t.Helper()
	cfg := serverConfig{
		nextProtos: []string{alpn},
		quic:       &quicgo.Config{Allow0RTT: true, EnableStreamResetPartialDelivery: true},
	}
	for _, mod := range mods {
		mod(&cfg)
	}
	if cfg.root == nil {
		cfg.root = testpki.NewRoot(t, "root")
	}
	cert := cfg.root.Server(t)
	if cfg.cert != nil {
		cert = *cfg.cert
	}
	tlsConf := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: cfg.nextProtos}
	ln, err := quicgo.ListenAddrEarly("127.0.0.1:0", tlsConf, cfg.quic)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &testServer{
		url:   &url.URL{Scheme: scheme, Host: ln.Addr().String()},
		root:  cfg.root,
		ln:    ln,
		conns: make(chan *serverConn, 16),
	}
	t.Cleanup(s.close)
	go s.accept()
	return s
}

// close closes the listener and every connection it accepted, which
// closing the listener alone leaves open, so no socket outlives the
// test.
func (s *testServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, qc := range s.accepted {
		_ = qc.CloseWithError(0, "test over")
	}
}

func (s *testServer) accept() {
	for {
		qc, err := s.ln.Accept(context.Background())
		if err != nil {
			return
		}
		s.mu.Lock()
		s.accepted = append(s.accepted, qc)
		s.mu.Unlock()
		go func() {
			st, err := qc.AcceptStream(context.Background())
			if err != nil {
				return
			}
			s.conns <- &serverConn{qc: qc, stream: st}
		}()
	}
}

// next returns the next connection whose client has written.
func (s *testServer) next(t *testing.T) *serverConn {
	t.Helper()
	select {
	case c := <-s.conns:
		return c
	case <-time.After(testTimeout):
		t.Fatal("no connection reached the server")
		return nil
	}
}

// echo serves every connection by echoing its stream, and answers the
// client's FIN with its own.
func (s *testServer) echo() {
	go func() {
		for c := range s.conns {
			go func() {
				_, _ = io.Copy(c.stream, c.stream)
				_ = c.stream.Close()
			}()
		}
	}()
}

// opts returns DialOpts trusting the server's root.
func (s *testServer) opts() DialOpts {
	return DialOpts{TLSConfig: trusting(s.root)}
}

func trusting(root *testpki.Authority) *tls.Config {
	return &tls.Config{RootCAs: root.Pool(), Time: testpki.Clock}
}

// dial dials u with opts or fails the test.
func dial(t *testing.T, u *url.URL, opts DialOpts) *Conn {
	t.Helper()
	conn, err := Dial(testContext(t), u, opts)
	if err != nil {
		t.Fatalf("dial %s: %v", u, err)
	}
	return conn.(*Conn)
}

// write writes p to w or fails the test.
func write(t *testing.T, w io.Writer, p []byte) {
	t.Helper()
	if _, err := w.Write(p); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// readFull reads len(want) bytes from r and compares them with want.
func readFull(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read %d bytes: %v", len(want), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("read %q, want %q", got, want)
	}
}

// connectPacket returns a packet with a CONNECT fixed header and a body
// of n bytes: all a transport looks at.
func connectPacket(n int) []byte {
	p := []byte{0x10}
	for v := n; ; {
		b := byte(v & 0x7F)
		v >>= 7
		if v > 0 {
			p = append(p, b|0x80)
			continue
		}
		p = append(p, b)
		break
	}
	return append(p, bytes.Repeat([]byte{'c'}, n)...)
}

// startTLSEcho serves TLS on 127.0.0.1 with a certificate from root,
// echoing each connection, and returns its mqtts:// URL.
func startTLSEcho(t *testing.T, root *testpki.Authority) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{root.Server(t)}})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return "mqtts://" + ln.Addr().String()
}

// blackhole is a UDP endpoint that reads and drops every datagram,
// counting the sockets that sent them.
type blackhole struct {
	pc      net.PacketConn
	mu      sync.Mutex
	sources map[string]bool
}

func startBlackhole(t *testing.T) *blackhole {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	b := &blackhole{pc: pc, sources: map[string]bool{}}
	go func() {
		buf := make([]byte, 2048)
		for {
			_, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			b.mu.Lock()
			b.sources[from.String()] = true
			b.mu.Unlock()
		}
	}()
	return b
}

func (b *blackhole) url() *url.URL {
	return &url.URL{Scheme: scheme, Host: b.pc.LocalAddr().String()}
}

// attempts is how many sockets have sent to the blackhole: one per QUIC
// attempt, since each opens a socket of its own.
func (b *blackhole) attempts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.sources)
}

// relay forwards UDP datagrams between one client and server, and can
// hold the server's datagrams back until released.
type relay struct {
	front *net.UDPConn
	back  *net.UDPConn

	mu     sync.Mutex
	client *net.UDPAddr
	held   bool
	queue  [][]byte
}

func startRelay(t *testing.T, server net.Addr) *relay {
	t.Helper()
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	back, err := net.DialUDP("udp", nil, server.(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial udp: %v", err)
	}
	t.Cleanup(func() {
		_ = front.Close()
		_ = back.Close()
	})
	r := &relay{front: front, back: back}
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			r.mu.Lock()
			r.client = from
			r.mu.Unlock()
			_, _ = back.Write(buf[:n])
		}
	}()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, err := back.Read(buf)
			if err != nil {
				return
			}
			r.mu.Lock()
			if r.held {
				r.queue = append(r.queue, bytes.Clone(buf[:n]))
				r.mu.Unlock()
				continue
			}
			client := r.client
			r.mu.Unlock()
			_, _ = front.WriteToUDP(buf[:n], client)
		}
	}()
	return r
}

func (r *relay) url() *url.URL {
	return &url.URL{Scheme: scheme, Host: r.front.LocalAddr().String()}
}

// hold queues the server's datagrams instead of forwarding them.
func (r *relay) hold() {
	r.mu.Lock()
	r.held = true
	r.mu.Unlock()
}

// release forwards what was queued and stops holding.
func (r *relay) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.held = false
	for _, p := range r.queue {
		_, _ = r.front.WriteToUDP(p, r.client)
	}
	r.queue = nil
}

// withClock returns opts paced by a fake clock.
func withClock(opts DialOpts) (DialOpts, *clock.Fake) {
	fake := clock.NewFake(time.Unix(1_000_000, 0))
	opts.clock = fake
	return opts, fake
}

// advanceWhenPending waits for a timer on fake and advances past d.
func advanceWhenPending(t *testing.T, fake *clock.Fake, d time.Duration) {
	t.Helper()
	if !fake.WaitPending(1, testTimeout) {
		t.Fatal("no timer armed")
	}
	fake.Advance(d)
}
