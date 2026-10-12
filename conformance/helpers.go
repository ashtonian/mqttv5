// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build conformance

// Package conformance houses tests that run mqttv5 against a real
// broker (mosquitto by default; emqx also supported via env). The
// suite is gated behind the `conformance` build tag so the default
// `go test ./...` does not require a broker to be running.
//
// To run:
//
//	docker compose -f conformance/docker-compose.yml up -d
//	go -C conformance test -tags conformance -race -v -timeout 120s
//	docker compose -f conformance/docker-compose.yml down
//
// MQTT_BROKER env var overrides the default mqtt://127.0.0.1:1883.
package conformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5"
	"github.com/ashtonian/mqttv5/wire"
)

const defaultBroker = "mqtt://127.0.0.1:1883"

// brokerURL returns the configured broker, defaulting to localhost
// mosquitto on 1883.
func brokerURL() string {
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		return v
	}
	return defaultBroker
}

// secondaryBrokerURL is used by multi-broker tests (e.g., ClientGroup).
// Defaults to the emqx port in our docker-compose.
func secondaryBrokerURL() string {
	if v := os.Getenv("MQTT_BROKER_2"); v != "" {
		return v
	}
	return "mqtt://127.0.0.1:1884"
}

// requireBroker skips the test when the broker is unreachable. Run the
// compose file before invoking the suite — see this package's docs.
func requireBroker(t *testing.T, url string) {
	t.Helper()
	host := stripScheme(url)
	c, err := net.DialTimeout("tcp", host, 500*time.Millisecond)
	if err != nil {
		t.Skipf("conformance: broker %s unreachable (%v) — start docker-compose first", url, err)
	}
	_ = c.Close()
}

func stripScheme(u string) string {
	// Cheap parse — these are well-known shapes (mqtt://host:port).
	for _, prefix := range []string{"mqtt://", "tcp://", "mqtts://", "tls://", "ssl://"} {
		if len(u) > len(prefix) && u[:len(prefix)] == prefix {
			return u[len(prefix):]
		}
	}
	return u
}

// connect builds a fresh, connected Client with sensible defaults.
// Caller defers cli.Disconnect.
func connect(t *testing.T, opts ...mqttv5.Option) *mqttv5.Client {
	t.Helper()
	requireBroker(t, brokerURL())

	all := []mqttv5.Option{
		mqttv5.WithBroker(brokerURL()),
		mqttv5.WithClientID(t.Name() + "-" + randSuffix()),
		mqttv5.WithKeepAlive(30),
		mqttv5.WithConnectTimeout(5 * time.Second),
	}
	all = append(all, opts...)

	cli, err := mqttv5.New(all...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = cli.Disconnect(shutdown)
	})
	return cli
}

// randSuffix returns a short random string so tests don't collide on
// ClientID when running concurrently.
func randSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	now := time.Now().UnixNano()
	for i := range b {
		b[i] = charset[int(now)%len(charset)]
		now /= int64(len(charset))
	}
	return string(b)
}

// faultProxy sits between one client and the broker and relays MQTT
// packet by packet. A test can cut every connection, refuse new ones
// for a while (so the broker's session expires), or drop packets of
// chosen types that the client sends.
type faultProxy struct {
	ln       net.Listener
	upstream string
	trace    *packetTrace

	drop    atomic.Pointer[func(wire.PacketType) bool]
	dropped atomic.Int64

	mu          sync.Mutex
	conns       []net.Conn
	accepted    int
	refuseUntil time.Time
}

// newFaultProxy relays to upstream; with traced it also records the
// packets it relays (see packetTrace).
func newFaultProxy(t *testing.T, upstream string, traced bool) *faultProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	px := &faultProxy{ln: ln, upstream: upstream}
	if traced {
		px.trace = newPacketTrace()
	}
	go px.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		px.cut()
	})
	return px
}

// url is the broker URL clients use to go through the proxy.
func (px *faultProxy) url() string { return "mqtt://" + px.ln.Addr().String() }

// dropFromClient discards every packet the client sends whose type
// matches; nil relays everything again.
func (px *faultProxy) dropFromClient(match func(wire.PacketType) bool) {
	if match == nil {
		px.drop.Store(nil)
		return
	}
	px.drop.Store(&match)
}

// refuse closes connections accepted during the next d.
func (px *faultProxy) refuse(d time.Duration) {
	px.mu.Lock()
	px.refuseUntil = time.Now().Add(d)
	px.mu.Unlock()
}

// cut closes every relayed connection and reports how many it closed.
func (px *faultProxy) cut() int {
	px.mu.Lock()
	conns := px.conns
	px.conns = nil
	px.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	return len(conns) / 2
}

// awaitDropped waits until n packets have been dropped.
func (px *faultProxy) awaitDropped(t *testing.T, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); px.dropped.Load() < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d packets dropped", px.dropped.Load(), n)
		}
	}
}

func (px *faultProxy) accept() {
	for {
		client, err := px.ln.Accept()
		if err != nil {
			return
		}
		px.mu.Lock()
		refused := time.Now().Before(px.refuseUntil)
		px.mu.Unlock()
		if refused {
			_ = client.Close()
			continue
		}
		broker, err := net.Dial("tcp", px.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}
		px.mu.Lock()
		px.conns = append(px.conns, client, broker)
		px.accepted++
		n := px.accepted
		px.mu.Unlock()
		go px.relay(broker, client, fmt.Sprintf("c%d>", n), true)
		go px.relay(client, broker, fmt.Sprintf("c%d<", n), false)
	}
}

func (px *faultProxy) relay(dst, src net.Conn, dir string, fromClient bool) {
	defer func() { _ = dst.Close() }()
	r := bufio.NewReaderSize(src, 64<<10)
	for {
		frame, err := readFrame(r)
		if err != nil {
			return
		}
		if fromClient {
			if match := px.drop.Load(); match != nil && (*match)(wire.PacketType(frame[0]>>4)) {
				px.dropped.Add(1)
				continue
			}
		}
		if px.trace != nil {
			px.trace.recordFrame(dir, frame)
		}
		if _, err := dst.Write(frame); err != nil {
			return
		}
	}
}

// readFrame reads one whole MQTT packet: fixed header, Remaining
// Length and body.
func readFrame(r *bufio.Reader) ([]byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	hdr := []byte{first}
	n, mul := 0, 1
	for i := 0; ; i++ {
		if i == 4 {
			return nil, errors.New("malformed Remaining Length")
		}
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		hdr = append(hdr, b)
		n += int(b&0x7f) * mul
		if b&0x80 == 0 {
			break
		}
		mul *= 128
	}
	frame := make([]byte, len(hdr)+n)
	copy(frame, hdr)
	if _, err := io.ReadFull(r, frame[len(hdr):]); err != nil {
		return nil, err
	}
	return frame, nil
}

// packetTrace keeps, for each message payload, the PUBLISH and
// acknowledgements that carried it, plus every CONNACK.
type packetTrace struct {
	start time.Time
	mu    sync.Mutex
	// byID maps a packet identifier of the client — one session across
	// all its connections — to the payload of the PUBLISH it currently
	// belongs to.
	byID     map[uint16]string
	events   map[string][]string
	connacks []string
}

func newPacketTrace() *packetTrace {
	return &packetTrace{start: time.Now(), byID: map[uint16]string{}, events: map[string][]string{}}
}

func (pt *packetTrace) recordFrame(dir string, frame []byte) {
	p, err := wire.NewDecoder(bytes.NewReader(frame)).ReadPacket()
	if err != nil {
		return
	}
	defer p.Release()
	at := time.Since(pt.start).Round(time.Millisecond)
	pt.mu.Lock()
	defer pt.mu.Unlock()
	switch x := p.(type) {
	case *wire.Publish:
		if x.QoS == 0 {
			return
		}
		payload := string(x.Payload)
		pt.byID[x.PacketID] = payload
		pt.events[payload] = append(pt.events[payload], fmt.Sprintf("%v %s PUBLISH#%d qos=%d dup=%v topic=%q",
			at, dir, x.PacketID, x.QoS, x.Dup, x.Topic))
	case *wire.PubResp:
		if payload, ok := pt.byID[x.PacketID]; ok {
			pt.events[payload] = append(pt.events[payload], fmt.Sprintf("%v %s %s#%d rc=0x%02x",
				at, dir, x.Type(), x.PacketID, byte(x.ReasonCode)))
		}
	case *wire.Connack:
		pt.connacks = append(pt.connacks, fmt.Sprintf("%v %s CONNACK sp=%v rc=0x%02x",
			at, dir, x.SessionPresent, byte(x.ReasonCode)))
	}
}

func (pt *packetTrace) history(payload string) []string {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.events[payload]
}

func (pt *packetTrace) sessions() []string {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return append([]string(nil), pt.connacks...)
}

// scramSHA256 is a minimal client-side SCRAM-SHA-256 (RFC 5802)
// Authenticator used to exercise MQTT v5 enhanced authentication and
// client-initiated re-authentication (§4.12) against a real broker (EMQX).
//
// It implements just enough of SCRAM to drive the n=.../r=.../p=...
// exchange the broker validates. mqttv5 concludes the exchange on CONNACK
// (initial connect) or AUTH 0x00 (re-auth) without re-invoking Continue,
// so the optional server-signature verification (mutual auth) step is not
// performed — the broker's acceptance is the success signal.
//
// A fresh client nonce is generated per Begin, so every (re-)auth is a
// distinct exchange. Begin runs on the caller/connect goroutine and
// Continue on the read loop; the mutex guards the nonce handed between
// them.
type scramSHA256 struct {
	username string
	password string

	mu          sync.Mutex
	clientNonce string
	firstBare   string
	serverSig   []byte // expected server signature, computed in Continue

	// verifies counts successful VerifyServerFinal calls, so tests can
	// assert mutual authentication actually ran (connect + each re-auth).
	verifies atomic.Int32
}

func newSCRAM(username, password string) *scramSHA256 {
	return &scramSHA256{username: username, password: password}
}

// Method returns the SCRAM-SHA-256 AuthenticationMethod name.
func (*scramSHA256) Method() string { return "SCRAM-SHA-256" }

// Begin emits the SCRAM client-first message: "n,,n=<user>,r=<nonce>".
func (s *scramSHA256) Begin(_ context.Context) ([]byte, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.clientNonce = nonce
	s.firstBare = "n=" + saslName(s.username) + ",r=" + nonce
	first := s.firstBare
	s.mu.Unlock()
	// GS2 header "n,," (no channel binding, no authzid) + first-bare.
	return []byte("n,," + first), nil
}

// Continue consumes the server-first message and returns the client-final
// message carrying the computed proof.
func (s *scramSHA256) Continue(serverFirst []byte) ([]byte, bool, error) {
	s.mu.Lock()
	clientNonce, firstBare := s.clientNonce, s.firstBare
	s.mu.Unlock()

	r, salt, iter, err := parseServerFirst(string(serverFirst))
	if err != nil {
		return nil, false, err
	}
	if !strings.HasPrefix(r, clientNonce) {
		return nil, false, fmt.Errorf("scram: server nonce %q does not extend client nonce", r)
	}

	saltedPassword, err := pbkdf2.Key(sha256.New, s.password, salt, iter, sha256.Size)
	if err != nil {
		return nil, false, fmt.Errorf("scram: pbkdf2: %w", err)
	}
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)

	finalNoProof := "c=biws,r=" + r // biws = base64("n,,")
	authMessage := firstBare + "," + string(serverFirst) + "," + finalNoProof
	clientSig := hmacSHA256(storedKey[:], []byte(authMessage))

	// Server signature, retained for mutual-auth verification of the
	// server-final message (see VerifyServerFinal).
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))
	s.mu.Lock()
	s.serverSig = hmacSHA256(serverKey, []byte(authMessage))
	s.mu.Unlock()

	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	final := finalNoProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	return []byte(final), false, nil
}

// VerifyServerFinal completes SCRAM mutual authentication: it checks the
// server-final "v=<base64-serversig>" against the server signature
// computed in Continue (RFC 5802 §5.1). It implements
// mqttv5.ServerFinalVerifier, so mqttv5 calls it with the broker's
// concluding AuthenticationData (CONNACK on connect, AUTH 0x00 on
// re-auth). A mismatch means the server did not prove knowledge of the
// credential.
func (s *scramSHA256) VerifyServerFinal(serverFinal []byte) error {
	s.mu.Lock()
	want := s.serverSig
	s.mu.Unlock()
	if len(want) == 0 {
		return fmt.Errorf("scram: no server signature computed (VerifyServerFinal before Continue)")
	}
	got, err := parseServerFinal(string(serverFinal))
	if err != nil {
		return err
	}
	if !hmac.Equal(got, want) {
		return fmt.Errorf("scram: server signature mismatch — server not authenticated")
	}
	s.verifies.Add(1)
	return nil
}

// parseServerFinal extracts the server signature from a SCRAM server-final
// message "v=<base64>", or surfaces a server error field "e=<reason>".
func parseServerFinal(msg string) ([]byte, error) {
	for _, field := range strings.Split(msg, ",") {
		switch {
		case strings.HasPrefix(field, "v="):
			sig, err := base64.StdEncoding.DecodeString(field[2:])
			if err != nil {
				return nil, fmt.Errorf("scram: bad server signature: %w", err)
			}
			return sig, nil
		case strings.HasPrefix(field, "e="):
			return nil, fmt.Errorf("scram: server reported error: %s", field[2:])
		}
	}
	return nil, fmt.Errorf("scram: server-final missing v= field (%q)", msg)
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

func randomNonce() (string, error) {
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// parseServerFirst extracts r (nonce), s (salt), i (iterations) from a
// SCRAM server-first message: "r=<nonce>,s=<b64-salt>,i=<iterations>".
func parseServerFirst(msg string) (nonce string, salt []byte, iter int, err error) {
	for _, field := range strings.Split(msg, ",") {
		if len(field) < 2 || field[1] != '=' {
			continue
		}
		val := field[2:]
		switch field[0] {
		case 'r':
			nonce = val
		case 's':
			salt, err = base64.StdEncoding.DecodeString(val)
			if err != nil {
				return "", nil, 0, fmt.Errorf("scram: bad salt: %w", err)
			}
		case 'i':
			iter, err = strconv.Atoi(val)
			if err != nil {
				return "", nil, 0, fmt.Errorf("scram: bad iteration count: %w", err)
			}
		}
	}
	if nonce == "" || len(salt) == 0 || iter == 0 {
		return "", nil, 0, fmt.Errorf("scram: incomplete server-first %q", msg)
	}
	return nonce, salt, iter, nil
}

// saslName applies the comma/equals escaping SCRAM requires for the
// username (RFC 5802 §5.1). Our test usernames contain neither, but keep
// it correct.
func saslName(s string) string {
	s = strings.ReplaceAll(s, "=", "=3D")
	s = strings.ReplaceAll(s, ",", "=2C")
	return s
}
