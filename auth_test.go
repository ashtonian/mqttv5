// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// echoAuth is a trivial Authenticator that demonstrates the protocol:
// initial data is "hello"; on every broker challenge "ping" it replies
// "pong". The fakeBroker drives one challenge then sends CONNACK.
type echoAuth struct {
	steps atomic.Int32
}

func (*echoAuth) Method() string                        { return "ECHO" }
func (*echoAuth) Begin(context.Context) ([]byte, error) { return []byte("hello"), nil }
func (e *echoAuth) Continue(brokerData []byte) ([]byte, bool, error) {
	e.steps.Add(1)
	if !bytes.Equal(brokerData, []byte("ping")) {
		return nil, false, errors.New("unexpected challenge: " + string(brokerData))
	}
	return []byte("pong"), false, nil
}

func TestConnect_MultiStepAUTH(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)

		// 1. Receive CONNECT with AuthenticationMethod + initial data.
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("read CONNECT: %v", err)
			return
		}
		conn, ok := pkt.(*wire.Connect)
		if !ok {
			t.Errorf("expected CONNECT, got %s", pkt.Type())
			pkt.Release()
			return
		}
		method, hasMethod := conn.Properties.String(wire.PropAuthMethod)
		initial, hasInitial := conn.Properties.Binary(wire.PropAuthData)
		pkt.Release()
		if !hasMethod || method != "ECHO" {
			t.Errorf("CONNECT method = %q (has=%v), want ECHO", method, hasMethod)
		}
		if !hasInitial || !bytes.Equal(initial, []byte("hello")) {
			t.Errorf("CONNECT initial auth data = %q (has=%v), want hello", initial, hasInitial)
		}

		// 2. Send broker AUTH challenge.
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("ping"),
		})

		// 3. Expect client AUTH response.
		pkt, err = dec.ReadPacket()
		if err != nil {
			t.Errorf("read client AUTH: %v", err)
			return
		}
		auth, ok := pkt.(*wire.Auth)
		if !ok {
			t.Errorf("expected AUTH, got %s", pkt.Type())
			pkt.Release()
			return
		}
		respData, _ := auth.Properties.Binary(wire.PropAuthData)
		pkt.Release()
		if !bytes.Equal(respData, []byte("pong")) {
			t.Errorf("client AUTH response = %q, want pong", respData)
		}

		// 4. Final CONNACK with success.
		_, _ = wire.WriteConnack(c, wire.ConnackOpts{
			ReasonCode: wire.ReasonSuccess,
		})
		<-fb.Done()
	})

	auth := &echoAuth{}
	cli, err := New(
		WithBroker(fb.URL()),
		WithAuthenticator(auth),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if !cli.Connected() {
		t.Fatal("Connected() = false after multi-step AUTH handshake")
	}
	if got := auth.steps.Load(); got != 1 {
		t.Errorf("Authenticator.Continue calls = %d, want 1", got)
	}
}

func TestConnect_AUTHWithoutAuthenticator(t *testing.T) {
	// Broker sends AUTH but the client didn't configure an
	// Authenticator — connectOnce must abort with a clear error
	// rather than silently hanging on more reads.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		if pkt, err := dec.ReadPacket(); err == nil {
			pkt.Release()
		}
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: "SOMETHING",
		})
		<-fb.Done()
	})

	cli, _ := New(WithBroker(fb.URL()))
	err := cli.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail with no Authenticator")
	}
	if cli.Connected() {
		t.Error("Connected() = true after auth failure")
	}
}

// TestMidSessionReAuth exercises the client's response to a mid-session
// AUTH challenge (MQTT v5 §4.12). The broker accepts CONNECT, sends
// CONNACK, then sends an AUTH the client must route to
// Authenticator.Continue, replying 0x18 Continue without dropping the
// connection.
//
// Note: a conformant broker cannot initiate re-auth — 0x19 is
// client-only (§3.15.2.1) — so the broker sending 0x19 here simulates a
// non-conformant prompt that handleServerAuth tolerates defensively.
// Either way the client's reply must be 0x18 Continue, never 0x00.
func TestMidSessionReAuth(t *testing.T) {
	respCh := make(chan []byte, 1)
	reasonCh := make(chan wire.ReasonCode, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// Trigger mid-session re-auth.
		if _, err := wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonReAuthenticate,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("ping"),
		}); err != nil {
			t.Errorf("broker write AUTH: %v", err)
			return
		}

		// Read the client's AUTH response.
		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("read client AUTH: %v", err)
			return
		}
		auth, ok := pkt.(*wire.Auth)
		if !ok {
			t.Errorf("expected AUTH, got %s", pkt.Type())
			pkt.Release()
			return
		}
		data, _ := auth.Properties.Binary(wire.PropAuthData)
		dataCopy := append([]byte(nil), data...)
		reason := auth.ReasonCode
		pkt.Release()
		respCh <- dataCopy
		reasonCh <- reason
		<-fb.Done()
	})

	auth := &echoAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case resp := <-respCh:
		if !bytes.Equal(resp, []byte("pong")) {
			t.Fatalf("client AUTH response = %q, want pong", resp)
		}
		if rc := <-reasonCh; rc != wire.ReasonContinueAuthentication {
			t.Fatalf("client reply reason = 0x%02X, want 0x18 Continue (never 0x00)", byte(rc))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not respond to mid-session AUTH within 2s")
	}

	if !cli.Connected() {
		t.Fatal("Connected() = false after mid-session re-auth")
	}
}

// TestMidSessionReAuthWithoutAuthenticator exercises the failure path:
// broker sends AUTH mid-session but client has no Authenticator. The
// client must emit DISCONNECT 0x87 (Not authorized) and the
// supervisor reconnects through the regular CONNECT path.
func TestMidSessionReAuthWithoutAuthenticator(t *testing.T) {
	disconnReceived := make(chan wire.ReasonCode, 1)
	var connects atomic.Int32
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		round := connects.Add(1)
		if round == 1 {
			// First connection: send mid-session AUTH, expect
			// DISCONNECT, then the connection will drop.
			if _, err := wire.WriteAuth(c, wire.AuthOpts{
				ReasonCode:           wire.ReasonReAuthenticate,
				AuthenticationMethod: "X",
			}); err != nil {
				return
			}
			pkt, err := dec.ReadPacket()
			if err != nil {
				return
			}
			d, ok := pkt.(*wire.Disconnect)
			if ok {
				disconnReceived <- d.ReasonCode
			}
			pkt.Release()
			return
		}
		<-fb.Done()
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithReconnectBackoff(ConstantBackoff(50*time.Millisecond)),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case rc := <-disconnReceived:
		if rc != wire.ReasonNotAuthorized {
			t.Fatalf("DISCONNECT reason = 0x%02X, want ReasonNotAuthorized", byte(rc))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no DISCONNECT received within 2s")
	}
}

// doneTrueAuth is an Authenticator whose Continue reports done=true. It
// guards the regression where a done=true return made the client emit a
// spec-violating AUTH 0x00 instead of 0x18 Continue (§3.15.2.1 — only the
// server concludes the exchange).
type doneTrueAuth struct {
	steps atomic.Int32
}

func (*doneTrueAuth) Method() string                        { return "ECHO" }
func (*doneTrueAuth) Begin(context.Context) ([]byte, error) { return []byte("hello"), nil }
func (a *doneTrueAuth) Continue(brokerData []byte) ([]byte, bool, error) {
	a.steps.Add(1)
	return []byte("pong"), true, nil // done=true — must NOT become AUTH 0x00
}

// TestMidSessionReAuth_DoneTrueRepliesContinue asserts the client replies
// 0x18 Continue (not 0x00 Success) to a mid-session challenge even when
// Authenticator.Continue returns done=true. Fails against the pre-fix
// handler, which mapped done=true to a client-sent 0x00.
func TestMidSessionReAuth_DoneTrueRepliesContinue(t *testing.T) {
	reasonCh := make(chan wire.ReasonCode, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		if _, err := wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("ping"),
		}); err != nil {
			t.Errorf("broker write AUTH: %v", err)
			return
		}

		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("read client AUTH: %v", err)
			return
		}
		auth, ok := pkt.(*wire.Auth)
		if !ok {
			t.Errorf("expected AUTH, got %s", pkt.Type())
			pkt.Release()
			return
		}
		reason := auth.ReasonCode
		pkt.Release()
		reasonCh <- reason
		<-fb.Done()
	})

	auth := &doneTrueAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case rc := <-reasonCh:
		if rc != wire.ReasonContinueAuthentication {
			t.Fatalf("client reply reason = 0x%02X, want 0x18 Continue (client must never send 0x00)", byte(rc))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not respond to mid-session AUTH within 2s")
	}
	if got := auth.steps.Load(); got != 1 {
		t.Errorf("Authenticator.Continue calls = %d, want 1", got)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after mid-session re-auth")
	}
}

// TestMidSessionReAuth_BrokerSuccessIsTerminal asserts that an inbound
// AUTH 0x00 Success concludes the exchange: the client neither replies
// nor re-enters the Authenticator, and the connection stays up. Fails
// against the pre-fix handler, which fed the 0x00 into Continue (with
// echoAuth that errors on non-"ping" data, forcing a DISCONNECT).
func TestMidSessionReAuth_BrokerSuccessIsTerminal(t *testing.T) {
	gotReply := make(chan string, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		// Conclude a (would-be) re-auth with 0x00 Success.
		if _, err := wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonSuccess,
			AuthenticationMethod: "ECHO",
		}); err != nil {
			t.Errorf("broker write AUTH: %v", err)
			return
		}

		// The client must send nothing back. Read with a short deadline
		// and expect a timeout; any packet (a spurious AUTH, or a
		// DISCONNECT from a failed Continue) is the bug.
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		pkt, err := dec.ReadPacket()
		switch {
		case err == nil:
			gotReply <- pkt.Type().String()
			pkt.Release()
		case isTimeout(err):
			gotReply <- "" // good: silence
		default:
			gotReply <- "err:" + err.Error()
		}
		<-fb.Done()
	})

	var reauthed atomic.Int32
	auth := &echoAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth),
		WithOnReauthenticated(func() { reauthed.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	select {
	case got := <-gotReply:
		if got != "" {
			t.Fatalf("client reacted to terminal AUTH 0x00 with %q, want silence", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker read did not settle within 2s")
	}
	if got := auth.steps.Load(); got != 0 {
		t.Errorf("Authenticator.Continue called %d times on terminal 0x00, want 0", got)
	}
	if !cli.Connected() {
		t.Fatal("Connected() = false after terminal AUTH 0x00 (connection should stay up)")
	}
	if got := reauthed.Load(); got != 1 {
		t.Errorf("OnReauthenticated fired %d times on broker-driven 0x00, want 1", got)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// countingAuth is the Authenticator used by the Reauthenticate tests.
// Each Begin returns a fresh, incrementing token so the broker can
// observe a refreshed credential on the wire; Continue echoes "pong" to a
// "ping" challenge. begins/continues count invocations. When beginGate is
// non-nil, every Begin AFTER the first (the CONNECT one) blocks on it,
// letting a test hold the single-flight slot open deterministically.
type countingAuth struct {
	begins    atomic.Int32
	continues atomic.Int32
	beginGate chan struct{}
}

func (*countingAuth) Method() string { return "ECHO" }

func (a *countingAuth) Begin(ctx context.Context) ([]byte, error) {
	n := a.begins.Add(1)
	if n > 1 && a.beginGate != nil {
		select {
		case <-a.beginGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return fmt.Appendf(nil, "token-%d", n), nil
}

func (a *countingAuth) Continue(brokerData []byte) ([]byte, bool, error) {
	a.continues.Add(1)
	if !bytes.Equal(brokerData, []byte("ping")) {
		return nil, false, errors.New("unexpected challenge: " + string(brokerData))
	}
	return []byte("pong"), false, nil
}

// readAuth reads one packet expected to be an AUTH and returns its reason
// code, method, and a copy of its auth data — all detached from the frame
// buffer so they are safe to use after Release / across a channel. On a
// read error or wrong type it t.Errors and returns zero values.
func readAuth(t *testing.T, dec *wire.Decoder) (wire.ReasonCode, string, []byte) {
	t.Helper()
	pkt, err := dec.ReadPacket()
	if err != nil {
		t.Errorf("read AUTH: %v", err)
		return 0, "", nil
	}
	auth, ok := pkt.(*wire.Auth)
	if !ok {
		t.Errorf("expected AUTH, got %s", pkt.Type())
		pkt.Release()
		return 0, "", nil
	}
	rc := auth.ReasonCode
	m, _ := auth.Properties.String(wire.PropAuthMethod)
	d, _ := auth.Properties.Binary(wire.PropAuthData)
	method := string([]byte(m))
	data := append([]byte(nil), d...)
	pkt.Release()
	return rc, method, data
}

// TestReauthenticate_HappyPath_SingleStep: a single 0x19 → 0x00 Success
// exchange. The broker observes a fresh credential and the client stays up.
func TestReauthenticate_HappyPath_SingleStep(t *testing.T) {
	type seen struct {
		reason wire.ReasonCode
		method string
		data   []byte
	}
	seenCh := make(chan seen, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		rc, method, data := readAuth(t, dec)
		seenCh <- seen{rc, method, data}
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonSuccess,
			AuthenticationMethod: "ECHO",
		})
		<-fb.Done()
	})

	auth := &countingAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("Reauthenticate: %v", err)
	}

	select {
	case s := <-seenCh:
		if s.reason != wire.ReasonReAuthenticate {
			t.Errorf("initiating AUTH reason = 0x%02X, want 0x19 Re-authentication", byte(s.reason))
		}
		if s.method != "ECHO" {
			t.Errorf("0x19 method = %q, want ECHO", s.method)
		}
		if !bytes.Equal(s.data, []byte("token-2")) {
			t.Errorf("0x19 data = %q, want token-2 (fresh Begin, distinct from CONNECT's token-1)", s.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker did not observe AUTH 0x19")
	}
	if !cli.Connected() {
		t.Error("Connected() = false after successful re-auth")
	}
	if got := auth.begins.Load(); got != 2 {
		t.Errorf("Begin calls = %d, want 2 (CONNECT + re-auth)", got)
	}
}

// TestReauthenticate_MultiStep: 0x19 → broker 0x18 ping → client 0x18 pong
// → broker 0x00. Continue runs exactly once.
func TestReauthenticate_MultiStep(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		if rc, _, _ := readAuth(t, dec); rc != wire.ReasonReAuthenticate {
			t.Errorf("initiating AUTH reason = 0x%02X, want 0x19", byte(rc))
		}
		if _, err := wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("ping"),
		}); err != nil {
			t.Errorf("broker write 0x18: %v", err)
			return
		}

		rc, _, data := readAuth(t, dec)
		if rc != wire.ReasonContinueAuthentication || !bytes.Equal(data, []byte("pong")) {
			t.Errorf("client continue = reason 0x%02X data %q, want 0x18 pong", byte(rc), data)
		}
		_, _ = wire.WriteAuth(c, wire.AuthOpts{ReasonCode: wire.ReasonSuccess, AuthenticationMethod: "ECHO"})
		<-fb.Done()
	})

	auth := &countingAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("Reauthenticate: %v", err)
	}
	if got := auth.continues.Load(); got != 1 {
		t.Errorf("Continue calls = %d, want 1", got)
	}
	if !cli.Connected() {
		t.Error("Connected() = false after multi-step re-auth")
	}
}

// TestReauthenticate_BrokerRejects: the broker answers the 0x19 with a
// DISCONNECT 0x87. The call returns ErrReauthRejected with the reason, the
// connection drops, and the supervisor reconnects.
func TestReauthenticate_BrokerRejects(t *testing.T) {
	var connects atomic.Int32
	reconnected := make(chan struct{}, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		if connects.Add(1) == 1 {
			if rc, _, _ := readAuth(t, dec); rc != wire.ReasonReAuthenticate {
				t.Errorf("initiating AUTH reason = 0x%02X, want 0x19", byte(rc))
			}
			_, _ = wire.WriteDisconnect(c, wire.DisconnectOpts{ReasonCode: wire.ReasonNotAuthorized})
			return
		}
		select {
		case reconnected <- struct{}{}:
		default:
		}
		<-fb.Done()
	})

	auth := &countingAuth{}
	cli, err := New(
		WithBroker(fb.URL()),
		WithAuthenticator(auth),
		WithReconnectBackoff(ConstantBackoff(50*time.Millisecond)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	err = cli.Reauthenticate(context.Background())
	if !errors.Is(err, ErrReauthRejected) {
		t.Fatalf("Reauthenticate err = %v, want ErrReauthRejected", err)
	}
	if !strings.Contains(err.Error(), "0x87") {
		t.Errorf("Reauthenticate err = %q, want it to surface reason 0x87", err.Error())
	}
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Error("supervisor did not reconnect after re-auth rejection")
	}
}

// TestReauthenticate_NoAuthenticator: without WithAuthenticator the call
// fails fast and nothing reaches the wire.
func TestReauthenticate_NoAuthenticator(t *testing.T) {
	sawAuth := make(chan string, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		pkt, err := dec.ReadPacket()
		switch {
		case err == nil:
			sawAuth <- pkt.Type().String()
			pkt.Release()
		case isTimeout(err):
			sawAuth <- ""
		default:
			sawAuth <- "err:" + err.Error()
		}
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL())) // no Authenticator
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); !errors.Is(err, ErrNoAuthenticator) {
		t.Fatalf("Reauthenticate err = %v, want ErrNoAuthenticator", err)
	}
	select {
	case got := <-sawAuth:
		if got != "" {
			t.Errorf("broker saw %q on the wire, want nothing", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker read did not settle")
	}
}

// TestReauthenticate_NotConnected: before the first Connect and after
// Disconnect, the call returns ErrNotConnected.
func TestReauthenticate_NotConnected(t *testing.T) {
	// Before Connect — nothing is dialled.
	cli, err := New(WithBroker("mqtt://127.0.0.1:1"), WithAuthenticator(&countingAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Reauthenticate(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("before Connect: err = %v, want ErrNotConnected", err)
	}

	// After Disconnect — c.cur is cleared.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})
	cli2, err := New(WithBroker(fb.URL()), WithAuthenticator(&countingAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli2.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := cli2.Disconnect(context.Background()); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if err := cli2.Reauthenticate(context.Background()); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("after Disconnect: err = %v, want ErrNotConnected", err)
	}
}

// TestReauthenticate_ContextDeadline: the broker reads the 0x19 but never
// concludes. The call returns a deadline error within the bound, the
// connection stays up, and the slot is retained (a follow-up call is
// single-flighted out — locking in the B1 ctx-cancel semantics).
func TestReauthenticate_ContextDeadline(t *testing.T) {
	got19 := make(chan struct{}, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		if rc, _, _ := readAuth(t, dec); rc == wire.ReasonReAuthenticate {
			got19 <- struct{}{}
		}
		<-fb.Done() // never reply
	})

	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(&countingAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = cli.Reauthenticate(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reauthenticate err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Reauthenticate blocked %v past its deadline, want ~200ms", elapsed)
	}
	if !cli.Connected() {
		t.Error("Connected() = false after ctx-cancelled re-auth (should stay up)")
	}
	select {
	case <-got19:
	case <-time.After(time.Second):
		t.Error("broker never saw the AUTH 0x19")
	}
	if err := cli.Reauthenticate(context.Background()); !errors.Is(err, ErrReauthInProgress) {
		t.Errorf("follow-up Reauthenticate err = %v, want ErrReauthInProgress (slot retained after ctx cancel)", err)
	}
}

// TestReauthenticate_ConcurrentSingleFlight: two concurrent callers; one
// drives the exchange, the other gets ErrReauthInProgress, and the broker
// sees a single 0x19.
func TestReauthenticate_ConcurrentSingleFlight(t *testing.T) {
	auth := &countingAuth{beginGate: make(chan struct{})}
	auths := make(chan wire.ReasonCode, 4)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)

		rc, _, _ := readAuth(t, dec)
		auths <- rc
		_, _ = wire.WriteAuth(c, wire.AuthOpts{ReasonCode: wire.ReasonSuccess, AuthenticationMethod: "ECHO"})

		// Guard: no second 0x19 should appear.
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if pkt, err := dec.ReadPacket(); err == nil {
			if a, ok := pkt.(*wire.Auth); ok {
				auths <- a.ReasonCode
			}
			pkt.Release()
		}
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	results := make(chan error, 2)
	for range 2 {
		go func() { results <- cli.Reauthenticate(context.Background()) }()
	}
	// Let the loser CAS-fail while the winner is parked in Begin, then
	// release the winner.
	time.Sleep(100 * time.Millisecond)
	close(auth.beginGate)

	var nNil, nInProgress int
	for range 2 {
		select {
		case err := <-results:
			switch {
			case err == nil:
				nNil++
			case errors.Is(err, ErrReauthInProgress):
				nInProgress++
			default:
				t.Errorf("unexpected Reauthenticate err: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Reauthenticate did not return")
		}
	}
	if nNil != 1 || nInProgress != 1 {
		t.Errorf("results: nil=%d inProgress=%d, want exactly one each", nNil, nInProgress)
	}

	select {
	case rc := <-auths:
		if rc != wire.ReasonReAuthenticate {
			t.Errorf("broker AUTH reason = 0x%02X, want 0x19", byte(rc))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker saw no 0x19")
	}
	select {
	case rc := <-auths:
		t.Errorf("broker saw a second AUTH (0x%02X), want only one 0x19", byte(rc))
	case <-time.After(400 * time.Millisecond):
	}
}

// TestReauthenticate_MethodConsistency: the same AuthenticationMethod is
// carried on CONNECT, the 0x19, and the client's 0x18 continue.
func TestReauthenticate_MethodConsistency(t *testing.T) {
	methods := make(chan string, 3)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)

		pkt, err := dec.ReadPacket()
		if err != nil {
			t.Errorf("read CONNECT: %v", err)
			return
		}
		if conn, ok := pkt.(*wire.Connect); ok {
			m, _ := conn.Properties.String(wire.PropAuthMethod)
			methods <- string([]byte(m))
		} else {
			t.Errorf("expected CONNECT, got %s", pkt.Type())
		}
		pkt.Release()
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{ReasonCode: wire.ReasonSuccess}); err != nil {
			t.Errorf("CONNACK: %v", err)
			return
		}

		_, m19, _ := readAuth(t, dec)
		methods <- m19
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("ping"),
		})

		_, m18, _ := readAuth(t, dec)
		methods <- m18
		_, _ = wire.WriteAuth(c, wire.AuthOpts{ReasonCode: wire.ReasonSuccess, AuthenticationMethod: "ECHO"})
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(&countingAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())
	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("Reauthenticate: %v", err)
	}

	labels := []string{"CONNECT", "0x19", "client 0x18"}
	for i := range 3 {
		select {
		case m := <-methods:
			if m != "ECHO" {
				t.Errorf("%s method = %q, want ECHO (consistent across the exchange)", labels[i], m)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("missing %s method capture", labels[i])
		}
	}
}

// TestReauthenticate_PresentsFreshData: two successive re-auths present
// distinct, freshly-resolved credentials (the JWT-refresh case).
func TestReauthenticate_PresentsFreshData(t *testing.T) {
	dataCh := make(chan []byte, 2)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		for range 2 {
			_, _, data := readAuth(t, dec)
			dataCh <- data
			_, _ = wire.WriteAuth(c, wire.AuthOpts{ReasonCode: wire.ReasonSuccess, AuthenticationMethod: "ECHO"})
		}
		<-fb.Done()
	})

	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(&countingAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("re-auth 1: %v", err)
	}
	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("re-auth 2: %v", err)
	}

	var first, second []byte
	select {
	case first = <-dataCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no 0x19 #1")
	}
	select {
	case second = <-dataCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no 0x19 #2")
	}
	if bytes.Equal(first, second) {
		t.Errorf("both re-auths presented %q — Begin was not re-run for a fresh credential", first)
	}
	if !bytes.Equal(first, []byte("token-2")) || !bytes.Equal(second, []byte("token-3")) {
		t.Errorf("re-auth data = %q, %q; want token-2, token-3", first, second)
	}
}

// TestReauthenticate_FiresOnReauthenticated asserts the OnReauthenticated
// hook fires exactly once when a client-initiated re-auth concludes with a
// broker 0x00 Success.
func TestReauthenticate_FiresOnReauthenticated(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		readAuth(t, dec) // 0x19
		_, _ = wire.WriteAuth(c, wire.AuthOpts{ReasonCode: wire.ReasonSuccess, AuthenticationMethod: "ECHO"})
		<-fb.Done()
	})

	var reauthed atomic.Int32
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(&countingAuth{}),
		WithOnReauthenticated(func() { reauthed.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("Reauthenticate: %v", err)
	}
	// The hook fires on the read loop before Reauthenticate's result is
	// delivered, so it has run by the time the call returns.
	if got := reauthed.Load(); got != 1 {
		t.Errorf("OnReauthenticated fired %d times, want 1", got)
	}
}

// verifyingAuth is an Authenticator that also implements
// ServerFinalVerifier. It records the server-final data passed to it and
// can be told (after connect) to fail verification, exercising the
// mutual-auth teardown paths without a real SCRAM broker.
type verifyingAuth struct {
	mu       sync.Mutex
	finals   [][]byte
	failWith error
}

func (*verifyingAuth) Method() string                        { return "ECHO" }
func (*verifyingAuth) Begin(context.Context) ([]byte, error) { return []byte("hello"), nil }
func (*verifyingAuth) Continue([]byte) ([]byte, bool, error) { return []byte("pong"), false, nil }

func (a *verifyingAuth) VerifyServerFinal(data []byte) error {
	a.mu.Lock()
	a.finals = append(a.finals, append([]byte(nil), data...))
	fail := a.failWith
	a.mu.Unlock()
	return fail
}

func (a *verifyingAuth) setFail(err error) {
	a.mu.Lock()
	a.failWith = err
	a.mu.Unlock()
}

func (a *verifyingAuth) lastFinal() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.finals) == 0 {
		return nil
	}
	return a.finals[len(a.finals)-1]
}

// TestReauthenticate_ServerVerifyReceivesFinal asserts the broker's
// concluding AUTH 0x00 AuthenticationData is handed to
// ServerFinalVerifier.VerifyServerFinal.
func TestReauthenticate_ServerVerifyReceivesFinal(t *testing.T) {
	const serverFinal = "v=server-signature-xyz"
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		readAuth(t, dec) // 0x19
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonSuccess,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte(serverFinal),
		})
		<-fb.Done()
	})

	auth := &verifyingAuth{}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Reauthenticate(context.Background()); err != nil {
		t.Fatalf("Reauthenticate: %v", err)
	}
	if got := auth.lastFinal(); string(got) != serverFinal {
		t.Errorf("VerifyServerFinal received %q, want %q", got, serverFinal)
	}
}

// TestReauthenticate_ServerVerifyFailureTearsDown asserts that a server
// verification failure on re-auth surfaces the error to the caller and
// tears the connection down (the broker observes a DISCONNECT).
func TestReauthenticate_ServerVerifyFailureTearsDown(t *testing.T) {
	errBadServer := errors.New("bad server signature")
	gotDisconnect := make(chan bool, 1)
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		readAuth(t, dec) // 0x19
		_, _ = wire.WriteAuth(c, wire.AuthOpts{
			ReasonCode:           wire.ReasonSuccess,
			AuthenticationMethod: "ECHO",
			AuthenticationData:   []byte("v=wrong"),
		})
		// The client must reject and DISCONNECT.
		pkt, err := dec.ReadPacket()
		if err == nil {
			_, isDisc := pkt.(*wire.Disconnect)
			gotDisconnect <- isDisc
			pkt.Release()
		} else {
			gotDisconnect <- false
		}
		<-fb.Done()
	})

	auth := &verifyingAuth{}
	cli, err := New(
		WithBroker(fb.URL()),
		WithAuthenticator(auth),
		// Long backoff: keep the post-teardown reconnect out of the test.
		WithReconnectBackoff(ConstantBackoff(30*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer cli.Disconnect(context.Background())

	auth.setFail(errBadServer) // fail the upcoming re-auth verification
	err = cli.Reauthenticate(context.Background())
	if !errors.Is(err, errBadServer) {
		t.Fatalf("Reauthenticate err = %v, want it to wrap the verification failure", err)
	}
	select {
	case isDisc := <-gotDisconnect:
		if !isDisc {
			t.Error("client did not DISCONNECT after server-verification failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broker saw no packet after the bad 0x00")
	}
}

// TestConnect_ServerVerifyFailure asserts that a server verification
// failure on the initial CONNACK aborts Connect.
func TestConnect_ServerVerifyFailure(t *testing.T) {
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	auth := &verifyingAuth{failWith: errors.New("bad server signature")}
	cli, err := New(WithBroker(fb.URL()), WithAuthenticator(auth))
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.Connect(context.Background()); err == nil {
		t.Fatal("Connect succeeded despite server-verification failure")
	}
	if cli.Connected() {
		t.Error("Connected() = true after a failed connect")
	}
}
