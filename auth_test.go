// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"context"
	"errors"
	"net"
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

func (*echoAuth) Method() string         { return "ECHO" }
func (*echoAuth) Begin() ([]byte, error) { return []byte("hello"), nil }
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

// TestMidSessionReAuth exercises broker-initiated re-authentication
// after CONNACK (MQTT v5 §4.12). The broker accepts the initial
// CONNECT, sends CONNACK, then later sends an unsolicited AUTH
// challenge. The client must route to Authenticator.Continue and
// emit an AUTH response without dropping the connection.
func TestMidSessionReAuth(t *testing.T) {
	respCh := make(chan []byte, 1)
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
		pkt.Release()
		respCh <- dataCopy
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
