// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/internal/testbroker"
	"github.com/ashtonian/mqttv5/wire"
)

func writeAuth(c *testbroker.Conn, rc wire.ReasonCode, method string, data []byte) {
	_ = c.Write(func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{ReasonCode: rc, AuthenticationMethod: method, AuthenticationData: data})
	})
}

func expectDisconnect(c *testbroker.Conn, reason wire.ReasonCode) {
	if p := c.Expect(wire.DISCONNECT, 0); p.Reason != reason {
		c.T.Errorf("DISCONNECT reason %#x, want %#x", byte(p.Reason), byte(reason))
	}
}

// §4.12: only the client starts a re-authentication; an AUTH from
// the broker outside one is a protocol error.
func TestUnsolicitedAuthIsProtocolError(t *testing.T) {
	for _, tt := range []struct {
		name string
		auth Authenticator
		rc   wire.ReasonCode
	}{
		{"continue", &doneTrueAuth{}, wire.ReasonContinueAuthentication},
		{"success", &doneTrueAuth{}, wire.ReasonSuccess},
		{"no authenticator", nil, wire.ReasonContinueAuthentication},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.Await(wire.CONNECT, 0)
				_ = c.Write(func(w io.Writer) (int64, error) { return wire.WriteConnack(w, wire.ConnackOpts{}) })
				writeAuth(c, tt.rc, "ECHO", []byte("x"))
				expectDisconnect(c, wire.ReasonProtocolError)
			})
			var opts []Option
			if tt.auth != nil {
				opts = append(opts, WithAuthenticator(tt.auth))
			}
			tbClient(t, b, opts...)
			waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
		})
	}
}

// During CONNECT the broker may only send AUTH 0x18 under the
// CONNECT's method, and a CONNACK may not name another method.
func TestConnectAuthValidation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		auth   Authenticator
		broker func(c *testbroker.Conn)
	}{
		{"wrong method", &doneTrueAuth{}, func(c *testbroker.Conn) {
			writeAuth(c, wire.ReasonContinueAuthentication, "OTHER", []byte("x"))
		}},
		{"wrong reason", &doneTrueAuth{}, func(c *testbroker.Conn) {
			writeAuth(c, wire.ReasonReAuthenticate, "ECHO", []byte("x"))
		}},
		{"no authenticator", nil, func(c *testbroker.Conn) {
			writeAuth(c, wire.ReasonContinueAuthentication, "ECHO", []byte("x"))
		}},
		{"connack method differs", &doneTrueAuth{}, func(c *testbroker.Conn) {
			_ = c.Write(func(w io.Writer) (int64, error) {
				return wire.WriteConnack(w, wire.ConnackOpts{AuthenticationMethod: "OTHER"})
			})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := testbroker.New(t, func(c *testbroker.Conn) {
				c.Await(wire.CONNECT, 0)
				tt.broker(c)
				expectDisconnect(c, wire.ReasonProtocolError)
			})
			opts := []Option{WithBroker(b.URL()), WithClientID("auth-validation"), WithLogger(quietLogger())}
			if tt.auth != nil {
				opts = append(opts, WithAuthenticator(tt.auth))
			}
			cli, err := New(opts...)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var perr *ProtocolError
			if err := cli.Connect(ctx); !errors.As(err, &perr) {
				t.Fatalf("Connect = %v, want ProtocolError", err)
			}
			waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
		})
	}
}

// churnAuth overwrites pooled frame buffers inside Continue and records
// what it was given, so a challenge still aliasing a released frame shows
// up as corrupted.
type churnAuth struct{ seen chan string }

func (*churnAuth) Method() string                        { return "ECHO" }
func (*churnAuth) Begin(context.Context) ([]byte, error) { return []byte("hello"), nil }
func (a *churnAuth) Continue(data []byte) ([]byte, bool, error) {
	var held []*[]byte
	for i := 0; i < 1024; i++ {
		b, _ := wire.EncodePublish(wire.PublishOpts{Topic: strings.Repeat("Z", 128)})
		held = append(held, b)
	}
	a.seen <- string(data)
	for _, b := range held {
		wire.ReleaseBuf(b)
	}
	return []byte("response"), false, nil
}

// The CONNECT-time challenge reaches the Authenticator intact.
func TestConnectAuthChallengeCopiedBeforeRelease(t *testing.T) {
	a := &churnAuth{seen: make(chan string, 1)}
	b := testbroker.New(t, func(c *testbroker.Conn) {
		c.Await(wire.CONNECT, 0)
		writeAuth(c, wire.ReasonContinueAuthentication, "ECHO", []byte("secret-challenge"))
		c.Expect(wire.AUTH, 0)
		_ = c.Write(func(w io.Writer) (int64, error) {
			return wire.WriteConnack(w, wire.ConnackOpts{AuthenticationMethod: "ECHO"})
		})
		c.ServeAuto()
	})
	tbClient(t, b, WithAuthenticator(a))
	if got := <-a.seen; got != "secret-challenge" {
		t.Fatalf("Continue saw %q", got)
	}
}

// Within a client-initiated re-authentication the client always answers a
// challenge with 0x18 (done=true is only a hint), a 0x00 ends it, and a
// challenge under another method is a protocol error.
func TestReauthenticateExchange(t *testing.T) {
	connack := func(c *testbroker.Conn) {
		c.Await(wire.CONNECT, 0)
		_ = c.Write(func(w io.Writer) (int64, error) {
			return wire.WriteConnack(w, wire.ConnackOpts{AuthenticationMethod: "ECHO"})
		})
	}
	t.Run("continue then success", func(t *testing.T) {
		b := testbroker.New(t, func(c *testbroker.Conn) {
			connack(c)
			if p := c.Expect(wire.AUTH, 0); p.Reason != wire.ReasonReAuthenticate {
				c.T.Errorf("client opened with %#x", byte(p.Reason))
			}
			writeAuth(c, wire.ReasonContinueAuthentication, "ECHO", []byte("ping"))
			if p := c.Expect(wire.AUTH, 0); p.Reason != wire.ReasonContinueAuthentication {
				c.T.Errorf("client answered with %#x, want 0x18", byte(p.Reason))
			}
			writeAuth(c, wire.ReasonSuccess, "ECHO", nil)
			c.ServeAuto()
		})
		cli := tbClient(t, b, WithAuthenticator(&doneTrueAuth{}))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := cli.Reauthenticate(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("wrong method", func(t *testing.T) {
		b := testbroker.New(t, func(c *testbroker.Conn) {
			connack(c)
			c.Expect(wire.AUTH, 0)
			writeAuth(c, wire.ReasonContinueAuthentication, "OTHER", []byte("ping"))
			expectDisconnect(c, wire.ReasonProtocolError)
		})
		cli := tbClient(t, b, WithAuthenticator(&doneTrueAuth{}))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := cli.Reauthenticate(ctx); err == nil {
			t.Fatal("Reauthenticate succeeded after a protocol violation")
		}
		waitLog(t, b.Conn(0, time.Second), wire.DISCONNECT, 1)
	})
}
