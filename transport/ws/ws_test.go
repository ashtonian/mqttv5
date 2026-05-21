// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package ws

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// startEchoWS spins up an HTTP server that upgrades each connection
// to a WebSocket and echoes every binary frame it receives. The
// returned base URL has a ws:// scheme — callers append a path.
func startEchoWS(t *testing.T) (base *url.URL, cleanup func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				return
			}
			if op != gws.OpBinary {
				continue
			}
			if err := wsutil.WriteServerBinary(conn, msg); err != nil {
				return
			}
		}
	}))
	u, _ := url.Parse(strings.Replace(srv.URL, "http://", "ws://", 1))
	return u, srv.Close
}

func TestDialAndRoundTrip(t *testing.T) {
	base, cleanup := startEchoWS(t)
	defer cleanup()

	conn, err := Dial(context.Background(), base, DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	want := []byte("hello mqtt over ws")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The echo server replies with a single binary frame containing
	// the same bytes. Read until we have them all.
	got := make([]byte, len(want))
	n := 0
	deadline := time.Now().Add(2 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for n < len(want) {
		k, err := conn.Read(got[n:])
		if err != nil {
			t.Fatalf("Read after %d/%d bytes: %v", n, len(want), err)
		}
		n += k
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("roundtrip mismatch: got %q want %q", got, want)
	}
}

func TestWssRequiresTLSConfig(t *testing.T) {
	u, _ := url.Parse("wss://example.com/")
	_, err := Dial(context.Background(), u, DialOpts{})
	if err == nil {
		t.Fatal("expected ErrMissingTLSConfig, got nil")
	}
	if err != ErrMissingTLSConfig {
		t.Fatalf("got %v, want ErrMissingTLSConfig", err)
	}
}

func TestRejectUnsupportedScheme(t *testing.T) {
	u, _ := url.Parse("mqtt://example.com:1883")
	_, err := Dial(context.Background(), u, DialOpts{})
	if err == nil {
		t.Fatal("expected error for mqtt:// scheme")
	}
}

// TestMultipleSmallReads verifies that the transport.Conn.Read API
// works when the consumer reads field-at-a-time (one byte each
// call) — this mirrors the MQTT decoder's behaviour.
func TestMultipleSmallReads(t *testing.T) {
	base, cleanup := startEchoWS(t)
	defer cleanup()

	conn, err := Dial(context.Background(), base, DialOpts{})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("AB"), 50) // 100 bytes
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	one := make([]byte, 1)
	got := make([]byte, 0, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < len(payload); i++ {
		n, err := conn.Read(one)
		if err != nil {
			t.Fatalf("Read at byte %d: %v", i, err)
		}
		if n != 1 {
			t.Fatalf("Read got %d bytes at i=%d, want 1", n, i)
		}
		got = append(got, one[0])
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("byte-by-byte read mismatch")
	}
}
