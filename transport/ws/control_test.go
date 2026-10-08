// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package ws

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	gws "github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

// startWS runs serve on the server side of each upgraded connection and
// returns a connected client.
func startWS(t *testing.T, serve func(conn net.Conn)) *Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := gws.UpgradeHTTP(r, w)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		serve(conn)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(strings.Replace(srv.URL, "http://", "ws://", 1))
	c, err := Dial(context.Background(), u, DialOpts{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	return c.(*Conn)
}

// readClientFrame reads one frame the client sent and unmasks it.
func readClientFrame(conn net.Conn) (gws.Header, []byte, error) {
	h, err := gws.ReadHeader(conn)
	if err != nil {
		return h, nil, err
	}
	payload := make([]byte, h.Length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return h, nil, err
	}
	if h.Masked {
		gws.Cipher(payload, h.Mask, 0)
	}
	return h, payload, nil
}

// RFC 6455 §5.5.2: a Ping between messages gets a Pong carrying its
// payload, and reading carries on.
func TestPingIsAnswered(t *testing.T) {
	pong := make(chan []byte, 1)
	c := startWS(t, func(conn net.Conn) {
		_ = wsutil.WriteServerMessage(conn, gws.OpPing, []byte("hb"))
		_ = wsutil.WriteServerBinary(conn, []byte("data"))
		h, payload, err := readClientFrame(conn)
		if err != nil || h.OpCode != gws.OpPong {
			t.Errorf("client answered the ping with %v (%v)", h.OpCode, err)
		}
		pong <- payload
		_, _, _ = readClientFrame(conn)
	})
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "data" {
		t.Fatalf("read %q, %v", got, err)
	}
	select {
	case p := <-pong:
		if string(p) != "hb" {
			t.Fatalf("pong payload %q, want %q", p, "hb")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no pong")
	}
}

// [MQTT-6.0.0-1]: MQTT travels only in binary frames.
func TestTextFrameIsAnError(t *testing.T) {
	c := startWS(t, func(conn net.Conn) {
		_ = wsutil.WriteServerText(conn, []byte("{}"))
		_, _, _ = readClientFrame(conn)
	})
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, ErrTextFrame) {
		t.Fatalf("Read after a text frame: %v", err)
	}
}

// A Close from the broker is answered with Close and ends the stream.
func TestBrokerCloseEndsRead(t *testing.T) {
	reply := make(chan gws.OpCode, 1)
	c := startWS(t, func(conn net.Conn) {
		_ = wsutil.WriteServerMessage(conn, gws.OpClose, gws.NewCloseFrameBody(gws.StatusNormalClosure, ""))
		h, _, _ := readClientFrame(conn)
		reply <- h.OpCode
	})
	if _, err := c.Read(make([]byte, 8)); !errors.Is(err, io.EOF) {
		t.Fatalf("Read after Close: %v", err)
	}
	if op := <-reply; op != gws.OpClose {
		t.Fatalf("client answered Close with %v", op)
	}
}

// Pongs sent while Read handles pings never land inside a data frame
// another goroutine is writing.
func TestPongsKeepFramesWhole(t *testing.T) {
	const n = 500
	payload := func(i int) []byte {
		p := bytes.Repeat([]byte{byte(i)}, 1000)
		binary.BigEndian.PutUint32(p, uint32(i))
		return p
	}
	result := make(chan error, 1)
	c := startWS(t, func(conn net.Conn) {
		go func() {
			for i := range n {
				if wsutil.WriteServerMessage(conn, gws.OpPing, []byte{byte(i)}) != nil {
					return
				}
			}
		}()
		data, pongs := 0, 0
		for data < n || pongs < n {
			h, p, err := readClientFrame(conn)
			if err != nil {
				result <- fmt.Errorf("after %d data frames and %d pongs: %w", data, pongs, err)
				return
			}
			switch h.OpCode {
			case gws.OpBinary:
				if !bytes.Equal(p, payload(data)) {
					result <- fmt.Errorf("data frame %d corrupted", data)
					return
				}
				data++
			case gws.OpPong:
				pongs++
			default:
				result <- fmt.Errorf("unexpected frame %v", h.OpCode)
				return
			}
		}
		result <- nil
	})
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()
	for i := range n {
		if _, err := c.Write(payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not receive every frame")
	}
}

// Close never waits behind a write stalled on a peer that stopped
// reading: it closes the socket, which ends the write.
func TestCloseEndsAStalledWrite(t *testing.T) {
	release := make(chan struct{})
	c := startWS(t, func(net.Conn) { <-release })
	defer close(release)
	_ = c.SetDeadline(time.Time{})
	// Larger than the socket buffers of a peer that never read, so the
	// write blocks.
	packet := make([]byte, 8<<20)
	wrote := make(chan error, 1)
	go func() {
		_, err := c.Write(packet)
		wrote <- err
	}()
	// Wait until the write holds the lock, blocked on the socket. Masking
	// the frame first takes a while under the race detector.
	for deadline := time.Now().Add(30 * time.Second); c.wmu.TryLock(); time.Sleep(time.Millisecond) {
		c.wmu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the write never started")
		}
	}
	closed := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked behind the stalled write")
	}
	select {
	case err := <-wrote:
		if err == nil {
			t.Fatal("the stalled write succeeded after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stalled write did not end with the connection")
	}
}

// An idle connection says goodbye with a Close frame.
func TestCloseSendsCloseFrame(t *testing.T) {
	got := make(chan gws.OpCode, 1)
	c := startWS(t, func(conn net.Conn) {
		h, _, err := readClientFrame(conn)
		if err == nil {
			got <- h.OpCode
		}
	})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case op := <-got:
		if op != gws.OpClose {
			t.Fatalf("frame %v, want Close", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no Close frame")
	}
}
