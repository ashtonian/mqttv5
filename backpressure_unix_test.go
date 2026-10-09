// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

//go:build unix

package mqttv5

import (
	"context"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// A broker that stops reading cannot stop the client reading: once the
// acknowledgements fill the socket, the read loop hands them to the
// writer goroutine, which waits, and carries on delivering. A Unix
// socket with a small send buffer fills after a few writes, where TCP's
// buffers grow to megabytes.
func TestFullSendBufferDoesNotStopReading(t *testing.T) {
	const n = 20000
	// Socket paths are limited to about 100 bytes; t.TempDir's is longer.
	dir, err := os.MkdirTemp("", "mqttv5")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "broker.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		dec := wire.NewDecoder(c)
		if p, err := dec.ReadPacket(); err == nil {
			p.Release()
		}
		if _, err := wire.WriteConnack(c, wire.ConnackOpts{}); err != nil {
			return
		}
		p, err := dec.ReadPacket()
		if err != nil {
			return
		}
		sub := p.(*wire.Subscribe)
		_, _ = wire.WriteSuback(c, wire.SubackOpts{PacketID: sub.PacketID, ReasonCodes: []wire.ReasonCode{1}})
		p.Release()
		// From here on the broker reads nothing.
		for i := range n {
			if _, err := wire.WritePublish(c, wire.PublishOpts{Topic: "t", QoS: 1, PacketID: uint16(i + 1), Payload: []byte{1}}); err != nil {
				return
			}
		}
		<-stop
	}()

	dial := func(ctx context.Context, _ *url.URL) (transport.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
		if err != nil {
			return nil, err
		}
		uc := c.(*net.UnixConn)
		if err := uc.SetWriteBuffer(4096); err != nil {
			return nil, err
		}
		return uc, nil
	}
	// The broker never reads the acknowledgements, so it sends more
	// unacknowledged messages than the default Receive Maximum allows.
	cli, err := New(WithBroker("mqtt://broker"), WithDialFunc(dial), WithClientID("backpressure"),
		WithReceiveMaximum(math.MaxUint16), WithoutKeepAlive(), WithLogger(quietLogger()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cli.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = cli.Disconnect(ctx)
	})
	var got atomic.Int64
	all := make(chan struct{})
	if _, err := cli.SubscribeCallback(ctx, []TopicFilter{{Topic: "t", QoS: 1}}, func(*Message) {
		if got.Add(1) == n {
			close(all)
		}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-all:
	case <-ctx.Done():
		t.Fatalf("delivered %d of %d messages while the broker read nothing", got.Load(), n)
	}
	// The case under test happened: acknowledgements were left waiting
	// for the writer goroutine.
	if cs := cli.cur.Load(); cs == nil || cs.queued.Load() == 0 {
		t.Fatal("the acknowledgements never filled the socket")
	}
}
