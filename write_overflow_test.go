// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/ashtonian/mqttv5/wire"
)

func TestPublish_WriteDropNewestSurfacesErrWriteQueueFull(t *testing.T) {
	// fakeBroker accepts CONNECT + CONNACK, then never reads from
	// the socket. Kernel TCP send buffer fills, the writer goroutine
	// blocks in conn.Write, writeQueue stops draining. With
	// WriteDropNewest + a small WriteQueueSize, the next
	// Publish call must return ErrWriteQueueFull rather than block.
	fb := newFakeBroker(t, func(fb *fakeBroker, c net.Conn) {
		defer c.Close()
		dec := wire.NewDecoder(c)
		acceptConnect(t, c, dec)
		<-fb.Done()
	})

	cli, _ := New(
		WithBroker(fb.URL()),
		WithWriteQueueSize(1),
		WithWriteOverflowPolicy(WriteDropNewest),
	)
	if err := cli.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer cli.Disconnect(context.Background())

	// 16 KiB payload * 500 iter = 8 MiB pushed. Even a generously
	// autotuned TCP send buffer (typically 4 MiB max on linux/darwin)
	// fills before we exhaust the loop, so the writer ends up
	// blocked in conn.Write and writeQueue cannot drain.
	payload := make([]byte, 16*1024)
	opts := wire.PublishOpts{Topic: "x", QoS: 0, Payload: payload}

	var sawDrop bool
	for range 500 {
		if errors.Is(cli.Publish(context.Background(), opts), ErrWriteQueueFull) {
			sawDrop = true
			break
		}
	}
	if !sawDrop {
		t.Fatal("expected at least one ErrWriteQueueFull, got none")
	}
}

func TestWithWriteOverflowPolicy_RejectsInvalid(t *testing.T) {
	_, err := New(
		WithBroker("mqtt://127.0.0.1:1883"),
		WithWriteOverflowPolicy(WriteOverflowPolicy(99)),
	)
	if err == nil {
		t.Fatal("expected New to reject invalid WriteOverflowPolicy, got nil")
	}
}
