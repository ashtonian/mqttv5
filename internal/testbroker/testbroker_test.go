// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package testbroker

import (
	"bytes"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

func dial(t *testing.T, b *Broker) (net.Conn, *wire.Decoder) {
	t.Helper()
	nc, err := net.Dial("tcp", strings.TrimPrefix(b.URL(), "mqtt://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	return nc, wire.NewDecoder(nc)
}

func read(t *testing.T, dec *wire.Decoder, want wire.PacketType) wire.Packet {
	t.Helper()
	p, err := dec.ReadPacket()
	if err != nil {
		t.Fatalf("read %s: %v", want, err)
	}
	if p.Type() != want {
		t.Fatalf("got %s, want %s", p.Type(), want)
	}
	return p
}

func TestFallbackAutoResponder(t *testing.T) {
	b := New(t)
	nc, dec := dial(t, b)

	if _, err := wire.WriteConnect(nc, wire.ConnectOpts{ClientID: "c1", CleanStart: true, KeepAlive: 30}); err != nil {
		t.Fatal(err)
	}
	ack := read(t, dec, wire.CONNACK).(*wire.Connack)
	if ack.SessionPresent {
		t.Error("Session Present in reply to CleanStart=1 [MQTT-3.2.2-2]")
	}
	ack.Release()

	if _, err := wire.WriteSubscribe(nc, wire.SubscribeOpts{PacketID: 1, Filters: []wire.SubscribeFilter{{Topic: "a/#", QoS: 1}}}); err != nil {
		t.Fatal(err)
	}
	sa := read(t, dec, wire.SUBACK).(*wire.Suback)
	if sa.PacketID != 1 || len(sa.ReasonCodes) != 1 || sa.ReasonCodes[0] != wire.ReasonGrantedQoS1 {
		t.Errorf("SUBACK = id %d codes %v", sa.PacketID, sa.ReasonCodes)
	}
	sa.Release()

	if _, err := wire.WritePingreq(nc); err != nil {
		t.Fatal(err)
	}
	read(t, dec, wire.PINGRESP).Release()

	if _, err := wire.WritePublish(nc, wire.PublishOpts{Topic: "a/b", QoS: 1, PacketID: 7, Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	pa := read(t, dec, wire.PUBACK).(*wire.PubResp)
	if pa.PacketID != 7 {
		t.Errorf("PUBACK id = %d", pa.PacketID)
	}
	pa.Release()

	c := b.Conn(0, time.Second)
	if c == nil {
		t.Fatal("no broker conn")
	}
	if _, err := wire.WriteDisconnect(nc, wire.DisconnectOpts{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(c.Log()) < 5 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	var types []wire.PacketType
	for _, p := range c.Log() {
		types = append(types, p.Type)
	}
	want := []wire.PacketType{wire.CONNECT, wire.SUBSCRIBE, wire.PINGREQ, wire.PUBLISH, wire.DISCONNECT}
	if len(types) != len(want) {
		t.Fatalf("log = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("log = %v, want %v", types, want)
		}
	}
	if got := c.Log()[0].Connect; got == nil || got.ClientID != "c1" || !got.CleanStart {
		t.Errorf("CONNECT snapshot = %+v", got)
	}
}

func TestScriptsRunPerConnection(t *testing.T) {
	order := make(chan int, 2)
	b := New(t,
		func(c *Conn) { order <- 0; c.AcceptConnect(wire.ConnackOpts{}) },
		func(c *Conn) { order <- 1; c.AcceptConnect(wire.ConnackOpts{SessionPresent: true}); c.Hold(0) },
	)
	for i := 0; i < 2; i++ {
		nc, dec := dial(t, b)
		if _, err := wire.WriteConnect(nc, wire.ConnectOpts{ClientID: "c"}); err != nil {
			t.Fatal(err)
		}
		ack := read(t, dec, wire.CONNACK).(*wire.Connack)
		if ack.SessionPresent != (i == 1) {
			t.Errorf("conn %d SessionPresent = %v", i, ack.SessionPresent)
		}
		ack.Release()
		if got := <-order; got != i {
			t.Errorf("script %d ran for connection %d", got, i)
		}
	}
	if b.Accepted() != 2 {
		t.Errorf("accepted = %d", b.Accepted())
	}
}

func TestWithSubscriptionIDs(t *testing.T) {
	for _, opts := range []wire.PublishOpts{
		{Topic: "a/b", Payload: []byte("zero")},
		{Topic: "a/b", Payload: []byte("one"), QoS: 1, PacketID: 7, ContentType: "text/plain"},
	} {
		frame, err := wire.MarshalPublish(opts)
		if err != nil {
			t.Fatal(err)
		}
		pkt, err := wire.NewDecoder(bytes.NewReader(withSubscriptionIDs(frame, []uint32{5, 300}))).ReadPacket()
		if err != nil {
			t.Fatalf("QoS %d: %v", opts.QoS, err)
		}
		pub := pkt.(*wire.Publish)
		var ids []uint32
		for id := range pub.Properties.SubscriptionIdentifiers() {
			ids = append(ids, id)
		}
		ct, _ := pub.Properties.String(wire.PropContentType)
		if pub.Topic != opts.Topic || string(pub.Payload) != string(opts.Payload) || pub.PacketID != opts.PacketID ||
			ct != opts.ContentType || !slices.Equal(ids, []uint32{5, 300}) {
			t.Fatalf("QoS %d: decoded %+v ids %v content type %q", opts.QoS, pub, ids, ct)
		}
		pub.Release()
	}
}

// DropAll ends live connections but not the broker.
func TestDropAll(t *testing.T) {
	b := New(t)
	nc, dec := dial(t, b)
	if _, err := wire.WriteConnect(nc, wire.ConnectOpts{ClientID: "c"}); err != nil {
		t.Fatal(err)
	}
	read(t, dec, wire.CONNACK).Release()
	b.DropAll()
	if _, err := dec.ReadPacket(); err == nil {
		t.Fatal("connection still open after DropAll")
	}
	nc2, dec2 := dial(t, b)
	if _, err := wire.WriteConnect(nc2, wire.ConnectOpts{ClientID: "c"}); err != nil {
		t.Fatal(err)
	}
	read(t, dec2, wire.CONNACK).Release()
}

// The auto-responder keeps every session: CleanStart=0 resumes one.
func TestFallbackResumesSessions(t *testing.T) {
	b := New(t)
	nc, dec := dial(t, b)
	if _, err := wire.WriteConnect(nc, wire.ConnectOpts{ClientID: "c1", KeepAlive: 30}); err != nil {
		t.Fatal(err)
	}
	ack := read(t, dec, wire.CONNACK).(*wire.Connack)
	if !ack.SessionPresent {
		t.Error("no Session Present in reply to CleanStart=0")
	}
	ack.Release()
}
