// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestAppendPubRespMatchesWriter(t *testing.T) {
	writers := map[PacketType]func(*bytes.Buffer, PubRespOpts) error{
		PUBACK:  func(b *bytes.Buffer, o PubRespOpts) error { _, err := WritePuback(b, o); return err },
		PUBREC:  func(b *bytes.Buffer, o PubRespOpts) error { _, err := WritePubrec(b, o); return err },
		PUBREL:  func(b *bytes.Buffer, o PubRespOpts) error { _, err := WritePubrel(b, o); return err },
		PUBCOMP: func(b *bytes.Buffer, o PubRespOpts) error { _, err := WritePubcomp(b, o); return err },
	}
	for typ, write := range writers {
		for _, rc := range reasonCodes[typ] {
			var want bytes.Buffer
			if err := write(&want, PubRespOpts{PacketID: 0xBEEF, ReasonCode: rc}); err != nil {
				t.Fatal(err)
			}
			got := AppendPubResp(nil, typ, 0xBEEF, rc)
			if !bytes.Equal(got, want.Bytes()) {
				t.Errorf("%s rc=%#x: AppendPubResp = % x, writer = % x", typ, byte(rc), got, want.Bytes())
			}
		}
	}
}

func TestAppendPubRespDoesNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 64)
	allocs := testing.AllocsPerRun(100, func() {
		buf = AppendPubResp(buf[:0], PUBACK, 7, ReasonSuccess)
	})
	if allocs != 0 {
		t.Fatalf("AppendPubResp allocates %v times", allocs)
	}
}

func TestMarshalPublishIsExactAndOwned(t *testing.T) {
	opts := PublishOpts{Topic: "a/b", Payload: []byte("hello"), QoS: 1, PacketID: 9}
	got, err := MarshalPublish(opts)
	if err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	if _, err := WritePublish(&want, opts); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) || cap(got) != len(got) {
		t.Fatalf("MarshalPublish = % x (cap %d), want % x", got, cap(got), want.Bytes())
	}
	// The result must not alias a pooled buffer that a later encode reuses.
	keep := bytes.Clone(got)
	for i := 0; i < 8; i++ {
		bp, _ := EncodePublish(PublishOpts{Topic: "zzz", Payload: bytes.Repeat([]byte{0xff}, 20)})
		ReleaseBuf(bp)
	}
	if !bytes.Equal(got, keep) {
		t.Fatal("MarshalPublish result was overwritten by a later encode")
	}
}

func TestWithMessageExpiry(t *testing.T) {
	mei := uint32(3600)
	tests := []struct {
		name string
		opts PublishOpts
	}{
		{"qos1 expiry only", PublishOpts{Topic: "a", Payload: []byte("p"), QoS: 1, PacketID: 3, MessageExpiryInterval: &mei}},
		{"qos2 expiry after other props", PublishOpts{Topic: "a/b/c", QoS: 2, PacketID: 9, ContentType: "text/plain",
			ResponseTopic: "r", MessageExpiryInterval: &mei, UserProperties: []UserProperty{{"k", "v"}}}},
		{"qos0", PublishOpts{Topic: "x", MessageExpiryInterval: &mei}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := MarshalPublish(tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			orig := bytes.Clone(frame)
			got := WithMessageExpiry(frame, 42)
			if !bytes.Equal(frame, orig) {
				t.Fatal("WithMessageExpiry modified its input")
			}
			p, err := NewDecoder(bytes.NewReader(got)).ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			defer p.Release()
			pub := p.(*Publish)
			if v, ok := pub.Properties.Uint32(PropMessageExpiryInterval); !ok || v != 42 {
				t.Fatalf("expiry = %d, %v; want 42", v, ok)
			}
			if pub.Topic != tt.opts.Topic || pub.PacketID != tt.opts.PacketID {
				t.Fatalf("rewritten frame decodes as %q id %d", pub.Topic, pub.PacketID)
			}
		})
	}
	t.Run("no expiry property", func(t *testing.T) {
		frame, _ := MarshalPublish(PublishOpts{Topic: "a", QoS: 1, PacketID: 1, ContentType: "x"})
		if got := WithMessageExpiry(frame, 5); !bytes.Equal(got, frame) {
			t.Fatal("frame without expiry changed")
		}
	})
	t.Run("garbage", func(t *testing.T) {
		for _, b := range [][]byte{nil, {0x30}, {0x32, 0x05, 0x00}, {0x20, 0x00}} {
			if got := WithMessageExpiry(b, 5); !bytes.Equal(got, b) {
				t.Fatalf("garbage % x changed to % x", b, got)
			}
		}
	})
}

func TestSubscriptionIdentifiers(t *testing.T) {
	// Two identifiers (one multi-byte) around a user property.
	raw := []byte{0x0B, 0x05, 0x26, 0, 1, 'k', 0, 1, 'v', 0x0B, 0x80, 0x01}
	var got []uint32
	for id := range PropertiesFromBytes(raw).SubscriptionIdentifiers() {
		got = append(got, id)
	}
	if len(got) != 2 || got[0] != 5 || got[1] != 128 {
		t.Fatalf("identifiers = %v, want [5 128]", got)
	}
	for range PropertiesFromBytes(nil).SubscriptionIdentifiers() {
		t.Fatal("identifier from empty properties")
	}
}

// The header AppendPublishHeader builds, followed by the payload, is the
// packet MarshalPublish encodes, including where the Remaining Length
// needs another byte.
func TestAppendPublishHeaderPlusPayloadIsThePacket(t *testing.T) {
	alias := uint16(3)
	mei := uint32(60)
	tests := []struct {
		name string
		opts PublishOpts
	}{
		{"qos0 empty payload", PublishOpts{Topic: "a"}},
		{"qos1", PublishOpts{Topic: "a/b", Payload: []byte("hello"), QoS: 1, PacketID: 9}},
		{"qos2 retained dup", PublishOpts{Topic: "a", Payload: []byte("x"), QoS: 2, PacketID: 1, Retain: true, Dup: true}},
		{"properties", PublishOpts{Topic: "t", Payload: []byte("p"), ContentType: "text/plain", ResponseTopic: "r",
			CorrelationData: []byte{1, 2}, MessageExpiryInterval: &mei, UserProperties: []UserProperty{{"k", "v"}}}},
		{"alias without topic", PublishOpts{TopicAlias: alias, Payload: []byte("p")}},
		{"remaining length 127", PublishOpts{Topic: "a", Payload: make([]byte, 124)}},
		{"remaining length 128", PublishOpts{Topic: "a", Payload: make([]byte, 125)}},
		{"remaining length 16384", PublishOpts{Topic: "a", Payload: make([]byte, 16381)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, err := MarshalPublish(tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			prefix := []byte("keep")
			got, err := AppendPublishHeader(bytes.Clone(prefix), tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(got, prefix) {
				t.Fatalf("dst prefix overwritten: % x", got[:len(prefix)])
			}
			got = append(got[len(prefix):], tt.opts.Payload...)
			if !bytes.Equal(got, want) {
				t.Fatalf("header+payload = % x\nwant            % x", got, want)
			}
		})
	}
}

func TestAppendPublishHeaderRejectsInvalidOptions(t *testing.T) {
	dst := []byte("keep")
	got, err := AppendPublishHeader(dst, PublishOpts{Topic: "a/#"})
	if !errors.Is(err, ErrInvalidTopic) {
		t.Fatalf("err = %v, want ErrInvalidTopic", err)
	}
	if string(got) != "keep" {
		t.Fatalf("dst = %q after an error", got)
	}
}

// With room in dst the header is encoded without allocating, whatever
// the payload's size: the payload is never touched.
func TestAppendPublishHeaderDoesNotAllocate(t *testing.T) {
	buf := make([]byte, 0, 256)
	opts := PublishOpts{Topic: "bench/devices/sensor-0001/telemetry", Payload: make([]byte, 1<<20), QoS: 1, PacketID: 1}
	allocs := testing.AllocsPerRun(100, func() {
		buf, _ = AppendPublishHeader(buf[:0], opts)
	})
	if allocs != 0 {
		t.Fatalf("AppendPublishHeader allocates %v times", allocs)
	}
}
