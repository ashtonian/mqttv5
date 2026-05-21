// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

// publishCase covers the matrix of PUBLISH header flags and property
// combinations a real subscriber would hit.
type publishCase struct {
	name string
	opts PublishOpts
}

var (
	pfi1               byte   = 1
	mei100      uint32        = 100
	contentTypeJSON           = "application/json"
	responseTopicRPC          = "rpc/responses/01HXZ7QY"
	correlationData           = []byte("\x01\x02\x03\x04")
)

func publishCases() []publishCase {
	return []publishCase{
		{
			name: "QoS0_minimal",
			opts: PublishOpts{
				Topic:   "sport/tennis/player1",
				Payload: []byte("score:6-4"),
				QoS:     0,
			},
		},
		{
			name: "QoS1_packet_id",
			opts: PublishOpts{
				Topic:    "sport/tennis/player1",
				Payload:  []byte("score:6-4"),
				QoS:      1,
				PacketID: 42,
			},
		},
		{
			name: "QoS2_retain_dup",
			opts: PublishOpts{
				Topic:    "config/devices",
				Payload:  []byte("{}"),
				QoS:      2,
				Retain:   true,
				Dup:      true,
				PacketID: 7,
			},
		},
		{
			name: "empty_payload",
			opts: PublishOpts{
				Topic:    "presence/heartbeat",
				Payload:  nil,
				QoS:      1,
				PacketID: 1,
			},
		},
		{
			name: "with_properties",
			opts: PublishOpts{
				Topic:                  "rpc/requests",
				Payload:                []byte(`{"method":"ping"}`),
				QoS:                    1,
				PacketID:               99,
				PayloadFormatIndicator: &pfi1,
				MessageExpiryInterval:  &mei100,
				ContentType:            contentTypeJSON,
				ResponseTopic:          responseTopicRPC,
				CorrelationData:        correlationData,
				TopicAlias:             3,
				UserProperties: []UserProperty{
					{Key: "device_id", Value: "sensor-0001"},
					{Key: "tenant", Value: "acme"},
				},
			},
		},
	}
}

func TestPublishRoundTrip(t *testing.T) {
	for _, tc := range publishCases() {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			n, err := WritePublish(&buf, tc.opts)
			if err != nil {
				t.Fatalf("WritePublish: %v", err)
			}
			if int(n) != buf.Len() {
				t.Fatalf("WritePublish returned n=%d but buf has %d bytes", n, buf.Len())
			}

			d := NewDecoder(&buf)
			pkt, err := d.ReadPacket()
			if err != nil {
				t.Fatalf("ReadPacket: %v", err)
			}
			defer pkt.Release()

			pub, ok := pkt.(*Publish)
			if !ok {
				t.Fatalf("got %T, want *Publish", pkt)
			}

			assertPublishMatches(t, &tc.opts, pub)
		})
	}
}

func TestPublishRejectsInvalidQoS(t *testing.T) {
	var buf bytes.Buffer
	_, err := WritePublish(&buf, PublishOpts{Topic: "t", QoS: 3})
	if !errors.Is(err, ErrInvalidQoS) {
		t.Fatalf("got %v, want ErrInvalidQoS", err)
	}
}

func TestPublishRequiresPacketIDForQoS1(t *testing.T) {
	var buf bytes.Buffer
	_, err := WritePublish(&buf, PublishOpts{Topic: "t", QoS: 1, PacketID: 0})
	if !errors.Is(err, ErrPacketIDRequired) {
		t.Fatalf("got %v, want ErrPacketIDRequired", err)
	}
}

func TestDecodeRejectsQoS3(t *testing.T) {
	// Hand-build a PUBLISH with QoS=3 in the flag nibble.
	body := encodeMinimalPublishBody("t", nil, 0)
	frame := append([]byte{}, body...)

	// Fixed header byte: type=PUBLISH<<4 | flags=0x06 (QoS=3).
	pkt := append([]byte{0x36}, encodeVBI(uint32(len(frame)))...)
	pkt = append(pkt, frame...)

	d := NewDecoder(bytes.NewReader(pkt))
	_, err := d.ReadPacket()
	if !errors.Is(err, ErrInvalidPacket) {
		t.Fatalf("got %v, want ErrInvalidPacket", err)
	}
}

func assertPublishMatches(t *testing.T, opts *PublishOpts, pub *Publish) {
	t.Helper()
	if pub.Topic != opts.Topic {
		t.Errorf("Topic: got %q, want %q", pub.Topic, opts.Topic)
	}
	if !bytes.Equal(pub.Payload, opts.Payload) {
		t.Errorf("Payload: got %q, want %q", pub.Payload, opts.Payload)
	}
	if pub.QoS != opts.QoS {
		t.Errorf("QoS: got %d, want %d", pub.QoS, opts.QoS)
	}
	if pub.Retain != opts.Retain {
		t.Errorf("Retain: got %v, want %v", pub.Retain, opts.Retain)
	}
	if pub.Dup != opts.Dup {
		t.Errorf("Dup: got %v, want %v", pub.Dup, opts.Dup)
	}
	if pub.PacketID != opts.PacketID {
		t.Errorf("PacketID: got %d, want %d", pub.PacketID, opts.PacketID)
	}

	if opts.PayloadFormatIndicator != nil {
		got, ok := pub.Properties.Byte(PropPayloadFormat)
		if !ok || got != *opts.PayloadFormatIndicator {
			t.Errorf("PayloadFormat: got %d/%v, want %d/true", got, ok, *opts.PayloadFormatIndicator)
		}
	}
	if opts.MessageExpiryInterval != nil {
		got, ok := pub.Properties.Uint32(PropMessageExpiryInterval)
		if !ok || got != *opts.MessageExpiryInterval {
			t.Errorf("MessageExpiry: got %d/%v, want %d/true", got, ok, *opts.MessageExpiryInterval)
		}
	}
	if opts.ContentType != "" {
		got, ok := pub.Properties.String(PropContentType)
		if !ok || got != opts.ContentType {
			t.Errorf("ContentType: got %q/%v, want %q/true", got, ok, opts.ContentType)
		}
	}
	if opts.ResponseTopic != "" {
		got, ok := pub.Properties.String(PropResponseTopic)
		if !ok || got != opts.ResponseTopic {
			t.Errorf("ResponseTopic: got %q/%v, want %q/true", got, ok, opts.ResponseTopic)
		}
	}
	if opts.CorrelationData != nil {
		got, ok := pub.Properties.Binary(PropCorrelationData)
		if !ok || !bytes.Equal(got, opts.CorrelationData) {
			t.Errorf("CorrelationData: got %x/%v, want %x/true", got, ok, opts.CorrelationData)
		}
	}
	if opts.TopicAlias != 0 {
		got, ok := pub.Properties.Uint16(PropTopicAlias)
		if !ok || got != opts.TopicAlias {
			t.Errorf("TopicAlias: got %d/%v, want %d/true", got, ok, opts.TopicAlias)
		}
	}
	if len(opts.UserProperties) > 0 {
		var got []UserProperty
		for k, v := range pub.Properties.UserProperties() {
			got = append(got, UserProperty{Key: k, Value: v})
		}
		if len(got) != len(opts.UserProperties) {
			t.Errorf("UserProperties count: got %d, want %d", len(got), len(opts.UserProperties))
		}
		for i := range got {
			if got[i] != opts.UserProperties[i] {
				t.Errorf("UserProperties[%d]: got %v, want %v", i, got[i], opts.UserProperties[i])
			}
		}
	}
}

// encodeMinimalPublishBody builds a PUBLISH body (everything after the
// fixed header) with no properties. Used by TestDecodeRejectsQoS3 to
// craft a deliberately bad packet.
func encodeMinimalPublishBody(topic string, payload []byte, qos byte) []byte {
	var b bytes.Buffer
	tl := make([]byte, 2)
	tl[0] = byte(len(topic) >> 8)
	tl[1] = byte(len(topic))
	b.Write(tl)
	b.WriteString(topic)
	if qos > 0 {
		b.Write([]byte{0x00, 0x01})
	}
	b.WriteByte(0x00) // empty properties length
	b.Write(payload)
	return b.Bytes()
}

func encodeVBI(v uint32) []byte {
	var buf [4]byte
	n, _ := EncodeVarint(buf[:], v)
	return buf[:n]
}
