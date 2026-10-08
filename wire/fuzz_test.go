// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"reflect"
	"testing"
	"unicode/utf8"
)

// Seeds: well-formed packets plus malformed and spec-invalid inputs.
var packetSeeds = [][]byte{
	{0xd0, 0x00},                             // PINGRESP
	{0x20, 0x03, 0x00, 0x00, 0x00},           // CONNACK
	{0x30, 0x05, 0x00, 0x01, 'x', 0x00, 'a'}, // PUBLISH QoS 0
	{0x32, 0x07, 0x00, 0x01, 'x', 0x00, 0x01, 0x00, 'a'},
	{0x40, 0x02, 0x00, 0x01},                                    // PUBACK
	{0x62, 0x02, 0x00, 0x01},                                    // PUBREL
	{0x90, 0x04, 0x00, 0x01, 0x00, 0x01},                        // SUBACK
	{0xe0, 0x00},                                                // DISCONNECT
	{0xf0, 0x00},                                                // AUTH
	{0xd0, 0x80, 0x00},                                          // non-minimal remaining length
	{0xd1, 0x00},                                                // PINGRESP reserved flags
	{0xd0, 0x01, 0x00},                                          // PINGRESP with body
	{0x20, 0x02, 0x00, 0x00},                                    // CONNACK without property length
	{0x30, 0x04, 0x00, 0x01, 0x00, 0x00},                        // NUL in topic
	{0x30, 0x04, 0x00, 0x01, 0xff, 0x00},                        // invalid UTF-8 topic
	{0x30, 0x06, 0x00, 0x03, 0xed, 0xa0, 0x80, 0x00},            // UTF-16 surrogate in topic
	{0x30, 0x04, 0x00, 0x01, '#', 0x00},                         // wildcard topic name
	{0x30, 0x03, 0x00, 0x00, 0x00},                              // empty topic, no alias
	{0x32, 0x06, 0x00, 0x01, 'x', 0x00, 0x00, 0x00},             // packet id 0
	{0x30, 0x05, 0x00, 0x01, 'x', 0x01, 0x7f},                   // unknown property
	{0x30, 0x07, 0x00, 0x01, 'x', 0x03, 0x21, 0x00, 0x01},       // property not allowed on PUBLISH
	{0x30, 0x08, 0x00, 0x01, 'x', 0x04, 0x01, 0x00, 0x01, 0x00}, // duplicate payload format
	{0x30, 0x06, 0x00, 0x01, 'x', 0x02, 0x0b, 0x00},             // subscription identifier 0
	{0x30, 0x05, 0x00, 0x01, 'x', 0x01, 0x02},                   // truncated property
}

func FuzzReadPacket(f *testing.F) {
	for _, s := range packetSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// Bodies are allocated as their bytes arrive, so a declared
		// length larger than the input costs nothing to try.
		dec := NewDecoder(bytes.NewReader(data))
		for i := 0; i < 8; i++ {
			p, err := dec.ReadPacket()
			if err != nil {
				return
			}
			exercise(p)
			p.Release()
		}
	})
}

// exercise touches every accessor a client uses, so lazy parsing errors
// surface as panics in the fuzzer rather than in production.
func exercise(p Packet) {
	var props Properties
	switch x := p.(type) {
	case *Publish:
		_ = len(x.Topic) + len(x.Payload)
		props = x.Properties
	case *Connack:
		props = x.Properties
	case *PubResp:
		props = x.Properties
	case *Suback:
		props = x.Properties
		_ = len(x.ReasonCodes)
	case *Unsuback:
		props = x.Properties
	case *Disconnect:
		props = x.Properties
		_ = x.Clone()
	case *Auth:
		props = x.Properties
	case *Connect:
		props = x.Properties
	case *Subscribe:
		props = x.Properties
	case *Unsubscribe:
		props = x.Properties
	}
	touchProperties(props)
}

func touchProperties(p Properties) {
	for id := byte(0); id < 0x30; id++ {
		_, _ = p.Byte(id)
		_, _ = p.Uint16(id)
		_, _ = p.Uint32(id)
		_, _ = p.Varint(id)
		_, _ = p.String(id)
		_, _ = p.Binary(id)
	}
	for k, v := range p.UserProperties() {
		_ = len(k) + len(v)
	}
}

func FuzzProperties(f *testing.F) {
	f.Add([]byte{0x01, 0x01, 0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v'})
	f.Add([]byte{0x0b, 0x80, 0x80, 0x80, 0x80, 0x01})
	f.Add([]byte{0x26, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, raw []byte) {
		touchProperties(PropertiesFromBytes(raw))
	})
}

func FuzzDecodeVarint(f *testing.F) {
	for _, s := range [][]byte{{0x00}, {0x7f}, {0x80, 0x01}, {0xff, 0xff, 0xff, 0x7f}, {0x80, 0x80, 0x80, 0x80, 0x01}} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, n, err := DecodeVarint(b)
		if err != nil {
			return
		}
		if n < 1 || n > 4 || v > MaxVarintValue {
			t.Fatalf("DecodeVarint(%x) = %d, %d", b, v, n)
		}
		var buf [4]byte
		m, err := EncodeVarint(buf[:], v)
		if err != nil {
			t.Fatalf("re-encode %d: %v", v, err)
		}
		got, _, err := DecodeVarint(buf[:m])
		if err != nil || got != v {
			t.Fatalf("round trip %d -> %x -> %d (%v)", v, buf[:m], got, err)
		}
	})
}

func FuzzPublishRoundTrip(f *testing.F) {
	f.Add("a/b", []byte("payload"), byte(1), true, uint16(7), "text/plain", "resp/x", []byte("corr"))
	f.Add("x", []byte{}, byte(0), false, uint16(0), "", "", []byte(nil))
	f.Fuzz(func(t *testing.T, topic string, payload []byte, qos byte, retain bool, id uint16, ctype, resp string, corr []byte) {
		qos %= 3
		if qos > 0 && id == 0 {
			id = 1
		}
		if qos == 0 {
			id = 0
		}
		// Only well-formed inputs: the encoder's validation is tested separately.
		for _, s := range []string{topic, ctype, resp} {
			if len(s) > 0xffff || !utf8.ValidString(s) {
				t.Skip()
			}
		}
		if topic == "" || len(corr) > 0xffff {
			t.Skip()
		}
		opts := PublishOpts{Topic: topic, Payload: payload, QoS: qos, Retain: retain, PacketID: id,
			ContentType: ctype, ResponseTopic: resp, CorrelationData: corr}
		bp, err := EncodePublish(opts)
		if err != nil {
			return
		}
		defer ReleaseBuf(bp)
		p, err := NewDecoder(bytes.NewReader(*bp)).ReadPacket()
		if err != nil {
			t.Fatalf("decode of encoded publish: %v", err)
		}
		defer p.Release()
		got := p.(*Publish).Opts()
		if len(opts.Payload) == 0 {
			opts.Payload = nil // nil and empty encode alike
		}
		if !reflect.DeepEqual(got, opts) {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, opts)
		}
	})
}
