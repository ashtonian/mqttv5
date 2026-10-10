// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

// Every input of a malformed-input matrix is
// rejected with the reason code the spec assigns.
func TestMalformedInputsRejected(t *testing.T) {
	tests := []struct {
		name   string
		in     []byte
		reason ReasonCode
	}{
		{"overlong remaining length", []byte{0xd0, 0x80, 0}, ReasonMalformedPacket},
		{"pingresp reserved flags", []byte{0xd1, 0}, ReasonMalformedPacket},
		{"pingresp with body", []byte{0xd0, 1, 0}, ReasonMalformedPacket},
		{"connack missing property length", []byte{0x20, 2, 0, 0}, ReasonMalformedPacket},
		{"publish NUL in topic", []byte{0x30, 4, 0, 1, 0, 0}, ReasonMalformedPacket},
		{"publish invalid UTF-8", []byte{0x30, 4, 0, 1, 0xff, 0}, ReasonMalformedPacket},
		{"publish UTF-16 surrogate", []byte{0x30, 6, 0, 3, 0xed, 0xa0, 0x80, 0}, ReasonMalformedPacket},
		{"publish wildcard topic", []byte{0x30, 4, 0, 1, '#', 0}, ReasonProtocolError},
		{"publish empty topic without alias", []byte{0x30, 3, 0, 0, 0}, ReasonProtocolError},
		{"publish packet id 0", []byte{0x32, 6, 0, 1, 'x', 0, 0, 0}, ReasonProtocolError},
		{"unknown property", []byte{0x30, 5, 0, 1, 'x', 1, 0x7f}, ReasonMalformedPacket},
		{"property not allowed on PUBLISH", []byte{0x30, 7, 0, 1, 'x', 3, 0x21, 0, 1}, ReasonMalformedPacket},
		{"duplicate payload format", []byte{0x30, 8, 0, 1, 'x', 4, 1, 0, 1, 0}, ReasonProtocolError},
		{"subscription identifier 0", []byte{0x30, 6, 0, 1, 'x', 2, 0x0b, 0}, ReasonProtocolError},
		{"truncated property", []byte{0x30, 5, 0, 1, 'x', 1, 0x02}, ReasonMalformedPacket},
		{"payload format 2", []byte{0x30, 6, 0, 1, 'x', 2, 0x01, 2}, ReasonProtocolError},
		{"topic alias 0", []byte{0x30, 7, 0, 1, 'x', 3, 0x23, 0, 0}, ReasonTopicAliasInvalid},
		{"response topic wildcard", []byte{0x30, 9, 0, 1, 'x', 5, 0x08, 0, 2, 'a', '+'}, ReasonProtocolError},
		{"puback trailing bytes", []byte{0x40, 5, 0, 1, 0, 0, 0xff}, ReasonMalformedPacket},
		{"puback reason not defined", []byte{0x40, 3, 0, 1, 0x92}, ReasonMalformedPacket},
		{"puback packet id 0", []byte{0x40, 2, 0, 0}, ReasonProtocolError},
		{"pubrel wrong flags", []byte{0x60, 2, 0, 1}, ReasonMalformedPacket},
		{"connack receive maximum 0", []byte{0x20, 6, 0, 0, 3, 0x21, 0, 0}, ReasonProtocolError},
		{"connack maximum QoS 2", []byte{0x20, 5, 0, 0, 2, 0x24, 2}, ReasonProtocolError},
		{"suback reason not defined", []byte{0x90, 4, 0, 1, 0, 0x10}, ReasonMalformedPacket},
		{"disconnect reason not defined", []byte{0xe0, 2, 0x10, 0}, ReasonMalformedPacket},
		{"auth reason not defined", []byte{0xf0, 2, 0x01, 0}, ReasonMalformedPacket},
		{"unknown packet type 0", []byte{0x00, 0}, ReasonMalformedPacket},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewDecoder(bytes.NewReader(tt.in)).ReadPacket()
			if err == nil {
				p.Release()
				t.Fatalf("% x accepted", tt.in)
			}
			var pe *PacketError
			if !errors.As(err, &pe) || !errors.Is(err, ErrInvalidPacket) {
				t.Fatalf("error %v is not a *PacketError", err)
			}
			if pe.Reason != tt.reason {
				t.Fatalf("reason %#x, want %#x (%v)", byte(pe.Reason), byte(tt.reason), err)
			}
		})
	}
}

// Lenient decoding lets the whitelisted violations through and
// reports them; everything else still fails.
func TestLenientDecoding(t *testing.T) {
	var reported []string
	report := func(e *PacketError) { reported = append(reported, e.Detail) }
	for _, in := range [][]byte{{0xd0, 0x80, 0}, {0xd1, 0}} {
		dec := NewDecoder(bytes.NewReader(in))
		dec.SetLenient(report)
		p, err := dec.ReadPacket()
		if err != nil {
			t.Fatalf("lenient decoding rejected % x: %v", in, err)
		}
		p.Release()
	}
	if len(reported) != 2 {
		t.Fatalf("reported %v", reported)
	}
	dec := NewDecoder(bytes.NewReader([]byte{0x30, 4, 0, 1, '#', 0}))
	dec.SetLenient(report)
	if _, err := dec.ReadPacket(); err == nil {
		t.Fatal("lenient decoding accepted a wildcard topic")
	}
}

// Valid packets of every kind the client receives still decode.
func TestValidPacketsAccepted(t *testing.T) {
	var buf bytes.Buffer
	mustWrite := func(_ int64, err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	one, two := byte(1), uint16(2)
	mustWrite(WriteConnack(&buf, ConnackOpts{MaximumQoS: &one, ReceiveMaximum: &two, AssignedClientIdentifier: "id",
		ReasonString: "ok", UserProperties: []UserProperty{{"k", "v"}}}))
	mustWrite(WritePublish(&buf, PublishOpts{Topic: "a/b", QoS: 2, PacketID: 3, ResponseTopic: "r/s",
		ContentType: "text/plain", CorrelationData: []byte{1}, UserProperties: []UserProperty{{"k", "v"}, {"k", "w"}}}))
	mustWrite(WritePublish(&buf, PublishOpts{TopicAlias: 4, Payload: []byte("aliased")}))
	mustWrite(WritePuback(&buf, PubRespOpts{PacketID: 1, ReasonCode: ReasonQuotaExceeded, ReasonString: "slow down"}))
	mustWrite(WritePubrel(&buf, PubRespOpts{PacketID: 1}))
	mustWrite(WriteSuback(&buf, SubackOpts{PacketID: 1, ReasonCodes: []ReasonCode{ReasonGrantedQoS2, ReasonNotAuthorized}}))
	mustWrite(WriteUnsuback(&buf, UnsubackOpts{PacketID: 1, ReasonCodes: []ReasonCode{ReasonNoSubscriptionExisted}}))
	mustWrite(WritePingresp(&buf))
	mustWrite(WriteDisconnect(&buf, DisconnectOpts{ReasonCode: ReasonServerMoved, ServerReference: "other:1883"}))
	mustWrite(WriteAuth(&buf, AuthOpts{ReasonCode: ReasonContinueAuthentication, AuthenticationMethod: "SCRAM-SHA-256", AuthenticationData: []byte{1}}))
	dec := NewDecoder(&buf)
	for i := 0; i < 10; i++ {
		p, err := dec.ReadPacket()
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		p.Release()
	}
}

func TestTopicValidators(t *testing.T) {
	for _, name := range []string{"a", "a/b", "/a", "$SYS/x", "a//b"} {
		if err := ValidTopicName(name); err != nil {
			t.Errorf("ValidTopicName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"a/#", "+", "a\x00b", string([]byte{0xff}), strings.Repeat("x", 65536)} {
		if err := ValidTopicName(name); err == nil {
			t.Errorf("ValidTopicName(%q) accepted", name)
		}
	}
	for _, f := range []string{"#", "+", "a/+/b", "a/#", "+/+", "$share/g/a/#", "/"} {
		if err := ValidTopicFilter(f); err != nil {
			t.Errorf("ValidTopicFilter(%q) = %v", f, err)
		}
	}
	for _, f := range []string{"", "a/#/b", "a#", "a/b+", "$share/g", "$share//a", "$share/g+/a", "$share/g/", "x\x00"} {
		if err := ValidTopicFilter(f); err == nil {
			t.Errorf("ValidTopicFilter(%q) accepted", f)
		}
	}
}

// Encoders refuse options MQTT v5 forbids instead of
// writing them (or truncating a length).
func TestEncodersRejectInvalidOptions(t *testing.T) {
	two := byte(2)
	zero16, zero32 := uint16(0), uint32(0)
	long := strings.Repeat("x", 65536)
	tests := []struct {
		name  string
		write func() error
		want  error
	}{
		{"publish NUL topic", pub(PublishOpts{Topic: "a\x00b"}), ErrInvalidTopic},
		{"publish wildcard topic", pub(PublishOpts{Topic: "#"}), ErrInvalidTopic},
		{"publish invalid UTF-8", pub(PublishOpts{Topic: string([]byte{255})}), ErrInvalidTopic},
		{"publish topic too long", pub(PublishOpts{Topic: long}), ErrStringTooLong},
		{"publish DUP at QoS 0", pub(PublishOpts{Topic: "x", Dup: true}), ErrInvalidField},
		{"publish response topic wildcard", pub(PublishOpts{Topic: "x", ResponseTopic: "a/+"}), ErrInvalidTopic},
		{"publish empty topic without alias", pub(PublishOpts{}), ErrInvalidTopic},
		{"publish payload format 2", pub(PublishOpts{Topic: "x", PayloadFormatIndicator: &two}), ErrInvalidField},
		{"publish content type too long", pub(PublishOpts{Topic: "x", ContentType: long}), ErrStringTooLong},
		{"publish user property invalid", pub(PublishOpts{Topic: "x", UserProperties: []UserProperty{{"k", "\x00"}}}), ErrInvalidField},
		{"subscribe no filters", func() error { _, err := WriteSubscribe(io.Discard, SubscribeOpts{PacketID: 1}); return err }, ErrEmptyFilterList},
		{"subscribe bad filter", sub(SubscribeFilter{Topic: "a/#/b"}), ErrInvalidTopic},
		{"subscribe shared no local", sub(SubscribeFilter{Topic: "$share/g/a", NoLocal: true}), ErrInvalidField},
		{"subscribe empty share name", sub(SubscribeFilter{Topic: "$share//a"}), ErrInvalidTopic},
		{"subscribe retain handling 3", sub(SubscribeFilter{Topic: "a", RetainHandling: 3}), ErrInvalidField},
		{"subscribe packet id 0", func() error {
			_, err := WriteSubscribe(io.Discard, SubscribeOpts{Filters: []SubscribeFilter{{Topic: "a"}}})
			return err
		}, ErrPacketIDRequired},
		{"subscribe id 0", func() error {
			_, err := WriteSubscribe(io.Discard, SubscribeOpts{PacketID: 1, Filters: []SubscribeFilter{{Topic: "a"}}, SubscriptionIdentifier: &zero32})
			return err
		}, ErrInvalidField},
		{"unsubscribe bad filter", func() error {
			_, err := WriteUnsubscribe(io.Discard, UnsubscribeOpts{PacketID: 1, Topics: []string{"a+"}})
			return err
		}, ErrInvalidTopic},
		{"connect receive maximum 0", func() error {
			_, err := WriteConnect(io.Discard, ConnectOpts{ClientID: "c", ReceiveMaximum: &zero16})
			return err
		}, ErrInvalidField},
		{"connect will wildcard", func() error {
			_, err := WriteConnect(io.Discard, ConnectOpts{ClientID: "c", Will: &WillOpts{Topic: "w/#"}})
			return err
		}, ErrInvalidTopic},
		{"connect client id too long", func() error {
			_, err := WriteConnect(io.Discard, ConnectOpts{ClientID: long})
			return err
		}, ErrStringTooLong},
		{"puback reason for pubrel", func() error {
			_, err := WritePubrel(io.Discard, PubRespOpts{PacketID: 1, ReasonCode: ReasonNoMatchingSubscribers})
			return err
		}, ErrInvalidField},
		{"disconnect undefined reason", func() error {
			_, err := WriteDisconnect(io.Discard, DisconnectOpts{ReasonCode: 0x10})
			return err
		}, ErrInvalidField},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.write(); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func pub(o PublishOpts) func() error {
	return func() error {
		if _, err := WritePublish(io.Discard, o); err != nil {
			return err
		}
		_, err := EncodePublish(o)
		return err
	}
}

func sub(f SubscribeFilter) func() error {
	return func() error {
		_, err := WriteSubscribe(io.Discard, SubscribeOpts{PacketID: 1, Filters: []SubscribeFilter{f}})
		return err
	}
}

func TestValidUTF8Bytes(t *testing.T) {
	cases := map[string]bool{
		"":                                       true,
		"ascii only":                             true,
		"exactly8":                               true,
		"sixteen bytes ok":                       true,
		"héllo wörld, ünïcode":                   true,
		"emoji 🙂 inside a longer":                true,
		"nul\x00inside":                          false,
		"longer string with nul in the tail\x00": false,
		"bad \xff byte in a long enough string":  false,
		"\xed\xa0\x80 surrogate":                 false,
		"12345678\x00":                           false,
		"1234567\x80":                            false,
	}
	for s, want := range cases {
		if got := validUTF8Bytes([]byte(s)); got != want {
			t.Errorf("validUTF8Bytes(%q) = %v, want %v", s, got, want)
		}
	}
}

func FuzzValidUTF8(f *testing.F) {
	for _, s := range []string{"", "abc", "héllo", "a\x00b", "\xff", "\xed\xa0\x80"} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		want := utf8.Valid(b) && bytes.IndexByte(b, 0) < 0
		if got := validUTF8Bytes(b); got != want {
			t.Fatalf("validUTF8Bytes(%q) = %v, want %v", b, got, want)
		}
	})
}

func FuzzScanTopicName(f *testing.F) {
	for _, s := range []string{"", "a/b", "a/+/b", "long/topic/name/with/#", "héllo/+", "\x00", "\xff#"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		valid, wildcard := scanTopicName(s)
		if want := utf8.ValidString(s) && !strings.Contains(s, "\x00"); valid != want {
			t.Fatalf("valid(%q) = %v, want %v", s, valid, want)
		}
		// The wildcard answer only matters for valid names.
		if want := strings.ContainsAny(s, "+#"); valid && wildcard != want {
			t.Fatalf("wildcard(%q) = %v, want %v", s, wildcard, want)
		}
	})
}
