// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"bytes"
	"errors"
	"testing"
)

// roundTrip writes opts via the supplied writeFn, decodes the resulting
// bytes via the Decoder, and returns the decoded Packet. Used by every
// per-type test below so they all exercise the same Decoder dispatch
// path (codec.go's switch).
func roundTrip(t *testing.T, writeFn func(*bytes.Buffer) (int64, error), wantType PacketType) Packet {
	t.Helper()
	var buf bytes.Buffer
	n, err := writeFn(&buf)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if int(n) != buf.Len() {
		t.Fatalf("write returned %d, buf has %d", n, buf.Len())
	}

	d := NewDecoder(&buf)
	pkt, err := d.ReadPacket()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if pkt.Type() != wantType {
		pkt.Release()
		t.Fatalf("type: got %s, want %s", pkt.Type(), wantType)
	}
	return pkt
}

// ---------------- PINGREQ / PINGRESP ----------------

func TestPingreqRoundTrip(t *testing.T) {
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WritePingreq(b) },
		PINGREQ,
	)
	defer pkt.Release()
	if _, ok := pkt.(*Pingreq); !ok {
		t.Fatalf("type assert: got %T", pkt)
	}
}

func TestPingrespRoundTrip(t *testing.T) {
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WritePingresp(b) },
		PINGRESP,
	)
	defer pkt.Release()
	if _, ok := pkt.(*Pingresp); !ok {
		t.Fatalf("type assert: got %T", pkt)
	}
}

// ---------------- PUBACK / PUBREC / PUBREL / PUBCOMP ----------------

func TestPubRespRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		writeFn func(*bytes.Buffer, PubRespOpts) (int64, error)
		want    PacketType
	}{
		{"PUBACK", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePuback(b, o) }, PUBACK},
		{"PUBREC", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubrec(b, o) }, PUBREC},
		{"PUBREL", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubrel(b, o) }, PUBREL},
		{"PUBCOMP", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubcomp(b, o) }, PUBCOMP},
	}

	shapes := []struct {
		name string
		opts PubRespOpts
	}{
		{
			"minimal_success",
			PubRespOpts{PacketID: 42},
		},
		{
			"reason_only",
			PubRespOpts{PacketID: 7, ReasonCode: ReasonNoMatchingSubscribers},
		},
		{
			"reason_and_props",
			PubRespOpts{
				PacketID:     99,
				ReasonCode:   ReasonNoMatchingSubscribers,
				ReasonString: "nobody is listening",
				UserProperties: []UserProperty{
					{Key: "trace_id", Value: "abc"},
				},
			},
		},
	}

	for _, tc := range cases {
		for _, sh := range shapes {
			t.Run(tc.name+"/"+sh.name, func(t *testing.T) {
				pkt := roundTrip(t,
					func(b *bytes.Buffer) (int64, error) { return tc.writeFn(b, sh.opts) },
					tc.want,
				)
				defer pkt.Release()
				pr := pkt.(*PubResp)
				if pr.PacketID != sh.opts.PacketID {
					t.Errorf("PacketID: got %d, want %d", pr.PacketID, sh.opts.PacketID)
				}
				if pr.ReasonCode != sh.opts.ReasonCode {
					t.Errorf("ReasonCode: got %d, want %d", pr.ReasonCode, sh.opts.ReasonCode)
				}
				if sh.opts.ReasonString != "" {
					got, ok := pr.Properties.String(PropReasonString)
					if !ok || got != sh.opts.ReasonString {
						t.Errorf("ReasonString: got %q/%v, want %q/true", got, ok, sh.opts.ReasonString)
					}
				}
			})
		}
	}
}

// ---------------- DISCONNECT ----------------

func TestDisconnectRoundTrip(t *testing.T) {
	sei := uint32(120)
	cases := []struct {
		name string
		opts DisconnectOpts
	}{
		{"minimal", DisconnectOpts{}},
		{"reason_only", DisconnectOpts{ReasonCode: ReasonServerShuttingDown}},
		{
			"with_props",
			DisconnectOpts{
				ReasonCode:            ReasonServerShuttingDown,
				SessionExpiryInterval: &sei,
				ReasonString:          "patching",
				ServerReference:       "mqtt://failover:1883",
				UserProperties:        []UserProperty{{Key: "k", Value: "v"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkt := roundTrip(t,
				func(b *bytes.Buffer) (int64, error) { return WriteDisconnect(b, tc.opts) },
				DISCONNECT,
			)
			defer pkt.Release()
			d := pkt.(*Disconnect)
			if d.ReasonCode != tc.opts.ReasonCode {
				t.Errorf("ReasonCode: got %d, want %d", d.ReasonCode, tc.opts.ReasonCode)
			}
			if tc.opts.SessionExpiryInterval != nil {
				got, ok := d.Properties.Uint32(PropSessionExpiryInterval)
				if !ok || got != *tc.opts.SessionExpiryInterval {
					t.Errorf("SessionExpiryInterval: got %d/%v, want %d", got, ok, *tc.opts.SessionExpiryInterval)
				}
			}
			if tc.opts.ReasonString != "" {
				got, _ := d.Properties.String(PropReasonString)
				if got != tc.opts.ReasonString {
					t.Errorf("ReasonString: got %q, want %q", got, tc.opts.ReasonString)
				}
			}
			if tc.opts.ServerReference != "" {
				got, _ := d.Properties.String(PropServerReference)
				if got != tc.opts.ServerReference {
					t.Errorf("ServerReference: got %q, want %q", got, tc.opts.ServerReference)
				}
			}
		})
	}
}

// ---------------- AUTH ----------------

func TestAuthRoundTrip(t *testing.T) {
	opts := AuthOpts{
		ReasonCode:           ReasonContinueAuthentication,
		AuthenticationMethod: "SCRAM-SHA-256",
		AuthenticationData:   []byte{0x01, 0x02, 0x03},
		ReasonString:         "more rounds",
		UserProperties:       []UserProperty{{Key: "round", Value: "2"}},
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteAuth(b, opts) },
		AUTH,
	)
	defer pkt.Release()
	a := pkt.(*Auth)
	if a.ReasonCode != opts.ReasonCode {
		t.Errorf("ReasonCode: got %d, want %d", a.ReasonCode, opts.ReasonCode)
	}
	if got, _ := a.Properties.String(PropAuthMethod); got != opts.AuthenticationMethod {
		t.Errorf("AuthMethod: got %q, want %q", got, opts.AuthenticationMethod)
	}
	if got, _ := a.Properties.Binary(PropAuthData); !bytes.Equal(got, opts.AuthenticationData) {
		t.Errorf("AuthData: got %x, want %x", got, opts.AuthenticationData)
	}
}

// ---------------- SUBSCRIBE / UNSUBSCRIBE ----------------

func TestSubscribeRoundTrip(t *testing.T) {
	subID := uint32(7)
	opts := SubscribeOpts{
		PacketID: 11,
		Filters: []SubscribeFilter{
			{Topic: "a/+", QoS: 1},
			{Topic: "b/#", QoS: 2, NoLocal: true, RetainAsPublished: true, RetainHandling: 1},
		},
		SubscriptionIdentifier: &subID,
		UserProperties:         []UserProperty{{Key: "k", Value: "v"}},
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteSubscribe(b, opts) },
		SUBSCRIBE,
	)
	defer pkt.Release()
	s := pkt.(*Subscribe)
	if s.PacketID != opts.PacketID {
		t.Errorf("PacketID: got %d, want %d", s.PacketID, opts.PacketID)
	}
	if len(s.Filters) != 2 {
		t.Fatalf("Filters: got %d, want 2", len(s.Filters))
	}
	if s.Filters[0].Topic != "a/+" || s.Filters[0].QoS != 1 {
		t.Errorf("filter[0]: got %+v", s.Filters[0])
	}
	if !s.Filters[1].NoLocal || !s.Filters[1].RetainAsPublished || s.Filters[1].RetainHandling != 1 {
		t.Errorf("filter[1]: got %+v", s.Filters[1])
	}
	if got, _ := s.Properties.Varint(PropSubscriptionIdentifier); got != subID {
		t.Errorf("SubscriptionIdentifier: got %d, want %d", got, subID)
	}
}

func TestSubscribeRejectsEmpty(t *testing.T) {
	if _, err := WriteSubscribe(&bytes.Buffer{}, SubscribeOpts{PacketID: 1}); !errors.Is(err, ErrEmptyFilterList) {
		t.Fatalf("got %v, want ErrEmptyFilterList", err)
	}
}

func TestUnsubscribeRoundTrip(t *testing.T) {
	opts := UnsubscribeOpts{
		PacketID:       12,
		Topics:         []string{"x/y", "z/#"},
		UserProperties: []UserProperty{{Key: "k", Value: "v"}},
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteUnsubscribe(b, opts) },
		UNSUBSCRIBE,
	)
	defer pkt.Release()
	u := pkt.(*Unsubscribe)
	if u.PacketID != opts.PacketID {
		t.Errorf("PacketID: got %d, want %d", u.PacketID, opts.PacketID)
	}
	if len(u.Topics) != 2 || u.Topics[0] != "x/y" || u.Topics[1] != "z/#" {
		t.Errorf("Topics: got %v", u.Topics)
	}
}

// ---------------- SUBACK / UNSUBACK ----------------

func TestSubackRoundTrip(t *testing.T) {
	opts := SubackOpts{
		PacketID:     11,
		ReasonCodes:  []ReasonCode{ReasonGrantedQoS0, ReasonGrantedQoS1, ReasonGrantedQoS2, ReasonNotAuthorized},
		ReasonString: "partial",
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteSuback(b, opts) },
		SUBACK,
	)
	defer pkt.Release()
	s := pkt.(*Suback)
	if s.PacketID != opts.PacketID {
		t.Errorf("PacketID: got %d, want %d", s.PacketID, opts.PacketID)
	}
	if len(s.ReasonCodes) != 4 || s.ReasonCodes[3] != ReasonNotAuthorized {
		t.Errorf("ReasonCodes: got %v", s.ReasonCodes)
	}
}

func TestUnsubackRoundTrip(t *testing.T) {
	opts := UnsubackOpts{
		PacketID:    12,
		ReasonCodes: []ReasonCode{ReasonSuccess, ReasonNoSubscriptionExisted},
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteUnsuback(b, opts) },
		UNSUBACK,
	)
	defer pkt.Release()
	u := pkt.(*Unsuback)
	if u.PacketID != opts.PacketID {
		t.Errorf("PacketID: got %d, want %d", u.PacketID, opts.PacketID)
	}
	if len(u.ReasonCodes) != 2 || u.ReasonCodes[1] != ReasonNoSubscriptionExisted {
		t.Errorf("ReasonCodes: got %v", u.ReasonCodes)
	}
}

// ---------------- CONNECT / CONNACK ----------------

func TestConnectRoundTrip(t *testing.T) {
	sei := uint32(3600)
	rcvMax := uint16(100)
	will := WillOpts{
		Topic:   "last/words",
		Payload: []byte("offline"),
		QoS:     1,
		Retain:  true,
	}
	opts := ConnectOpts{
		ClientID:              "client-001",
		CleanStart:            true,
		KeepAlive:             30,
		Username:              "alice",
		Password:              []byte("secret"),
		Will:                  &will,
		SessionExpiryInterval: &sei,
		ReceiveMaximum:        &rcvMax,
		UserProperties:        []UserProperty{{Key: "client", Value: "go"}},
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteConnect(b, opts) },
		CONNECT,
	)
	defer pkt.Release()
	c := pkt.(*Connect)
	if c.ClientID != opts.ClientID {
		t.Errorf("ClientID: got %q, want %q", c.ClientID, opts.ClientID)
	}
	if !c.CleanStart || c.KeepAlive != 30 {
		t.Errorf("CleanStart/KeepAlive: got %v/%d", c.CleanStart, c.KeepAlive)
	}
	if c.Username != "alice" || !bytes.Equal(c.Password, []byte("secret")) {
		t.Errorf("credentials: got %q / %q", c.Username, c.Password)
	}
	if c.Will == nil {
		t.Fatal("Will: got nil")
	}
	if c.Will.Topic != "last/words" || !bytes.Equal(c.Will.Payload, []byte("offline")) || c.Will.QoS != 1 || !c.Will.Retain {
		t.Errorf("Will: got %+v", c.Will)
	}
	if got, _ := c.Properties.Uint32(PropSessionExpiryInterval); got != sei {
		t.Errorf("SessionExpiryInterval: got %d, want %d", got, sei)
	}
}

func TestConnectRejectsBadProtocol(t *testing.T) {
	// Hand-craft a CONNECT with the wrong protocol name.
	body := encodeBadCONNECTBody()
	pkt := append([]byte{byte(CONNECT) << 4}, encodeVBI(uint32(len(body)))...)
	pkt = append(pkt, body...)

	d := NewDecoder(bytes.NewReader(pkt))
	_, err := d.ReadPacket()
	if !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("got %v, want ErrInvalidProtocol", err)
	}
}

func encodeBadCONNECTBody() []byte {
	var b bytes.Buffer
	// Protocol Name "WRONG"
	b.Write([]byte{0x00, 0x05})
	b.WriteString("WRONG")
	// Version 5, no flags, KA=0, no props, empty ClientID.
	b.WriteByte(0x05)
	b.WriteByte(0x00)
	b.Write([]byte{0x00, 0x00})
	b.WriteByte(0x00)           // empty properties
	b.Write([]byte{0x00, 0x00}) // empty client id
	return b.Bytes()
}

func TestConnackRoundTrip(t *testing.T) {
	sei := uint32(60)
	maxQoS := byte(2)
	opts := ConnackOpts{
		SessionPresent:           true,
		ReasonCode:               ReasonSuccess,
		SessionExpiryInterval:    &sei,
		MaximumQoS:               &maxQoS,
		AssignedClientIdentifier: "broker-assigned-id",
		ReasonString:             "ok",
	}
	pkt := roundTrip(t,
		func(b *bytes.Buffer) (int64, error) { return WriteConnack(b, opts) },
		CONNACK,
	)
	defer pkt.Release()
	c := pkt.(*Connack)
	if !c.SessionPresent {
		t.Error("SessionPresent: got false, want true")
	}
	if c.ReasonCode != ReasonSuccess {
		t.Errorf("ReasonCode: got %d, want %d", c.ReasonCode, ReasonSuccess)
	}
	if got, _ := c.Properties.Uint32(PropSessionExpiryInterval); got != sei {
		t.Errorf("SessionExpiryInterval: got %d, want %d", got, sei)
	}
	if got, _ := c.Properties.Byte(PropMaximumQoS); got != maxQoS {
		t.Errorf("MaximumQoS: got %d, want %d", got, maxQoS)
	}
	if got, _ := c.Properties.String(PropAssignedClientID); got != "broker-assigned-id" {
		t.Errorf("AssignedClientID: got %q", got)
	}
}
