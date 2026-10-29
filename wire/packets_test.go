package wire

import (
	"bytes"
	"errors"
	"reflect"
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

func TestPubRespRoundTrip(t *testing.T) {
	// Each type gets a non-success code it may legally carry.
	cases := []struct {
		name    string
		writeFn func(*bytes.Buffer, PubRespOpts) (int64, error)
		want    PacketType
		reason  ReasonCode
	}{
		{"PUBACK", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePuback(b, o) }, PUBACK, ReasonNoMatchingSubscribers},
		{"PUBREC", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubrec(b, o) }, PUBREC, ReasonNoMatchingSubscribers},
		{"PUBREL", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubrel(b, o) }, PUBREL, ReasonPacketIdentifierNotFound},
		{"PUBCOMP", func(b *bytes.Buffer, o PubRespOpts) (int64, error) { return WritePubcomp(b, o) }, PUBCOMP, ReasonPacketIdentifierNotFound},
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
			if sh.opts.ReasonCode != ReasonSuccess {
				sh.opts.ReasonCode = tc.reason
			}
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
	maxQoS := byte(1)
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

func TestPublishOptsRoundTrip(t *testing.T) {
	zero, one := uint32(0), byte(1)
	expiry := uint32(3600)
	for _, opts := range []PublishOpts{
		{Topic: "a/b", Payload: []byte("p"), QoS: 1, PacketID: 9, Retain: true},
		{Topic: "a/b", QoS: 2, PacketID: 1, Dup: true, MessageExpiryInterval: &zero, CorrelationData: []byte{}},
		{Topic: "t", Payload: []byte{1, 2}, PayloadFormatIndicator: &one, MessageExpiryInterval: &expiry,
			ContentType: "application/json", ResponseTopic: "reply/to", CorrelationData: []byte("c"), TopicAlias: 3,
			UserProperties: []UserProperty{{Key: "k", Value: "v"}, {Key: "k", Value: "w"}, {Key: "", Value: ""}}},
	} {
		frame, err := MarshalPublish(opts)
		if err != nil {
			t.Fatal(err)
		}
		pkt, err := NewDecoder(bytes.NewReader(frame)).ReadPacket()
		if err != nil {
			t.Fatal(err)
		}
		got := pkt.(*Publish).Opts()
		pkt.Release()
		if !reflect.DeepEqual(got, opts) {
			t.Fatalf("got  %+v\nwant %+v", got, opts)
		}
	}
}

func TestPublishOptsClone(t *testing.T) {
	pfi, mei := byte(1), uint32(30)
	orig := PublishOpts{Topic: "t", Payload: []byte("p"), PayloadFormatIndicator: &pfi, MessageExpiryInterval: &mei,
		CorrelationData: []byte{}, UserProperties: []UserProperty{{Key: "k", Value: "v"}}}
	c := orig.Clone()
	if !reflect.DeepEqual(c, orig) {
		t.Fatalf("clone %+v differs from %+v", c, orig)
	}
	orig.Payload[0], orig.UserProperties[0].Value, *orig.PayloadFormatIndicator, *orig.MessageExpiryInterval = 'x', "w", 0, 1
	if string(c.Payload) != "p" || c.UserProperties[0].Value != "v" || *c.PayloadFormatIndicator != 1 || *c.MessageExpiryInterval != 30 {
		t.Fatalf("clone shares memory with the original: %+v", c)
	}
	if c.CorrelationData == nil || (PublishOpts{}).Clone().CorrelationData != nil {
		t.Fatal("Clone changed nil-ness of Correlation Data")
	}
}

// publishCase covers the matrix of PUBLISH header flags and property
// combinations a real subscriber would hit.
type publishCase struct {
	name string
	opts PublishOpts
}

var (
	pfi1             byte   = 1
	mei100           uint32 = 100
	contentTypeJSON         = "application/json"
	responseTopicRPC        = "rpc/responses/01HXZ7QY"
	correlationData         = []byte("\x01\x02\x03\x04")
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
