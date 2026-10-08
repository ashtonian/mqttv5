// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/ashtonian/mqttv5/wire"
)

// The client's own API types. The wire package is the codec underneath:
// exported for tools and tests, but not part of the client's stable API,
// so these types carry what applications need without wire's pooled
// packets, frame-aliasing views or protocol bookkeeping fields.

// ReasonCode is an MQTT v5 reason code (§2.4). Codes of 0x80 and above
// are errors; see IsError and String.
type ReasonCode = wire.ReasonCode

// PacketType is an MQTT control packet type.
type PacketType = wire.PacketType

// UserProperty is one User Property name/value pair. A packet may carry
// the same name more than once.
type UserProperty = wire.UserProperty

// Reason codes, as named by MQTT v5 §2.4. Several share a value; the
// packet they arrive in decides the meaning.
const (
	ReasonSuccess                           = wire.ReasonSuccess
	ReasonNormalDisconnection               = wire.ReasonNormalDisconnection
	ReasonGrantedQoS0                       = wire.ReasonGrantedQoS0
	ReasonGrantedQoS1                       = wire.ReasonGrantedQoS1
	ReasonGrantedQoS2                       = wire.ReasonGrantedQoS2
	ReasonDisconnectWithWill                = wire.ReasonDisconnectWithWill
	ReasonNoMatchingSubscribers             = wire.ReasonNoMatchingSubscribers
	ReasonNoSubscriptionExisted             = wire.ReasonNoSubscriptionExisted
	ReasonContinueAuthentication            = wire.ReasonContinueAuthentication
	ReasonReAuthenticate                    = wire.ReasonReAuthenticate
	ReasonUnspecifiedError                  = wire.ReasonUnspecifiedError
	ReasonMalformedPacket                   = wire.ReasonMalformedPacket
	ReasonProtocolError                     = wire.ReasonProtocolError
	ReasonImplementationSpecificError       = wire.ReasonImplementationSpecificError
	ReasonUnsupportedProtocolVersion        = wire.ReasonUnsupportedProtocolVersion
	ReasonClientIdentifierNotValid          = wire.ReasonClientIdentifierNotValid
	ReasonBadUsernameOrPassword             = wire.ReasonBadUsernameOrPassword
	ReasonNotAuthorized                     = wire.ReasonNotAuthorized
	ReasonServerUnavailable                 = wire.ReasonServerUnavailable
	ReasonServerBusy                        = wire.ReasonServerBusy
	ReasonBanned                            = wire.ReasonBanned
	ReasonServerShuttingDown                = wire.ReasonServerShuttingDown
	ReasonBadAuthenticationMethod           = wire.ReasonBadAuthenticationMethod
	ReasonKeepAliveTimeout                  = wire.ReasonKeepAliveTimeout
	ReasonSessionTakenOver                  = wire.ReasonSessionTakenOver
	ReasonTopicFilterInvalid                = wire.ReasonTopicFilterInvalid
	ReasonTopicNameInvalid                  = wire.ReasonTopicNameInvalid
	ReasonPacketIdentifierInUse             = wire.ReasonPacketIdentifierInUse
	ReasonPacketIdentifierNotFound          = wire.ReasonPacketIdentifierNotFound
	ReasonReceiveMaximumExceeded            = wire.ReasonReceiveMaximumExceeded
	ReasonTopicAliasInvalid                 = wire.ReasonTopicAliasInvalid
	ReasonPacketTooLarge                    = wire.ReasonPacketTooLarge
	ReasonMessageRateTooHigh                = wire.ReasonMessageRateTooHigh
	ReasonQuotaExceeded                     = wire.ReasonQuotaExceeded
	ReasonAdministrativeAction              = wire.ReasonAdministrativeAction
	ReasonPayloadFormatInvalid              = wire.ReasonPayloadFormatInvalid
	ReasonRetainNotSupported                = wire.ReasonRetainNotSupported
	ReasonQoSNotSupported                   = wire.ReasonQoSNotSupported
	ReasonUseAnotherServer                  = wire.ReasonUseAnotherServer
	ReasonServerMoved                       = wire.ReasonServerMoved
	ReasonSharedSubscriptionsNotSupported   = wire.ReasonSharedSubscriptionsNotSupported
	ReasonConnectionRateExceeded            = wire.ReasonConnectionRateExceeded
	ReasonMaximumConnectTime                = wire.ReasonMaximumConnectTime
	ReasonSubscriptionIDsNotSupported       = wire.ReasonSubscriptionIDsNotSupported
	ReasonWildcardSubscriptionsNotSupported = wire.ReasonWildcardSubscriptionsNotSupported
)

// Control packet types.
const (
	CONNECT     = wire.CONNECT
	CONNACK     = wire.CONNACK
	PUBLISH     = wire.PUBLISH
	PUBACK      = wire.PUBACK
	PUBREC      = wire.PUBREC
	PUBREL      = wire.PUBREL
	PUBCOMP     = wire.PUBCOMP
	SUBSCRIBE   = wire.SUBSCRIBE
	SUBACK      = wire.SUBACK
	UNSUBSCRIBE = wire.UNSUBSCRIBE
	UNSUBACK    = wire.UNSUBACK
	PINGREQ     = wire.PINGREQ
	PINGRESP    = wire.PINGRESP
	DISCONNECT  = wire.DISCONNECT
	AUTH        = wire.AUTH
)

// PublishOptions is a message to publish. Topic is required unless a
// QoS 0 message uses TopicAlias; optional properties are sent only when
// set (a nil pointer or empty string or slice is absent).
type PublishOptions struct {
	Topic   string
	Payload []byte
	QoS     byte // 0, 1 or 2
	Retain  bool

	// PayloadFormatIndicator 1 declares the payload UTF-8 text, 0 bytes.
	PayloadFormatIndicator *byte
	// MessageExpiryInterval is the message's lifetime in seconds; 0 is
	// valid (deliver now or never).
	MessageExpiryInterval *uint32
	ContentType           string
	ResponseTopic         string
	CorrelationData       []byte
	// TopicAlias sends an alias the broker already knows instead of the
	// topic (QoS 0 only); see also WithOutboundTopicAliases.
	TopicAlias     uint16
	UserProperties []UserProperty
}

// wire returns the codec's options for o; the slices are shared.
func (o PublishOptions) wire() wire.PublishOpts {
	return wire.PublishOpts{
		Topic: o.Topic, Payload: o.Payload, QoS: o.QoS, Retain: o.Retain,
		PayloadFormatIndicator: o.PayloadFormatIndicator, MessageExpiryInterval: o.MessageExpiryInterval,
		ContentType: o.ContentType, ResponseTopic: o.ResponseTopic, CorrelationData: o.CorrelationData,
		TopicAlias: o.TopicAlias, UserProperties: o.UserProperties,
	}
}

// clone returns a deep copy of o.
func (o PublishOptions) clone() PublishOptions {
	return publishOptionsOf(o.wire().Clone())
}

func publishOptionsOf(w wire.PublishOpts) PublishOptions {
	return PublishOptions{
		Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain,
		PayloadFormatIndicator: w.PayloadFormatIndicator, MessageExpiryInterval: w.MessageExpiryInterval,
		ContentType: w.ContentType, ResponseTopic: w.ResponseTopic, CorrelationData: w.CorrelationData,
		TopicAlias: w.TopicAlias, UserProperties: w.UserProperties,
	}
}

// EncodePublish encodes o as a PUBLISH packet with packet identifier id
// (ignored for QoS 0). For tools and storage formats that need the
// message's exact MQTT encoding, such as a PublisherQueue.
func EncodePublish(o PublishOptions, id uint16) ([]byte, error) {
	w := o.wire()
	w.PacketID = id
	return wire.MarshalPublish(w)
}

// DecodePublish is the inverse of EncodePublish: it returns the options
// of the PUBLISH packet in frame and its packet identifier. The options
// own their memory.
func DecodePublish(frame []byte) (PublishOptions, uint16, error) {
	pkt, err := wire.NewDecoderSize(bytes.NewReader(frame), 512).ReadPacket()
	if err != nil {
		return PublishOptions{}, 0, err
	}
	defer pkt.Release()
	pub, ok := pkt.(*wire.Publish)
	if !ok {
		return PublishOptions{}, 0, fmt.Errorf("mqttv5: frame holds %s, not PUBLISH", pkt.Type())
	}
	w := pub.Opts()
	return publishOptionsOf(w), w.PacketID, nil
}

// TopicFilter is one entry of a SUBSCRIBE: a topic filter and its
// subscription options (§3.8.3.1). Only Topic is required.
//
//	{Topic: "events/#"}                        // QoS 0
//	{Topic: "events/#", QoS: 1}                // at-least-once
//	{Topic: "events/#", QoS: 1, NoLocal: true} // don't echo own publishes back
type TopicFilter struct {
	Topic             string
	QoS               byte // maximum QoS: 0, 1 or 2
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte // 0 send retained, 1 only for a new subscription, 2 never
}

func wireFilters(filters []TopicFilter) []wire.SubscribeFilter {
	out := make([]wire.SubscribeFilter, len(filters))
	for i, f := range filters {
		out[i] = wire.SubscribeFilter(f)
	}
	return out
}

// WillOptions is the Will Message the broker publishes for the client
// when its connection ends without a DISCONNECT (§3.1.3.2).
type WillOptions struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool

	// DelayInterval delays the Will by this many seconds after the
	// connection ends; nil or 0 publishes it at once.
	DelayInterval          *uint32
	PayloadFormatIndicator *byte
	MessageExpiryInterval  *uint32
	ContentType            string
	ResponseTopic          string
	CorrelationData        []byte
	UserProperties         []UserProperty
}

func (w *WillOptions) wire() *wire.WillOpts {
	if w == nil {
		return nil
	}
	return &wire.WillOpts{
		Topic: w.Topic, Payload: w.Payload, QoS: w.QoS, Retain: w.Retain,
		WillDelayInterval: w.DelayInterval, PayloadFormatIndicator: w.PayloadFormatIndicator,
		MessageExpiryInterval: w.MessageExpiryInterval, ContentType: w.ContentType,
		ResponseTopic: w.ResponseTopic, CorrelationData: w.CorrelationData, UserProperties: w.UserProperties,
	}
}

// ConnectOptions are the parts of a CONNECT that
// [WithConnectPacketBuilder] may change before each attempt, typically to
// present a fresh credential.
type ConnectOptions struct {
	Username       string
	Password       []byte
	UserProperties []UserProperty
}

// DisconnectOptions is the DISCONNECT [Client.DisconnectWith] sends.
type DisconnectOptions struct {
	ReasonCode ReasonCode // 0x00 Normal disconnection, 0x04 with Will, or an error code
	// SessionExpiryInterval replaces the value CONNECT set; it cannot
	// be non-zero if CONNECT sent 0 [MQTT-3.14.2-2].
	SessionExpiryInterval *uint32
	ReasonString          string
	UserProperties        []UserProperty
}

func (o DisconnectOptions) wire() wire.DisconnectOpts {
	return wire.DisconnectOpts{
		ReasonCode: o.ReasonCode, SessionExpiryInterval: o.SessionExpiryInterval,
		ReasonString: o.ReasonString, UserProperties: o.UserProperties,
	}
}

// DisconnectInfo is a DISCONNECT the broker sent.
type DisconnectInfo struct {
	ReasonCode   ReasonCode
	ReasonString string
	// ServerReference names another server to use, with reason 0x9C
	// Use another server or 0x9D Server moved.
	ServerReference string
	UserProperties  []UserProperty
}

func disconnectInfoOf(d *wire.Disconnect) DisconnectInfo {
	return DisconnectInfo{
		ReasonCode:      d.ReasonCode,
		ReasonString:    cloneString(d.Properties, wire.PropReasonString),
		ServerReference: cloneString(d.Properties, wire.PropServerReference),
		UserProperties:  userProperties(d.Properties),
	}
}

// Properties are the properties of a received PUBLISH (§3.3.2.3). Their
// memory follows the Message's: owned unless the subscription is
// zero-copy.
type Properties struct {
	p wire.Properties
}

// PayloadFormatIndicator returns the Payload Format Indicator: 1 for
// UTF-8 text, 0 for bytes.
func (p Properties) PayloadFormatIndicator() (byte, bool) { return p.p.Byte(wire.PropPayloadFormat) }

// MessageExpiryInterval returns the lifetime in seconds the broker left
// the message.
func (p Properties) MessageExpiryInterval() (uint32, bool) {
	return p.p.Uint32(wire.PropMessageExpiryInterval)
}

// ContentType returns the Content Type, or "".
func (p Properties) ContentType() string { v, _ := p.p.String(wire.PropContentType); return v }

// ResponseTopic returns the Response Topic, or "".
func (p Properties) ResponseTopic() string { v, _ := p.p.String(wire.PropResponseTopic); return v }

// CorrelationData returns the Correlation Data, or nil.
func (p Properties) CorrelationData() []byte { v, _ := p.p.Binary(wire.PropCorrelationData); return v }

// UserProperties yields every User Property in order.
func (p Properties) UserProperties() iter.Seq2[string, string] { return p.p.UserProperties() }

// UserProperty returns the first value of the User Property named key.
func (p Properties) UserProperty(key string) (string, bool) {
	for k, v := range p.p.UserProperties() {
		if k == key {
			return v, true
		}
	}
	return "", false
}

// SubscriptionIdentifiers yields the identifiers of the subscriptions the
// broker matched (§3.3.4).
func (p Properties) SubscriptionIdentifiers() iter.Seq[uint32] { return p.p.SubscriptionIdentifiers() }

// userProperties copies a packet's User Properties.
func userProperties(p wire.Properties) []UserProperty {
	var out []UserProperty
	for k, v := range p.UserProperties() {
		out = append(out, UserProperty{Key: strings.Clone(k), Value: strings.Clone(v)})
	}
	return slices.Clip(out)
}
