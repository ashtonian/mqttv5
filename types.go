// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/ashtonian/mqttv5/internal/inflight"
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

// Errors returned by the client.
var (
	ErrNotConnected     = errors.New("mqttv5: client not connected")
	ErrAlreadyConnected = errors.New("mqttv5: client already connected")
	ErrClosed           = errors.New("mqttv5: client closed")
	ErrConnectRefused   = errors.New("mqttv5: broker refused CONNECT")
	ErrUnexpectedPacket = errors.New("mqttv5: unexpected packet from broker")

	// Enhanced-authentication errors (§4.12), returned by Reauthenticate.
	// ErrReauthRejected is wrapped with the broker's DISCONNECT reason
	// code (mirroring ErrConnectRefused) — match with errors.Is.
	ErrNoAuthenticator  = errors.New("mqttv5: no Authenticator configured")
	ErrReauthInProgress = errors.New("mqttv5: re-authentication already in progress")
	ErrReauthRejected   = errors.New("mqttv5: broker rejected re-authentication")

	// ErrWriteQueueFull is returned by QoS 0 Publish when the client
	// is configured with WriteDropNewest and the writer queue has no
	// room. The publish never reaches the wire.
	ErrWriteQueueFull = errors.New("mqttv5: write queue full")

	// Subscribe-time errors raised when the broker advertised the
	// feature as unsupported in CONNACK (MQTT v5 §3.2.2.3.{11,13}).
	// Returned before any SUBSCRIBE traffic goes on the wire.
	ErrSharedSubsUnsupported   = errors.New("mqttv5: broker does not support shared subscriptions")
	ErrWildcardSubsUnsupported = errors.New("mqttv5: broker does not support wildcard subscriptions")

	// ErrSessionLost completes a QoS 1/2 Publish whose message was
	// discarded because a connection started without the old session
	// and [WithSessionLossPolicy] is [SessionLossFail].
	ErrSessionLost = inflight.ErrSessionLost

	// ErrMessageExpired completes a QoS 1/2 Publish whose Message Expiry
	// Interval ran out before it could be sent again after a session
	// loss.
	ErrMessageExpired = inflight.ErrMessageExpired

	// ErrPacketIDsExhausted is returned when all 65,535 packet
	// identifiers are in use and the caller's context ends first.
	ErrPacketIDsExhausted = inflight.ErrIDsExhausted

	// ErrStoreFailed is matched by every [*StoreError]: a write to the
	// [WithStore] store failed.
	ErrStoreFailed = inflight.ErrStoreFailed

	// ErrQoSNotSupported is returned by Publish for a QoS above the
	// broker's Maximum QoS (see [WithQoSDowngrade]), and completes a
	// stored message that a reconnect to such a broker can no longer
	// send.
	ErrQoSNotSupported = inflight.ErrQoSNotSupported

	// ErrInvalidSessionExpiry is returned by [Client.DisconnectWith]
	// for a non-zero Session Expiry Interval when the CONNECT carried 0
	// [MQTT-3.14.2-2]. Nothing is sent.
	ErrInvalidSessionExpiry = errors.New("mqttv5: DISCONNECT cannot set a Session Expiry Interval when CONNECT sent 0")

	// ErrTopicAliasInvalid is returned by Publish for a Topic Alias the
	// broker's Topic Alias Maximum does not allow, or for a QoS 1/2
	// message that has an alias but no topic name.
	ErrTopicAliasInvalid = errors.New("mqttv5: invalid topic alias")

	// ErrInvalidTopic is returned for a topic name or filter MQTT v5
	// forbids (§4.7): wildcards in a topic name, misplaced wildcards in
	// a filter, a malformed shared subscription, NUL or invalid UTF-8.
	ErrInvalidTopic = wire.ErrInvalidTopic

	// ErrInvalidField is returned for any other option value MQTT v5
	// forbids, such as No Local on a shared subscription or DUP on a
	// QoS 0 publish. Nothing is sent.
	ErrInvalidField = wire.ErrInvalidField

	// ErrFieldTooLong is returned for a string or binary field over the
	// 65,535 bytes the protocol can encode.
	ErrFieldTooLong = wire.ErrStringTooLong

	// ErrRetainNotSupported is returned by Publish for a retained
	// message when the broker's Retain Available is 0.
	ErrRetainNotSupported = inflight.ErrRetainNotSupported

	// ErrPacketTooLarge is returned for a packet larger than the
	// broker's Maximum Packet Size, and matches a broker refusal with
	// reason code 0x95.
	ErrPacketTooLarge = inflight.ErrPacketTooLarge

	// Sentinels matched by [ReasonCodeError] for the broker refusals
	// callers most often branch on.
	ErrNotAuthorized        = errors.New("mqttv5: not authorized")
	ErrQuotaExceeded        = errors.New("mqttv5: quota exceeded")
	ErrTopicNameInvalid     = errors.New("mqttv5: topic name invalid")
	ErrPayloadFormatInvalid = errors.New("mqttv5: payload format invalid")
)

// StoreError reports a failed write to the [WithStore] store: Op names
// the write and Err is the store's error. errors.Is matches both
// [ErrStoreFailed] and Err.
type StoreError = inflight.StoreError

// ReasonCodeError is a broker's refusal: a reason code of 0x80 or above
// on CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK or DISCONNECT, with the
// optional diagnostics the broker attached.
//
// errors.Is matches it against [ErrConnectRefused] (for CONNACK) and
// against [ErrNotAuthorized], [ErrQuotaExceeded], [ErrTopicNameInvalid],
// [ErrPayloadFormatInvalid] and [ErrPacketTooLarge] by code. Use
// errors.As to read the code itself.
type ReasonCodeError struct {
	Packet          wire.PacketType
	Code            wire.ReasonCode
	ReasonString    string
	UserProperties  []wire.UserProperty
	ServerReference string
}

func (e *ReasonCodeError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "mqttv5: %s refused with reason code 0x%02X", e.Packet, byte(e.Code))
	if name := reasonName(e.Code); name != "" {
		b.WriteString(" (" + name + ")")
	}
	if e.ReasonString != "" {
		b.WriteString(": " + e.ReasonString)
	}
	return b.String()
}

// Is reports whether e corresponds to target.
func (e *ReasonCodeError) Is(target error) bool {
	switch target {
	case ErrConnectRefused:
		return e.Packet == wire.CONNACK
	case ErrNotAuthorized:
		return e.Code == wire.ReasonNotAuthorized
	case ErrQuotaExceeded:
		return e.Code == wire.ReasonQuotaExceeded
	case ErrTopicNameInvalid:
		return e.Code == wire.ReasonTopicNameInvalid
	case ErrPayloadFormatInvalid:
		return e.Code == wire.ReasonPayloadFormatInvalid
	case ErrPacketTooLarge:
		return e.Code == wire.ReasonPacketTooLarge
	}
	return false
}

// newReasonCodeError copies the diagnostics out of props, which belong
// to a frame the caller releases.
func newReasonCodeError(t wire.PacketType, rc wire.ReasonCode, props wire.Properties) *ReasonCodeError {
	return &ReasonCodeError{
		Packet:          t,
		Code:            rc,
		ReasonString:    cloneString(props, wire.PropReasonString),
		ServerReference: cloneString(props, wire.PropServerReference),
		UserProperties:  userProperties(props),
	}
}

// RejectedByDisconnectError reports a SUBSCRIBE or UNSUBSCRIBE the
// client gave up on: each of the Attempts times it was sent, the broker
// ended the connection with a DISCONNECT blaming a packet it received
// instead of answering. Some brokers do this for limits they do not
// announce, such as mosquitto's 200 topic levels. A Subscribe that fails
// this way has closed its subscription.
type RejectedByDisconnectError struct {
	Packet     PacketType // SUBSCRIBE or UNSUBSCRIBE
	Attempts   int
	Disconnect DisconnectInfo // the last of them
}

func (e *RejectedByDisconnectError) Error() string {
	msg := fmt.Sprintf("mqttv5: the broker closed the connection with DISCONNECT 0x%02X (%s) each of the %d times it received this %s",
		byte(e.Disconnect.ReasonCode), reasonName(e.Disconnect.ReasonCode), e.Attempts, e.Packet)
	if e.Disconnect.ReasonString != "" {
		msg += ": " + e.Disconnect.ReasonString
	}
	return msg
}

// ProtocolError is a broker packet that violates MQTT v5. The client
// sends DISCONNECT with Reason, closes the connection and reconnects.
type ProtocolError struct {
	// Reason is the DISCONNECT reason code sent: 0x81 Malformed Packet,
	// 0x82 Protocol Error, or a more specific code such as 0x93 Receive
	// Maximum exceeded.
	Reason wire.ReasonCode
	Packet wire.PacketType
	Detail string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("mqttv5: broker protocol violation in %s (0x%02X): %s", e.Packet, byte(e.Reason), e.Detail)
}

// reasonName names the reason codes a client sees as refusals.
func reasonName(rc wire.ReasonCode) string {
	switch rc {
	case wire.ReasonUnspecifiedError:
		return "unspecified error"
	case wire.ReasonMalformedPacket:
		return "malformed packet"
	case wire.ReasonProtocolError:
		return "protocol error"
	case wire.ReasonImplementationSpecificError:
		return "implementation specific error"
	case wire.ReasonUnsupportedProtocolVersion:
		return "unsupported protocol version"
	case wire.ReasonClientIdentifierNotValid:
		return "client identifier not valid"
	case wire.ReasonBadUsernameOrPassword:
		return "bad user name or password"
	case wire.ReasonNotAuthorized:
		return "not authorized"
	case wire.ReasonServerUnavailable:
		return "server unavailable"
	case wire.ReasonServerBusy:
		return "server busy"
	case wire.ReasonBanned:
		return "banned"
	case wire.ReasonBadAuthenticationMethod:
		return "bad authentication method"
	case wire.ReasonTopicFilterInvalid:
		return "topic filter invalid"
	case wire.ReasonTopicNameInvalid:
		return "topic name invalid"
	case wire.ReasonPacketIdentifierInUse:
		return "packet identifier in use"
	case wire.ReasonPacketIdentifierNotFound:
		return "packet identifier not found"
	case wire.ReasonPacketTooLarge:
		return "packet too large"
	case wire.ReasonQuotaExceeded:
		return "quota exceeded"
	case wire.ReasonPayloadFormatInvalid:
		return "payload format invalid"
	case wire.ReasonRetainNotSupported:
		return "retain not supported"
	case wire.ReasonQoSNotSupported:
		return "QoS not supported"
	case wire.ReasonUseAnotherServer:
		return "use another server"
	case wire.ReasonServerMoved:
		return "server moved"
	case wire.ReasonSharedSubscriptionsNotSupported:
		return "shared subscriptions not supported"
	case wire.ReasonConnectionRateExceeded:
		return "connection rate exceeded"
	case wire.ReasonSubscriptionIDsNotSupported:
		return "subscription identifiers not supported"
	case wire.ReasonWildcardSubscriptionsNotSupported:
		return "wildcard subscriptions not supported"
	}
	return ""
}
