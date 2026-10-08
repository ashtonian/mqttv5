// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/wire"
)

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
