// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

// ReasonCode is the single-byte reason code carried by CONNACK,
// PUBACK/REC/REL/COMP, SUBACK, UNSUBACK, DISCONNECT, and AUTH. The
// meaning of each value is context-dependent — see MQTT v5 §3.x.2 for
// which codes are valid in which packets.
type ReasonCode byte

// Success and granted-QoS codes (0x00 – 0x04).
const (
	ReasonSuccess                ReasonCode = 0x00 // CONNACK, PUBACK/REC/REL/COMP, SUBACK, UNSUBACK
	ReasonNormalDisconnection    ReasonCode = 0x00 // DISCONNECT
	ReasonGrantedQoS0            ReasonCode = 0x00 // SUBACK
	ReasonGrantedQoS1            ReasonCode = 0x01 // SUBACK
	ReasonGrantedQoS2            ReasonCode = 0x02 // SUBACK
	ReasonDisconnectWithWill     ReasonCode = 0x04 // DISCONNECT
	ReasonNoMatchingSubscribers  ReasonCode = 0x10 // PUBACK, PUBREC
	ReasonNoSubscriptionExisted  ReasonCode = 0x11 // UNSUBACK
	ReasonContinueAuthentication ReasonCode = 0x18 // AUTH
	ReasonReAuthenticate         ReasonCode = 0x19 // AUTH
)

// Error and refusal codes (0x80 – 0xA2).
const (
	ReasonUnspecifiedError                  ReasonCode = 0x80
	ReasonMalformedPacket                   ReasonCode = 0x81
	ReasonProtocolError                     ReasonCode = 0x82
	ReasonImplementationSpecificError       ReasonCode = 0x83
	ReasonUnsupportedProtocolVersion        ReasonCode = 0x84 // CONNACK
	ReasonClientIdentifierNotValid          ReasonCode = 0x85 // CONNACK
	ReasonBadUsernameOrPassword             ReasonCode = 0x86 // CONNACK
	ReasonNotAuthorized                     ReasonCode = 0x87
	ReasonServerUnavailable                 ReasonCode = 0x88 // CONNACK
	ReasonServerBusy                        ReasonCode = 0x89
	ReasonBanned                            ReasonCode = 0x8A // CONNACK
	ReasonServerShuttingDown                ReasonCode = 0x8B // DISCONNECT
	ReasonBadAuthenticationMethod           ReasonCode = 0x8C
	ReasonKeepAliveTimeout                  ReasonCode = 0x8D // DISCONNECT
	ReasonSessionTakenOver                  ReasonCode = 0x8E // DISCONNECT
	ReasonTopicFilterInvalid                ReasonCode = 0x8F
	ReasonTopicNameInvalid                  ReasonCode = 0x90
	ReasonPacketIdentifierInUse             ReasonCode = 0x91 // PUBACK, PUBREC, SUBACK, UNSUBACK
	ReasonPacketIdentifierNotFound          ReasonCode = 0x92 // PUBREL, PUBCOMP
	ReasonReceiveMaximumExceeded            ReasonCode = 0x93 // DISCONNECT
	ReasonTopicAliasInvalid                 ReasonCode = 0x94 // DISCONNECT
	ReasonPacketTooLarge                    ReasonCode = 0x95
	ReasonMessageRateTooHigh                ReasonCode = 0x96 // DISCONNECT
	ReasonQuotaExceeded                     ReasonCode = 0x97
	ReasonAdministrativeAction              ReasonCode = 0x98 // DISCONNECT
	ReasonPayloadFormatInvalid              ReasonCode = 0x99
	ReasonRetainNotSupported                ReasonCode = 0x9A
	ReasonQoSNotSupported                   ReasonCode = 0x9B
	ReasonUseAnotherServer                  ReasonCode = 0x9C
	ReasonServerMoved                       ReasonCode = 0x9D
	ReasonSharedSubscriptionsNotSupported   ReasonCode = 0x9E
	ReasonConnectionRateExceeded            ReasonCode = 0x9F
	ReasonMaximumConnectTime                ReasonCode = 0xA0 // DISCONNECT
	ReasonSubscriptionIDsNotSupported       ReasonCode = 0xA1 // SUBACK, DISCONNECT
	ReasonWildcardSubscriptionsNotSupported ReasonCode = 0xA2 // SUBACK, DISCONNECT
)

// IsError reports whether the reason code indicates an error (high bit set).
func (r ReasonCode) IsError() bool { return r >= 0x80 }
