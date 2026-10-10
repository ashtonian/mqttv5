// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package wire

import (
	"errors"
	"fmt"
)

// ErrInvalidField marks an option value MQTT v5 does not allow; the
// packet is not written.
var ErrInvalidField = errors.New("mqttv5: invalid field")

// The encoders check their options with these before writing anything,
// so an invalid packet is never sent and no length is ever truncated to
// fit its 2-byte prefix.

func checkString(field, s string) error {
	if len(s) > 0xffff {
		return fmt.Errorf("%w: %s is %d bytes", ErrStringTooLong, field, len(s))
	}
	if !validUTF8(s) {
		return fmt.Errorf("%w: %s is not well-formed UTF-8 or contains U+0000", ErrInvalidField, field)
	}
	return nil
}

func checkBinary(field string, b []byte) error {
	if len(b) > 0xffff {
		return fmt.Errorf("%w: %s is %d bytes", ErrStringTooLong, field, len(b))
	}
	return nil
}

func checkUserProps(props []UserProperty) error {
	for _, p := range props {
		if err := checkString("user property name", p.Key); err != nil {
			return err
		}
		if err := checkString("user property value", p.Value); err != nil {
			return err
		}
	}
	return nil
}

func checkBool(field string, b *byte) error {
	if b != nil && *b > 1 {
		return fmt.Errorf("%w: %s must be 0 or 1, got %d", ErrInvalidField, field, *b)
	}
	return nil
}

func checkReasonCode(t PacketType, rc ReasonCode) error {
	if perr := checkReason(t, rc); perr != nil {
		return fmt.Errorf("%w: reason code %#x is not defined for %s", ErrInvalidField, byte(rc), t)
	}
	return nil
}

func validatePublishOpts(o *PublishOpts) error {
	if o.QoS > 2 {
		return ErrInvalidQoS
	}
	if o.QoS > 0 && o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if o.Dup && o.QoS == 0 {
		return fmt.Errorf("%w: DUP set on a QoS 0 PUBLISH", ErrInvalidField)
	}
	if o.Topic == "" && o.TopicAlias == 0 {
		return fmt.Errorf("%w: empty topic name without a Topic Alias", ErrInvalidTopic)
	}
	if err := ValidTopicName(o.Topic); err != nil {
		return err
	}
	if o.ResponseTopic != "" {
		if err := ValidTopicName(o.ResponseTopic); err != nil {
			return fmt.Errorf("response topic: %w", err)
		}
	}
	if err := checkBool("Payload Format Indicator", o.PayloadFormatIndicator); err != nil {
		return err
	}
	if err := checkString("content type", o.ContentType); err != nil {
		return err
	}
	if err := checkBinary("correlation data", o.CorrelationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateWillOpts(w *WillOpts) error {
	if w.Topic == "" {
		return fmt.Errorf("%w: empty will topic", ErrInvalidTopic)
	}
	if err := ValidTopicName(w.Topic); err != nil {
		return fmt.Errorf("will topic: %w", err)
	}
	if w.QoS > 2 {
		return ErrInvalidQoS
	}
	if w.ResponseTopic != "" {
		if err := ValidTopicName(w.ResponseTopic); err != nil {
			return fmt.Errorf("will response topic: %w", err)
		}
	}
	if err := checkBool("will Payload Format Indicator", w.PayloadFormatIndicator); err != nil {
		return err
	}
	if err := checkString("will content type", w.ContentType); err != nil {
		return err
	}
	if err := checkBinary("will payload", w.Payload); err != nil {
		return err
	}
	if err := checkBinary("will correlation data", w.CorrelationData); err != nil {
		return err
	}
	return checkUserProps(w.UserProperties)
}

func validateConnectOpts(o *ConnectOpts) error {
	if err := checkString("client identifier", o.ClientID); err != nil {
		return err
	}
	if err := checkString("user name", o.Username); err != nil {
		return err
	}
	if err := checkBinary("password", o.Password); err != nil {
		return err
	}
	if o.Will != nil {
		if err := validateWillOpts(o.Will); err != nil {
			return err
		}
	}
	if o.ReceiveMaximum != nil && *o.ReceiveMaximum == 0 {
		return fmt.Errorf("%w: Receive Maximum 0", ErrInvalidField)
	}
	if o.MaximumPacketSize != nil && *o.MaximumPacketSize == 0 {
		return fmt.Errorf("%w: Maximum Packet Size 0", ErrInvalidField)
	}
	if err := checkBool("Request Response Information", o.RequestResponseInformation); err != nil {
		return err
	}
	if err := checkBool("Request Problem Information", o.RequestProblemInformation); err != nil {
		return err
	}
	if err := checkString("authentication method", o.AuthenticationMethod); err != nil {
		return err
	}
	if o.AuthenticationData != nil && o.AuthenticationMethod == "" {
		return fmt.Errorf("%w: authentication data without an authentication method", ErrInvalidField)
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateSubscribeOpts(o *SubscribeOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if len(o.Filters) == 0 {
		return ErrEmptyFilterList
	}
	for _, f := range o.Filters {
		if err := ValidTopicFilter(f.Topic); err != nil {
			return err
		}
		if f.QoS > 2 {
			return ErrInvalidQoS
		}
		if f.RetainHandling > 2 {
			return fmt.Errorf("%w: Retain Handling %d", ErrInvalidField, f.RetainHandling)
		}
		if f.NoLocal && IsSharedFilter(f.Topic) {
			return fmt.Errorf("%w: No Local on shared subscription %q [MQTT-3.8.3-4]", ErrInvalidField, f.Topic)
		}
	}
	if id := o.SubscriptionIdentifier; id != nil && (*id == 0 || *id > MaxVarintValue) {
		return fmt.Errorf("%w: Subscription Identifier %d outside 1..%d", ErrInvalidField, *id, MaxVarintValue)
	}
	return checkUserProps(o.UserProperties)
}

func validateUnsubscribeOpts(o *UnsubscribeOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if len(o.Topics) == 0 {
		return ErrEmptyFilterList
	}
	for _, f := range o.Topics {
		if err := ValidTopicFilter(f); err != nil {
			return err
		}
	}
	return checkUserProps(o.UserProperties)
}

func validatePubRespOpts(t PacketType, o *PubRespOpts) error {
	if o.PacketID == 0 {
		return ErrPacketIDRequired
	}
	if err := checkReasonCode(t, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateDisconnectOpts(o *DisconnectOpts) error {
	if err := checkReasonCode(DISCONNECT, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	if err := checkString("server reference", o.ServerReference); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateAuthOpts(o *AuthOpts) error {
	if err := checkReasonCode(AUTH, o.ReasonCode); err != nil {
		return err
	}
	if err := checkString("authentication method", o.AuthenticationMethod); err != nil {
		return err
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	if err := checkString("reason string", o.ReasonString); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}

func validateAckList(t PacketType, id uint16, codes []ReasonCode, reason string, props []UserProperty) error {
	if id == 0 {
		return ErrPacketIDRequired
	}
	if len(codes) == 0 {
		return fmt.Errorf("%w: %s needs at least one reason code", ErrInvalidField, t)
	}
	for _, rc := range codes {
		if err := checkReasonCode(t, rc); err != nil {
			return err
		}
	}
	if err := checkString("reason string", reason); err != nil {
		return err
	}
	return checkUserProps(props)
}

func validateConnackOpts(o *ConnackOpts) error {
	if err := checkReasonCode(CONNACK, o.ReasonCode); err != nil {
		return err
	}
	for _, f := range []struct {
		name string
		v    *byte
	}{{"Maximum QoS", o.MaximumQoS}, {"Retain Available", o.RetainAvailable},
		{"Wildcard Subscription Available", o.WildcardSubscriptionAvailable},
		{"Subscription Identifier Available", o.SubscriptionIdentifierAvailable},
		{"Shared Subscription Available", o.SharedSubscriptionAvailable}} {
		if err := checkBool(f.name, f.v); err != nil {
			return err
		}
	}
	if o.ReceiveMaximum != nil && *o.ReceiveMaximum == 0 {
		return fmt.Errorf("%w: Receive Maximum 0", ErrInvalidField)
	}
	if o.MaximumPacketSize != nil && *o.MaximumPacketSize == 0 {
		return fmt.Errorf("%w: Maximum Packet Size 0", ErrInvalidField)
	}
	for _, s := range []struct{ name, v string }{{"assigned client identifier", o.AssignedClientIdentifier},
		{"reason string", o.ReasonString}, {"response information", o.ResponseInformation},
		{"server reference", o.ServerReference}, {"authentication method", o.AuthenticationMethod}} {
		if err := checkString(s.name, s.v); err != nil {
			return err
		}
	}
	if err := checkBinary("authentication data", o.AuthenticationData); err != nil {
		return err
	}
	return checkUserProps(o.UserProperties)
}
