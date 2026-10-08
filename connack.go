// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"slices"
	"strings"

	"github.com/ashtonian/mqttv5/wire"
)

// ConnackInfo is what the broker granted in CONNACK (§3.2.2.3), with the
// spec's defaults applied to properties it did not send. The client
// enforces these limits on everything it sends: a publish that exceeds
// them fails before it reaches the wire.
type ConnackInfo struct {
	// SessionPresent reports whether the broker resumed an existing
	// session.
	SessionPresent bool

	// SessionExpiry is the effective Session Expiry Interval in seconds:
	// the broker's value when it overrode the requested one.
	SessionExpiry uint32

	// KeepAlive is the effective keep-alive in seconds: the broker's
	// Server Keep Alive when sent, otherwise the requested value.
	// Zero disables keep-alive.
	KeepAlive uint16

	// ReceiveMaximum caps the QoS 1/2 PUBLISHes the client may have
	// unacknowledged (default 65,535).
	ReceiveMaximum uint16

	// MaximumQoS is the highest QoS the broker accepts (default 2).
	MaximumQoS byte

	// RetainAvailable reports whether retained messages are accepted
	// (default true).
	RetainAvailable bool

	// MaximumPacketSize is the largest packet the broker accepts, in
	// bytes; zero means no limit beyond the protocol's.
	MaximumPacketSize uint32

	// TopicAliasMaximum is the number of outbound topic aliases the
	// broker accepts (default 0: none).
	TopicAliasMaximum uint16

	// AssignedClientID is the identifier the broker chose when the
	// client connected without one.
	AssignedClientID string

	// Subscription features (default true when not sent).
	WildcardSubscriptionAvailable    bool
	SubscriptionIdentifiersAvailable bool
	SharedSubscriptionAvailable      bool

	ResponseInformation  string
	ServerReference      string
	ReasonString         string
	UserProperties       []wire.UserProperty
	AuthenticationMethod string
	AuthenticationData   []byte
}

// parseConnack reads every CONNACK property once, copying what it keeps
// out of the frame. sent is the CONNECT that produced it.
func parseConnack(p *wire.Connack, sent wire.ConnectOpts) ConnackInfo {
	props := p.Properties
	info := ConnackInfo{
		SessionPresent:                   p.SessionPresent,
		KeepAlive:                        sent.KeepAlive,
		ReceiveMaximum:                   65535,
		MaximumQoS:                       2,
		RetainAvailable:                  true,
		WildcardSubscriptionAvailable:    connackCapability(props, wire.PropWildcardSubAvailable),
		SubscriptionIdentifiersAvailable: connackCapability(props, wire.PropSubscriptionIDAvailable),
		SharedSubscriptionAvailable:      connackCapability(props, wire.PropSharedSubAvailable),
	}
	if sent.SessionExpiryInterval != nil {
		info.SessionExpiry = *sent.SessionExpiryInterval
	}
	if v, ok := props.Uint32(wire.PropSessionExpiryInterval); ok {
		info.SessionExpiry = v
	}
	if v, ok := props.Uint16(wire.PropServerKeepAlive); ok {
		info.KeepAlive = v
	}
	if v, ok := props.Uint16(wire.PropReceiveMaximum); ok && v > 0 {
		info.ReceiveMaximum = v
	}
	if v, ok := props.Byte(wire.PropMaximumQoS); ok && v < 2 {
		info.MaximumQoS = v
	}
	if v, ok := props.Byte(wire.PropRetainAvailable); ok {
		info.RetainAvailable = v != 0
	}
	if v, ok := props.Uint32(wire.PropMaximumPacketSize); ok {
		info.MaximumPacketSize = v
	}
	info.TopicAliasMaximum, _ = props.Uint16(wire.PropTopicAliasMaximum)
	info.AssignedClientID = cloneString(props, wire.PropAssignedClientID)
	info.ResponseInformation = cloneString(props, wire.PropResponseInformation)
	info.ServerReference = cloneString(props, wire.PropServerReference)
	info.ReasonString = cloneString(props, wire.PropReasonString)
	info.AuthenticationMethod = cloneString(props, wire.PropAuthMethod)
	if v, ok := props.Binary(wire.PropAuthData); ok {
		info.AuthenticationData = bytes.Clone(v)
	}
	info.UserProperties = userProperties(props)
	return info
}

func cloneString(props wire.Properties, id byte) string {
	v, _ := props.String(id)
	return strings.Clone(v)
}

// connackCapability reads one of the SubscriptionIdentifiersAvailable
// / WildcardSubscriptionAvailable / SharedSubscriptionAvailable
// CONNACK properties. The MQTT v5 default when the property is absent
// is "supported" (true) for all three.
func connackCapability(props wire.Properties, id byte) bool {
	v, ok := props.Byte(id)
	if !ok {
		return true
	}
	return v != 0
}

// ServerInfo returns what the broker granted on the current connection,
// and false when the client is not connected.
func (c *Client) ServerInfo() (ConnackInfo, bool) {
	cs := c.cur.Load()
	if cs == nil {
		return ConnackInfo{}, false
	}
	return cs.info.clone(), true
}

// clone deep-copies the slices so a caller cannot modify the client's
// copy.
func (i ConnackInfo) clone() ConnackInfo {
	i.UserProperties = slices.Clone(i.UserProperties)
	i.AuthenticationData = bytes.Clone(i.AuthenticationData)
	return i
}
