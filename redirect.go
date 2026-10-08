// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
)

// ServerRedirect is a broker's request that the client use another
// server (§4.11): a CONNACK or DISCONNECT with reason 0x9C Use another
// server or 0x9D Server moved, carrying a Server Reference.
type ServerRedirect struct {
	Packet    PacketType // CONNACK or DISCONNECT
	Reason    ReasonCode
	Reference string // as the broker sent it
}

// Permanent reports a move (0x9D Server moved) rather than a temporary
// redirection (0x9C Use another server).
func (r ServerRedirect) Permanent() bool { return r.Reason == ReasonServerMoved }

// nextBrokerURL is the URL the next connection attempt dials: a pending
// temporary redirect, once, else the current entry of the broker list.
func (c *Client) nextBrokerURL() string {
	if u := c.redirectOnce.Swap(nil); u != nil {
		return *u
	}
	return c.currentBrokerURL()
}

// redirectFrom handles a CONNACK refusal that redirects.
func (c *Client) redirectFrom(err error) {
	var rce *ReasonCodeError
	if errors.As(err, &rce) && rce.Packet == CONNACK {
		c.redirect(ServerRedirect{Packet: CONNACK, Reason: rce.Code, Reference: rce.ServerReference})
	}
}

// redirect reports r and, with WithFollowServerRedirects, points the
// next connection at its reference: a move replaces the broker list, a
// temporary redirection applies to the next attempt only.
func (c *Client) redirect(r ServerRedirect) {
	if r.Reference == "" || (r.Reason != ReasonUseAnotherServer && r.Reason != ReasonServerMoved) {
		return
	}
	if c.cfg.OnServerRedirect != nil {
		c.cfg.OnServerRedirect(r)
	}
	if !c.cfg.FollowServerRedirects {
		return
	}
	target, err := resolveServerReference(c.currentBrokerURL(), r.Reference)
	if err == nil {
		err = validateBrokerURLs([]string{target}, c.cfg.DialFunc != nil)
	}
	if err != nil {
		c.cfg.Logger.Warn("mqttv5: ignoring a server redirect", slog.String("reference", r.Reference), slog.Any("error", err))
		return
	}
	c.cfg.Logger.Info("mqttv5: following a server redirect", slog.String("to", target), slog.Bool("permanent", r.Permanent()))
	if r.Permanent() {
		_ = c.SetBrokers(target)
		return
	}
	c.redirectOnce.Store(&target)
}

// resolveServerReference turns a Server Reference into a broker URL.
// MQTT leaves its format open (§4.11): a full URL is used as is; a host
// or host:port keeps the scheme and path of current. Of several
// space-separated references the first is used.
func resolveServerReference(current, ref string) (string, error) {
	ref, _, _ = strings.Cut(strings.TrimSpace(ref), " ")
	if strings.Contains(ref, "://") {
		return ref, nil
	}
	u, err := url.Parse(current)
	if err != nil {
		return "", err
	}
	if ref == "" || strings.ContainsAny(ref, "/?#@") {
		return "", fmt.Errorf("server reference %q is not a host or host:port", ref)
	}
	u.Host = ref
	return u.String(), nil
}
