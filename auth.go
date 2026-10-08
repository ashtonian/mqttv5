// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/ashtonian/mqttv5/wire"
)

// writeDisconnectAndClose synchronously writes a DISCONNECT with the
// given opts, waits briefly for the write to flush, then signals the
// connection down. Used by error paths that need the broker to see
// our reason code before the socket disappears (e.g. mid-session
// auth failure).
func (c *Client) writeDisconnectAndClose(cs *connState, opts wire.DisconnectOpts) {
	done := make(chan error, 1)
	req := writeReq{
		fn: func(w io.Writer) (int64, error) {
			return wire.WriteDisconnect(w, opts)
		},
		done: done,
	}
	if err := cs.queue(context.Background(), req, false); err != nil {
		// Write queue full / writer dead — best-effort only.
		c.cfg.Logger.Debug("mqttv5: DISCONNECT dropped (writer wedged)",
			slog.Int("reason", int(opts.ReasonCode)), slog.Any("error", err))
	} else {
		select {
		case <-done:
		case <-c.cfg.clock.After(c.cfg.DisconnectFlushTimeout):
		case <-cs.dying:
		}
	}
	cs.signalDown()
}

// handleServerAuth processes an AUTH packet after CONNACK (§4.12).
//
// Only the client starts a re-authentication (AUTH 0x19); the broker
// answers within it with 0x18 Continue, under the same Authentication
// Method, or ends it with 0x00 Success (§3.15.2.1). An AUTH with no
// re-authentication in progress, with another reason code, or with a
// different method is a protocol error: DISCONNECT 0x82.
func (c *Client) handleServerAuth(cs *connState, p *wire.Auth) {
	reason := p.ReasonCode
	method, hasMethod := p.Properties.String(wire.PropAuthMethod)
	method = strings.Clone(method)
	brokerData := authData(p.Properties)
	p.Release()

	call := cs.reauth.Load()
	var violation string
	switch {
	case call == nil:
		violation = "AUTH with no re-authentication in progress"
	case reason != wire.ReasonSuccess && reason != wire.ReasonContinueAuthentication:
		violation = fmt.Sprintf("AUTH reason %#x during re-authentication", byte(reason))
	case reason == wire.ReasonContinueAuthentication && (!hasMethod || method != c.cfg.Authenticator.Method()),
		reason == wire.ReasonSuccess && hasMethod && method != c.cfg.Authenticator.Method():
		violation = fmt.Sprintf("AUTH method %q, re-authentication uses %q", method, c.cfg.Authenticator.Method())
	}
	if violation != "" {
		c.protocolViolation(cs, &ProtocolError{Reason: wire.ReasonProtocolError, Packet: wire.AUTH, Detail: violation})
		return
	}
	c.handleReauthInbound(cs, call, reason, brokerData)
}

// authData copies the Authentication Data property out of a frame the
// caller is about to release.
func authData(props wire.Properties) []byte {
	b, _ := props.Binary(wire.PropAuthData)
	return append([]byte(nil), b...)
}

// authContinue asks the Authenticator for the response to a broker
// challenge, through ContinueContext when it supports cancellation.
func (c *Client) authContinue(ctx context.Context, brokerData []byte) ([]byte, error) {
	if ca, ok := c.cfg.Authenticator.(ContextAuthenticator); ok {
		response, _, err := ca.ContinueContext(ctx, brokerData)
		return response, err
	}
	response, _, err := c.cfg.Authenticator.Continue(brokerData)
	return response, err
}

// reauthCall tracks one in-flight client-initiated re-authentication
// (§4.12). result delivers the terminal outcome to the Reauthenticate
// caller: nil on AUTH 0x00 Success, or a non-nil error if the
// Authenticator fails mid-exchange. It is buffered (cap 1) so the read
// loop never blocks delivering the outcome even when the caller has
// stopped waiting (ctx elapsed). Broker rejection (DISCONNECT) and plain
// drops are reported via cs.dying, not result.
type reauthCall struct {
	result chan error
}

// Reauthenticate initiates MQTT v5 re-authentication (§4.12) on the live
// connection: it sends an AUTH 0x19 carrying the configured
// Authenticator's Method() and a fresh Begin(ctx) payload, services any
// broker 0x18 Continue challenges via Authenticator.Continue, and returns
// when the broker concludes the exchange — without dropping the
// connection or disturbing in-flight QoS state.
//
// It returns nil when the broker accepts re-authentication (AUTH 0x00
// Success), or:
//   - ErrNoAuthenticator if the client was not built WithAuthenticator;
//   - ErrNotConnected if there is no live connection (before the first
//     CONNACK, or after a drop);
//   - ErrReauthInProgress if another re-auth is already in flight on this
//     connection (calls are single-flighted per connection);
//   - ErrReauthRejected (wrapped with the DISCONNECT reason code) if the
//     broker rejects re-auth and tears the connection down;
//   - ctx.Err() if ctx is cancelled or its deadline elapses first.
//
// The AuthenticationMethod established at CONNECT is reused; Method() MUST
// be stable for the client's lifetime (§4.12 requires it to match across
// the exchange). Reauthenticate is safe for concurrent use.
//
// ctx bounds the whole operation including credential resolution via
// Begin(ctx). On ctx cancellation after the AUTH 0x19 has been sent the
// connection is left intact and the exchange resolves on the wire — re-auth
// stays single-flighted until the broker concludes, so a subsequent call
// may observe ErrReauthInProgress until then. On a broker rejection the
// connection is torn down and the supervisor reconnects through the normal
// CONNECT path (which re-presents credentials).
func (c *Client) Reauthenticate(ctx context.Context) error {
	if c.cfg.Authenticator == nil {
		return ErrNoAuthenticator
	}
	cs := c.cur.Load()
	if cs == nil {
		return ErrNotConnected
	}

	call := &reauthCall{result: make(chan error, 1)}
	if !cs.reauth.CompareAndSwap(nil, call) {
		return ErrReauthInProgress
	}

	// Resolve a fresh credential. ctx bounds any I/O Begin performs.
	// Nothing is on the wire yet, so a failure here releases the slot.
	data, err := c.cfg.Authenticator.Begin(ctx)
	if err != nil {
		cs.reauth.CompareAndSwap(call, nil)
		return fmt.Errorf("mqttv5: Authenticator.Begin: %w", err)
	}
	method := c.cfg.Authenticator.Method()

	// Send AUTH 0x19 to initiate. Until it is enqueued nothing is on the
	// wire, so the ctx/dying/shutdown exits may still release the slot.
	authFn := func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{
			ReasonCode:           wire.ReasonReAuthenticate,
			AuthenticationMethod: method,
			AuthenticationData:   data,
		})
	}
	if err := cs.queue(ctx, writeReq{fn: authFn}, true); err != nil {
		cs.reauth.CompareAndSwap(call, nil)
		return err
	}

	// The 0x19 is queued and will flush. The read loop now owns the
	// exchange: it answers 0x18 challenges and delivers the terminal
	// outcome on call.result. From here the slot is released only by the
	// read loop (on 0x00) or by this connState being disposed on teardown
	// — never on ctx cancel — so a late terminal AUTH cannot land on a
	// subsequent Reauthenticate.
	select {
	case err := <-call.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cs.dying:
		// A delivered result (e.g. a server-verification failure that
		// also tore the connection down) takes precedence over the
		// generic down error when both are ready.
		select {
		case err := <-call.result:
			return err
		default:
			return c.reauthDownError(cs)
		}
	case <-c.done():
		return ErrClosed
	}
}

// reauthDownError maps a connection teardown observed mid-re-auth to the
// right error: a broker DISCONNECT (rejection) surfaces as
// ErrReauthRejected with its reason code; a plain socket drop surfaces as
// ErrNotConnected.
func (c *Client) reauthDownError(cs *connState) error {
	if sd := cs.serverDisconnect.Load(); sd != nil {
		return fmt.Errorf("%w: reason 0x%02X", ErrReauthRejected, byte(sd.ReasonCode))
	}
	return ErrNotConnected
}

// fireReauthenticated invokes the OnReauthenticated observability hook (if
// configured) on the read-loop goroutine. The callback must not block.
func (c *Client) fireReauthenticated() {
	if c.cfg.OnReauthenticated != nil {
		c.cfg.OnReauthenticated()
	}
}

// handleReauthInbound services a validated broker AUTH within a
// client-initiated re-authentication (slot held by call). Runs on the
// read-loop goroutine.
//
//   - 0x00 Success: terminal — release the slot and signal the waiter.
//   - otherwise (0x18 Continue): feed the Authenticator and reply 0x18,
//     keeping the slot so the exchange continues. A Continue error tears
//     the connection down; the waiter then observes the drop via cs.dying.
func (c *Client) handleReauthInbound(cs *connState, call *reauthCall, pktReason wire.ReasonCode, brokerData []byte) {
	if pktReason == wire.ReasonSuccess {
		cs.reauth.CompareAndSwap(call, nil)
		// Mutual verification (§4.12): if the Authenticator verifies the
		// server's final data, a failure means the broker is not trusted
		// — report it to the caller and tear the connection down.
		if v, ok := c.cfg.Authenticator.(ServerFinalVerifier); ok {
			if err := v.VerifyServerFinal(brokerData); err != nil {
				call.result <- fmt.Errorf("mqttv5: server re-authentication verification failed: %w", err)
				c.writeDisconnectAndClose(cs, wire.DisconnectOpts{ReasonCode: wire.ReasonNotAuthorized})
				return
			}
		}
		c.fireReauthenticated()
		call.result <- nil
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
	defer cancel()
	response, err := c.authContinue(ctx, brokerData)
	if err != nil {
		c.cfg.Logger.Warn("mqttv5: Authenticator.Continue failed during re-authentication",
			slog.Any("error", err),
		)
		// Tear down — the waiter wakes on cs.dying and reports the drop.
		// The slot rides down with this disposed connState.
		c.writeDisconnectAndClose(cs, wire.DisconnectOpts{
			ReasonCode: wire.ReasonNotAuthorized,
		})
		return
	}

	c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
		return wire.WriteAuth(w, wire.AuthOpts{
			ReasonCode:           wire.ReasonContinueAuthentication,
			AuthenticationMethod: c.cfg.Authenticator.Method(),
			AuthenticationData:   response,
		})
	})
}
