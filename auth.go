// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/ashtonian/mqttv5/wire"
)

// Authenticator drives the client side of MQTT v5 enhanced
// authentication — the challenge-response handshake that runs during
// CONNECT and during re-authentication (SCRAM, Kerberos, OAuth/JWT,
// etc.).
//
// Method() supplies the AuthenticationMethod property. Begin(ctx)
// resolves the initial AuthenticationData — for a bearer method this is
// where a fresh token is fetched, so ctx carries the deadline of the
// triggering operation (the connect timeout, or the ctx passed to
// Reauthenticate); honor it for any I/O. For each broker AUTH challenge
// — during CONNECT (before CONNACK) and during mid-session
// re-authentication (§4.12) — Continue(brokerData) returns the next
// response, which the client always sends as AUTH 0x18 Continue.
//
// done is informational only. The client never concludes the exchange:
// per §3.15.2.1 only the server sends AUTH 0x00 Success (or CONNACK for
// the initial handshake). Returning done=true does NOT make the client
// emit 0x00 — treat it as a hint that the Authenticator has no further
// data to send. A non-nil error from Continue aborts the connection.
//
// Implementations need only be safe for concurrent use when shared
// across multiple Clients (e.g. with WithPublisherPool).
type Authenticator interface {
	Method() string
	Begin(ctx context.Context) (data []byte, err error)
	Continue(brokerData []byte) (response []byte, done bool, err error)
}

// ContextAuthenticator is an optional interface an Authenticator may
// implement so answering a broker challenge can be cancelled. The client
// then calls ContinueContext instead of Continue, with a ctx bounded by
// the connect timeout. Mechanisms that do I/O to answer (a token
// service, a hardware key) should implement it.
type ContextAuthenticator interface {
	ContinueContext(ctx context.Context, brokerData []byte) (response []byte, done bool, err error)
}

// ServerFinalVerifier is an optional interface an Authenticator may also
// implement to verify the server's concluding AuthenticationData — the
// CONNACK on the initial handshake, or the AUTH 0x00 Success that ends a
// re-authentication (§4.12). For mechanisms with mutual authentication
// (e.g. SCRAM, where the server proves knowledge of the credential via a
// server signature) this closes the loop; a non-nil error aborts the
// connect, or fails Reauthenticate and tears the connection down.
//
// The serverData is whatever AuthenticationData the broker attached to the
// terminal packet (possibly empty). Authenticators whose mechanism has no
// server proof (e.g. a bearer token) simply do not implement this
// interface, and the terminal data is ignored. VerifyServerFinal is
// called on the same goroutine as the corresponding Continue.
type ServerFinalVerifier interface {
	VerifyServerFinal(serverData []byte) error
}

// WithAuthenticator enables MQTT v5 enhanced authentication. When
// combined with WithPublisherPool, the same Authenticator runs
// concurrently across the parent and every pool member; implementations
// used that way must be safe for concurrent use.
func WithAuthenticator(a Authenticator) Option {
	return func(c *Config) error {
		c.Authenticator = a
		return nil
	}
}

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

// Reauthenticate initiates MQTT v5 re-authentication (§4.12) on each live
// connection the client holds: the main one and, with
// [WithPublisherPool], every connected pool member, all at once. On each
// connection it sends an AUTH 0x19 carrying the configured
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
//
// With a publisher pool the outcomes above describe the main connection,
// joined with any pool member's failure (prefixed with the member), so
// errors.Is sees every one: a rejected member reconnects as the main
// connection would. A member that is not connected is skipped; its next
// CONNECT presents a fresh credential.
func (c *Client) Reauthenticate(ctx context.Context) error {
	if c.cfg.Authenticator == nil {
		return ErrNoAuthenticator
	}
	if c.pool == nil {
		return c.reauthenticate(ctx)
	}
	poolDone := make(chan error, 1)
	go func() {
		poolDone <- c.pool.each(func(m *Client) error {
			if err := m.reauthenticate(ctx); !errors.Is(err, ErrNotConnected) {
				return err
			}
			return nil
		})
	}()
	err := c.reauthenticate(ctx)
	poolErr := <-poolDone
	if poolErr == nil {
		return err
	}
	return errors.Join(err, poolErr)
}

// reauthenticate re-authenticates the client's own connection; see
// [Client.Reauthenticate].
func (c *Client) reauthenticate(ctx context.Context) error {
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

// fireReauthenticated posts the OnReauthenticated hook, if configured.
func (c *Client) fireReauthenticated() {
	if fn := c.cfg.OnReauthenticated; fn != nil {
		c.events.post(fn)
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
