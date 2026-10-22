// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/transport"
	"github.com/ashtonian/mqttv5/wire"
)

// Connect performs the initial CONNECT/CONNACK handshake (bounded
// by ctx) and starts the supervisor. With [WithBrokers] it tries each
// broker once, from a random one on, and fails only when none accepts,
// with every broker's error. Reconnect attempts run on internal
// background contexts until [Client.Disconnect]. With
// [WithRetryInitialConnect] a failed handshake is retried the same way
// and Connect returns nil; see [Client.AwaitConnection]. Returns
// [ErrAlreadyConnected] when called twice without an intervening
// Disconnect; while a Disconnect, or a supervisor told to stop
// reconnecting, is still tearing down, Connect waits for it. A
// Disconnect during Connect cancels it, and Connect returns
// [ErrClosed].
func (c *Client) Connect(ctx context.Context) error {
	life, err := c.newSpan(ctx)
	if err != nil {
		return err
	}
	err = c.start(ctx, life)
	life.running.Done()
	if err == nil {
		return nil
	}
	if life.claimStop() {
		// Nothing else is ending the span: end it here, so the next
		// Connect starts from a finished one. The teardown runs even
		// when ctx has ended, within the connect timeout.
		opts := wire.DisconnectOpts{ReasonCode: wire.ReasonNormalDisconnection}
		if errors.Is(err, ErrStoreFailed) {
			opts.ReasonCode = wire.ReasonUnspecifiedError
		}
		tctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ConnectTimeout)
		_ = c.teardown(tctx, life, opts, err)
		cancel()
		return err
	}
	select {
	case <-life.finished:
	case <-ctx.Done():
	}
	return err
}

// newSpan begins a Connect…Disconnect span, first waiting for the
// previous one's teardown to finish. The span counts the calling
// Connect as running.
func (c *Client) newSpan(ctx context.Context) (*lifecycle, error) {
	for {
		c.startMu.Lock()
		prev := c.life.Load()
		if !c.started {
			c.started = true
			life := newLifecycle()
			// Counted before any teardown can see the span.
			life.running.Add(1)
			c.life.Store(life)
			c.startMu.Unlock()
			return life, nil
		}
		c.startMu.Unlock()
		select {
		case <-prev.shutdown:
		default:
			return nil, ErrAlreadyConnected
		}
		select {
		case <-prev.finished:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// start brings span life up: it loads the session, makes the first
// connection attempt, connects the publisher pool and starts the
// supervisor. The span's teardown cancels it and waits for it.
func (c *Client) start(ctx context.Context, life *lifecycle) error {
	ctx, cancel := life.context(ctx)
	defer cancel()

	if !c.restored.Load() {
		if err := c.engine.Restore(ctx); err != nil {
			if ended := life.endedErr(); ended != nil {
				return ended
			}
			return fmt.Errorf("mqttv5: load session store: %w", err)
		}
		c.restored.Store(true)
	}
	cleanStart := c.cfg.CleanStart
	switch {
	case !c.cfg.cleanStartSet:
		// Resume whatever session state there is.
		cleanStart = !c.engine.HasState()
		if !cleanStart {
			c.cfg.Logger.Info("mqttv5: resuming session with unfinished QoS 1/2 flows",
				slog.Int("outbound", c.engine.OutboundLen()))
		}
	case cleanStart && c.engine.HasState():
		c.cfg.Logger.Info("mqttv5: WithCleanStart(true) discards the stored session",
			slog.Int("outbound", c.engine.OutboundLen()))
		if err := c.engine.Discard(ctx); err != nil {
			if ended := life.endedErr(); ended != nil {
				return ended
			}
			return fmt.Errorf("mqttv5: reset session store: %w", err)
		}
	}

	result, err := c.connectFirst(ctx, life, cleanStart)
	connected := err == nil
	if connected {
		if err = c.runConnection(life, result); err != nil {
			return err
		}
	} else {
		if !c.cfg.RetryInitialConnect {
			return err
		}
		if ctx.Err() != nil {
			// A callback reporting the failure may have ended the span,
			// which cancels ctx too: that is what Connect reports.
			if ended := life.endedErr(); ended != nil {
				return ended
			}
			return err
		}
		c.cfg.Logger.Warn("mqttv5: initial connect failed; retrying in the background", slog.Any("error", err))
	}

	if c.pool != nil {
		if err := c.pool.connect(ctx); err != nil {
			c.cfg.Logger.Warn("mqttv5: publisher pool partial connect", slog.Any("error", err))
		}
	}

	// The supervisor starts only while the span is not ending: a
	// teardown waits for a supervisor that started before it, and none
	// may start after it.
	life.mu.Lock()
	defer life.mu.Unlock()
	if err := c.activationErr(life); err != nil {
		return err
	}
	life.running.Add(1)
	go c.supervisor(life, cleanStart, connected)
	return nil
}

// AwaitConnection waits until the client is connected and returns nil,
// at once if it already is. It returns ErrClosed when the client is
// disconnected, or ctx's error. Pair with [WithRetryInitialConnect] to
// start without the broker and wait for it.
func (c *Client) AwaitConnection(ctx context.Context) error {
	for {
		changed := c.connChanged()
		if c.cur.Load() != nil {
			return nil
		}
		select {
		case <-changed:
		case <-c.done():
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Disconnect sends a graceful DISCONNECT (reason Normal
// Disconnection), stops the supervisor, and tears down the current
// connection. For a custom reason / properties use
// [Client.DisconnectWith]. Does not fire OnConnectionDown.
// Idempotent.
//
// Lifecycle callbacks may call Disconnect. [Client.SubscribeCallback]
// handlers and [SubOnDrop] hooks must not: Disconnect waits for the read
// loop that runs them, so it would wait for itself. Start it on another
// goroutine.
func (c *Client) Disconnect(ctx context.Context) error {
	return c.DisconnectWith(ctx, DisconnectOptions{ReasonCode: ReasonNormalDisconnection})
}

// DisconnectWith sends a DISCONNECT with opts, stops the supervisor,
// and tears down the current connection. With [WithPublisherPool] it
// then refuses new publishes with [ErrClosed] and waits, bounded by ctx,
// for those in flight on pool members, whose sessions end with their
// connections, before disconnecting them. It returns ctx's error if ctx
// ended before the session store writes or the pool's publishes were
// done, joined with the pool members' disconnect errors. Idempotent. Like
// [Client.Disconnect], it must not be called from a SubscribeCallback
// handler or SubOnDrop hook.
func (c *Client) DisconnectWith(ctx context.Context, opts DisconnectOptions) error {
	c.startMu.Lock()
	started, life := c.started, c.life.Load()
	c.startMu.Unlock()
	if !started {
		return nil
	}
	if cs := c.cur.Load(); cs != nil && opts.SessionExpiryInterval != nil &&
		*opts.SessionExpiryInterval != 0 && cs.connectExpiry == 0 {
		return fmt.Errorf("%w: CONNECT sent 0, DISCONNECT asks for %d", ErrInvalidSessionExpiry, *opts.SessionExpiryInterval)
	}

	return c.stop(ctx, life, opts.wire(), nil)
}

// stop ends span life with a DISCONNECT carrying opts and tears it
// down; cause, when not nil, is why. From here on no connection is
// installed in the span and no supervisor started, so the teardown sees
// whatever was.
func (c *Client) stop(ctx context.Context, life *lifecycle, opts wire.DisconnectOpts, cause error) error {
	life.claimStop()
	return c.teardown(ctx, life, opts, cause)
}

// teardown ends span life, which is stopping: it sends the DISCONNECT,
// ends the span, which cancels what the span is still starting, waits
// for what runs in it and finishes it. Every caller returns once the
// span is finished.
func (c *Client) teardown(ctx context.Context, life *lifecycle, opts wire.DisconnectOpts, cause error) error {
	life.disconnOnce.Do(func() {
		c.sendDisconnectWith(ctx, opts)
		life.endWith(cause)
		if cs := c.cur.Load(); cs != nil {
			cs.signalDown()
		}
	})
	life.running.Wait()
	return c.finish(ctx, life)
}

// finish tears down what a span left once its connection is gone:
// subscriptions are closed, store writes drained, the publisher pool's
// publishes in flight waited for, bounded by ctx, and the pool
// disconnected. It runs once per span, from Disconnect or from a
// supervisor told to stop reconnecting, and every caller gets its
// result: ctx's error for a wait it cut short, joined with the pool
// members' disconnect errors.
func (c *Client) finish(ctx context.Context, life *lifecycle) error {
	life.finishOnce.Do(func() {
		var errs []error
		cs := c.cur.Load()
		if cs != nil {
			// Its workers have exited, or are exiting: the supervisor
			// joins them, but a connection activated by a Connect whose
			// span ended before the supervisor started has no supervisor.
			c.join(cs)
		}
		// The read loop has exited, so nothing dispatches any more.
		c.closeAllSubs()
		if cs != nil {
			c.engine.Disconnected(cs)
		}
		c.setCur(nil)
		if err := c.engine.Drain(ctx); err != nil {
			errs = append(errs, fmt.Errorf("mqttv5: session store writes still pending: %w", err))
		}
		if c.pool != nil {
			if err := c.pool.drain(ctx); err != nil {
				errs = append(errs, fmt.Errorf("mqttv5: publisher pool publishes still in flight: %w", err))
			}
			errs = append(errs, c.pool.disconnect(ctx))
		}
		life.finishErr = errors.Join(errs...)
		c.cfg.Logger.Info("mqttv5: disconnected", slog.String("client_id", c.ClientID()))
		c.startMu.Lock()
		c.started = false
		c.startMu.Unlock()
		close(life.finished)
	})
	return life.finishErr
}

// connectResult bundles everything the CONNACK reveals to the
// supervisor. Returned by connectOnce, consumed by runConnection.
type connectResult struct {
	url     string
	conn    transport.Conn
	decoder *wire.Decoder
	info    ConnackInfo
	// connectExpiry is the Session Expiry Interval the CONNECT carried.
	connectExpiry uint32
}

// dial resolves the broker URL to a live transport.Conn. If a custom
// DialFunc is configured it takes precedence and is given a parsed
// *url.URL so the implementation doesn't re-parse. Otherwise the
// built-in transport.Dial handles TCP and TLS schemes.
func (c *Client) dial(ctx context.Context, brokerURL string) (transport.Conn, error) {
	if c.cfg.DialFunc != nil {
		u, err := url.Parse(brokerURL)
		if err != nil {
			return nil, fmt.Errorf("parse broker URL: %w", err)
		}
		return c.cfg.DialFunc(ctx, u)
	}
	return transport.Dial(ctx, brokerURL, transport.DialOpts{
		TLSConfig: c.cfg.TLSConfig,
		Dialer:    c.cfg.Dialer,
	})
}

// handshakeViolation answers a broker protocol violation during the
// handshake: DISCONNECT 0x82, close, and the error to return.
func (c *Client) handshakeViolation(conn transport.Conn, t wire.PacketType, detail string) *ProtocolError {
	perr := &ProtocolError{Reason: wire.ReasonProtocolError, Packet: t, Detail: detail}
	c.stats.addProtocolError()
	_, _ = wire.WriteDisconnect(conn, wire.DisconnectOpts{ReasonCode: perr.Reason})
	_ = conn.Close()
	return perr
}

// newDecoder builds the decoder for one connection: the configured read
// window, the client's advertised Maximum Packet Size, and lenient
// decoding when enabled.
func (c *Client) newDecoder(conn transport.Conn) *wire.Decoder {
	dec := wire.NewDecoderSize(conn, c.cfg.ReadBufferSize)
	dec.SetMaxPacketSize(c.cfg.MaximumPacketSize)
	if c.cfg.LenientDecoding {
		dec.SetLenient(func(pe *wire.PacketError) {
			c.cfg.Logger.Warn("mqttv5: tolerating broker protocol violation",
				slog.String("packet", pe.Packet.String()), slog.String("detail", pe.Detail))
		})
	}
	return dec
}

// connectOnce performs the dial + CONNECT/CONNACK handshake with the
// given CleanStart flag and returns what the CONNACK revealed. ctx
// bounds the whole handshake. A broker that reports a present session
// for a CleanStart=1 CONNECT violates [MQTT-3.2.2-2]; the attempt fails
// with a [*ProtocolError] after DISCONNECT 0x82.
func (c *Client) connectOnce(ctx context.Context, brokerURL string, cleanStart bool) (*connectResult, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancel()

	conn, err := c.dial(dialCtx, brokerURL)
	if err != nil {
		return nil, fmt.Errorf("mqttv5: dial: %w", err)
	}

	if dl, ok := dialCtx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	// The deadline covers expiry; a cancelled ctx fails the read or
	// write in progress through a deadline in the past.
	interrupt := context.AfterFunc(dialCtx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer interrupt()
	// ioErr reports a handshake I/O failure as ctx's error when ctx
	// ended: the timeout the past deadline produced is not the cause.
	ioErr := func(err error) error {
		if cerr := dialCtx.Err(); cerr != nil {
			return cerr
		}
		return err
	}

	sent, err := c.writeConnect(dialCtx, conn, cleanStart)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mqttv5: write CONNECT: %w", ioErr(err))
	}

	dec := c.newDecoder(conn)

	// Loop on AUTH packets until CONNACK arrives.
	for {
		pkt, err := dec.ReadPacket()
		if perr := decodeViolation(err); perr != nil {
			c.stats.addProtocolError()
			_, _ = wire.WriteDisconnect(conn, wire.DisconnectOpts{ReasonCode: perr.Reason})
			_ = conn.Close()
			return nil, perr
		}
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("mqttv5: read CONNACK: %w", ioErr(err))
		}
		switch p := pkt.(type) {
		case *wire.Auth:
			// During CONNECT the broker may only continue the exchange the
			// CONNECT opened, with the same method (§4.12, [MQTT-4.12.0-5]).
			// Copy the challenge before Release recycles the frame.
			reason := p.ReasonCode
			method, _ := p.Properties.String(wire.PropAuthMethod)
			methodOK := sent.AuthenticationMethod != "" && method == sent.AuthenticationMethod
			brokerData := authData(p.Properties)
			p.Release()
			if reason != wire.ReasonContinueAuthentication || !methodOK {
				return nil, c.handshakeViolation(conn, wire.AUTH, fmt.Sprintf(
					"AUTH reason %#x method %q during CONNECT (method %q)", byte(reason), method, sent.AuthenticationMethod))
			}
			response, err := c.authContinue(dialCtx, brokerData)
			if err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: Authenticator.Continue: %w", err)
			}
			if _, err := wire.WriteAuth(conn, wire.AuthOpts{
				ReasonCode:           wire.ReasonContinueAuthentication,
				AuthenticationMethod: sent.AuthenticationMethod,
				AuthenticationData:   response,
			}); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: write AUTH: %w", ioErr(err))
			}
			// Loop — broker, not us, decides when to send CONNACK.
		case *wire.Connack:
			if p.ReasonCode.IsError() {
				refused := newReasonCodeError(wire.CONNACK, p.ReasonCode, p.Properties)
				p.Release()
				_ = conn.Close()
				return nil, refused
			}
			if sent.CleanStart && p.SessionPresent {
				p.Release()
				return nil, c.handshakeViolation(conn, wire.CONNACK, "Session Present set in reply to CleanStart=1")
			}
			if method, ok := p.Properties.String(wire.PropAuthMethod); ok && method != sent.AuthenticationMethod {
				method = strings.Clone(method)
				p.Release()
				return nil, c.handshakeViolation(conn, wire.CONNACK, fmt.Sprintf(
					"CONNACK authentication method %q, CONNECT used %q", method, sent.AuthenticationMethod))
			}
			// Enhanced-auth mutual verification (§4.12): an Authenticator
			// that implements ServerFinalVerifier checks the server's
			// concluding AuthenticationData (e.g. the SCRAM server
			// signature) carried on the CONNACK. Failure aborts the connect.
			if v, ok := c.cfg.Authenticator.(ServerFinalVerifier); ok {
				if err := v.VerifyServerFinal(authData(p.Properties)); err != nil {
					p.Release()
					_ = conn.Close()
					return nil, fmt.Errorf("mqttv5: server authentication verification failed: %w", err)
				}
			}
			info := parseConnack(p, sent)
			info.Broker = brokerURL
			p.Release()
			if !interrupt() {
				// ctx ended as the CONNACK arrived.
				_ = conn.Close()
				return nil, fmt.Errorf("mqttv5: connect: %w", dialCtx.Err())
			}
			_ = conn.SetDeadline(time.Time{})
			r := &connectResult{url: brokerURL, conn: conn, decoder: dec, info: info}
			if sent.SessionExpiryInterval != nil {
				r.connectExpiry = *sent.SessionExpiryInterval
			}
			return r, nil
		default:
			pkt.Release()
			_ = conn.Close()
			return nil, fmt.Errorf("%w: expected CONNACK or AUTH, got %s",
				ErrUnexpectedPacket, pkt.Type())
		}
	}
}

// runConnection installs a freshly-handshaken transport: it reconciles
// the session with the CONNACK (resume or session loss), starts the
// read/write/ping goroutines, and re-issues subscriptions when the
// broker has no session. Each call allocates a fresh connState. An
// error means the span is ending, because the session store failed or
// the client is being stopped: the connection is closed and the
// teardown is under way.
func (c *Client) runConnection(life *lifecycle, r *connectResult) error {
	cs := &connState{
		clk:           c.cfg.clock,
		connectedAt:   c.cfg.clock.Now(),
		conn:          r.conn,
		decoder:       r.decoder,
		writeQueue:    make(chan writeReq, c.cfg.WriteQueueSize),
		direct:        supportsDirectWrites(r.conn),
		engine:        c.engine,
		wake:          make(chan struct{}, 1),
		aliasSlot:     make(chan struct{}, 1),
		life:          life,
		dying:         make(chan struct{}),
		resumed:       make(chan struct{}),
		writerDone:    make(chan struct{}),
		done:          make(chan struct{}),
		aliasMap:      make(map[uint16]string),
		outAliasMap:   make(map[string]uint16),
		info:          r.info,
		brokerURL:     r.url,
		connectExpiry: r.connectExpiry,
	}
	if cs.direct {
		cs.raw = newRawWriter(r.conn)
	}
	cs.writeFailed = func(err error) { c.handleConnError(cs, err) }
	// Seed activity timestamps so the first PINGREQ fires KeepAlive
	// seconds after handshake, not immediately.
	now := c.cfg.clock.Now().UnixNano()
	cs.lastWriteUnixNano.Store(now)
	cs.lastReadUnixNano.Store(now)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
	meta := c.engine.Meta()
	meta.SessionExpiry = r.info.SessionExpiry
	if c.cfg.ClientID == "" && r.info.AssignedClientID != "" {
		meta.ClientID = r.info.AssignedClientID
	}
	err := c.engine.SetMeta(ctx, meta)
	cancel()
	if err != nil {
		_ = r.conn.Close()
		return err
	}
	gen, held, err := c.engine.Connected(inflight.ConnInfo{
		SessionPresent: r.info.SessionPresent,
		ReceiveMaximum: r.info.ReceiveMaximum,
		Limits: &inflight.Limits{
			MaximumQoS:        r.info.MaximumQoS,
			RetainAvailable:   r.info.RetainAvailable,
			MaximumPacketSize: r.info.MaximumPacketSize,
			Downgrade:         c.cfg.QoSDowngrade,
		},
		Link:            cs,
		HoldRepublished: true,
	})
	if err != nil {
		_ = r.conn.Close()
		return err
	}
	if !r.info.SessionPresent {
		c.cfg.Logger.Info("mqttv5: connected without a session; unacknowledged publishes follow the session-loss policy",
			slog.Int("outbound", c.engine.OutboundLen()))
	}
	// A store write Connected started may already have failed and the
	// span be ending: then the connection is never installed. Otherwise
	// it is installed, and OnConnectionUp fired, before the teardown can
	// start, which then takes it down.
	life.mu.Lock()
	defer life.mu.Unlock()
	if err := c.activationErr(life); err != nil {
		c.engine.Disconnected(cs)
		_ = r.conn.Close()
		return err
	}
	cs.gen.Store(gen)
	if !r.info.SessionPresent {
		c.markSessionLost()
	}
	c.setCur(cs)

	cs.wg.Add(2)
	go c.readLoop(cs)
	go c.writeLoop(cs)
	if cs.info.KeepAlive > 0 {
		cs.wg.Add(1)
		go c.pingLoop(cs)
	}
	cs.wg.Add(1)
	go c.resumeSubscriptions(cs, held)

	go func() {
		cs.wg.Wait()
		cs.doneOnce.Do(func() { close(cs.done) })
	}()

	c.stats.addConnect()
	if fn := c.cfg.OnConnectionUp; fn != nil {
		// Posted under the fence: before any callback the teardown
		// causes.
		info := r.info.clone()
		c.events.post(func() { fn(info) })
	}
	c.cfg.Logger.Info("mqttv5: connected",
		slog.String("broker", r.url),
		slog.String("client_id", c.ClientID()),
		slog.Bool("session_present", r.info.SessionPresent),
	)
	return nil
}

// connectError reports a failed connection attempt to OnConnectError.
func (c *Client) connectError(err error) {
	if fn := c.cfg.OnConnectError; fn != nil {
		c.events.post(func() { fn(err) })
	}
}

// join takes cs down and waits for its goroutines. The read loop runs
// SubscribeCallback handlers and SubOnDrop hooks: one that does not
// return, as one that called Disconnect cannot, holds the teardown, and
// join says so in the log rather than hang without a word.
func (c *Client) join(cs *connState) {
	cs.signalDown()
	select {
	case <-cs.done:
		return
	case <-c.cfg.clock.After(joinStallWarning):
	}
	c.cfg.Logger.Warn("mqttv5: disconnect is waiting for a SubscribeCallback handler or SubOnDrop hook to return; "+
		"Disconnect called from one never returns", slog.String("client_id", c.ClientID()),
		slog.Duration("waited", joinStallWarning))
	<-cs.done
}

// joinStallWarning is how long join waits before it reports a stall.
const joinStallWarning = 5 * time.Second

// activationErr is why a connection must not be installed in span life:
// the session store failed, or the span is ending. The caller holds
// life.mu.
func (c *Client) activationErr(life *lifecycle) error {
	if err := c.engine.Failure(); err != nil {
		return err
	}
	if err := life.endedErr(); err != nil {
		return err
	}
	if life.stopping {
		return ErrClosed
	}
	return nil
}

// storeFailed is the session engine's OnFailure: a store write failed,
// the engine has dropped its session state, and the client stops rather
// than run without the guarantee the store exists for (see WithStore).
// It runs on the goroutine that made the write, so the teardown, which
// waits for the store's writes, runs on its own.
func (c *Client) storeFailed(err error) {
	life := c.life.Load()
	// The next Connect reloads the session from the store.
	c.restored.Store(false)
	c.cfg.Logger.Error("mqttv5: session store write failed; stopping the client", slog.Any("error", err))
	go c.stopForStoreFailure(life, err)
}

// stopForStoreFailure ends span life as Disconnect would, with err as
// the reason calls cut short report, and DISCONNECT 0x80 so the broker
// treats the end as abnormal and publishes the Will.
func (c *Client) stopForStoreFailure(life *lifecycle, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
	defer cancel()
	_ = c.stop(ctx, life, wire.DisconnectOpts{ReasonCode: wire.ReasonUnspecifiedError}, err)
	if fn := c.cfg.OnStoreFailure; fn != nil {
		c.events.post(func() { fn(err) })
	}
}

// supervisor watches the current connState's done channel and
// reconnects (with backoff) until Disconnect is called. The reconnect
// attempts follow the FailoverOrder (see redial); a successful connect
// leaves the index parked on whichever URL accepted us.
func (c *Client) supervisor(life *lifecycle, cleanStart, connected bool) {
	defer life.running.Done()
	// attempts counts reconnect attempts since the last connection that
	// lasted; it carries the backoff across connections that do not.
	attempts := 0
	if !connected {
		var ok bool
		if ok, attempts = c.redial(life, cleanStart, 0); !ok {
			return
		}
	}
	for {
		cs := c.cur.Load()
		// Wait for the current connection to drop or the client to
		// shut down.
		select {
		case <-cs.done:
		case <-life.shutdown:
			c.join(cs)
			return
		}
		if life.isStopping() {
			// The broker closed the connection after the teardown's
			// DISCONNECT: that is the teardown, not a drop.
			return
		}

		// Connection has dropped. Clear current pointer + notify.
		c.engine.Disconnected(cs)
		c.connectionLost(cs.serverDisconnect.Load())
		c.setCur(nil)
		c.stats.addDisconnect()
		if sd := cs.serverDisconnect.Load(); sd != nil {
			c.stats.addServerDisconnect()
			if fn := c.cfg.OnServerDisconnect; fn != nil {
				info := *sd
				c.events.post(func() { fn(info) })
			}
			c.redirect(ServerRedirect{Packet: DISCONNECT, Reason: sd.ReasonCode, Reference: sd.ServerReference})
		}
		c.cfg.Logger.Warn("mqttv5: connection lost", slog.String("broker", cs.brokerURL))
		// OnConnectionDown's answer decides whether to keep retrying;
		// without the callback the supervisor keeps going.
		keepRetrying := true
		if fn := c.cfg.OnConnectionDown; fn != nil {
			c.events.post(func() { keepRetrying = fn() })
			if !c.events.flush(life.shutdown) {
				// The span ended while the callback ran.
				return
			}
		}
		if !keepRetrying {
			// Caller wants no further reconnect attempts: end the span
			// as Disconnect would, so a later Connect starts clean.
			life.end()
			ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ConnectTimeout)
			_ = c.finish(ctx, life)
			cancel()
			return
		}
		// A connection that outlived the delay before the next attempt
		// was healthy: start the backoff over. One that died sooner —
		// a broker that accepts the CONNECT and drops what follows —
		// keeps it growing instead of retrying in a tight loop.
		if c.cfg.clock.Now().Sub(cs.connectedAt) >= c.cfg.ReconnectBackoff(attempts) {
			attempts = 0
		}
		var ok bool
		if ok, attempts = c.redial(life, c.cfg.CleanStartOnReconnect, attempts); !ok {
			return
		}
	}
}

// redial keeps backing off and making reconnect attempts until a
// connection is up, or the span ends (false). Under FailoverRotate an
// attempt dials the entry after the current one; under FailoverPriority
// it is one pass over the list from the first entry. The backoff
// continues from attempt; redial returns the attempts made so far.
func (c *Client) redial(life *lifecycle, cleanStart bool, attempt int) (bool, int) {
	for {
		delay := c.cfg.ReconnectBackoff(attempt)
		select {
		case <-c.cfg.clock.After(delay):
		case <-life.shutdown:
			return false, attempt
		}
		// A callback may change the brokers, as OnServerDisconnect and
		// OnServerRedirect may for a redirect: the attempt dials what
		// the callbacks so far left.
		if !c.events.flush(life.shutdown) {
			return false, attempt
		}
		attempt++

		dials := 1
		if c.cfg.FailoverOrder == FailoverPriority {
			c.brokerIdx.Store(0)
			dials = len(*c.brokerURLs.Load())
		} else {
			// The first attempt after a drop already moves off the URL
			// that just failed — fail-fast over a known-bad endpoint.
			c.brokerIdx.Add(1)
		}
		fromList := false
		for i := range dials {
			if i > 0 {
				// A callback may change the brokers, as in connectFirst.
				if !c.events.flush(life.shutdown) {
					return false, attempt
				}
				if fromList {
					c.brokerIdx.Add(1)
				}
			}
			brokerURL, redirected := c.nextBrokerURL()
			fromList = !redirected
			result, live := c.tryBroker(life, attempt, brokerURL, cleanStart)
			if !live {
				return false, attempt
			}
			if result == nil {
				continue
			}
			if err := c.runConnection(life, result); err != nil {
				return false, attempt
			}
			return true, attempt
		}
	}
}

// tryBroker makes reconnect attempt's dial of brokerURL. It returns the
// connection when the broker accepts it and nil when it fails; live is
// false when the span ended.
func (c *Client) tryBroker(life *lifecycle, attempt int, brokerURL string, cleanStart bool) (result *connectResult, live bool) {
	if fn := c.cfg.OnReconnectAttempt; fn != nil {
		c.events.post(func() { fn(attempt, brokerURL) })
		if !c.events.flush(life.shutdown) {
			return nil, false
		}
	}
	ctx, cancel := life.context(context.Background())
	result, err := c.connectOnce(ctx, brokerURL, cleanStart)
	cancel()
	if err == nil {
		return result, true
	}
	select {
	case <-life.shutdown:
		// The span ended during the attempt and cut it short.
		return nil, false
	default:
	}
	c.stats.addConnectFailure()
	c.redirectFrom(err)
	c.connectError(err)
	c.cfg.Logger.Warn("mqttv5: reconnect failed",
		slog.String("broker", brokerURL),
		slog.Int("attempt", attempt),
		slog.Any("error", err),
	)
	return nil, true
}

// sendDisconnectWith best-effort writes a graceful DISCONNECT carrying
// opts. cs.dying short-circuits the wait if the connection is going
// down before the writer drains; combined with drainWriteQueue in
// writeLoop's deferred exit, req.done is always signalled.
func (c *Client) sendDisconnectWith(ctx context.Context, opts wire.DisconnectOpts) {
	cs := c.cur.Load()
	if cs == nil {
		return
	}
	done := make(chan error, 1)
	err := cs.queue(ctx, writeReq{
		// Runs on the writer: acknowledgements the application already
		// made go out before the DISCONNECT, so the broker does not
		// redeliver those messages.
		fn: func(w io.Writer) (int64, error) {
			if _, err := cs.flushEngine(context.Background(), 0); err != nil {
				return 0, err
			}
			return wire.WriteDisconnect(w, opts)
		},
		done: done,
	}, false)
	if err != nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	case <-cs.dying:
	}
}

// writeConnect emits the CONNECT packet directly against w (used during
// the handshake, before the writer goroutine has started) and returns
// the options actually sent.
//
// The ClientID is the configured one or, when that is empty, the one the
// broker assigned earlier, so the session can resume.
//
// If cfg.Authenticator is set, the CONNECT carries
// AuthenticationMethod + the initial AuthenticationData from
// Authenticator.Begin(ctx). connectOnce then drives the AUTH loop.
//
// If cfg.ConnectPacketBuilder is set, it runs after this method has
// populated opts and may mutate any field — useful for per-attempt
// credential rotation. A non-nil error from the builder fails the
// attempt.
func (c *Client) writeConnect(ctx context.Context, w io.Writer, cleanStart bool) (wire.ConnectOpts, error) {
	cfg := c.cfg
	opts := wire.ConnectOpts{
		ClientID:          c.ClientID(),
		CleanStart:        cleanStart,
		KeepAlive:         cfg.KeepAlive,
		Username:          cfg.Username,
		Password:          cfg.Password,
		Will:              cfg.WillMessage.wire(),
		TopicAliasMaximum: cfg.InboundTopicAliasMaximum,
		UserProperties:    cfg.ConnectUserProperties,
	}
	if cfg.SessionExpiry > 0 {
		opts.SessionExpiryInterval = &cfg.SessionExpiry
	}
	if cfg.ReceiveMaximum > 0 && cfg.ReceiveMaximum < 65535 {
		opts.ReceiveMaximum = &cfg.ReceiveMaximum
	}
	if cfg.MaximumPacketSize > 0 {
		opts.MaximumPacketSize = &cfg.MaximumPacketSize
	}
	if cfg.RequestResponseInformation {
		v := byte(1)
		opts.RequestResponseInformation = &v
	}
	if !cfg.RequestProblemInformation {
		// The property defaults to 1 when absent (§3.1.2.11.7), so
		// opting out has to be explicit.
		v := byte(0)
		opts.RequestProblemInformation = &v
	}
	if cfg.Authenticator != nil {
		opts.AuthenticationMethod = cfg.Authenticator.Method()
		data, err := cfg.Authenticator.Begin(ctx)
		if err != nil {
			return opts, fmt.Errorf("mqttv5: Authenticator.Begin: %w", err)
		}
		opts.AuthenticationData = data
	}
	if cfg.ConnectPacketBuilder != nil {
		co := ConnectOptions{Username: opts.Username, Password: opts.Password, UserProperties: opts.UserProperties}
		if err := cfg.ConnectPacketBuilder(ctx, &co); err != nil {
			return opts, fmt.Errorf("mqttv5: ConnectPacketBuilder: %w", err)
		}
		opts.Username, opts.Password, opts.UserProperties = co.Username, co.Password, co.UserProperties
	}
	_, err := wire.WriteConnect(w, opts)
	return opts, err
}

// events runs the lifecycle callbacks one at a time, in the order they
// were posted, on a goroutine that exists while there are callbacks to
// run. Nothing the client does waits on a callback without also watching
// for its span to end, so a callback may call Disconnect or Connect.
type events struct {
	mu      sync.Mutex
	pending []func()
	running bool
}

// post queues fn to run after every callback posted before it.
func (q *events) post(fn func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, fn)
	if !q.running {
		q.running = true
		go q.run()
	}
}

func (q *events) run() {
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.pending = nil
			q.running = false
			q.mu.Unlock()
			return
		}
		fn := q.pending[0]
		q.pending[0] = nil
		q.pending = q.pending[1:]
		q.mu.Unlock()
		fn()
	}
}

// flush waits until every callback posted before it has run and reports
// true, or reports false once stop is closed.
func (q *events) flush(stop <-chan struct{}) bool {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return true
	}
	done := make(chan struct{})
	q.pending = append(q.pending, func() { close(done) })
	q.mu.Unlock()
	select {
	case <-done:
		return true
	case <-stop:
		return false
	}
}

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

// connectFirst makes a span's first connection. It makes one attempt
// per listed broker, starting from the current one (the first under
// FailoverPriority), so that Connect
// fails only when none of them accepts; a redirect it follows takes one
// of those attempts. Each failure is counted and reported as a reconnect
// failure is, and the brokers' errors are joined, each naming its
// broker.
func (c *Client) connectFirst(ctx context.Context, life *lifecycle, cleanStart bool) (*connectResult, error) {
	if c.cfg.FailoverOrder == FailoverPriority {
		c.brokerIdx.Store(0)
	}
	attempts := len(*c.brokerURLs.Load())
	var errs []error
	fromList := false
	for i := range attempts {
		if i > 0 {
			// A callback may change the brokers, as in redial.
			if !c.events.flush(ctx.Done()) {
				break
			}
			if fromList {
				c.brokerIdx.Add(1)
			}
		}
		brokerURL, redirected := c.nextBrokerURL()
		fromList = !redirected
		result, err := c.connectOnce(ctx, brokerURL, cleanStart)
		if err == nil {
			return result, nil
		}
		if ended := life.endedErr(); ended != nil {
			// The teardown cut the attempt short.
			return nil, ended
		}
		c.stats.addConnectFailure()
		c.redirectFrom(err)
		c.connectError(err)
		if attempts == 1 {
			return nil, err
		}
		errs = append(errs, fmt.Errorf("%s: %w", brokerURL, err))
		if ctx.Err() != nil {
			break
		}
		if i+1 < attempts {
			c.cfg.Logger.Warn("mqttv5: connect failed; trying the next broker",
				slog.String("broker", brokerURL), slog.Any("error", err))
		}
	}
	return nil, errors.Join(errs...)
}

// nextBrokerURL is the URL the next connection attempt dials: a pending
// temporary redirect, once, else the current entry of the broker list.
func (c *Client) nextBrokerURL() (url string, redirected bool) {
	if u := c.redirectOnce.Swap(nil); u != nil {
		return *u, true
	}
	return c.currentBrokerURL(), false
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
	if fn := c.cfg.OnServerRedirect; fn != nil {
		c.events.post(func() { fn(r) })
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
		// A redirect concerns the connection that received it; pool
		// members follow the ones their own brokers send.
		c.setBrokers([]string{target})
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
