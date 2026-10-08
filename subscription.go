// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/ashtonian/mqttv5/internal/inflight"
	"github.com/ashtonian/mqttv5/internal/trie"
	"github.com/ashtonian/mqttv5/wire"
)

// SubscribeResult is the broker's answer for one filter of a SUBSCRIBE.
type SubscribeResult struct {
	Filter TopicFilter
	// Reason is the SUBACK reason code: 0x00–0x02 grant that QoS,
	// 0x80 and above refuse the filter.
	Reason wire.ReasonCode
}

// Granted reports whether the broker accepted the filter.
func (r SubscribeResult) Granted() bool { return !r.Reason.IsError() }

// QoS is the maximum QoS the broker granted; meaningful when Granted.
func (r SubscribeResult) QoS() byte { return byte(r.Reason) }

// SubscribeError reports filters the broker refused. Results holds the
// outcome of every filter of the SUBSCRIBE. When some filters were
// granted the subscription is active for those; when none were, its
// channel or queue is closed.
type SubscribeError struct {
	Results []SubscribeResult
}

func (e *SubscribeError) Error() string {
	var refused []string
	for _, r := range e.Results {
		if !r.Granted() {
			refused = append(refused, fmt.Sprintf("%q (0x%02X %s)", r.Filter.Topic, byte(r.Reason), reasonName(r.Reason)))
		}
	}
	return fmt.Sprintf("mqttv5: broker refused %d of %d filters: %s", len(refused), len(e.Results), strings.Join(refused, ", "))
}

// Is matches the sentinel of any refusal's reason code (see
// [ReasonCodeError]).
func (e *SubscribeError) Is(target error) bool {
	for _, r := range e.Results {
		if !r.Granted() && (&ReasonCodeError{Packet: wire.SUBACK, Code: r.Reason}).Is(target) {
			return true
		}
	}
	return false
}

// UnsubscribeResult is the broker's answer for one topic filter of an
// UNSUBSCRIBE.
type UnsubscribeResult struct {
	Topic string
	// Reason is the UNSUBACK reason code: 0x00 success, 0x11 there was no
	// such subscription (not an error), 0x80 and above refused.
	Reason wire.ReasonCode
}

// UnsubscribeError reports topic filters the broker refused to
// unsubscribe. The local subscription is closed regardless; the broker
// may keep delivering messages for them, which the client then drops.
type UnsubscribeError struct {
	Results []UnsubscribeResult
}

func (e *UnsubscribeError) Error() string {
	var refused []string
	for _, r := range e.Results {
		if r.Reason.IsError() {
			refused = append(refused, fmt.Sprintf("%q (0x%02X %s)", r.Topic, byte(r.Reason), reasonName(r.Reason)))
		}
	}
	return "mqttv5: broker refused to unsubscribe " + strings.Join(refused, ", ")
}

// SubscriptionToken identifies a subscription for [Client.Unsubscribe]
// and reports what the broker decided.
type SubscriptionToken struct {
	client *Client
	sub    *activeSub
}

// Results returns the broker's answer per filter from the subscription's
// SUBACK, or nil before it arrived.
func (t SubscriptionToken) Results() []SubscribeResult {
	if t.sub == nil {
		return nil
	}
	t.client.subsMu.Lock()
	defer t.client.subsMu.Unlock()
	return slices.Clone(t.sub.results)
}

// Err reports why the subscription ended other than by Unsubscribe or
// Disconnect: the broker refused every filter, initially or when the
// client re-subscribed after a session loss. Nil while it is active.
func (t SubscriptionToken) Err() error {
	if t.sub == nil {
		return nil
	}
	t.client.subsMu.Lock()
	defer t.client.subsMu.Unlock()
	return t.sub.err
}

type subState uint8

const (
	subPending subState = iota // no SUBACK yet
	subActive                  // granted
	subClosed
)

// activeSub is one local subscription, from Subscribe until it closes.
// Its fields are guarded by Client.subsMu.
type activeSub struct {
	id      uint64 // key in Client.activeSubs; orders re-subscription
	route   *route
	onUnsub func() // closes the channel or queue

	state   subState
	filters []TopicFilter // requested while pending, granted afterwards
	results []SubscribeResult
	err     error
	op      *ctrlOp // its SUBSCRIBE awaiting a SUBACK, or nil

	// The UNSUBSCRIBE Unsubscribe waits on, and the filters it ended.
	unsub       *ctrlOp
	unsubTopics []string
}

// brokerSub is one subscription as the broker holds it. The broker keys
// subscriptions by exact topic filter, and a SUBSCRIBE naming a filter it
// already holds replaces that subscription (§3.8.4), so every local
// subscription with the same filter shares one brokerSub. It is removed,
// with an UNSUBSCRIBE, once none holds it.
type brokerSub struct {
	filter  string // exact, $share prefix included
	trieKey string
	trieID  uint64

	// Guarded by Client.subsMu.
	holders []*activeSub // one entry per filter occurrence, in join order
	// Subscription Identifiers the broker may tag this subscription's
	// messages with, oldest first: the one it holds and those of
	// SUBSCRIBEs sent since (§3.3.4).
	ids []uint32
	// granted is set once a SUBACK granted the filter in the broker's
	// current session. pending counts SUBSCRIBEs carrying the filter
	// that were sent and whose answer is not yet applied. With neither,
	// the broker does not hold the filter.
	granted bool
	pending int

	view atomic.Pointer[subView] // read by the read loop
}

// subView is an immutable snapshot of a brokerSub for routing.
type subView struct {
	ids    []uint32
	routes []*route
}

// accepts reports whether a PUBLISH tagged with Subscription Identifiers
// ids was sent for this subscription. A subscription made without an
// identifier accepts any.
func (v *subView) accepts(ids []uint32) bool {
	if len(v.ids) == 0 {
		return true
	}
	for _, id := range ids {
		if slices.Contains(v.ids, id) {
			return true
		}
	}
	return false
}

// ctrlOp is one SUBSCRIBE or UNSUBSCRIBE, queued until acknowledged. A
// packet identifier belongs to one connection: when that connection
// drops the identifier is released and the operation is sent again, with
// a new one, on the next.
type ctrlOp struct {
	kind wire.PacketType

	sub     *activeSub    // SUBSCRIBE: whose filters
	filters []TopicFilter // SUBSCRIBE
	ident   uint32        // SUBSCRIBE: its Subscription Identifier
	resub   bool          // SUBSCRIBE re-issued after a session loss; nobody waits for it

	topics []string // UNSUBSCRIBE
	// UNSUBSCRIBE: identifiers of the subscriptions it removes, reserved
	// until the broker confirms.
	retired []uint32

	// Guarded by Client.ctrlMu.
	queued bool
	id     uint16     // packet identifier on conn
	conn   *connState // connection it was last sent on
	sent   bool       // may have reached a broker
	// rejections counts connections the broker ended, blaming a packet
	// it received, while op was sent and unanswered.
	rejections int
	// counted holds the broker subscriptions whose pending count op
	// raised when it was first sent. Guarded by Client.subsMu.
	counted []*brokerSub

	done   chan struct{} // closed once result is set
	result ctrlResult
}

type ctrlResult struct {
	codes []wire.ReasonCode
	err   error
}

func (op *ctrlOp) owner() inflight.Owner {
	if op.kind == wire.SUBSCRIBE {
		return inflight.OwnerSubscribe
	}
	return inflight.OwnerUnsubscribe
}

func (op *ctrlOp) codeCount() int {
	if op.kind == wire.SUBSCRIBE {
		return len(op.filters)
	}
	return len(op.topics)
}

// complete records op's outcome. Only whoever took op off the queue
// calls it, so it runs once.
func (op *ctrlOp) complete(r ctrlResult) {
	op.result = r
	close(op.done)
}

// lockCtrl takes ctrlSem, which orders every change to the broker's
// subscriptions with the sending of its packet: the broker sees
// SUBSCRIBEs and UNSUBSCRIBEs in the order the client decided them.
func (c *Client) lockCtrl(ctx context.Context, stop <-chan struct{}) error {
	select {
	case c.ctrlSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return ErrClosed
	}
	select {
	case <-c.done():
		<-c.ctrlSem
		return ErrClosed
	default:
		return nil
	}
}

func (c *Client) unlockCtrl() { <-c.ctrlSem }

// subscribe registers a subscription and sends its SUBSCRIBE, then waits
// for the SUBACK across reconnects, ctx, or shutdown.
func (c *Client) subscribe(ctx context.Context, filters []TopicFilter, r *route, onUnsub func()) (SubscriptionToken, error) {
	cs := c.cur.Load()
	if cs == nil {
		return SubscriptionToken{}, ErrNotConnected
	}
	if len(filters) == 0 {
		return SubscriptionToken{}, wire.ErrEmptyFilterList
	}
	if r == nil || r.deliver == nil {
		return SubscriptionToken{}, ErrNilHandler
	}
	if err := checkFilterCapabilities(cs, filters); err != nil {
		return SubscriptionToken{}, err
	}
	// Validate (and size-check) the packet before anything is
	// registered; sendCtrl encodes it again with the real identifiers.
	if _, err := c.encodeSubscribe(cs, 1, wire.MaxVarintValue, filters); err != nil {
		return SubscriptionToken{}, err
	}

	if err := c.lockCtrl(ctx, c.done()); err != nil {
		return SubscriptionToken{}, err
	}
	sub, op, err := c.addSub(filters, r, onUnsub)
	if err == nil {
		err = c.flushCtrl(ctx, c.done())
	}
	c.unlockCtrl()
	if sub == nil {
		return SubscriptionToken{}, err
	}
	c.stats.addSubscribeSent()
	if err != nil {
		c.abandon(sub)
		return SubscriptionToken{}, err
	}

	select {
	case <-op.done:
	case <-ctx.Done():
		c.abandon(sub)
		return SubscriptionToken{}, ctx.Err()
	case <-c.done():
		return SubscriptionToken{}, ErrClosed
	}
	if op.result.err != nil {
		return SubscriptionToken{}, op.result.err
	}
	token := SubscriptionToken{client: c, sub: sub}
	if err := token.Err(); err != nil {
		return token, err // every filter refused; outputs closed
	}
	results := token.Results()
	for _, r := range results {
		if !r.Granted() {
			return token, &SubscribeError{Results: results}
		}
	}
	return token, nil
}

// addSub registers a pending subscription and queues its SUBSCRIBE.
// Routes go in first: a matching PUBLISH can arrive before the SUBACK.
// The caller holds ctrlSem.
func (c *Client) addSub(filters []TopicFilter, r *route, onUnsub func()) (*activeSub, *ctrlOp, error) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	select {
	case <-c.done():
		return nil, nil, ErrClosed
	default:
	}
	c.nextSubID++
	sub := &activeSub{id: c.nextSubID, route: r, onUnsub: onUnsub, state: subPending, filters: slices.Clone(filters)}
	for _, f := range sub.filters {
		bs := c.brokerSubLocked(f.Topic)
		bs.holders = append(bs.holders, sub)
	}
	c.activeSubs[sub.id] = sub
	op := &ctrlOp{kind: wire.SUBSCRIBE, sub: sub, filters: sub.filters, done: make(chan struct{})}
	c.queueSubscribeLocked(op)
	return sub, op, nil
}

// brokerSubLocked returns the broker subscription for filter, creating it
// and its trie entry when the client holds none yet.
func (c *Client) brokerSubLocked(filter string) *brokerSub {
	if bs := c.brokerSubs[filter]; bs != nil {
		return bs
	}
	// A shared subscription's PUBLISHes carry the underlying topic
	// (§4.8.2), so it matches under the filter without its prefix.
	_, key, _ := parseShareFilter(filter)
	bs := &brokerSub{filter: filter, trieKey: key}
	bs.view.Store(&subView{})
	bs.trieID = c.router.Register(key, bs)
	c.brokerSubs[filter] = bs
	return bs
}

// queueSubscribeLocked gives op a fresh Subscription Identifier, which
// the broker may tag its filters' messages with as soon as op is sent,
// and queues it.
func (c *Client) queueSubscribeLocked(op *ctrlOp) {
	op.ident = c.allocIdentLocked()
	op.sub.op = op
	for _, f := range op.filters {
		bs := c.brokerSubs[f.Topic]
		bs.ids = append(bs.ids, op.ident)
		c.identRefs[op.ident]++
		c.publishLocked(bs)
	}
	c.enqueue(op)
}

// untagLocked withdraws op's identifier from its filters' subscriptions.
func (c *Client) untagLocked(op *ctrlOp) {
	for _, f := range op.filters {
		bs := c.brokerSubs[f.Topic]
		if bs == nil {
			continue
		}
		if i := slices.Index(bs.ids, op.ident); i >= 0 {
			bs.ids = slices.Delete(bs.ids, i, i+1)
			c.releaseIdentsLocked(op.ident)
			c.publishLocked(bs)
		}
	}
}

// allocIdentLocked returns a Subscription Identifier (1..268,435,455)
// no subscription uses.
func (c *Client) allocIdentLocked() uint32 {
	for {
		c.nextIdent = c.nextIdent%wire.MaxVarintValue + 1
		if c.identRefs[c.nextIdent] == 0 {
			return c.nextIdent
		}
	}
}

func (c *Client) releaseIdentsLocked(ids ...uint32) {
	for _, id := range ids {
		if c.identRefs[id]--; c.identRefs[id] <= 0 {
			delete(c.identRefs, id)
		}
	}
}

// publishLocked makes bs's current identifiers and holders visible to
// the read loop.
func (c *Client) publishLocked(bs *brokerSub) {
	v := &subView{ids: slices.Clone(bs.ids)}
	for _, h := range bs.holders {
		if !slices.Contains(v.routes, h.route) {
			v.routes = append(v.routes, h.route)
		}
	}
	bs.view.Store(v)
}

// closeSubLocked ends sub: it stops holding its broker subscriptions and
// stops receiving. The returned func closes its channel or queue and must
// run after subsMu is released.
func (c *Client) closeSubLocked(sub *activeSub, err error) (closeRoute func()) {
	if sub.state == subClosed {
		return func() {}
	}
	sub.state, sub.err = subClosed, err
	delete(c.activeSubs, sub.id)
	for _, f := range sub.filters {
		c.dropHolderLocked(f.Topic, sub)
	}
	return func() { sub.route.close(sub.onUnsub) }
}

func (c *Client) dropHolderLocked(filter string, sub *activeSub) {
	bs := c.brokerSubs[filter]
	if bs == nil {
		return
	}
	if i := slices.Index(bs.holders, sub); i >= 0 {
		bs.holders = slices.Delete(bs.holders, i, i+1)
		c.publishLocked(bs)
	}
}

// abandon ends a subscription whose caller stopped waiting for its
// SUBACK and takes back from the broker what nobody else holds.
func (c *Client) abandon(sub *activeSub) {
	c.subsMu.Lock()
	closeRoute := c.closeSubLocked(sub, nil)
	c.subsMu.Unlock()
	closeRoute()
	go c.settleAsync()
}

// settleAsync removes broker subscriptions left without holders by the
// read loop or an abandoned Subscribe, which cannot wait for ctrlSem.
func (c *Client) settleAsync() {
	stop := c.done()
	if err := c.lockCtrl(context.Background(), stop); err != nil {
		return
	}
	defer c.unlockCtrl()
	c.settle()
	if err := c.flushCtrl(context.Background(), stop); err != nil && !errors.Is(err, ErrClosed) {
		c.cfg.Logger.Warn("mqttv5: sending UNSUBSCRIBE failed", slog.Any("error", err))
	}
}

// settle queues one UNSUBSCRIBE for every broker subscription no local
// subscription holds any more. The caller holds ctrlSem, so it goes out
// before any later SUBSCRIBE that creates the subscription again.
func (c *Client) settle() *ctrlOp {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	var op *ctrlOp
	for filter, bs := range c.brokerSubs {
		if len(bs.holders) > 0 {
			continue
		}
		c.router.Unregister(bs.trieKey, bs.trieID)
		delete(c.brokerSubs, filter)
		if !bs.granted && bs.pending == 0 {
			// The broker never took it: nothing to take back.
			c.releaseIdentsLocked(bs.ids...)
			continue
		}
		if op == nil {
			op = &ctrlOp{kind: wire.UNSUBSCRIBE, done: make(chan struct{})}
		}
		op.topics = append(op.topics, filter)
		op.retired = append(op.retired, bs.ids...)
	}
	if op == nil {
		return nil
	}
	slices.Sort(op.topics)
	c.enqueue(op)
	return op
}

// markSent records that op is about to be written, so it may reach
// the broker: a SUBSCRIBE sent for the first time counts as pending on
// its filters' broker subscriptions. It reports whether this was the
// first time.
func (c *Client) markSent(op *ctrlOp) bool {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	c.ctrlMu.Lock()
	first := !op.sent
	op.sent = true
	c.ctrlMu.Unlock()
	if first && op.kind == wire.SUBSCRIBE {
		for _, f := range op.filters {
			if bs := c.brokerSubs[f.Topic]; bs != nil {
				bs.pending++
				op.counted = append(op.counted, bs)
			}
		}
	}
	return first
}

// unmarkSent undoes the first markSent of an op that was not written
// after all.
func (c *Client) unmarkSent(op *ctrlOp) {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	c.ctrlMu.Lock()
	op.sent = false
	c.ctrlMu.Unlock()
	c.uncountLocked(op)
}

// uncountLocked takes op off the pending counts it raised. The caller
// holds subsMu.
func (c *Client) uncountLocked(op *ctrlOp) {
	for _, bs := range op.counted {
		bs.pending--
	}
	op.counted = nil
}

// Unsubscribe ends the subscription: its channel or queue is closed
// before Unsubscribe returns, and an UNSUBSCRIBE is sent for every filter
// no other subscription of this client uses — the broker holds one
// subscription per filter, shared by all of them. Unsubscribe then waits
// for the broker's answer, across reconnects, until ctx ends; the
// UNSUBSCRIBE is still sent after that. Refusals are returned as an
// [*UnsubscribeError].
func (c *Client) Unsubscribe(ctx context.Context, token SubscriptionToken) error {
	sub := token.sub
	if sub == nil {
		return nil
	}
	if err := c.lockCtrl(ctx, c.done()); err != nil {
		return err
	}
	c.subsMu.Lock()
	if sub.state == subClosed {
		op, topics := sub.unsub, sub.unsubTopics
		c.subsMu.Unlock()
		c.unlockCtrl()
		return c.awaitUnsubscribe(ctx, op, topics)
	}
	topics := make([]string, len(sub.filters))
	for i, f := range sub.filters {
		topics[i] = f.Topic
	}
	closeRoute := c.closeSubLocked(sub, nil)
	c.subsMu.Unlock()
	closeRoute()

	op := c.settle()
	c.subsMu.Lock()
	sub.unsub, sub.unsubTopics = op, topics
	c.subsMu.Unlock()
	if op != nil {
		c.stats.addUnsubscribeSent()
	}
	err := c.flushCtrl(ctx, c.done())
	c.unlockCtrl()
	if err != nil {
		return err
	}
	return c.awaitUnsubscribe(ctx, op, topics)
}

// awaitUnsubscribe waits for op's UNSUBACK and reports topics' outcome. A
// topic op does not carry is still held by another subscription and
// counts as success.
func (c *Client) awaitUnsubscribe(ctx context.Context, op *ctrlOp, topics []string) error {
	if op == nil || !slices.ContainsFunc(topics, func(t string) bool { return slices.Contains(op.topics, t) }) {
		return nil
	}
	select {
	case <-op.done:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done():
		return ErrClosed
	}
	if op.result.err != nil {
		return op.result.err
	}
	results := make([]UnsubscribeResult, len(topics))
	refused := false
	for i, t := range topics {
		results[i] = UnsubscribeResult{Topic: t, Reason: wire.ReasonSuccess}
		if k := slices.Index(op.topics, t); k >= 0 {
			results[i].Reason = op.result.codes[k]
		}
		refused = refused || results[i].Reason.IsError()
	}
	if refused {
		return &UnsubscribeError{Results: results}
	}
	return nil
}

func (c *Client) enqueue(op *ctrlOp) {
	c.ctrlMu.Lock()
	op.queued = true
	c.ctrlQueue = append(c.ctrlQueue, op)
	c.ctrlMu.Unlock()
}

// dequeueLocked takes op off the queue and forgets its packet
// identifier, which the caller releases. It reports false when op was
// no longer queued. The caller holds ctrlMu.
func (c *Client) dequeueLocked(op *ctrlOp) (ok bool, release uint16) {
	if !op.queued {
		return false, 0
	}
	op.queued = false
	if i := slices.Index(c.ctrlQueue, op); i >= 0 {
		c.ctrlQueue = slices.Delete(c.ctrlQueue, i, i+1)
	}
	return true, c.unregisterLocked(op)
}

// unregisterLocked drops op's packet identifier registration and returns
// the identifier for the caller to release, or 0.
func (c *Client) unregisterLocked(op *ctrlOp) uint16 {
	id := op.id
	op.id, op.conn = 0, nil
	if id == 0 || c.ctrlOps[id] != op {
		return 0
	}
	delete(c.ctrlOps, id)
	return id
}

// markSessionLost records that the broker kept no session, for the next
// flushCtrl to act on before it sends anything.
func (c *Client) markSessionLost() {
	c.ctrlMu.Lock()
	c.sessionLost = true
	c.ctrlMu.Unlock()
}

// resumeSubscriptions brings the broker's subscriptions in line on a new
// connection: whatever was not acknowledged on the last one is sent
// again, and everything is re-subscribed when the session was lost.
func (c *Client) resumeSubscriptions(cs *connState) {
	defer cs.wg.Done()
	if err := c.lockCtrl(context.Background(), cs.dying); err != nil {
		return
	}
	defer c.unlockCtrl()
	if err := c.flushCtrl(context.Background(), cs.dying); err != nil && !errors.Is(err, ErrClosed) {
		c.cfg.Logger.Warn("mqttv5: re-issuing subscriptions failed", slog.Any("error", err))
	}
}

// flushCtrl sends, in queue order, every queued operation not yet sent on
// the current connection. Without one they wait for the next. The caller
// holds ctrlSem.
func (c *Client) flushCtrl(ctx context.Context, stop <-chan struct{}) error {
	cs := c.cur.Load()
	if cs == nil {
		return nil
	}
	c.ctrlMu.Lock()
	lost := c.sessionLost
	c.sessionLost = false
	c.ctrlMu.Unlock()
	if lost {
		c.resetSession()
	}
	for {
		op := c.nextToSend(cs)
		if op == nil {
			return nil
		}
		err := c.sendCtrl(ctx, stop, cs, op)
		if errors.Is(err, ErrNotConnected) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// nextToSend returns the first queued operation not sent on cs. A
// SUBSCRIBE of a closed subscription that never reached a broker is
// dropped instead.
func (c *Client) nextToSend(cs *connState) *ctrlOp {
	var dropped []*ctrlOp
	var next *ctrlOp
	c.subsMu.Lock()
	c.ctrlMu.Lock()
	for i := 0; i < len(c.ctrlQueue); i++ {
		op := c.ctrlQueue[i]
		if op.conn == cs {
			continue
		}
		if op.kind == wire.SUBSCRIBE && op.sub.state == subClosed && !op.sent {
			// Never sent, so it holds no packet identifier.
			c.dequeueLocked(op)
			dropped = append(dropped, op)
			i--
			continue
		}
		next = op
		break
	}
	c.ctrlMu.Unlock()
	for _, op := range dropped {
		c.untagLocked(op)
	}
	c.subsMu.Unlock()
	for _, op := range dropped {
		op.complete(ctrlResult{err: ErrClosed})
	}
	return next
}

// resetSession rebuilds the queue for a broker that kept no session. It
// holds none of the client's subscriptions, so pending UNSUBSCRIBEs
// complete without being sent and every live subscription is subscribed
// again, oldest first so that the newest options win on a shared filter.
// The caller holds ctrlSem.
func (c *Client) resetSession() {
	type idRelease struct {
		id uint16
		o  inflight.Owner
	}
	type finish struct {
		op *ctrlOp
		r  ctrlResult
	}
	var released []idRelease
	var finished []finish

	c.subsMu.Lock()
	c.ctrlMu.Lock()
	old := c.ctrlQueue
	c.ctrlQueue = nil
	for _, op := range old {
		op.queued = false
		if id := c.unregisterLocked(op); id != 0 {
			released = append(released, idRelease{id, op.owner()})
		}
	}
	c.ctrlMu.Unlock()

	for _, op := range old {
		switch {
		case op.kind == wire.UNSUBSCRIBE:
			c.releaseIdentsLocked(op.retired...)
			finished = append(finished, finish{op, ctrlResult{codes: make([]wire.ReasonCode, len(op.topics))}})
		case op.sub.state == subClosed:
			finished = append(finished, finish{op, ctrlResult{err: ErrClosed}})
		}
	}
	for filter, bs := range c.brokerSubs {
		c.releaseIdentsLocked(bs.ids...)
		bs.ids = nil
		bs.granted, bs.pending = false, 0
		if len(bs.holders) == 0 {
			c.router.Unregister(bs.trieKey, bs.trieID)
			delete(c.brokerSubs, filter)
			continue
		}
		c.publishLocked(bs)
	}
	subs := make([]*activeSub, 0, len(c.activeSubs))
	for _, s := range c.activeSubs {
		subs = append(subs, s)
	}
	slices.SortFunc(subs, func(a, b *activeSub) int { return cmp.Compare(a.id, b.id) })
	for _, sub := range subs {
		op := sub.op
		if op == nil {
			op = &ctrlOp{kind: wire.SUBSCRIBE, sub: sub, filters: slices.Clone(sub.filters), resub: true, done: make(chan struct{})}
		}
		// Sent to a session the broker no longer has.
		c.ctrlMu.Lock()
		op.sent = false
		c.ctrlMu.Unlock()
		op.counted = nil
		c.queueSubscribeLocked(op)
	}
	c.subsMu.Unlock()

	for _, r := range released {
		c.engine.ReleaseID(r.id, r.o)
	}
	for _, f := range finished {
		f.op.complete(f.r)
	}
}

// sendCtrl allocates a packet identifier for op on cs and queues its
// packet. ErrNotConnected means cs went away first; op stays queued for
// the next connection. A packet this connection cannot carry fails op.
func (c *Client) sendCtrl(ctx context.Context, stop <-chan struct{}, cs *connState, op *ctrlOp) error {
	id, err := c.engine.AllocateID(ctx, op.owner(), stop)
	if errors.Is(err, inflight.ErrStopped) {
		return ErrClosed
	}
	if err != nil {
		return fmt.Errorf("mqttv5: allocate packet id: %w", err)
	}
	var packet []byte
	if op.kind == wire.SUBSCRIBE {
		packet, err = c.encodeSubscribe(cs, id, op.ident, op.filters)
	} else {
		packet, err = c.encodeUnsubscribe(cs, id, op.topics)
	}
	if err != nil {
		c.engine.ReleaseID(id, op.owner())
		c.failCtrl(op, err)
		return nil
	}
	c.ctrlMu.Lock()
	if !op.queued {
		// Completed meanwhile (Disconnect).
		c.ctrlMu.Unlock()
		c.engine.ReleaseID(id, op.owner())
		return nil
	}
	// A registration left from a connection that died after it.
	stale := c.unregisterLocked(op)
	op.id, op.conn = id, cs
	c.ctrlOps[id] = op
	c.ctrlMu.Unlock()
	if stale != 0 {
		c.engine.ReleaseID(stale, op.owner())
	}

	// Counted before the write, so the answer can never be applied
	// before the send was recorded.
	first := c.markSent(op)
	if err = cs.queue(ctx, writeReq{fn: writeBytes(packet)}, true); err == nil {
		return nil
	}
	if first {
		c.unmarkSent(op)
	}
	c.ctrlMu.Lock()
	if op.conn != cs {
		id = 0
	} else {
		id = c.unregisterLocked(op)
	}
	c.ctrlMu.Unlock()
	if id != 0 {
		c.engine.ReleaseID(id, op.owner())
	}
	return err
}

// failCtrl ends op with err: a SUBSCRIBE ends its subscription. The
// caller holds ctrlSem.
func (c *Client) failCtrl(op *ctrlOp, err error) {
	c.subsMu.Lock()
	c.ctrlMu.Lock()
	ok, stale := c.dequeueLocked(op)
	c.ctrlMu.Unlock()
	if !ok {
		c.subsMu.Unlock()
		return
	}
	closeRoute := func() {}
	if op.kind == wire.SUBSCRIBE {
		c.untagLocked(op)
		closeRoute = c.closeSubLocked(op.sub, err)
	} else {
		c.releaseIdentsLocked(op.retired...)
	}
	c.subsMu.Unlock()
	if stale != 0 {
		c.engine.ReleaseID(stale, op.owner())
	}
	c.cfg.Logger.Warn("mqttv5: cannot send "+op.kind.String(), slog.Any("error", err))
	op.complete(ctrlResult{err: err})
	closeRoute()
	if op.kind == wire.SUBSCRIBE {
		c.settle()
		if op.resub {
			c.reportResubscribeError(op.sub, err)
		}
	}
}

func (c *Client) encodeSubscribe(cs *connState, id uint16, ident uint32, filters []TopicFilter) ([]byte, error) {
	opts := wire.SubscribeOpts{PacketID: id, Filters: wireFilters(filters)}
	if cs.info.SubscriptionIdentifiersAvailable {
		opts.SubscriptionIdentifier = &ident
	}
	var b bytes.Buffer
	if _, err := wire.WriteSubscribe(&b, opts); err != nil {
		return nil, err
	}
	if err := checkPacketSize(cs, b.Len()); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *Client) encodeUnsubscribe(cs *connState, id uint16, topics []string) ([]byte, error) {
	var b bytes.Buffer
	if _, err := wire.WriteUnsubscribe(&b, wire.UnsubscribeOpts{PacketID: id, Topics: topics}); err != nil {
		return nil, err
	}
	if err := checkPacketSize(cs, b.Len()); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// deliverControlAck completes the SUBSCRIBE or UNSUBSCRIBE this SUBACK
// or UNSUBACK answers. An acknowledgement for no operation of its kind on
// this connection is counted and ignored; one with the wrong number of
// reason codes is a protocol error, and the operation is asked again on
// the next connection.
func (c *Client) deliverControlAck(cs *connState, pkt wire.Packet) {
	var (
		id    uint16
		codes []wire.ReasonCode
		want  = wire.SUBSCRIBE
	)
	switch p := pkt.(type) {
	case *wire.Suback:
		id, codes = p.PacketID, slices.Clone(p.ReasonCodes)
	case *wire.Unsuback:
		id, codes, want = p.PacketID, slices.Clone(p.ReasonCodes), wire.UNSUBSCRIBE
	}
	t := pkt.Type()
	pkt.Release()

	c.ctrlMu.Lock()
	op := c.ctrlOps[id]
	if op == nil || op.kind != want || op.conn != cs {
		c.ctrlMu.Unlock()
		c.stats.addAckIgnored()
		c.cfg.Logger.Warn("mqttv5: ignoring acknowledgement for unknown packet identifier",
			slog.String("type", t.String()), slog.Int("packet_id", int(id)))
		return
	}
	c.unregisterLocked(op)
	malformed := len(codes) != op.codeCount()
	queued := true
	if malformed {
		// Not again on this connection, which is about to close.
		op.conn = cs
	} else {
		queued, _ = c.dequeueLocked(op)
	}
	c.ctrlMu.Unlock()
	c.engine.ReleaseID(id, op.owner())
	if !queued {
		return
	}

	if malformed {
		c.protocolViolation(cs, &ProtocolError{Reason: wire.ReasonProtocolError, Packet: t,
			Detail: fmt.Sprintf("%d reason codes for %d filters", len(codes), op.codeCount())})
		return
	}
	if op.kind == wire.SUBSCRIBE {
		c.completeSubscribe(op, codes, nil)
	} else {
		c.completeUnsubscribe(op, codes, nil)
	}
}

// completeSubscribe applies a SUBACK. Each granted filter's subscription
// is now tagged with op's identifier alone; a refused filter is dropped
// from its subscription, which closes with a SubscribeError if none is
// left. A non-nil rejected replaces the SubscribeError: op was given up
// without a SUBACK.
func (c *Client) completeSubscribe(op *ctrlOp, codes []wire.ReasonCode, rejected error) {
	sub := op.sub
	results := make([]SubscribeResult, len(codes))
	emptied := false

	c.subsMu.Lock()
	c.uncountLocked(op)
	for i, rc := range codes {
		f := op.filters[i]
		results[i] = SubscribeResult{Filter: f, Reason: rc}
		bs := c.brokerSubs[f.Topic]
		if bs == nil {
			continue
		}
		if k := slices.Index(bs.ids, op.ident); k >= 0 {
			if rc.IsError() {
				bs.ids = slices.Delete(bs.ids, k, k+1)
				c.releaseIdentsLocked(op.ident)
			} else {
				c.releaseIdentsLocked(bs.ids[:k]...)
				bs.ids = slices.Delete(bs.ids, 0, k)
			}
		}
		if !rc.IsError() {
			bs.granted = true
		}
		if rc.IsError() && sub.state != subClosed {
			if k := slices.Index(bs.holders, sub); k >= 0 {
				bs.holders = slices.Delete(bs.holders, k, k+1)
			}
			emptied = emptied || len(bs.holders) == 0
		}
		c.publishLocked(bs)
	}

	closeRoute := func() {}
	var resubErr error
	if sub.state != subClosed {
		sub.op = nil
		var granted []TopicFilter
		for _, r := range results {
			if r.Granted() {
				granted = append(granted, r.Filter)
			}
		}
		sub.filters = granted
		if !op.resub || sub.results == nil {
			sub.results = results
		}
		var failure error = &SubscribeError{Results: results}
		if rejected != nil {
			failure = rejected
		}
		if len(granted) == 0 {
			closeRoute = c.closeSubLocked(sub, failure)
		} else {
			sub.state = subActive
		}
		if op.resub && len(granted) < len(results) {
			resubErr = failure
		}
	}
	c.subsMu.Unlock()

	op.complete(ctrlResult{codes: codes, err: rejected})
	closeRoute()
	if emptied {
		go c.settleAsync()
	}
	if resubErr != nil {
		c.reportResubscribeError(sub, resubErr)
	}
}

func (c *Client) reportResubscribeError(sub *activeSub, err error) {
	c.cfg.Logger.Warn("mqttv5: broker refused filters when re-subscribing", slog.Any("error", err))
	if c.cfg.OnResubscribeError != nil {
		c.cfg.OnResubscribeError(SubscriptionToken{client: c, sub: sub}, err)
	}
}

func (c *Client) completeUnsubscribe(op *ctrlOp, codes []wire.ReasonCode, rejected error) {
	c.subsMu.Lock()
	c.releaseIdentsLocked(op.retired...)
	c.subsMu.Unlock()
	op.complete(ctrlResult{codes: codes, err: rejected})
}

// maxCtrlAttempts bounds how often a SUBSCRIBE or UNSUBSCRIBE is sent
// when the broker answers it by disconnecting. Some brokers close the
// connection over a packet they will never accept instead of refusing
// it — mosquitto does for a filter deeper than 200 levels — and sending
// it again on every reconnect would never end.
const maxCtrlAttempts = 3

// connectionLost releases the packet identifiers of operations still
// waiting on the dead connection; they stay queued for the next one,
// unless the broker ended the connection blaming a packet it received
// (sd) for the maxCtrlAttempts-th time while they were outstanding.
func (c *Client) connectionLost(sd *DisconnectInfo) {
	blamed := sd != nil && blamesReceivedPacket(sd.ReasonCode)
	var rejected []*ctrlOp
	c.ctrlMu.Lock()
	ops := c.ctrlOps
	c.ctrlOps = make(map[uint16]*ctrlOp)
	for _, op := range ops {
		op.id, op.conn = 0, nil
		if !blamed {
			continue
		}
		if op.rejections++; op.rejections >= maxCtrlAttempts {
			if ok, _ := c.dequeueLocked(op); ok {
				rejected = append(rejected, op)
			}
		}
	}
	c.ctrlMu.Unlock()
	for id, op := range ops {
		c.engine.ReleaseID(id, op.owner())
	}
	for _, op := range rejected {
		err := &RejectedByDisconnectError{Packet: op.kind, Attempts: op.rejections, Disconnect: *sd}
		c.cfg.Logger.Warn("mqttv5: giving up on "+op.kind.String(), slog.Any("error", err))
		codes := make([]wire.ReasonCode, op.codeCount())
		for i := range codes {
			codes[i] = sd.ReasonCode
		}
		if op.kind == wire.SUBSCRIBE {
			c.completeSubscribe(op, codes, err)
		} else {
			c.completeUnsubscribe(op, codes, err)
		}
	}
}

// blamesReceivedPacket reports whether a DISCONNECT reason says the
// broker objected to a packet it received, rather than to the
// connection, its load or its own state.
func blamesReceivedPacket(rc wire.ReasonCode) bool {
	switch rc {
	case wire.ReasonMalformedPacket, wire.ReasonProtocolError, wire.ReasonImplementationSpecificError,
		wire.ReasonNotAuthorized, wire.ReasonTopicFilterInvalid, wire.ReasonTopicNameInvalid,
		wire.ReasonTopicAliasInvalid, wire.ReasonPacketTooLarge, wire.ReasonQuotaExceeded,
		wire.ReasonPayloadFormatInvalid, wire.ReasonRetainNotSupported, wire.ReasonQoSNotSupported,
		wire.ReasonSharedSubscriptionsNotSupported, wire.ReasonSubscriptionIDsNotSupported,
		wire.ReasonWildcardSubscriptionsNotSupported:
		return true
	}
	return false
}

// closeAllSubs ends every subscription and fails every queued operation
// with ErrClosed. Used by Disconnect.
func (c *Client) closeAllSubs() {
	c.subsMu.Lock()
	c.ctrlMu.Lock()
	queue, registered := c.ctrlQueue, c.ctrlOps
	c.ctrlQueue, c.ctrlOps = nil, make(map[uint16]*ctrlOp)
	c.sessionLost = false
	for _, op := range queue {
		op.queued = false
		op.id, op.conn = 0, nil
	}
	c.ctrlMu.Unlock()
	closers := make([]func(), 0, len(c.activeSubs))
	for _, sub := range c.activeSubs {
		closers = append(closers, c.closeSubLocked(sub, nil))
	}
	c.brokerSubs = make(map[string]*brokerSub)
	c.identRefs = make(map[uint32]int)
	c.router.Reset()
	c.subsMu.Unlock()

	for id, op := range registered {
		c.engine.ReleaseID(id, op.owner())
	}
	for _, op := range queue {
		op.complete(ctrlResult{err: ErrClosed})
	}
	for _, closeRoute := range closers {
		closeRoute()
	}
}

// checkFilterCapabilities fails a Subscribe whose filters use a feature
// the broker disabled in CONNACK (§3.2.2.3.{11,13}).
func checkFilterCapabilities(cs *connState, filters []TopicFilter) error {
	for i, f := range filters {
		topic, isShared := stripShareGroup(f.Topic)
		if isShared && !cs.info.SharedSubscriptionAvailable {
			return fmt.Errorf("%w: filter[%d] = %q", ErrSharedSubsUnsupported, i, f.Topic)
		}
		if containsWildcard(topic) && !cs.info.WildcardSubscriptionAvailable {
			return fmt.Errorf("%w: filter[%d] = %q", ErrWildcardSubsUnsupported, i, f.Topic)
		}
	}
	return nil
}

var _ trie.Handler = (*brokerSub)(nil)
