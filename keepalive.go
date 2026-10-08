// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/ashtonian/mqttv5/internal/clock"
	"github.com/ashtonian/mqttv5/wire"
)

// keepAlive is a snapshot the keep-alive scheduler decides from.
type keepAlive struct {
	now       time.Time
	lastWrite time.Time
	lastRead  time.Time
	pingSent  time.Time // zero when no PINGREQ is outstanding
	// heard reports a packet read since pingSent.
	heard   bool
	keep    time.Duration
	timeout time.Duration
}

// decide reports whether to send a PINGREQ now, whether the connection
// is dead, and otherwise how long to wait before deciding again.
//
// A PINGREQ is due once either direction has been quiet for the
// keep-alive: outbound silence would let the broker drop us
// (§3.1.2.10), inbound silence leaves us unsure the broker is alive.
// After a PINGREQ, any inbound packet — not only the PINGRESP — proves
// the broker alive: under an inbound flood the PINGRESP can sit behind
// a long read backlog. With nothing inbound for the ping timeout the
// connection is dead. Because the timeout is shorter than the
// keep-alive, a dead connection is detected before the next PINGREQ
// would be due, and a live one never goes a keep-alive without one.
func (k keepAlive) decide() (send, dead bool, wait time.Duration) {
	if !k.pingSent.IsZero() && !k.heard {
		deadline := k.pingSent.Add(k.timeout)
		if !k.now.Before(deadline) {
			return false, true, 0
		}
		return false, false, deadline.Sub(k.now)
	}
	quiet := k.lastWrite
	if k.lastRead.Before(quiet) {
		quiet = k.lastRead
	}
	due := quiet.Add(k.keep)
	if !k.now.Before(due) {
		return true, false, 0
	}
	return false, false, due.Sub(k.now)
}

// effectivePingTimeout keeps the ping timeout below the keep-alive the
// broker actually granted, which can be shorter than the one configured.
func effectivePingTimeout(configured, keep time.Duration) time.Duration {
	if configured >= keep {
		return keep / 2
	}
	return configured
}

// pingLoop runs the keep-alive for one connection using the effective
// keep-alive from CONNACK (the broker's Server Keep Alive when sent).
func (c *Client) pingLoop(cs *connState) {
	defer cs.wg.Done()
	keep := time.Duration(cs.info.KeepAlive) * time.Second
	timeout := effectivePingTimeout(c.cfg.PingTimeout, keep)
	var timer clock.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var (
		pingSent  time.Time
		readsThen uint64
	)
	for {
		k := keepAlive{
			now:       cs.clk.Now(),
			lastWrite: time.Unix(0, cs.lastWriteUnixNano.Load()),
			lastRead:  time.Unix(0, cs.lastReadUnixNano.Load()),
			pingSent:  pingSent,
			heard:     cs.reads.Load() != readsThen,
			keep:      keep,
			timeout:   timeout,
		}
		if !pingSent.IsZero() && k.heard {
			pingSent = time.Time{}
			k.pingSent = pingSent
		}
		send, dead, wait := k.decide()
		if dead {
			c.cfg.Logger.Warn("mqttv5: no response to PINGREQ; connection presumed dead",
				slog.Duration("ping_timeout", timeout))
			c.stats.addPingTimeout()
			c.handleConnError(cs, errors.New("PINGRESP timeout"))
			return
		}
		if send {
			c.enqueueFireAndForget(cs, func(w io.Writer) (int64, error) {
				return wire.WritePingreq(w)
			})
			pingSent = k.now
			readsThen = cs.reads.Load()
			continue
		}
		if timer == nil {
			timer = cs.clk.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		select {
		case <-timer.C():
		case <-cs.dying:
			return
		case <-c.done():
			return
		}
	}
}
