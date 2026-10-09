// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package mqttv5

import "sync"

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
