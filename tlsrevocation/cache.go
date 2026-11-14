package tlsrevocation

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// cache is a bounded LRU map whose entries expire. A zero-capacity cache
// stores nothing.
type cache[V any] struct {
	mu    sync.Mutex
	max   int
	order *list.List // front: most recently used
	items map[string]*list.Element
}

type cacheEntry[V any] struct {
	key     string
	value   V
	expires time.Time
}

func newCache[V any](max int) *cache[V] {
	return &cache[V]{max: max, order: list.New(), items: make(map[string]*list.Element)}
}

func (c *cache[V]) get(key string, now time.Time) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	e := el.Value.(*cacheEntry[V])
	if !now.Before(e.expires) {
		c.order.Remove(el)
		delete(c.items, key)
		return zero, false
	}
	c.order.MoveToFront(el)
	return e.value, true
}

func (c *cache[V]) put(key string, value V, expires time.Time) {
	if c.max == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		el.Value = &cacheEntry[V]{key: key, value: value, expires: expires}
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&cacheEntry[V]{key: key, value: value, expires: expires})
	for c.order.Len() > c.max {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*cacheEntry[V]).key)
	}
}

// flight runs one fetch per key at a time; callers asking for a key
// already being fetched wait for that fetch.
//
// The fetch runs on its own context, detached from the caller's and
// bounded by timeout, so a caller that gives up does not fail the others
// waiting on it, and the result still reaches the cache. Each caller
// stops waiting when its own context ends.
type flight[V any] struct {
	mu    sync.Mutex
	calls map[string]*flightCall[V]
}

type flightCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

func (f *flight[V]) do(ctx context.Context, key string, timeout time.Duration, fetch func(context.Context) (V, error)) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	f.mu.Lock()
	if f.calls == nil {
		f.calls = make(map[string]*flightCall[V])
	}
	call, ok := f.calls[key]
	if !ok {
		call = &flightCall[V]{done: make(chan struct{})}
		f.calls[key] = call
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		go func() {
			defer cancel()
			call.value, call.err = fetch(fetchCtx)
			f.mu.Lock()
			delete(f.calls, key)
			f.mu.Unlock()
			close(call.done)
		}()
	}
	f.mu.Unlock()
	select {
	case <-call.done:
		// A context that ended while the fetch finished still wins: the
		// caller has abandoned the check.
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return call.value, call.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Stats counts the checker's work since it was built.
type Stats struct {
	// Checked counts certificates checked; each ends Good, Revoked or
	// Unknown.
	Checked, Good, Revoked, Unknown uint64

	// AllowedUnknown counts the Unknown certificates FailOpen allowed.
	AllowedUnknown uint64

	// FromStaple, FromCRLProvider, FromOCSP and FromCRL count the Good
	// and Revoked answers by source.
	FromStaple, FromCRLProvider, FromOCSP, FromCRL uint64

	// CacheHits and CacheMisses count in-memory lookups of OCSP answers
	// and CRLs.
	CacheHits, CacheMisses uint64

	// StoreHits, StoreMisses and StoreRejects count [Store] reads after
	// a cache miss: a value that verified, no value, and a value that
	// failed verification and was refetched. StoreErrors counts Get and
	// Put calls that returned an error.
	StoreHits, StoreMisses, StoreRejects, StoreErrors uint64

	// FetchFailures counts OCSP and CRL fetches that produced no usable
	// answer.
	FetchFailures uint64
}

// Stats returns a snapshot of the checker's counters.
func (c *Checker) Stats() Stats {
	s := &c.stats
	return Stats{
		Checked:         s.checked.Load(),
		Good:            s.good.Load(),
		Revoked:         s.revoked.Load(),
		Unknown:         s.unknown.Load(),
		AllowedUnknown:  s.allowedUnknown.Load(),
		FromStaple:      s.fromStaple.Load(),
		FromCRLProvider: s.fromProvider.Load(),
		FromOCSP:        s.fromOCSP.Load(),
		FromCRL:         s.fromCRL.Load(),
		CacheHits:       s.cacheHits.Load(),
		CacheMisses:     s.cacheMisses.Load(),
		StoreHits:       s.storeHits.Load(),
		StoreMisses:     s.storeMisses.Load(),
		StoreRejects:    s.storeRejects.Load(),
		StoreErrors:     s.storeErrors.Load(),
		FetchFailures:   s.fetchFailures.Load(),
	}
}

type counters struct {
	checked, good, revoked, unknown, allowedUnknown   atomic.Uint64
	fromStaple, fromProvider, fromOCSP, fromCRL       atomic.Uint64
	cacheHits, cacheMisses, fetchFailures             atomic.Uint64
	storeHits, storeMisses, storeRejects, storeErrors atomic.Uint64
}

func (s *counters) bySource(src Source) {
	switch src {
	case SourceStaple:
		s.fromStaple.Add(1)
	case SourceCRLProvider:
		s.fromProvider.Add(1)
	case SourceOCSP:
		s.fromOCSP.Add(1)
	case SourceCRL:
		s.fromCRL.Add(1)
	}
}
