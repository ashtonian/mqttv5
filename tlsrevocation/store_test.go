package tlsrevocation

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"

	"github.com/ashtonian/mqttv5/internal/testpki"
)

// memStore is a Store in memory that counts its calls. While fail is
// set every call fails; while gate is set, Get waits for it to close.
type memStore struct {
	mu     sync.Mutex
	values map[string][]byte

	gets, puts atomic.Int32
	fail       atomic.Bool
	gate       atomic.Pointer[chan struct{}]
}

var errStoreDown = errors.New("store down")

func newMemStore() *memStore { return &memStore{values: make(map[string][]byte)} }

func (s *memStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.gets.Add(1)
	if gate := s.gate.Load(); gate != nil {
		select {
		case <-*gate:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	if s.fail.Load() {
		return nil, false, errStoreDown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	return v, ok, nil
}

func (s *memStore) Put(_ context.Context, key string, value []byte, _ time.Time) error {
	s.puts.Add(1)
	if s.fail.Load() {
		return errStoreDown
	}
	s.set(key, value)
	return nil
}

func (s *memStore) set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
}

func (s *memStore) value(key string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[key]
}

// fetches counts the requests every responder and CRL server received.
func (f *fixture) fetches() int32 {
	return f.ocsp.requests.Load() + f.leafCRL.requests.Load() + f.rootCRL.requests.Load()
}

func (f *fixture) down() {
	f.ocsp.down.Store(true)
	f.leafCRL.down.Store(true)
	f.rootCRL.down.Store(true)
}

// What a checker fetches reaches the Store, and a checker built on that
// Store after a restart answers from it while every source is down.
func TestVerifyStoreSurvivesRestart(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		wantErr  error
		wantHits uint64 // the leaf's OCSP response, then the root's CRL
	}{
		{name: "good", status: ocsp.Good, wantHits: 2},
		{name: "revoked", status: ocsp.Revoked, wantErr: ErrRevoked, wantHits: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.ocsp.set(f.leaf.Leaf, tt.status)
			store := newMemStore()
			state := f.state(nil)
			if err := f.checker(t, WithStore(store)).Verify(context.Background(), nil, state); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Verify = %v, want %v", err, tt.wantErr)
			}
			if store.value(ocspKey(f.intermediate.Cert, f.leaf.Leaf)) == nil {
				t.Fatal("the fetched OCSP response is not in the store")
			}

			f.down()
			before := f.fetches()
			restarted := f.checker(t, WithStore(store))
			for range 2 {
				err := restarted.Verify(context.Background(), nil, state)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Verify after a restart with every source down = %v, want %v", err, tt.wantErr)
				}
				var rerr *RevocationError
				if errors.As(err, &rerr) && rerr.Source != SourceOCSP {
					t.Fatalf("revocation Source = %v, want %v", rerr.Source, SourceOCSP)
				}
			}
			if n := f.fetches() - before; n != 0 {
				t.Fatalf("%d fetches after the restart, want none", n)
			}
			// The second Verify is answered by the cache the first filled.
			if s := restarted.Stats(); s.StoreHits != tt.wantHits || s.CacheHits != tt.wantHits || s.FetchFailures != 0 {
				t.Fatalf("Stats = %+v, want %d store hits and %d cache hits, no fetch failures", s, tt.wantHits, tt.wantHits)
			}
		})
	}
}

// The Store is not trusted with the verdict: a stored value that fails
// verification is refetched and replaced.
func TestVerifyStoreRejects(t *testing.T) {
	other := testpki.NewRoot(t, "unrelated")
	stale := func(r *ocsp.Response) {
		r.ThisUpdate = testpki.Now.Add(-3 * time.Hour)
		r.NextUpdate = testpki.Now.Add(-DefaultClockSkew - time.Minute)
	}
	tests := []struct {
		name string
		// crl tampers with the root's CRL, which the intermediate is
		// checked against, instead of the leaf's OCSP response.
		crl    bool
		status int // the responder's answer about the leaf
		value  func(f *fixture) []byte
		// capBelowValue caps OCSP responses one byte below the stored
		// value, which is valid, but larger than the responder's.
		capBelowValue bool
		wantErr       error
	}{
		{name: "malformed OCSP response", value: func(*fixture) []byte { return []byte("not DER") }},
		{name: "OCSP response from an unrelated CA", value: func(f *fixture) []byte {
			return ocspResponse(t, other, nil, f.leaf.Leaf.SerialNumber, ocsp.Good)
		}},
		{name: "forged good response for a revoked certificate", status: ocsp.Revoked, wantErr: ErrRevoked, value: func(f *fixture) []byte {
			return ocspResponse(t, other, nil, f.leaf.Leaf.SerialNumber, ocsp.Good)
		}},
		{name: "stale OCSP response", value: func(f *fixture) []byte { return f.staple(t, ocsp.Good, stale) }},
		{name: "unknown OCSP response", value: func(f *fixture) []byte { return f.staple(t, ocsp.Unknown) }},
		{name: "oversize OCSP response", capBelowValue: true, value: func(f *fixture) []byte {
			// Valid, and larger than the responder's own: it embeds the
			// delegate's certificate.
			delegate := f.intermediate.Leaf(t, "responder", testpki.WithExtKeyUsage(x509.ExtKeyUsageOCSPSigning))
			return ocspResponse(t, f.intermediate, &delegate, f.leaf.Leaf.SerialNumber, ocsp.Good)
		}},
		{name: "CRL from an unrelated CA", crl: true, value: func(*fixture) []byte { return crl(t, other, nil) }},
		{name: "malformed CRL", crl: true, value: func(*fixture) []byte { return []byte("not DER") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			status := tt.status
			if status == 0 {
				status = ocsp.Good
			}
			f.ocsp.set(f.leaf.Leaf, status)
			store := newMemStore()
			key, source := ocspKey(f.intermediate.Cert, f.leaf.Leaf), &f.ocsp.requests
			if tt.crl {
				key, source = crlKey(f.root.Cert, f.rootCRL.URL()), &f.rootCRL.requests
			}
			bad := tt.value(f)
			store.set(key, bad)
			opts := []Option{WithStore(store)}
			if tt.capBelowValue {
				opts = append(opts, WithMaxOCSPResponseBytes(int64(len(bad)-1)))
			}
			c := f.checker(t, opts...)

			if err := c.Verify(context.Background(), nil, f.state(nil)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Verify = %v, want %v", err, tt.wantErr)
			}
			if s := c.Stats(); s.StoreRejects != 1 {
				t.Fatalf("StoreRejects = %d, want 1", s.StoreRejects)
			}
			if n := source.Load(); n != 1 {
				t.Fatalf("source fetched %d times, want 1", n)
			}
			if bytes.Equal(store.value(key), bad) {
				t.Fatal("the rejected value was not replaced by the fetch")
			}
		})
	}
}

// A failing Store degrades to fetching: its errors are counted, never
// returned.
func TestVerifyStoreErrors(t *testing.T) {
	f := newFixture(t)
	f.ocsp.set(f.leaf.Leaf, ocsp.Good)
	store := newMemStore()
	store.fail.Store(true)
	c := f.checker(t, WithStore(store), WithScope(LeafOnly))
	if err := c.Verify(context.Background(), nil, f.state(nil)); err != nil {
		t.Fatalf("Verify with a failing store: %v", err)
	}
	if s := c.Stats(); s.StoreErrors != 2 || s.FromOCSP != 1 {
		t.Fatalf("Stats = %+v, want 2 store errors (Get and Put) and an answer from the responder", s)
	}
}

// Concurrent checks that miss the cache share one Store read.
func TestVerifyStoreSingleFlight(t *testing.T) {
	f := newFixture(t)
	f.ocsp.set(f.leaf.Leaf, ocsp.Good)
	store := newMemStore()
	state := f.state(nil)
	if err := f.checker(t, WithStore(store), WithScope(LeafOnly)).Verify(context.Background(), nil, state); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	gate := make(chan struct{})
	store.gate.Store(&gate)
	store.gets.Store(0)
	c := f.checker(t, WithStore(store), WithScope(LeafOnly))

	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Go(func() { errs <- c.Verify(context.Background(), nil, state) })
	}
	deadline := time.Now().Add(3 * time.Second)
	for store.gets.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
	if n := store.gets.Load(); n != 1 {
		t.Fatalf("store read %d times by %d concurrent checks, want 1", n, callers)
	}
	if n := f.ocsp.requests.Load(); n != 1 {
		t.Fatalf("responder asked %d times, want only the first checker's fetch", n)
	}
}

// Keys are file-name safe, and a cross-signed copy of an issuer, with
// the same name and key, shares its entries.
func TestStoreKeys(t *testing.T) {
	f := newFixture(t)
	crossSigned := f.intermediate.CrossSigned(t, testpki.NewRoot(t, "other root"))
	format := regexp.MustCompile(`^(ocsp|crl)-[0-9a-f]{64}$`)
	keys := []string{
		ocspKey(f.intermediate.Cert, f.leaf.Leaf),
		ocspKey(f.root.Cert, f.leaf.Leaf),
		ocspKey(f.root.Cert, f.intermediate.Cert),
		crlKey(f.intermediate.Cert, f.leafCRL.URL()),
		crlKey(f.intermediate.Cert, f.rootCRL.URL()),
		crlKey(f.root.Cert, f.rootCRL.URL()),
	}
	seen := make(map[string]bool)
	for _, k := range keys {
		if !format.MatchString(k) {
			t.Errorf("key %q does not match %v", k, format)
		}
		if seen[k] {
			t.Errorf("key %q identifies two entries", k)
		}
		seen[k] = true
	}
	if ocspKey(crossSigned.Cert, f.leaf.Leaf) != keys[0] {
		t.Error("a cross-signed issuer does not share its original's OCSP key")
	}
}
