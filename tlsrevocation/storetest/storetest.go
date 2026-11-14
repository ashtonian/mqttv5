// Package storetest checks that a [tlsrevocation.Store] implementation
// honours the contract the checker relies on. Call [Run] from the
// implementation's tests:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, storetest.Factory{
//			Open: func(t *testing.T) tlsrevocation.Store { return mystore.Open(t.TempDir()) },
//		})
//	}
package storetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/tlsrevocation"
)

// Factory opens stores under test.
type Factory struct {
	// Open returns a new, empty store. A store that needs closing
	// registers that with t.Cleanup.
	Open func(t *testing.T) tlsrevocation.Store

	// Reopen, when set, closes s and opens the same backing storage
	// again, so Run can check that entries survive a restart. Leave it
	// nil for stores that live only in memory.
	Reopen func(t *testing.T, s tlsrevocation.Store) tlsrevocation.Store
}

// Run executes the conformance suite as subtests of t.
func Run(t *testing.T, f Factory) {
	t.Run("Missing", func(t *testing.T) { testMissing(t, f.Open(t)) })
	t.Run("RoundTrip", func(t *testing.T) { testRoundTrip(t, f.Open(t)) })
	t.Run("PutReplaces", func(t *testing.T) { testPutReplaces(t, f.Open(t)) })
	t.Run("ReturnedValueUnchanged", func(t *testing.T) { testReturnedValueUnchanged(t, f.Open(t)) })
	t.Run("LargestCRL", func(t *testing.T) { testLargestCRL(t, f.Open(t)) })
	t.Run("Concurrent", func(t *testing.T) { testConcurrent(t, f.Open(t)) })
	if f.Reopen != nil {
		t.Run("SurvivesReopen", func(t *testing.T) {
			s := f.Open(t)
			testSurvivesReopen(t, s, func() tlsrevocation.Store { return f.Reopen(t, s) })
		})
	}
}

// expires is far enough ahead that no store drops an entry during the
// suite, and fixed so the suite does not depend on the clock.
var expires = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

// key returns a key of the form the checker uses.
func key(kind string, n int) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%d", n))
	return kind + "-" + hex.EncodeToString(sum[:])
}

func value(n, size int) []byte {
	v := bytes.Repeat([]byte{byte(n)}, size)
	copy(v, fmt.Sprintf("value-%d", n))
	return v
}

func get(t *testing.T, s tlsrevocation.Store, k string) ([]byte, bool) {
	t.Helper()
	v, ok, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get(%s): %v", k, err)
	}
	return v, ok
}

func put(t *testing.T, s tlsrevocation.Store, k string, v []byte) {
	t.Helper()
	if err := s.Put(context.Background(), k, v, expires); err != nil {
		t.Fatalf("Put(%s): %v", k, err)
	}
}

func want(t *testing.T, s tlsrevocation.Store, k string, v []byte) {
	t.Helper()
	got, ok := get(t, s, k)
	if !ok {
		t.Fatalf("Get(%s): not found", k)
	}
	if !bytes.Equal(got, v) {
		t.Fatalf("Get(%s) = %d bytes %.16q, want %d bytes %.16q", k, len(got), got, len(v), v)
	}
}

func testMissing(t *testing.T, s tlsrevocation.Store) {
	if v, ok := get(t, s, key("ocsp", 1)); ok {
		t.Fatalf("empty store Get = %q, true; want not found", v)
	}
}

func testRoundTrip(t *testing.T, s tlsrevocation.Store) {
	keys := []string{key("ocsp", 1), key("ocsp", 2), key("crl", 1)}
	for i, k := range keys {
		put(t, s, k, value(i, 512))
	}
	for i, k := range keys {
		want(t, s, k, value(i, 512))
	}
}

func testPutReplaces(t *testing.T, s tlsrevocation.Store) {
	k := key("crl", 1)
	put(t, s, k, value(1, 2048))
	put(t, s, k, value(2, 100))
	want(t, s, k, value(2, 100))
}

// The checker keeps using a value Get returned while the store goes on.
func testReturnedValueUnchanged(t *testing.T, s tlsrevocation.Store) {
	k := key("ocsp", 1)
	put(t, s, k, value(1, 512))
	got, _ := get(t, s, k)
	put(t, s, k, value(2, 512))
	if !bytes.Equal(got, value(1, 512)) {
		t.Fatal("a value Get returned changed when the key was replaced")
	}
}

// The checker stores CRLs up to its default size cap.
func testLargestCRL(t *testing.T, s tlsrevocation.Store) {
	k := key("crl", 1)
	v := value(7, tlsrevocation.DefaultMaxCRLBytes)
	put(t, s, k, v)
	want(t, s, k, v)
}

// Concurrent readers see a whole value one writer wrote, never a mix.
func testConcurrent(t *testing.T, s tlsrevocation.Store) {
	const writers, rounds, keys = 8, 50, 4
	var wg sync.WaitGroup
	errs := make(chan error, writers*rounds)
	for w := range writers {
		wg.Go(func() {
			for r := range rounds {
				k := key("ocsp", r%keys)
				if err := s.Put(context.Background(), k, value(w, 256), expires); err != nil {
					errs <- fmt.Errorf("Put(%s): %w", k, err)
					return
				}
				v, ok, err := s.Get(context.Background(), k)
				switch {
				case err != nil:
					errs <- fmt.Errorf("Get(%s): %w", k, err)
					return
				case !ok:
					errs <- fmt.Errorf("Get(%s) after Put: not found", k)
					return
				case !wholeValue(v, writers):
					errs <- fmt.Errorf("Get(%s) = %.16q: not a value any writer wrote", k, v)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func wholeValue(v []byte, writers int) bool {
	for w := range writers {
		if bytes.Equal(v, value(w, 256)) {
			return true
		}
	}
	return false
}

func testSurvivesReopen(t *testing.T, s tlsrevocation.Store, reopen func() tlsrevocation.Store) {
	ocspKey, crlKey := key("ocsp", 1), key("crl", 1)
	put(t, s, ocspKey, value(1, 512))
	put(t, s, crlKey, value(2, 4096))
	r := reopen()
	want(t, r, ocspKey, value(1, 512))
	want(t, r, crlKey, value(2, 4096))
}
