package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/tlsrevocation"
)

// mapStore is the simplest correct Store, against which the suite must
// pass.
type mapStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func (s *mapStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.values[key]
	return v, ok, nil
}

func (s *mapStore) Put(_ context.Context, key string, value []byte, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
	return nil
}

func TestRun(t *testing.T) {
	Run(t, Factory{
		Open: func(*testing.T) tlsrevocation.Store { return &mapStore{values: make(map[string][]byte)} },
		// Reopening returns the same map: the backing storage outlives
		// the handle.
		Reopen: func(_ *testing.T, s tlsrevocation.Store) tlsrevocation.Store { return s },
	})
}
