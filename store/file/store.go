// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

// Package file is a filesystem-backed implementation of
// session.Store. State persists across process restarts: in-flight
// outbound QoS 1/2 publishes survive a crash and are replayed when
// the client reconnects, and inbound QoS 2 PUBREL-pending entries
// are remembered so duplicate PUBLISHes after restart are dropped
// correctly.
//
// Layout under root/:
//
//	outbound/0001.pkt   (binary PUBLISH packet bytes)
//	outbound/0042.pkt
//	inbound/0007        (zero-byte marker file)
//
// Writes use the write-temp-then-rename pattern so a crash mid-write
// can never produce a partial entry. Reads scan the directory and
// parse filenames as uint16 packet IDs.
//
// Stdlib only — opt-in via this submodule so the core mqttv5 module
// stays free of filesystem deps.
package file

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/ashtonian/mqttv5/session"
)

// Store persists session state into root. Safe for concurrent use.
type Store struct {
	root        string
	outboundDir string
	inboundDir  string

	mu sync.Mutex
}

// _ statically verifies the interface.
var _ session.Store = (*Store)(nil)

// Open returns a Store rooted at dir. The directory is created if
// missing. Existing entries are kept (this is the resume-after-crash
// path).
//
// Pass an empty dir to use a fresh temp directory — useful for
// tests; production callers should provide an explicit path.
func Open(dir string) (*Store, error) {
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "mqttv5-store-*")
		if err != nil {
			return nil, fmt.Errorf("mqttv5/store/file: mkdir tmp: %w", err)
		}
	}
	out := filepath.Join(dir, "outbound")
	in := filepath.Join(dir, "inbound")
	for _, d := range []string{out, in} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("mqttv5/store/file: mkdir %s: %w", d, err)
		}
	}
	return &Store{root: dir, outboundDir: out, inboundDir: in}, nil
}

// Root returns the directory the store is rooted in.
func (s *Store) Root() string { return s.root }

// PutOutbound writes the packet bytes under outbound/{id}.pkt using
// the write-temp-then-rename pattern so a crash mid-write produces no
// partial file.
func (s *Store) PutOutbound(id uint16, packet []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeAtomic(filepath.Join(s.outboundDir, formatID(id)+".pkt"), packet)
}

// DeleteOutbound removes the outbound entry. Missing entries are not
// an error (matches MemoryStore semantics).
func (s *Store) DeleteOutbound(id uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(filepath.Join(s.outboundDir, formatID(id)+".pkt"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// RangeOutbound reads every outbound/*.pkt file. The yield function
// receives the packet ID parsed from the filename plus the file
// bytes. Stops early if yield returns false.
func (s *Store) RangeOutbound(yield func(id uint16, packet []byte) bool) error {
	s.mu.Lock()
	entries, err := os.ReadDir(s.outboundDir)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("mqttv5/store/file: read outbound: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !hasSuffix(name, ".pkt") {
			continue
		}
		id, ok := parseID(name[:len(name)-4])
		if !ok {
			continue
		}
		s.mu.Lock()
		b, err := os.ReadFile(filepath.Join(s.outboundDir, name))
		s.mu.Unlock()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				// Deleted between ReadDir and ReadFile — skip.
				continue
			}
			return fmt.Errorf("mqttv5/store/file: read %s: %w", name, err)
		}
		if !yield(id, b) {
			return nil
		}
	}
	return nil
}

// PutInbound creates a zero-byte marker file. The file's presence is
// the entire signal — there's no payload to store for QoS 2
// in-progress entries.
func (s *Store) PutInbound(id uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.inboundDir, formatID(id))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("mqttv5/store/file: create inbound %s: %w", path, err)
	}
	return f.Close()
}

// DeleteInbound removes the inbound marker. Missing entries are not
// an error.
func (s *Store) DeleteInbound(id uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := os.Remove(filepath.Join(s.inboundDir, formatID(id)))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// RangeInbound iterates inbound marker filenames.
func (s *Store) RangeInbound(yield func(id uint16) bool) error {
	s.mu.Lock()
	entries, err := os.ReadDir(s.inboundDir)
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("mqttv5/store/file: read inbound: %w", err)
	}
	for _, e := range entries {
		id, ok := parseID(e.Name())
		if !ok {
			continue
		}
		if !yield(id) {
			return nil
		}
	}
	return nil
}

// Reset wipes both directories (called on CleanStart=true).
func (s *Store) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range []string{s.outboundDir, s.inboundDir} {
		entries, err := os.ReadDir(d)
		if err != nil {
			return fmt.Errorf("mqttv5/store/file: read %s: %w", d, err)
		}
		for _, e := range entries {
			if err := os.Remove(filepath.Join(d, e.Name())); err != nil {
				return fmt.Errorf("mqttv5/store/file: remove %s: %w", e.Name(), err)
			}
		}
	}
	return nil
}

// writeAtomic writes data to path via a tmp file + rename. On most
// modern filesystems rename is atomic (the file is either fully
// written or absent — never partial).
//
// fsync is intentionally not called: that's a much harder durability
// guarantee that the caller can layer on top if they need it.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("mqttv5/store/file: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("mqttv5/store/file: rename tmp: %w", err)
	}
	return nil
}

// formatID renders id as a zero-padded 5-digit string so filenames
// sort lexicographically by packet ID. Max packet ID is 65535 = 5
// digits.
func formatID(id uint16) string {
	return fmt.Sprintf("%05d", id)
}

// parseID is the inverse of formatID. Returns ok=false for filenames
// that aren't pure decimal digits.
func parseID(name string) (uint16, bool) {
	n, err := strconv.ParseUint(name, 10, 16)
	if err != nil {
		return 0, false
	}
	return uint16(n), true
}

// hasSuffix avoids importing strings just for one call.
func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
