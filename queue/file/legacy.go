// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ashtonian/mqttv5"
)

// legacySuffix names the one-file-per-entry layout of v0.10.
const legacySuffix = ".qent"

// ImportV010 moves the entries of a v0.10 queue directory (one
// gob-encoded *.qent file per entry) into q, oldest first, and deletes
// each file once its entry is stored, so an interrupted import can be
// run again without duplicating entries. It returns how many entries it
// moved. Each entry gets a new ID; a Message Expiry Interval becomes an
// expiry counted from the entry's original enqueue time. v0.10 lost an
// explicit Message Expiry Interval of 0 when it wrote the file; such
// entries come back without expiry.
func ImportV010(ctx context.Context, q *Queue, dir string) (int, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("mqttv5/queue/file: read %s: %w", dir, err)
	}
	var names []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), legacySuffix) && len(f.Name()) == 16+len(legacySuffix) {
			names = append(names, f.Name())
		}
	}
	slices.Sort(names) // fixed-width hex: lexical order is sequence order
	moved := 0
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return moved, fmt.Errorf("mqttv5/queue/file: read %s: %w", path, err)
		}
		var e mqttv5.QueueEntry
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&e); err != nil {
			return moved, fmt.Errorf("mqttv5/queue/file: decode %s: %w", path, err)
		}
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return moved, err
		}
		e.Seq, e.ID = 0, hex.EncodeToString(id[:])
		if mei := e.Publish.MessageExpiryInterval; mei != nil {
			e.ExpiresAt = e.EnqueuedAt.Add(time.Duration(*mei) * time.Second)
			e.Publish.MessageExpiryInterval = nil
		}
		if _, _, err := q.Enqueue(ctx, e, mqttv5.QueueLimit{}); err != nil {
			return moved, fmt.Errorf("mqttv5/queue/file: import %s: %w", path, err)
		}
		if err := os.Remove(path); err != nil {
			return moved, fmt.Errorf("mqttv5/queue/file: imported %s but could not remove it: %w", path, err)
		}
		moved++
	}
	return moved, nil
}
