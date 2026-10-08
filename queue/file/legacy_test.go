// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ashtonian/mqttv5/wire"
)

// legacyEntry is the v0.10 QueueEntry as its queue/file wrote it.
type legacyEntry struct {
	Publish    wire.PublishOpts
	EnqueuedAt time.Time
}

func TestImportV010(t *testing.T) {
	legacy := t.TempDir()
	at := time.Unix(1_800_000_000, 0)
	mei := uint32(60)
	for i, e := range []legacyEntry{
		{Publish: wire.PublishOpts{Topic: "a", QoS: 1, Payload: []byte("first"), PacketID: 9}, EnqueuedAt: at},
		{Publish: wire.PublishOpts{Topic: "b", QoS: 2, Payload: []byte("second"), MessageExpiryInterval: &mei}, EnqueuedAt: at},
	} {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(e); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(legacy, fmt.Sprintf("%016x.qent", i+1)), buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(legacy, ".tmp-123"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	q, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	n, err := ImportV010(ctx, q, legacy)
	if err != nil || n != 2 {
		t.Fatalf("ImportV010 = %d, %v", n, err)
	}
	got, err := q.Peek(ctx, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Publish.Payload) != "first" || string(got[1].Publish.Payload) != "second" ||
		got[0].ID == "" || got[0].ID == got[1].ID {
		t.Fatalf("imported %+v", got)
	}
	if got[1].Publish.MessageExpiryInterval != nil || !got[1].ExpiresAt.Equal(at.Add(time.Minute)) {
		t.Fatalf("expiry imported as %v / %v", got[1].Publish.MessageExpiryInterval, got[1].ExpiresAt)
	}
	left, _ := filepath.Glob(filepath.Join(legacy, "*.qent"))
	if len(left) != 0 {
		t.Fatalf("imported files left behind: %v", left)
	}
	if n, err := ImportV010(ctx, q, legacy); n != 0 || err != nil {
		t.Fatalf("second import = %d, %v", n, err)
	}
}
