// Copyright 2026 Ashton Kinslow. SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ashtonian/mqttv5/session"
)

// BenchmarkPut measures durable record writes per SyncPolicy with an
// explicit number of concurrent writers. ns/op is wall time per write
// across all writers, so 1e9/ns_op is the store's write throughput.
func BenchmarkPut(b *testing.B) {
	policies := []struct {
		name string
		p    SyncPolicy
	}{{"group-commit", SyncGroupCommit}, {"every-write", SyncEveryWrite}, {"none", SyncNone}}
	packet := make([]byte, 256)
	for _, pol := range policies {
		for _, workers := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("%s/workers=%d", pol.name, workers), func(b *testing.B) {
				s, err := Open(b.TempDir(), WithSyncPolicy(pol.p))
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				ctx := context.Background()
				var next atomic.Uint64
				b.SetBytes(int64(len(packet)))
				b.ResetTimer()
				var wg sync.WaitGroup
				for w := 0; w < workers; w++ {
					wg.Go(func() {
						for {
							i := next.Add(1)
							if i > uint64(b.N) {
								return
							}
							r := session.Record{
								Key: session.RecordKey{Dir: session.Outbound, PacketID: uint16(i%65535) + 1},
								Seq: i, QoS: 1, Phase: session.AwaitPuback, Packet: packet,
							}
							if err := s.Put(ctx, r); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				wg.Wait()
			})
		}
	}
}
