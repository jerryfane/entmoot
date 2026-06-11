package gossip

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func BenchmarkInMemoryBroadcast50(b *testing.B) {
	benchmarkInMemoryBroadcast(b, 50)
}

func BenchmarkInMemoryBroadcast100(b *testing.B) {
	benchmarkInMemoryBroadcast(b, 100)
}

func benchmarkInMemoryBroadcast(b *testing.B, n int) {
	nodeIDs := make([]entmoot.NodeID, n)
	for i := range nodeIDs {
		nodeIDs[i] = entmoot.NodeID(30_000 + i)
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f := newFixture(b, nodeIDs)
		discard := slog.New(slog.NewTextHandler(io.Discard, nil))
		for _, ns := range f.nodes {
			ns.gossip.logger = discard
			ns.gossip.cfg.Logger = discard
		}
		ctx, cancel := context.WithCancel(context.Background())
		f.startAll(ctx)
		author := nodeIDs[0]
		msg := f.buildMessage(author, fmt.Sprintf("bench-%d", i), int64(10_000+i))
		start := time.Now()
		b.StartTimer()
		if err := f.nodes[author].gossip.Publish(ctx, msg); err != nil {
			b.Fatal(err)
		}
		waitUntil(b, 10*time.Second, "benchmark message convergence", func() bool {
			for _, id := range nodeIDs {
				has, err := f.nodes[id].storeM.Has(ctx, f.groupID, msg.ID)
				if err != nil || !has {
					return false
				}
			}
			return true
		})
		b.StopTimer()
		b.ReportMetric(float64(time.Since(start).Microseconds())/1000, "converge_ms/op")
		cancel()
		f.closeTransports()
		b.StartTimer()
	}
}
