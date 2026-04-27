package gossip

import (
	"log/slog"
	"time"

	"entmoot/pkg/entmoot"
)

func (g *Gossiper) traceReconcile(peer entmoot.NodeID, phase string, attrs ...any) {
	if !g.cfg.TraceReconcile {
		return
	}
	base := []any{
		slog.String("phase", phase),
		slog.Uint64("peer", uint64(peer)),
		slog.String("group", g.cfg.GroupID.String()),
	}
	g.logger.Info("reconcile trace", append(base, attrs...)...)
}

func since(start time.Time) slog.Attr {
	return slog.Duration("duration", time.Since(start))
}
