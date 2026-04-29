package pilot

import (
	"context"
	"net"
	"sync"

	"entmoot/pkg/entmoot"
)

type peerDialLimiter struct {
	mu    sync.Mutex
	limit int
	sems  map[entmoot.NodeID]chan struct{}
}

type peerDialSlot struct {
	once    sync.Once
	release func()
}

func (s *peerDialSlot) Release() {
	if s == nil {
		return
	}
	s.once.Do(s.release)
}

func newPeerDialLimiter(limit int) *peerDialLimiter {
	if limit <= 0 {
		limit = 1
	}
	return &peerDialLimiter{
		limit: limit,
		sems:  make(map[entmoot.NodeID]chan struct{}),
	}
}

func (l *peerDialLimiter) acquire(ctx context.Context, peer entmoot.NodeID, closed <-chan struct{}) error {
	_, err := l.acquireSlot(ctx, peer, closed)
	return err
}

func (l *peerDialLimiter) acquireSlot(ctx context.Context, peer entmoot.NodeID, closed <-chan struct{}) (*peerDialSlot, error) {
	select {
	case <-closed:
		return nil, net.ErrClosed
	default:
	}

	l.mu.Lock()
	sem := l.sems[peer]
	if sem == nil {
		sem = make(chan struct{}, l.limit)
		l.sems[peer] = sem
	}
	l.mu.Unlock()

	select {
	case sem <- struct{}{}:
		select {
		case <-closed:
			l.release(peer)
			return nil, net.ErrClosed
		default:
		}
		return &peerDialSlot{release: func() { l.release(peer) }}, nil
	case <-closed:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *peerDialLimiter) release(peer entmoot.NodeID) {
	l.mu.Lock()
	sem := l.sems[peer]
	l.mu.Unlock()
	if sem == nil {
		return
	}
	select {
	case <-sem:
	default:
	}
}
