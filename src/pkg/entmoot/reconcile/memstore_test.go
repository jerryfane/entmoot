package reconcile

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"entmoot/pkg/entmoot"
)

// memStorage is a mutex-guarded in-memory Storage implementation used only
// by tests in this package. It keeps ids in a map and materialises sorted
// slices on each IterIDs call, which is wasteful but keeps the code
// obvious.
type memStorage struct {
	mu  sync.RWMutex
	ids map[[32]byte]struct{}
}

func newMemStorage() *memStorage {
	return &memStorage{ids: make(map[[32]byte]struct{})}
}

func (m *memStorage) add(id entmoot.MessageID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ids[[32]byte(id)] = struct{}{}
}

func (m *memStorage) addMany(ids []entmoot.MessageID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.ids[[32]byte(id)] = struct{}{}
	}
}

func (m *memStorage) IterIDs(_ context.Context, lo, hi entmoot.MessageID) ([]entmoot.MessageID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]entmoot.MessageID, 0)
	for k := range m.ids {
		id := entmoot.MessageID(k)
		if !inRange(id, lo, hi) {
			continue
		}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out, nil
}

func (m *memStorage) CountInRange(ctx context.Context, lo, hi entmoot.MessageID) (int, error) {
	ids, err := m.IterIDs(ctx, lo, hi)
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (m *memStorage) HasID(_ context.Context, id entmoot.MessageID) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.ids[[32]byte(id)]
	return ok, nil
}

// inRange reports whether id falls in [lo, hi). A hi of zeroID means "max"
// (every id >= lo matches).
func inRange(id, lo, hi entmoot.MessageID) bool {
	if bytes.Compare(id[:], lo[:]) < 0 {
		return false
	}
	if hi == zeroID {
		return true
	}
	return bytes.Compare(id[:], hi[:]) < 0
}
