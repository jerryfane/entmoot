package mailbox

import (
	"context"
	"sync"

	"entmoot/pkg/entmoot"
)

// MemoryCursorStore stores mailbox cursors in process memory.
type MemoryCursorStore struct {
	mu      sync.RWMutex
	cursors map[cursorKey]Cursor
}

type cursorKey struct {
	group  entmoot.GroupID
	client string
}

// NewMemoryCursorStore returns an in-memory cursor store. It is useful for
// tests and ephemeral service peers; ESP deployments should prefer SQLite.
func NewMemoryCursorStore() *MemoryCursorStore {
	return &MemoryCursorStore{cursors: make(map[cursorKey]Cursor)}
}

// GetCursor implements CursorStore.
func (s *MemoryCursorStore) GetCursor(_ context.Context, groupID entmoot.GroupID, clientID string) (Cursor, error) {
	if clientID == "" {
		return Cursor{}, ErrInvalidClient
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cursors[cursorKey{group: groupID, client: clientID}], nil
}

// AckCursor implements CursorStore.
func (s *MemoryCursorStore) AckCursor(_ context.Context, groupID entmoot.GroupID, clientID string, cursor Cursor) (bool, error) {
	if clientID == "" {
		return false, ErrInvalidClient
	}
	if cursor == (Cursor{}) {
		return false, nil
	}
	k := cursorKey{group: groupID, client: clientID}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursorAfter(cursor, s.cursors[k]) {
		s.cursors[k] = cursor
		return true, nil
	}
	return false, nil
}

// Close implements CursorStore.
func (s *MemoryCursorStore) Close() error {
	return nil
}
