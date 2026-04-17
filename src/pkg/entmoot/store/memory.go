package store

import (
	"context"
	"fmt"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/order"
)

// Memory is an in-memory MessageStore. All state lives in a mutex-guarded map
// and does not survive process restart. Every method is safe for concurrent
// use by any number of goroutines.
//
// It exists primarily for tests and as a reference baseline exercised by the
// shared store test suite alongside JSONL.
type Memory struct {
	mu     sync.RWMutex
	groups map[entmoot.GroupID]map[entmoot.MessageID]entmoot.Message
}

// NewMemory returns an empty in-memory MessageStore.
func NewMemory() *Memory {
	return &Memory{
		groups: make(map[entmoot.GroupID]map[entmoot.MessageID]entmoot.Message),
	}
}

// Put implements MessageStore.Put.
func (s *Memory) Put(_ context.Context, m entmoot.Message) error {
	if isZeroGroupID(m.GroupID) {
		return fmt.Errorf("%w: zero group id", ErrInvalidMessage)
	}
	if isZeroMessageID(m.ID) {
		return fmt.Errorf("%w: zero message id", ErrInvalidMessage)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, ok := s.groups[m.GroupID]
	if !ok {
		bucket = make(map[entmoot.MessageID]entmoot.Message)
		s.groups[m.GroupID] = bucket
	}
	if _, exists := bucket[m.ID]; exists {
		// Idempotent: same id already present, no-op.
		return nil
	}
	bucket[m.ID] = m
	return nil
}

// Get implements MessageStore.Get.
func (s *Memory) Get(_ context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket, ok := s.groups[groupID]
	if !ok {
		return entmoot.Message{}, ErrNotFound
	}
	m, ok := bucket[id]
	if !ok {
		return entmoot.Message{}, ErrNotFound
	}
	return m, nil
}

// Has implements MessageStore.Has.
func (s *Memory) Has(_ context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket, ok := s.groups[groupID]
	if !ok {
		return false, nil
	}
	_, ok = bucket[id]
	return ok, nil
}

// Range implements MessageStore.Range. See the interface docs for semantics.
func (s *Memory) Range(_ context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error) {
	s.mu.RLock()
	bucket := s.groups[groupID]
	// Copy out under the lock so ordering can happen after release.
	candidates := make([]entmoot.Message, 0, len(bucket))
	for _, m := range bucket {
		if m.Timestamp < sinceMillis {
			continue
		}
		if untilMillis != 0 && m.Timestamp >= untilMillis {
			continue
		}
		candidates = append(candidates, m)
	}
	s.mu.RUnlock()

	return topoOrder(candidates)
}

// MerkleRoot implements MessageStore.MerkleRoot.
func (s *Memory) MerkleRoot(_ context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	s.mu.RLock()
	bucket := s.groups[groupID]
	all := make([]entmoot.Message, 0, len(bucket))
	for _, m := range bucket {
		all = append(all, m)
	}
	s.mu.RUnlock()

	return merkleRootOf(all)
}

// Close implements MessageStore.Close. No-op for Memory.
func (s *Memory) Close() error { return nil }

// topoOrder returns msgs materialized in canonical topological order.
//
// order.Topological returns ids; we rebuild the slice of messages in that
// order using a local index. Shared between Memory and JSONL to keep the
// ordering rule in one place.
func topoOrder(msgs []entmoot.Message) ([]entmoot.Message, error) {
	if len(msgs) == 0 {
		return []entmoot.Message{}, nil
	}
	ids, err := order.Topological(msgs)
	if err != nil {
		return nil, err
	}
	index := make(map[entmoot.MessageID]entmoot.Message, len(msgs))
	for _, m := range msgs {
		index[m.ID] = m
	}
	out := make([]entmoot.Message, 0, len(ids))
	for _, id := range ids {
		if m, ok := index[id]; ok {
			out = append(out, m)
		}
	}
	return out, nil
}

// merkleRootOf returns the Merkle root over msgs in topological order. An
// empty input yields the zero hash and a nil error.
func merkleRootOf(msgs []entmoot.Message) ([32]byte, error) {
	if len(msgs) == 0 {
		return [32]byte{}, nil
	}
	ids, err := order.Topological(msgs)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle.New(ids).Root(), nil
}
