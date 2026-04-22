// Package store persists Entmoot messages grouped by GroupID and exposes them
// in deterministic topological order for Merkle-root computation and range
// queries.
//
// The package defines a small MessageStore interface and ships two
// implementations:
//
//   - Memory: a pure in-memory map-backed store. Fast, volatile. Useful for
//     tests, ephemeral peers, and anywhere the caller does its own persistence.
//   - JSONL: an append-only file-backed store using one messages.jsonl file
//     per group under <root>/groups/<group_id>/. Durable; designed for the
//     v0 canary binary.
//
// All methods are safe for concurrent use. v0 keeps retention flat — every
// stored message is kept until explicit deletion primitives (not in v0) are
// added; see ARCHITECTURE.md §8 for the future retention-tier story.
package store

import (
	"context"
	"errors"

	"entmoot/pkg/entmoot"
)

// ErrNotFound is returned by MessageStore.Get when the requested message id is
// absent from the store.
//
// Callers should check with errors.Is so wrapping implementations stay
// compatible. This sentinel is intentionally store-package-local; it is not
// promoted to the top-level entmoot.Err* namespace because message absence is
// a storage-layer concern, not a protocol-level one.
var ErrNotFound = errors.New("store: message not found")

// MessageStore persists messages grouped by GroupID. All methods are safe for
// concurrent use.
type MessageStore interface {
	// Put stores m. If a message with the same ID already exists, Put is a
	// no-op and returns nil (idempotent). Returns a non-nil error if m is
	// malformed (e.g., zero GroupID, zero ID).
	Put(ctx context.Context, m entmoot.Message) error

	// Get retrieves a message by id. Returns ErrNotFound if missing.
	Get(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error)

	// Has reports whether a message exists. Never returns ErrNotFound.
	Has(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error)

	// Range returns all messages in the group whose Timestamp falls in
	// [sinceMillis, untilMillis). An untilMillis of 0 means "no upper bound."
	// Results are ordered via pkg/entmoot/order.Topological; the returned slice
	// contains message values, not ids.
	Range(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error)

	// MerkleRoot returns the Merkle root (pkg/entmoot/merkle) over every
	// message in the group, ordered topologically. An empty group returns the
	// zero root and a nil error.
	MerkleRoot(ctx context.Context, groupID entmoot.GroupID) ([32]byte, error)

	// IterMessageIDsInIDRange returns every message ID in the given group
	// whose 32-byte identifier lies in the half-open range [loID, hiID),
	// sorted ascending by byte order. If hiID is the zero MessageID, the
	// upper bound is treated as "unbounded" (equivalent to all 0xFF).
	//
	// This is used by the reconcile package for range-based anti-entropy
	// (Entmoot v1.2.1); it is NOT the same ordering as Range() (which is
	// topological / timestamp-based). An empty or unknown group returns an
	// empty slice and a nil error.
	IterMessageIDsInIDRange(ctx context.Context, groupID entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error)

	// Close releases any resources held by the store. For Memory this is a
	// no-op; for JSONL it closes any open file handles.
	Close() error
}

// isZeroGroupID reports whether g is the zero GroupID.
func isZeroGroupID(g entmoot.GroupID) bool {
	var z entmoot.GroupID
	return g == z
}

// isZeroMessageID reports whether id is the zero MessageID.
func isZeroMessageID(id entmoot.MessageID) bool {
	var z entmoot.MessageID
	return id == z
}

// ErrInvalidMessage is returned by Put when the supplied message is missing
// required identifying fields (zero GroupID or zero ID).
var ErrInvalidMessage = errors.New("store: invalid message")
