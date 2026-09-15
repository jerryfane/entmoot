// Package store persists Entmoot messages grouped by GroupID and exposes them
// in deterministic topological order for Merkle-root computation and range
// queries.
//
// The package defines a small MessageStore interface with one implementation:
// SQLite, one messages.sqlite per group under <root>/groups/<group_id>/. It
// also maintains a search index and answers message-context queries, declared
// as MessageSearcher and MessageContexter and combined in SearchableStore.
// Callers that need those queries take the capability rather than plain
// MessageStore, so there is no in-process scan path to fall into.
//
// All methods are safe for concurrent use. Retention is driven by the pruning
// primitives below rather than by the store itself; see ARCHITECTURE.md §8.
package store

import (
	"context"
	"errors"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
)

// ErrNotFound is returned by MessageStore.Get when the requested message id is
// absent from the store.
//
// Callers should check with errors.Is so wrapping implementations stay
// compatible. This sentinel is intentionally store-package-local; it is not
// promoted to the top-level entmoot.Err* namespace because message absence is
// a storage-layer concern, not a protocol-level one.
var ErrNotFound = errors.New("store: message not found")

// ErrPruned is returned by persistent stores when an exact message ID was
// intentionally removed by retention and cannot be resurrected.
var ErrPruned = errors.New("store: message was pruned")

// TopicSummary is the storage-level aggregate for one message topic in a group.
type TopicSummary struct {
	Topic             string
	Count             int
	LatestMessageAtMS int64
}

// PageBoundary identifies the exclusive upper edge for older-history keyset
// pagination in the same recency order used by Latest.
type PageBoundary struct {
	TimestampMS    int64
	AuthorMemberID entmoot.MemberID
	MessageID      entmoot.MessageID
}

// MessageStore persists messages grouped by GroupID. All methods are safe for
// concurrent use.
type MessageStore interface {
	// Put stores m for expectedGroup. It rejects a mismatch before touching
	// storage. The returned bool reports whether this call inserted the
	// message; duplicates return false, nil.
	Put(ctx context.Context, expectedGroup entmoot.GroupID, m entmoot.Message) (inserted bool, err error)

	// Get retrieves a message by id. Returns ErrNotFound if missing.
	Get(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error)

	// Has reports whether a message exists. Never returns ErrNotFound.
	Has(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error)

	// Range returns all messages in the group whose Timestamp falls in
	// [sinceMillis, untilMillis). An untilMillis of 0 means "no upper bound."
	// Results are ordered via pkg/entmoot/order.Topological; the returned slice
	// contains message values, not ids.
	Range(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error)

	// Latest returns at most limit recent messages in groupID. The recency
	// window is selected by descending (Timestamp, Author MemberID, ID), then
	// returned in topological order within that bounded window. A limit <= 0
	// returns an empty slice.
	Latest(ctx context.Context, groupID entmoot.GroupID, limit int) ([]entmoot.Message, error)

	// LatestBefore returns at most limit messages older than boundary in the
	// same recency order used by Latest, then returns that bounded window in
	// topological order. A nil boundary is equivalent to Latest.
	LatestBefore(ctx context.Context, groupID entmoot.GroupID, limit int, boundary *PageBoundary) ([]entmoot.Message, error)

	// Topics returns topic aggregates for groupID ordered by message count
	// descending, latest message timestamp descending, then topic name ascending.
	// A limit <= 0 returns an empty slice.
	Topics(ctx context.Context, groupID entmoot.GroupID, limit int) ([]TopicSummary, error)

	// LatestByTopic returns at most limit recent messages in groupID that contain
	// topic exactly. The recency and output ordering match Latest.
	LatestByTopic(ctx context.Context, groupID entmoot.GroupID, topic string, limit int) ([]entmoot.Message, error)

	// LatestByTopicBefore is LatestBefore restricted to messages containing
	// topic exactly. A nil boundary is equivalent to LatestByTopic.
	LatestByTopicBefore(ctx context.Context, groupID entmoot.GroupID, topic string, limit int, boundary *PageBoundary) ([]entmoot.Message, error)

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

	// Close releases any resources held by the store: for SQLite it closes
	// the per-group database handles.
	Close() error
}

// SearchableStore is a MessageStore that also answers search and
// message-context queries from its own index. SQLite is the only
// implementation; callers that need those queries take this interface so a
// store without an index is a compile error rather than a silent full scan.
type SearchableStore interface {
	MessageStore
	MessageSearcher
	MessageContexter
}

// RangeCursor is the exclusive keyset boundary for a stable message-id page.
// All fields participate because timestamp alone is not unique.
type RangeCursor struct {
	TimestampMS    int64
	AuthorMemberID entmoot.MemberID
	ID             entmoot.MessageID
}

// MessageIDPage is one generation-bound page used by history synchronization.
type MessageIDPage struct {
	IDs             []entmoot.MessageID
	Generation      uint64
	Next            *RangeCursor
	HasMore         bool
	SnapshotChanged bool
	CoverageFloorMS int64
}

// PagedMessageIDStore provides bounded, restartable history enumeration.
type PagedMessageIDStore interface {
	MessageIDsPage(ctx context.Context, groupID entmoot.GroupID, sinceMillis int64, after *RangeCursor, expectedGeneration uint64, limit int) (MessageIDPage, error)
}

// WindowedPagedMessageIDStore constrains enumeration to an explicit
// [sinceMillis, untilMillis) coverage window.
type WindowedPagedMessageIDStore interface {
	MessageIDsPageWindow(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64, after *RangeCursor, expectedGeneration uint64, limit int) (MessageIDPage, error)
}

// TombstoneStore reports exact IDs intentionally removed by retention.
type TombstoneStore interface {
	HasTombstone(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error)
}

// CoverageStore reports the earliest timestamp for which a group claims
// retained history coverage. Zero means no retention floor is known.
type CoverageStore interface {
	CoverageFloor(ctx context.Context, groupID entmoot.GroupID) (int64, error)
}

// CoverageFloor returns the store's retention floor, or zero for stores that
// do not persist coverage metadata.
func CoverageFloor(ctx context.Context, st MessageStore, groupID entmoot.GroupID) (int64, error) {
	if covered, ok := st.(CoverageStore); ok {
		return covered.CoverageFloor(ctx, groupID)
	}
	return 0, nil
}

// HasTombstone reports whether retention intentionally removed this exact id,
// so a caller can tell "we never had it" from "we deliberately dropped it".
// Stores without tombstones answer false.
func HasTombstone(ctx context.Context, st MessageStore, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	if tombstoned, ok := st.(TombstoneStore); ok {
		return tombstoned.HasTombstone(ctx, groupID, id)
	}
	return false, nil
}

// MerkleRootSince returns the deterministic root for messages at or after the
// agreed retention floor. The full-history path retains the store's cached
// MerkleRoot implementation.
func MerkleRootSince(ctx context.Context, st MessageStore, groupID entmoot.GroupID, sinceMillis int64) ([32]byte, error) {
	if sinceMillis <= 0 {
		return st.MerkleRoot(ctx, groupID)
	}
	messages, err := st.Range(ctx, groupID, sinceMillis, 0)
	if err != nil {
		return [32]byte{}, err
	}
	ids := make([]entmoot.MessageID, len(messages))
	for i := range messages {
		ids[i] = messages[i].ID
	}
	return merkle.New(ids).Root(), nil
}

// RetentionPruner is implemented by stores that can remove old persisted
// messages for a group.
type RetentionPruner interface {
	PruneBefore(ctx context.Context, groupID entmoot.GroupID, beforeMillis int64) (int64, error)
}

// PruneBefore removes messages older than beforeMillis when st supports
// retention pruning. Stores that do not implement RetentionPruner are left
// unchanged so older test doubles keep current behavior.
func PruneBefore(ctx context.Context, st MessageStore, groupID entmoot.GroupID, beforeMillis int64) (int64, error) {
	return PruneBeforeExceptTopics(ctx, st, groupID, beforeMillis, nil)
}

// RetentionPrunerWithExemptTopics is implemented by stores that can keep
// control-plane messages while pruning old content history.
type RetentionPrunerWithExemptTopics interface {
	PruneBeforeExceptTopics(ctx context.Context, groupID entmoot.GroupID, beforeMillis int64, exemptTopics []string) (int64, error)
}

// PruneBeforeExceptTopics removes old messages except messages carrying one of
// exemptTopics. Stores without topic-aware pruning only prune when no exemption
// is requested, preserving the old interface contract for test doubles.
func PruneBeforeExceptTopics(ctx context.Context, st MessageStore, groupID entmoot.GroupID, beforeMillis int64, exemptTopics []string) (int64, error) {
	if pruner, ok := st.(RetentionPrunerWithExemptTopics); ok {
		return pruner.PruneBeforeExceptTopics(ctx, groupID, beforeMillis, exemptTopics)
	}
	pruner, ok := st.(RetentionPruner)
	if !ok || beforeMillis <= 0 || len(exemptTopics) > 0 {
		return 0, nil
	}
	return pruner.PruneBefore(ctx, groupID, beforeMillis)
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

func messageMemberID(m entmoot.Message) entmoot.MemberID {
	if m.Author.MemberID != nil {
		return *m.Author.MemberID
	}
	memberID, _ := entmoot.MemberIDFromPublicKey(m.Author.EntmootPubKey)
	return memberID
}
