// Package canary is the end-to-end integration test for Entmoot v0.
//
// This package contains two tests: TestCanaryInMemory (uses the in-memory
// gossip.Transport mock) and TestCanaryPilot (spins up real sandboxed Pilot
// daemons and drives gossip over them). Both exercise the same logical flow
// described in ARCHITECTURE.md §12:
//
//	"two peers join a group, exchange 3 messages, a third peer joins and
//	Merkle-verifies completeness."
//
// helpers.go is the transport-agnostic part: message/roster-entry
// construction, convergence polling, and a couple of shared sanity utilities.
// pilot_helpers.go is the subprocess-orchestration layer consumed only by the
// Pilot test.
package canary

import (
	"context"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// mkRosterAdd builds a group-bound founder-signed add entry against rlog's
// current head.
func mkRosterAdd(
	t *testing.T,
	rlog *roster.RosterLog,
	founder *keystore.Identity,
	founderID entmoot.NodeID,
	subject entmoot.NodeInfo,
	ts int64,
) entmoot.RosterEntry {
	t.Helper()
	entry, err := rlog.SignEntry(founder, "add", subject, nil, founderID, ts)
	if err != nil {
		t.Fatalf("sign roster entry: %v", err)
	}
	return entry
}

// mkMessage builds, signs, and ids a message authored by `author`. Parents is
// deliberately empty — the canary's 3 messages do not form a chain; the test
// only asserts that the Merkle root converges across nodes, not that causal
// links are respected.
func mkMessage(
	t *testing.T,
	author *keystore.Identity,
	authorInfo entmoot.NodeInfo,
	groupID entmoot.GroupID,
	rosterHead entmoot.RosterEntryID,
	topic string,
	content string,
	ts int64,
) entmoot.Message {
	t.Helper()
	msg := entmoot.Message{
		Version:   2,
		GroupID:   groupID,
		Author:    authorInfo,
		Timestamp: ts,
		Topics:    []string{topic},
		Content:   []byte(content),
	}
	msg.RosterHead = &rosterHead
	sigInput, err := canonical.MessageSigningBytes(msg)
	if err != nil {
		t.Fatalf("canonical message signing bytes: %v", err)
	}
	msg.Signature = author.Sign(sigInput)
	msg.ID = canonical.MessageID(msg)
	return msg
}

// waitForConvergence polls every `interval` until all stores report
// `expectedCount` messages AND all stores report byte-identical Merkle roots
// (which need not equal `expectedRoot` on entry — callers typically pass the
// zero root and instead read the final converged root off any of the stores
// once the function returns). Returns nil on convergence, a descriptive error
// on timeout. Any I/O error against a store aborts the poll immediately.
func waitForConvergence(
	t *testing.T,
	stores map[entmoot.NodeID]store.MessageStore,
	groupID entmoot.GroupID,
	expectedCount int,
	timeout time.Duration,
	interval time.Duration,
	logEach bool,
) ([32]byte, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	iter := 0
	for {
		iter++
		roots := make(map[entmoot.NodeID][32]byte, len(stores))
		counts := make(map[entmoot.NodeID]int, len(stores))
		allGood := true
		var firstRoot [32]byte
		first := true
		for id, s := range stores {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			msgs, err := s.Range(ctx, groupID, 0, 0)
			cancel()
			if err != nil {
				return [32]byte{}, err
			}
			counts[id] = len(msgs)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			root, err := s.MerkleRoot(ctx2, groupID)
			cancel2()
			if err != nil {
				return [32]byte{}, err
			}
			roots[id] = root
			if first {
				firstRoot = root
				first = false
			} else if root != firstRoot {
				allGood = false
			}
			if len(msgs) != expectedCount {
				allGood = false
			}
		}
		if logEach {
			t.Logf("canary: convergence iter=%d counts=%v", iter, counts)
		}
		if allGood {
			return firstRoot, nil
		}
		if time.Now().After(deadline) {
			return [32]byte{}, &convergenceTimeout{iter: iter, counts: counts, roots: roots, expected: expectedCount}
		}
		time.Sleep(interval)
	}
}

// convergenceTimeout is the error returned by waitForConvergence when the
// deadline expires. It surfaces the per-node counts and first-4-bytes of each
// root so the failing assertion in the test log is immediately debuggable.
type convergenceTimeout struct {
	iter     int
	counts   map[entmoot.NodeID]int
	roots    map[entmoot.NodeID][32]byte
	expected int
}

func (c *convergenceTimeout) Error() string {
	out := "canary: convergence timeout"
	out += " (expected count: " + itoa(c.expected) + ", iterations: " + itoa(c.iter) + "; per-node counts:"
	for id, n := range c.counts {
		out += " " + itoa(int(id)) + "=" + itoa(n)
	}
	out += ")"
	return out
}

// itoa is a tiny local integer→string to keep the error message construction
// free of fmt imports (avoids any circularity with our own logger fields).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
