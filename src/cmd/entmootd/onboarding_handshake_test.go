package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestSelectOnboardingHandshakePeersPrioritizesInviteAndFillsToTarget(t *testing.T) {
	invite := entmoot.Invite{
		Founder: entmoot.NodeInfo{PilotNodeID: 40},
		BootstrapPeers: []entmoot.BootstrapPeer{
			{NodeID: 30},
			{NodeID: 20},
			{NodeID: 20},
			{NodeID: 1},
		},
	}
	rosterMembers := []entmoot.NodeID{1, 10, 20, 30, 40, 50, 60, 70}
	trusted := []entmoot.NodeID{10}

	got := selectOnboardingHandshakePeers(1, invite, rosterMembers, trusted)
	want := []entmoot.NodeID{30, 20, 40, 50, 60}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectOnboardingHandshakePeers = %v, want %v", got, want)
	}
}

func TestSelectOnboardingHandshakePeersCapsBootstrapCandidates(t *testing.T) {
	invite := entmoot.Invite{Founder: entmoot.NodeInfo{PilotNodeID: 99}}
	rosterMembers := []entmoot.NodeID{1}
	for i := 2; i < 20; i++ {
		peer := entmoot.NodeID(i)
		invite.BootstrapPeers = append(invite.BootstrapPeers, entmoot.BootstrapPeer{NodeID: peer})
		rosterMembers = append(rosterMembers, peer)
	}

	got := selectOnboardingHandshakePeers(1, invite, rosterMembers, nil)
	if len(got) != onboardingHandshakeMaxPeers {
		t.Fatalf("len(selectOnboardingHandshakePeers) = %d, want %d", len(got), onboardingHandshakeMaxPeers)
	}
	for i, peer := range got {
		want := entmoot.NodeID(i + 2)
		if peer != want {
			t.Fatalf("peer[%d] = %d, want %d", i, peer, want)
		}
	}
}

func TestSelectOnboardingHandshakePeersSkipsWhenEnoughTrustedPeers(t *testing.T) {
	invite := entmoot.Invite{
		Founder: entmoot.NodeInfo{PilotNodeID: 40},
		BootstrapPeers: []entmoot.BootstrapPeer{
			{NodeID: 20},
		},
	}
	rosterMembers := []entmoot.NodeID{10, 20, 30, 40, 50, 60, 70}
	trusted := []entmoot.NodeID{10, 30, 50, 60, 70}

	got := selectOnboardingHandshakePeers(1, invite, rosterMembers, trusted)
	want := []entmoot.NodeID{20, 40}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectOnboardingHandshakePeers = %v, want %v", got, want)
	}
}

func TestSelectOnboardingHandshakePeersSkipsStaleInvitePeers(t *testing.T) {
	invite := entmoot.Invite{
		Founder: entmoot.NodeInfo{PilotNodeID: 90},
		BootstrapPeers: []entmoot.BootstrapPeer{
			{NodeID: 80},
			{NodeID: 20},
			{NodeID: 30},
		},
	}
	rosterMembers := []entmoot.NodeID{1, 20, 30, 40, 50, 60, 70}

	got := selectOnboardingHandshakePeers(1, invite, rosterMembers, nil)
	want := []entmoot.NodeID{20, 30, 40, 50, 60, 70}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectOnboardingHandshakePeers = %v, want %v", got, want)
	}
}

func TestSelectOnboardingHandshakePeersIgnoresUnrelatedTrustedPeersForCoverage(t *testing.T) {
	invite := entmoot.Invite{
		Founder: entmoot.NodeInfo{PilotNodeID: 20},
	}
	rosterMembers := []entmoot.NodeID{1, 20, 30, 40, 50, 60, 70}
	trusted := []entmoot.NodeID{500, 501, 502, 503, 504, 505}

	got := selectOnboardingHandshakePeers(1, invite, rosterMembers, trusted)
	want := []entmoot.NodeID{20, 30, 40, 50, 60, 70}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectOnboardingHandshakePeers = %v, want %v", got, want)
	}
}

func TestOnboardingHandshakeRoundsAttemptAllPeersBeforeRetries(t *testing.T) {
	withOnboardingHandshakeBackoffs(t, []time.Duration{time.Millisecond}, func() {
		r := testOnboardingHandshakeRuntime()
		h := newScriptedOnboardingHandshaker(map[entmoot.NodeID]bool{
			1: true,
			2: true,
		})
		peers := []entmoot.NodeID{1, 2, 3, 4}

		r.runOnboardingHandshakeRounds(context.Background(), h, entmoot.GroupID{}, peers, "test")

		records := h.Records()
		if len(records) < len(peers) {
			t.Fatalf("records len = %d, want at least %d: %+v", len(records), len(peers), records)
		}
		seen := make(map[entmoot.NodeID]struct{})
		for i, rec := range records[:len(peers)] {
			if rec.Attempt != 0 {
				t.Fatalf("records[%d] = %+v, want all initial peers attempted before retries; records=%+v", i, rec, records)
			}
			seen[rec.Peer] = struct{}{}
		}
		for _, peer := range peers {
			if _, ok := seen[peer]; !ok {
				t.Fatalf("initial round did not attempt peer %d before retries; records=%+v", peer, records)
			}
		}
	})
}

func TestOnboardingHandshakeRoundsStopAtRetryLimit(t *testing.T) {
	backoffs := []time.Duration{0, 0, 0}
	withOnboardingHandshakeBackoffs(t, backoffs, func() {
		r := testOnboardingHandshakeRuntime()
		h := newScriptedOnboardingHandshaker(map[entmoot.NodeID]bool{1: true})

		r.runOnboardingHandshakeRounds(context.Background(), h, entmoot.GroupID{}, []entmoot.NodeID{1}, "test")

		records := h.Records()
		want := 1 + len(backoffs)
		if len(records) != want {
			t.Fatalf("records len = %d, want %d: %+v", len(records), want, records)
		}
		for i, rec := range records {
			if rec.Peer != 1 || rec.Attempt != i {
				t.Fatalf("records[%d] = %+v, want peer 1 attempt %d", i, rec, i)
			}
		}
	})
}

func TestOnboardingHandshakeRoundsStopDuringRetryBackoffOnCancel(t *testing.T) {
	withOnboardingHandshakeBackoffs(t, []time.Duration{time.Hour}, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r := testOnboardingHandshakeRuntime()
		h := newScriptedOnboardingHandshaker(map[entmoot.NodeID]bool{1: true})
		done := make(chan struct{})

		go func() {
			defer close(done)
			r.runOnboardingHandshakeRounds(ctx, h, entmoot.GroupID{}, []entmoot.NodeID{1}, "test")
		}()

		select {
		case <-h.firstAttempt:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for initial handshake attempt")
		}
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("retry loop did not stop after context cancellation")
		}
		records := h.Records()
		if len(records) != 1 {
			t.Fatalf("records len = %d, want 1: %+v", len(records), records)
		}
	})
}

func withOnboardingHandshakeBackoffs(t *testing.T, backoffs []time.Duration, fn func()) {
	t.Helper()
	old := onboardingHandshakeBackoffs
	onboardingHandshakeBackoffs = backoffs
	t.Cleanup(func() {
		onboardingHandshakeBackoffs = old
	})
	fn()
}

func testOnboardingHandshakeRuntime() *groupRuntime {
	return &groupRuntime{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

type onboardingHandshakeRecord struct {
	Peer    entmoot.NodeID
	Attempt int
}

type scriptedOnboardingHandshaker struct {
	mu           sync.Mutex
	fail         map[entmoot.NodeID]bool
	counts       map[entmoot.NodeID]int
	records      []onboardingHandshakeRecord
	firstAttempt chan struct{}
	firstOnce    sync.Once
}

func newScriptedOnboardingHandshaker(fail map[entmoot.NodeID]bool) *scriptedOnboardingHandshaker {
	return &scriptedOnboardingHandshaker{
		fail:         fail,
		counts:       make(map[entmoot.NodeID]int),
		firstAttempt: make(chan struct{}),
	}
}

func (h *scriptedOnboardingHandshaker) Handshake(ctx context.Context, peer entmoot.NodeID, _ string) (map[string]interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	attempt := h.counts[peer]
	h.counts[peer] = attempt + 1
	h.records = append(h.records, onboardingHandshakeRecord{Peer: peer, Attempt: attempt})
	shouldFail := h.fail[peer]
	h.mu.Unlock()
	h.firstOnce.Do(func() {
		close(h.firstAttempt)
	})
	if shouldFail {
		return nil, errors.New("scripted handshake failure")
	}
	return map[string]interface{}{"ok": true}, nil
}

func (h *scriptedOnboardingHandshaker) Records() []onboardingHandshakeRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]onboardingHandshakeRecord(nil), h.records...)
}
