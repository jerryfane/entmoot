package gossip

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

func TestPublishMemberProfileAdStoresLocalHostname(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ns := f.nodes[10]
	ns.gossip.cfg.MemberProfileStore = s
	ns.gossip.cfg.LocalHostname = func() (string, bool) { return "mars.local", true }

	ns.gossip.tryPublishMemberProfileAd(ctx, "test", true, nil)

	ad, ok, err := s.GetMemberProfileAd(ctx, f.groupID, 10, ns.gossip.clk.Now())
	if err != nil {
		t.Fatalf("GetMemberProfileAd: %v", err)
	}
	if !ok {
		t.Fatal("GetMemberProfileAd ok=false after publish")
	}
	if ad.Hostname != "mars.local" {
		t.Fatalf("Hostname = %q, want mars.local", ad.Hostname)
	}
	if ad.Seq != 1 {
		t.Fatalf("Seq = %d, want 1", ad.Seq)
	}
}

func TestPublishMemberProfileAdSkipsHostnameLookupFailure(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	hostname := "mars.local"
	lookupOK := true
	ns := f.nodes[10]
	ns.gossip.cfg.MemberProfileStore = s
	ns.gossip.cfg.LocalHostname = func() (string, bool) { return hostname, lookupOK }
	state := memberProfilePublishState{}

	ns.gossip.tryPublishMemberProfileAd(ctx, "startup", true, &state)
	hostname = ""
	lookupOK = false
	ns.gossip.tryPublishMemberProfileAd(ctx, "refresh", true, &state)

	ad, ok, err := s.GetMemberProfileAd(ctx, f.groupID, 10, ns.gossip.clk.Now())
	if err != nil {
		t.Fatalf("GetMemberProfileAd: %v", err)
	}
	if !ok {
		t.Fatal("GetMemberProfileAd ok=false after publish")
	}
	if ad.Hostname != "mars.local" || ad.Seq != 1 {
		t.Fatalf("profile = hostname %q seq %d, want mars.local seq 1", ad.Hostname, ad.Seq)
	}
	if state.lastPublishedHostname != "mars.local" || !state.republishPending {
		t.Fatalf("state = %+v, want mars.local with republish pending", state)
	}
}

func TestPublishMemberProfileAdRepublishesAfterForcedLookupFailure(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	hostname := "mars.local"
	lookupOK := true
	ns := f.nodes[10]
	ns.gossip.cfg.MemberProfileStore = s
	ns.gossip.cfg.LocalHostname = func() (string, bool) { return hostname, lookupOK }
	state := memberProfilePublishState{}

	ns.gossip.tryPublishMemberProfileAd(ctx, "startup", true, &state)
	lookupOK = false
	ns.gossip.tryPublishMemberProfileAd(ctx, "refresh", true, &state)
	lookupOK = true
	ns.gossip.tryPublishMemberProfileAd(ctx, "hostname_poll", false, &state)

	ad, ok, err := s.GetMemberProfileAd(ctx, f.groupID, 10, ns.gossip.clk.Now())
	if err != nil {
		t.Fatalf("GetMemberProfileAd: %v", err)
	}
	if !ok {
		t.Fatal("GetMemberProfileAd ok=false after publish")
	}
	if ad.Hostname != "mars.local" || ad.Seq != 2 {
		t.Fatalf("profile = hostname %q seq %d, want mars.local seq 2", ad.Hostname, ad.Seq)
	}
	if state.republishPending {
		t.Fatalf("republishPending = true after successful recovery publish")
	}
}

func TestPublishMemberProfileAdRepublishesAfterForcedPublishFailure(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ns := f.nodes[10]
	ns.gossip.cfg.MemberProfileStore = s
	ns.gossip.cfg.LocalHostname = func() (string, bool) { return "mars.local", true }
	state := memberProfilePublishState{}

	ns.gossip.tryPublishMemberProfileAd(ctx, "startup", true, &state)
	ns.gossip.cfg.MemberProfileStore = nil
	ns.gossip.tryPublishMemberProfileAd(ctx, "refresh", true, &state)
	ns.gossip.cfg.MemberProfileStore = s
	ns.gossip.tryPublishMemberProfileAd(ctx, "hostname_poll", false, &state)

	ad, ok, err := s.GetMemberProfileAd(ctx, f.groupID, 10, ns.gossip.clk.Now())
	if err != nil {
		t.Fatalf("GetMemberProfileAd: %v", err)
	}
	if !ok {
		t.Fatal("GetMemberProfileAd ok=false after publish")
	}
	if ad.Hostname != "mars.local" || ad.Seq != 2 {
		t.Fatalf("profile = hostname %q seq %d, want mars.local seq 2", ad.Hostname, ad.Seq)
	}
	if state.republishPending {
		t.Fatalf("republishPending = true after successful recovery publish")
	}
}

func TestPublishMemberProfileAdFanoutStoresOnPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	f.nodes[20].gossip.cfg.MemberProfileStore = storeB
	f.startAll(ctx)

	if err := f.nodes[10].gossip.publishMemberProfileAd(ctx, "mars.local"); err != nil {
		t.Fatalf("publishMemberProfileAd: %v", err)
	}
	waitUntil(t, 2*time.Second, "B stores A's member profile", func() bool {
		ad, ok, err := storeB.GetMemberProfileAd(ctx, f.groupID, 10, f.nodes[20].gossip.clk.Now())
		return err == nil && ok && ad.Hostname == "mars.local"
	})
}

func TestMemberProfileAdRetryOnFanoutFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()
	wrappers := wrapWithFailingTransport(t, f)

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	f.nodes[20].gossip.cfg.MemberProfileStore = storeB

	wrappers[10].setFail(20, true)
	if err := f.nodes[10].gossip.publishMemberProfileAd(ctx, "mars.local"); err != nil {
		t.Fatalf("publishMemberProfileAd: %v", err)
	}
	if _, ok := memberProfileRetryState(f.nodes[10].gossip, 20, 10); !ok {
		t.Fatal("missing member_profile_ad retry after failed fanout")
	}

	go func() { _ = f.nodes[20].gossip.Start(ctx) }()
	wrappers[10].setFail(20, false)
	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)
	advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, 2*time.Second)

	waitUntil(t, 2*time.Second, "B stores retried A member profile", func() bool {
		ad, ok, err := storeB.GetMemberProfileAd(ctx, f.groupID, 10, f.nodes[20].gossip.clk.Now())
		return err == nil && ok && ad.Hostname == "mars.local"
	})
	if _, ok := memberProfileRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatal("member_profile_ad retry still pending after success")
	}
}

func TestMemberProfileSnapshotRepairAfterMissedFanout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	f.nodes[20].gossip.cfg.MemberProfileStore = storeB

	ad := f.buildMemberProfileAd(10, 1, "mars.local", f.nodes[10].gossip.clk.Now())
	if _, err := storeA.PutMemberProfileAd(ctx, ad); err != nil {
		t.Fatalf("PutMemberProfileAd: %v", err)
	}

	go func() { _ = f.nodes[10].gossip.Start(ctx) }()
	f.nodes[20].gossip.scheduleMemberProfileSnapshotPull(ctx, 10, "test")

	waitUntil(t, 2*time.Second, "B repairs A member profile from snapshot", func() bool {
		got, ok, err := storeB.GetMemberProfileAd(ctx, f.groupID, 10, f.nodes[20].gossip.clk.Now())
		return err == nil && ok && got.Hostname == "mars.local"
	})
}

func TestMemberProfileDoesNotInstallTunnelUpCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	tr := newTunnelCallbackCaptureTransport()
	f.nodes[10].gossip.cfg.Transport = tr
	f.nodes[10].gossip.cfg.MemberProfileStore = s
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	time.Sleep(100 * time.Millisecond)
	if cb := tr.callback(); cb != nil {
		t.Fatal("gossiper installed a tunnel-up callback; reactive reconcile must be accept/write owned")
	}

	f.nodes[10].gossip.pendMu.Lock()
	pulls := len(f.nodes[10].gossip.profilePulls)
	last := len(f.nodes[10].gossip.profilePullLast)
	f.nodes[10].gossip.pendMu.Unlock()
	if pulls != 0 || last != 0 {
		t.Fatalf("unexpected profile pulls: in_flight=%d last=%d", pulls, last)
	}
}

func TestMemberProfileSnapshotPullCooldownSuppressesRepeats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	g := f.nodes[10].gossip
	g.cfg.MemberProfileStore = s

	if !g.scheduleMemberProfileSnapshotPull(ctx, 20, "test") {
		t.Fatal("first snapshot pull was not scheduled")
	}
	waitUntil(t, time.Second, "first snapshot pull leaves in-flight set", func() bool {
		g.pendMu.Lock()
		defer g.pendMu.Unlock()
		_, ok := g.profilePulls[20]
		return !ok
	})
	if g.scheduleMemberProfileSnapshotPull(context.Background(), 20, "test") {
		t.Fatal("second snapshot pull scheduled inside cooldown")
	}
}

func TestMemberProfileSnapshotSweepIsBatchedAndConcurrencyLimited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	nodeIDs := []entmoot.NodeID{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	f := newFixture(t, nodeIDs)
	defer f.closeTransports()

	s, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	g := f.nodes[10].gossip
	g.cfg.MemberProfileStore = s

	if scheduled := g.scheduleMemberProfileSnapshotSweep(ctx, "test"); scheduled != memberProfileSnapshotSweepBatch {
		t.Fatalf("scheduled = %d, want %d", scheduled, memberProfileSnapshotSweepBatch)
	}
	waitUntil(t, time.Second, "snapshot pulls enter slots", func() bool {
		return len(g.profilePullSlots) == memberProfileSnapshotMaxConcurrent
	})
	if got := len(g.profilePullSlots); got > memberProfileSnapshotMaxConcurrent {
		t.Fatalf("active profile pulls = %d, cap = %d", got, memberProfileSnapshotMaxConcurrent)
	}
}

func TestMemberProfileAdRetrySupersededByNewerProfile(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA

	oldAd := f.buildMemberProfileAd(10, 1, "old-mars.local", f.nodes[10].gossip.clk.Now())
	newAd := f.buildMemberProfileAd(10, 2, "mars.local", f.nodes[10].gossip.clk.Now())
	if _, err := storeA.PutMemberProfileAd(ctx, newAd); err != nil {
		t.Fatalf("PutMemberProfileAd: %v", err)
	}
	f.nodes[10].gossip.enqueueMemberProfileRetry(20, &oldAd)

	fakeClk := f.nodes[10].gossip.clk.(*clock.Fake)
	advanceAndDrain(ctx, f.nodes[10].gossip, fakeClk, 2*time.Second)
	if _, ok := memberProfileRetryState(f.nodes[10].gossip, 20, 10); ok {
		t.Fatal("superseded member_profile_ad retry still pending")
	}
}

func TestMemberProfileAdRetryRefreshesPendingSlot(t *testing.T) {
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	oldAd := f.buildMemberProfileAd(10, 1, "old-mars.local", f.nodes[10].gossip.clk.Now())
	newAd := f.buildMemberProfileAd(10, 2, "mars.local", f.nodes[10].gossip.clk.Now())
	f.nodes[10].gossip.enqueueMemberProfileRetry(20, &oldAd)
	f.nodes[10].gossip.enqueueMemberProfileRetry(20, &newAd)

	state, ok := memberProfileRetryState(f.nodes[10].gossip, 20, 10)
	if !ok {
		t.Fatal("missing member_profile_ad retry")
	}
	if state.seq != 2 || state.profileAd == nil || state.profileAd.Hostname != "mars.local" {
		t.Fatalf("retry state = %+v, want seq=2 hostname=mars.local", state)
	}
}

func TestMemberProfileSnapshotBypassesPerEntryTopicLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nodeIDs := []entmoot.NodeID{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120}
	f := newFixture(t, nodeIDs)
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	f.nodes[20].gossip.cfg.MemberProfileStore = storeB
	f.nodes[20].gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), f.nodes[20].gossip.clk)

	want := 0
	now := f.nodes[10].gossip.clk.Now()
	for _, nodeID := range nodeIDs {
		if nodeID == 20 {
			continue
		}
		ad := f.buildMemberProfileAd(nodeID, 1, "member.local", now)
		if _, err := storeA.PutMemberProfileAd(ctx, ad); err != nil {
			t.Fatalf("PutMemberProfileAd %d: %v", nodeID, err)
		}
		want++
	}

	go func() { _ = f.nodes[10].gossip.Start(ctx) }()
	if _, err := f.nodes[20].gossip.pullMemberProfileSnapshot(ctx, 10); err != nil {
		t.Fatalf("pullMemberProfileSnapshot: %v", err)
	}

	got := 0
	for _, nodeID := range nodeIDs {
		if nodeID == 20 {
			continue
		}
		ad, ok, err := storeB.GetMemberProfileAd(ctx, f.groupID, nodeID, f.nodes[20].gossip.clk.Now())
		if err != nil {
			t.Fatalf("GetMemberProfileAd %d: %v", nodeID, err)
		}
		if ok && ad.Hostname == "member.local" {
			got++
		}
	}
	if got != want {
		t.Fatalf("stored profiles = %d, want %d", got, want)
	}
}

func TestMemberProfileSnapshotReqReturnsCurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	ad := f.buildMemberProfileAd(10, 1, "mars.local", f.nodes[10].gossip.clk.Now())
	if _, err := storeA.PutMemberProfileAd(ctx, ad); err != nil {
		t.Fatalf("PutMemberProfileAd: %v", err)
	}

	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	conn, err := f.transports[20].Dial(ctx, 10)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.MemberProfileSnapshotReq{GroupID: f.groupID}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	resp, ok := payload.(*wire.MemberProfileSnapshotResp)
	if !ok {
		t.Fatalf("unexpected response type %T", payload)
	}
	if resp.GroupID != f.groupID {
		t.Fatalf("resp group = %s, want %s", resp.GroupID, f.groupID)
	}
	if len(resp.Profiles) != 1 || resp.Profiles[0].Hostname != "mars.local" {
		t.Fatalf("Profiles = %+v, want mars.local", resp.Profiles)
	}
}

func TestMemberProfileSnapshotReqSkipsReplacedIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA

	oldID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate old identity: %v", err)
	}
	staleInfo := entmoot.NodeInfo{
		PilotNodeID:   10,
		EntmootPubKey: append([]byte(nil), oldID.PublicKey...),
	}
	ad := buildMemberProfileAdForIdentity(t, f.groupID, oldID, staleInfo, 1, "old-mars.local", f.nodes[10].gossip.clk.Now())
	if _, err := storeA.PutMemberProfileAd(ctx, ad); err != nil {
		t.Fatalf("PutMemberProfileAd: %v", err)
	}

	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	conn, err := f.transports[20].Dial(ctx, 10)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.MemberProfileSnapshotReq{GroupID: f.groupID}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	resp, ok := payload.(*wire.MemberProfileSnapshotResp)
	if !ok {
		t.Fatalf("unexpected response type %T", payload)
	}
	if len(resp.Profiles) != 0 {
		t.Fatalf("Profiles = %+v, want stale replaced identity omitted", resp.Profiles)
	}
}

func TestJoinPullsMemberProfileSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	storeA, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeC, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite C: %v", err)
	}
	t.Cleanup(func() { _ = storeC.Close() })
	f.nodes[10].gossip.cfg.MemberProfileStore = storeA
	f.nodes[99].gossip.cfg.MemberProfileStore = storeC

	now := f.nodes[20].gossip.clk.Now()
	ad := f.buildMemberProfileAd(20, 1, "phobos.local", now)
	if _, err := storeA.PutMemberProfileAd(ctx, ad); err != nil {
		t.Fatalf("A PutMemberProfileAd for B: %v", err)
	}
	f.nodes[99].gossip.recordDialFailure(20)
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	inv := f.buildInvite([]entmoot.NodeID{10})
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got, ok, err := storeC.GetMemberProfileAd(ctx, f.groupID, 20, f.nodes[99].gossip.clk.Now())
	if err != nil {
		t.Fatalf("GetMemberProfileAd: %v", err)
	}
	if !ok || got.Hostname != "phobos.local" {
		t.Fatalf("C profile for B = %+v ok=%v, want phobos.local", got, ok)
	}
}

func (f *fixture) buildMemberProfileAd(author entmoot.NodeID, seq uint64, hostname string, ts time.Time) wire.MemberProfileAd {
	ns, ok := f.nodes[author]
	if !ok {
		f.t.Fatalf("buildMemberProfileAd: unknown author %d", author)
	}
	return buildMemberProfileAdForIdentity(f.t, f.groupID, ns.id, ns.info, seq, hostname, ts)
}

func buildMemberProfileAdForIdentity(t *testing.T, groupID entmoot.GroupID, id *keystore.Identity, info entmoot.NodeInfo, seq uint64, hostname string, ts time.Time) wire.MemberProfileAd {
	t.Helper()
	ad := wire.MemberProfileAd{
		GroupID:  groupID,
		Author:   info,
		Seq:      seq,
		Hostname: hostname,
		IssuedAt: ts.UnixMilli(),
		NotAfter: ts.Add(memberProfileTTL).UnixMilli(),
	}
	signing := ad
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		t.Fatalf("buildMemberProfileAd: canonical encode: %v", err)
	}
	ad.Signature = id.Sign(sigInput)
	return ad
}

type tunnelCallbackCaptureTransport struct {
	mu   sync.Mutex
	cb   func(entmoot.NodeID)
	once sync.Once
	ch   chan struct{}
}

func newTunnelCallbackCaptureTransport() *tunnelCallbackCaptureTransport {
	return &tunnelCallbackCaptureTransport{ch: make(chan struct{})}
}

func (t *tunnelCallbackCaptureTransport) Dial(context.Context, entmoot.NodeID) (net.Conn, error) {
	return nil, net.ErrClosed
}

func (t *tunnelCallbackCaptureTransport) Accept(ctx context.Context) (net.Conn, entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

func (t *tunnelCallbackCaptureTransport) TrustedPeers(context.Context) ([]entmoot.NodeID, error) {
	return nil, nil
}

func (t *tunnelCallbackCaptureTransport) SetPeerEndpoints(context.Context, entmoot.NodeID, []entmoot.NodeEndpoint) error {
	return nil
}

func (t *tunnelCallbackCaptureTransport) SetOnTunnelUp(cb func(entmoot.NodeID)) {
	t.mu.Lock()
	t.cb = cb
	t.mu.Unlock()
	t.once.Do(func() { close(t.ch) })
}

func (t *tunnelCallbackCaptureTransport) Close() error {
	return nil
}

func (t *tunnelCallbackCaptureTransport) callback() func(entmoot.NodeID) {
	select {
	case <-t.ch:
	default:
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.cb
}

func memberProfileRetryState(g *Gossiper, peer, author entmoot.NodeID) (*retryState, bool) {
	g.pendMu.Lock()
	defer g.pendMu.Unlock()
	s, ok := g.pending[retryKey{peer: peer, author: author, op: opMemberProfileAd}]
	return s, ok
}
