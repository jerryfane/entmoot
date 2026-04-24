package gossip

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"

	_ "modernc.org/sqlite"
)

// memTransportAdStore is a small in-memory TransportAdStore backed by a
// map keyed on (group, author). LWW semantics (seq-first, then
// lexicographic signature tiebreak) mirror the SQLite production store
// so the transport-ad gossip paths behave identically against either.
// We keep this local to the test so the store package stays message-only.
type memTransportAdStore struct {
	mu       sync.Mutex
	ads      map[memAdKey]wire.TransportAd
	seqs     map[memAdKey]uint64
	putCalls int
}

type memAdKey struct {
	gid   entmoot.GroupID
	nodeID entmoot.NodeID
}

func newMemTransportAdStore() *memTransportAdStore {
	return &memTransportAdStore{
		ads:  make(map[memAdKey]wire.TransportAd),
		seqs: make(map[memAdKey]uint64),
	}
}

func (s *memTransportAdStore) PutTransportAd(_ context.Context, ad wire.TransportAd) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putCalls++
	k := memAdKey{gid: ad.GroupID, nodeID: ad.Author.PilotNodeID}
	cur, ok := s.ads[k]
	if ok {
		if cur.Seq > ad.Seq {
			return false, nil
		}
		if cur.Seq == ad.Seq {
			if string(ad.Signature) <= string(cur.Signature) {
				return false, nil
			}
		}
	}
	s.ads[k] = ad
	return true, nil
}

func (s *memTransportAdStore) GetTransportAd(_ context.Context, gid entmoot.GroupID, author entmoot.NodeID) (wire.TransportAd, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ad, ok := s.ads[memAdKey{gid: gid, nodeID: author}]
	return ad, ok, nil
}

func (s *memTransportAdStore) GetAllTransportAds(_ context.Context, gid entmoot.GroupID, now time.Time, includeExpired bool) ([]wire.TransportAd, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []wire.TransportAd
	nowMs := now.UnixMilli()
	for k, ad := range s.ads {
		if k.gid != gid {
			continue
		}
		if !includeExpired && ad.NotAfter < nowMs {
			continue
		}
		out = append(out, ad)
	}
	return out, nil
}

func (s *memTransportAdStore) BumpTransportAdSeq(_ context.Context, gid entmoot.GroupID, author entmoot.NodeID) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := memAdKey{gid: gid, nodeID: author}
	s.seqs[k]++
	return s.seqs[k], nil
}

func (s *memTransportAdStore) GCExpiredTransportAds(_ context.Context, now time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := now.UnixMilli()
	var n int64
	for k, ad := range s.ads {
		if ad.NotAfter < nowMs {
			delete(s.ads, k)
			n++
		}
	}
	return n, nil
}

// putCallCount returns the number of PutTransportAd calls since construction.
func (s *memTransportAdStore) putCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putCalls
}

// adFor returns the stored ad for (gid, author), if any.
func (s *memTransportAdStore) adFor(gid entmoot.GroupID, author entmoot.NodeID) (wire.TransportAd, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ad, ok := s.ads[memAdKey{gid: gid, nodeID: author}]
	return ad, ok
}

// installTransportAdStores equips every node in the fixture with its own
// memTransportAdStore, a ratelimit.Limiter backed by the fixture's fake
// clock, a LocalEndpoints closure returning the given endpoints, and an
// optional EndpointsChanged signal. Returns the per-node stores so tests
// can assert on what was stored. The fixture's Gossiper instances are
// still usable after this; it mutates cfg in place before Start.
func installTransportAdStores(t *testing.T, f *fixture, localEPs map[entmoot.NodeID][]entmoot.NodeEndpoint) map[entmoot.NodeID]*memTransportAdStore {
	t.Helper()
	stores := make(map[entmoot.NodeID]*memTransportAdStore, len(f.nodes))
	for id, ns := range f.nodes {
		tas := newMemTransportAdStore()
		stores[id] = tas
		lim := ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)
		ns.gossip.cfg.TransportAdStore = tas
		ns.gossip.cfg.RateLimiter = lim
		if eps, ok := localEPs[id]; ok {
			epsCopy := append([]entmoot.NodeEndpoint(nil), eps...)
			ns.gossip.cfg.LocalEndpoints = func() []entmoot.NodeEndpoint {
				return append([]entmoot.NodeEndpoint(nil), epsCopy...)
			}
		}
	}
	return stores
}

// buildTransportAd assembles and signs a TransportAd as though `author`
// had published it via publishTransportAd. Used by tests that need to
// deliver an ad directly via onTransportAd without spinning up the
// advertiser.
func (f *fixture) buildTransportAd(author entmoot.NodeID, seq uint64, endpoints []entmoot.NodeEndpoint, ts time.Time) wire.TransportAd {
	ns, ok := f.nodes[author]
	if !ok {
		f.t.Fatalf("buildTransportAd: unknown author %d", author)
	}
	ad := wire.TransportAd{
		GroupID: f.groupID,
		Author: entmoot.NodeInfo{
			PilotNodeID:   author,
			EntmootPubKey: ns.info.EntmootPubKey,
		},
		Seq:       seq,
		Endpoints: endpoints,
		IssuedAt:  ts.UnixMilli(),
		NotAfter:  ts.Add(transportAdTTL).UnixMilli(),
	}
	signing := ad
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		f.t.Fatalf("buildTransportAd: canonical encode: %v", err)
	}
	ad.Signature = ns.id.Sign(sigInput)
	return ad
}

// TestTransportAdPublishedOnStartup asserts the advertiser loop runs
// on Start and fanouts a TransportAd to every eager peer within a
// short window. Uses the in-memory transport fixture.
func TestTransportAdPublishedOnStartup(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
		20: {{Network: "tcp", Addr: "198.51.100.2:4001"}},
	}
	stores := installTransportAdStores(t, f, eps)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	waitUntil(t, 2*time.Second, "B receives A's startup TransportAd", func() bool {
		ad, ok := stores[20].adFor(f.groupID, 10)
		if !ok {
			return false
		}
		return len(ad.Endpoints) == 1 && ad.Endpoints[0].Addr == "198.51.100.1:4001"
	})
	waitUntil(t, 2*time.Second, "A receives B's startup TransportAd", func() bool {
		ad, ok := stores[10].adFor(f.groupID, 20)
		if !ok {
			return false
		}
		return len(ad.Endpoints) == 1 && ad.Endpoints[0].Addr == "198.51.100.2:4001"
	})
}

// TestTransportAdSpamBoundedByReplace publishes 1000 ads from A with
// ever-increasing Seq; B's store must retain exactly one row — the one
// with the highest Seq — at any point. This is the structural O(1)
// retained-state guarantee.
func TestTransportAdSpamBoundedByReplace(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil /* advertiser off */)

	// Loosen B's rate limiter so the 1000 deliveries aren't clipped.
	f.nodes[20].gossip.cfg.RateLimiter = ratelimit.New(ratelimit.Limits{
		MsgRate:    10_000,
		MsgBurst:   10_000,
		BytesRate:  10 << 20,
		BytesBurst: 10 << 20,
		TopicLimits: map[string]ratelimit.TopicLimit{
			"_pilot/transport/v1": {MsgRate: 10_000, MsgBurst: 10_000},
		},
	}, f.nodes[20].gossip.clk)

	ctx := context.Background()
	now := f.nodes[10].gossip.clk.Now()
	endpoints := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.1:4001"}}
	for seq := uint64(1); seq <= 1000; seq++ {
		ad := f.buildTransportAd(10, seq, endpoints, now)
		f.nodes[20].gossip.onTransportAd(ctx, 10, &ad)
	}

	// Exactly one row for (group, A) should exist, with the highest seq.
	ad, ok := stores[20].adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("B has no stored ad for A")
	}
	if ad.Seq != 1000 {
		t.Fatalf("B stored seq %d, want 1000", ad.Seq)
	}
	// The putCallCount is incremented per Put invocation (whether or not
	// it replaced), so all 1000 ads reached PutTransportAd. The retained
	// state, however, is one row — see adFor above.
	if got := stores[20].putCallCount(); got != 1000 {
		t.Fatalf("PutTransportAd call count = %d, want 1000", got)
	}
}

// TestTransportAdRejectCrossAuthor delivers a frame on a connection
// from A but with Author=B. The publisher-allowlist check at step 2
// must drop it before any signature work happens, so B's ad must NOT
// replace A's slot.
func TestTransportAdRejectCrossAuthor(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil)

	ctx := context.Background()
	now := f.nodes[10].gossip.clk.Now()
	endpoints := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.2:4001"}}
	// Ad claims Author=B (=20) but is delivered "from" A (=10).
	ad := f.buildTransportAd(20, 1, endpoints, now)
	f.nodes[30].gossip.onTransportAd(ctx, 10 /* sender */, &ad)

	// Nothing stored at C — the allowlist drop fires before any
	// roster/crypto work.
	if _, ok := stores[30].adFor(f.groupID, 20); ok {
		t.Fatalf("C stored an ad delivered with mismatched sender/author")
	}
	if got := stores[30].putCallCount(); got != 0 {
		t.Fatalf("PutTransportAd called %d times on cross-author frame; want 0", got)
	}
}

// TestTransportAdRejectOutOfOrderSeq ensures that once store has seq=5,
// an ad with seq=3 from the same author is silently dropped (LWW keeps
// the newer ad unchanged).
func TestTransportAdRejectOutOfOrderSeq(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil)

	ctx := context.Background()
	now := f.nodes[10].gossip.clk.Now()
	endpoints5 := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.5:4005"}}
	endpoints3 := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.3:4003"}}

	ad5 := f.buildTransportAd(10, 5, endpoints5, now)
	f.nodes[20].gossip.onTransportAd(ctx, 10, &ad5)
	if ad, ok := stores[20].adFor(f.groupID, 10); !ok || ad.Seq != 5 {
		t.Fatalf("after seq=5 put: stored=%v ok=%v", ad, ok)
	}

	ad3 := f.buildTransportAd(10, 3, endpoints3, now)
	f.nodes[20].gossip.onTransportAd(ctx, 10, &ad3)
	ad, ok := stores[20].adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("stored ad disappeared after seq=3 delivery")
	}
	if ad.Seq != 5 || ad.Endpoints[0].Addr != "198.51.100.5:4005" {
		t.Fatalf("LWW violated: stored=%+v (wanted seq=5 addr=198.51.100.5:4005)", ad)
	}
}

// TestTransportAdInstallCallsTransport verifies the post-accept hook:
// B's Transport.SetPeerEndpoints is called with A's NodeID and A's
// endpoints once A's ad is accepted. Uses memTransport's
// EndpointsFor side channel.
func TestTransportAdInstallCallsTransport(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	installTransportAdStores(t, f, eps)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Wait for B's transport to see A's endpoints.
	bTransport := f.transports[20].(*memTransport)
	waitUntil(t, 2*time.Second, "B transport records A's endpoints", func() bool {
		got := bTransport.EndpointsFor(10)
		return len(got) == 1 && got[0].Addr == "198.51.100.1:4001"
	})
}

// TestTransportAdInstallRoundTripsTurnEndpoint verifies the receive-side
// TURN path (v1.4.0): a published ad containing a "turn" network string
// is dispatched through onTransportAd to the Transport's SetPeerEndpoints
// call unmodified. The gossiper does not interpret the network string —
// it's Pilot's SetPeerEndpoints handler that routes "turn" entries to
// AddPeerTURNEndpoint. The mock transport just records every call, so
// this test confirms we don't accidentally filter "turn" out of the
// receive path.
func TestTransportAdInstallRoundTripsTurnEndpoint(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	// A advertises exactly one TURN endpoint (simulating a hide-IP peer).
	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "turn", Addr: "relay.example.test:3478"}},
	}
	installTransportAdStores(t, f, eps)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	bTransport := f.transports[20].(*memTransport)
	waitUntil(t, 2*time.Second, "B transport records A's TURN endpoint", func() bool {
		got := bTransport.EndpointsFor(10)
		return len(got) == 1 &&
			got[0].Network == "turn" &&
			got[0].Addr == "relay.example.test:3478"
	})
}

// TestTransportAdRateLimited exercises the per-(peer, topic) bucket.
// We deliver 20 valid ads very quickly from A; B's transport-ad bucket
// has a 10-token burst, so 10 pass and 10 reject. The stored seq must
// be exactly 10 — anything higher would mean a reject still got
// through.
func TestTransportAdRateLimited(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil)

	ctx := context.Background()
	now := f.nodes[10].gossip.clk.Now()
	endpoints := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.1:4001"}}

	// The default topic bucket is 10-burst; deliver 20 in one shot so
	// no refill happens between them. Over-quota deliveries must be
	// dropped after the PutTransportAd call site.
	for seq := uint64(1); seq <= 20; seq++ {
		ad := f.buildTransportAd(10, seq, endpoints, now)
		f.nodes[20].gossip.onTransportAd(ctx, 10, &ad)
	}

	ad, ok := stores[20].adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("no ad stored after 20 deliveries")
	}
	if ad.Seq != 10 {
		t.Fatalf("stored seq = %d, want 10 (only the burst made it past the rate limiter)", ad.Seq)
	}
	if got := stores[20].putCallCount(); got != 10 {
		t.Fatalf("PutTransportAd call count = %d, want 10", got)
	}
}

// TestTransportSnapshotReqReturnsCurrent exercises the bootstrap-time
// snapshot: after A has published, a TransportSnapshotReq issued to B
// returns exactly that one ad. Uses the real on-the-wire dispatch
// over the in-memory transport.
func TestTransportSnapshotReqReturnsCurrent(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	eps := map[entmoot.NodeID][]entmoot.NodeEndpoint{
		10: {{Network: "tcp", Addr: "198.51.100.1:4001"}},
	}
	stores := installTransportAdStores(t, f, eps)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Wait for A's ad to land at B.
	waitUntil(t, 2*time.Second, "B stores A's ad", func() bool {
		_, ok := stores[20].adFor(f.groupID, 10)
		return ok
	})

	// A dials B and issues a TransportSnapshotReq.
	conn, err := f.transports[10].Dial(ctx, 20)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.TransportSnapshotReq{GroupID: f.groupID}); err != nil {
		t.Fatalf("write req: %v", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	resp, ok := payload.(*wire.TransportSnapshotResp)
	if !ok {
		t.Fatalf("unexpected response type %T", payload)
	}
	if resp.GroupID != f.groupID {
		t.Fatalf("resp group = %s, want %s", resp.GroupID, f.groupID)
	}
	// B has A's ad (from the startup publish) and may also have its
	// own stored — but the "own stored" is part of the snapshot too.
	// Assert A=10 is present and reachable in the list.
	found := false
	for _, ad := range resp.Ads {
		if ad.Author.PilotNodeID == 10 {
			found = true
			if len(ad.Endpoints) != 1 || ad.Endpoints[0].Addr != "198.51.100.1:4001" {
				t.Fatalf("snapshot ad for A has wrong endpoints: %+v", ad.Endpoints)
			}
		}
	}
	if !found {
		t.Fatalf("snapshot response missing A's ad; got %d ads", len(resp.Ads))
	}
}

// TestTransportAdRejectExpired guards the "already expired" short-circuit
// at step 1. An ad whose NotAfter has elapsed relative to the receiver's
// clock must be dropped before any signature work.
func TestTransportAdRejectExpired(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil)

	// Build with a far-past IssuedAt so NotAfter lands in the past too.
	pastNow := time.UnixMilli(1)
	endpoints := []entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.1:4001"}}
	ad := f.buildTransportAd(10, 1, endpoints, pastNow)
	// Advance the receiver's clock well past the ad's window.
	fakeClk := f.nodes[20].gossip.clk.(*clock.Fake)
	fakeClk.Advance(transportAdTTL * 2)

	f.nodes[20].gossip.onTransportAd(context.Background(), 10, &ad)
	if _, ok := stores[20].adFor(f.groupID, 10); ok {
		t.Fatalf("expired ad was stored; want drop")
	}
}

// TestTransportAdUsesSQLiteStoreInProduction is a single-node smoke
// test that exercises the *store.SQLite implementation of the
// TransportAdStore interface through the publishTransportAd code
// path. Kept single-node so there's no concurrent-writer contention
// on the production SQLite lock; multi-node convergence is covered
// above against the memTransportAdStore fixture.
func TestTransportAdUsesSQLiteStoreInProduction(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	dir := filepath.Join(t.TempDir(), "sqlite-10")
	s, err := store.OpenSQLite(dir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ns := f.nodes[10]
	ns.gossip.cfg.TransportAdStore = s
	ns.gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)

	ctx := context.Background()
	if err := ns.gossip.publishTransportAd(ctx, []entmoot.NodeEndpoint{
		{Network: "tcp", Addr: "198.51.100.10:4001"},
	}); err != nil {
		t.Fatalf("publishTransportAd: %v", err)
	}

	ad, ok, err := s.GetTransportAd(ctx, f.groupID, 10)
	if err != nil || !ok {
		t.Fatalf("GetTransportAd: ok=%v err=%v", ok, err)
	}
	if len(ad.Endpoints) != 1 || ad.Endpoints[0].Addr != "198.51.100.10:4001" {
		t.Fatalf("stored ad has wrong endpoints: %+v", ad.Endpoints)
	}
	if ad.Seq != 1 {
		t.Fatalf("first seq = %d, want 1", ad.Seq)
	}

	// Second publish bumps seq.
	if err := ns.gossip.publishTransportAd(ctx, []entmoot.NodeEndpoint{
		{Network: "tcp", Addr: "198.51.100.10:4002"},
	}); err != nil {
		t.Fatalf("publishTransportAd #2: %v", err)
	}
	ad2, _, err := s.GetTransportAd(ctx, f.groupID, 10)
	if err != nil {
		t.Fatalf("GetTransportAd #2: %v", err)
	}
	if ad2.Seq != 2 {
		t.Fatalf("second seq = %d, want 2", ad2.Seq)
	}
}

// TestTransportAdAdvertiserOff verifies that a gossiper constructed
// without LocalEndpoints does NOT publish a startup ad — peers receive
// nothing, and no error is logged at Warn.
func TestTransportAdAdvertiserOff(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil /* no LocalEndpoints */)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Give the advertiser a full tick's worth of opportunity.
	time.Sleep(200 * time.Millisecond)

	if _, ok := stores[20].adFor(f.groupID, 10); ok {
		t.Fatalf("B received an ad from A despite LocalEndpoints being nil")
	}
	if got := stores[10].putCallCount(); got != 0 {
		t.Fatalf("A wrote %d ads despite LocalEndpoints being nil", got)
	}
}

// TestTransportAdRejectBadSignature tampers with an otherwise-valid ad
// and confirms the signature check at step 5 rejects it.
func TestTransportAdRejectBadSignature(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	stores := installTransportAdStores(t, f, nil)

	ctx := context.Background()
	now := f.nodes[10].gossip.clk.Now()
	ad := f.buildTransportAd(10, 1,
		[]entmoot.NodeEndpoint{{Network: "tcp", Addr: "198.51.100.1:4001"}}, now)
	// Flip a byte in the signature.
	if len(ad.Signature) > 0 {
		ad.Signature[0] ^= 0xFF
	}
	f.nodes[20].gossip.onTransportAd(ctx, 10, &ad)

	if _, ok := stores[20].adFor(f.groupID, 10); ok {
		t.Fatalf("bad-signature ad was stored; want drop")
	}
}

