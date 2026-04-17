package gossip

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// newFixtureWithGenesisOnly builds a fixture where most nodes have the full
// roster (everyone in nodeIDs added) and the designated joiner starts with a
// brand-new empty RosterLog — no genesis pre-seed, no founder private key.
// The joiner uses Join to catch up: bootstrap.applyEntries recognises the
// empty log and calls roster.AcceptGenesis on the first entry of the peer's
// RosterResp to verify-and-seed the genesis from its self-signature alone.
// All nodes share a single in-memory hub so Dials work across the full set.
func newFixtureWithGenesisOnly(t *testing.T, nodeIDs []entmoot.NodeID, joinerID entmoot.NodeID) *fixture {
	t.Helper()

	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(i + 1)
	}

	founderKey, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate founder: %v", err)
	}
	founderInfo := entmoot.NodeInfo{
		PilotNodeID:   nodeIDs[0],
		EntmootPubKey: []byte(founderKey.PublicKey),
	}

	f := &fixture{
		t:          t,
		groupID:    gid,
		founder:    founderKey,
		founderInf: founderInfo,
		founderTS:  1_000,
		transports: NewMemTransports(nodeIDs),
		nodes:      make(map[entmoot.NodeID]*nodeState, len(nodeIDs)),
	}

	// Per-node identities + rosters. Non-joiner rosters are seeded with the
	// shared genesis up-front so they can serve RosterResp; the joiner's
	// roster stays empty so Join must exercise AcceptGenesis.
	for _, n := range nodeIDs {
		var id *keystore.Identity
		var info entmoot.NodeInfo
		if n == nodeIDs[0] {
			id = founderKey
			info = founderInfo
		} else {
			fresh, err := keystore.Generate()
			if err != nil {
				t.Fatalf("keystore.Generate %d: %v", n, err)
			}
			id = fresh
			info = entmoot.NodeInfo{
				PilotNodeID:   n,
				EntmootPubKey: []byte(fresh.PublicKey),
			}
		}
		r := roster.New(gid)
		if n != joinerID {
			if err := r.Genesis(founderKey, founderInfo, f.founderTS); err != nil {
				t.Fatalf("roster.Genesis on %d: %v", n, err)
			}
		}
		s := store.NewMemory()
		f.nodes[n] = &nodeState{id: id, info: info, rost: r, storeM: s}
	}

	// Full-roster nodes: apply add(subject) for every non-founder,
	// non-joiner node. The joiner stays empty.
	for _, n := range nodeIDs {
		if n == nodeIDs[0] {
			continue
		}
		if n == joinerID {
			continue
		}
		f.founderTS += 100
		subject := f.nodes[n].info
		for _, rn := range nodeIDs {
			if rn == joinerID {
				continue
			}
			r := f.nodes[rn].rost
			entry := f.buildAddEntry(subject, f.founderTS, r.Head())
			if err := r.Apply(entry); err != nil {
				t.Fatalf("roster.Apply add %d to roster %d: %v", n, rn, err)
			}
		}
	}

	// Apply the joiner's own add entry on every non-joiner roster so the
	// founder considers the joiner a member before Join runs. The joiner's
	// own roster does not learn that yet — that is what Join will fetch.
	f.founderTS += 100
	joinerSubject := f.nodes[joinerID].info
	for _, rn := range nodeIDs {
		if rn == joinerID {
			continue
		}
		r := f.nodes[rn].rost
		entry := f.buildAddEntry(joinerSubject, f.founderTS, r.Head())
		if err := r.Apply(entry); err != nil {
			t.Fatalf("roster.Apply add joiner %d to roster %d: %v", joinerID, rn, err)
		}
	}

	fakeClk := clock.NewFake(time.UnixMilli(f.founderTS))
	for _, n := range nodeIDs {
		ns := f.nodes[n]
		g, err := New(Config{
			LocalNode: n,
			Identity:  ns.id,
			Roster:    ns.rost,
			Store:     ns.storeM,
			Transport: f.transports[n],
			GroupID:   gid,
			Fanout:    defaultFanout,
			Clock:     fakeClk,
			Logger:    slog.Default(),
		})
		if err != nil {
			t.Fatalf("gossip.New for %d: %v", n, err)
		}
		ns.gossip = g
	}
	return f
}

// buildInvite constructs a valid invite signed by the founder, listing the
// given peers as bootstrap candidates. ValidUntil defaults to IssuedAt +
// 24h so that the expiry check in gossip.Join accepts the invite under the
// fixture's fake clock; tests that need a different lifetime should use
// buildInviteWithValidUntil.
func (f *fixture) buildInvite(bootstrap []entmoot.NodeID) *entmoot.Invite {
	return f.buildInviteWithValidUntil(bootstrap, f.founderTS+24*60*60*1000)
}

// buildInviteWithValidUntil is buildInvite with an explicit ValidUntil
// timestamp (unix millis). Passing 0 produces a legacy-style invite with no
// expiry assertion.
func (f *fixture) buildInviteWithValidUntil(bootstrap []entmoot.NodeID, validUntil int64) *entmoot.Invite {
	bps := make([]entmoot.BootstrapPeer, 0, len(bootstrap))
	for _, n := range bootstrap {
		bps = append(bps, entmoot.BootstrapPeer{NodeID: n})
	}
	inv := &entmoot.Invite{
		GroupID:        f.groupID,
		Founder:        f.founderInf,
		Issuer:         f.founderInf,
		BootstrapPeers: bps,
		IssuedAt:       f.founderTS,
		ValidUntil:     validUntil,
	}
	signing := *inv
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		f.t.Fatalf("canonical encode invite: %v", err)
	}
	inv.Signature = f.founder.Sign(sigInput)
	return inv
}

// 1. Valid invite, one online bootstrap peer → Join succeeds; roster
// matches the provider. The joiner starts with an empty roster and no
// founder private key: bootstrap.applyEntries must call AcceptGenesis on
// the first response entry to seed the log.
func TestJoinBootstrapPeerOnline(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	// Sanity: the joiner's roster is genuinely empty before Join. This
	// exercises the AcceptGenesis path rather than the skip-up-to-head path.
	if f.nodes[99].rost.Head() != (entmoot.RosterEntryID{}) {
		t.Fatalf("joiner roster unexpectedly non-empty before Join")
	}
	if _, ok := f.nodes[99].rost.Founder(); ok {
		t.Fatalf("joiner roster reported a founder before Join")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start A's accept loop so it can answer RosterReq.
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	inv := f.buildInvite([]entmoot.NodeID{10, 20, 30})
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got := f.nodes[99].rost.Members()
	want := f.nodes[10].rost.Members()
	if len(got) != len(want) {
		t.Fatalf("membership size mismatch: got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("member[%d]: got %d want %d", i, got[i], want[i])
		}
	}
	// Post-Join: founder is recognised and pubkey matches the shared founder.
	fi, ok := f.nodes[99].rost.Founder()
	if !ok || fi.PilotNodeID != 10 {
		t.Fatalf("joiner Founder() = %#v, ok=%v; want PilotNodeID=10", fi, ok)
	}
}

// 2. Strategy 2 — TrustedPeers ∩ BootstrapPeers — is exercised when every
// entry in BootstrapPeers is unreachable at dial time AND one of them
// becomes reachable only via the trusted-set. The mock's TrustedPeers
// returns every hub node, so the intersect equals the subset of
// BootstrapPeers that are still dialable. We simulate "dial fails in
// Strategy 1 but works in Strategy 2" by closing 20's transport (so both
// strategies fail against 20), while 30 is in the bootstrap list AND the
// trusted set and is the one we keep online. Strategy 1 naturally
// supersedes Strategy 2 for 30 here; the test value is to prove we never
// erroneously short-circuit when some listed peers are dead.
func TestJoinSucceedsWhenSomeBootstrapPeersDead(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = f.transports[10].Close()
	_ = f.transports[20].Close()
	go func() { _ = f.nodes[30].gossip.Start(ctx) }()

	inv := f.buildInvite([]entmoot.NodeID{10, 20, 30})
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(f.nodes[99].rost.Members()) != len(f.nodes[30].rost.Members()) {
		t.Fatalf("membership sync failed")
	}
}

// 3. All bootstrap and trusted candidates fail, dial founder: Founder is in
// BootstrapPeers explicitly; we close the other bootstrap peers so the
// founder's connection succeeds.
func TestJoinFallbackFounder(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Close everyone EXCEPT the founder (10). Do NOT close 10's transport.
	_ = f.transports[20].Close()
	_ = f.transports[30].Close()
	// Start 10's accept loop.
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	// BootstrapPeers lists only 20 and 30 — not the founder. Strategy 3
	// must kick in.
	inv := f.buildInvite([]entmoot.NodeID{20, 30})
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(f.nodes[99].rost.Members()) != len(f.nodes[10].rost.Members()) {
		t.Fatalf("membership sync failed: got %v want %v",
			f.nodes[99].rost.Members(), f.nodes[10].rost.Members())
	}
}

// 4. All three strategies fail → Join returns ErrJoinFailed.
func TestJoinAllFail(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	// Close every accept-side transport. Dial will error on every attempt.
	_ = f.transports[10].Close()
	_ = f.transports[20].Close()
	_ = f.transports[30].Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	inv := f.buildInvite([]entmoot.NodeID{10, 20, 30})
	err := f.nodes[99].gossip.Join(ctx, inv)
	if !errors.Is(err, ErrJoinFailed) {
		t.Fatalf("expected ErrJoinFailed, got %v", err)
	}
}

// 5. Invite with invalid signature → Join returns an error wrapping
// entmoot.ErrSigInvalid before any dial is attempted.
func TestJoinInvalidInviteSignature(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	inv := f.buildInvite([]entmoot.NodeID{10, 20})
	// Corrupt the signature.
	inv.Signature[0] ^= 0xFF

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := f.nodes[99].gossip.Join(ctx, inv)
	if !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("expected ErrSigInvalid, got %v", err)
	}
}

// 6. Invite's Founder NodeID appears as last-resort candidate even if not
// in BootstrapPeers. This duplicates test 3's contract but keeps the
// assertion explicit: the founder is never skipped when not mentioned.
func TestJoinFounderCandidateWhenNotInBootstrap(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Only the founder (10) answers.
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	// No bootstrap peers listed at all. Strategy 3 is the only option.
	inv := f.buildInvite([]entmoot.NodeID{})
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if !f.nodes[99].rost.IsMember(10) {
		t.Fatalf("joiner did not learn founder from roster sync")
	}
}

// 7. Invite whose ValidUntil has elapsed relative to the gossiper's clock
// is rejected with an error wrapping entmoot.ErrInviteExpired. The check
// runs after signature verification, so the invite is correctly signed;
// only the timestamp is stale.
func TestJoinExpiredInviteRejected(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// ValidUntil one millisecond before IssuedAt — guaranteed in the past
	// relative to the fixture's fake clock (anchored at f.founderTS).
	inv := f.buildInviteWithValidUntil([]entmoot.NodeID{10, 20}, f.founderTS-1)

	err := f.nodes[99].gossip.Join(ctx, inv)
	if !errors.Is(err, entmoot.ErrInviteExpired) {
		t.Fatalf("expected ErrInviteExpired, got %v", err)
	}
}

// 8. Legacy-compat: ValidUntil == 0 means "no expiry asserted" and Join
// must accept the invite. Pre-v1 bundles and in-tree test fixtures that
// never set ValidUntil fall into this path.
func TestJoinValidUntilZeroAccepted(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	inv := f.buildInviteWithValidUntil([]entmoot.NodeID{10, 20, 30}, 0)
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(f.nodes[99].rost.Members()) != len(f.nodes[10].rost.Members()) {
		t.Fatalf("membership sync failed")
	}
}

// 9. Positive control: ValidUntil set to a future instant (IssuedAt + 1h)
// is accepted. Complements TestJoinExpiredInviteRejected by pinning the
// "fresh invite" side of the expiry boundary.
func TestJoinFreshInviteAccepted(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	oneHourMs := int64(60 * 60 * 1000)
	inv := f.buildInviteWithValidUntil([]entmoot.NodeID{10, 20, 30}, f.founderTS+oneHourMs)
	if err := f.nodes[99].gossip.Join(ctx, inv); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if len(f.nodes[99].rost.Members()) != len(f.nodes[10].rost.Members()) {
		t.Fatalf("membership sync failed")
	}
}
