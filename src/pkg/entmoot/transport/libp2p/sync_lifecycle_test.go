package libp2ptransport

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

func TestTerminalSnapshotsDoNotStarveMultiGroupHistory(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	// Repeated terminal roster polls across two groups previously occupied
	// every slot before the same member could begin history recovery.
	for i := 0; i < maxPeerSnapshots; i++ {
		page, err := f.rosterPage(f.groups[i%len(f.groups)], nil, 100)
		if err != nil || !page.Complete || len(page.Entries) != 2 {
			t.Fatalf("roster poll %d: page=%+v err=%v", i, page, err)
		}
	}
	for i := 0; i <= maxPeerSnapshots; i++ {
		groupID := f.groups[i%len(f.groups)]
		page, err := f.historyPage(groupID, nil, 100)
		if err != nil || page.HasMore || !slices.Equal(page.IDs, f.ids[groupID]) {
			t.Fatalf("history recovery %d: page=%+v err=%v", i, page, err)
		}
		reused, err := f.historyPage(groupID, &page, 100)
		if err == nil || reused.Error != SyncSnapshotExpired {
			t.Fatalf("completed history token remained usable: page=%+v err=%v", reused, err)
		}
	}
}

func TestActiveSnapshotsRemainPinnedBoundedAndExpire(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	var rosters [2]RosterSyncResponse
	var histories [2]HistorySyncResponse
	for i, groupID := range f.groups {
		var err error
		rosters[i], err = f.rosterPage(groupID, nil, 1)
		if err != nil || rosters[i].Complete || len(rosters[i].Entries) != 1 {
			t.Fatalf("start roster %d: page=%+v err=%v", i, rosters[i], err)
		}
		histories[i], err = f.historyPage(groupID, nil, 1)
		if err != nil || !histories[i].HasMore || !slices.Equal(histories[i].IDs, f.ids[groupID][:1]) {
			t.Fatalf("start history %d: page=%+v err=%v", i, histories[i], err)
		}
	}
	fullRoster, err := f.rosterPage(f.groups[0], nil, 1)
	if err == nil || fullRoster.Error != SyncResourceExhausted {
		t.Fatalf("active roster quota: page=%+v err=%v", fullRoster, err)
	}
	fullHistory, err := f.historyPage(f.groups[1], nil, 1)
	if err == nil || fullHistory.Error != SyncResourceExhausted {
		t.Fatalf("active history quota: page=%+v err=%v", fullHistory, err)
	}

	// An unrelated roster update must not change the in-flight pinned head.
	extra := mustIdentity(t)
	entry, err := f.logs[f.groups[0]].SignEntry(f.founder, "add", mustNodeInfo(t, extra.PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.logs[f.groups[0]].Apply(entry); err != nil {
		t.Fatal(err)
	}
	lastRoster, err := f.rosterPage(f.groups[0], &rosters[0], 100)
	if err != nil || !lastRoster.Complete || len(lastRoster.Entries) != 1 ||
		lastRoster.SnapshotToken != rosters[0].SnapshotToken || lastRoster.CommittedHead != rosters[0].CommittedHead {
		t.Fatalf("pinned roster completion: page=%+v err=%v", lastRoster, err)
	}
	chain := append(append([]entmoot.RosterEntry(nil), rosters[0].Entries...), lastRoster.Entries...)
	if _, err := ValidateRosterChain(f.groups[0], mustNodeInfo(t, f.founder.PublicKey), rosters[0].CommittedHead, chain); err != nil {
		t.Fatalf("completed pinned roster: %v", err)
	}
	retired, err := f.rosterPage(f.groups[0], &rosters[0], 100)
	if err == nil || retired.Error != SyncSnapshotExpired {
		t.Fatalf("completed roster token remained usable: page=%+v err=%v", retired, err)
	}

	// Completion frees exactly one slot without evicting the other sessions.
	replacement, err := f.historyPage(f.groups[1], nil, 1)
	if err != nil || !replacement.HasMore {
		t.Fatalf("reusing completed roster slot: page=%+v err=%v", replacement, err)
	}
	fullHistory, err = f.historyPage(f.groups[0], nil, 1)
	if err == nil || fullHistory.Error != SyncResourceExhausted {
		t.Fatalf("replacement exceeded active quota: page=%+v err=%v", fullHistory, err)
	}
	for i, groupID := range f.groups {
		last, err := f.historyPage(groupID, &histories[i], 100)
		if err != nil || last.HasMore || last.Generation != histories[i].Generation ||
			last.SnapshotToken != histories[i].SnapshotToken || !slices.Equal(last.IDs, f.ids[groupID][1:]) {
			t.Fatalf("pinned history completion %d: page=%+v err=%v", i, last, err)
		}
		paused, err := f.rosterPage(f.groups[0], nil, 1)
		if err != nil || paused.Complete {
			t.Fatalf("reusing completed history slot %d: page=%+v err=%v", i, paused, err)
		}
	}

	// Rejected cross-group tokens do not evict the legitimate active session.
	wrongGroup, err := f.rosterPage(f.groups[0], &rosters[1], 1)
	if err == nil || wrongGroup.Error != SyncSnapshotExpired {
		t.Fatalf("cross-group token: page=%+v err=%v", wrongGroup, err)
	}
	f.elapsed.Store(int64(syncSnapshotLifetime - time.Nanosecond))
	fullHistory, err = f.historyPage(f.groups[0], nil, 1)
	if err == nil || fullHistory.Error != SyncResourceExhausted {
		t.Fatalf("abandoned slots reclaimed before expiry: page=%+v err=%v", fullHistory, err)
	}
	retry := rosters[1]
	retry.NextSequence = 0
	beforeExpiry, err := f.rosterPage(f.groups[1], &retry, 1)
	if err != nil || beforeExpiry.Complete || beforeExpiry.CommittedHead != rosters[1].CommittedHead ||
		len(beforeExpiry.Entries) != 1 || beforeExpiry.Entries[0].ID != rosters[1].Entries[0].ID {
		t.Fatalf("live page before expiry: page=%+v err=%v", beforeExpiry, err)
	}

	// A continuation does not extend the original lifetime. No sleeps or
	// inspection of the server's private snapshot map are needed.
	f.elapsed.Store(int64(syncSnapshotLifetime))
	expiredRoster, err := f.rosterPage(f.groups[1], &rosters[1], 100)
	if err == nil || expiredRoster.Error != SyncSnapshotExpired {
		t.Fatalf("expired roster token: page=%+v err=%v", expiredRoster, err)
	}
	expiredHistory, err := f.historyPage(f.groups[1], &replacement, 100)
	if err == nil || expiredHistory.Error != SyncSnapshotExpired {
		t.Fatalf("expired history token: page=%+v err=%v", expiredHistory, err)
	}
	recovered, err := f.historyPage(f.groups[1], nil, 100)
	if err != nil || recovered.HasMore || !slices.Equal(recovered.IDs, f.ids[f.groups[1]]) {
		t.Fatalf("history after abandoned-slot expiry: page=%+v err=%v", recovered, err)
	}
}

func TestInvalidatedHistorySnapshotReleasesItsSlot(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	var first HistorySyncResponse
	for i := 0; i < maxPeerSnapshots; i++ {
		page, err := f.historyPage(f.groups[0], nil, 1)
		if err != nil || !page.HasMore {
			t.Fatalf("start history %d: page=%+v err=%v", i, page, err)
		}
		if i == 0 {
			first = page
		}
	}
	f.addMessage(t, f.groups[0], 3)
	changed, err := f.historyPage(f.groups[0], &first, 1)
	if err == nil || changed.Error != SyncSnapshotExpired {
		t.Fatalf("changed generation: page=%+v err=%v", changed, err)
	}
	next, err := f.historyPage(f.groups[1], nil, 1)
	if err != nil || !next.HasMore || !slices.Equal(next.IDs, f.ids[f.groups[1]][:1]) {
		t.Fatalf("invalidated slot starved the other group: page=%+v err=%v", next, err)
	}
	full, err := f.rosterPage(f.groups[1], nil, 1)
	if err == nil || full.Error != SyncResourceExhausted {
		t.Fatalf("invalidation removed other active slots: page=%+v err=%v", full, err)
	}
}

type snapshotLifecycleFixture struct {
	ctx        context.Context
	founder    *keystore.Identity
	client     host.Host
	serverHost host.Host
	remote     peer.AddrInfo
	groups     [2]entmoot.GroupID
	logs       map[entmoot.GroupID]*roster.RosterLog
	store      *store.SQLite
	ids        map[entmoot.GroupID][]entmoot.MessageID
	elapsed    atomic.Int64
}

func newSnapshotLifecycleFixture(t *testing.T) *snapshotLifecycleFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	founder, member := mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverHost.Close() })
	clientHost, _, err := NewHost(ctx, member, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientHost.Close() })
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	f := &snapshotLifecycleFixture{
		ctx: ctx, founder: founder, client: clientHost,
		serverHost: serverHost,
		remote:     peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()},
		groups:     [2]entmoot.GroupID{{1}, {2}},
		logs:       make(map[entmoot.GroupID]*roster.RosterLog), store: source,
		ids: make(map[entmoot.GroupID][]entmoot.MessageID),
	}
	for _, groupID := range f.groups {
		log := roster.New(groupID)
		if err := log.Genesis(founder, mustNodeInfo(t, founder.PublicKey), 1_000); err != nil {
			t.Fatal(err)
		}
		entry, err := log.SignEntry(founder, "add", mustNodeInfo(t, member.PublicKey), nil, 2_000)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Apply(entry); err != nil {
			t.Fatal(err)
		}
		f.logs[groupID] = log
		f.addMessage(t, groupID, 1)
		f.addMessage(t, groupID, 2)
	}
	server := &SyncServer{
		Host: serverHost, Admission: NewBootstrapAdmission(), Store: source,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) {
			log, ok := f.logs[id]
			return log, ok
		},
		Now: func() time.Time { return time.Unix(200, 0).Add(time.Duration(f.elapsed.Load())) },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *snapshotLifecycleFixture) addMessage(t *testing.T, groupID entmoot.GroupID, sequence int) {
	t.Helper()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, f.founder.PublicKey), f.founder)
	if err != nil {
		t.Fatal(err)
	}
	head := f.logs[groupID].Head()
	message, err := signer.SignMessage(f.ctx, entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: int64(10_000 + sequence),
		Topics: []string{"sync"}, Content: []byte(fmt.Sprintf("message-%d", sequence)), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(f.ctx, groupID, message); err != nil {
		t.Fatal(err)
	}
	f.ids[groupID] = append(f.ids[groupID], message.ID)
}

func (f *snapshotLifecycleFixture) rosterPage(groupID entmoot.GroupID, previous *RosterSyncResponse, limit int) (RosterSyncResponse, error) {
	request := RosterSyncRequest{Version: 2, RequestID: "roster", GroupID: groupID, Limit: limit}
	if previous != nil {
		request.SnapshotToken = previous.SnapshotToken
		request.AfterSequence = previous.NextSequence
	}
	return RequestRosterPage(f.ctx, f.client, f.remote, request)
}

func (f *snapshotLifecycleFixture) historyPage(groupID entmoot.GroupID, previous *HistorySyncResponse, limit int) (HistorySyncResponse, error) {
	request := HistorySyncRequest{Version: 2, RequestID: "history", GroupID: groupID, Mode: "list", Limit: limit}
	if previous != nil {
		request.SnapshotToken = previous.SnapshotToken
		request.Generation = previous.Generation
		request.AfterID = previous.NextID
		request.AfterTimestampMS = previous.NextTimestampMS
		request.AfterAuthorMemberID = previous.NextAuthorMemberID
	}
	return RequestHistoryPage(f.ctx, f.client, f.remote, request)
}

// The head probe runs on every maintenance tick, so it must cost the server
// nothing that lingers. Reserving a paging snapshot it never finishes would
// pin one of the few per-peer slots for the snapshot lifetime and starve the
// roster pull the probe exists to decide on.
func TestRosterHeadProbeReservesNoSnapshotSlot(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	head := f.logs[group].Head()

	// Occupy every per-peer slot with genuine paged sessions, the state a busy
	// peer is in. A probe that reserves a snapshot cannot be served now.
	held := make([]RosterSyncResponse, 0, maxPeerSnapshots)
	for i := 0; i < maxPeerSnapshots; i++ {
		page, err := f.rosterPage(f.groups[i%len(f.groups)], nil, 1)
		if err != nil || page.Complete {
			t.Fatalf("holding reservation %d: page=%+v err=%v", i, page, err)
		}
		held = append(held, page)
	}
	exhausted, err := f.rosterPage(group, nil, 1)
	if err == nil || exhausted.Error != SyncResourceExhausted {
		t.Fatalf("slots are not full: page=%+v err=%v", exhausted, err)
	}

	// Probes must still answer, repeatedly, with every slot taken.
	for i := 0; i < maxPeerSnapshots*3; i++ {
		got, err := FetchRosterHead(f.ctx, f.client, f.remote, group)
		if err != nil {
			t.Fatalf("probe %d with all slots held: %v", i, err)
		}
		if got != head {
			t.Fatalf("probe %d returned head %s, want %s", i, got, head)
		}
	}

	// And the probes must not have consumed anything themselves: a pull works
	// as soon as the paged sessions finish.
	for i, page := range held {
		done, err := f.rosterPage(f.groups[i%len(f.groups)], &page, 100)
		if err != nil || !done.Complete {
			t.Fatalf("completing reservation %d: page=%+v err=%v", i, done, err)
		}
	}
	local := f.logs[group].Entries()[:1]
	updates, complete, err := FetchRosterUpdates(f.ctx, f.client, f.remote, group, local)
	if !complete {
		t.Fatal("a small chain was not served completely in one pull")
	}
	if err != nil {
		t.Fatalf("roster pull after the probes: %v", err)
	}
	if len(updates) != len(f.logs[group].Entries())-1 {
		t.Fatalf("pull returned %d entries, want %d", len(updates), len(f.logs[group].Entries())-1)
	}
}

// The pull ceiling is a per-round cost limit, not a limit on how many
// membership changes a group may ever make. Counting the local prefix would
// turn it into a lifetime cap: a group whose chain passed the ceiling could
// never be synced again by any node, and roster entries are never compacted.
func TestRosterPullCeilingBoundsTheDeltaNotTheChainLength(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	founder := mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientIdentity := mustIdentity(t)
	clientHost, _, err := NewHost(ctx, clientIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()

	groupID := entmoot.GroupID{0x6c}
	log := roster.New(groupID)
	founderInfo := mustNodeInfo(t, founder.PublicKey)
	if err := log.Genesis(founder, founderInfo, 1_000); err != nil {
		t.Fatal(err)
	}
	// The client must be a member to read the roster at all.
	client := mustNodeInfo(t, clientIdentity.PublicKey)
	entry, err := log.SignEntry(founder, "add", client, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Apply(entry); err != nil {
		t.Fatal(err)
	}
	// Grow the chain past the per-round ceiling with founder-signed policy
	// entries of a type this build does not interpret: cheap to produce and
	// accepted exactly like any other entry.
	foreign := []byte(`{"type":"legacy-identity-upgrade/v1"}`)
	for timestamp := int64(3_000); len(log.Entries()) <= maxRosterSyncEntries+2; timestamp++ {
		policyEntry, err := log.SignEntry(founder, "policy_change", entmoot.NodeInfo{}, foreign, timestamp)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Apply(policyEntry); err != nil {
			t.Fatal(err)
		}
	}
	chain := log.Entries()
	if len(chain) <= maxRosterSyncEntries {
		t.Fatalf("chain is %d entries, want more than the ceiling %d", len(chain), maxRosterSyncEntries)
	}

	server := &SyncServer{
		Host: serverHost, Admission: NewBootstrapAdmission(), Store: store.NewMemory(),
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) {
			if id != groupID {
				return nil, false
			}
			return log, true
		},
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}

	// A node that is one entry behind on an over-ceiling chain must still be
	// able to catch up: the delta is one entry.
	local := chain[:len(chain)-1]
	updates, complete, err := FetchRosterUpdates(ctx, clientHost, remote, groupID, local)
	if !complete {
		t.Fatal("a small chain was not served completely in one pull")
	}
	if err != nil {
		t.Fatalf("catching up one entry on a %d-entry chain: %v", len(chain), err)
	}
	if len(updates) != 1 || updates[0].ID != chain[len(chain)-1].ID {
		t.Fatalf("pull returned %d entries, want the single missing one", len(updates))
	}

	// The ceiling still bounds one round, and a node far behind converges by
	// keeping each round's validated progress instead of restarting.
	held := chain[:1]
	rounds := 0
	for {
		rounds++
		if rounds > 8 {
			t.Fatalf("catching up from the genesis did not converge in %d rounds", rounds)
		}
		part, complete, err := FetchRosterUpdates(ctx, clientHost, remote, groupID, held)
		if err != nil {
			t.Fatalf("round %d: %v", rounds, err)
		}
		if len(part) == 0 {
			t.Fatalf("round %d served nothing", rounds)
		}
		if len(part) > maxRosterSyncEntries {
			t.Fatalf("round %d downloaded %d entries, past the ceiling %d", rounds, len(part), maxRosterSyncEntries)
		}
		held = append(held, part...)
		if complete {
			break
		}
	}
	if rounds < 2 {
		t.Fatalf("an over-ceiling chain was taken in %d round; the ceiling did not bound the round", rounds)
	}
	if len(held) != len(chain) || held[len(held)-1].ID != chain[len(chain)-1].ID {
		t.Fatalf("catch-up produced %d entries ending %s, want %d ending %s",
			len(held), held[len(held)-1].ID, len(chain), chain[len(chain)-1].ID)
	}
}

// A peer asking for entries past the end of our chain is not making a
// malformed request: it holds a longer prefix than we do. Saying which of the
// two it is lets the caller tell a fork from a transport problem.
func TestRosterPageReportsAChainShorterThanTheRequest(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	held := len(f.logs[group].Entries())

	beyond, err := RequestRosterPage(f.ctx, f.client, f.remote, RosterSyncRequest{
		Version: 2, RequestID: "past-the-end", GroupID: group,
		AfterSequence: uint64(held + 3), Limit: 16,
	})
	if err == nil {
		t.Fatal("a request past the end of the chain succeeded")
	}
	if beyond.Error != SyncShortChain {
		t.Fatalf("error = %q, want %q", beyond.Error, SyncShortChain)
	}
	// A request inside the chain is unaffected.
	inside, err := RequestRosterPage(f.ctx, f.client, f.remote, RosterSyncRequest{
		Version: 2, RequestID: "inside", GroupID: group, AfterSequence: 1, Limit: 16,
	})
	if err != nil || !inside.Complete || len(inside.Entries) != held-1 {
		t.Fatalf("page inside the chain: page=%+v err=%v", inside, err)
	}
}
