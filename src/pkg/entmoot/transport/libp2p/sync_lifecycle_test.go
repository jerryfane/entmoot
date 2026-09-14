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
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// A completed pull must cost the server nothing afterwards: its token is
// retired and its slot freed, so a member polling two groups in turn keeps
// being served instead of locking itself out after maxPeerSnapshots polls.
func TestTerminalSnapshotsDoNotStarveMultiGroupHistory(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	for i := range maxPeerSnapshots*2 + 1 {
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
	var sessions [maxPeerSnapshots]HistorySyncResponse
	for i := range sessions {
		groupID := f.groups[i%len(f.groups)]
		page, err := f.historyPage(groupID, nil, 1)
		if err != nil || !page.HasMore || !slices.Equal(page.IDs, f.ids[groupID][:1]) {
			t.Fatalf("start history %d: page=%+v err=%v", i, page, err)
		}
		sessions[i] = page
	}
	overQuota, err := f.historyPage(f.groups[0], nil, 1)
	if err == nil || overQuota.Error != SyncResourceExhausted {
		t.Fatalf("active history quota: page=%+v err=%v", overQuota, err)
	}

	// Completion frees exactly one slot and retires exactly one token.
	last, err := f.historyPage(f.groups[0], &sessions[0], 100)
	if err != nil || last.HasMore || last.Generation != sessions[0].Generation ||
		last.SnapshotToken != sessions[0].SnapshotToken || !slices.Equal(last.IDs, f.ids[f.groups[0]][1:]) {
		t.Fatalf("pinned history completion: page=%+v err=%v", last, err)
	}
	retired, err := f.historyPage(f.groups[0], &sessions[0], 100)
	if err == nil || retired.Error != SyncSnapshotExpired {
		t.Fatalf("completed history token remained usable: page=%+v err=%v", retired, err)
	}
	replacement, err := f.historyPage(f.groups[1], nil, 1)
	if err != nil || !replacement.HasMore {
		t.Fatalf("reusing the completed slot: page=%+v err=%v", replacement, err)
	}
	full, err := f.historyPage(f.groups[0], nil, 1)
	if err == nil || full.Error != SyncResourceExhausted {
		t.Fatalf("replacement exceeded the active quota: page=%+v err=%v", full, err)
	}

	// A token is a handle for one group's pull, and rejecting a misdirected
	// one must not evict the session it names.
	wrongGroup, err := f.historyPage(f.groups[0], &sessions[1], 1)
	if err == nil || wrongGroup.Error != SyncSnapshotExpired {
		t.Fatalf("cross-group token: page=%+v err=%v", wrongGroup, err)
	}
	second, err := f.historyPage(f.groups[1], &sessions[1], 1)
	if err != nil || !second.HasMore || second.SnapshotToken != sessions[1].SnapshotToken ||
		!slices.Equal(second.IDs, f.ids[f.groups[1]][1:2]) {
		t.Fatalf("live continuation after a rejected token: page=%+v err=%v", second, err)
	}

	// Abandoned slots are held until they expire, and a continuation does not
	// extend the original lifetime. No sleeps or peeking at the server's
	// private snapshot map are needed.
	f.elapsed.Store(int64(syncSnapshotLifetime - time.Nanosecond))
	stillFull, err := f.historyPage(f.groups[0], nil, 1)
	if err == nil || stillFull.Error != SyncResourceExhausted {
		t.Fatalf("abandoned slots reclaimed before expiry: page=%+v err=%v", stillFull, err)
	}
	third, err := f.historyPage(f.groups[1], &second, 1)
	if err != nil || !third.HasMore || !slices.Equal(third.IDs, f.ids[f.groups[1]][2:3]) {
		t.Fatalf("continuation before expiry: page=%+v err=%v", third, err)
	}
	f.elapsed.Store(int64(syncSnapshotLifetime))
	expired, err := f.historyPage(f.groups[1], &third, 1)
	if err == nil || expired.Error != SyncSnapshotExpired {
		t.Fatalf("expired history token: page=%+v err=%v", expired, err)
	}
	recovered, err := f.historyPage(f.groups[1], nil, 100)
	if err != nil || recovered.HasMore || !slices.Equal(recovered.IDs, f.ids[f.groups[1]]) {
		t.Fatalf("history after abandoned-slot expiry: page=%+v err=%v", recovered, err)
	}
}

func TestInvalidatedHistorySnapshotReleasesItsSlot(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	var first HistorySyncResponse
	for i := range maxPeerSnapshots {
		page, err := f.historyPage(f.groups[0], nil, 1)
		if err != nil || !page.HasMore {
			t.Fatalf("start history %d: page=%+v err=%v", i, page, err)
		}
		if i == 0 {
			first = page
		}
	}
	f.addMessage(t, f.groups[0], 9)
	changed, err := f.historyPage(f.groups[0], &first, 1)
	if err == nil || changed.Error != SyncSnapshotExpired {
		t.Fatalf("changed generation: page=%+v err=%v", changed, err)
	}
	next, err := f.historyPage(f.groups[1], nil, 1)
	if err != nil || !next.HasMore || !slices.Equal(next.IDs, f.ids[f.groups[1]][:1]) {
		t.Fatalf("invalidated slot starved the other group: page=%+v err=%v", next, err)
	}
	full, err := f.historyPage(f.groups[1], nil, 1)
	if err == nil || full.Error != SyncResourceExhausted {
		t.Fatalf("invalidation removed other active slots: page=%+v err=%v", full, err)
	}
}

// A snapshot token is a handle, not an authorisation: the paged path checks
// owner, group and generation before honouring one. Another member of the same
// group is authorized, so its request reaches the handler — and must still not
// be able to drive, or disturb, someone else's in-flight pull.
func TestHistorySnapshotTokenBelongsToItsOwner(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	page, err := f.historyPage(group, nil, 1)
	if err != nil || !page.HasMore || page.SnapshotToken == "" {
		t.Fatalf("starting the pull: page=%+v err=%v", page, err)
	}

	otherIdentity := mustIdentity(t)
	if _, err := f.membership[group].SignRecord(otherIdentity, membership.Record{Kind: membership.KindJoin}); err != nil {
		t.Fatal(err)
	}
	other, _, err := NewHost(f.ctx, otherIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ownPage, err := RequestHistoryPage(f.ctx, other, f.remote, HistorySyncRequest{
		Version: 2, RequestID: "member-can-read", GroupID: group, Mode: "list", Limit: 1,
	})
	if err != nil || !ownPage.HasMore {
		t.Fatalf("the second member cannot read history, so the test proves nothing: page=%+v err=%v", ownPage, err)
	}
	stolen, err := RequestHistoryPage(f.ctx, other, f.remote, HistorySyncRequest{
		Version: 2, RequestID: "steal-token", GroupID: group, Mode: "list", Limit: 1,
		SnapshotToken: page.SnapshotToken, Generation: page.Generation,
		AfterID: page.NextID, AfterTimestampMS: page.NextTimestampMS, AfterAuthorMemberID: page.NextAuthorMemberID,
	})
	if err == nil || stolen.Error != SyncSnapshotExpired {
		t.Fatalf("another member drove someone else's snapshot: page=%+v err=%v", stolen, err)
	}

	// The owner's pull must still continue on its own snapshot.
	next, err := f.historyPage(group, &page, 100)
	if err != nil || next.HasMore || next.SnapshotToken != page.SnapshotToken {
		t.Fatalf("the owner's pull was disturbed by another caller: page=%+v err=%v", next, err)
	}
}

type snapshotLifecycleFixture struct {
	ctx        context.Context
	founder    *keystore.Identity
	client     host.Host
	serverHost host.Host
	remote     peer.AddrInfo
	groups     [2]entmoot.GroupID
	membership map[entmoot.GroupID]*membership.Group
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
		membership: make(map[entmoot.GroupID]*membership.Group),
		store:      source,
		ids:        make(map[entmoot.GroupID][]entmoot.MessageID),
	}
	for i := range f.groups {
		groupID, group := mustOpenGroup(t, founder, member)
		f.groups[i] = groupID
		f.membership[groupID] = group
		// Four messages per group: enough for a paged pull to stay in flight
		// across several continuations.
		for sequence := 1; sequence <= 4; sequence++ {
			f.addMessage(t, groupID, sequence)
		}
	}
	server := &SyncServer{
		Host: serverHost, Store: source,
		Group: func(id entmoot.GroupID) (*membership.Group, bool) {
			group, ok := f.membership[id]
			return group, ok
		},
		Now: func() time.Time { return time.Unix(200, 0).Add(time.Duration(f.elapsed.Load())) },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	return f
}

// head is the checkpoint a message published now must cite.
func (f *snapshotLifecycleFixture) head(groupID entmoot.GroupID) entmoot.RosterEntryID {
	return f.membership[groupID].Canonical().ID
}

func (f *snapshotLifecycleFixture) addMessage(t *testing.T, groupID entmoot.GroupID, sequence int) {
	t.Helper()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, f.founder.PublicKey), f.founder)
	if err != nil {
		t.Fatal(err)
	}
	head := f.head(groupID)
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
