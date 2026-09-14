package libp2ptransport

import (
	"context"
	"fmt"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

func TestRosterPagesPinSnapshotAndRequireMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	serverIdentity := mustIdentity(t)
	clientIdentity := mustIdentity(t)
	serverHost, serverBinding, err := NewHost(ctx, serverIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, clientBinding, err := NewHost(ctx, clientIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	groupID, rosterLog := syncRoster(t, serverIdentity, serverBinding.MemberID, clientIdentity, clientBinding.MemberID)
	server := SyncServer{
		Host:      serverHost,
		Admission: NewBootstrapAdmission(),
		Roster: func(want entmoot.GroupID) (*roster.RosterLog, bool) {
			return rosterLog, want == groupID
		},
		Store: store.NewMemory(),
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}
	first, err := RequestRosterPage(ctx, clientHost, remote, RosterSyncRequest{Version: 2, RequestID: "first", GroupID: groupID, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete || first.SnapshotToken == "" || len(first.Entries) != 1 {
		t.Fatalf("first roster page = %+v", first)
	}
	extra := mustIdentity(t)
	extraID, _ := entmoot.MemberIDFromPublicKey(extra.PublicKey)
	entry, err := rosterLog.SignEntry(serverIdentity, "add", mustNodeInfo(t, extra.PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(entry); err != nil {
		t.Fatal(err)
	}
	second, err := RequestRosterPage(ctx, clientHost, remote, RosterSyncRequest{Version: 2, RequestID: "second", GroupID: groupID, SnapshotToken: first.SnapshotToken, AfterSequence: first.NextSequence, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete || len(second.Entries) != 1 {
		t.Fatalf("snapshot was not pinned to original roster: %+v", second)
	}
	entries := append(append([]entmoot.RosterEntry(nil), first.Entries...), second.Entries...)
	validated, err := ValidateRosterChain(groupID, mustNodeInfo(t, serverIdentity.PublicKey), first.CommittedHead, entries)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Head() != first.CommittedHead || validated.IsMemberID(extraID) {
		t.Fatal("temporary roster validation did not preserve the pinned snapshot")
	}
}

func TestKeeperAvailabilitySummaryIsExplicit(t *testing.T) {
	if got := SummarizeKeeperProgress(nil); got.Availability != NoKeeperAvailable || got.Eligible != 0 {
		t.Fatalf("zero-keeper summary = %+v", got)
	}
	one := SummarizeKeeperProgress([]KeeperProgress{{Available: true, Inserted: 2, ConvergedHint: true}})
	if one.Availability != OneKeeperAvailable || one.Available != 1 || one.Inserted != 2 {
		t.Fatalf("one-keeper summary = %+v", one)
	}
	multiple := SummarizeKeeperProgress([]KeeperProgress{{Available: true}, {Available: true, MissingBodies: 1}})
	if multiple.Availability != MultipleKeepersAvailable || multiple.Available != 2 || multiple.MissingBodies != 1 {
		t.Fatalf("multi-keeper summary = %+v", multiple)
	}
}

func TestHistorySyncContinuesAfterWithholdingKeeperAndResumesPages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	serverIdentity := mustIdentity(t)
	clientIdentity := mustIdentity(t)
	serverHost, serverBinding, err := NewHost(ctx, serverIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, clientBinding, err := NewHost(ctx, clientIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	withholdingIdentity := mustIdentity(t)
	withholdingHost, _, err := NewHost(ctx, withholdingIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer withholdingHost.Close()
	groupID, rosterLog := syncRoster(t, serverIdentity, serverBinding.MemberID, clientIdentity, clientBinding.MemberID)
	withholdingEntry, err := rosterLog.SignEntry(serverIdentity, "add", mustNodeInfo(t, withholdingIdentity.PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(withholdingEntry); err != nil {
		t.Fatal(err)
	}
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, serverIdentity.PublicKey), serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		head := rosterLog.Head()
		message, err := signer.SignMessage(ctx, entmoot.Message{Version: 2, GroupID: groupID, Timestamp: int64(10_000 + i), Topics: []string{"sync"}, Content: []byte(fmt.Sprintf("message-%03d", i)), RosterHead: &head})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Put(ctx, groupID, message); err != nil {
			t.Fatal(err)
		}
	}
	server := SyncServer{
		Host:      serverHost,
		Admission: NewBootstrapAdmission(),
		Roster: func(want entmoot.GroupID) (*roster.RosterLog, bool) {
			return rosterLog, want == groupID
		},
		Store: source,
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	window, err := RequestHistoryPage(ctx, clientHost, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}, HistorySyncRequest{
		Version:         2,
		RequestID:       "bounded-window",
		GroupID:         groupID,
		Mode:            "list",
		CoverageFloorMS: 10_000,
		CoverageCeilMS:  10_100,
		Limit:           200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(window.IDs) != 100 || window.HasMore {
		t.Fatalf("bounded history window = %+v", window)
	}
	withholdingServer := SyncServer{
		Host:      withholdingHost,
		Admission: NewBootstrapAdmission(),
		Roster: func(want entmoot.GroupID) (*roster.RosterLog, bool) {
			return rosterLog, want == groupID
		},
		Store: &withholdingStore{SQLite: source},
	}
	if err := withholdingServer.Install(); err != nil {
		t.Fatal(err)
	}
	destination, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	progress := SyncFromKeepers(ctx, clientHost, groupID, []peer.AddrInfo{
		{ID: withholdingHost.ID(), Addrs: withholdingHost.Addrs()},
		{ID: serverHost.ID(), Addrs: serverHost.Addrs()},
	}, destination, func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}, &HistorySyncState{})
	if len(progress) != 2 || !progress[0].Available || progress[0].MissingBodies != 300 || progress[0].ConvergedHint || !progress[1].Available {
		t.Fatalf("keeper progress = %+v", progress)
	}
	if progress[1].Inserted != 300 || progress[1].Listed != 300 || !progress[1].ConvergedHint {
		t.Fatalf("honest keeper progress = %+v", progress[1])
	}
	messages, err := destination.Range(ctx, groupID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 300 {
		t.Fatalf("destination has %d messages, want 300", len(messages))
	}
}
func TestFetchRosterUpdatesAdvancesExistingMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	thirdIdentity := mustIdentity(t)
	founderID, err := entmoot.MemberIDFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberID, err := entmoot.MemberIDFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	groupID, founderRoster := syncRoster(t, founderIdentity, founderID, memberIdentity, memberID)
	stale := roster.New(groupID)
	for index, entry := range founderRoster.Entries() {
		if index == 0 {
			if err := stale.AcceptGenesis(entry); err != nil {
				t.Fatal(err)
			}
		} else if err := stale.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	third := mustNodeInfo(t, thirdIdentity.PublicKey)
	add, err := founderRoster.SignEntry(founderIdentity, "add", third, nil, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if err := founderRoster.Apply(add); err != nil {
		t.Fatal(err)
	}
	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	memberHost, _, err := NewHost(ctx, memberIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer memberHost.Close()
	messageStore := store.NewMemory()
	defer messageStore.Close()
	server := SyncServer{
		Host: founderHost, Admission: NewBootstrapAdmission(), Store: messageStore,
		Roster: func(candidate entmoot.GroupID) (*roster.RosterLog, bool) {
			return founderRoster, candidate == groupID
		},
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	updates, complete, err := FetchRosterUpdates(ctx, memberHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, groupID, stale.Entries())
	if !complete {
		t.Fatal("a small chain was not served completely in one pull")
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 {
		t.Fatalf("received %d roster updates, want 1", len(updates))
	}
	if err := stale.Apply(updates[0]); err != nil {
		t.Fatal(err)
	}
	thirdID, err := entmoot.MemberIDFromPublicKey(thirdIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !stale.IsMemberID(thirdID) {
		t.Fatal("existing member roster did not advance to include the new member")
	}
}

func syncRoster(t *testing.T, founderIdentity *keystore.Identity, founderID entmoot.MemberID, memberIdentity *keystore.Identity, memberID entmoot.MemberID) (entmoot.GroupID, *roster.RosterLog) {
	t.Helper()
	var groupID entmoot.GroupID
	groupID[0] = 9
	result := roster.New(groupID)
	if err := result.Genesis(founderIdentity, mustNodeInfo(t, founderIdentity.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	entry, err := result.SignEntry(founderIdentity, "add", mustNodeInfo(t, memberIdentity.PublicKey), nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.Apply(entry); err != nil {
		t.Fatal(err)
	}
	return groupID, result
}

type withholdingStore struct {
	*store.SQLite
}

func (*withholdingStore) Get(context.Context, entmoot.GroupID, entmoot.MessageID) (entmoot.Message, error) {
	return entmoot.Message{}, store.ErrNotFound
}

func TestSyncBootstrapAuthorityAndMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	founder, target := mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, targetBinding, err := NewHost(ctx, target, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	groupID := entmoot.GroupID{42}
	log := roster.New(groupID)
	if err := log.Genesis(founder, mustNodeInfo(t, founder.PublicKey), 1_000); err != nil {
		t.Fatal(err)
	}
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, founder.PublicKey), founder)
	if err != nil {
		t.Fatal(err)
	}
	head := log.Head()
	message, err := signer.SignMessage(ctx, entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: 2_000, Topics: []string{"private"},
		Content: []byte("private history"), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put(ctx, groupID, message); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	admission, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admission.Close() }()
	server := SyncServer{
		Host: serverHost, Admission: admission.BootstrapAdmission, Store: source,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { return log, id == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}
	now := time.Now()
	grant := BootstrapCapability{
		GroupID: groupID, RosterHead: head, Nonce: [32]byte{1},
		Founder: mustNodeInfo(t, founder.PublicKey), TargetPublicKey: target.PublicKey,
		TargetMemberID: targetBinding.MemberID, TargetPeerID: clientHost.ID().String(),
		AllowedPeerIDs: []string{serverHost.ID().String()},
		IssuedAtMS:     now.Add(-time.Minute).UnixMilli(), ExpiresAtMS: now.Add(time.Hour).UnixMilli(),
	}
	sign := func(capability *BootstrapCapability, identity *keystore.Identity) {
		t.Helper()
		if err := SignBootstrapCapability(identity, capability); err != nil {
			t.Fatal(err)
		}
	}
	sign(&grant, founder)
	check := func(name string, capability *BootstrapCapability, allowed bool) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			response, err := RequestRosterPage(ctx, clientHost, remote, RosterSyncRequest{
				Version: 2, RequestID: name, GroupID: groupID, Capability: capability,
			})
			if allowed {
				if err != nil || response.CommittedHead != log.Head() || len(response.Entries) != len(log.Entries()) {
					t.Fatalf("roster read: response=%+v err=%v", response, err)
				}
			} else if err == nil || response.Error != SyncUnauthorized || len(response.Entries) != 0 {
				t.Fatalf("roster denial: response=%+v err=%v", response, err)
			}
			for _, mode := range []string{"list", "bodies"} {
				history, err := RequestHistoryPage(ctx, clientHost, remote, HistorySyncRequest{
					Version: 2, RequestID: name + mode, GroupID: groupID, Capability: capability,
					Mode: mode, IDs: []entmoot.MessageID{message.ID},
				})
				if allowed {
					if err != nil {
						t.Fatal(err)
					}
					if mode == "bodies" && (len(history.Messages) != 1 || history.Messages[0].ID != message.ID) {
						t.Fatalf("history read: %+v", history)
					}
				} else if err == nil || history.Error != SyncUnauthorized || len(history.Messages) != 0 || len(history.IDs) != 0 {
					t.Fatalf("history denial: response=%+v err=%v", history, err)
				}
			}
			t.Logf("roster and history list/bodies allowed=%t", allowed)
		})
	}
	check("no_grant", nil, false)
	wrongFounder := grant
	wrongFounder.Founder = mustNodeInfo(t, target.PublicKey)
	sign(&wrongFounder, target)
	check("untrusted_founder", &wrongFounder, false)
	wrongServer := grant
	wrongServer.AllowedPeerIDs = []string{clientHost.ID().String()}
	sign(&wrongServer, founder)
	check("unlisted_server", &wrongServer, false)
	// The capability's signature verifies against whatever issuer it names, so
	// naming the real founder as the anchor and yourself as the issuer must not
	// buy pre-membership roster or history access.
	selfIssued := grant
	selfIssued.Nonce = [32]byte{9}
	selfIssuer := mustNodeInfo(t, target.PublicKey)
	selfIssued.Issuer = &selfIssuer
	sign(&selfIssued, target)
	check("unauthorized_issuer", &selfIssued, false)
	check("fresh_grant", &grant, true)
	if err := admission.Reserve(grant, clientHost.ID(), EnrollmentProtocol, now); err != nil {
		t.Fatal(err)
	}
	check("reserved_grant", &grant, false)
	if err := admission.Release(grant, clientHost.ID()); err != nil {
		t.Fatal(err)
	}
	check("released_grant", &grant, true)
	// Consume a grant without changing the roster so nonce enforcement is
	// tested independently of checkpoint freshness, including after restart.
	if err := admission.Authorize(grant, clientHost.ID(), EnrollmentProtocol, now); err != nil {
		t.Fatal(err)
	}
	check("consumed_grant", &grant, false)
	if err := admission.Close(); err != nil {
		t.Fatal(err)
	}
	admission, err = OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	restarted := SyncServer{
		Host: serverHost, Admission: admission.BootstrapAdmission, Store: source,
		Roster: server.Roster,
	}
	if err := restarted.Install(); err != nil {
		t.Fatal(err)
	}
	check("consumed_after_restart", &grant, false)
	fresh := grant
	fresh.Nonce = [32]byte{2}
	sign(&fresh, founder)
	enrollment := EnrollmentServer{
		Admission: admission.BootstrapAdmission,
		Enroll: func(context.Context, BootstrapCapability, entmoot.NodeInfo) (EnrollmentResponse, error) {
			entry, err := log.SignEntry(founder, "add", mustNodeInfo(t, target.PublicKey), nil, 3_000)
			if err != nil {
				return EnrollmentResponse{}, err
			}
			if err := log.Apply(entry); err != nil {
				return EnrollmentResponse{}, err
			}
			return EnrollmentResponse{RosterHead: log.Head(), Entries: log.Entries()}, nil
		},
	}
	if err := enrollment.Install(serverHost); err != nil {
		t.Fatal(err)
	}
	if _, err := Enroll(ctx, clientHost, remote, fresh, target.PublicKey); err != nil {
		t.Fatal(err)
	}
	check("admitted_member", nil, true)
	check("member_with_consumed_grant", &fresh, true)
	entry, err := log.SignEntry(founder, "remove", mustNodeInfo(t, target.PublicKey), nil, 4_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Apply(entry); err != nil {
		t.Fatal(err)
	}
	check("removed_member", nil, false)
	check("removed_consumed_grant", &fresh, false)
	stale := fresh
	stale.Nonce = [32]byte{3}
	sign(&stale, founder)
	check("stale_unused_grant", &stale, false)
}
