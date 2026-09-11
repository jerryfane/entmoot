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
	entry, err := rosterLog.SignEntry(serverIdentity, "add", entmoot.NodeInfo{EntmootPubKey: extra.PublicKey, MemberID: &extraID}, nil, 0, 3_000)
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
	validated, err := ValidateRosterChain(groupID, entmoot.NodeInfo{EntmootPubKey: serverIdentity.PublicKey, MemberID: &serverBinding.MemberID}, first.CommittedHead, entries)
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
	withholdingHost, withholdingBinding, err := NewHost(ctx, withholdingIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer withholdingHost.Close()
	groupID, rosterLog := syncRoster(t, serverIdentity, serverBinding.MemberID, clientIdentity, clientBinding.MemberID)
	withholdingEntry, err := rosterLog.SignEntry(serverIdentity, "add", entmoot.NodeInfo{
		EntmootPubKey: withholdingIdentity.PublicKey,
		MemberID:      &withholdingBinding.MemberID,
	}, nil, 0, 3_000)
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
	signer, err := signing.NewLocalSigner(entmoot.NodeInfo{EntmootPubKey: serverIdentity.PublicKey, MemberID: &serverBinding.MemberID}, serverIdentity)
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
	}, destination, func(message entmoot.Message) error {
		return signing.VerifyMessage(message, message.Author)
	})
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

func syncRoster(t *testing.T, founderIdentity *keystore.Identity, founderID entmoot.MemberID, memberIdentity *keystore.Identity, memberID entmoot.MemberID) (entmoot.GroupID, *roster.RosterLog) {
	t.Helper()
	var groupID entmoot.GroupID
	groupID[0] = 9
	result := roster.New(groupID)
	if err := result.Genesis(founderIdentity, entmoot.NodeInfo{EntmootPubKey: founderIdentity.PublicKey, MemberID: &founderID}, 1_000); err != nil {
		t.Fatal(err)
	}
	entry, err := result.SignEntry(founderIdentity, "add", entmoot.NodeInfo{EntmootPubKey: memberIdentity.PublicKey, MemberID: &memberID}, nil, 0, 2_000)
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
