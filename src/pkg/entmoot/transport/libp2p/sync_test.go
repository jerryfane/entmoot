package libp2ptransport

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// membershipSyncPair is a served group plus a member that pulls from it over a
// real pair of hosts, which is the only way the wire format and the group's
// admission rules are exercised together.
type membershipSyncPair struct {
	ctx        context.Context
	founder    *keystore.Identity
	member     *keystore.Identity
	serverHost host.Host
	clientHost host.Host
	remote     peer.AddrInfo
	groupID    entmoot.GroupID
	group      *membership.Group
	// root is the group's first checkpoint, which is what a peer starting from
	// nothing adopts.
	root membership.Checkpoint
}

func newMembershipSyncPair(t *testing.T, joiners ...*keystore.Identity) *membershipSyncPair {
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
	groupID, group := mustOpenGroup(t, founder, append([]*keystore.Identity{member}, joiners...)...)
	messages := store.NewMemory()
	t.Cleanup(func() { _ = messages.Close() })
	server := &SyncServer{
		Host:  serverHost,
		Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	return &membershipSyncPair{
		ctx: ctx, founder: founder, member: member,
		serverHost: serverHost, clientHost: clientHost,
		remote:  peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()},
		groupID: groupID, group: group, root: group.Canonical(),
	}
}

// adoptClient gives the pulling member a store of its own holding nothing but
// the group's first checkpoint.
func (p *membershipSyncPair) adoptClient(t *testing.T) *membership.Group {
	t.Helper()
	group, err := membership.Adopt(t.TempDir(), p.root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return group
}

func TestMembershipPullCarriesRecordsThenCheckpoints(t *testing.T) {
	late := mustIdentity(t)
	p := newMembershipSyncPair(t, late)
	client := p.adoptClient(t)
	if slices.Equal(client.MemberIDs(), p.group.MemberIDs()) {
		t.Fatal("the client already agrees with the server, so the pull proves nothing")
	}

	checkpoints, records, complete, err := FetchMembership(p.ctx, p.clientHost, p.remote, client)
	if err != nil {
		t.Fatal(err)
	}
	if records != 2 || checkpoints != 0 || !complete {
		t.Fatalf("first pull: checkpoints=%d records=%d complete=%t", checkpoints, records, complete)
	}
	if !slices.Equal(client.MemberIDs(), p.group.MemberIDs()) {
		t.Fatalf("client members %v, server members %v", client.MemberIDs(), p.group.MemberIDs())
	}

	// Folding those records into a checkpoint is the other half of what a pull
	// has to carry: the caller already holds the records, so the answer is the
	// checkpoint alone.
	if _, signed, err := p.group.SignCheckpoint(p.founder, true); err != nil || !signed {
		t.Fatalf("sign checkpoint: signed=%t err=%v", signed, err)
	}
	if p.group.Canonical().Sequence != 1 {
		t.Fatalf("server canonical sequence = %d, want 1", p.group.Canonical().Sequence)
	}
	checkpoints, records, complete, err = FetchMembership(p.ctx, p.clientHost, p.remote, client)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoints != 1 || records != 0 || !complete {
		t.Fatalf("checkpoint pull: checkpoints=%d records=%d complete=%t", checkpoints, records, complete)
	}
	if client.Canonical().ID != p.group.Canonical().ID {
		t.Fatalf("client canonical %s, server canonical %s", client.Canonical().ID, p.group.Canonical().ID)
	}
	if !slices.Equal(client.MemberIDs(), p.group.MemberIDs()) {
		t.Fatalf("after checkpoint: client members %v, server members %v", client.MemberIDs(), p.group.MemberIDs())
	}
}

// A synchronised node pulls on every maintenance tick, so the steady state has
// to be free: nothing applied, and the peer reporting it had nothing left.
func TestMembershipPullAppliesNothingWhenAlreadySynchronised(t *testing.T) {
	p := newMembershipSyncPair(t, mustIdentity(t))
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client); err != nil {
		t.Fatal(err)
	}
	checkpoints, records, complete, err := FetchMembership(p.ctx, p.clientHost, p.remote, client)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoints != 0 || records != 0 || !complete {
		t.Fatalf("second pull: checkpoints=%d records=%d complete=%t", checkpoints, records, complete)
	}
}

// Every door is shut to a removed node, so the refusal itself has to carry the
// proof: the one signed record that names it. Applying that record is how the
// node's view of itself becomes correct without trusting an error code.
func TestMembershipPullTellsARemovedNodeItWasRemoved(t *testing.T) {
	p := newMembershipSyncPair(t)
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client); err != nil {
		t.Fatal(err)
	}
	binding, err := BindingFromPublicKey(p.member.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !client.IsMemberID(binding.MemberID) {
		t.Fatal("the pulling member is not a member locally, so removal proves nothing")
	}
	if _, err := p.group.SignRecord(p.founder, membership.Record{
		Kind: membership.KindRemove, Subject: mustNode(t, p.member),
	}); err != nil {
		t.Fatal(err)
	}
	_, records, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client)
	if !errors.Is(err, ErrRemoved) {
		t.Fatalf("pull after removal: records=%d err=%v", records, err)
	}
	if records != 1 {
		t.Fatalf("removal notice applied %d records, want 1", records)
	}
	if client.IsMemberID(binding.MemberID) {
		t.Fatal("the removed node still believes it is a member")
	}
}

// A truncated answer is still progress, because records are a set: the caller
// applies what arrived and asks again rather than resuming a cursor.
func TestTruncatedMembershipPageConverges(t *testing.T) {
	p := newMembershipSyncPair(t, mustIdentity(t), mustIdentity(t))
	client := p.adoptClient(t)
	response, err := RequestMembership(p.ctx, p.clientHost, p.remote, MembershipSyncRequest{
		Version:        1,
		RequestID:      "truncated",
		GroupID:        p.groupID,
		HaveSequence:   client.Canonical().Sequence,
		HaveCheckpoint: client.Canonical().ID,
		Limit:          1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Records) != 1 || response.Complete {
		t.Fatalf("page of one: records=%d complete=%t", len(response.Records), response.Complete)
	}
	if applied, err := client.Apply(response.Records[0]); err != nil || !applied {
		t.Fatalf("apply truncated record: applied=%t err=%v", applied, err)
	}
	if slices.Equal(client.MemberIDs(), p.group.MemberIDs()) {
		t.Fatal("one record was the whole set, so truncation proves nothing")
	}
	_, records, complete, err := FetchMembership(p.ctx, p.clientHost, p.remote, client)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || records == 0 {
		t.Fatalf("follow-up pull: records=%d complete=%t", records, complete)
	}
	if !slices.Equal(client.MemberIDs(), p.group.MemberIDs()) {
		t.Fatalf("client members %v, server members %v", client.MemberIDs(), p.group.MemberIDs())
	}
}

// Reading membership before membership is exactly what a joiner must do, and
// the invite is the only credential it can present. That credential has to
// stop working the moment the group withdraws it, on every node, without any
// node being told.
func TestMembershipReadRequiresMembershipOrALiveInvite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	founder, joiner := mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	joinerHost, _, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer joinerHost.Close()
	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	messages := store.NewMemory()
	defer messages.Close()
	server := SyncServer{
		Host:  serverHost,
		Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}
	read := func(name string, capability *entmoot.BootstrapCapability) (MembershipSyncResponse, error) {
		return RequestMembership(ctx, joinerHost, remote, MembershipSyncRequest{
			Version: 1, RequestID: name, GroupID: groupID, Capability: capability,
		})
	}

	stranger, err := read("stranger", nil)
	if err == nil || stranger.Error != SyncUnauthorized || len(stranger.Checkpoints) != 0 {
		t.Fatalf("a stranger read membership: response=%+v err=%v", stranger, err)
	}

	invite := mustInvite(t, group, founder, joiner.PublicKey, 0, []string{serverHost.ID().String()})
	invited, err := read("invited", &invite)
	if err != nil {
		t.Fatal(err)
	}
	if len(invited.Checkpoints) != 1 || invited.Checkpoints[0].ID != group.Canonical().ID || invited.Canonical != group.Canonical().ID {
		t.Fatalf("invited read did not carry the checkpoint to sign into: %+v", invited)
	}

	if _, err := group.SignRecord(founder, membership.Record{
		Kind: membership.KindRevokeInvite, InviteNonce: invite.Nonce,
	}); err != nil {
		t.Fatal(err)
	}
	if !group.IsInviteRevoked(invite.Nonce) {
		t.Fatal("the revocation record did not take effect")
	}
	revoked, err := read("revoked", &invite)
	if err == nil || revoked.Error != SyncUnauthorized || len(revoked.Checkpoints) != 0 {
		t.Fatalf("a revoked invite still read membership: response=%+v err=%v", revoked, err)
	}
}

// The whole point of self-signed joins is that nobody has to admit you: the
// joiner reads the checkpoint, signs itself in, and hands the record over. The
// peer it spoke to must end up holding the same membership it does.
func TestJoinGroupAdmitsTheJoinerOnBothSides(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	founder, joiner := mustIdentity(t), mustIdentity(t)
	serverHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	joinerHost, joinerBinding, err := NewHost(ctx, joiner, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer joinerHost.Close()
	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	messages := store.NewMemory()
	defer messages.Close()
	server := SyncServer{
		Host:  serverHost,
		Store: messages,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	invite := mustInvite(t, group, founder, joiner.PublicKey, 0, []string{serverHost.ID().String()})
	local, err := JoinGroup(ctx, joinerHost, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()},
		t.TempDir(), joiner, invite, mustNode(t, joiner))
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if !local.IsMemberID(joinerBinding.MemberID) {
		t.Fatal("the joiner's own group does not hold the joiner")
	}
	if !group.IsMemberID(joinerBinding.MemberID) {
		t.Fatal("the served group did not accept the pushed join record")
	}
	if local.GroupID() != groupID || local.Canonical().ID != group.Canonical().ID {
		t.Fatalf("joiner adopted group %s at %s, server is %s at %s",
			local.GroupID(), local.Canonical().ID, groupID, group.Canonical().ID)
	}
	if !slices.Equal(local.MemberIDs(), group.MemberIDs()) {
		t.Fatalf("joiner members %v, server members %v", local.MemberIDs(), group.MemberIDs())
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
	withholdingIdentity := mustIdentity(t)
	serverHost, _, err := NewHost(ctx, serverIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, _, err := NewHost(ctx, clientIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	withholdingHost, _, err := NewHost(ctx, withholdingIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer withholdingHost.Close()
	groupID, group := mustOpenGroup(t, serverIdentity, clientIdentity, withholdingIdentity)
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, serverIdentity.PublicKey), serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 300 {
		head := group.Canonical().ID
		message, err := signer.SignMessage(ctx, entmoot.Message{Version: 2, GroupID: groupID, Timestamp: int64(10_000 + i), Topics: []string{"sync"}, Content: []byte(fmt.Sprintf("message-%03d", i)), RosterHead: &head})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Put(ctx, groupID, message); err != nil {
			t.Fatal(err)
		}
	}
	serveGroup := func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID }
	server := SyncServer{Host: serverHost, Group: serveGroup, Store: source}
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
	withholdingServer := SyncServer{Host: withholdingHost, Group: serveGroup, Store: &withholdingStore{SQLite: source}}
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

type withholdingStore struct {
	*store.SQLite
}

func (*withholdingStore) Get(context.Context, entmoot.GroupID, entmoot.MessageID) (entmoot.Message, error) {
	return entmoot.Message{}, store.ErrNotFound
}

// Pre-membership reads are the group's front door: a holder chooses what its
// invite claims, so every claim in it has to be checked against the group's
// own state before membership and history open up.
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
	groupID, group := mustInviteOnlyGroup(t, t.TempDir(), founder)
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, founder.PublicKey), founder)
	if err != nil {
		t.Fatal(err)
	}
	head := group.Canonical().ID
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
	server := SyncServer{
		Host: serverHost, Store: source,
		Group: func(want entmoot.GroupID) (*membership.Group, bool) { return group, want == groupID },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}
	resign := func(capability *BootstrapCapability, identity *keystore.Identity) {
		t.Helper()
		capability.Signature = nil
		if err := SignBootstrapCapability(identity, capability); err != nil {
			t.Fatal(err)
		}
	}
	// denial is the membership error code expected, or "" when the read must
	// be served.
	check := func(name string, capability *BootstrapCapability, denial SyncErrorCode) {
		t.Helper()
		allowed := denial == ""
		t.Run(name, func(t *testing.T) {
			response, err := RequestMembership(ctx, clientHost, remote, MembershipSyncRequest{
				Version: 1, RequestID: name, GroupID: groupID, Capability: capability,
			})
			switch {
			case allowed:
				if err != nil || response.Canonical != group.Canonical().ID || len(response.Checkpoints) == 0 {
					t.Fatalf("membership read: response=%+v err=%v", response, err)
				}
			case err == nil || response.Error != denial || len(response.Checkpoints) != 0:
				t.Fatalf("membership denial: response=%+v err=%v", response, err)
			case denial == SyncNotMember:
				// A refused former member is told exactly why, with the one
				// signed record that names it and nothing about the group.
				if len(response.Records) != 1 || response.Records[0].Kind != membership.KindRemove {
					t.Fatalf("removal notice: %+v", response.Records)
				}
				subject, err := response.Records[0].SubjectMemberID()
				if err != nil || subject != targetBinding.MemberID {
					t.Fatalf("removal notice names %v (err=%v), want the caller", subject, err)
				}
			case len(response.Records) != 0:
				t.Fatalf("denial disclosed records: %+v", response.Records)
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
		})
	}

	grant := mustInvite(t, group, founder, target.PublicKey, 0, []string{serverHost.ID().String()})
	check("no_grant", nil, SyncUnauthorized)
	wrongFounder := grant
	wrongFounder.Founder = mustNode(t, target)
	resign(&wrongFounder, target)
	check("untrusted_founder", &wrongFounder, SyncUnauthorized)
	wrongServer := grant
	wrongServer.AllowedPeerIDs = []string{clientHost.ID().String()}
	resign(&wrongServer, founder)
	check("unlisted_server", &wrongServer, SyncUnauthorized)
	// The capability's signature verifies against whatever issuer it names, so
	// naming the real founder as the anchor and yourself as the issuer must not
	// buy pre-membership membership or history access.
	selfIssued := grant
	selfIssued.Nonce = [32]byte{9}
	selfIssuer := mustNode(t, target)
	selfIssued.Issuer = &selfIssuer
	resign(&selfIssued, target)
	check("unauthorized_issuer", &selfIssued, SyncUnauthorized)

	// An invite the group has withdrawn with a signed record stops working
	// everywhere, without this node being told by anyone.
	revoked := mustInvite(t, group, founder, target.PublicKey, 0, []string{serverHost.ID().String()})
	if _, err := group.SignRecord(founder, membership.Record{
		Kind: membership.KindRevokeInvite, InviteNonce: revoked.Nonce,
	}); err != nil {
		t.Fatal(err)
	}
	check("revoked_invite", &revoked, SyncUnauthorized)
	check("fresh_grant", &grant, "")

	// Redeeming the invite exhausts it, and membership takes over as the
	// credential: the holder keeps its access under its own key.
	mustJoinWithInvite(t, group, target, grant)
	if !group.IsMemberID(targetBinding.MemberID) {
		t.Fatal("the redeemed invite did not admit its target")
	}
	check("admitted_member", nil, "")
	check("member_with_exhausted_invite", &grant, "")

	if _, err := group.SignRecord(founder, membership.Record{
		Kind: membership.KindRemove, Subject: mustNode(t, target),
	}); err != nil {
		t.Fatal(err)
	}
	if group.IsMemberID(targetBinding.MemberID) {
		t.Fatal("the removal record did not take effect")
	}
	check("removed_member", nil, SyncNotMember)
	check("removed_member_with_exhausted_invite", &grant, SyncNotMember)

	// Removal is not a ban: a fresh invite readmits the same key, and that
	// invite outranks the removal notice because it authorises the read.
	second := mustInvite(t, group, founder, target.PublicKey, 0, []string{serverHost.ID().String()})
	check("fresh_invite_after_removal", &second, "")
}
