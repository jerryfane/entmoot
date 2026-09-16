package libp2ptransport

import (
	"errors"
	"testing"

	libp2p "github.com/libp2p/go-libp2p"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A node with a real backlog must still converge. The client used to name the
// ids it held in the request, which (a) overran the request frame once it held
// a few hundred records and (b) made the server re-serve records the caller
// already had, page after page, without ever reaching the ones it lacked.
func TestMembershipPullConvergesWithABacklogLargerThanOnePage(t *testing.T) {
	p := newMembershipSyncPair(t)
	client := p.adoptClient(t)

	// More records than one page carries, so convergence depends on the
	// cursor advancing rather than on the page size.
	joiners := make([]*keystore.Identity, 0, maxMembershipRecords+8)
	for i := 0; i < maxMembershipRecords+8; i++ {
		joiner := mustIdentity(t)
		joiners = append(joiners, joiner)
		invite := mustInvite(t, p.group, p.founder, joiner.PublicKey, 1, nil)
		mustJoinWithInvite(t, p.group, joiner, invite)
	}
	if got := len(p.group.Pending()); got <= maxMembershipRecords {
		t.Fatalf("server holds %d pending records, want more than one page (%d)", got, maxMembershipRecords)
	}

	checkpoints, records, complete, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID)
	if err != nil {
		t.Fatalf("pull: %v (checkpoints=%d records=%d)", err, checkpoints, records)
	}
	if !complete {
		t.Fatalf("pull stopped short: applied %d records of %d", records, len(p.group.Pending()))
	}
	if got, want := len(client.MemberIDs()), len(p.group.MemberIDs()); got != want {
		t.Fatalf("client holds %d members, server %d", got, want)
	}
	for _, joiner := range joiners {
		id, err := entmoot.MemberIDFromPublicKey(joiner.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		if !client.IsMemberID(id) {
			t.Fatalf("member %s never arrived", id.String())
		}
	}
}

// A refused pull must not be able to talk a node out of its own membership.
// The peer's answer counted as proof of removal as soon as the record was
// STORED, but storing a record and giving it effect are different things.
func TestRefusalWithAnIneffectiveRecordIsNotAnEviction(t *testing.T) {
	p := newMembershipSyncPair(t)
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID); err != nil {
		t.Fatal(err)
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("the pulling member is not a member of its own group")
	}

	// A record that names the caller but cannot take effect: signed by a
	// stranger, so the projection ignores it even though it stores.
	stranger := mustIdentity(t)
	subject := mustNode(t, p.member)
	ineffective, err := membership.SignRecord(stranger, membership.Record{
		Version:   membership.Version,
		GroupID:   p.groupID,
		Kind:      membership.KindRemove,
		Actor:     mustNode(t, stranger),
		Subject:   entmoot.NodeInfo{MemberID: subject.MemberID},
		Timestamp: client.Canonical().Timestamp + 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := client.Apply(ineffective)
	if err != nil {
		t.Fatalf("a stranger's removal should store and be ignored, got %v", err)
	}
	if !applied {
		// Not a reason to skip: this test exists because a record that STORES
		// and does nothing was once read as proof of removal. If the store
		// stops holding it, the spoof this guards against is unreachable and
		// the guard needs rewriting rather than quietly passing.
		t.Fatal("the store refused the ineffective record outright; this test no longer exercises the spoof it guards")
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("a stranger's removal took effect")
	}
}

// A peer that refuses us may serve the record it says removed us. Only the
// projection decides whether it did: a record that stores but changes nothing
// is not an eviction, and this must hold through the real pull path, not just
// at Group.Apply.
func TestRefusedPullOnlyReportsRemovalWhenMembershipActuallyDrops(t *testing.T) {
	p := newMembershipSyncPair(t)
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID); err != nil {
		t.Fatal(err)
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("the pulling member is not a member of its own group")
	}

	// The server holds a removal naming the client, signed by a stranger, so
	// it stores and is ignored. The server itself still counts the client as a
	// member, so it will refuse the pull on the strength of... nothing: the
	// refusal has to come from somewhere, so remove the client for real on the
	// server and hand it a SECOND, ineffective record for the same subject.
	stranger := mustIdentity(t)
	subject := mustNode(t, p.member)
	ineffective, err := membership.SignRecord(stranger, membership.Record{
		Version:   membership.Version,
		GroupID:   p.groupID,
		Kind:      membership.KindRemove,
		Actor:     mustNode(t, stranger),
		Subject:   entmoot.NodeInfo{MemberID: subject.MemberID},
		Timestamp: p.group.Canonical().Timestamp + 5_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := p.group.Apply(ineffective); err != nil || !applied {
		t.Fatalf("the server would not hold the ineffective record: applied=%t err=%v", applied, err)
	}
	if !p.group.IsMemberID(p.clientMemberID) {
		t.Fatal("a stranger's removal took effect on the server")
	}

	// The client is still a member here, so the pull is authorised and must
	// come back clean: the ineffective record may travel, but it must not be
	// read as an eviction.
	_, _, _, err = FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID)
	if errors.Is(err, ErrRemoved) {
		t.Fatal("an ineffective removal was reported as an eviction")
	}
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("the client evicted itself on a record that changes nothing")
	}

	// Now a real removal, signed by the founder: the same path must report it.
	if _, err := p.group.SignRecord(p.founder, membership.Record{
		Kind:    membership.KindRemove,
		Subject: subject,
	}); err != nil {
		t.Fatal(err)
	}
	if p.group.IsMemberID(p.clientMemberID) {
		t.Fatal("the founder's removal did not take effect on the server")
	}
	_, _, _, err = FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID)
	if !errors.Is(err, ErrRemoved) {
		t.Fatalf("a real removal was not reported: %v", err)
	}
	if client.IsMemberID(p.clientMemberID) {
		t.Fatal("the client still counts itself a member after learning of its removal")
	}
}

// A hostile peer can say anything, including "you were removed", and attach a
// record that names this node but changes nothing. The client must not act on
// it: the only thing that evicts a node is its own projection dropping it.
func TestHostilePeerCannotTalkANodeOutOfItsMembership(t *testing.T) {
	p := newMembershipSyncPair(t)
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID); err != nil {
		t.Fatal(err)
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("the pulling member is not a member of its own group")
	}

	// A peer that answers every membership request with a refusal plus a
	// removal signed by a stranger.
	hostileIdentity := mustIdentity(t)
	hostileHost, _, err := NewHost(p.ctx, hostileIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer hostileHost.Close()
	stranger := mustIdentity(t)
	subject := mustNode(t, p.member)
	bogus, err := membership.SignRecord(stranger, membership.Record{
		Version:   membership.Version,
		GroupID:   p.groupID,
		Kind:      membership.KindRemove,
		Actor:     mustNode(t, stranger),
		Subject:   entmoot.NodeInfo{MemberID: subject.MemberID},
		Timestamp: client.Canonical().Timestamp + 9_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	hostileHost.SetStreamHandler(MembershipProtocol, func(stream network.Stream) {
		defer stream.Close()
		var request MembershipSyncRequest
		if err := decodeJSONLimit(stream, maxSyncRequestBytes, &request); err != nil {
			return
		}
		_ = encodeJSONLimit(stream, MembershipSyncResponse{
			Version:   1,
			RequestID: request.RequestID,
			GroupID:   request.GroupID,
			Error:     SyncNotMember,
			Records:   []membership.Record{bogus},
		}, maxMembershipResponse)
	})

	hostile := peer.AddrInfo{ID: hostileHost.ID(), Addrs: hostileHost.Addrs()}
	_, _, _, err = FetchMembership(p.ctx, p.clientHost, hostile, client, p.clientMemberID)
	if errors.Is(err, ErrRemoved) {
		t.Fatal("a hostile peer evicted this node with a record that changes nothing")
	}
	if err == nil {
		t.Fatal("a refusal was reported as a successful pull")
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("the node dropped its own membership on a stranger's word")
	}
}

// The order an eviction actually happens in: a delegated admin removes a
// member, and the founder then removes the admin. The removed member must
// still be told why it is refused — the record that removed it is the record
// that removed it, whatever became of its author afterwards.
func TestRemovedMemberIsToldEvenAfterItsRemoverLosesAdmin(t *testing.T) {
	adminIdentity := mustIdentity(t)
	p := newMembershipSyncPair(t, adminIdentity)
	client := p.adoptClient(t)
	if _, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID); err != nil {
		t.Fatal(err)
	}

	admin := mustNode(t, adminIdentity)
	policy := p.group.Policy()
	policy.Admins = membership.SortAdmins([]entmoot.MemberID{*admin.MemberID})
	if _, err := p.group.SignRecord(p.founder, membership.Record{
		Kind:   membership.KindPolicy,
		Policy: &policy,
	}); err != nil {
		t.Fatal(err)
	}
	if !p.group.CanAdminister(*admin.MemberID) {
		t.Fatal("the grant did not take effect")
	}
	if _, err := p.group.SignRecord(adminIdentity, membership.Record{
		Kind:    membership.KindRemove,
		Subject: mustNode(t, p.member),
	}); err != nil {
		t.Fatal(err)
	}
	if p.group.IsMemberID(p.clientMemberID) {
		t.Fatal("the admin's removal did not take effect")
	}
	// And now the admin itself goes.
	if _, err := p.group.SignRecord(p.founder, membership.Record{
		Kind:    membership.KindRemove,
		Subject: admin,
	}); err != nil {
		t.Fatal(err)
	}
	if p.group.CanAdminister(*admin.MemberID) {
		t.Fatal("the removed admin still holds authority")
	}

	_, _, _, err := FetchMembership(p.ctx, p.clientHost, p.remote, client, p.clientMemberID)
	if !errors.Is(err, ErrRemoved) {
		t.Fatalf("the removed member was not told why it is refused: %v", err)
	}
	if client.IsMemberID(p.clientMemberID) {
		t.Fatal("the member still counts itself in after learning of its removal")
	}
}
