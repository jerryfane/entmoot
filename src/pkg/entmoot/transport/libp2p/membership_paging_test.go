package libp2ptransport

import (
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
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
		t.Skip("the store refused the record outright, so there is nothing to spoof")
	}
	if !client.IsMemberID(p.clientMemberID) {
		t.Fatal("a stranger's removal took effect")
	}
}
