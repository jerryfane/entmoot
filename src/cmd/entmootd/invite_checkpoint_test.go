package main

import (
	"errors"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

func mustTestIdentity(t *testing.T) (*keystore.Identity, entmoot.NodeInfo, libp2ptransport.Binding) {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := libp2ptransport.BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	info := entmoot.NodeInfo{
		MemberID: &binding.MemberID, PeerID: binding.PeerID.String(),
		EntmootPubKey: identity.PublicKey,
	}
	return identity, info, binding
}

// Invites are handed out before anyone joins, so every outstanding invite
// names a checkpoint that the first joiner then supersedes. Enrollment must
// accept any checkpoint on this group's chain, and only reject one that is not
// on it at all.
func TestEnrollmentAcceptsSupersededInviteCheckpoint(t *testing.T) {
	founderIdentity, founder, founderBinding := mustTestIdentity(t)
	_, first, _ := mustTestIdentity(t)
	_, second, _ := mustTestIdentity(t)
	_, stranger, _ := mustTestIdentity(t)
	groupID := entmoot.GroupID{11}
	groupRoster := roster.New(groupID)
	if err := groupRoster.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	// Both invites are issued at the genesis head, as a founder handing out
	// several invites at once would do.
	inviteHead := groupRoster.Head()

	runtime := &groupRuntime{
		identity: founderIdentity, binding: founderBinding,
		sessions: map[entmoot.GroupID]*groupSession{groupID: {groupID: groupID, roster: groupRoster}},
	}
	invite := func(head entmoot.RosterEntryID) entmoot.BootstrapCapability {
		return entmoot.BootstrapCapability{GroupID: groupID, Founder: founder, RosterHead: head}
	}

	if _, err := runtime.enroll(nil, invite(inviteHead), first); err != nil {
		t.Fatalf("first joiner refused: %v", err)
	}
	if !groupRoster.IsMemberID(*first.MemberID) {
		t.Fatal("first joiner is not a member")
	}
	if groupRoster.Head() == inviteHead {
		t.Fatal("first enrollment did not advance the roster head")
	}
	if _, err := runtime.enroll(nil, invite(inviteHead), second); err != nil {
		t.Fatalf("second invite issued at the same head was refused: %v", err)
	}
	if !groupRoster.IsMemberID(*second.MemberID) {
		t.Fatal("second joiner is not a member")
	}

	var foreign entmoot.RosterEntryID
	foreign[0] = 0xAB
	_, err := runtime.enroll(nil, invite(foreign), stranger)
	if err == nil {
		t.Fatal("invite naming a checkpoint outside this chain was accepted")
	}
	var rejection *libp2ptransport.EnrollmentRejection
	if !errors.As(err, &rejection) {
		t.Fatalf("error %v is not a typed enrollment rejection", err)
	}
	if rejection.Code != libp2ptransport.EnrollRejectUnknownCheckpoint || !strings.Contains(rejection.Detail, "roster chain") {
		t.Fatalf("rejection = %+v, want unknown checkpoint with a reason", rejection)
	}
	if groupRoster.IsMemberID(*stranger.MemberID) {
		t.Fatal("rejected applicant was added to the roster")
	}
}
