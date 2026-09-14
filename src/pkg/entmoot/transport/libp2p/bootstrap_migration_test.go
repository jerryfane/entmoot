package libp2ptransport

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// Pre-upgrade databases recorded spent single-use capabilities in
// used_bootstrap_capabilities. If the migration lost them, every invite
// already redeemed before the upgrade would become redeemable again.
func TestLegacyAdmissionRowsStayConsumedAfterMigration(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "bootstrap-admission.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE used_bootstrap_capabilities (
		group_id BLOB NOT NULL,
		nonce BLOB NOT NULL,
		state TEXT NOT NULL DEFAULT 'used',
		reserved_at_ms INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (group_id, nonce));`); err != nil {
		t.Fatal(err)
	}
	founder := mustIdentity(t)
	spent := legacyCapability(t, founder, 1)
	inflight := legacyCapability(t, founder, 2)
	if _, err := db.Exec(`INSERT INTO used_bootstrap_capabilities (group_id, nonce, state, reserved_at_ms) VALUES (?, ?, 'used', 0)`,
		spent.GroupID[:], spent.Nonce[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO used_bootstrap_capabilities (group_id, nonce, state, reserved_at_ms) VALUES (?, ?, 'reserved', ?)`,
		inflight.GroupID[:], inflight.Nonce[:], time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	admission, err := OpenPersistentBootstrapAdmission(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()

	legacy, err := tableExists(admission.db, "used_bootstrap_capabilities")
	if err != nil {
		t.Fatal(err)
	}
	if legacy {
		t.Fatal("legacy table survived the migration")
	}
	target, err := BindingFromPublicKey(spent.TargetPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := admission.Reserve(spent, target.PeerID, EnrollmentProtocol, now); err == nil {
		t.Fatal("a capability spent before the upgrade was reserved again")
	}
	// An unfinished reservation is not a commitment: after a restart it must
	// be usable again, exactly as the pre-upgrade store behaved once its
	// reservation went stale.
	inflightTarget, err := BindingFromPublicKey(inflight.TargetPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := admission.Reserve(inflight, inflightTarget.PeerID, EnrollmentProtocol, now); err != nil {
		t.Fatalf("an unfinished pre-upgrade reservation stayed blocked: %v", err)
	}
}

// Removal must void the invites that would readmit the removed identity, and
// report the bearer invites it cannot attribute.
func TestRemovalRevocationVoidsMemberInvitesAndReportsOpenOnes(t *testing.T) {
	admission, err := OpenPersistentBootstrapAdmission(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()
	founder := mustIdentity(t)
	member := mustIdentity(t)
	memberBinding, err := BindingFromPublicKey(member.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	targeted := legacyCapability(t, founder, 3)
	targeted.TargetPublicKey = member.PublicKey
	targeted.TargetMemberID = memberBinding.MemberID
	targeted.TargetPeerID = memberBinding.PeerID.String()
	if err := SignBootstrapCapability(founder, &targeted); err != nil {
		t.Fatal(err)
	}
	open := legacyCapability(t, founder, 4)
	open.GroupID = targeted.GroupID
	open.TargetPublicKey = nil
	open.TargetMemberID = entmoot.MemberID{}
	open.TargetPeerID = ""
	open.MaxUses = 3
	if err := SignBootstrapCapability(founder, &open); err != nil {
		t.Fatal(err)
	}
	for _, capability := range []BootstrapCapability{targeted, open} {
		if err := admission.RecordIssuedInvite(capability); err != nil {
			t.Fatal(err)
		}
	}

	revoked, err := admission.RevokeInvitesForMember(targeted.GroupID, memberBinding.MemberID)
	if err != nil || revoked != 1 {
		t.Fatalf("RevokeInvitesForMember = %d/%v, want 1/nil", revoked, err)
	}
	if err := admission.Reserve(targeted, memberBinding.PeerID, EnrollmentProtocol, time.Now()); err == nil {
		t.Fatal("the removed member's invite still admitted it")
	}
	live, err := admission.LiveOpenInvites(targeted.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].Nonce != open.Nonce {
		t.Fatalf("live open invites = %+v, want the one open invite", live)
	}
}

// legacyCapability builds a signed single-use capability for a scratch target.
func legacyCapability(t *testing.T, founder *keystore.Identity, seed byte) BootstrapCapability {
	t.Helper()
	target := mustIdentity(t)
	binding, err := BindingFromPublicKey(target.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	capability := BootstrapCapability{
		TargetPublicKey: target.PublicKey,
		TargetMemberID:  binding.MemberID,
		TargetPeerID:    binding.PeerID.String(),
		Founder:         mustNodeInfo(t, founder.PublicKey),
		AllowedPeerIDs:  []string{binding.PeerID.String()},
		IssuedAtMS:      now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:     now.Add(time.Hour).UnixMilli(),
	}
	capability.GroupID[0] = seed
	capability.Nonce[0] = seed
	if err := SignBootstrapCapability(founder, &capability); err != nil {
		t.Fatal(err)
	}
	return capability
}
