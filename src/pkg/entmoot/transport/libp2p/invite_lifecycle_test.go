package libp2ptransport

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// inviteFixture is a founder enrollment server plus an admission store, so
// tests exercise the real reserve/commit/revoke paths over a real stream.
type inviteFixture struct {
	ctx        context.Context
	founder    *keystore.Identity
	founderHos host.Host
	remote     peer.AddrInfo
	admission  *PersistentBootstrapAdmission
	groupID    entmoot.GroupID
	admitted   []entmoot.MemberID
	failNext   bool
}

func newInviteFixture(t *testing.T) *inviteFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	founder := mustIdentity(t)
	founderHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { founderHost.Close() })
	admission, err := OpenPersistentBootstrapAdmission(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admission.Close() })
	f := &inviteFixture{
		ctx:        ctx,
		founder:    founder,
		founderHos: founderHost,
		remote:     peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()},
		admission:  admission,
		groupID:    entmoot.GroupID{9},
	}
	server := EnrollmentServer{
		Admission: admission.BootstrapAdmission,
		Enroll: func(_ context.Context, _ BootstrapCapability, applicant entmoot.NodeInfo) (EnrollmentResponse, error) {
			if f.failNext {
				f.failNext = false
				return EnrollmentResponse{}, RejectEnrollment(EnrollRejectUnknownCheckpoint, "invite checkpoint is not on this group's roster chain")
			}
			f.admitted = append(f.admitted, *applicant.MemberID)
			return EnrollmentResponse{}, nil
		},
	}
	if err := server.Install(founderHost); err != nil {
		t.Fatal(err)
	}
	return f
}

// invite mints a signed capability. An empty target means an open invite.
func (f *inviteFixture) invite(t *testing.T, target *keystore.Identity, maxUses int) BootstrapCapability {
	t.Helper()
	now := time.Now()
	capability := BootstrapCapability{
		GroupID:        f.groupID,
		Founder:        mustNodeInfo(t, f.founder.PublicKey),
		AllowedPeerIDs: []string{f.founderHos.ID().String()},
		MaxUses:        maxUses,
		IssuedAtMS:     now.Add(-time.Minute).UnixMilli(),
		ExpiresAtMS:    now.Add(time.Hour).UnixMilli(),
	}
	if target != nil {
		binding, err := BindingFromPublicKey(target.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		capability.TargetPublicKey = target.PublicKey
		capability.TargetMemberID = binding.MemberID
		capability.TargetPeerID = binding.PeerID.String()
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		t.Fatal(err)
	}
	if err := SignBootstrapCapability(f.founder, &capability); err != nil {
		t.Fatal(err)
	}
	if err := f.admission.RecordIssuedInvite(capability); err != nil {
		t.Fatal(err)
	}
	return capability
}

// join runs one real enrollment as a fresh joining identity.
func (f *inviteFixture) join(t *testing.T, applicant *keystore.Identity, capability BootstrapCapability) error {
	t.Helper()
	applicantHost, _, err := NewHost(f.ctx, applicant, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer applicantHost.Close()
	_, err = Enroll(f.ctx, applicantHost, f.remote, capability, applicant.PublicKey)
	return err
}

// A multi-use invite is what lets an operator hand one link to a small team.
func TestMultiUseInviteAdmitsDistinctPeersUpToItsLimit(t *testing.T) {
	f := newInviteFixture(t)
	capability := f.invite(t, nil, 2)
	for _, applicant := range []*keystore.Identity{mustIdentity(t), mustIdentity(t)} {
		if err := f.join(t, applicant, capability); err != nil {
			t.Fatalf("multi-use invite refused an applicant within its limit: %v", err)
		}
	}
	err := f.join(t, mustIdentity(t), capability)
	if err == nil {
		t.Fatal("multi-use invite admitted more identities than its limit")
	}
	if !strings.Contains(err.Error(), EnrollRejectCapability) {
		t.Fatalf("exhausted invite error = %v, want %s", err, EnrollRejectCapability)
	}
	if len(f.admitted) != 2 {
		t.Fatalf("admitted %d identities, want 2", len(f.admitted))
	}
}

// Single use stays the default: an invite without MaxUses admits one identity.
func TestInviteWithoutMaxUsesAdmitsOneIdentity(t *testing.T) {
	f := newInviteFixture(t)
	capability := f.invite(t, nil, 0)
	if err := f.join(t, mustIdentity(t), capability); err != nil {
		t.Fatal(err)
	}
	if err := f.join(t, mustIdentity(t), capability); err == nil {
		t.Fatal("default invite admitted a second identity")
	}
}

// A target-bound invite is not transferable, so the same link cannot be handed
// to someone else.
func TestTargetBoundInviteRefusesAnotherApplicant(t *testing.T) {
	f := newInviteFixture(t)
	target := mustIdentity(t)
	capability := f.invite(t, target, 1)
	err := f.join(t, mustIdentity(t), capability)
	if err == nil {
		t.Fatal("target-bound invite admitted a different identity")
	}
	if !strings.Contains(err.Error(), EnrollRejectApplicant) && !strings.Contains(err.Error(), EnrollRejectCapability) {
		t.Fatalf("stranger error = %v, want an applicant or capability rejection", err)
	}
	if err := f.join(t, target, capability); err != nil {
		t.Fatalf("target-bound invite refused its own target: %v", err)
	}
}

// Revocation is the missing lever: an invite can be withdrawn before expiry,
// with uses still remaining.
func TestRevokedInviteStopsRemainingUses(t *testing.T) {
	f := newInviteFixture(t)
	capability := f.invite(t, nil, 3)
	if err := f.join(t, mustIdentity(t), capability); err != nil {
		t.Fatal(err)
	}
	revoked, err := f.admission.RevokeInvite(capability.GroupID, capability.Nonce)
	if err != nil || !revoked {
		t.Fatalf("RevokeInvite revoked/err = %v/%v", revoked, err)
	}
	err = f.join(t, mustIdentity(t), capability)
	if err == nil {
		t.Fatal("revoked invite still admitted an identity")
	}
	if !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("revoked invite error = %v, want a revocation reason", err)
	}
	records, err := f.admission.ListInvites(&capability.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].UsesCommitted != 1 || records[0].MaxUses != 3 || records[0].RevokedAtMS == 0 {
		t.Fatalf("invite records = %+v", records)
	}
}

// Revoking an invite whose file was lost must still work, so a leaked link is
// never a dead end.
func TestRevokeWorksForUnrecordedInvite(t *testing.T) {
	f := newInviteFixture(t)
	capability := BootstrapCapability{GroupID: f.groupID}
	capability.Nonce[0] = 42
	revoked, err := f.admission.RevokeInvite(capability.GroupID, capability.Nonce)
	if err != nil || !revoked {
		t.Fatalf("first revoke = %v/%v", revoked, err)
	}
	again, err := f.admission.RevokeInvite(capability.GroupID, capability.Nonce)
	if err != nil || again {
		t.Fatalf("second revoke = %v/%v, want false/nil", again, err)
	}
}

// A rejection the applicant could fix must not consume a use, and it must say
// what was wrong instead of one opaque failure code.
func TestFailedEnrollmentKeepsUseAndReportsReason(t *testing.T) {
	f := newInviteFixture(t)
	capability := f.invite(t, nil, 1)
	applicant := mustIdentity(t)
	f.failNext = true
	err := f.join(t, applicant, capability)
	if err == nil {
		t.Fatal("rejected enrollment reported success")
	}
	if !strings.Contains(err.Error(), EnrollRejectUnknownCheckpoint) || !strings.Contains(err.Error(), "roster chain") {
		t.Fatalf("rejection error = %v, want the checkpoint code and reason", err)
	}
	if err := f.join(t, applicant, capability); err != nil {
		t.Fatalf("retry after a failed enrollment was refused: %v", err)
	}
}
