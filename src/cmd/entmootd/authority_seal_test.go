package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// mustSignedJoinAt signs a join dated by its signer, the way a holder of a
// withdrawn invite would date one before the change that withdrew it.
func mustSignedJoinAt(t *testing.T, gid entmoot.GroupID, joiner *keystore.Identity, capability entmoot.BootstrapCapability, atMS int64) membership.Record {
	t.Helper()
	info := mustDaemonNodeInfo(t, joiner)
	record, err := membership.SignRecord(joiner, membership.Record{
		GroupID: gid, Kind: membership.KindJoin, Actor: info, Subject: info,
		Invite: &capability, Timestamp: atMS,
	})
	if err != nil {
		t.Fatalf("sign join: %v", err)
	}
	return record
}

func mustRevokeOffline(t *testing.T, dataDir string, gid entmoot.GroupID, identity *keystore.Identity, capability entmoot.BootstrapCapability) {
	t.Helper()
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdInvite(daemonFlags(t, dataDir, identity), []string{"revoke", "-group", gid.String(),
			"-nonce", base64.StdEncoding.EncodeToString(capability.Nonce[:])})
	})
	if code != exitOK {
		t.Fatalf("invite revoke code = %d (%s)", code, stderr)
	}
}

func connectBothWays(t *testing.T, ctx context.Context, left, right host.Host) {
	t.Helper()
	if err := left.Connect(ctx, peer.AddrInfo{ID: right.ID(), Addrs: right.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := right.Connect(ctx, peer.AddrInfo{ID: left.ID(), Addrs: left.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
}

// `invite revoke` runs with the daemon stopped, so it cannot have pulled what
// the other members hold, and a checkpoint signed there would make every
// record they hold and it lacks stale. It signs only the record; the
// founder's daemon seals the revocation on its maintenance rounds, after
// which a join dated before the revoke is refused.
func TestOfflineRevokeIsSealedByTheFoundersDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	holder, holderInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xa1)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	invite := mustDaemonInvite(t, group, founder, holderInfo, 1)
	base := group.Canonical()
	mustCloseGroup(t, group)

	mustRevokeOffline(t, root, gid, founder, invite)
	group = mustOpenGroup(t, root, gid)
	revoked := group.IsInviteRevoked(invite.Nonce)
	canonical := group.Canonical().ID
	mustCloseGroup(t, group)
	if !revoked || canonical != base.ID {
		t.Fatalf("offline revoke: recorded=%t, checkpoint moved=%t; want recorded and not moved", revoked, canonical != base.ID)
	}

	runtime, session, h := startTestRuntime(t, ctx, root, founder, gid)
	defer h.Close()
	defer runtime.Close()
	runtime.syncMembership(ctx, session)
	runtime.syncMembership(ctx, session)
	if session.group.Canonical().ID == base.ID {
		t.Fatal("the founder's daemon did not seal the revoke")
	}
	backdated := mustSignedJoinAt(t, gid, holder, invite, base.Timestamp+1)
	if _, err := session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join dated before the revoke was not refused as stale: %v (member=%t)",
			err, session.group.IsMemberID(*holderInfo.MemberID))
	}
}

// The seal makes every record dated before it stale, so it must include the
// records the other members already hold. A member that accepted an honest
// join the founder has not seen yet, dated before a revoke the founder signs
// offline, must still converge on the founder's chain with that join in it,
// and a join dated before the revoke is refused on both nodes afterwards.
func TestFounderSealKeepsAnHonestJoinOnlyAMemberHeld(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0xa2)
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	honest, honestInfo := mustDaemonIdentity(t)
	holder, holderInfo := mustDaemonIdentity(t)
	founderRoot, memberRoot := t.TempDir(), t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())

	founderGroup := mustOpenGroup(t, founderRoot, gid)
	join := mustJoinWithInvite(t, founderGroup, member, mustDaemonInvite(t, founderGroup, founder, memberInfo, 1))
	honestInvite := mustDaemonInvite(t, founderGroup, founder, honestInfo, 1)
	revokedInvite := mustDaemonInvite(t, founderGroup, founder, holderInfo, 1)
	base := founderGroup.Canonical()
	mustCloseGroup(t, founderGroup)

	memberGroup, err := membership.Adopt(memberRoot, base)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	if _, err := memberGroup.Apply(join); err != nil {
		t.Fatalf("apply join on the member: %v", err)
	}
	// The honest joiner pushed its join to the member, not the founder.
	time.Sleep(5 * time.Millisecond)
	honestJoin := mustSignedJoinAt(t, gid, honest, honestInvite, time.Now().UnixMilli())
	if _, err := memberGroup.Apply(honestJoin); err != nil {
		t.Fatalf("apply honest join on the member: %v", err)
	}
	mustCloseGroup(t, memberGroup)
	time.Sleep(5 * time.Millisecond)
	mustRevokeOffline(t, founderRoot, gid, founder, revokedInvite)

	founderRuntime, founderSession, founderHost := startTestRuntime(t, ctx, founderRoot, founder, gid)
	defer founderHost.Close()
	defer founderRuntime.Close()
	memberRuntime, memberSession, memberHost := startTestRuntime(t, ctx, memberRoot, member, gid)
	defer memberHost.Close()
	defer memberRuntime.Close()
	connectBothWays(t, ctx, founderHost, memberHost)

	founderRuntime.syncMembership(ctx, founderSession)
	founderRuntime.syncMembership(ctx, founderSession)
	sealed := founderSession.group.Canonical()
	if sealed.ID == base.ID {
		t.Fatal("the founder's daemon did not seal the revoke")
	}
	if !founderSession.group.IsMemberID(*honestInfo.MemberID) {
		t.Fatal("the seal left out the honest join the member held")
	}
	// A pull applies checkpoints before records, so a member that lacks the
	// revoke refuses the seal on the first pull and adopts it on the next,
	// once it holds the revoke.
	memberRuntime.syncMembership(ctx, memberSession)
	memberRuntime.syncMembership(ctx, memberSession)
	if got := memberSession.group.Canonical().ID; got != sealed.ID {
		t.Fatalf("the member is on checkpoint %s, the founder sealed %s: the member split from the chain", got, sealed.ID)
	}
	if !memberSession.group.IsMemberID(*honestInfo.MemberID) {
		t.Fatal("the member lost the honest join")
	}

	backdated := mustSignedJoinAt(t, gid, holder, revokedInvite, honestJoin.Timestamp)
	for name, group := range map[string]*membership.Group{"founder": founderSession.group, "member": memberSession.group} {
		if _, err := group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
			t.Fatalf("%s applied a join dated before the revoke: %v", name, err)
		}
	}
}

// One signer seals, so no change is ever sealed twice into sibling
// checkpoints over different records. An admin that signs a revoke - here
// offline, where it could have sealed it itself - leaves sealing to the
// founder, whose daemon pulls the revoke and seals it; the admin then follows
// that single checkpoint.
func TestOnlyTheFounderSealsAnAdminsRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0xa3)
	founder, founderInfo := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	holder, holderInfo := mustDaemonIdentity(t)
	founderRoot, adminRoot := t.TempDir(), t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())

	// The admin is named by the canonical checkpoint, so it could sign one.
	founderGroup := mustOpenGroup(t, founderRoot, gid)
	genesis := founderGroup.Canonical()
	join := mustJoinWithInvite(t, founderGroup, admin, mustDaemonInvite(t, founderGroup, founder, adminInfo, 1))
	policy := founderGroup.Policy()
	policy.Admins = []entmoot.MemberID{*adminInfo.MemberID}
	grant, err := founderGroup.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy})
	if err != nil {
		t.Fatalf("grant admin: %v", err)
	}
	if _, _, err := founderGroup.SignCheckpoint(founder, true); err != nil {
		t.Fatalf("checkpoint the grant: %v", err)
	}
	base := founderGroup.Canonical()
	mustCloseGroup(t, founderGroup)

	adminGroup, err := membership.Adopt(adminRoot, genesis)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	for _, record := range []membership.Record{join, grant} {
		if _, err := adminGroup.Apply(record); err != nil {
			t.Fatalf("seed admin: %v", err)
		}
	}
	if _, err := adminGroup.ApplyCheckpoint(base); err != nil {
		t.Fatalf("admin adopts the base: %v", err)
	}
	invite := mustDaemonInvite(t, adminGroup, admin, holderInfo, 1)
	mustCloseGroup(t, adminGroup)
	mustRevokeOffline(t, adminRoot, gid, admin, invite)

	founderRuntime, founderSession, founderHost := startTestRuntime(t, ctx, founderRoot, founder, gid)
	defer founderHost.Close()
	defer founderRuntime.Close()
	adminRuntime, adminSession, adminHost := startTestRuntime(t, ctx, adminRoot, admin, gid)
	defer adminHost.Close()
	defer adminRuntime.Close()
	if got := adminSession.group.Canonical().ID; got != base.ID {
		t.Fatalf("the admin's offline revoke signed a checkpoint (%s)", got)
	}
	connectBothWays(t, ctx, founderHost, adminHost)

	adminRuntime.syncMembership(ctx, adminSession)
	adminRuntime.syncMembership(ctx, adminSession)
	if got := adminSession.group.Canonical().ID; got != base.ID {
		t.Fatalf("the admin's daemon sealed its own revoke (%s)", got)
	}
	founderRuntime.syncMembership(ctx, founderSession)
	founderRuntime.syncMembership(ctx, founderSession)
	adminRuntime.syncMembership(ctx, adminSession)

	for name, group := range map[string]*membership.Group{"founder": founderSession.group, "admin": adminSession.group} {
		chain := group.CheckpointsSince(base.Sequence)
		if len(chain) != 1 || chain[0].ID != group.Canonical().ID {
			t.Fatalf("%s holds %d checkpoints after the base, want the founder's one seal", name, len(chain))
		}
		if signer := chain[0].Signer; signer.MemberID == nil || *signer.MemberID != *founderInfo.MemberID {
			t.Fatalf("%s's seal was not signed by the founder", name)
		}
		backdated := mustSignedJoinAt(t, gid, holder, invite, grant.Timestamp+1)
		if _, err := group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
			t.Fatalf("%s applied a join dated before the revoke: %v", name, err)
		}
	}
	if founderSession.group.Canonical().ID != adminSession.group.Canonical().ID {
		t.Fatal("the founder and the admin are on different checkpoints")
	}
}

// The daemon removes members over IPC, which is also the path ESP's
// member_remove takes. Removing an admin signs only the record; the founder's
// next rounds seal it, after which a join through the admin's invite dated
// before the removal is refused.
func TestMemberRemoveOverIPCIsSealedByTheFoundersRounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	outsider, outsiderInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xa4)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	mustJoinWithInvite(t, group, admin, mustDaemonInvite(t, group, founder, adminInfo, 1))
	policy := group.Policy()
	policy.Admins = []entmoot.MemberID{*adminInfo.MemberID}
	grant, err := group.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy})
	if err != nil {
		t.Fatalf("grant admin: %v", err)
	}
	invite := mustDaemonInvite(t, group, admin, outsiderInfo, 1)
	mustCloseGroup(t, group)

	runtime, session, h := startTestRuntime(t, ctx, root, founder, gid)
	defer h.Close()
	defer runtime.Close()
	server := &ipcServer{
		memberID: *founderInfo.MemberID,
		peerID:   founderInfo.PeerID,
		identity: founder,
		dataDir:  root,
		runtime:  runtime,
	}
	client, daemon := net.Pipe()
	defer client.Close()
	go func() {
		defer daemon.Close()
		server.handleMemberRemove(ctx, daemon, &ipc.MemberRemoveReq{GroupID: gid, Target: adminInfo})
	}()
	_, decoded, err := ipc.ReadAndDecode(client)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if frame, ok := decoded.(*ipc.ErrorFrame); ok {
		t.Fatalf("member_remove refused: %s: %s", frame.Code, frame.Message)
	}
	if session.group.IsMemberID(*adminInfo.MemberID) {
		t.Fatal("member_remove left the admin in the group")
	}

	runtime.syncMembership(ctx, session)
	runtime.syncMembership(ctx, session)
	backdated := mustSignedJoinAt(t, gid, outsider, invite, grant.Timestamp+1)
	if _, err := session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join through the removed admin's invite dated before the removal was not refused: %v (member=%t)",
			err, session.group.IsMemberID(*outsiderInfo.MemberID))
	}
}

// A seal vouches for what the other members hold, so a round that reached
// none of them - every addressable member refused the dial - seals nothing
// until the seal is overdue; then it is signed on what the founder holds.
func TestFounderDefersTheSealWhileNoMemberAnswersUntilItIsOverdue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	_, holderInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xa5)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	invite := mustDaemonInvite(t, group, founder, holderInfo, 1)
	base := group.Canonical()
	mustCloseGroup(t, group)
	mustRevokeOffline(t, root, gid, founder, invite)

	// The member has an address, but nothing listens there any more.
	gone, _, err := libp2ptransport.NewHost(ctx, member, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	addrs := gone.Addrs()
	if err := gone.Close(); err != nil {
		t.Fatal(err)
	}

	runtime, session, h := startTestRuntime(t, ctx, root, founder, gid)
	defer h.Close()
	defer runtime.Close()
	h.Peerstore().AddAddrs(gone.ID(), addrs, peerstore.PermanentAddrTTL)
	if got := len(runtime.membershipPeers(session)); got != 1 {
		t.Fatalf("the fixture has %d addressable members, want the departed one", got)
	}
	for range sealDeadlineRounds {
		runtime.syncMembership(ctx, session)
	}
	if session.group.Canonical().ID != base.ID {
		t.Fatal("the founder sealed a revoke on a round that reached no member before it was overdue")
	}
	overdue(session)
	runtime.syncMembership(ctx, session)
	if session.group.Canonical().ID == base.ID {
		t.Fatal("the founder did not seal an overdue revoke")
	}
}

// overdue moves a session's armed seal back past both of its bounds, as if
// sealDeadline had passed since the round that armed it.
func overdue(session *groupSession) {
	session.seal.mu.Lock()
	defer session.seal.mu.Unlock()
	session.seal.armedAt = session.seal.armedAt.Add(-sealDeadline)
	session.seal.rounds = max(session.seal.rounds, sealDeadlineRounds)
}

// The seal waits for a synced round that starts after the change was seen,
// so records dated before the change have a round to arrive, but not past
// both of its bounds; and the founder signs at most one seal per
// minSealInterval however many changes arrive.
func TestSealWaitsForASyncedRoundAndIsBoundedAndRateLimited(t *testing.T) {
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xa6)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	defer mustCloseGroup(t, group)
	founderRuntime := &groupRuntime{identity: founder, binding: libp2ptransport.Binding{MemberID: *founderInfo.MemberID}, logger: slog.Default()}
	adminRuntime := &groupRuntime{identity: admin, binding: libp2ptransport.Binding{MemberID: *adminInfo.MemberID}, logger: slog.Default()}
	founderSession := &groupSession{groupID: gid, group: group}
	adminSession := &groupSession{groupID: gid, group: group}
	revoke := func() {
		t.Helper()
		var nonce [32]byte
		nonce[0] = byte(time.Now().UnixNano())
		if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindRevokeInvite, InviteNonce: nonce}); err != nil {
			t.Fatal(err)
		}
	}
	sequence := func() uint64 { return group.Canonical().Sequence }
	lagging := []string{"lagging-peer"}

	revoke()
	founderRuntime.sealAuthorityChanges(founderSession, true, nil)
	if sequence() != 0 {
		t.Fatal("sealed on the round that first saw the change")
	}
	for range sealDeadlineRounds + 1 {
		founderRuntime.sealAuthorityChanges(founderSession, false, lagging)
	}
	if sequence() != 0 {
		t.Fatal("sealed on rounds that did not sync, before sealDeadline passed")
	}
	for range 3 {
		adminRuntime.sealAuthorityChanges(adminSession, true, nil)
	}
	if sequence() != 0 {
		t.Fatal("a node other than the founder sealed")
	}
	founderRuntime.sealAuthorityChanges(founderSession, true, nil)
	if sequence() != 1 {
		t.Fatalf("not sealed on the next synced round: sequence %d", sequence())
	}

	// sealDeadline alone does not force a seal: the rounds must pass too, so
	// a founder whose daemon was asleep still gives members a round.
	founderSession.seal.mu.Lock()
	founderSession.seal.last = time.Now().Add(-minSealInterval)
	founderSession.seal.mu.Unlock()
	time.Sleep(2 * time.Millisecond)
	revoke()
	founderRuntime.sealAuthorityChanges(founderSession, false, lagging)
	founderSession.seal.mu.Lock()
	founderSession.seal.armedAt = founderSession.seal.armedAt.Add(-sealDeadline)
	founderSession.seal.mu.Unlock()
	for range sealDeadlineRounds - 1 {
		founderRuntime.sealAuthorityChanges(founderSession, false, lagging)
	}
	if sequence() != 1 {
		t.Fatal("forced a seal before sealDeadlineRounds rounds")
	}
	founderRuntime.sealAuthorityChanges(founderSession, false, lagging)
	if sequence() != 2 {
		t.Fatalf("an overdue seal was not forced: sequence %d", sequence())
	}

	time.Sleep(2 * time.Millisecond)
	revoke()
	founderRuntime.sealAuthorityChanges(founderSession, true, nil)
	founderRuntime.sealAuthorityChanges(founderSession, true, nil)
	if sequence() != 2 {
		t.Fatal("sealed again inside minSealInterval")
	}
	founderSession.seal.mu.Lock()
	founderSession.seal.last = time.Now().Add(-minSealInterval)
	founderSession.seal.mu.Unlock()
	founderRuntime.sealAuthorityChanges(founderSession, true, nil)
	if sequence() != 3 {
		t.Fatalf("not sealed once the interval passed: sequence %d", sequence())
	}
}

// syncBuffer is a log sink the daemon's goroutines may write while the test
// reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// A seal waits for a round that pulled everything from every member it
// reached, and a member decides what its own answers say. The member with
// the most reason to hold the seal off is an admin whose demotion it would
// make final: it stays a member, so it can answer every pull as incomplete
// while it signs joins dated before its demotion. It must not be able to
// delay the seal past its deadline, and the founder must name it when the
// seal is forced.
func TestAMemberAnsweringIncompleteCannotHoldOffTheSeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	outsider, outsiderInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xa7)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	mustJoinWithInvite(t, group, admin, mustDaemonInvite(t, group, founder, adminInfo, 1))
	policy := group.Policy()
	policy.Admins = []entmoot.MemberID{*adminInfo.MemberID}
	grant, err := group.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy})
	if err != nil {
		t.Fatalf("grant admin: %v", err)
	}
	invite := mustDaemonInvite(t, group, admin, outsiderInfo, 1)
	base := group.Canonical()
	mustCloseGroup(t, group)
	if code, _, stderr := runRosterCommand(t, daemonFlags(t, root, founder), "admin", "revoke", "-group", gid.String(), "-member", adminInfo.MemberID.String()); code != exitOK {
		t.Fatalf("roster admin revoke code = %d (%s)", code, stderr)
	}

	hostile, _, err := libp2ptransport.NewHost(ctx, admin, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer hostile.Close()
	hostile.SetStreamHandler(libp2ptransport.MembershipProtocol, func(stream network.Stream) {
		defer stream.Close()
		var request libp2ptransport.MembershipSyncRequest
		if err := json.NewDecoder(stream).Decode(&request); err != nil {
			return
		}
		_ = json.NewEncoder(stream).Encode(libp2ptransport.MembershipSyncResponse{
			Version: 1, RequestID: request.RequestID, GroupID: request.GroupID, Complete: false,
		})
	})

	logs := &syncBuffer{}
	runtime, session, h := startLoggedTestRuntime(t, ctx, root, founder, gid, slog.New(slog.NewTextHandler(logs, nil)))
	defer h.Close()
	defer runtime.Close()
	connectBothWays(t, ctx, h, hostile)

	for range sealDeadlineRounds + 1 {
		runtime.syncMembership(ctx, session)
	}
	if session.group.Canonical().ID != base.ID {
		t.Fatal("the founder sealed on rounds a member held incomplete, before the seal was overdue")
	}
	overdue(session)
	runtime.syncMembership(ctx, session)
	if session.group.Canonical().ID == base.ID {
		t.Fatal("a member answering every pull as incomplete held the seal off past its deadline")
	}
	backdated := mustSignedJoinAt(t, gid, outsider, invite, grant.Timestamp+1)
	if _, err := session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join through the demoted admin's invite dated before the demotion was not refused: %v (member=%t)",
			err, session.group.IsMemberID(*outsiderInfo.MemberID))
	}
	if out := logs.String(); !strings.Contains(out, "sealed without a fully synchronized round") || !strings.Contains(out, hostile.ID().String()) {
		t.Fatalf("the forced seal did not name the member that held it off:\n%s", out)
	}
}

// startLoggedTestRuntime is startTestRuntime with the daemon's logger given.
func startLoggedTestRuntime(t *testing.T, ctx context.Context, root string, identity *keystore.Identity, gid entmoot.GroupID, logger *slog.Logger) (*groupRuntime, *groupSession, host.Host) {
	t.Helper()
	h, binding, err := libp2ptransport.NewHost(ctx, identity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	messages, err := store.OpenSQLite(root)
	if err != nil {
		h.Close()
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = messages.Close() })
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: identity, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: h, Binding: binding, Mode: libp2ptransport.DirectConnectivity, Logger: logger,
	})
	if err != nil {
		h.Close()
		t.Fatalf("newGroupRuntime: %v", err)
	}
	session, _, err := runtime.AddLocalGroup(ctx, gid)
	if err != nil {
		runtime.Close()
		h.Close()
		t.Fatalf("AddLocalGroup: %v", err)
	}
	return runtime, session, h
}
