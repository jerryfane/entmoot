package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
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

// A seal vouches for what the other members hold. A founder that reaches none
// of them - every addressable member refuses the dial, as when the founder is
// the one partitioned off - may be the node that is behind, so those rounds
// count for nothing and it does not seal however long it waits. Once a member
// answers again, even only in part, the rounds count and the deadline
// applies as usual.
func TestIsolatedFounderDoesNotSealUntilAMemberAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	holder, holderInfo := mustDaemonIdentity(t)
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
	for range sealDeadlineRounds + 2 {
		runtime.syncMembership(ctx, session)
	}
	ageWaitingSeals(session, 10*sealDeadline)
	for range sealDeadlineRounds + 2 {
		runtime.syncMembership(ctx, session)
	}
	if session.group.Canonical().ID != base.ID {
		t.Fatal("a founder that reached no member sealed its own view")
	}

	// The member is back, answering every pull only in part: rounds reach it
	// now, so the long-passed deadline applies once enough of them have.
	back, _, err := libp2ptransport.NewHost(ctx, member, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer back.Close()
	answerIncomplete(back)
	h.Peerstore().AddAddrs(back.ID(), back.Addrs(), peerstore.PermanentAddrTTL)
	for range sealDeadlineRounds - 1 {
		runtime.syncMembership(ctx, session)
	}
	if session.group.Canonical().ID != base.ID {
		t.Fatal("the founder sealed before enough rounds reached a member")
	}
	runtime.syncMembership(ctx, session)
	if session.group.Canonical().ID == base.ID {
		t.Fatal("the founder did not seal once rounds reached a member again")
	}
	backdated := mustSignedJoinAt(t, gid, holder, invite, base.Timestamp+1)
	if _, err := session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join dated before the revoke was not refused as stale: %v", err)
	}
}

// ageWaitingSeals moves every waiting seal's start back by d, as if d had
// passed since a round first found it. Only time passes: a round that
// reached nobody still does not count.
func ageWaitingSeals(session *groupSession, d time.Duration) {
	session.seal.mu.Lock()
	defer session.seal.mu.Unlock()
	for i := range session.seal.waiting {
		session.seal.waiting[i].since = session.seal.waiting[i].since.Add(-d)
	}
}

// allowSealNow lifts minSealInterval for the next round.
func allowSealNow(session *groupSession) {
	session.seal.mu.Lock()
	defer session.seal.mu.Unlock()
	session.seal.last = time.Now().Add(-minSealInterval)
}

// answerIncomplete makes a host answer every membership pull with nothing,
// claiming there is more.
func answerIncomplete(h host.Host) {
	h.SetStreamHandler(libp2ptransport.MembershipProtocol, func(stream network.Stream) {
		defer stream.Close()
		var request libp2ptransport.MembershipSyncRequest
		if err := json.NewDecoder(stream).Decode(&request); err != nil {
			return
		}
		_ = json.NewEncoder(stream).Encode(libp2ptransport.MembershipSyncResponse{
			Version: 1, RequestID: request.RequestID, GroupID: request.GroupID, Complete: false,
		})
	})
}

// sealTestGroup is a founder's group driven round by round, without a
// network, for the rules of when a seal is signed.
type sealTestGroup struct {
	t        *testing.T
	founder  *keystore.Identity
	group    *membership.Group
	runtime  *groupRuntime
	session  *groupSession
	revoked  byte
	reached  sealRound
	unsynced sealRound
}

func newSealTestGroup(t *testing.T, seed byte) *sealTestGroup {
	t.Helper()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(seed)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	t.Cleanup(func() { mustCloseGroup(t, group) })
	return &sealTestGroup{
		t: t, founder: founder, group: group,
		runtime:  &groupRuntime{identity: founder, binding: libp2ptransport.Binding{MemberID: *founderInfo.MemberID}, logger: slog.Default()},
		session:  &groupSession{groupID: gid, group: group},
		reached:  sealRound{reached: true, synced: true},
		unsynced: sealRound{reached: true, lagging: []string{"lagging-peer"}},
	}
}

// revoke signs a revocation, an authority change, and returns its timestamp.
func (g *sealTestGroup) revoke() int64 {
	g.t.Helper()
	time.Sleep(2 * time.Millisecond)
	g.revoked++
	var nonce [32]byte
	nonce[0] = g.revoked
	record, err := g.group.SignRecord(g.founder, membership.Record{Kind: membership.KindRevokeInvite, InviteNonce: nonce})
	if err != nil {
		g.t.Fatal(err)
	}
	return record.Timestamp
}

func (g *sealTestGroup) round(round sealRound) { g.runtime.sealAuthorityChanges(g.session, round) }
func (g *sealTestGroup) sequence() uint64      { return g.group.Canonical().Sequence }

// The seal waits for a synced round that starts after the change was seen,
// so records dated before the change have a round to arrive, but not past
// both of its bounds, counting only rounds that reached another member; only
// the founder seals; and it signs at most one seal per minSealInterval.
func TestSealWaitsForASyncedRoundAndIsBoundedAndRateLimited(t *testing.T) {
	g := newSealTestGroup(t, 0xa6)
	_, adminInfo := mustDaemonIdentity(t)
	admin := &groupRuntime{identity: g.founder, binding: libp2ptransport.Binding{MemberID: *adminInfo.MemberID}, logger: slog.Default()}
	adminSession := &groupSession{groupID: g.session.groupID, group: g.group}

	g.revoke()
	g.round(g.reached)
	if g.sequence() != 0 {
		t.Fatal("sealed on the round that first saw the change")
	}
	for range sealDeadlineRounds + 1 {
		g.round(g.unsynced)
	}
	if g.sequence() != 0 {
		t.Fatal("sealed on rounds that did not sync, before sealDeadline passed")
	}
	for range 3 {
		admin.sealAuthorityChanges(adminSession, g.reached)
	}
	if g.sequence() != 0 {
		t.Fatal("a node other than the founder sealed")
	}
	g.round(g.reached)
	if g.sequence() != 1 {
		t.Fatalf("not sealed on the next synced round: sequence %d", g.sequence())
	}

	// sealDeadline alone does not force a seal: the rounds must pass too.
	allowSealNow(g.session)
	g.revoke()
	g.round(g.unsynced)
	ageWaitingSeals(g.session, sealDeadline)
	for range sealDeadlineRounds - 1 {
		g.round(g.unsynced)
	}
	if g.sequence() != 1 {
		t.Fatal("forced a seal before sealDeadlineRounds rounds")
	}
	g.round(g.unsynced)
	if g.sequence() != 2 {
		t.Fatalf("an overdue seal was not forced: sequence %d", g.sequence())
	}

	// Rounds that reached nobody do not count towards the deadline.
	allowSealNow(g.session)
	g.revoke()
	g.round(sealRound{})
	ageWaitingSeals(g.session, 10*sealDeadline)
	for range 3 * sealDeadlineRounds {
		g.round(sealRound{})
	}
	if g.sequence() != 2 {
		t.Fatal("rounds that reached no member forced a seal")
	}
	for range sealDeadlineRounds {
		g.round(g.unsynced)
	}
	if g.sequence() != 3 {
		t.Fatalf("an overdue seal was not forced once rounds reached a member: sequence %d", g.sequence())
	}

	g.revoke()
	g.round(g.reached)
	g.round(g.reached)
	if g.sequence() != 3 {
		t.Fatal("sealed again inside minSealInterval")
	}
	allowSealNow(g.session)
	g.round(g.reached)
	if g.sequence() != 4 {
		t.Fatalf("not sealed once the interval passed: sequence %d", g.sequence())
	}
}

// Each change waits its own deadline. A change that arrives while an older
// one is being held back is not sealed when the older one falls due: the
// forced seal goes only through the older change, and the newer one gets a
// full wait of its own.
func TestANewerChangeWaitsItsOwnDeadline(t *testing.T) {
	g := newSealTestGroup(t, 0xa8)
	older := g.revoke()
	g.round(g.unsynced)
	for range sealDeadlineRounds - 2 {
		g.round(g.unsynced)
	}
	ageWaitingSeals(g.session, sealDeadline)
	// The newer change is found while the older one is still held back.
	newer := g.revoke()
	g.round(g.unsynced)
	if g.sequence() != 0 {
		t.Fatal("sealed before the older change was overdue")
	}

	g.round(g.unsynced)
	if g.sequence() != 1 {
		t.Fatalf("the older change was not sealed when overdue: sequence %d", g.sequence())
	}
	if got := g.group.Canonical().Timestamp; got != older {
		t.Fatalf("the forced seal reaches %d, want only the older change at %d (the newer is at %d)", got, older, newer)
	}
	if due, ok := g.group.SealDue(); !ok || due != newer {
		t.Fatalf("the newer change is no longer due: due=%t at %d", ok, due)
	}

	allowSealNow(g.session)
	for range sealDeadlineRounds + 2 {
		g.round(g.unsynced)
	}
	if g.sequence() != 1 {
		t.Fatal("the newer change was forced before its own deadline")
	}
	ageWaitingSeals(g.session, sealDeadline)
	g.round(g.unsynced)
	if g.sequence() != 2 || g.group.Canonical().Timestamp != newer {
		t.Fatalf("the newer change was not sealed at its own deadline: sequence %d", g.sequence())
	}
}

// hangingFixture is a founder's group in which some members accept every
// membership pull and never answer, and optionally one honest member, asked
// after all of them, that holds a join nobody else has - dated before a
// revoke the founder signed offline.
type hangingFixture struct {
	gid        entmoot.GroupID
	base       membership.Checkpoint
	revoked    entmoot.BootstrapCapability
	holder     *keystore.Identity
	runtime    *groupRuntime
	session    *groupSession
	host       host.Host
	honest     *groupRuntime
	honestSess *groupSession
	newcomer   entmoot.MemberID
}

func newHangingFixture(t *testing.T, ctx context.Context, seed byte, hanging int, withHonest bool) *hangingFixture {
	t.Helper()
	f := &hangingFixture{gid: daemonTestGroupID(seed)}
	founder, _ := mustDaemonIdentity(t)
	hangers := make([]*keystore.Identity, hanging)
	var last entmoot.MemberID
	for i := range hangers {
		identity, info := mustDaemonIdentity(t)
		hangers[i] = identity
		if bytes.Compare(info.MemberID[:], last[:]) > 0 {
			last = *info.MemberID
		}
	}
	founderRoot, honestRoot := t.TempDir(), t.TempDir()
	mustCreateGroup(t, founderRoot, f.gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, founderRoot, f.gid)
	f.base = group.Canonical()
	members := hangers
	var honest *keystore.Identity
	if withHonest {
		// Peers are asked in member-id order, so the honest member sorts
		// after every member that hangs.
		var honestInfo entmoot.NodeInfo
		honest, honestInfo = mustDaemonIdentity(t)
		for bytes.Compare(honestInfo.MemberID[:], last[:]) <= 0 {
			honest, honestInfo = mustDaemonIdentity(t)
		}
		members = append(append([]*keystore.Identity(nil), hangers...), honest)
	}
	var joins []membership.Record
	for _, identity := range members {
		info := mustDaemonNodeInfo(t, identity)
		joins = append(joins, mustJoinWithInvite(t, group, identity, mustDaemonInvite(t, group, founder, info, 1)))
	}
	newcomer, newcomerInfo := mustDaemonIdentity(t)
	f.newcomer = *newcomerInfo.MemberID
	newcomerInvite := mustDaemonInvite(t, group, founder, newcomerInfo, 1)
	holder, holderInfo := mustDaemonIdentity(t)
	f.holder = holder
	f.revoked = mustDaemonInvite(t, group, founder, holderInfo, 1)
	mustCloseGroup(t, group)

	if withHonest {
		honestGroup, err := membership.Adopt(honestRoot, f.base)
		if err != nil {
			t.Fatalf("Adopt: %v", err)
		}
		for _, join := range joins {
			if _, err := honestGroup.Apply(join); err != nil {
				t.Fatalf("seed the honest member: %v", err)
			}
		}
		if _, err := honestGroup.Apply(mustSignedJoinAt(t, f.gid, newcomer, newcomerInvite, time.Now().UnixMilli())); err != nil {
			t.Fatalf("apply the newcomer's join: %v", err)
		}
		mustCloseGroup(t, honestGroup)
	}
	time.Sleep(5 * time.Millisecond)
	mustRevokeOffline(t, founderRoot, f.gid, founder, f.revoked)

	f.runtime, f.session, f.host = startTestRuntime(t, ctx, founderRoot, founder, f.gid)
	t.Cleanup(func() { _ = f.host.Close() })
	t.Cleanup(f.runtime.Close)
	// The test drives the founder's rounds itself. Stop the session's own
	// loop, and wait out the round it starts with: no member has an address
	// yet, so that round asks nobody and only finds the revoke.
	f.session.cancel()
	for deadline := time.Now().Add(5 * time.Second); !f.session.seal.isArmed(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the founder's first round did not find the revoke")
		}
	}
	f.runtime.pullTimeout = time.Second
	f.runtime.roundTimeout = 1500 * time.Millisecond
	if withHonest {
		var honestHost host.Host
		f.honest, f.honestSess, honestHost = startTestRuntime(t, ctx, honestRoot, honest, f.gid)
		t.Cleanup(func() { _ = honestHost.Close() })
		t.Cleanup(f.honest.Close)
		f.host.Peerstore().AddAddrs(honestHost.ID(), honestHost.Addrs(), peerstore.PermanentAddrTTL)
		honestHost.Peerstore().AddAddrs(f.host.ID(), f.host.Addrs(), peerstore.PermanentAddrTTL)
	}
	release := make(chan struct{})
	for _, identity := range hangers {
		member, _, err := libp2ptransport.NewHost(ctx, identity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatalf("NewHost: %v", err)
		}
		t.Cleanup(func() { _ = member.Close() })
		member.SetStreamHandler(libp2ptransport.MembershipProtocol, func(stream network.Stream) {
			defer stream.Close()
			<-release
		})
		f.host.Peerstore().AddAddrs(member.ID(), member.Addrs(), peerstore.PermanentAddrTTL)
	}
	t.Cleanup(func() { close(release) })
	return f
}

// countRounds counts n rounds towards every waiting seal's deadline.
func countRounds(session *groupSession, n int) {
	session.seal.mu.Lock()
	defer session.seal.mu.Unlock()
	for i := range session.seal.waiting {
		session.seal.waiting[i].rounds = max(session.seal.waiting[i].rounds, n)
	}
}

// A round asks members maxMembershipSyncPeers at a time, each pull with its
// own timeout, and stops starting pulls after its round timeout, so members
// that accept a pull and never answer cost a round at most the two timeouts
// however many there are - not one pull timeout per eight members, which
// would stretch the seal's deadline with every hanging member. Such a round
// reached them, so it counts towards the deadline, and it is not
// synchronized.
func TestHangingMembersCostOneTimeoutPerRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const hanging = 4 * maxMembershipSyncPeers
	f := newHangingFixture(t, ctx, 0xa9, hanging, false)
	if !f.session.seal.isArmed() {
		t.Fatal("the revoke is not waiting to be sealed")
	}
	if got := len(f.runtime.membershipPeersUpTo(f.session, math.MaxInt)); got != hanging {
		t.Fatalf("the fixture has %d addressable members, want %d", got, hanging)
	}

	started := time.Now()
	f.runtime.syncMembership(ctx, f.session)
	took := time.Since(started)
	t.Logf("a round with %d hanging members, pullTimeout %s and roundTimeout %s took %s", hanging, f.runtime.pullTimeout, f.runtime.roundTimeout, took)
	// Without the round's own timeout this would take four pull timeouts.
	if limit := f.runtime.roundTimeout + f.runtime.pullTimeout + f.runtime.pullTimeout/2; took > limit {
		t.Fatalf("a round with %d hanging members took %s, want at most %s", hanging, took, limit)
	}
	f.session.seal.mu.Lock()
	rounds := f.session.seal.waiting[0].rounds
	f.session.seal.mu.Unlock()
	if rounds != 1 {
		t.Fatalf("the round counted %d towards the deadline, want 1: it reached the hanging members", rounds)
	}
	if f.session.group.Canonical().ID != f.base.ID {
		t.Fatal("a round in which every member hung was treated as synchronized")
	}
}

// More members hang than one round can get through, all asked before an
// honest member that holds a join the founder lacks, dated before the
// revoke. A seal forced on the deadline alone would leave that join out and
// cut the honest member off the chain. Instead the forced seal waits until
// every addressable member has been asked; rounds start where the previous
// one ran out of time and a member that hangs frees its slot after its own
// timeout, so the honest member is asked on the next round, and the seal
// carries its join.
func TestHangingMembersCannotKeepAnHonestMemberOutOfTheSeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const hanging = 2*maxMembershipSyncPeers + 4
	f := newHangingFixture(t, ctx, 0xaa, hanging, true)
	peers := f.runtime.membershipPeersUpTo(f.session, math.MaxInt)
	if len(peers) != hanging+1 {
		t.Fatalf("the fixture has %d addressable members, want %d", len(peers), hanging+1)
	}

	// The revoke is waiting. Make it overdue at once, so that only the rule
	// under test holds the seal back.
	ageWaitingSeals(f.session, sealDeadline)
	countRounds(f.session, sealDeadlineRounds)

	// This round asks two waves of hanging members and runs out of time
	// before the rest, the honest member among them.
	f.runtime.syncMembership(ctx, f.session)
	if f.session.group.IsMemberID(f.newcomer) {
		t.Fatal("the fixture let the round reach the honest member; it must run out of time first")
	}
	if f.session.group.Canonical().ID != f.base.ID {
		t.Fatal("forced a seal before the honest member had been asked")
	}

	f.runtime.syncMembership(ctx, f.session)
	if !f.session.group.IsMemberID(f.newcomer) {
		t.Fatal("the honest member was not asked on the next round")
	}
	sealed := f.session.group.Canonical()
	if sealed.ID == f.base.ID {
		t.Fatal("the seal was not forced once every member had been asked")
	}
	backdated := mustSignedJoinAt(t, f.gid, f.holder, f.revoked, f.base.Timestamp+1)
	if _, err := f.session.group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join dated before the revoke was not refused as stale: %v", err)
	}

	f.honest.syncMembership(ctx, f.honestSess)
	f.honest.syncMembership(ctx, f.honestSess)
	if got := f.honestSess.group.Canonical().ID; got != sealed.ID {
		t.Fatalf("the honest member is on checkpoint %s, the founder sealed %s: it was cut off the chain", got, sealed.ID)
	}
	if !f.honestSess.group.IsMemberID(f.newcomer) {
		t.Fatal("the honest member lost its join")
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
	answerIncomplete(hostile)

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
	ageWaitingSeals(session, sealDeadline)
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
