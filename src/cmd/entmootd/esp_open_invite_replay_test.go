package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/store/storetest"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
)

// replayFixture runs the open-invite redeem path end to end: the ESP HTTP
// handler and executor, the real daemon (ipcServer, group runtime, libp2p
// host, invite ledger) of a group the founder created, and between the two a
// control-socket relay that can lose the daemon's answer to one refresh.
type replayFixture struct {
	t       *testing.T
	ctx     context.Context
	founder *keystore.Identity
	// admin is the second member startReplayFixture adds withPeer: a
	// delegated admin whose node is unreachable.
	admin   *keystore.Identity
	gid     entmoot.GroupID
	host    host.Host
	runtime *groupRuntime
	session *groupSession
	server  *ipcServer
	state   *esphttp.SQLiteStateStore
	handler http.Handler
	// loseRefreshAnswer makes the relay drop the daemon's answer to the next
	// invite_refresh after the daemon has acted on it.
	loseRefreshAnswer atomic.Bool
}

type replayRedemption struct {
	UseCount   int                         `json:"use_count"`
	Capability entmoot.BootstrapCapability `json:"capability"`
}

// startReplayFixture starts the fixture. withPeer adds a second member, a
// delegated admin with no address, so the founder is not the group's only
// member and its daemon never seals: nothing may rely on the seal arriving.
func startReplayFixture(t *testing.T, gidSeed byte, withPeer bool) *replayFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	f := &replayFixture{t: t, ctx: ctx, founder: founder, gid: testESPGroupID(gidSeed)}
	mustCreateGroup(t, root, f.gid, founder, membership.DefaultPolicy())
	if withPeer {
		group := mustOpenGroup(t, root, f.gid)
		admin, adminInfo := mustDaemonIdentity(t)
		mustJoinWithInvite(t, group, admin, mustDaemonInvite(t, group, founder, adminInfo, 1))
		policy := group.Policy()
		policy.Admins = withAdmin(policy.Admins, *adminInfo.MemberID)
		if _, err := group.SignRecord(founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy}); err != nil {
			t.Fatalf("grant admin: %v", err)
		}
		mustCloseGroup(t, group)
		f.admin = admin
	}

	h, hostBinding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	f.host = h
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = messages.Close() })
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: h, Binding: hostBinding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	t.Cleanup(runtime.Close)
	if _, _, err := runtime.AddLocalGroup(ctx, f.gid); err != nil {
		t.Fatalf("AddLocalGroup: %v", err)
	}
	session, ok := runtime.Get(f.gid)
	if !ok {
		t.Fatal("group session missing")
	}
	f.runtime, f.session = runtime, session
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	f.state, err = esphttp.OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	t.Cleanup(func() { _ = f.state.Close() })
	f.server = &ipcServer{
		memberID: founderBinding.MemberID, peerID: founderBinding.PeerID.String(),
		identity: founder, dataDir: root, runtime: runtime, metadataStore: f.state,
	}
	daemonSock := testUnixSocketPath(t)
	serveUnix(t, daemonSock, func(conn net.Conn) { f.server.handleConn(ctx, conn) })
	relaySock := testUnixSocketPath(t)
	serveUnix(t, relaySock, func(conn net.Conn) {
		msgType, body, err := ipc.ReadFrame(conn)
		if err != nil {
			return
		}
		daemon, err := net.Dial("unix", daemonSock)
		if err != nil {
			return
		}
		defer daemon.Close()
		if ipc.WriteFrame(daemon, msgType, body) != nil {
			return
		}
		answerType, answer, err := ipc.ReadFrame(daemon)
		if err != nil || (msgType == ipc.MsgInviteRefreshReq && f.loseRefreshAnswer.CompareAndSwap(true, false)) {
			return
		}
		_ = ipc.WriteFrame(conn, answerType, answer)
	})

	f.handler, err = esphttp.NewHandler(esphttp.Config{
		Token:      "replay-test",
		Service:    mailboxtest.New(t, storetest.New(t), nil),
		State:      f.state,
		Operations: espOperationExecutor{dataDir: root, socketPath: relaySock, timeout: 5 * time.Second, stateStore: f.state},
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// serveUnix answers each connection to sock with serve, one at a time, until
// the test ends.
func serveUnix(t *testing.T, sock string, serve func(net.Conn)) {
	t.Helper()
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			serve(conn)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
}

func (f *replayFixture) createInvite(token string, expiresAt time.Time) string {
	f.t.Helper()
	hash := esphttp.HashOpenInviteToken(token)
	if _, err := f.state.CreateOpenInvite(f.ctx, esphttp.OpenInviteRecord{
		TokenHash: hash, GroupID: f.gid, DeviceID: "device-1", MaxUses: 1, ExpiresAtMS: expiresAt.UnixMilli(),
	}); err != nil {
		f.t.Fatalf("CreateOpenInvite: %v", err)
	}
	return hash
}

func (f *replayFixture) redeem(token string, joiner *keystore.Identity) *httptest.ResponseRecorder {
	f.t.Helper()
	info := mustDaemonNodeInfo(f.t, joiner)
	body, err := json.Marshal(map[string]any{
		"member_id": info.MemberID.String(), "peer_id": info.PeerID, "entmoot_pubkey": info.EntmootPubKey,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/open-invites/"+token+"/redeem", bytes.NewReader(body))
	response := httptest.NewRecorder()
	f.handler.ServeHTTP(response, request)
	return response
}

func (f *replayFixture) redeemed(token string, joiner *keystore.Identity) (replayRedemption, []byte) {
	f.t.Helper()
	response := f.redeem(token, joiner)
	if response.Code != http.StatusOK {
		f.t.Fatalf("redeem: %d %s", response.Code, response.Body.String())
	}
	var got replayRedemption
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		f.t.Fatalf("redeem response: %v", err)
	}
	return got, response.Body.Bytes()
}

func (f *replayFixture) refused(token string, joiner *keystore.Identity, code string) {
	f.t.Helper()
	response := f.redeem(token, joiner)
	var got struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || response.Code != http.StatusConflict || got.Error.Code != code {
		f.t.Fatalf("redeem = %d %s, want 409 %s", response.Code, response.Body.String(), code)
	}
}

func (f *replayFixture) useCount(hash string) int {
	f.t.Helper()
	rec, ok, err := f.state.GetOpenInviteByTokenHash(f.ctx, hash)
	if err != nil || !ok {
		f.t.Fatalf("GetOpenInviteByTokenHash: ok=%t err=%v", ok, err)
	}
	return rec.UseCount
}

func (f *replayFixture) ledgerRows() int {
	f.t.Helper()
	records, err := f.runtime.invites.ListInvites(&f.gid)
	if err != nil {
		f.t.Fatalf("ListInvites: %v", err)
	}
	return len(records)
}

func (f *replayFixture) listenWebSocket() {
	f.t.Helper()
	if err := f.host.Network().Listen(multiaddr.StringCast("/ip4/127.0.0.1/tcp/0/ws")); err != nil {
		f.t.Fatalf("listen on WebSocket: %v", err)
	}
}

// admits reports whether the group would let capability's holder join now.
func (f *replayFixture) admits(capability entmoot.BootstrapCapability) bool {
	return f.session.group.CheckInvite(capability, time.Now().UnixMilli()) == nil
}

// expireStoredCapability rewrites the capability stored for redeemer's
// redemption of the invite with tokenHash so that it expires at expiresAtMS,
// signed again by the founder, as if it had been issued that long ago.
func (f *replayFixture) expireStoredCapability(tokenHash string, redeemer *keystore.Identity, issued entmoot.BootstrapCapability, expiresAtMS int64) entmoot.BootstrapCapability {
	f.t.Helper()
	key := mustDaemonNodeInfo(f.t, redeemer).MemberID.String()
	stored, ok, err := f.state.GetOpenInviteRedemption(f.ctx, tokenHash, key)
	if err != nil || !ok {
		f.t.Fatalf("GetOpenInviteRedemption: ok=%t err=%v", ok, err)
	}
	var result map[string]any
	if err := json.Unmarshal(stored.Result, &result); err != nil {
		f.t.Fatal(err)
	}
	expired := issued
	if expired.IssuedAtMS > expiresAtMS {
		expired.IssuedAtMS = expiresAtMS - time.Hour.Milliseconds()
	}
	expired.ExpiresAtMS = expiresAtMS
	if err := libp2ptransport.SignBootstrapCapability(f.founder, &expired); err != nil {
		f.t.Fatal(err)
	}
	result["capability"] = expired
	encoded, err := json.Marshal(result)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.state.CompleteOpenInviteRedemption(f.ctx, tokenHash, key, encoded, time.Now().UnixMilli()); err != nil {
		f.t.Fatalf("expire the stored capability: %v", err)
	}
	return expired
}

// removeMember removes member through the daemon's member_remove, the path
// the ESP and the CLI take while the daemon runs.
func (f *replayFixture) removeMember(member *keystore.Identity) *ipc.MemberRemoveResp {
	f.t.Helper()
	target := mustDaemonNodeInfo(f.t, member)
	client, daemon := net.Pipe()
	defer client.Close()
	go func() {
		defer daemon.Close()
		f.server.handleMemberRemove(f.ctx, daemon, &ipc.MemberRemoveReq{GroupID: f.gid, Target: target})
	}()
	_, decoded, err := ipc.ReadAndDecode(client)
	if err != nil {
		f.t.Fatalf("member_remove: %v", err)
	}
	resp, ok := decoded.(*ipc.MemberRemoveResp)
	if !ok {
		f.t.Fatalf("member_remove answered %#v", decoded)
	}
	return resp
}

// joinNow has member sign and apply a join with capability dated now, and
// reports whether it is a member afterwards.
func (f *replayFixture) joinNow(member *keystore.Identity, capability entmoot.BootstrapCapability) bool {
	f.t.Helper()
	_, _ = f.session.group.Apply(mustSignedJoinAt(f.t, f.gid, member, capability, time.Now().UnixMilli()))
	return f.session.group.IsMemberID(*mustDaemonNodeInfo(f.t, member).MemberID)
}

// removeAs has signer sign member's removal, dated at, and this node apply it
// as it applies a pulled or pushed record. It returns once the daemon has
// reconciled its invites with the result.
func (f *replayFixture) removeAs(signer, member *keystore.Identity, at int64) {
	f.t.Helper()
	f.applyAndSettle(f.signAs(signer, membership.Record{Kind: membership.KindRemove, Subject: mustDaemonNodeInfo(f.t, member), Timestamp: at}))
}

// signAs has signer sign rec for the fixture's group, as its own node would.
func (f *replayFixture) signAs(signer *keystore.Identity, rec membership.Record) membership.Record {
	f.t.Helper()
	rec.GroupID = f.gid
	rec.Actor = mustDaemonNodeInfo(f.t, signer)
	signed, err := membership.SignRecord(signer, rec)
	if err != nil {
		f.t.Fatalf("sign %s: %v", rec.Kind, err)
	}
	return signed
}

// applyAndSettle applies records here, in order, as pulled or pushed records
// are, and waits for the daemon to reconcile its invites with the result.
func (f *replayFixture) applyAndSettle(records ...membership.Record) {
	f.t.Helper()
	for _, rec := range records {
		if _, err := f.session.group.Apply(rec); err != nil {
			f.t.Fatalf("apply %s: %v", rec.Kind, err)
		}
	}
	f.session.reconciler.wait()
}

// countChanges has the group count the changes it reports, still waking the
// daemon's reconciler, and returns the counter. Every record the daemon signs
// in response is a change too, so the count says whether it signed anything.
func (f *replayFixture) countChanges() *atomic.Int32 {
	var count atomic.Int32
	reconciler := f.session.reconciler
	f.session.group.SetChangeHook(func() {
		count.Add(1)
		reconciler.signal()
	})
	return &count
}

// cannotRejoinButReinviteWorks is the outcome every removal case must end in:
// none of the invites this node issued member before its removal readmits
// it, and an invite issued to it now does.
func (f *replayFixture) cannotRejoinButReinviteWorks(member *keystore.Identity, before ...entmoot.BootstrapCapability) {
	f.t.Helper()
	for _, capability := range before {
		if f.admits(capability) {
			f.t.Fatal("an invite issued before the removal still admits the removed member")
		}
		if f.joinNow(member, capability) {
			f.t.Fatal("the removed member rejoined with an invite issued before its removal")
		}
	}
	time.Sleep(5 * time.Millisecond)
	reinvite := f.inviteTargeted(member)
	f.session.reconciler.signal()
	f.session.reconciler.wait()
	if !f.admits(reinvite) || !f.joinNow(member, reinvite) {
		f.t.Fatal("an invite issued after the removal does not readmit the member")
	}
	f.session.reconciler.wait()
	if !f.session.group.IsMemberID(*mustDaemonNodeInfo(f.t, member).MemberID) {
		f.t.Fatal("the readmitted member did not stay in")
	}
}

// inviteTargeted has the daemon issue a single-use invite to member through
// invite_create, so it is in the node's invite ledger.
func (f *replayFixture) inviteTargeted(member *keystore.Identity) entmoot.BootstrapCapability {
	f.t.Helper()
	client, daemon := net.Pipe()
	go func() {
		defer daemon.Close()
		f.server.handleInviteCreate(f.ctx, daemon, &ipc.InviteCreateReq{GroupID: f.gid, TargetPublicKey: member.PublicKey, MaxUses: 1})
	}()
	_, decoded, err := ipc.ReadAndDecode(client)
	_ = client.Close()
	created, ok := decoded.(*ipc.InviteCreateResp)
	if err != nil || !ok {
		f.t.Fatalf("invite_create: %#v %v", decoded, err)
	}
	return created.Capability
}

func hasWebSocket(capability entmoot.BootstrapCapability) bool {
	for _, address := range capability.AllowedMultiaddrs {
		if strings.Contains(address, "/ws/") {
			return true
		}
	}
	return false
}

func generateIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	identity, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

// TestOpenInviteReplayReplacesOnlyAStaleUnusedCapability drives repeat
// redemptions of ESP open invites through the HTTP handler, the executor and
// the real daemon mint. Replaying the stored result forever left an identity
// that redeemed before the node announced its WebSocket address holding a
// TCP-only grant no restricted cloud could use. Minting on every replay fixed
// that but let anyone holding a token and a public key sign without bound, and
// let a removed member mint its way back in through an exhausted link. A
// replay may be minted again only once per change, and only for a capability
// that never got anyone in.
func TestOpenInviteReplayReplacesOnlyAStaleUnusedCapability(t *testing.T) {
	f := startReplayFixture(t, 32, false)
	group := f.session.group

	// Two identities redeem while the node listens on TCP only: one never
	// joins, the other joins with its capability and is then removed.
	stuck, removed := generateIdentity(t), generateIdentity(t)
	link := f.createInvite("link", time.Now().Add(time.Hour))
	used := f.createInvite("used", time.Now().Add(time.Hour))
	first, firstBody := f.redeemed("link", stuck)
	if hasWebSocket(first.Capability) {
		t.Fatalf("first capability already carries a WebSocket address: %v", first.Capability.AllowedMultiaddrs)
	}
	removedGrant, removedBody := f.redeemed("used", removed)
	mustJoinWithInvite(t, group, removed, removedGrant.Capability)
	if err := applyRosterRemove(f.founder, group, mustDaemonNodeInfo(t, removed)); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	rows := f.ledgerRows()

	// Nothing changed: the stored bytes come back and nothing is minted.
	if _, body := f.redeemed("link", stuck); !bytes.Equal(body, firstBody) {
		t.Fatalf("an unchanged replay returned different bytes:\n%s\nwant\n%s", body, firstBody)
	}
	if got := f.ledgerRows(); got != rows {
		t.Fatalf("an unchanged replay minted: ledger rows %d, want %d", got, rows)
	}

	f.listenWebSocket()

	// The stuck identity gets one replacement carrying the new address.
	again, againBody := f.redeemed("link", stuck)
	if !hasWebSocket(again.Capability) || again.Capability.Nonce == first.Capability.Nonce {
		t.Fatalf("stale replay was not replaced: %v", again.Capability.AllowedMultiaddrs)
	}
	stuckBinding, err := libp2ptransport.BindingFromPublicKey(stuck.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := libp2ptransport.VerifyBootstrapCapability(again.Capability, stuckBinding.PeerID, time.Now()); err != nil {
		t.Fatalf("replacement does not verify for its redeemer: %v", err)
	}
	if !f.admits(again.Capability) {
		t.Fatal("group refuses the replacement")
	}
	// The replaced capability had not expired, so it was revoked before the
	// replacement left: the holder never carries two that admit it.
	if f.admits(first.Capability) || !group.IsInviteRevoked(first.Capability.Nonce) {
		t.Fatal("the replaced capability still admits alongside its replacement")
	}
	if again.UseCount != 1 || f.useCount(link) != 1 {
		t.Fatalf("replacement spent a use: response %d, stored %d, want 1", again.UseCount, f.useCount(link))
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("replacement left %d ledger rows, want %d", got, rows+1)
	}
	for range 3 {
		if _, body := f.redeemed("link", stuck); !bytes.Equal(body, againBody) {
			t.Fatalf("a replay after the replacement returned different bytes:\n%s\nwant\n%s", body, againBody)
		}
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("replays after the replacement minted: ledger rows %d, want %d", got, rows+1)
	}

	// Joining with the replacement and being removed leaves nothing to rejoin
	// with: the replay hands back the spent capability, not a new one.
	mustJoinWithInvite(t, group, stuck, again.Capability)
	if err := applyRosterRemove(f.founder, group, mustDaemonNodeInfo(t, stuck)); err != nil {
		t.Fatalf("remove stuck member: %v", err)
	}
	f.listenWebSocket()
	if _, body := f.redeemed("link", stuck); !bytes.Equal(body, againBody) {
		t.Fatalf("a removed member's replay after an address change returned different bytes:\n%s\nwant\n%s", body, againBody)
	}
	if f.admits(first.Capability) || f.admits(again.Capability) {
		t.Fatal("a removed member still holds a capability that admits it")
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("a removed member's replay minted: ledger rows %d, want %d", got, rows+1)
	}

	// A stored capability that has expired is replaced even when the
	// addresses have not moved, and the replacement is kept.
	aged := generateIdentity(t)
	agedLink := f.createInvite("aged", time.Now().Add(time.Hour))
	agedFirst, _ := f.redeemed("aged", aged)
	stale := f.expireStoredCapability(agedLink, aged, agedFirst.Capability, time.Now().Add(-time.Hour).UnixMilli())
	renewed, renewedBody := f.redeemed("aged", aged)
	if renewed.Capability.Nonce == stale.Nonce || renewed.Capability.ExpiresAtMS <= time.Now().UnixMilli() {
		t.Fatal("an expired stored capability was replayed")
	}
	if _, body := f.redeemed("aged", aged); !bytes.Equal(body, renewedBody) {
		t.Fatal("a replay after the renewal returned different bytes")
	}
	rows += 2 // the aged identity's first redemption and its one renewal

	// The removed member's capability got it in once; it gets the same bytes
	// back, not a fresh nonce to rejoin through an exhausted link.
	if _, body := f.redeemed("used", removed); !bytes.Equal(body, removedBody) {
		t.Fatalf("a removed member's replay returned different bytes:\n%s\nwant\n%s", body, removedBody)
	}
	if f.admits(removedGrant.Capability) {
		t.Fatal("the removed member's spent capability still admits")
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("the removed member's replay minted: ledger rows %d, want %d", got, rows+1)
	}
	if f.useCount(used) != 1 {
		t.Fatalf("the removed member's replay changed the use count to %d", f.useCount(used))
	}

	// A banned identity is never given a fresh nonce either.
	banned := generateIdentity(t)
	bannedLink := f.createInvite("banned", time.Now().Add(time.Hour))
	_, bannedBody := f.redeemed("banned", banned)
	if _, err := group.SignRecord(f.founder, membership.Record{Kind: membership.KindRemove, Subject: mustDaemonNodeInfo(t, banned), Banned: true}); err != nil {
		t.Fatalf("ban: %v", err)
	}
	rows = f.ledgerRows()
	f.listenWebSocket()
	if _, body := f.redeemed("banned", banned); !bytes.Equal(body, bannedBody) {
		t.Fatal("a banned identity's replay after an address change returned different bytes")
	}
	if got := f.ledgerRows(); got != rows || f.useCount(bannedLink) != 1 {
		t.Fatalf("a banned identity's replay minted: ledger rows %d, want %d", got, rows)
	}

	// The existing refusals hold.
	f.refused("link", generateIdentity(t), "open_invite_exhausted")
	if f.useCount(link) != 1 {
		t.Fatalf("refused stranger changed the use count to %d", f.useCount(link))
	}
	if _, _, err := f.state.RevokeOpenInvite(f.ctx, link, time.Now().UnixMilli()); err != nil {
		t.Fatalf("RevokeOpenInvite: %v", err)
	}
	f.refused("link", stuck, "open_invite_revoked")
	expiresAt := time.Now().Add(time.Second)
	f.createInvite("short", expiresAt)
	f.redeemed("short", stuck)
	time.Sleep(time.Until(expiresAt) + 10*time.Millisecond)
	f.refused("short", stuck, "open_invite_expired")
}

// TestOpenInviteRefreshSurvivesALostAnswerAndIsSealed loses the daemon's
// answer to a refresh after the daemon has revoked the old capability and
// issued its replacement. The ESP keeps the stored, now revoked, capability;
// before the replacement was linked to what it replaced, every later refresh
// was refused as revoked and the redeemer was stuck for good. The retry must
// be handed the replacement already issued, not a second one, so the redeemer
// holds exactly one capability that admits it. And the revocation is one the
// founder's daemon seals like any other: afterwards a join with the old
// capability, dated before the revocation, is refused as stale.
func TestOpenInviteRefreshSurvivesALostAnswerAndIsSealed(t *testing.T) {
	f := startReplayFixture(t, 34, false)
	group := f.session.group
	joiner := generateIdentity(t)
	joinerBinding, err := libp2ptransport.BindingFromPublicKey(joiner.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	link := f.createInvite("link", time.Now().Add(time.Hour))
	first, firstBody := f.redeemed("link", joiner)
	rows := f.ledgerRows()
	base := group.Canonical()
	time.Sleep(5 * time.Millisecond)
	beforeRevoke := time.Now().UnixMilli()
	time.Sleep(5 * time.Millisecond)

	f.listenWebSocket()
	f.loseRefreshAnswer.Store(true)
	if _, body := f.redeemed("link", joiner); !bytes.Equal(body, firstBody) {
		t.Fatalf("a refresh whose answer was lost did not fall back to the stored bytes:\n%s", body)
	}
	if f.loseRefreshAnswer.Load() {
		t.Fatal("the relay never saw a refresh to lose")
	}
	if !group.IsInviteRevoked(first.Capability.Nonce) || f.ledgerRows() != rows+1 {
		t.Fatalf("the daemon did not act on the lost refresh: revoked=%t ledger rows %d, want %d",
			group.IsInviteRevoked(first.Capability.Nonce), f.ledgerRows(), rows+1)
	}

	// The retry is handed the replacement the lost answer carried.
	again, againBody := f.redeemed("link", joiner)
	if again.Capability.Nonce == first.Capability.Nonce || !hasWebSocket(again.Capability) {
		t.Fatalf("the retry after a lost answer was not given the replacement: %v", again.Capability.AllowedMultiaddrs)
	}
	if err := libp2ptransport.VerifyBootstrapCapability(again.Capability, joinerBinding.PeerID, time.Now()); err != nil {
		t.Fatalf("replacement does not verify for its redeemer: %v", err)
	}
	if !f.admits(again.Capability) || f.admits(first.Capability) {
		t.Fatalf("want only the replacement to admit: replacement=%t replaced=%t", f.admits(again.Capability), f.admits(first.Capability))
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("the retry minted a second replacement: ledger rows %d, want %d", got, rows+1)
	}
	if again.UseCount != 1 || f.useCount(link) != 1 {
		t.Fatalf("the retry spent a use: response %d, stored %d", again.UseCount, f.useCount(link))
	}
	for range 3 {
		if _, body := f.redeemed("link", joiner); !bytes.Equal(body, againBody) {
			t.Fatalf("a replay after the recovered replacement returned different bytes:\n%s\nwant\n%s", body, againBody)
		}
	}
	if got := f.ledgerRows(); got != rows+1 {
		t.Fatalf("replays after the recovery minted: ledger rows %d, want %d", got, rows+1)
	}

	// The founder's maintenance rounds seal the revocation.
	f.runtime.syncMembership(f.ctx, f.session)
	f.runtime.syncMembership(f.ctx, f.session)
	if group.Canonical().ID == base.ID {
		t.Fatal("the founder's daemon did not seal the revocation of the replaced capability")
	}
	backdated := mustSignedJoinAt(t, f.gid, joiner, first.Capability, beforeRevoke)
	if _, err := group.Apply(backdated); !errors.Is(err, membership.ErrStale) {
		t.Fatalf("a join with the replaced capability dated before its revocation was not refused as stale: %v (member=%t)",
			err, group.IsMemberID(joinerBinding.MemberID))
	}
	if !f.admits(again.Capability) {
		t.Fatal("the seal took the replacement down with the capability it replaced")
	}
}

// TestOpenInviteReplacementCannotReadmitARemovedHolder covers what the seal
// cannot: a group where the founder is not the only member, so its daemon has
// not sealed. The holder of a replaced capability A still gets in with a join
// dated before A's revocation - or, for an A that had expired when it was
// replaced, dated inside its window - while its replacement B is live. Before
// B was revoked with the holder's removal, the removed holder walked back in
// with B through an exhausted link that member_remove reported as having
// nothing outstanding.
//
// The join with A is a use of B's chain, so the daemon's invite worker revokes
// B as soon as it has applied it. Any other live invite this node issued the
// holder is revoked by a removal signed on this node before it, and by one
// signed by another admin once this node has applied it - but that revocation
// is dated after the removal, so a join with such an invite dated between the
// two is still accepted until the founder seals, as for any revoked invite.
// The last two cases pin that residual down and show the seal closing it.
func TestOpenInviteReplacementCannotReadmitARemovedHolder(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed byte
		// expired replaces an A that has expired rather than one whose
		// addresses changed.
		expired bool
		// elsewhere has the removal signed by the other admin and applied
		// here as a pulled record, before any maintenance round.
		elsewhere bool
		// sealed has the founder seal before the holder tries a join with B
		// dated between the removal and B's revocation.
		sealed bool
	}{
		{"join with the replaced capability before the seal", 35, false, false, false},
		{"join with an expired replaced capability", 36, true, false, false},
		{"removal signed elsewhere, window before the seal", 37, false, true, false},
		{"removal signed elsewhere, then sealed", 38, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := startReplayFixture(t, tc.seed, true)
			group := f.session.group
			base := group.Canonical()
			holder := generateIdentity(t)
			holderID := *mustDaemonNodeInfo(t, holder).MemberID
			link := f.createInvite("link", time.Now().Add(time.Hour))
			first, _ := f.redeemed("link", holder)
			replaced := first.Capability
			joinAt := time.Now().UnixMilli()
			if tc.expired {
				// A expires inside the group's life, so a join dated in its
				// window is not older than the group's checkpoint.
				joinAt = base.Timestamp + 10
				replaced = f.expireStoredCapability(link, holder, first.Capability, base.Timestamp+20)
				time.Sleep(time.Until(time.UnixMilli(base.Timestamp + 30)))
			} else {
				time.Sleep(5 * time.Millisecond)
				f.listenWebSocket()
			}
			time.Sleep(5 * time.Millisecond)

			again, _ := f.redeemed("link", holder)
			if again.Capability.Nonce == replaced.Nonce {
				t.Fatal("the stale capability was not replaced")
			}
			if !group.IsInviteRevoked(replaced.Nonce) {
				t.Fatal("the replaced capability was not revoked when its replacement was handed out")
			}
			// The revocation is not sealed, so a join dated before it lands.
			if _, err := group.Apply(mustSignedJoinAt(t, f.gid, holder, replaced, joinAt)); err != nil || !group.IsMemberID(holderID) {
				t.Fatalf("precondition: a join with the replaced capability dated before its revocation should land before the seal: %v", err)
			}
			f.session.reconciler.wait()
			if !group.IsInviteRevoked(again.Capability.Nonce) {
				t.Fatal("a join with the replaced capability left its replacement live")
			}
			// Another invite this node issues the holder while it is in.
			spare := f.inviteTargeted(holder)

			var removedAt int64
			if tc.elsewhere {
				removedAt = time.Now().UnixMilli()
				time.Sleep(10 * time.Millisecond)
				f.removeAs(f.admin, holder, removedAt)
				if !group.IsInviteRevoked(spare.Nonce) {
					t.Fatal("applying a removal signed elsewhere left the member's invite live")
				}
			} else {
				resp := f.removeMember(holder)
				if resp.OutstandingESPOpenInvites == nil || *resp.OutstandingESPOpenInvites != 0 || len(resp.OutstandingOpenInvites) != 0 {
					t.Fatalf("member_remove reported outstanding invites: esp=%v open=%v (%s)",
						resp.OutstandingESPOpenInvites, resp.OutstandingOpenInvites, resp.ESPOpenInvitesError)
				}
				if !group.IsInviteRevoked(spare.Nonce) {
					t.Fatal("member_remove left the member's invite unrevoked")
				}
			}
			if group.IsMemberID(holderID) {
				t.Fatal("the holder is still a member after its removal")
			}
			if f.joinNow(holder, again.Capability) || f.joinNow(holder, spare) {
				t.Fatal("the removed holder rejoined with an invite this node had issued it")
			}
			if f.admits(again.Capability) || f.useCount(link) != 1 {
				t.Fatalf("the replacement still admits (use count %d)", f.useCount(link))
			}

			if tc.elsewhere {
				// Dated after the removal and before the spare's revocation.
				window := mustSignedJoinAt(t, f.gid, holder, spare, removedAt+2)
				if tc.sealed {
					due, ok := group.SealDue()
					if !ok {
						t.Fatal("the revocation did not arm the founder's seal")
					}
					if _, signed, err := group.SealThrough(f.founder, due); err != nil || !signed {
						t.Fatalf("founder seal: signed=%t err=%v", signed, err)
					}
					if _, err := group.Apply(window); !errors.Is(err, membership.ErrStale) {
						t.Fatalf("after the seal a join with the revoked invite dated inside the window was not refused as stale: %v (member=%t)",
							err, group.IsMemberID(holderID))
					}
					return
				}
				// The residual this PR does not close: until the founder
				// seals, the revocation is dated after the removal, so a join
				// dated between them is accepted.
				if _, err := group.Apply(window); err != nil || !group.IsMemberID(holderID) {
					t.Fatalf("the documented pre-seal window changed: err=%v member=%t", err, group.IsMemberID(holderID))
				}
			}
			// Nothing here leaned on the seal: the founder is not alone and
			// reaches nobody, so it has not signed one.
			f.runtime.syncMembership(f.ctx, f.session)
			if group.Canonical().ID != base.ID {
				t.Fatal("the founder sealed; this test must not rely on it")
			}
		})
	}
}

// TestIneffectiveRemovalRevokesNoInvites: a removal the projection ignores - a
// delegated admin removing another admin - takes nothing away, so it must not
// revoke the target's invites either: not ahead of signing one here, and not
// on applying one signed elsewhere.
func TestIneffectiveRemovalRevokesNoInvites(t *testing.T) {
	f := startReplayFixture(t, 39, true)
	group := f.session.group
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	adminID := *mustDaemonNodeInfo(t, f.admin).MemberID
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	// A second capability this node issued to the target, still live.
	spare := f.inviteTargeted(target)
	policy := group.Policy()
	policy.Admins = withAdmin(policy.Admins, targetID)
	if _, err := group.SignRecord(f.founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy}); err != nil {
		t.Fatalf("grant admin: %v", err)
	}

	if group.RemovalTakesEffect(adminID, targetID) {
		t.Fatal("an admin's removal of a peer admin was judged effective")
	}
	if revoked, err := revokeInvitesForRemoval(f.founder, adminID, group, f.runtime.invites, targetID); err != nil || len(revoked) != 0 {
		t.Fatalf("revoked %d invites ahead of an ineffective removal (err %v)", len(revoked), err)
	}
	time.Sleep(5 * time.Millisecond)
	f.removeAs(f.admin, target, time.Now().UnixMilli())
	if !group.IsMemberID(targetID) {
		t.Fatal("precondition: an admin cannot remove a peer admin")
	}
	if group.IsInviteRevoked(spare.Nonce) || !f.admits(spare) {
		t.Fatal("applying an ineffective removal revoked the target's invite")
	}
}

// TestRemovalThatChangesNothingRevokesNothing: once a member has been
// removed, a later removal naming it - by a plain member, which the
// projection ignores, or by an admin, of somebody no longer in the group -
// changes nothing, so the daemon signs nothing and the re-invite issued since
// keeps working; a hostile member repeating such a record cannot use it
// against the re-invite or to arm a seal.
func TestRemovalThatChangesNothingRevokesNothing(t *testing.T) {
	f := startReplayFixture(t, 40, true)
	group := f.session.group
	plain, plainInfo := mustDaemonIdentity(t)
	mustJoinWithInvite(t, group, plain, mustDaemonInvite(t, group, f.founder, plainInfo, 1))
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	spare := f.inviteTargeted(target)
	f.removeMember(target)
	f.session.reconciler.wait()
	if group.IsMemberID(targetID) || !group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("precondition: the founder removed the target and revoked its invite")
	}
	time.Sleep(5 * time.Millisecond)
	reinvite := f.inviteTargeted(target)
	due, armed := group.SealDue()
	changes := f.countChanges()

	time.Sleep(5 * time.Millisecond)
	f.removeAs(plain, target, time.Now().UnixMilli())
	time.Sleep(5 * time.Millisecond)
	f.removeAs(f.admin, target, time.Now().UnixMilli())

	if n := changes.Load(); n != 2 {
		t.Fatalf("two removals that changed nothing led to %d changes; the daemon signed %d record(s)", n, n-2)
	}
	if group.IsInviteRevoked(reinvite.Nonce) || !f.admits(reinvite) {
		t.Fatal("a removal of somebody already removed revoked the re-invite issued since")
	}
	if gotDue, gotArmed := group.SealDue(); gotDue != due || gotArmed != armed {
		t.Fatalf("a removal that changed nothing armed a seal: due %d/%t, was %d/%t", gotDue, gotArmed, due, armed)
	}
	f.cannotRejoinButReinviteWorks(target, first.Capability, spare)
}

// TestIssuerRevokesInvitesOfAMemberRemovedInsideACheckpoint: an issuer that
// lagged receives a removal only folded inside a checkpoint another admin
// signed, never the record. Adopting that checkpoint takes the member out all
// the same, so the issuer must revoke the live invites it issued to it. In the
// second case the issuer also holds the member's leave, signed after the
// removal and so of no effect where the removal is held; without the removal
// the issuer cannot tell, and the checkpoint fallback used to let that leave
// excuse the member.
func TestIssuerRevokesInvitesOfAMemberRemovedInsideACheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name      string
		seed      byte
		heldLeave bool
	}{
		{"removal only", 41, false},
		{"with a later leave held", 48, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := startReplayFixture(t, tc.seed, true)
			group := f.session.group
			target := generateIdentity(t)
			targetInfo := mustDaemonNodeInfo(t, target)
			f.createInvite("link", time.Now().Add(time.Hour))
			first, _ := f.redeemed("link", target)
			mustJoinWithInvite(t, group, target, first.Capability)
			spare := f.inviteTargeted(target)
			// Two checkpoints, so the join is behind the window the next one
			// covers and the issuer holds no record of its own to check that
			// one against.
			for range 2 {
				if _, signed, err := group.SignCheckpoint(f.founder, true); err != nil || !signed {
					t.Fatalf("checkpoint: signed=%t err=%v", signed, err)
				}
			}
			adminGroup, err := membership.Adopt(t.TempDir(), group.Canonical())
			if err != nil {
				t.Fatalf("admin adopts: %v", err)
			}
			defer mustCloseGroup(t, adminGroup)
			time.Sleep(5 * time.Millisecond)
			if _, err := adminGroup.SignRecord(f.admin, membership.Record{Kind: membership.KindRemove, Subject: targetInfo}); err != nil {
				t.Fatalf("admin removes: %v", err)
			}
			if tc.heldLeave {
				time.Sleep(5 * time.Millisecond)
				leave := f.signAs(target, membership.Record{Kind: membership.KindLeave, Subject: targetInfo, Timestamp: time.Now().UnixMilli()})
				if _, err := adminGroup.Apply(leave); err != nil {
					t.Fatalf("admin applies the leave: %v", err)
				}
				// The issuer gets the leave first: it takes the member out
				// there, and by itself revokes nothing.
				f.applyAndSettle(leave)
				if group.IsInviteRevoked(spare.Nonce) {
					t.Fatal("precondition: a leave alone revokes nothing")
				}
			}
			time.Sleep(5 * time.Millisecond)
			folded, signed, err := adminGroup.SignCheckpoint(f.admin, true)
			if err != nil || !signed {
				t.Fatalf("admin checkpoint: signed=%t err=%v", signed, err)
			}

			if _, err := group.ApplyCheckpoint(folded); err != nil {
				t.Fatalf("issuer adopts the admin's checkpoint: %v", err)
			}
			f.session.reconciler.wait()
			if group.Canonical().ID != folded.ID || group.IsMemberID(*targetInfo.MemberID) {
				t.Fatal("precondition: the issuer adopted the checkpoint that removes the target")
			}
			if !group.IsInviteRevoked(spare.Nonce) {
				t.Fatal("a removal folded inside a checkpoint left the removed member's invite live")
			}
			f.cannotRejoinButReinviteWorks(target, spare)
		})
	}
}

// TestRemovalByAnAdminWhoseClockIsBehindRevokes: the removing admin's clock is
// a second behind this node's, so its removal is dated before an invite this
// node minted to the member a moment before the removal arrived. Judged by
// the removal's date alone, that invite looked like a re-invite and stayed
// live. It was minted before this node knew of the removal, so it goes; one
// minted once this node had the removal stays.
func TestRemovalByAnAdminWhoseClockIsBehindRevokes(t *testing.T) {
	f := startReplayFixture(t, 49, true)
	target := generateIdentity(t)
	info := mustDaemonNodeInfo(t, target)
	mustJoinWithInvite(t, f.session.group, target, f.inviteTargeted(target))
	time.Sleep(1200 * time.Millisecond)
	spare := f.inviteTargeted(target)
	time.Sleep(50 * time.Millisecond)
	behind := time.Now().Add(-time.Second).UnixMilli()
	f.removeAs(f.admin, target, behind)
	if f.session.group.IsMemberID(*info.MemberID) {
		t.Fatal("precondition: the admin removed the target")
	}
	if !f.session.group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("an invite minted before this node had the removal stayed live because the remover's clock is behind")
	}
	f.cannotRejoinButReinviteWorks(target, spare)
}

// TestLockedRevocationThatEndsAJoinDoesNotDeadlock: a member removed by
// another admin rejoins with a join dated minutes ahead, with an invite this
// node issued it after the removal. This node revokes that invite while it
// holds lockESPInviteRoster, as a refresh or chain retirement does, and the
// join lands between the moment SignRecord dates the revocation and the
// moment it applies it - so the revocation comes before the join and takes
// the member out again, on the goroutine that holds the lock. Acting on that
// inline took the same lock again on the same goroutine and wedged every
// invite and removal on the node. The change hook only wakes the session's
// worker. The test stands in for that interleaving by applying, under the
// lock, a revocation dated before a join this node already holds.
func TestLockedRevocationThatEndsAJoinDoesNotDeadlock(t *testing.T) {
	f := startReplayFixture(t, 42, true)
	group := f.session.group
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	spare := f.inviteTargeted(target)
	time.Sleep(5 * time.Millisecond)
	f.removeAs(f.admin, target, time.Now().UnixMilli())
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the other admin removed the target")
	}
	time.Sleep(5 * time.Millisecond)
	reinvite := f.inviteTargeted(target)
	revokedAt := time.Now().UnixMilli()
	ahead := mustSignedJoinAt(t, f.gid, target, reinvite, time.Now().Add(2*time.Minute).UnixMilli())
	if _, err := group.Apply(ahead); err != nil || !group.IsMemberID(targetID) {
		t.Fatalf("precondition: a join dated ahead with the re-invite should land: %v", err)
	}
	revocation := f.signAs(f.founder, membership.Record{Kind: membership.KindRevokeInvite, InviteNonce: reinvite.Nonce, Timestamp: revokedAt})

	done := make(chan error, 1)
	go func() {
		unlock := lockESPInviteRoster(f.gid)
		defer unlock()
		_, err := group.Apply(revocation)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("revoke: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a revocation applied under the roster lock never returned: the change hook deadlocked on that lock")
	}
	f.session.reconciler.wait()
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the revocation dated before the join should take the member out again")
	}
	f.cannotRejoinButReinviteWorks(target, first.Capability, spare)
}

// TestLeaveOrRekeyOfAReadmittedMemberIsNoRemoval: a member removed by another
// admin, re-invited and readmitted, then leaves or moves to a new key. Its
// earlier removal is still held, but what ended its membership now is the
// leave or rekey: it is not a removed member, and the daemon signs nothing.
func TestLeaveOrRekeyOfAReadmittedMemberIsNoRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed byte
		kind membership.Kind
	}{
		{"leave", 43, membership.KindLeave},
		{"rekey", 44, membership.KindRekey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := startReplayFixture(t, tc.seed, true)
			group := f.session.group
			member := generateIdentity(t)
			info := mustDaemonNodeInfo(t, member)
			f.createInvite("link", time.Now().Add(time.Hour))
			first, _ := f.redeemed("link", member)
			mustJoinWithInvite(t, group, member, first.Capability)
			time.Sleep(5 * time.Millisecond)
			f.removeAs(f.admin, member, time.Now().UnixMilli())
			time.Sleep(5 * time.Millisecond)
			if !f.joinNow(member, f.inviteTargeted(member)) {
				t.Fatal("precondition: the re-invited member rejoins")
			}
			spare := f.inviteTargeted(member)
			f.session.reconciler.wait()
			changes := f.countChanges()

			time.Sleep(5 * time.Millisecond)
			rec := membership.Record{Kind: tc.kind, Subject: info, Timestamp: time.Now().UnixMilli()}
			if tc.kind == membership.KindRekey {
				rec.Subject = mustDaemonNodeInfo(t, generateIdentity(t))
			}
			f.applyAndSettle(f.signAs(member, rec))
			if group.IsMemberID(*info.MemberID) {
				t.Fatalf("precondition: the %s takes the member's identity out", tc.kind)
			}
			if _, removed := group.RemovedAt([]entmoot.MemberID{*info.MemberID}, f.server.memberID)[*info.MemberID]; removed {
				t.Fatalf("a member that ended its own membership by a %s is reported as removed", tc.kind)
			}
			if n := changes.Load(); n != 1 || group.IsInviteRevoked(spare.Nonce) {
				t.Fatalf("a %s led the daemon to sign %d record(s)", tc.kind, n-1)
			}
		})
	}
}

// TestRemovalThatTakesEffectLaterRevokes: records arrive out of order. This
// node receives an admin's removal of a member before the founder's grant
// that made that admin one, so the removal is held but takes nobody out until
// the grant arrives. The grant is a policy record, not a removal, and judging
// by the record applied missed it: the member kept this node's invites.
func TestRemovalThatTakesEffectLaterRevokes(t *testing.T) {
	f := startReplayFixture(t, 46, true)
	group := f.session.group
	late, lateInfo := mustDaemonIdentity(t)
	mustJoinWithInvite(t, group, late, f.inviteTargeted(late))
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	mustJoinWithInvite(t, group, target, f.inviteTargeted(target))
	spare := f.inviteTargeted(target)

	time.Sleep(5 * time.Millisecond)
	policy := group.Policy()
	policy.Admins = withAdmin(policy.Admins, *lateInfo.MemberID)
	grant := f.signAs(f.founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy, Timestamp: time.Now().UnixMilli()})
	time.Sleep(5 * time.Millisecond)
	f.removeAs(late, target, time.Now().UnixMilli())
	if !group.IsMemberID(targetID) || group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("precondition: a removal by a member not yet admin takes nobody out")
	}

	f.applyAndSettle(grant)
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the grant makes the held removal take effect")
	}
	if !group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("the removal that took effect with the grant left the member's invite live")
	}
	f.cannotRejoinButReinviteWorks(target, spare)
}

// TestLeaveBeforeAnEarlierRemovalRevokes: a member's leave, dated after an
// admin's removal of it, reaches this node first. The member is already gone
// when the removal arrives, so diffing members before and after applying it
// saw nothing. In the group's own order the removal comes first and ended the
// membership, so the member is a removed one and loses this node's invites.
func TestLeaveBeforeAnEarlierRemovalRevokes(t *testing.T) {
	f := startReplayFixture(t, 47, true)
	target := generateIdentity(t)
	info := mustDaemonNodeInfo(t, target)
	mustJoinWithInvite(t, f.session.group, target, f.inviteTargeted(target))
	spare := f.inviteTargeted(target)
	time.Sleep(5 * time.Millisecond)
	removedAt := time.Now().UnixMilli()
	removal := f.signAs(f.admin, membership.Record{Kind: membership.KindRemove, Subject: info, Timestamp: removedAt})
	leave := f.signAs(target, membership.Record{Kind: membership.KindLeave, Subject: info, Timestamp: removedAt + 10})
	time.Sleep(20 * time.Millisecond)

	f.applyAndSettle(leave)
	if f.session.group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("precondition: a leave alone revokes nothing")
	}
	f.applyAndSettle(removal)
	if !f.session.group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("a removal that arrived after the member's later leave left its invite live")
	}
	f.cannotRejoinButReinviteWorks(target, spare)
}

// TestRevocationsDoNotDependOnArrivalOrder applies one set of records - the
// founder's grant making a member admin, that admin's removal of another
// member, and the removed member's later leave - in every order, each on a
// fresh node, and checks every order ends with the same invites revoked: the
// one minted before the removal, and the one minted after the removal's date
// but before this node had it, while one minted once it had it stays live.
func TestRevocationsDoNotDependOnArrivalOrder(t *testing.T) {
	orders := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for i, order := range orders {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			f := startReplayFixture(t, byte(50+i), true)
			group := f.session.group
			late, lateInfo := mustDaemonIdentity(t)
			mustJoinWithInvite(t, group, late, f.inviteTargeted(late))
			target := generateIdentity(t)
			info := mustDaemonNodeInfo(t, target)
			mustJoinWithInvite(t, group, target, f.inviteTargeted(target))
			pre := f.inviteTargeted(target)
			time.Sleep(5 * time.Millisecond)
			start := time.Now().UnixMilli()
			policy := group.Policy()
			policy.Admins = withAdmin(policy.Admins, *lateInfo.MemberID)
			records := [3]membership.Record{
				f.signAs(f.founder, membership.Record{Kind: membership.KindPolicy, Policy: &policy, Timestamp: start}),
				f.signAs(late, membership.Record{Kind: membership.KindRemove, Subject: info, Timestamp: start + 10}),
				f.signAs(target, membership.Record{Kind: membership.KindLeave, Subject: info, Timestamp: start + 20}),
			}
			time.Sleep(40 * time.Millisecond)
			unaware := f.inviteTargeted(target)
			for _, k := range order {
				f.applyAndSettle(records[k])
			}
			time.Sleep(5 * time.Millisecond)
			known := f.inviteTargeted(target)
			f.session.reconciler.signal()
			f.session.reconciler.wait()
			got := [4]bool{group.IsMemberID(*info.MemberID), group.IsInviteRevoked(pre.Nonce),
				group.IsInviteRevoked(unaware.Nonce), group.IsInviteRevoked(known.Nonce)}
			if want := [4]bool{false, true, true, false}; got != want {
				t.Fatalf("member; revoked: minted before the removal, before this node had it, after = %v, want %v", got, want)
			}
			if at := group.RemovedAt([]entmoot.MemberID{*info.MemberID}, f.server.memberID); at[*info.MemberID].At != start+10 {
				t.Fatalf("removed at %v, want %d", at, start+10)
			}
			f.cannotRejoinButReinviteWorks(target, pre)
		})
	}
}

// TestLeaverReinviteSurvivesTheFoundersCheckpoint: a member leaves of its own
// accord, this node re-invites it, and then the founder's daemon - this node -
// signs a checkpoint that folds the leave in. Reading only the checkpoints, a
// member that went out inside one looks removed, and the re-invite, minted
// before the checkpoint, was revoked. This node signed that checkpoint from
// the records it holds, so those records say the member left: it is no
// removed member, and nothing is revoked.
func TestLeaverReinviteSurvivesTheFoundersCheckpoint(t *testing.T) {
	f := startReplayFixture(t, 60, true)
	group := f.session.group
	leaver := generateIdentity(t)
	info := mustDaemonNodeInfo(t, leaver)
	mustJoinWithInvite(t, group, leaver, f.inviteTargeted(leaver))
	time.Sleep(5 * time.Millisecond)
	f.applyAndSettle(f.signAs(leaver, membership.Record{Kind: membership.KindLeave, Subject: info, Timestamp: time.Now().UnixMilli()}))
	if group.IsMemberID(*info.MemberID) {
		t.Fatal("precondition: the leave takes the member out")
	}
	time.Sleep(5 * time.Millisecond)
	reinvite := f.inviteTargeted(leaver)

	sealed, signed, err := group.SignCheckpoint(f.founder, true)
	if err != nil || !signed {
		t.Fatalf("founder checkpoint: signed=%t err=%v", signed, err)
	}
	f.session.reconciler.signal()
	f.session.reconciler.wait()
	if group.Canonical().ID != sealed.ID {
		t.Fatal("precondition: the founder's checkpoint is canonical and covers the leave")
	}
	if _, removed := group.RemovedAt([]entmoot.MemberID{*info.MemberID}, f.server.memberID)[*info.MemberID]; removed {
		t.Fatal("a member that left inside this node's own checkpoint is reported as removed")
	}
	if group.IsInviteRevoked(reinvite.Nonce) || !f.admits(reinvite) {
		t.Fatal("the re-invite of a member that left was revoked once a checkpoint covered the leave")
	}
	if !f.joinNow(leaver, reinvite) {
		t.Fatal("the member that left could not come back with its re-invite")
	}
}

// TestReinviteMintedWhileTheWorkerIsBusySurvives: when this node learned of a
// removal was taken as the moment the invite worker got round to it. With the
// worker held up by an earlier pass, a re-invite this node minted after
// applying the removal was dated before that moment and revoked. The time is
// taken when the removal is applied, so the re-invite stands while the
// member's earlier invite goes.
func TestReinviteMintedWhileTheWorkerIsBusySurvives(t *testing.T) {
	f := startReplayFixture(t, 61, true)
	group := f.session.group
	earlier, removed := generateIdentity(t), generateIdentity(t)
	earlierInfo, removedInfo := mustDaemonNodeInfo(t, earlier), mustDaemonNodeInfo(t, removed)
	mustJoinWithInvite(t, group, earlier, f.inviteTargeted(earlier))
	mustJoinWithInvite(t, group, removed, f.inviteTargeted(removed))
	earlierSpare, removedSpare := f.inviteTargeted(earlier), f.inviteTargeted(removed)
	f.session.reconciler.wait()

	// Hold the roster lock, so the pass for the first removal - which has an
	// invite to revoke - stalls when it comes to sign.
	unlock := lockESPInviteRoster(f.gid)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	time.Sleep(5 * time.Millisecond)
	apply := func(rec membership.Record) {
		t.Helper()
		if _, err := group.Apply(rec); err != nil {
			t.Fatalf("apply %s: %v", rec.Kind, err)
		}
	}
	apply(f.signAs(f.admin, membership.Record{Kind: membership.KindRemove, Subject: earlierInfo, Timestamp: time.Now().UnixMilli()}))
	time.Sleep(300 * time.Millisecond) // the worker reaches the lock
	apply(f.signAs(f.admin, membership.Record{Kind: membership.KindRemove, Subject: removedInfo, Timestamp: time.Now().UnixMilli()}))
	time.Sleep(20 * time.Millisecond)
	reinvite := f.inviteTargeted(removed)
	time.Sleep(20 * time.Millisecond)
	unlock()
	locked = false
	f.session.reconciler.wait()

	if !group.IsInviteRevoked(earlierSpare.Nonce) || !group.IsInviteRevoked(removedSpare.Nonce) {
		t.Fatal("precondition: the removed members' earlier invites are revoked")
	}
	if group.IsInviteRevoked(reinvite.Nonce) {
		t.Fatal("a re-invite minted after this node applied the removal was revoked because the worker was busy")
	}
	f.cannotRejoinButReinviteWorks(removed, removedSpare)
}

// TestInvitesOfARemovalQueuedAtShutdownAreRevoked: a removal applied just
// before the group is removed, as at shutdown, is reconciled before the group
// is closed, and a signal after that is a harmless no-op.
func TestInvitesOfARemovalQueuedAtShutdownAreRevoked(t *testing.T) {
	f := startReplayFixture(t, 45, true)
	group := f.session.group
	target := generateIdentity(t)
	targetInfo := mustDaemonNodeInfo(t, target)
	mustJoinWithInvite(t, group, target, f.inviteTargeted(target))
	spare := f.inviteTargeted(target)
	time.Sleep(5 * time.Millisecond)
	removal := f.signAs(f.admin, membership.Record{Kind: membership.KindRemove, Subject: targetInfo, Timestamp: time.Now().UnixMilli()})
	reconciler := f.session.reconciler
	if _, err := group.Apply(removal); err != nil {
		t.Fatalf("apply removal: %v", err)
	}
	if !f.runtime.RemoveGroup(f.gid) {
		t.Fatal("RemoveGroup found no session")
	}
	records, err := f.runtime.invites.ListInvites(&f.gid)
	if err != nil {
		t.Fatalf("ListInvites: %v", err)
	}
	for _, record := range records {
		if record.Nonce == spare.Nonce && record.RevokedAtMS == 0 {
			t.Fatal("a removal applied at shutdown was not reconciled: the removed member's invite is still live")
		}
	}
	reconciler.signal()
	reconciler.wait()
}

// TestOpenInviteReplayKeepsStoredBytesUnlessTheDaemonChecked covers an ESP
// upgraded ahead of its daemon. The fake daemons here know only the frames a
// daemon from before invite_refresh knows. One hangs up on any other frame, as
// that daemon's decoder does. The other reads every request as an
// invite_create and mints, the way lenient JSON decoding treats a field it
// does not know. Neither answer says the stored capability was checked, so
// the replay must return the stored bytes and store nothing. Taking any new
// nonce would let every replay mint again.
func TestOpenInviteReplayKeepsStoredBytesUnlessTheDaemonChecked(t *testing.T) {
	for _, daemon := range []struct {
		name            string
		mintsAnyRequest bool
	}{
		{"hangs up on an unknown frame", false},
		{"mints for any request", true},
	} {
		t.Run(daemon.name, func(t *testing.T) {
			ctx := context.Background()
			gid := testESPGroupID(33)
			sock := testUnixSocketPath(t)
			var requests atomic.Int32
			serveUnix(t, sock, func(conn net.Conn) {
				requests.Add(1)
				msgType, body, err := ipc.ReadFrame(conn)
				var req ipc.InviteCreateReq
				if err == nil && (msgType == ipc.MsgInviteCreateReq || daemon.mintsAnyRequest) && json.Unmarshal(body, &req) == nil {
					capability := entmoot.BootstrapCapability{GroupID: req.GroupID, TargetPublicKey: req.TargetPublicKey}
					_, _ = rand.Read(capability.Nonce[:])
					_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{Status: "created", GroupID: req.GroupID, Capability: capability})
				}
			})
			state, err := esphttp.OpenSQLiteStateStore(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLiteStateStore: %v", err)
			}
			defer state.Close()
			exec := espOperationExecutor{dataDir: t.TempDir(), socketPath: sock, timeout: 5 * time.Second, stateStore: state}
			hash := esphttp.HashOpenInviteToken("link")
			if _, err := state.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
				TokenHash: hash, GroupID: gid, DeviceID: "device-1", MaxUses: 1, ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli(),
			}); err != nil {
				t.Fatalf("CreateOpenInvite: %v", err)
			}
			info := mustDaemonNodeInfo(t, generateIdentity(t))
			payload, err := json.Marshal(map[string]any{
				"member_id": info.MemberID.String(), "peer_id": info.PeerID, "entmoot_pubkey": info.EntmootPubKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			first, err := exec.RedeemOpenInvite(ctx, "link", payload)
			if err != nil {
				t.Fatalf("first redemption: %v", err)
			}
			for range 3 {
				again, err := exec.RedeemOpenInvite(ctx, "link", payload)
				if err != nil {
					t.Fatalf("repeat redemption: %v", err)
				}
				if !bytes.Equal(again, first) {
					t.Fatalf("repeat took an unchecked answer:\n%s\nwant the stored\n%s", again, first)
				}
			}
			stored, ok, err := state.GetOpenInviteRedemption(ctx, hash, info.MemberID.String())
			if err != nil || !ok || !bytes.Equal(stored.Result, first) {
				t.Fatalf("stored result changed: ok=%t err=%v\n%s", ok, err, stored.Result)
			}
			if got := requests.Load(); got != 4 {
				t.Fatalf("daemon saw %d requests, want 4: the first mint and one per replay", got)
			}
		})
	}
}

// withAdmin adds id to an admin set, which a policy record must keep sorted.
func withAdmin(admins []entmoot.MemberID, id entmoot.MemberID) []entmoot.MemberID {
	admins = append(slices.Clone(admins), id)
	slices.SortFunc(admins, func(a, b entmoot.MemberID) int { return bytes.Compare(a[:], b[:]) })
	return admins
}
