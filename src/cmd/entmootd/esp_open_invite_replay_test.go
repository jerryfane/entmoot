package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
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

// startReplayFixture starts the fixture. withPeer adds a second member with
// no address, so the founder is not the group's only member and its daemon
// never seals: nothing may rely on the seal arriving.
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
		peer, peerInfo := mustDaemonIdentity(t)
		mustJoinWithInvite(t, group, peer, mustDaemonInvite(t, group, founder, peerInfo, 1))
		mustCloseGroup(t, group)
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
// nothing outstanding. Whoever removes it, B must not admit it again.
func TestOpenInviteReplacementCannotReadmitARemovedHolder(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed byte
		// expired replaces an A that has expired rather than one whose
		// addresses changed.
		expired bool
		// elsewhere has the removal signed without this daemon's
		// member_remove, as by another admin, after a maintenance round.
		elsewhere bool
	}{
		{"join with the replaced capability before the seal", 35, false, false},
		{"join with an expired replaced capability", 36, true, false},
		{"removal signed elsewhere", 37, false, true},
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

			if tc.elsewhere {
				f.runtime.syncMembership(f.ctx, f.session)
				if !group.IsInviteRevoked(again.Capability.Nonce) {
					t.Fatal("a maintenance round left the replacement live after its chain was used to join")
				}
				if err := applyRosterRemove(f.founder, group, mustDaemonNodeInfo(t, holder)); err != nil {
					t.Fatalf("remove: %v", err)
				}
			} else {
				resp := f.removeMember(holder)
				if resp.OutstandingESPOpenInvites == nil || *resp.OutstandingESPOpenInvites != 0 || len(resp.OutstandingOpenInvites) != 0 {
					t.Fatalf("member_remove reported outstanding invites: esp=%v open=%v (%s)",
						resp.OutstandingESPOpenInvites, resp.OutstandingOpenInvites, resp.ESPOpenInvitesError)
				}
				if !group.IsInviteRevoked(again.Capability.Nonce) {
					t.Fatal("member_remove left the replacement unrevoked")
				}
			}
			if group.IsMemberID(holderID) {
				t.Fatal("the holder is still a member after its removal")
			}
			if f.joinNow(holder, again.Capability) {
				t.Fatal("the removed holder rejoined with the replacement")
			}
			if f.admits(again.Capability) || f.useCount(link) != 1 {
				t.Fatalf("the replacement still admits (use count %d)", f.useCount(link))
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
