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
// acted on the departure.
func (f *replayFixture) removeAs(signer, member *keystore.Identity, at int64) {
	f.t.Helper()
	record, err := membership.SignRecord(signer, membership.Record{
		GroupID: f.gid, Kind: membership.KindRemove,
		Actor: mustDaemonNodeInfo(f.t, signer), Subject: mustDaemonNodeInfo(f.t, member), Timestamp: at,
	})
	if err != nil {
		f.t.Fatalf("sign removal: %v", err)
	}
	if _, err := f.session.group.Apply(record); err != nil {
		f.t.Fatalf("apply removal: %v", err)
	}
	f.session.departures.wait()
}

// countDepartures has the group report departures to a counter as well as
// to the daemon, and returns the counter.
func (f *replayFixture) countDepartures() *atomic.Int32 {
	var count atomic.Int32
	queue := f.session.departures
	f.session.group.SetDepartureHook(func(departed []membership.Departure) {
		count.Add(int32(len(departed)))
		queue.push(departed)
	})
	return &count
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
// A removal signed on this node revokes B before it, so B never readmits. A
// removal signed by another admin revokes B once this node has applied it, on
// the session's departure worker with no maintenance round in between - but
// that revocation is dated after
// the removal, so a join with B dated between the two is still accepted until
// the founder seals, as for any revoked invite. The last two cases pin that
// residual down and show the seal closing it.
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
			if group.IsInviteRevoked(again.Capability.Nonce) {
				t.Fatal("precondition: the replacement is live until the removal")
			}

			var removedAt int64
			if tc.elsewhere {
				// The removal is applied before this node has run a round
				// over the join, so only applying it can revoke B.
				removedAt = time.Now().UnixMilli()
				time.Sleep(10 * time.Millisecond)
				f.removeAs(f.admin, holder, removedAt)
				if !group.IsInviteRevoked(again.Capability.Nonce) {
					t.Fatal("applying a removal signed elsewhere left the replacement live")
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

			if tc.elsewhere {
				// Dated after the removal and before B's revocation.
				window := mustSignedJoinAt(t, f.gid, holder, again.Capability, removedAt+2)
				if tc.sealed {
					due, ok := group.SealDue()
					if !ok {
						t.Fatal("B's revocation did not arm the founder's seal")
					}
					if _, signed, err := group.SealThrough(f.founder, due); err != nil || !signed {
						t.Fatalf("founder seal: signed=%t err=%v", signed, err)
					}
					if _, err := group.Apply(window); !errors.Is(err, membership.ErrStale) {
						t.Fatalf("after the seal a join with the replacement dated inside the window was not refused as stale: %v (member=%t)",
							err, group.IsMemberID(holderID))
					}
					return
				}
				// The residual this PR does not close: until the founder
				// seals, B's revocation is dated after the removal, so a join
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
// takes nobody out. The issuing daemon used to judge such a record by the
// subject's standing, already removed, and revoked the re-invite the founder
// had issued since; a hostile member could repeat that at will and arm a seal
// each time. Only a record that takes the member out revokes anything.
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
	f.removeMember(target)
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the founder removed the target")
	}
	reinvite := f.inviteTargeted(target)
	due, armed := group.SealDue()

	time.Sleep(5 * time.Millisecond)
	f.removeAs(plain, target, time.Now().UnixMilli())
	time.Sleep(5 * time.Millisecond)
	f.removeAs(f.admin, target, time.Now().UnixMilli())

	if group.IsInviteRevoked(reinvite.Nonce) || !f.admits(reinvite) {
		t.Fatal("a removal of somebody already removed revoked the re-invite issued since")
	}
	if gotDue, gotArmed := group.SealDue(); gotDue != due || gotArmed != armed {
		t.Fatalf("a removal that changed nothing armed a seal: due %d/%t, was %d/%t", gotDue, gotArmed, due, armed)
	}
	if !f.joinNow(target, reinvite) {
		t.Fatal("the re-invited member could not rejoin")
	}
}

// TestIssuerRevokesInvitesOfAMemberRemovedInsideACheckpoint: an issuer that
// lagged receives a removal only folded inside a checkpoint another admin
// signed, never the record. Adopting that checkpoint takes the member out all
// the same, so the issuer must revoke the live invites it issued to it.
func TestIssuerRevokesInvitesOfAMemberRemovedInsideACheckpoint(t *testing.T) {
	f := startReplayFixture(t, 41, true)
	group := f.session.group
	target := generateIdentity(t)
	targetInfo := mustDaemonNodeInfo(t, target)
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	spare := f.inviteTargeted(target)
	// Two checkpoints, so the join is behind the window the next one covers
	// and the issuer holds no record of its own to check that one against.
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
	folded, signed, err := adminGroup.SignCheckpoint(f.admin, true)
	if err != nil || !signed {
		t.Fatalf("admin checkpoint: signed=%t err=%v", signed, err)
	}

	if _, err := group.ApplyCheckpoint(folded); err != nil {
		t.Fatalf("issuer adopts the admin's checkpoint: %v", err)
	}
	f.session.departures.wait()
	if group.Canonical().ID != folded.ID || group.IsMemberID(*targetInfo.MemberID) {
		t.Fatal("precondition: the issuer adopted the checkpoint that removes the target")
	}
	if !group.IsInviteRevoked(spare.Nonce) || f.admits(spare) {
		t.Fatal("a removal folded inside a checkpoint left the removed member's invite live")
	}
	if f.joinNow(target, spare) {
		t.Fatal("the removed member rejoined with an invite the issuer had handed it")
	}
}

// TestDepartureDuringALockedRevocationDoesNotDeadlock: a member removed by
// another admin rejoins with a join dated minutes ahead, with an invite this
// node issued it after the removal. This node revokes that invite while it
// holds lockESPInviteRoster, as a refresh or chain retirement does, and the
// join lands between the moment SignRecord dates the revocation and the
// moment it applies it - so the revocation comes before the join and takes
// the member out again, on the goroutine that holds the lock. The departure
// hook used to blame that on the old removal and take the same lock again,
// wedging every invite and removal on the node for good. The test stands in
// for that interleaving by applying, under the lock, a revocation this node
// signed dated before the join it already holds. A revocation is no removal,
// so it reports no departure, and departures are acted on by the session's
// own worker anyway.
func TestDepartureDuringALockedRevocationDoesNotDeadlock(t *testing.T) {
	f := startReplayFixture(t, 42, true)
	group := f.session.group
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	f.removeAs(f.admin, target, time.Now().UnixMilli())
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the other admin removed the target")
	}
	reinvite := f.inviteTargeted(target)
	revokedAt := time.Now().UnixMilli()
	ahead := mustSignedJoinAt(t, f.gid, target, reinvite, time.Now().Add(2*time.Minute).UnixMilli())
	if _, err := group.Apply(ahead); err != nil || !group.IsMemberID(targetID) {
		t.Fatalf("precondition: a join dated ahead with the re-invite should land: %v", err)
	}
	revocation, err := membership.SignRecord(f.founder, membership.Record{
		GroupID: f.gid, Kind: membership.KindRevokeInvite, InviteNonce: reinvite.Nonce,
		Actor: mustDaemonNodeInfo(t, f.founder), Timestamp: revokedAt,
	})
	if err != nil {
		t.Fatalf("sign revocation: %v", err)
	}
	departures := f.countDepartures()

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
		t.Fatal("a revocation signed under the roster lock never returned: the departure hook deadlocked on that lock")
	}
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the revocation dated before the join should take the member out again")
	}
	f.session.departures.wait()
	if n := departures.Load(); n != 0 {
		t.Fatalf("a revocation was reported as %d removal departure(s)", n)
	}
	// The roster lock is free again: another invite can be issued.
	f.inviteTargeted(target)
}

// TestLeaveOrRekeyOfAReadmittedMemberIsNoDeparture: a member removed by
// another admin, re-invited and readmitted, then leaves or moves to a new key.
// Its earlier removal is still held, and the hook used to blame the departure
// on it and revoke invites issued to the member since. Only a removal record
// that takes a member out is a departure.
func TestLeaveOrRekeyOfAReadmittedMemberIsNoDeparture(t *testing.T) {
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
			f.removeAs(f.admin, member, time.Now().UnixMilli())
			if !f.joinNow(member, f.inviteTargeted(member)) {
				t.Fatal("precondition: the re-invited member rejoins")
			}
			// Issued after the removal, but dated (InviteIssuedAtMS) before it.
			spare := f.inviteTargeted(member)
			departures := f.countDepartures()

			time.Sleep(5 * time.Millisecond)
			rec := membership.Record{GroupID: f.gid, Kind: tc.kind, Actor: info, Subject: info, Timestamp: time.Now().UnixMilli()}
			if tc.kind == membership.KindRekey {
				rec.Subject = mustDaemonNodeInfo(t, generateIdentity(t))
			}
			signed, err := membership.SignRecord(member, rec)
			if err != nil {
				t.Fatalf("sign %s: %v", tc.kind, err)
			}
			if _, err := group.Apply(signed); err != nil || group.IsMemberID(*info.MemberID) {
				t.Fatalf("precondition: the %s takes the member's identity out: %v", tc.kind, err)
			}
			f.session.departures.wait()
			if n := departures.Load(); n != 0 {
				t.Fatalf("a %s was reported as %d removal departure(s)", tc.kind, n)
			}
			if group.IsInviteRevoked(spare.Nonce) {
				t.Fatalf("a %s revoked an invite issued to the member", tc.kind)
			}
		})
	}
}

// TestRemovalThatTakesEffectLaterIsADeparture: records arrive out of order.
// This node receives an admin's removal of a member before the founder's
// grant that made that admin one, so the removal is held but ineffective;
// it takes the member out only when the grant arrives. The record applied
// then is a policy, not a removal, and attributing departures to the applied
// record reported nothing: the member kept the invites this node had issued
// it and walked back in. The departure is the removal's, dated and signed as
// it is, whatever record made it count.
func TestRemovalThatTakesEffectLaterIsADeparture(t *testing.T) {
	f := startReplayFixture(t, 46, true)
	group := f.session.group
	late, lateInfo := mustDaemonIdentity(t)
	mustJoinWithInvite(t, group, late, f.inviteTargeted(late))
	target := generateIdentity(t)
	targetID := *mustDaemonNodeInfo(t, target).MemberID
	mustJoinWithInvite(t, group, target, f.inviteTargeted(target))
	spare := f.inviteTargeted(target)

	// The founder's grant, signed on another of its devices and not yet here.
	time.Sleep(5 * time.Millisecond)
	policy := group.Policy()
	policy.Admins = withAdmin(policy.Admins, *lateInfo.MemberID)
	grant, err := membership.SignRecord(f.founder, membership.Record{
		GroupID: f.gid, Kind: membership.KindPolicy, Actor: mustDaemonNodeInfo(t, f.founder),
		Policy: &policy, Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("sign grant: %v", err)
	}
	departures := f.countDepartures()
	time.Sleep(5 * time.Millisecond)
	removedAt := time.Now().UnixMilli()
	f.removeAs(late, target, removedAt)
	if !group.IsMemberID(targetID) || departures.Load() != 0 || group.IsInviteRevoked(spare.Nonce) {
		t.Fatal("precondition: a removal by a member not yet admin takes nobody out")
	}

	if _, err := group.Apply(grant); err != nil {
		t.Fatalf("apply grant: %v", err)
	}
	f.session.departures.wait()
	if group.IsMemberID(targetID) {
		t.Fatal("precondition: the grant makes the held removal take effect")
	}
	if n := departures.Load(); n != 1 {
		t.Fatalf("the removal that took effect with the grant was reported as %d departure(s), want 1", n)
	}
	if !group.IsInviteRevoked(spare.Nonce) || f.admits(spare) {
		t.Fatal("the removed member's invite is still live")
	}
	if f.joinNow(target, spare) {
		t.Fatal("the removed member rejoined with an invite this node had issued it")
	}
}

// TestDeparturesQueuedAtShutdownAreProcessed: departures wait in the
// session's queue for its worker. Removing the group, as shutdown does, must
// process what is queued before the group is closed, and a departure pushed
// after that is dropped rather than blocking or panicking.
func TestDeparturesQueuedAtShutdownAreProcessed(t *testing.T) {
	f := startReplayFixture(t, 45, true)
	group := f.session.group
	target := generateIdentity(t)
	targetInfo := mustDaemonNodeInfo(t, target)
	f.createInvite("link", time.Now().Add(time.Hour))
	first, _ := f.redeemed("link", target)
	mustJoinWithInvite(t, group, target, first.Capability)
	spare := f.inviteTargeted(target)
	removal, err := membership.SignRecord(f.admin, membership.Record{
		GroupID: f.gid, Kind: membership.KindRemove,
		Actor: mustDaemonNodeInfo(t, f.admin), Subject: targetInfo, Timestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("sign removal: %v", err)
	}
	queue := f.session.departures
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
			t.Fatal("a departure queued at shutdown was dropped: the removed member's invite is still live")
		}
	}
	queue.push([]membership.Departure{{Member: *targetInfo.MemberID, At: time.Now().UnixMilli()}})
	queue.wait()
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
