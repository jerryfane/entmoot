package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
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
	"github.com/multiformats/go-multiaddr"
)

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	gid := testESPGroupID(32)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())

	host, hostBinding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close()
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer messages.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: host, Binding: hostBinding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatalf("newGroupRuntime: %v", err)
	}
	defer runtime.Close()
	if _, _, err := runtime.AddLocalGroup(ctx, gid); err != nil {
		t.Fatalf("AddLocalGroup: %v", err)
	}
	session, ok := runtime.Get(gid)
	if !ok {
		t.Fatal("group session missing")
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founderInfo.EntmootPubKey)
	if err != nil {
		t.Fatal(err)
	}
	server := &ipcServer{
		memberID: founderBinding.MemberID, peerID: founderBinding.PeerID.String(),
		identity: founder, dataDir: root, runtime: runtime,
	}
	sock := testUnixSocketPath(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			server.handleConn(ctx, conn)
			_ = conn.Close()
		}
	}()
	defer func() {
		_ = ln.Close()
		<-served
	}()

	state, err := esphttp.OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	defer state.Close()
	handler, err := esphttp.NewHandler(esphttp.Config{
		Token:      "replay-test",
		Service:    mailboxtest.New(t, storetest.New(t), nil),
		State:      state,
		Operations: espOperationExecutor{dataDir: root, socketPath: sock, timeout: 5 * time.Second, stateStore: state},
	})
	if err != nil {
		t.Fatal(err)
	}

	createInvite := func(token string, expiresAt time.Time) string {
		t.Helper()
		hash := esphttp.HashOpenInviteToken(token)
		if _, err := state.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
			TokenHash: hash, GroupID: gid, DeviceID: "device-1", MaxUses: 1, ExpiresAtMS: expiresAt.UnixMilli(),
		}); err != nil {
			t.Fatalf("CreateOpenInvite: %v", err)
		}
		return hash
	}
	redeem := func(token string, joiner *keystore.Identity) *httptest.ResponseRecorder {
		t.Helper()
		info := mustDaemonNodeInfo(t, joiner)
		body, err := json.Marshal(map[string]any{
			"member_id": info.MemberID.String(), "peer_id": info.PeerID, "entmoot_pubkey": info.EntmootPubKey,
		})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/open-invites/"+token+"/redeem", bytes.NewReader(body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	type redemption struct {
		UseCount   int                         `json:"use_count"`
		Capability entmoot.BootstrapCapability `json:"capability"`
	}
	redeemed := func(response *httptest.ResponseRecorder) (redemption, []byte) {
		t.Helper()
		if response.Code != http.StatusOK {
			t.Fatalf("redeem: %d %s", response.Code, response.Body.String())
		}
		var got redemption
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatalf("redeem response: %v", err)
		}
		return got, response.Body.Bytes()
	}
	refused := func(response *httptest.ResponseRecorder, code string) {
		t.Helper()
		var got struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || response.Code != http.StatusConflict || got.Error.Code != code {
			t.Fatalf("redeem = %d %s, want 409 %s", response.Code, response.Body.String(), code)
		}
	}
	useCount := func(hash string) int {
		t.Helper()
		rec, ok, err := state.GetOpenInviteByTokenHash(ctx, hash)
		if err != nil || !ok {
			t.Fatalf("GetOpenInviteByTokenHash: ok=%t err=%v", ok, err)
		}
		return rec.UseCount
	}
	ledgerRows := func() int {
		t.Helper()
		records, err := runtime.invites.ListInvites(&gid)
		if err != nil {
			t.Fatalf("ListInvites: %v", err)
		}
		return len(records)
	}
	hasWebSocket := func(capability entmoot.BootstrapCapability) bool {
		for _, address := range capability.AllowedMultiaddrs {
			if strings.Contains(address, "/ws/") {
				return true
			}
		}
		return false
	}
	generate := func() *keystore.Identity {
		t.Helper()
		identity, err := keystore.Generate()
		if err != nil {
			t.Fatal(err)
		}
		return identity
	}

	// Two identities redeem while the node listens on TCP only: one never
	// joins, the other joins with its capability and is then removed.
	stuck, removed := generate(), generate()
	link := createInvite("link", time.Now().Add(time.Hour))
	used := createInvite("used", time.Now().Add(time.Hour))
	first, firstBody := redeemed(redeem("link", stuck))
	if hasWebSocket(first.Capability) {
		t.Fatalf("first capability already carries a WebSocket address: %v", first.Capability.AllowedMultiaddrs)
	}
	removedGrant, removedBody := redeemed(redeem("used", removed))
	mustJoinWithInvite(t, session.group, removed, removedGrant.Capability)
	if err := applyRosterRemove(founder, session.group, mustDaemonNodeInfo(t, removed)); err != nil {
		t.Fatalf("remove member: %v", err)
	}
	rows := ledgerRows()

	// Nothing changed: the stored bytes come back and nothing is minted.
	if _, body := redeemed(redeem("link", stuck)); !bytes.Equal(body, firstBody) {
		t.Fatalf("an unchanged replay returned different bytes:\n%s\nwant\n%s", body, firstBody)
	}
	if got := ledgerRows(); got != rows {
		t.Fatalf("an unchanged replay minted: ledger rows %d, want %d", got, rows)
	}

	if err := host.Network().Listen(multiaddr.StringCast("/ip4/127.0.0.1/tcp/0/ws")); err != nil {
		t.Fatalf("listen on WebSocket: %v", err)
	}

	// The stuck identity gets one replacement carrying the new address.
	again, againBody := redeemed(redeem("link", stuck))
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
	if err := session.group.CheckInvite(again.Capability, time.Now().UnixMilli()); err != nil {
		t.Fatalf("group refuses the replacement: %v", err)
	}
	// The replaced capability had not expired, so it was revoked before the
	// replacement left: the holder never carries two that admit it.
	if err := session.group.CheckInvite(first.Capability, time.Now().UnixMilli()); err == nil {
		t.Fatal("the replaced capability still admits alongside its replacement")
	}
	if !session.group.IsInviteRevoked(first.Capability.Nonce) {
		t.Fatal("the replaced capability was not revoked in the roster")
	}
	if again.UseCount != 1 || useCount(link) != 1 {
		t.Fatalf("replacement spent a use: response %d, stored %d, want 1", again.UseCount, useCount(link))
	}
	if got := ledgerRows(); got != rows+1 {
		t.Fatalf("replacement left %d ledger rows, want %d", got, rows+1)
	}
	for range 3 {
		if _, body := redeemed(redeem("link", stuck)); !bytes.Equal(body, againBody) {
			t.Fatalf("a replay after the replacement returned different bytes:\n%s\nwant\n%s", body, againBody)
		}
	}
	if got := ledgerRows(); got != rows+1 {
		t.Fatalf("replays after the replacement minted: ledger rows %d, want %d", got, rows+1)
	}

	// Joining with the replacement and being removed leaves nothing to rejoin
	// with: the replay hands back the spent capability, not a new one.
	mustJoinWithInvite(t, session.group, stuck, again.Capability)
	if err := applyRosterRemove(founder, session.group, mustDaemonNodeInfo(t, stuck)); err != nil {
		t.Fatalf("remove stuck member: %v", err)
	}
	if err := host.Network().Listen(multiaddr.StringCast("/ip4/127.0.0.1/tcp/0/ws")); err != nil {
		t.Fatalf("listen on a second WebSocket: %v", err)
	}
	if _, body := redeemed(redeem("link", stuck)); !bytes.Equal(body, againBody) {
		t.Fatalf("a removed member's replay after an address change returned different bytes:\n%s\nwant\n%s", body, againBody)
	}
	for _, capability := range []entmoot.BootstrapCapability{first.Capability, again.Capability} {
		if err := session.group.CheckInvite(capability, time.Now().UnixMilli()); err == nil {
			t.Fatal("a removed member still holds a capability that admits it")
		}
	}
	if got := ledgerRows(); got != rows+1 {
		t.Fatalf("a removed member's replay minted: ledger rows %d, want %d", got, rows+1)
	}

	// A stored capability that has expired is replaced even when the
	// addresses have not moved, and the replacement is kept.
	aged := generate()
	agedLink := createInvite("aged", time.Now().Add(time.Hour))
	agedFirst, _ := redeemed(redeem("aged", aged))
	agedBinding, err := libp2ptransport.BindingFromPublicKey(aged.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	stored, ok, err := state.GetOpenInviteRedemption(ctx, agedLink, agedBinding.MemberID.String())
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteRedemption: ok=%t err=%v", ok, err)
	}
	var aging map[string]any
	if err := json.Unmarshal(stored.Result, &aging); err != nil {
		t.Fatal(err)
	}
	stale := agedFirst.Capability
	stale.IssuedAtMS = time.Now().Add(-2 * time.Hour).UnixMilli()
	stale.ExpiresAtMS = time.Now().Add(-time.Hour).UnixMilli()
	aging["capability"] = stale
	agingResult, err := json.Marshal(aging)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.CompleteOpenInviteRedemption(ctx, agedLink, agedBinding.MemberID.String(), agingResult, time.Now().UnixMilli()); err != nil {
		t.Fatalf("age the stored capability: %v", err)
	}
	renewed, renewedBody := redeemed(redeem("aged", aged))
	if renewed.Capability.Nonce == stale.Nonce || renewed.Capability.ExpiresAtMS <= time.Now().UnixMilli() {
		t.Fatal("an expired stored capability was replayed")
	}
	if _, body := redeemed(redeem("aged", aged)); !bytes.Equal(body, renewedBody) {
		t.Fatal("a replay after the renewal returned different bytes")
	}
	rows += 2 // the aged identity's first redemption and its one renewal

	// The removed member's capability got it in once; it gets the same bytes
	// back, not a fresh nonce to rejoin through an exhausted link.
	if _, body := redeemed(redeem("used", removed)); !bytes.Equal(body, removedBody) {
		t.Fatalf("a removed member's replay returned different bytes:\n%s\nwant\n%s", body, removedBody)
	}
	if err := session.group.CheckInvite(removedGrant.Capability, time.Now().UnixMilli()); err == nil {
		t.Fatal("the removed member's spent capability still admits")
	}
	if got := ledgerRows(); got != rows+1 {
		t.Fatalf("the removed member's replay minted: ledger rows %d, want %d", got, rows+1)
	}
	if useCount(used) != 1 {
		t.Fatalf("the removed member's replay changed the use count to %d", useCount(used))
	}

	// The existing refusals hold.
	refused(redeem("link", generate()), "open_invite_exhausted")
	if useCount(link) != 1 {
		t.Fatalf("refused stranger changed the use count to %d", useCount(link))
	}
	if _, _, err := state.RevokeOpenInvite(ctx, link, time.Now().UnixMilli()); err != nil {
		t.Fatalf("RevokeOpenInvite: %v", err)
	}
	refused(redeem("link", stuck), "open_invite_revoked")
	expiresAt := time.Now().Add(time.Second)
	createInvite("short", expiresAt)
	redeemed(redeem("short", stuck))
	time.Sleep(time.Until(expiresAt) + 10*time.Millisecond)
	refused(redeem("short", stuck), "open_invite_expired")
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
			ln, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatalf("listen unix: %v", err)
			}
			var requests atomic.Int32
			done := make(chan struct{})
			go func() {
				defer close(done)
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					requests.Add(1)
					msgType, body, err := ipc.ReadFrame(conn)
					var req ipc.InviteCreateReq
					if err == nil && (msgType == ipc.MsgInviteCreateReq || daemon.mintsAnyRequest) && json.Unmarshal(body, &req) == nil {
						capability := entmoot.BootstrapCapability{GroupID: req.GroupID, TargetPublicKey: req.TargetPublicKey}
						_, _ = rand.Read(capability.Nonce[:])
						_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{Status: "created", GroupID: req.GroupID, Capability: capability})
					}
					_ = conn.Close()
				}
			}()
			defer func() {
				_ = ln.Close()
				<-done
			}()
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
			joiner, err := keystore.Generate()
			if err != nil {
				t.Fatal(err)
			}
			info := mustDaemonNodeInfo(t, joiner)
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
