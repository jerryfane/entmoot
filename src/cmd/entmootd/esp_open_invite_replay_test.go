package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store/storetest"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// TestOpenInviteRepeatRedemptionMintsFromCurrentAddresses covers an identity
// that redeemed an open invite before the node announced its WSS address. The
// ESP used to replay the capability stored at the first redemption, so that
// identity was handed a TCP-only grant forever and a restricted cloud could
// never join with it. A repeat redemption must be minted again from what the
// daemon announces now, without spending another use, and the invite's
// revocation, expiry and use limit must still hold.
func TestOpenInviteRepeatRedemptionMintsFromCurrentAddresses(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	gid := testESPGroupID(31)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	daemon := &mintingInviteDaemon{
		identity:   founder,
		founder:    group.Founder(),
		rosterHead: group.Canonical().ID,
	}
	mustCloseGroup(t, group)
	tcp := "/ip4/203.0.113.10/tcp/1004/p2p/" + founderInfo.PeerID
	wss := "/dns4/moot.example/tcp/443/tls/sni/moot.example/ws/p2p/" + founderInfo.PeerID
	daemon.announce(tcp)
	sock := testUnixSocketPath(t)
	stop := daemon.serve(t, sock)
	defer stop()

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
	redeemed := func(response *httptest.ResponseRecorder) redemption {
		t.Helper()
		if response.Code != http.StatusOK {
			t.Fatalf("redeem: %d %s", response.Code, response.Body.String())
		}
		var got redemption
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatalf("redeem response: %v", err)
		}
		return got
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

	joiner, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	joinerBinding, err := libp2ptransport.BindingFromPublicKey(joiner.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	link := createInvite("link", time.Now().Add(time.Hour))
	first := redeemed(redeem("link", joiner))
	if slices.Contains(first.Capability.AllowedMultiaddrs, wss) {
		t.Fatalf("first capability already carries the WSS address: %v", first.Capability.AllowedMultiaddrs)
	}

	daemon.announce(tcp, wss)
	again := redeemed(redeem("link", joiner))
	if !slices.Contains(again.Capability.AllowedMultiaddrs, wss) {
		t.Fatalf("repeat redemption replayed the old addresses %v, want the announced WSS address", again.Capability.AllowedMultiaddrs)
	}
	if err := libp2ptransport.VerifyBootstrapCapability(again.Capability, joinerBinding.PeerID, time.Now()); err != nil {
		t.Fatalf("repeat capability does not verify for its redeemer: %v", err)
	}
	if again.Capability.TargetMemberID != joinerBinding.MemberID || !bytes.Equal(again.Capability.TargetPublicKey, joiner.PublicKey) {
		t.Fatal("repeat capability is not bound to the redeeming identity")
	}
	if again.UseCount != 1 || useCount(link) != 1 {
		t.Fatalf("repeat redemption spent a use: response %d, stored %d, want 1", again.UseCount, useCount(link))
	}
	stored, ok, err := state.GetOpenInviteRedemption(ctx, link, joinerBinding.MemberID.String())
	if err != nil || !ok {
		t.Fatalf("GetOpenInviteRedemption: ok=%t err=%v", ok, err)
	}
	var storedResult redemption
	if err := json.Unmarshal(stored.Result, &storedResult); err != nil || storedResult.Capability.Nonce != again.Capability.Nonce {
		t.Fatalf("stored result is not the capability just returned (err %v)", err)
	}

	stranger, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	refused(redeem("link", stranger), "open_invite_exhausted")
	if useCount(link) != 1 {
		t.Fatalf("refused stranger changed the use count to %d", useCount(link))
	}

	if _, _, err := state.RevokeOpenInvite(ctx, link, time.Now().UnixMilli()); err != nil {
		t.Fatalf("RevokeOpenInvite: %v", err)
	}
	refused(redeem("link", joiner), "open_invite_revoked")

	expiresAt := time.Now().Add(time.Second)
	createInvite("short", expiresAt)
	redeemed(redeem("short", joiner))
	time.Sleep(time.Until(expiresAt) + 10*time.Millisecond)
	refused(redeem("short", joiner), "open_invite_expired")
}

// mintingInviteDaemon answers invite_create the way the daemon does: a
// capability signed by the founder, carrying whatever addresses the node
// announces at the moment of the request.
type mintingInviteDaemon struct {
	identity   *keystore.Identity
	founder    entmoot.NodeInfo
	rosterHead entmoot.RosterEntryID

	mu    sync.Mutex
	addrs []string
}

func (d *mintingInviteDaemon) announce(addrs ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addrs = addrs
}

func (d *mintingInviteDaemon) mint(req *ipc.InviteCreateReq) (entmoot.BootstrapCapability, error) {
	d.mu.Lock()
	addrs := append([]string(nil), d.addrs...)
	d.mu.Unlock()
	member, err := entmoot.MemberIDFromPublicKey(req.TargetPublicKey)
	if err != nil {
		return entmoot.BootstrapCapability{}, err
	}
	peer, err := entmoot.PeerIDFromPublicKey(req.TargetPublicKey)
	if err != nil {
		return entmoot.BootstrapCapability{}, err
	}
	now := time.Now()
	capability := entmoot.BootstrapCapability{
		GroupID:           req.GroupID,
		TargetPublicKey:   append([]byte(nil), req.TargetPublicKey...),
		TargetMemberID:    member,
		TargetPeerID:      peer,
		Founder:           d.founder,
		RosterHead:        d.rosterHead,
		AllowedPeerIDs:    []string{d.founder.PeerID},
		AllowedMultiaddrs: addrs,
		IssuedAtMS:        now.UnixMilli(),
		ExpiresAtMS:       now.Add(24 * time.Hour).UnixMilli(),
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		return entmoot.BootstrapCapability{}, err
	}
	err = libp2ptransport.SignBootstrapCapability(d.identity, &capability)
	return capability, err
}

func (d *mintingInviteDaemon) serve(t *testing.T, sock string) func() {
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
			_, payload, err := ipc.ReadAndDecode(conn)
			if req, ok := payload.(*ipc.InviteCreateReq); err == nil && ok {
				if capability, err := d.mint(req); err == nil {
					_ = ipc.EncodeAndWrite(conn, &ipc.InviteCreateResp{Status: "created", GroupID: req.GroupID, Capability: capability})
				} else {
					_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, Message: err.Error()})
				}
			} else {
				_ = ipc.EncodeAndWrite(conn, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "unexpected request"})
			}
			_ = conn.Close()
		}
	}()
	return func() {
		_ = ln.Close()
		<-done
	}
}
