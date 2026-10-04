package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// espPublishNode is a founder daemon as `esp serve` runs it: a real group
// runtime, its control socket, and an ESP handler whose publisher talks to
// that socket.
type espPublishNode struct {
	root      string
	gid       entmoot.GroupID
	founder   *keystore.Identity
	runtime   *groupRuntime
	session   *groupSession
	messages  store.MessageStore
	publisher controlSocketSignedPublisher
	esp       *httptest.Server
}

func startESPPublishNode(t *testing.T, ctx context.Context, root string, gid entmoot.GroupID, founder *keystore.Identity) *espPublishNode {
	t.Helper()
	runtime, session, host := startTestRuntime(t, ctx, root, founder, gid)
	t.Cleanup(func() { _ = host.Close() })
	t.Cleanup(runtime.Close)

	binding, err := libp2ptransport.BindingFromPublicKey(founder.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	sockPath := controlSocketPath(root)
	daemon := &ipcServer{
		memberID:          binding.MemberID,
		peerID:            binding.PeerID.String(),
		identity:          founder,
		dataDir:           root,
		controlSocketPath: sockPath,
		runtime:           runtime,
	}
	listener, err := ipc.Listen(sockPath, "unix")
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go daemon.acceptLoop(ctx, listener)

	resources, err := openMailboxServiceResources(&globalFlags{data: root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resources.close)
	publisher := controlSocketSignedPublisher{socketPath: sockPath, timeout: 10 * time.Second}
	handler, err := esphttp.NewHandler(esphttp.Config{
		AuthMode:      esphttp.AuthModeDevice,
		Devices:       mustEmptyMemberRegistry(t),
		Service:       resources.service,
		Publisher:     publisher,
		Groups:        localGroupCatalog{dataDir: root},
		GroupExists:   espGroupExists(root),
		MemberRoster:  &esphttp.MembershipRoster{Root: root},
		MemberDevices: mustEmptyMemberRegistry(t),
		MemberConnect: esphttp.MemberConnectConfig{
			Enabled:      true,
			RegistryPath: filepath.Join(root, espMemberDevicesFile),
		},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &espPublishNode{
		root: root, gid: gid, founder: founder,
		runtime: runtime, session: session, messages: resources.store,
		publisher: publisher, esp: server,
	}
}

// connectESPMember runs `entmootd esp connect` for a member against the node.
func (n *espPublishNode) connectESPMember(t *testing.T, member *keystore.Identity) *globalFlags {
	t.Helper()
	flags := daemonFlags(t, t.TempDir(), member)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdESPConnect(flags, []string{"-esp", n.esp.URL, "-group", n.gid.String()})
	})
	if code != exitOK {
		t.Fatalf("esp connect exit = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	return flags
}

func (n *espPublishNode) stored(t *testing.T, ctx context.Context, id entmoot.MessageID) bool {
	t.Helper()
	found, err := n.messages.Has(ctx, n.gid, id)
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// publishResultJSON reads a publisher result the way the ESP renders it.
func publishResultJSON(t *testing.T, result esphttp.PublishResult) map[string]any {
	t.Helper()
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// A member that is not the ESP's daemon publishes through `entmootd esp
// publish`. The daemon cannot gossip a message another member authored, so it
// stores it, and a third member's daemon fetches it by history catch-up.
func TestESPMemberPublishReachesMembersThroughHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0x5e)
	founder, _ := mustDaemonIdentity(t)
	memberB, memberBInfo := mustDaemonIdentity(t)
	memberC, memberCInfo := mustDaemonIdentity(t)

	founderRoot := t.TempDir()
	memberCRoot := t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, founderRoot, gid)
	joinB := mustJoinWithInvite(t, group, memberB, mustDaemonInvite(t, group, founder, memberBInfo, 1))
	joinC := mustJoinWithInvite(t, group, memberC, mustDaemonInvite(t, group, founder, memberCInfo, 1))
	checkpoint := group.Canonical()
	mustCloseGroup(t, group)
	cGroup, err := membership.Adopt(memberCRoot, checkpoint)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	for _, join := range []membership.Record{joinB, joinC} {
		if _, err := cGroup.Apply(join); err != nil {
			t.Fatalf("apply join on member C: %v", err)
		}
	}
	mustCloseGroup(t, cGroup)

	node := startESPPublishNode(t, ctx, founderRoot, gid, founder)
	cRuntime, cSession, cHost := startTestRuntime(t, ctx, memberCRoot, memberC, gid)
	defer cHost.Close()
	defer cRuntime.Close()

	// B has no daemon in this group at all: only its identity and the ESP.
	flagsB := node.connectESPMember(t, memberB)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdESPPublish(flagsB, []string{"-group", gid.String(), "-topic", "general", "-content", "posted from B's device"})
	})
	if code != exitOK {
		t.Fatalf("esp publish exit = %d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var published struct {
		Status         string            `json:"status"`
		Delivery       string            `json:"delivery"`
		MessageID      entmoot.MessageID `json:"message_id"`
		AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
	}
	if err := json.Unmarshal([]byte(stdout), &published); err != nil {
		t.Fatalf("decode publish output %q: %v", stdout, err)
	}
	if published.Status != "accepted" || published.Delivery != string(libp2ptransport.DeliveryPendingHistory) {
		t.Fatalf("publish = %+v, want accepted for history delivery", published)
	}
	if published.AuthorMemberID != *memberBInfo.MemberID {
		t.Fatalf("publish author = %s, want member B", published.AuthorMemberID)
	}
	if !node.stored(t, ctx, published.MessageID) {
		t.Fatal("the ESP's daemon did not store B's message")
	}

	// The ESP's own history serves it back to B.
	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdESPHistory(flagsB, []string{"-group", gid.String()})
	})
	if code != exitOK {
		t.Fatalf("esp history exit = %d stderr=%s", code, stderr)
	}
	var history struct {
		Messages []struct {
			MessageID      entmoot.MessageID `json:"message_id"`
			AuthorMemberID entmoot.MemberID  `json:"author_member_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(stdout), &history); err != nil {
		t.Fatalf("decode history %q: %v", stdout, err)
	}
	inHistory := false
	for _, message := range history.Messages {
		inHistory = inHistory || (message.MessageID == published.MessageID && message.AuthorMemberID == *memberBInfo.MemberID)
	}
	if !inHistory {
		t.Fatalf("ESP history does not show B's message: %s", stdout)
	}

	// C catches up from the founder, the only keeper holding the message.
	founderHost := node.runtime.host
	if err := cHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatalf("connect C to founder: %v", err)
	}
	for {
		cRuntime.catchUp(ctx, cSession)
		found, err := cRuntime.store.Has(ctx, gid, published.MessageID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("member C never fetched B's message through history catch-up")
		case <-time.After(100 * time.Millisecond):
		}
	}
	received, err := cRuntime.store.Get(ctx, gid, published.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if received.Author.MemberID == nil || *received.Author.MemberID != *memberBInfo.MemberID || string(received.Content) != "posted from B's device" {
		t.Fatalf("C stored %+v, want B's message", received)
	}
	if err := signing.VerifyMessage(received, memberBInfo); err != nil {
		t.Fatalf("C's copy does not verify under B's key: %v", err)
	}
}

// The daemon stores another member's message only after the checks a
// receiving member applies, and the ESP keeps a device to its bound member.
// A message the daemon itself authored still goes out live.
func TestESPSignedPublishVerifiesForeignAuthors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0x6e)
	unserved := daemonTestGroupID(0x7e)
	founder, founderInfo := mustDaemonIdentity(t)
	memberB, memberBInfo := mustDaemonIdentity(t)
	memberC, memberCInfo := mustDaemonIdentity(t)
	removed, removedInfo := mustDaemonIdentity(t)
	outsider, _ := mustDaemonIdentity(t)

	root := t.TempDir()
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	for _, joiner := range []struct {
		identity *keystore.Identity
		info     entmoot.NodeInfo
	}{{memberB, memberBInfo}, {memberC, memberCInfo}, {removed, removedInfo}} {
		mustJoinWithInvite(t, group, joiner.identity, mustDaemonInvite(t, group, founder, joiner.info, 1))
	}
	mustCloseGroup(t, group)

	node := startESPPublishNode(t, ctx, root, gid, founder)
	head := node.session.group.Canonical().ID
	sign := func(author *keystore.Identity, gid entmoot.GroupID, content string) entmoot.Message {
		t.Helper()
		message, err := buildESPSignedMessage(ctx, author, gid, head, []string{"general"}, []byte(content), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		return message
	}
	if _, err := node.session.group.SignRecord(founder, membership.Record{Kind: membership.KindRemove, Subject: removedInfo}); err != nil {
		t.Fatalf("remove member: %v", err)
	}

	forged := sign(memberB, gid, "not really from B")
	signingBytes, err := signing.MessageSigningBytes(forged)
	if err != nil {
		t.Fatal(err)
	}
	forged.Signature = outsider.Sign(signingBytes)

	refusals := []struct {
		name    string
		message entmoot.Message
		status  int
		code    string
	}{
		{"forged signature", forged, http.StatusBadRequest, "bad_request"},
		{"non-member author", sign(outsider, gid, "let me in"), http.StatusForbidden, "not_member"},
		{"removed member", sign(removed, gid, "still here?"), http.StatusForbidden, "not_member"},
		{"group not served", sign(memberB, unserved, "wrong moot"), http.StatusNotFound, "group_not_found"},
	}
	for _, refusal := range refusals {
		_, err := node.publisher.PublishSigned(ctx, refusal.message)
		pubErr, ok := err.(*esphttp.PublishError)
		if !ok || pubErr.HTTPStatus != refusal.status || pubErr.Code != refusal.code {
			t.Fatalf("%s: error = %v (%#v), want %d %s", refusal.name, err, err, refusal.status, refusal.code)
		}
		if node.stored(t, ctx, refusal.message.ID) {
			t.Fatalf("%s: the daemon stored a refused message", refusal.name)
		}
	}

	// B's device may not publish a message C signed, even a valid one.
	flagsB := node.connectESPMember(t, memberB)
	clientB, err := openESPClient(flagsB)
	if err != nil {
		t.Fatal(err)
	}
	fromC := sign(memberC, gid, "C's words through B's device")
	body, err := json.Marshal(map[string]entmoot.Message{"message": fromC})
	if err != nil {
		t.Fatal(err)
	}
	var out json.RawMessage
	if err := clientB.do(ctx, http.MethodPost, espGroupPath(gid, "messages"), body, &out); err == nil {
		t.Fatalf("B's device published C's message: %s", out)
	}
	if node.stored(t, ctx, fromC.ID) {
		t.Fatal("the daemon stored a message the ESP refused")
	}

	// A message the daemon itself authored keeps the live path.
	own := sign(founder, gid, "from the founder")
	if own.Author.PeerID != founderInfo.PeerID {
		t.Fatal("fixture: founder message names another peer")
	}
	result, err := node.publisher.PublishSigned(ctx, own)
	if err != nil {
		t.Fatalf("founder signed publish: %v", err)
	}
	if delivery := publishResultJSON(t, result)["delivery"]; delivery != string(libp2ptransport.DeliveryPublished) {
		t.Fatalf("founder delivery = %v, want %s", delivery, libp2ptransport.DeliveryPublished)
	}
	if !node.stored(t, ctx, own.ID) {
		t.Fatal("the daemon did not store its own message")
	}
}
