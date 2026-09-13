package libp2ptransport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

func TestThreePeerGossipSubPersistsAndEmitsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	identities := []*keystore.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	hosts := make([]host.Host, 3)
	bindings := make([]Binding, 3)
	for i := range hosts {
		var err error
		hosts[i], bindings[i], err = NewHost(ctx, identities[i], libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		defer hosts[i].Close()
	}
	for i := 1; i < len(hosts); i++ {
		if err := hosts[i].Connect(ctx, peer.AddrInfo{ID: hosts[0].ID(), Addrs: hosts[0].Addrs()}); err != nil {
			t.Fatal(err)
		}
	}
	groupID, rosterLog := liveRoster(t, identities, bindings)
	stores := []store.MessageStore{store.NewMemory(), store.NewMemory(), store.NewMemory()}
	groups := make([]*LiveGroup, 3)
	var ingests [3]atomic.Int32
	for i := range groups {
		index := i
		var err error
		groups[i], err = NewLiveGroup(ctx, LiveConfig{Host: hosts[i], GroupID: groupID, Roster: rosterLog, Store: stores[i], OnIngest: func(entmoot.Message) { ingests[index].Add(1) }})
		if err != nil {
			t.Fatal(err)
		}
		defer groups[i].Close()
	}
	time.Sleep(1500 * time.Millisecond)
	message := signedAcceptedLiveMessage(t, identities[0], bindings[0].MemberID, rosterLog, groupID, 10_000, "valid")
	tampered := message
	tampered.Content = []byte("malformed variant")
	rawTampered, err := json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	_ = groups[1].topic.Publish(ctx, rawTampered)
	state, err := groups[0].Publish(ctx, message)
	if err != nil || state != DeliveryPublished {
		t.Fatalf("publish state=%q err=%v", state, err)
	}
	waitForStoredMessage(t, ctx, stores[1], groupID, message.ID)
	waitForStoredMessage(t, ctx, stores[2], groupID, message.ID)
	if ingests[0].Load() != 1 || ingests[1].Load() != 1 || ingests[2].Load() != 1 {
		t.Fatalf("ingest counts = %d,%d,%d", ingests[0].Load(), ingests[1].Load(), ingests[2].Load())
	}
	state, err = groups[0].Publish(ctx, message)
	if err != nil || state != DeliveryAlreadyStored {
		t.Fatalf("duplicate publish state=%q err=%v", state, err)
	}
	time.Sleep(200 * time.Millisecond)
	if ingests[0].Load() != 1 || ingests[1].Load() != 1 || ingests[2].Load() != 1 {
		t.Fatalf("duplicate emitted again: %d,%d,%d", ingests[0].Load(), ingests[1].Load(), ingests[2].Load())
	}
	removeEntry, err := rosterLog.SignEntry(identities[0], "remove", mustNodeInfo(t, identities[1].PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(removeEntry); err != nil {
		t.Fatal(err)
	}
	afterRemoval := signedAcceptedLiveMessage(t, identities[0], bindings[0].MemberID, rosterLog, groupID, 11_000, "after removal")
	if _, err := groups[0].Publish(ctx, afterRemoval); err != nil {
		t.Fatal(err)
	}
	waitForStoredMessage(t, ctx, stores[2], groupID, afterRemoval.ID)
	time.Sleep(300 * time.Millisecond)
	if has, err := stores[1].Has(ctx, groupID, afterRemoval.ID); err != nil || has {
		t.Fatalf("removed member stored live message: has=%v err=%v", has, err)
	}
}

func TestUnauthorizedPeerCannotJoinAuthorizedTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founder := mustIdentity(t)
	member := mustIdentity(t)
	outsider := mustIdentity(t)
	founderHost, founderBinding, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	memberHost, memberBinding, err := NewHost(ctx, member, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer memberHost.Close()
	outsiderHost, outsiderBinding, err := NewHost(ctx, outsider, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer outsiderHost.Close()
	groupID, rosterLog := liveRoster(t, []*keystore.Identity{founder, member}, []Binding{founderBinding, memberBinding})
	if _, err := NewLiveGroup(ctx, LiveConfig{Host: outsiderHost, GroupID: groupID, Roster: rosterLog, Store: store.NewMemory()}); err == nil {
		t.Fatal("non-member created an authorized live group")
	}
	_ = outsiderBinding
	if err := memberHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if err := outsiderHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	founderLive, err := NewLiveGroup(ctx, LiveConfig{Host: founderHost, GroupID: groupID, Roster: rosterLog, Store: store.NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	defer founderLive.Close()
	memberLive, err := NewLiveGroup(ctx, LiveConfig{Host: memberHost, GroupID: groupID, Roster: rosterLog, Store: store.NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	defer memberLive.Close()
	outsiderPubSub, err := pubsub.NewGossipSub(ctx, outsiderHost, pubsub.WithMessageSignaturePolicy(pubsub.StrictSign))
	if err != nil {
		t.Fatal(err)
	}
	outsiderTopic, err := outsiderPubSub.Join(GroupTopic(groupID))
	if err != nil {
		t.Fatal(err)
	}
	defer outsiderTopic.Close()
	outsiderSubscription, err := outsiderTopic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer outsiderSubscription.Cancel()
	time.Sleep(1500 * time.Millisecond)
	message := signedAcceptedLiveMessage(t, founder, founderBinding.MemberID, rosterLog, groupID, 20_000, "members only")
	if _, err := founderLive.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer readCancel()
	if envelope, err := outsiderSubscription.Next(readCtx); err == nil {
		t.Fatalf("outsider received authorized topic payload from %s", envelope.GetFrom())
	}
}

func TestSharedRouterCarriesMultipleGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	identities := []*keystore.Identity{mustIdentity(t), mustIdentity(t)}
	hosts := make([]host.Host, 2)
	bindings := make([]Binding, 2)
	for i := range hosts {
		var err error
		hosts[i], bindings[i], err = NewHost(ctx, identities[i], libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		defer hosts[i].Close()
	}
	groupOne, rosterOne := liveRoster(t, identities, bindings)
	groupTwo := groupOne
	groupTwo[0]++
	rosterTwo := roster.New(groupTwo)
	if err := rosterTwo.Genesis(identities[0], mustNodeInfo(t, identities[0].PublicKey), 2_000); err != nil {
		t.Fatal(err)
	}
	add, err := rosterTwo.SignEntry(identities[0], "add", mustNodeInfo(t, identities[1].PublicKey), nil, 2_001)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterTwo.Apply(add); err != nil {
		t.Fatal(err)
	}
	stores := []store.MessageStore{store.NewMemory(), store.NewMemory()}
	defer stores[0].Close()
	defer stores[1].Close()
	specs := []struct {
		id     entmoot.GroupID
		roster *roster.RosterLog
		label  string
	}{{groupOne, rosterOne, "one"}, {groupTwo, rosterTwo, "two"}}
	groups := make([][]*LiveGroup, len(hosts))
	for i := range hosts {
		router, err := NewLiveRouter(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}
		defer router.Close()
		for _, spec := range specs {
			group, err := router.AddGroup(ctx, LiveConfig{Host: hosts[i], GroupID: spec.id, Roster: spec.roster, Store: stores[i]})
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()
			groups[i] = append(groups[i], group)
		}
	}
	if err := hosts[1].Connect(ctx, peer.AddrInfo{ID: hosts[0].ID(), Addrs: hosts[0].Addrs()}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	founderID := bindings[0].MemberID
	for index, spec := range specs {
		message := signedAcceptedLiveMessage(t, identities[0], founderID, spec.roster, spec.id, time.Now().UnixMilli(), spec.label)
		if _, err := groups[0][index].Publish(ctx, message); err != nil {
			t.Fatal(err)
		}
		waitForStoredMessage(t, ctx, stores[1], spec.id, message.ID)
	}
}

func TestLocalPublishChargesAuthorizationOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	identity := mustIdentity(t)
	localHost, binding, err := NewHost(ctx, identity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()
	groupID, rosterLog := liveRoster(t, []*keystore.Identity{identity}, []Binding{binding})
	messageStore := store.NewMemory()
	defer messageStore.Close()
	var calls atomic.Int32
	group, err := NewLiveGroup(ctx, LiveConfig{
		Host: localHost, GroupID: groupID, Roster: rosterLog, Store: messageStore,
		Authorize: func(entmoot.Message) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	message := signedAcceptedLiveMessage(t, identity, binding.MemberID, rosterLog, groupID, time.Now().UnixMilli(), "one charge")
	if _, err := group.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("authorization called %d times, want 1", got)
	}
}

func TestLegacyHistoryRequiresFounderCommittedMessageID(t *testing.T) {
	founderIdentity := mustIdentity(t)
	removedIdentity := mustIdentity(t)
	groupID := entmoot.GroupID{0x72}
	rosterLog := roster.New(groupID)
	founder := mustNodeInfo(t, founderIdentity.PublicKey)
	if err := rosterLog.Genesis(founderIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	removed := mustNodeInfo(t, removedIdentity.PublicKey)
	add, err := rosterLog.SignEntry(founderIdentity, "add", removed, nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(add); err != nil {
		t.Fatal(err)
	}
	signLegacy := func(content string, timestamp int64) entmoot.Message {
		message := entmoot.Message{Version: 0, GroupID: groupID, Author: removed, Timestamp: timestamp, Topics: []string{"legacy"}, Content: []byte(content)}
		message.ID = canonical.MessageID(message)
		payload, err := canonical.MessageSigningBytes(message)
		if err != nil {
			t.Fatal(err)
		}
		message.Signature = removedIdentity.Sign(payload)
		return message
	}
	accepted := signLegacy("accepted before removal", 2_500)
	remove, err := rosterLog.SignEntry(founderIdentity, "remove", removed, nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(remove); err != nil {
		t.Fatal(err)
	}
	tree := merkle.New([]entmoot.MessageID{accepted.ID})
	root := tree.Root()
	policy, err := canonical.Encode(entmoot.LegacyIdentityUpgradePolicy{
		Type: "legacy_identity_upgrade", MappingsSHA256: hex.EncodeToString(make([]byte, 32)),
		LegacyHistoryRoot: hex.EncodeToString(root[:]), LegacyHistoryCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	upgrade, err := rosterLog.SignEntry(founderIdentity, "policy_change", entmoot.NodeInfo{}, policy, 3_001)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(upgrade); err != nil {
		t.Fatal(err)
	}
	proof, err := tree.Proof(accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyHistoricalMessageWithProof(rosterLog, accepted, time.Now(), &proof); err != nil {
		t.Fatalf("committed legacy message rejected: %v", err)
	}
	forged := signLegacy("forged after removal", 4_000)
	if err := VerifyHistoricalMessageWithProof(rosterLog, forged, time.Now(), &proof); err == nil {
		t.Fatal("uncommitted legacy message was accepted with another message's proof")
	}
}

func liveRoster(t *testing.T, identities []*keystore.Identity, bindings []Binding) (entmoot.GroupID, *roster.RosterLog) {
	t.Helper()
	var groupID entmoot.GroupID
	groupID[0] = 0x61
	result := roster.New(groupID)
	founder := mustNodeInfo(t, identities[0].PublicKey)
	if err := result.Genesis(identities[0], founder, 1_000); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(identities); i++ {
		subject := mustNodeInfo(t, identities[i].PublicKey)
		entry, err := result.SignEntry(identities[0], "add", subject, nil, int64(1_000+i))
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Apply(entry); err != nil {
			t.Fatal(err)
		}
	}
	return groupID, result
}

func signedAcceptedLiveMessage(t *testing.T, identity *keystore.Identity, memberID entmoot.MemberID, rosterLog *roster.RosterLog, groupID entmoot.GroupID, timestamp int64, content string) entmoot.Message {
	t.Helper()
	author := mustNodeInfo(t, identity.PublicKey)
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		t.Fatal(err)
	}
	head := rosterLog.Head()
	message, err := signer.SignMessage(context.Background(), entmoot.Message{Version: 2, GroupID: groupID, Timestamp: timestamp, Topics: []string{"live"}, Content: []byte(content), RosterHead: &head})
	if err != nil {
		t.Fatal(err)
	}
	founder, ok := rosterLog.Founder()
	if !ok || string(founder.EntmootPubKey) != string(identity.PublicKey) {
		t.Fatal("test publisher must be founder")
	}
	message.Acceptance = &entmoot.MessageAcceptance{Version: 1, GroupID: groupID, MessageID: message.ID, RosterHead: head, Authority: founder}
	payload, err := canonical.MessageAcceptanceSigningBytes(*message.Acceptance)
	if err != nil {
		t.Fatal(err)
	}
	message.Acceptance.Signature = identity.Sign(payload)
	if err := VerifyLiveMessage(rosterLog, message, time.Now()); err != nil {
		t.Fatal(err)
	}
	return message
}

func TestCurrentMemberObtainsFounderAcceptance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	founderHost, founderBinding, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	memberHost, memberBinding, err := NewHost(ctx, memberIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer memberHost.Close()
	groupID, rosterLog := liveRoster(t, []*keystore.Identity{founderIdentity, memberIdentity}, []Binding{founderBinding, memberBinding})
	server := AcceptanceServer{
		Host:     founderHost,
		Identity: founderIdentity,
		Roster: func(candidate entmoot.GroupID) (*roster.RosterLog, bool) {
			return rosterLog, candidate == groupID
		},
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	author := mustNodeInfo(t, memberIdentity.PublicKey)
	signer, err := signing.NewLocalSigner(author, memberIdentity)
	if err != nil {
		t.Fatal(err)
	}
	head := rosterLog.Head()
	message, err := signer.SignMessage(ctx, entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: time.Now().UnixMilli(),
		Topics: []string{"live"}, Content: []byte("member authored"), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptance, err := RequestMessageAcceptance(ctx, memberHost, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}, message)
	if err != nil {
		t.Fatal(err)
	}
	message.Acceptance = &acceptance
	if err := VerifyLiveMessage(rosterLog, message, time.Now()); err != nil {
		t.Fatalf("accepted member message did not verify: %v", err)
	}
}

func waitForStoredMessage(t *testing.T, ctx context.Context, messageStore store.MessageStore, groupID entmoot.GroupID, messageID entmoot.MessageID) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		has, err := messageStore.Has(ctx, groupID, messageID)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}
