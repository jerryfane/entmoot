package libp2ptransport

import (
	"context"
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
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
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
	removeEntry, err := rosterLog.SignEntry(identities[0], "remove", entmoot.NodeInfo{
		EntmootPubKey: identities[1].PublicKey,
		MemberID:      &bindings[1].MemberID,
	}, nil, 0, 3_000)
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

func liveRoster(t *testing.T, identities []*keystore.Identity, bindings []Binding) (entmoot.GroupID, *roster.RosterLog) {
	t.Helper()
	var groupID entmoot.GroupID
	groupID[0] = 0x61
	result := roster.New(groupID)
	founder := entmoot.NodeInfo{EntmootPubKey: identities[0].PublicKey, MemberID: &bindings[0].MemberID}
	if err := result.Genesis(identities[0], founder, 1_000); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(identities); i++ {
		subject := entmoot.NodeInfo{EntmootPubKey: identities[i].PublicKey, MemberID: &bindings[i].MemberID}
		entry, err := result.SignEntry(identities[0], "add", subject, nil, 0, int64(1_000+i))
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
	author := entmoot.NodeInfo{EntmootPubKey: identity.PublicKey, MemberID: &memberID}
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
	if err := gossip.VerifyLiveMessage(rosterLog, message, time.Now()); err != nil {
		t.Fatal(err)
	}
	return message
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
