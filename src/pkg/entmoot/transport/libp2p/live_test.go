package libp2ptransport

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	_ "modernc.org/sqlite"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

func TestThreePeerGossipSubPersistsAndEmitsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	identities := []*keystore.Identity{mustIdentity(t), mustIdentity(t), mustIdentity(t)}
	hosts := make([]host.Host, 3)
	for i := range hosts {
		var err error
		hosts[i], _, err = NewHost(ctx, identities[i], libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
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
	groupID, group := mustOpenGroup(t, identities[0], identities[1], identities[2])
	stores := []store.MessageStore{store.NewMemory(), store.NewMemory(), store.NewMemory()}
	groups := make([]*LiveGroup, 3)
	var ingests [3]atomic.Int32
	for i := range groups {
		index := i
		var err error
		groups[i], err = NewLiveGroup(ctx, LiveConfig{Host: hosts[i], GroupID: groupID, Group: group, Store: stores[i], OnIngest: func(entmoot.Message) { ingests[index].Add(1) }})
		if err != nil {
			t.Fatal(err)
		}
		defer groups[i].Close()
	}
	time.Sleep(1500 * time.Millisecond)
	message := signedLiveMessage(t, identities[0], group, groupID, 10_000, "valid")
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
	if _, err := group.SignRecord(identities[0], membership.Record{
		Kind: membership.KindRemove, Subject: mustNode(t, identities[1]),
	}); err != nil {
		t.Fatal(err)
	}
	afterRemoval := signedLiveMessage(t, identities[0], group, groupID, 11_000, "after removal")
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
	founderHost, _, err := NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	memberHost, _, err := NewHost(ctx, member, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer memberHost.Close()
	outsiderHost, _, err := NewHost(ctx, outsider, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer outsiderHost.Close()
	groupID, group := mustOpenGroup(t, founder, member)
	if _, err := NewLiveGroup(ctx, LiveConfig{Host: outsiderHost, GroupID: groupID, Group: group, Store: store.NewMemory()}); err == nil {
		t.Fatal("non-member created an authorized live group")
	}
	if err := memberHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if err := outsiderHost.Connect(ctx, peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	founderLive, err := NewLiveGroup(ctx, LiveConfig{Host: founderHost, GroupID: groupID, Group: group, Store: store.NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	defer founderLive.Close()
	memberLive, err := NewLiveGroup(ctx, LiveConfig{Host: memberHost, GroupID: groupID, Group: group, Store: store.NewMemory()})
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
	message := signedLiveMessage(t, founder, group, groupID, 20_000, "members only")
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
	for i := range hosts {
		var err error
		hosts[i], _, err = NewHost(ctx, identities[i], libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
		if err != nil {
			t.Fatal(err)
		}
		defer hosts[i].Close()
	}
	groupOne, membershipOne := mustOpenGroup(t, identities[0], identities[1])
	groupTwo, membershipTwo := mustOpenGroup(t, identities[0], identities[1])
	stores := []store.MessageStore{store.NewMemory(), store.NewMemory()}
	defer stores[0].Close()
	defer stores[1].Close()
	specs := []struct {
		id    entmoot.GroupID
		group *membership.Group
		label string
	}{{groupOne, membershipOne, "one"}, {groupTwo, membershipTwo, "two"}}
	groups := make([][]*LiveGroup, len(hosts))
	for i := range hosts {
		router, err := NewLiveRouter(ctx, hosts[i])
		if err != nil {
			t.Fatal(err)
		}
		defer router.Close()
		for _, spec := range specs {
			group, err := router.AddGroup(ctx, LiveConfig{Host: hosts[i], GroupID: spec.id, Group: spec.group, Store: stores[i]})
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
	for index, spec := range specs {
		message := signedLiveMessage(t, identities[0], spec.group, spec.id, time.Now().UnixMilli(), spec.label)
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
	localHost, _, err := NewHost(ctx, identity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()
	groupID, membershipGroup := mustOpenGroup(t, identity)
	messageStore := store.NewMemory()
	defer messageStore.Close()
	var calls atomic.Int32
	group, err := NewLiveGroup(ctx, LiveConfig{
		Host: localHost, GroupID: groupID, Group: membershipGroup, Store: messageStore,
		Authorize: func(entmoot.Message) error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	message := signedLiveMessage(t, identity, membershipGroup, groupID, time.Now().UnixMilli(), "one charge")
	if _, err := group.Publish(ctx, message); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("authorization called %d times, want 1", got)
	}
}

// Version-0 messages predate member ids and checkpoints: their author and the
// set of messages carried over at conversion both come from the linear chain
// the group upgraded from, which a group keeps on disk beside its membership
// store.
func TestLegacyHistoryRequiresFounderCommittedMessageID(t *testing.T) {
	root := t.TempDir()
	groupID := mustGroupID(t)
	founderIdentity := mustIdentity(t)
	removedIdentity := mustIdentity(t)
	founder := mustNodeInfo(t, founderIdentity.PublicKey)
	removed := mustNodeInfo(t, removedIdentity.PublicKey)

	chain := []entmoot.RosterEntry{legacyEntry(t, founderIdentity, nil, "add", founder, nil, 1_000)}
	chain = append(chain, legacyEntry(t, founderIdentity, chain, "add", removed, nil, 2_000))

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
	chain = append(chain, legacyEntry(t, founderIdentity, chain, "remove", removed, nil, 3_000))

	tree := merkle.New([]entmoot.MessageID{accepted.ID})
	root32 := tree.Root()
	policy, err := canonical.Encode(entmoot.LegacyIdentityUpgradePolicy{
		Type: "legacy_identity_upgrade", MappingsSHA256: hex.EncodeToString(make([]byte, 32)),
		LegacyHistoryRoot: hex.EncodeToString(root32[:]), LegacyHistoryCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	chain = append(chain, legacyEntry(t, founderIdentity, chain, "policy_change", entmoot.NodeInfo{}, policy, 3_001))
	writeLegacyChain(t, root, groupID, chain)

	// The upgrade path, not Create: a group that holds a chain must mint a
	// checkpoint 0 that names that chain's head, or the checkpoint would
	// silently discard the membership the chain records.
	group := mustUpgradeLegacyGroup(t, root, groupID, founderIdentity)
	defer group.Close()
	if group.Legacy() == nil {
		t.Fatal("group did not load the legacy chain beside its membership store")
	}

	proof, err := tree.Proof(accepted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyHistoricalMessageWithProof(group, accepted, time.Now(), &proof); err != nil {
		t.Fatalf("committed legacy message rejected: %v", err)
	}
	forged := signLegacy("forged after removal", 4_000)
	if err := VerifyHistoricalMessageWithProof(group, forged, time.Now(), &proof); err == nil {
		t.Fatal("uncommitted legacy message was accepted with another message's proof")
	}
}

// legacyEntry builds one entry of a pre-checkpoint linear chain, linked to the
// entry before it and signed by the founder, as the chain on disk is.
func legacyEntry(t *testing.T, signer *keystore.Identity, previous []entmoot.RosterEntry, op string, subject entmoot.NodeInfo, policy json.RawMessage, timestamp int64) entmoot.RosterEntry {
	t.Helper()
	entry := entmoot.RosterEntry{Op: op, Subject: subject, Policy: policy, Timestamp: timestamp}
	if len(previous) > 0 {
		entry.Parents = []entmoot.RosterEntryID{previous[len(previous)-1].ID}
	}
	payload, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.Signature = signer.Sign(payload)
	entry.ID = canonical.RosterEntryID(entry)
	return entry
}

// writeLegacyChain stores a linear chain in the group's data directory, where
// membership finds it when the group is opened.
func writeLegacyChain(t *testing.T, root string, groupID entmoot.GroupID, entries []entmoot.RosterEntry) {
	t.Helper()
	dir := filepath.Join(root, "groups", membership.GroupDirName(groupID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "roster.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE roster_entries (
  entry_id        BLOB PRIMARY KEY,
  group_id        BLOB NOT NULL,
  sequence        INTEGER NOT NULL,
  parent_id       BLOB,
  canonical_bytes BLOB NOT NULL,
  op              TEXT NOT NULL,
  timestamp_ms    INTEGER NOT NULL,
  UNIQUE (group_id, sequence)
);`); err != nil {
		t.Fatal(err)
	}
	for sequence, entry := range entries {
		encoded, err := canonical.Encode(entry)
		if err != nil {
			t.Fatal(err)
		}
		var parent []byte
		if len(entry.Parents) == 1 {
			parent = entry.Parents[0][:]
		}
		if _, err := db.Exec(
			`INSERT INTO roster_entries (entry_id, group_id, sequence, parent_id, canonical_bytes, op, timestamp_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?);`,
			entry.ID[:], groupID[:], sequence, parent, encoded, entry.Op, entry.Timestamp); err != nil {
			t.Fatal(err)
		}
	}
}

func signedLiveMessage(t *testing.T, identity *keystore.Identity, group *membership.Group, groupID entmoot.GroupID, timestamp int64, content string) entmoot.Message {
	t.Helper()
	author := mustNodeInfo(t, identity.PublicKey)
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		t.Fatal(err)
	}
	head := group.Canonical().ID
	message, err := signer.SignMessage(context.Background(), entmoot.Message{Version: 2, GroupID: groupID, Timestamp: timestamp, Topics: []string{"live"}, Content: []byte(content), RosterHead: &head})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyLiveMessage(group, message, time.Now()); err != nil {
		t.Fatal(err)
	}
	return message
}

// Any current member publishes on its own signature. No other member has to be
// reachable, which is what keeps a group usable when the founder is offline.
func TestNonFounderMemberPublishesWithoutAnyOtherAuthority(t *testing.T) {
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	groupID, group := mustOpenGroup(t, founderIdentity, memberIdentity)
	author := mustNodeInfo(t, memberIdentity.PublicKey)
	signer, err := signing.NewLocalSigner(author, memberIdentity)
	if err != nil {
		t.Fatal(err)
	}
	head := group.Canonical().ID
	message, err := signer.SignMessage(context.Background(), entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: time.Now().UnixMilli(),
		Topics: []string{"live"}, Content: []byte("member authored"), RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyLiveMessage(group, message, time.Now()); err != nil {
		t.Fatalf("member message did not verify: %v", err)
	}
	// Removal is the moderation lever: once out of the group the same message
	// stops verifying.
	if _, err := group.SignRecord(founderIdentity, membership.Record{
		Kind: membership.KindRemove, Subject: author,
	}); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLiveMessage(group, message, time.Now()); err == nil {
		t.Fatal("removed member still authorised to publish")
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

// mustUpgradeLegacyGroup mints checkpoint 0 from a linear roster chain the way
// `entmootd membership upgrade` does, binding it to the chain it replaces.
func mustUpgradeLegacyGroup(t *testing.T, root string, groupID entmoot.GroupID, founderIdentity *keystore.Identity) *membership.Group {
	t.Helper()
	legacy, err := membership.LoadLegacyChain(root, groupID)
	if err != nil {
		t.Fatal(err)
	}
	founder := legacy.Founder()
	state := membership.State{
		Founder:        founder,
		Members:        make(map[entmoot.MemberID]entmoot.NodeInfo),
		Policy:         membership.DefaultPolicy(),
		Banned:         make(map[entmoot.MemberID]struct{}),
		RevokedInvites: make(map[[32]byte]struct{}),
		InviteUses:     make(map[[32]byte]int),
	}
	state.Policy.Admins = membership.SortAdmins(legacy.Admins())
	for _, member := range legacy.Members() {
		id, err := entmoot.ResolvedMemberID(member)
		if err != nil {
			t.Fatal(err)
		}
		state.Members[id] = member
	}
	head := legacy.Head()
	body := state.Checkpoint(groupID, 0, entmoot.RosterEntryID{}, 0, time.Now().UnixMilli())
	body.LegacyHead = &head
	signed, err := membership.SignCheckpoint(founderIdentity, founder, body)
	if err != nil {
		t.Fatal(err)
	}
	group, err := membership.Adopt(root, signed)
	if err != nil {
		t.Fatal(err)
	}
	return group
}
