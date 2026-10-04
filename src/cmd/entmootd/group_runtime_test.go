package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/conversion"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

func TestGroupRuntimeServesConvertedLegacyHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	member, err := keystore.Generate()
	if err != nil {
		t.Fatal(err)
	}
	// This ID exercises padding and the alphabet differences in directory names.
	groupID := entmoot.GroupID{0xfb, 0xff, 0xff}
	groupDir := filepath.Join(root, "groups", base64.RawURLEncoding.EncodeToString(groupID[:]))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	legacyFounder := entmoot.NodeInfo{PilotNodeID: 45981, EntmootPubKey: founder.PublicKey}
	entry := entmoot.RosterEntry{Op: "add", Subject: legacyFounder, Actor: 45981, Timestamp: 1_700_000_000_000}
	entry.ID = canonical.RosterEntryID(entry)
	payload, err := canonical.RosterEntrySigningBytes(entry)
	if err != nil {
		t.Fatal(err)
	}
	entry.Signature = founder.Sign(payload)
	encoded, err := canonical.Encode(entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(groupDir, "roster.jsonl"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	message := entmoot.Message{
		GroupID: groupID, Author: legacyFounder, Timestamp: 1_700_000_000_100,
		Topics: []string{"legacy/history"}, Content: []byte("converted private history"),
	}
	message.ID = canonical.MessageID(message)
	payload, err = canonical.MessageSigningBytes(message)
	if err != nil {
		t.Fatal(err)
	}
	message.Signature = founder.Sign(payload)
	encoded, err = canonical.Encode(message)
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimeLegacyMessage(t, filepath.Join(groupDir, "messages.sqlite"), message, encoded)
	if err := conversion.Run(root, founder); err != nil {
		t.Fatal(err)
	}
	serverHost, serverBinding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	clientHost, clientBinding, err := libp2ptransport.NewHost(ctx, member, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	// The converted chain has no checkpoint, so the founder mints one before
	// the daemon can serve the group. The genesis entry of a Pilot-era chain
	// names its founder by node id alone, so the checkpoint restates that
	// founder under the member identity derived from the same key.
	mustAdoptCheckpointZero(t, root, groupID, founder)
	group, err := membership.Open(root, groupID)
	if err != nil {
		t.Fatal(err)
	}
	memberInfo := entmoot.NodeInfo{MemberID: &clientBinding.MemberID, PeerID: clientHost.ID().String(), EntmootPubKey: member.PublicKey}
	mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatal(err)
	}
	defer messages.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: serverHost, Binding: serverBinding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	session, _, err := runtime.AddLocalGroup(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	response, err := libp2ptransport.RequestHistoryPage(ctx, clientHost, peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}, libp2ptransport.HistorySyncRequest{
		Version: 2, RequestID: "converted-history", GroupID: groupID,
		Mode: "bodies", IDs: []entmoot.MessageID{message.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Messages) != 1 || len(response.LegacyProofs) != 1 || response.LegacyProofs[0].MessageID != message.ID {
		t.Fatalf("converted history response = %+v", response)
	}
	got, err := canonical.Encode(response.Messages[0])
	if err != nil {
		t.Fatal(err)
	}
	// Byte identity is the contract here, not an incidental layout: the
	// founder's Ed25519 signature and the conversion proof's leaf are both
	// taken over exactly these canonical bytes, and this node did not sign
	// them and cannot re-derive them. A server that re-encodes a legacy
	// message even equivalently hands a receiver something unverifiable, so
	// the comparison is against the bytes the founder signed, computed by this
	// test rather than pasted in.
	if !bytes.Equal(got, encoded) {
		t.Fatalf("served legacy bytes differ from the signed bytes: served %d bytes, stored %d", len(got), len(encoded))
	}
	if err := libp2ptransport.VerifyHistoricalMessageWithProof(session.group, response.Messages[0], time.Now(), &response.LegacyProofs[0].Proof); err != nil {
		t.Fatalf("served conversion proof failed validation: %v", err)
	}
	t.Log("authenticated daemon member received 1 unchanged legacy message and 1 verified conversion proof")
}

func TestGroupRuntimeRecoversHistoryFromConnectedMemberWithoutAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	groupID := entmoot.GroupID{0x71}
	mustCreateGroup(t, root, groupID, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, groupID)
	mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	mustCloseGroup(t, group)

	receiver, binding, err := libp2ptransport.NewHost(ctx, founder, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, _, err := libp2ptransport.NewHost(ctx, member, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatal(err)
	}
	defer messages.Close()
	source, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: founder, DataDir: root, Store: messages, Notify: newNotifyingStore(messages, nil),
		Host: receiver, Binding: binding, Mode: libp2ptransport.DirectConnectivity,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	session, _, err := runtime.AddLocalGroup(ctx, groupID)
	if err != nil {
		t.Fatal(err)
	}
	// A publication made before connectivity exists survives in the sender's
	// store. Do not broadcast it here: only authenticated history can recover it.
	message := mustSignedMessage(t, ctx, groupID, memberInfo, member, session.group.Canonical().ID)
	if _, err := source.Put(ctx, groupID, message); err != nil {
		t.Fatal(err)
	}
	history := &libp2ptransport.SyncServer{
		Host: sender, Store: source,
		Group: func(id entmoot.GroupID) (*membership.Group, bool) {
			return session.group, id == groupID
		},
	}
	if err := history.Install(); err != nil {
		t.Fatal(err)
	}
	if err := sender.Connect(ctx, peer.AddrInfo{ID: receiver.ID(), Addrs: receiver.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if addresses := receiver.Peerstore().Addrs(sender.ID()); len(addresses) != 0 {
		t.Fatalf("outbound-only fixture unexpectedly advertises addresses: %v", addresses)
	}
	// Exercise the maintenance path directly, without waiting for its minute
	// ticker. The initial background pass may still own the catch-up lock.
	for {
		runtime.catchUp(ctx, session)
		found, err := messages.Has(ctx, groupID, message.ID)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("connected member's stored message was not recovered")
		case <-time.After(10 * time.Millisecond):
		}
	}
	recovered, err := messages.Range(ctx, groupID, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0].ID != message.ID || !bytes.Equal(recovered[0].Signature, message.Signature) {
		t.Fatalf("recovered history differs from the original signed message: %+v", recovered)
	}
	if err := libp2ptransport.VerifyHistoricalMessageWithProof(session.group, recovered[0], time.Now(), nil); err != nil {
		t.Fatalf("recovered member message failed verification: %v", err)
	}
}

func seedRuntimeLegacyMessage(t *testing.T, path string, message entmoot.Message, encoded []byte) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE messages(message_id BLOB PRIMARY KEY,group_id BLOB NOT NULL,author_node_id INTEGER NOT NULL,timestamp_ms INTEGER NOT NULL,content BLOB NOT NULL,parents BLOB NOT NULL,signature BLOB NOT NULL,canonical_bytes BLOB NOT NULL); CREATE INDEX idx_messages_group_author ON messages(group_id,author_node_id,timestamp_ms DESC);`); err != nil {
		t.Fatal(err)
	}
	parents, err := json.Marshal(message.Parents)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?,?)`, message.ID[:], message.GroupID[:], int64(message.Author.PilotNodeID), message.Timestamp, message.Content, parents, message.Signature, encoded); err != nil {
		t.Fatal(err)
	}
}
