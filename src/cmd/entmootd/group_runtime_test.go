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
	"entmoot/pkg/entmoot/roster"
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
	rlog, err := roster.OpenJSONL(root, groupID)
	if err != nil {
		t.Fatal(err)
	}
	defer rlog.Close()
	memberInfo := entmoot.NodeInfo{MemberID: &clientBinding.MemberID, PeerID: clientHost.ID().String(), EntmootPubKey: member.PublicKey}
	add, err := rlog.SignEntry(founder, "add", memberInfo, nil, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if err := rlog.Apply(add); err != nil {
		t.Fatal(err)
	}
	if err := rlog.Close(); err != nil {
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
	if !bytes.Equal(got, encoded) {
		t.Fatal("served legacy signed bytes changed")
	}
	if err := libp2ptransport.VerifyHistoricalMessageWithProof(session.roster, response.Messages[0], time.Now(), &response.LegacyProofs[0].Proof); err != nil {
		t.Fatalf("served conversion proof failed validation: %v", err)
	}
	t.Log("authenticated daemon member received 1 unchanged legacy message and 1 verified conversion proof")
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
