package membership

import (
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

func node(t *testing.T, id *keystore.Identity) entmoot.NodeInfo {
	t.Helper()
	mid, err := entmoot.MemberIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := entmoot.PeerIDFromPublicKey(id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return entmoot.NodeInfo{EntmootPubKey: append([]byte(nil), id.PublicKey...), MemberID: &mid, PeerID: pid}
}

func TestReproStaleJoinAfterCheckpoint(t *testing.T) {
	founder, _ := keystore.Generate()
	first, _ := keystore.Generate()
	second, _ := keystore.Generate()
	var gid entmoot.GroupID
	gid[0] = 9
	policy := DefaultPolicy()
	policy.JoinRule = JoinRuleOpen
	server, err := Create(t.TempDir(), founder, node(t, founder), gid, policy, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.SignRecord(first, Record{Kind: KindJoin}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.SignRecord(founder, Record{Kind: KindRemove, Subject: node(t, first), Banned: true}); err != nil {
		t.Fatal(err)
	}
	cp, signed, err := server.SignCheckpoint(founder, true)
	if err != nil || !signed {
		t.Fatalf("checkpoint: %v signed=%v", err, signed)
	}
	t.Logf("checkpoint seq=%d ts=%d covered=%d now=%d", cp.Sequence, cp.Timestamp, cp.Covered, time.Now().UnixMilli())
	served := server.CheckpointsSince(0)
	t.Logf("served %d checkpoints", len(served))
	joiner, err := Adopt(t.TempDir(), served[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range served[1:] {
		applied, err := joiner.ApplyCheckpoint(extra)
		t.Logf("apply cp seq=%d applied=%v err=%v", extra.Sequence, applied, err)
		if err != nil {
			t.Fatalf("apply checkpoint: %v", err)
		}
	}
	t.Logf("joiner canonical seq=%d ts=%d", joiner.Canonical().Sequence, joiner.Canonical().Timestamp)
	if _, err := joiner.SignRecord(second, Record{Kind: KindJoin}); err != nil {
		t.Fatalf("join after checkpoint: %v", err)
	}
}
