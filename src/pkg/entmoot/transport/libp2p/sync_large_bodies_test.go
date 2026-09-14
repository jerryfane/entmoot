package libp2ptransport

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// Bodies are batched by item count, so a group of large messages used to build
// a page above the response cap. The keeper then refused the whole page, the
// client retried the identical request, and no message ever transferred: a
// single region of large messages blocked the entire group forever.
func TestHistoryTransfersMessagesTooLargeForAFullBatch(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	const bodies = 80
	// 8 KiB of content puts a 64-item batch over the 384 KiB page cap.
	payload := bytes.Repeat([]byte("p"), 8<<10)
	for sequence := 3; sequence < 3+bodies; sequence++ {
		f.addLargeMessage(t, group, sequence, payload)
	}
	server := &SyncServer{
		Host: f.serverHost, Admission: NewBootstrapAdmission(), Store: f.store,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	destination := store.NewMemory()
	defer destination.Close()
	validate := func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}
	state := new(HistorySyncState)
	keepers := []peer.AddrInfo{f.remote}

	inserted := 0
	for pass := 0; pass < 12 && inserted < len(f.ids[group]); pass++ {
		item := SyncFromKeepers(f.ctx, f.client, group, keepers, destination, validate, state)[0]
		if item.Err != nil {
			t.Fatalf("pass %d: %v", pass, item.Err)
		}
		inserted += item.Inserted
	}
	if inserted != len(f.ids[group]) {
		t.Fatalf("inserted %d of %d large messages", inserted, len(f.ids[group]))
	}
	for _, id := range f.ids[group] {
		present, err := destination.Has(f.ctx, group, id)
		if err != nil || !present {
			t.Fatalf("missing message %s: %v", id, err)
		}
	}
}

// A truncated page must say so, so the client knows identifiers are outstanding
// rather than treating the request as fully answered.
func TestHistoryBodyPageReportsTruncation(t *testing.T) {
	f := newSnapshotLifecycleFixture(t)
	group := f.groups[0]
	payload := bytes.Repeat([]byte("q"), 8<<10)
	for sequence := 3; sequence < 3+maxHistoryBodyItems; sequence++ {
		f.addLargeMessage(t, group, sequence, payload)
	}
	server := &SyncServer{
		Host: f.serverHost, Admission: NewBootstrapAdmission(), Store: f.store,
		Roster: func(id entmoot.GroupID) (*roster.RosterLog, bool) { log, ok := f.logs[id]; return log, ok },
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	requested := f.ids[group][:maxHistoryBodyItems]
	response, err := RequestHistoryPage(f.ctx, f.client, f.remote, HistorySyncRequest{
		Version: 2, RequestID: "bodies-truncation", GroupID: group, Mode: "bodies", IDs: requested,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Messages) == 0 || len(response.Messages) >= len(requested) {
		t.Fatalf("served %d of %d bodies, want a non-empty prefix", len(response.Messages), len(requested))
	}
	if !response.HasMore {
		t.Fatal("truncated body page did not report HasMore")
	}
	if size := encodedJSONSize(response); size > maxHistoryBodyBytes {
		t.Fatalf("served page is %d bytes, cap is %d", size, maxHistoryBodyBytes)
	}
}

func (f *snapshotLifecycleFixture) addLargeMessage(t *testing.T, groupID entmoot.GroupID, sequence int, payload []byte) {
	t.Helper()
	signer, err := signing.NewLocalSigner(mustNodeInfo(t, f.founder.PublicKey), f.founder)
	if err != nil {
		t.Fatal(err)
	}
	head := f.logs[groupID].Head()
	message, err := signer.SignMessage(f.ctx, entmoot.Message{
		Version: 2, GroupID: groupID, Timestamp: int64(20_000 + sequence),
		Topics:     []string{"bulk"},
		Content:    append([]byte(fmt.Sprintf("large-%d-", sequence)), payload...),
		RosterHead: &head,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(f.ctx, groupID, message); err != nil {
		t.Fatal(err)
	}
	f.ids[groupID] = append(f.ids[groupID], message.ID)
}
