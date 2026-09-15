package mailbox_test

import (
	"context"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store/storetest"
)

func TestMobileServiceMailboxFlowForANonDaemonAuthor(t *testing.T) {
	ctx := context.Background()
	gid := groupID(42)

	phoneID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate phone identity: %v", err)
	}
	memberID, err := entmoot.MemberIDFromPublicKey(phoneID.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerID, err := entmoot.PeerIDFromPublicKey(phoneID.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	author := entmoot.NodeInfo{
		EntmootPubKey: phoneID.PublicKey,
		MemberID:      &memberID,
		PeerID:        peerID,
	}

	bus := events.NewBus()
	eventCh := make(chan events.Event, 1)
	cancelSub := bus.Subscribe(eventCh)
	defer cancelSub()

	st := storetest.New(t)
	defer func() { _ = st.Close() }()
	cursorDir := t.TempDir()
	cursorStore, err := mailbox.OpenSQLiteCursorStore(cursorDir)
	if err != nil {
		t.Fatalf("OpenSQLiteCursorStore: %v", err)
	}
	mbox, err := mailbox.NewWithCursorStore(st, cursorStore, bus)
	if err != nil {
		t.Fatalf("mailbox.NewWithCursorStore: %v", err)
	}

	phoneSigner, err := signing.NewLocalSigner(author, phoneID)
	if err != nil {
		t.Fatalf("NewLocalSigner: %v", err)
	}
	msg, err := signing.SignMessage(ctx, phoneSigner, entmoot.Message{
		GroupID:   gid,
		Timestamp: time.Now().UnixMilli(),
		Topics:    []string{"mobile/service"},
		Content:   []byte("phone-held identity publish"),
	})
	if err != nil {
		t.Fatalf("SignMessage: %v", err)
	}
	if err := signing.VerifyMessage(msg, author); err != nil {
		t.Fatalf("VerifyMessage: %v", err)
	}

	if _, err := st.Put(ctx, msg.GroupID, msg); err != nil {
		t.Fatalf("store.Put: %v", err)
	}
	bus.Emit(events.Event{
		Type:           events.TypeMessageIngested,
		GroupID:        gid,
		MessageID:      msg.ID,
		AuthorMemberID: memberID,
	})
	select {
	case ev := <-eventCh:
		if ev.Type != events.TypeMessageIngested || ev.MessageID != msg.ID {
			t.Fatalf("event = %+v, want message_ingested for %s", ev, msg.ID)
		}
	case <-time.After(time.Second):
		t.Fatalf("message_ingested event not delivered")
	}

	const clientID = "ios-1"
	unread, err := mbox.UnreadCount(ctx, gid, clientID)
	if err != nil {
		t.Fatalf("UnreadCount before ack: %v", err)
	}
	if unread != 1 {
		t.Fatalf("UnreadCount before ack = %d, want 1", unread)
	}

	msgs, next, err := mbox.MessagesSince(ctx, gid, clientID, mailbox.Cursor{}, 10)
	if err != nil {
		t.Fatalf("MessagesSince: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != msg.ID {
		t.Fatalf("MessagesSince returned %+v, want only %s", msgs, msg.ID)
	}
	if err := mbox.AckCursor(gid, clientID, next); err != nil {
		t.Fatalf("AckCursor: %v", err)
	}
	unread, err = mbox.UnreadCount(ctx, gid, clientID)
	if err != nil {
		t.Fatalf("UnreadCount after ack: %v", err)
	}
	if unread != 0 {
		t.Fatalf("UnreadCount after ack = %d, want 0", unread)
	}

	if err := cursorStore.Close(); err != nil {
		t.Fatalf("cursorStore.Close: %v", err)
	}
	cursorStore, err = mailbox.OpenSQLiteCursorStore(cursorDir)
	if err != nil {
		t.Fatalf("OpenSQLiteCursorStore reopen: %v", err)
	}
	defer func() { _ = cursorStore.Close() }()
	mbox, err = mailbox.NewWithCursorStore(st, cursorStore, bus)
	if err != nil {
		t.Fatalf("mailbox.NewWithCursorStore after reopen: %v", err)
	}
	unread, err = mbox.UnreadCount(ctx, gid, clientID)
	if err != nil {
		t.Fatalf("UnreadCount after cursor reopen: %v", err)
	}
	if unread != 0 {
		t.Fatalf("UnreadCount after cursor reopen = %d, want 0", unread)
	}
}

func groupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
