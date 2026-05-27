package mailbox

import (
	"context"
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/store"
)

func TestMessagesSinceAndAckCursor(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	msgs, next, err := svc.MessagesSince(ctx, gid, "ios-1", Cursor{}, 2)
	if err != nil {
		t.Fatalf("MessagesSince: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("MessagesSince len = %d, want 2", len(msgs))
	}
	if err := svc.AckCursor(gid, "ios-1", next); err != nil {
		t.Fatalf("AckCursor: %v", err)
	}
	count, err := svc.UnreadCount(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("UnreadCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("UnreadCount = %d, want 1", count)
	}

	msgs, _, err = svc.MessagesSince(ctx, gid, "ios-1", Cursor{}, 0)
	if err != nil {
		t.Fatalf("MessagesSince after ack: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Timestamp != 3 {
		t.Fatalf("MessagesSince after ack = %+v, want only timestamp 3", msgs)
	}
}

func TestMessagesSinceRejectsEmptyClient(t *testing.T) {
	svc, err := New(store.NewMemory(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := svc.MessagesSince(context.Background(), groupID(1), "", Cursor{}, 0); err == nil {
		t.Fatalf("MessagesSince accepted empty client")
	}
}

func TestHistoryLimitZeroReturnsEmptyPage(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	history, err := svc.History(ctx, gid, 0)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if history.Count != 0 || len(history.Messages) != 0 {
		t.Fatalf("history count/messages = %d/%d, want empty page", history.Count, len(history.Messages))
	}
}

func TestHistoryReturnsLatestTopologicalPage(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	parent := message(gid, 100)
	parent.Content = []byte("parent")
	parent.ID = canonical.MessageID(parent)
	child := message(gid, 50)
	child.Content = []byte("child")
	child.Parents = []entmoot.MessageID{parent.ID}
	child.ID = canonical.MessageID(child)
	for _, msg := range []entmoot.Message{child, parent} {
		if err := st.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	history, err := svc.History(ctx, gid, 2)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if got := []string{history.Messages[0].Content, history.Messages[1].Content}; got[0] != "parent" || got[1] != "child" {
		t.Fatalf("history contents = %q, want parent, child", got)
	}

	history, err = svc.History(ctx, gid, 1)
	if err != nil {
		t.Fatalf("History limited: %v", err)
	}
	if history.Count != 1 || history.Messages[0].Content != "parent" {
		t.Fatalf("limited history = %+v, want parent", history.Messages)
	}
}

func TestSearchReturnsNewestFirstWithoutAdvancingCursor(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	old := messageWithContent(gid, 1, "mars policy limits old", "ops")
	mid := messageWithContent(gid, 2, "mars policy limits middle", "chat")
	newest := messageWithContent(gid, 3, "mars policy limits newest", "ops")
	missing := messageWithContent(gid, 4, "mars policy only", "ops")
	for _, msg := range []entmoot.Message{old, mid, newest, missing} {
		if err := st.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.AckCursorContext(ctx, gid, "ios-1", Cursor{MessageID: mid.ID, TimestampMS: mid.Timestamp}); err != nil {
		t.Fatalf("AckCursorContext: %v", err)
	}

	result, err := svc.Search(ctx, gid, "policy limits", 2, nil, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if result.Query != "policy limits" || result.Count != 2 || len(result.Results) != 2 {
		t.Fatalf("search result = %+v, want two normalized hits", result)
	}
	if !result.HasMore || result.NextCursorBoundary == nil {
		t.Fatalf("search has_more/cursor = %v/%v, want cursor", result.HasMore, result.NextCursorBoundary)
	}
	if got := []string{result.Results[0].Message.Content, result.Results[1].Message.Content}; got[0] != "mars policy limits newest" || got[1] != "mars policy limits middle" {
		t.Fatalf("search contents = %q, want newest,middle", got)
	}

	next, err := svc.Search(ctx, gid, "policy limits", 2, result.NextCursorBoundary, "")
	if err != nil {
		t.Fatalf("Search next: %v", err)
	}
	if next.HasMore || len(next.Results) != 1 || next.Results[0].Message.Content != "mars policy limits old" {
		t.Fatalf("next search = %+v, want old only", next)
	}

	topic, err := svc.Search(ctx, gid, "policy limits", 10, nil, "ops")
	if err != nil {
		t.Fatalf("Search topic: %v", err)
	}
	if len(topic.Results) != 2 || topic.Results[0].Message.Content != "mars policy limits newest" || topic.Results[1].Message.Content != "mars policy limits old" {
		t.Fatalf("topic search = %+v, want newest,old", topic.Results)
	}

	cursor, err := svc.CursorStatus(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("CursorStatus: %v", err)
	}
	if cursor.Cursor.MessageID != mid.ID || cursor.Cursor.TimestampMS != mid.Timestamp {
		t.Fatalf("cursor = %+v, want unchanged mid cursor", cursor.Cursor)
	}
}

func TestMessageContextReturnsConversationWindowWithoutAdvancingCursor(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	old := messageWithContent(gid, 10, "old", "ops")
	target := messageWithContent(gid, 20, "target", "ops")
	newer := messageWithContent(gid, 30, "newer", "ops")
	newest := messageWithContent(gid, 40, "newest", "ops")
	otherTopic := messageWithContent(gid, 50, "other topic", "chat")
	for _, msg := range []entmoot.Message{newest, target, otherTopic, old, newer} {
		if err := st.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := svc.AckCursorContext(ctx, gid, "ios-1", Cursor{MessageID: target.ID, TimestampMS: target.Timestamp}); err != nil {
		t.Fatalf("AckCursorContext: %v", err)
	}

	result, err := svc.MessageContext(ctx, gid, target.ID, 1, 2, "ops")
	if err != nil {
		t.Fatalf("MessageContext: %v", err)
	}
	if result.GroupID != gid || result.MessageID != target.ID || result.TargetMessageID != target.ID {
		t.Fatalf("result ids = group %v message %v target %v, want target", result.GroupID, result.MessageID, result.TargetMessageID)
	}
	if result.Topic != "ops" || result.Before != 1 || result.After != 2 {
		t.Fatalf("result options = topic %q before %d after %d, want ops/1/2", result.Topic, result.Before, result.After)
	}
	if result.Count != 4 || len(result.Messages) != 4 {
		t.Fatalf("result count/messages = %d/%d, want 4/4", result.Count, len(result.Messages))
	}
	if got := []string{result.Messages[0].Content, result.Messages[1].Content, result.Messages[2].Content, result.Messages[3].Content}; got[0] != "old" || got[1] != "target" || got[2] != "newer" || got[3] != "newest" {
		t.Fatalf("context contents = %q, want old,target,newer,newest", got)
	}
	if result.HasMoreOlder || result.OlderCursorBoundary != nil {
		t.Fatalf("has_more_older/cursor = %v/%v, want exhausted", result.HasMoreOlder, result.OlderCursorBoundary)
	}

	cursor, err := svc.CursorStatus(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("CursorStatus: %v", err)
	}
	if cursor.Cursor.MessageID != target.ID || cursor.Cursor.TimestampMS != target.Timestamp {
		t.Fatalf("cursor = %+v, want unchanged target cursor", cursor.Cursor)
	}

	if _, err := svc.MessageContext(ctx, gid, otherTopic.ID, 1, 1, "ops"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("topic miss err = %v, want ErrNotFound", err)
	}
}

func TestMemoryCursorStoreIsMonotonic(t *testing.T) {
	ctx := context.Background()
	cursors := NewMemoryCursorStore()
	gid := groupID(1)
	newer := Cursor{MessageID: messageID(2), TimestampMS: 2}
	older := Cursor{MessageID: messageID(1), TimestampMS: 1}

	advanced, err := cursors.AckCursor(ctx, gid, "ios-1", newer)
	if err != nil {
		t.Fatalf("AckCursor newer: %v", err)
	}
	if !advanced {
		t.Fatalf("AckCursor newer did not advance")
	}
	advanced, err = cursors.AckCursor(ctx, gid, "ios-1", older)
	if err != nil {
		t.Fatalf("AckCursor older: %v", err)
	}
	if advanced {
		t.Fatalf("AckCursor older advanced")
	}
	got, err := cursors.GetCursor(ctx, gid, "ios-1")
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}
	if got != newer {
		t.Fatalf("cursor = %+v, want %+v", got, newer)
	}
}

func TestMessagesSinceFallsBackToTimestampWhenCursorIDMissing(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	gid := groupID(1)
	for i := 1; i <= 3; i++ {
		if err := st.Put(ctx, message(gid, int64(i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	svc, err := New(st, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	msgs, next, err := svc.MessagesSince(ctx, gid, "ios-1", Cursor{MessageID: messageID(99), TimestampMS: 2}, 0)
	if err != nil {
		t.Fatalf("MessagesSince: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Timestamp != 3 {
		t.Fatalf("MessagesSince = %+v, want only timestamp 3", msgs)
	}
	if next.MessageID != msgs[0].ID || next.TimestampMS != msgs[0].Timestamp {
		t.Fatalf("next cursor = %+v, want last returned message", next)
	}
}

func message(gid entmoot.GroupID, ts int64) entmoot.Message {
	m := entmoot.Message{
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: 10, EntmootPubKey: []byte("pub")},
		Timestamp: ts,
		Topics:    []string{"t"},
		Content:   []byte{byte(ts)},
	}
	m.ID = canonical.MessageID(m)
	return m
}

func messageWithContent(gid entmoot.GroupID, ts int64, content string, topics ...string) entmoot.Message {
	m := message(gid, ts)
	m.Content = []byte(content)
	m.Topics = topics
	m.ID = canonical.MessageID(m)
	return m
}

func messageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}

func groupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
