package store

import (
	"context"
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

func TestNormalizeMessageContextOptions(t *testing.T) {
	opts := NormalizeMessageContextOptions(MessageContextOptions{
		Before: -1,
		After:  MaxMessageContextSide + 50,
		Topic:  "ops",
	})
	if opts.Before != 0 {
		t.Fatalf("Before = %d, want 0", opts.Before)
	}
	if opts.After != MaxMessageContextSide {
		t.Fatalf("After = %d, want %d", opts.After, MaxMessageContextSide)
	}
	if opts.Topic != "ops" {
		t.Fatalf("Topic = %q, want ops", opts.Topic)
	}
}

func TestMessageContext(t *testing.T) {
	run := func(t *testing.T, newStore func(t *testing.T) MessageStore) {
		t.Helper()
		ctx := context.Background()
		s := newStore(t)
		gid := randGroupID(t)
		author := testAuthor(1, 0xAA)

		oldest := mkContextMsg(t, gid, author, 10, "oldest", "ops")
		older := mkContextMsg(t, gid, author, 20, "older", "ops")
		target := mkContextMsg(t, gid, author, 30, "target", "ops")
		newer := mkContextMsg(t, gid, author, 40, "newer", "ops")
		newest := mkContextMsg(t, gid, author, 50, "newest", "ops")
		otherTopic := mkContextMsg(t, gid, author, 60, "other topic", "chat")
		for _, msg := range []entmoot.Message{newest, oldest, otherTopic, target, older, newer} {
			if err := s.Put(ctx, msg); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		result, err := MessageContext(ctx, s, gid, target.ID, MessageContextOptions{Before: 2, After: 1})
		if err != nil {
			t.Fatalf("MessageContext: %v", err)
		}
		if result.Target.ID != target.ID {
			t.Fatalf("Target = %v, want target", result.Target.ID)
		}
		if got := messageContents(result.Messages); !equalStrings(got, []string{"oldest", "older", "target", "newer"}) {
			t.Fatalf("context contents = %q, want oldest,older,target,newer", got)
		}
		if result.HasMoreOlder || result.OlderCursorBoundary != nil {
			t.Fatalf("has_more_older/cursor = %v/%v, want exhausted", result.HasMoreOlder, result.OlderCursorBoundary)
		}

		limited, err := MessageContext(ctx, s, gid, target.ID, MessageContextOptions{Before: 1, After: 2})
		if err != nil {
			t.Fatalf("MessageContext limited: %v", err)
		}
		if got := messageContents(limited.Messages); !equalStrings(got, []string{"older", "target", "newer", "newest"}) {
			t.Fatalf("limited contents = %q, want older,target,newer,newest", got)
		}
		if !limited.HasMoreOlder || limited.OlderCursorBoundary == nil {
			t.Fatalf("limited has_more_older/cursor = %v/%v, want cursor", limited.HasMoreOlder, limited.OlderCursorBoundary)
		}
		if limited.OlderCursorBoundary.MessageID != older.ID {
			t.Fatalf("older cursor id = %v, want older", limited.OlderCursorBoundary.MessageID)
		}

		topic, err := MessageContext(ctx, s, gid, target.ID, MessageContextOptions{Before: 10, After: 10, Topic: "ops"})
		if err != nil {
			t.Fatalf("MessageContext topic: %v", err)
		}
		if got := messageContents(topic.Messages); !equalStrings(got, []string{"oldest", "older", "target", "newer", "newest"}) {
			t.Fatalf("topic contents = %q, want ops-only context", got)
		}

		if _, err := MessageContext(ctx, s, gid, otherTopic.ID, MessageContextOptions{Before: 1, After: 1, Topic: "ops"}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("topic miss err = %v, want ErrNotFound", err)
		}
		var missing entmoot.MessageID
		missing[0] = 0x99
		if _, err := MessageContext(ctx, s, gid, missing, MessageContextOptions{Before: 1, After: 1}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing err = %v, want ErrNotFound", err)
		}
	}

	t.Run("memory", func(t *testing.T) {
		run(t, func(t *testing.T) MessageStore {
			return NewMemory()
		})
	})
	t.Run("jsonl", func(t *testing.T) {
		run(t, func(t *testing.T) MessageStore {
			s, err := OpenJSONL(t.TempDir())
			if err != nil {
				t.Fatalf("OpenJSONL: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		})
	})
	t.Run("sqlite", func(t *testing.T) {
		run(t, func(t *testing.T) MessageStore {
			s, err := OpenSQLite(t.TempDir())
			if err != nil {
				t.Fatalf("OpenSQLite: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		})
	})
}

func TestMessageContextAtEdges(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()
	gid := randGroupID(t)
	author := testAuthor(1, 0xAA)
	oldest := mkContextMsg(t, gid, author, 10, "oldest")
	middle := mkContextMsg(t, gid, author, 20, "middle")
	newest := mkContextMsg(t, gid, author, 30, "newest")
	for _, msg := range []entmoot.Message{middle, newest, oldest} {
		if err := s.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	oldestContext, err := MessageContext(ctx, s, gid, oldest.ID, MessageContextOptions{Before: 10, After: 10})
	if err != nil {
		t.Fatalf("MessageContext oldest: %v", err)
	}
	if got := messageContents(oldestContext.Messages); !equalStrings(got, []string{"oldest", "middle", "newest"}) {
		t.Fatalf("oldest context = %q, want oldest,middle,newest", got)
	}
	if oldestContext.HasMoreOlder {
		t.Fatalf("oldest HasMoreOlder = true, want false")
	}

	newestContext, err := MessageContext(ctx, s, gid, newest.ID, MessageContextOptions{Before: 10, After: 10})
	if err != nil {
		t.Fatalf("MessageContext newest: %v", err)
	}
	if got := messageContents(newestContext.Messages); !equalStrings(got, []string{"oldest", "middle", "newest"}) {
		t.Fatalf("newest context = %q, want oldest,middle,newest", got)
	}
	if newestContext.HasMoreOlder {
		t.Fatalf("newest HasMoreOlder = true, want false")
	}
}

func mkContextMsg(t *testing.T, gid entmoot.GroupID, author entmoot.NodeInfo, ts int64, content string, topics ...string) entmoot.Message {
	t.Helper()
	m := mkMsg(t, gid, author, ts, content)
	m.Topics = topics
	m.ID = canonical.MessageID(m)
	return m
}

func messageContents(msgs []entmoot.Message) []string {
	out := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		out = append(out, string(msg.Content))
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
