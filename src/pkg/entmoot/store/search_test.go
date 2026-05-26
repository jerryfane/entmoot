package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

func TestNormalizeSearchQuery(t *testing.T) {
	t.Run("builds safe terms and fts expression", func(t *testing.T) {
		query, err := NormalizeSearchQuery(`  Policy-limits "Mars" policy  `)
		if err != nil {
			t.Fatalf("NormalizeSearchQuery: %v", err)
		}
		if got, want := query.Query, "policy limits mars"; got != want {
			t.Fatalf("Query = %q, want %q", got, want)
		}
		if got, want := query.FTS5, `"policy" AND "limits" AND "mars"`; got != want {
			t.Fatalf("FTS5 = %q, want %q", got, want)
		}
	})

	t.Run("rejects empty query", func(t *testing.T) {
		_, err := NormalizeSearchQuery(" , - ")
		if !errors.Is(err, ErrInvalidSearchQuery) {
			t.Fatalf("err = %v, want ErrInvalidSearchQuery", err)
		}
	})

	t.Run("rejects overlong query", func(t *testing.T) {
		long := make([]rune, maxSearchQueryLength+1)
		for i := range long {
			long[i] = 'x'
		}
		_, err := NormalizeSearchQuery(string(long))
		if !errors.Is(err, ErrInvalidSearchQuery) {
			t.Fatalf("err = %v, want ErrInvalidSearchQuery", err)
		}
	})

	t.Run("counts unicode characters", func(t *testing.T) {
		query, err := NormalizeSearchQuery(strings.Repeat("界", 100))
		if err != nil {
			t.Fatalf("NormalizeSearchQuery unicode: %v", err)
		}
		if len(query.Terms) != 1 {
			t.Fatalf("Terms len = %d, want 1", len(query.Terms))
		}
	})
}

func TestSearchMessagesFallback(t *testing.T) {
	run := func(t *testing.T, newStore func(t *testing.T) MessageStore) {
		t.Helper()
		ctx := context.Background()
		s := newStore(t)
		gid := randGroupID(t)
		author := testAuthor(1, 0xAA)
		withTopics := func(ts int64, content string, topics ...string) entmoot.Message {
			m := mkMsg(t, gid, author, ts, content)
			m.Topics = topics
			m.ID = canonical.MessageID(m)
			return m
		}

		old := withTopics(10, "mars hub policy limits old", "ops")
		middle := withTopics(20, "mars hub policy limits middle", "ops")
		newest := withTopics(30, "mars hub policy limits newest", "chat")
		missingTerm := withTopics(40, "mars hub policy only", "ops")
		otherTopic := withTopics(50, "mars hub policy limits other topic", "other")
		embeddedTerm := withTopics(60, "mars hub xpolicyx limits", "ops")
		for _, msg := range []entmoot.Message{old, middle, newest, missingTerm, otherTopic, embeddedTerm} {
			if err := s.Put(ctx, msg); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}

		result, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 2})
		if err != nil {
			t.Fatalf("SearchMessages: %v", err)
		}
		if !result.HasMore {
			t.Fatalf("HasMore = false, want true")
		}
		if len(result.Hits) != 2 || result.Hits[0].Message.ID != otherTopic.ID || result.Hits[1].Message.ID != newest.ID {
			t.Fatalf("first page ids = %v, want otherTopic,newest", hitIDs(result.Hits))
		}
		if result.NextCursorBoundary == nil {
			t.Fatal("NextCursorBoundary is nil")
		}

		next, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{
			Limit:          2,
			CursorBoundary: result.NextCursorBoundary,
		})
		if err != nil {
			t.Fatalf("SearchMessages next: %v", err)
		}
		if next.HasMore {
			t.Fatalf("next HasMore = true, want false")
		}
		if len(next.Hits) != 2 || next.Hits[0].Message.ID != middle.ID || next.Hits[1].Message.ID != old.ID {
			t.Fatalf("next ids = %v, want middle,old", hitIDs(next.Hits))
		}

		topicResult, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 10, Topic: "ops"})
		if err != nil {
			t.Fatalf("SearchMessages topic: %v", err)
		}
		if len(topicResult.Hits) != 2 || topicResult.Hits[0].Message.ID != middle.ID || topicResult.Hits[1].Message.ID != old.ID {
			t.Fatalf("topic ids = %v, want middle,old", hitIDs(topicResult.Hits))
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
}

func hitIDs(hits []SearchHit) []entmoot.MessageID {
	out := make([]entmoot.MessageID, len(hits))
	for i, hit := range hits {
		out[i] = hit.Message.ID
	}
	return out
}
