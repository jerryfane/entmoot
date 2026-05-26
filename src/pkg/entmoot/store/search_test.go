package store

import (
	"context"
	"database/sql"
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

func TestSQLiteFTS5Available(t *testing.T) {
	db, err := sql.Open(sqliteDriver, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE VIRTUAL TABLE messages_fts USING fts5(content);`); err != nil {
		t.Fatalf("create FTS5 table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO messages_fts(content) VALUES (?);`, "mars hub policy limits"); err != nil {
		t.Fatalf("insert FTS5 row: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH ?;`, `"policy"`).Scan(&count); err != nil {
		t.Fatalf("match FTS5 row: %v", err)
	}
	if count != 1 {
		t.Fatalf("FTS5 count = %d, want 1", count)
	}
}

func TestSearchMessagesFallback(t *testing.T) {
	run := func(t *testing.T, newStore func(t *testing.T) MessageStore) {
		t.Helper()
		ctx := context.Background()
		s := newStore(t)
		gid := randGroupID(t)
		author := testAuthor(1, 0xAA)

		old := mkSearchMsg(t, gid, author, 10, "mars hub policy limits old", "ops")
		middle := mkSearchMsg(t, gid, author, 20, "mars hub policy limits middle", "ops")
		newest := mkSearchMsg(t, gid, author, 30, "mars hub policy limits newest", "chat")
		missingTerm := mkSearchMsg(t, gid, author, 40, "mars hub policy only", "ops")
		otherTopic := mkSearchMsg(t, gid, author, 50, "mars hub policy limits other topic", "other")
		embeddedTerm := mkSearchMsg(t, gid, author, 60, "mars hub xpolicyx limits", "ops")
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

func TestSQLiteSearchIndexMaintenance(t *testing.T) {
	ctx := context.Background()
	s, err := OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	gid := randGroupID(t)
	author := testAuthor(1, 0xAA)
	pruned := mkSearchMsg(t, gid, author, 10, "mars hub policy limits old", "ops")
	kept := mkSearchMsg(t, gid, author, 20, "mars hub policy limits keep", "keep")
	missingTerm := mkSearchMsg(t, gid, author, 30, "mars hub policy only", "keep")
	for _, msg := range []entmoot.Message{pruned, kept, missingTerm, kept} {
		if err := s.Put(ctx, msg); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	result, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages: %v", err)
	}
	if len(result.Hits) != 2 || result.Hits[0].Message.ID != kept.ID || result.Hits[1].Message.ID != pruned.ID {
		t.Fatalf("search ids = %v, want kept,pruned", hitIDs(result.Hits))
	}
	if !strings.Contains(result.Hits[0].Snippet, "[policy]") {
		t.Fatalf("snippet = %q, want highlighted policy", result.Hits[0].Snippet)
	}

	andResult, err := SearchMessages(ctx, s, gid, "policy missing", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages AND: %v", err)
	}
	if len(andResult.Hits) != 0 {
		t.Fatalf("AND hits = %d, want 0", len(andResult.Hits))
	}

	topicResult, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 10, Topic: "keep"})
	if err != nil {
		t.Fatalf("SearchMessages topic: %v", err)
	}
	if len(topicResult.Hits) != 1 || topicResult.Hits[0].Message.ID != kept.ID {
		t.Fatalf("topic ids = %v, want kept", hitIDs(topicResult.Hits))
	}

	prunedCount, err := s.PruneBeforeExceptTopics(ctx, gid, 30, []string{"keep"})
	if err != nil {
		t.Fatalf("PruneBeforeExceptTopics: %v", err)
	}
	if prunedCount != 1 {
		t.Fatalf("pruned = %d, want 1", prunedCount)
	}
	afterPrune, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages after prune: %v", err)
	}
	if len(afterPrune.Hits) != 1 || afterPrune.Hits[0].Message.ID != kept.ID {
		t.Fatalf("after prune ids = %v, want kept", hitIDs(afterPrune.Hits))
	}
}

func TestSQLiteSearchBackfillsMissingDocsOnReopen(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}

	gid := randGroupID(t)
	msg := mkSearchMsg(t, gid, testAuthor(1, 0xAA), 10, "mars hub policy limits backfill", "ops")
	if err := s.Put(ctx, msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	db, err := s.dbFor(gid)
	if err != nil {
		t.Fatalf("dbFor: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM message_search_docs WHERE message_id = ?;`, msg.ID[:]); err != nil {
		t.Fatalf("delete search doc: %v", err)
	}
	empty, err := SearchMessages(ctx, s, gid, "policy limits", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages empty: %v", err)
	}
	if len(empty.Hits) != 0 {
		t.Fatalf("hits before reopen = %d, want 0", len(empty.Hits))
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := OpenSQLite(root)
	if err != nil {
		t.Fatalf("OpenSQLite #2: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	backfilled, err := SearchMessages(ctx, s2, gid, "policy limits", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchMessages backfilled: %v", err)
	}
	if len(backfilled.Hits) != 1 || backfilled.Hits[0].Message.ID != msg.ID {
		t.Fatalf("backfilled ids = %v, want msg", hitIDs(backfilled.Hits))
	}
}

func hitIDs(hits []SearchHit) []entmoot.MessageID {
	out := make([]entmoot.MessageID, len(hits))
	for i, hit := range hits {
		out[i] = hit.Message.ID
	}
	return out
}

func mkSearchMsg(t *testing.T, gid entmoot.GroupID, author entmoot.NodeInfo, ts int64, content string, topics ...string) entmoot.Message {
	t.Helper()
	m := mkMsg(t, gid, author, ts, content)
	m.Topics = topics
	m.ID = canonical.MessageID(m)
	return m
}
