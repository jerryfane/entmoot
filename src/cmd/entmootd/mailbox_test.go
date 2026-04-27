package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/store"
)

func TestMailboxPullAckCursorPersists(t *testing.T) {
	gf, gid, msgs := mailboxTestData(t, []entmoot.GroupID{mailboxTestGroupID(1)})
	clientID := "ios-1"

	code, out, _ := captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"pull", "-client", clientID, "-limit", "1"})
	})
	if code != exitOK {
		t.Fatalf("pull exit = %d, stdout=%s", code, out)
	}
	var pull struct {
		ClientID   string            `json:"client_id"`
		GroupID    entmoot.GroupID   `json:"group_id"`
		Count      int               `json:"count"`
		HasMore    bool              `json:"has_more"`
		NextCursor mailboxCursorJSON `json:"next_cursor"`
		Messages   []messageJSONLine `json:"messages"`
	}
	if err := json.Unmarshal([]byte(out), &pull); err != nil {
		t.Fatalf("decode pull: %v\n%s", err, out)
	}
	if pull.ClientID != clientID || pull.GroupID != gid {
		t.Fatalf("pull envelope = %+v, want client/group", pull)
	}
	if pull.Count != 1 || len(pull.Messages) != 1 || !pull.HasMore {
		t.Fatalf("pull count/messages/has_more = %d/%d/%v, want 1/1/true", pull.Count, len(pull.Messages), pull.HasMore)
	}
	if pull.Messages[0].MessageID != msgs[0].ID || pull.NextCursor.MessageID != msgs[0].ID {
		t.Fatalf("pull returned message/cursor %+v %+v, want first message", pull.Messages[0], pull.NextCursor)
	}

	code, out, _ = captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"ack", "-client", clientID, "-message", msgs[0].ID.String()})
	})
	if code != exitOK {
		t.Fatalf("ack exit = %d, stdout=%s", code, out)
	}
	var ack struct {
		ClientID  string            `json:"client_id"`
		GroupID   entmoot.GroupID   `json:"group_id"`
		MessageID entmoot.MessageID `json:"message_id"`
		Cursor    mailboxCursorJSON `json:"cursor"`
	}
	if err := json.Unmarshal([]byte(out), &ack); err != nil {
		t.Fatalf("decode ack: %v\n%s", err, out)
	}
	if ack.MessageID != msgs[0].ID || ack.Cursor.MessageID != msgs[0].ID {
		t.Fatalf("ack = %+v, want first message cursor", ack)
	}

	code, out, _ = captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"pull", "-client", clientID, "-limit", "10"})
	})
	if code != exitOK {
		t.Fatalf("second pull exit = %d, stdout=%s", code, out)
	}
	if err := json.Unmarshal([]byte(out), &pull); err != nil {
		t.Fatalf("decode second pull: %v\n%s", err, out)
	}
	if pull.Count != 1 || len(pull.Messages) != 1 || pull.Messages[0].MessageID != msgs[1].ID {
		t.Fatalf("second pull = %+v, want only second message", pull)
	}

	code, out, _ = captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"cursor", "-client", clientID})
	})
	if code != exitOK {
		t.Fatalf("cursor exit = %d, stdout=%s", code, out)
	}
	var cursorResp struct {
		ClientID string            `json:"client_id"`
		GroupID  entmoot.GroupID   `json:"group_id"`
		Cursor   mailboxCursorJSON `json:"cursor"`
		Unread   int               `json:"unread"`
	}
	if err := json.Unmarshal([]byte(out), &cursorResp); err != nil {
		t.Fatalf("decode cursor: %v\n%s", err, out)
	}
	if cursorResp.Cursor.MessageID != msgs[0].ID || cursorResp.Unread != 1 {
		t.Fatalf("cursor response = %+v, want first cursor and one unread", cursorResp)
	}
}

func TestMailboxAckRejectsUnknownMessage(t *testing.T) {
	gf, _, _ := mailboxTestData(t, []entmoot.GroupID{mailboxTestGroupID(1)})
	var missing entmoot.MessageID
	missing[0] = 0xFF

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"ack", "-client", "ios-1", "-message", missing.String()})
	})
	if code != exitInvalidArgument {
		t.Fatalf("ack unknown exit = %d, want %d", code, exitInvalidArgument)
	}
	if !strings.Contains(stderr, "not found") {
		t.Fatalf("stderr = %q, want not found", stderr)
	}
}

func TestMailboxRejectsEmptyClient(t *testing.T) {
	gf, _, _ := mailboxTestData(t, []entmoot.GroupID{mailboxTestGroupID(1)})

	for _, tc := range [][]string{
		{"pull"},
		{"ack", "-message", mailboxTestMessageID(9).String()},
		{"cursor"},
	} {
		code, _, stderr := captureCommandOutput(t, func() int {
			return cmdMailbox(gf, tc)
		})
		if code != exitInvalidArgument {
			t.Fatalf("cmdMailbox(%v) exit = %d, want %d", tc, code, exitInvalidArgument)
		}
		if !strings.Contains(stderr, "-client is required") {
			t.Fatalf("cmdMailbox(%v) stderr = %q, want client required", tc, stderr)
		}
	}
}

func TestMailboxRequiresGroupWhenAmbiguous(t *testing.T) {
	gf, _, _ := mailboxTestData(t, []entmoot.GroupID{mailboxTestGroupID(1), mailboxTestGroupID(2)})

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdMailbox(gf, []string{"pull", "-client", "ios-1"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("pull ambiguous exit = %d, want %d", code, exitInvalidArgument)
	}
	if !strings.Contains(stderr, "pass -group") {
		t.Fatalf("stderr = %q, want pass -group", stderr)
	}
}

type mailboxCursorJSON struct {
	MessageID   entmoot.MessageID `json:"message_id"`
	TimestampMS int64             `json:"timestamp_ms"`
}

type messageJSONLine struct {
	MessageID   entmoot.MessageID `json:"message_id"`
	GroupID     entmoot.GroupID   `json:"group_id"`
	Author      uint32            `json:"author"`
	Topics      []string          `json:"topic"`
	Content     string            `json:"content"`
	TimestampMS int64             `json:"timestamp_ms"`
}

func mailboxTestData(t *testing.T, gids []entmoot.GroupID) (*globalFlags, entmoot.GroupID, []entmoot.Message) {
	t.Helper()
	dataDir := t.TempDir()
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	defer func() { _ = st.Close() }()

	var firstMsgs []entmoot.Message
	for _, gid := range gids {
		msgs := []entmoot.Message{
			mailboxTestMessage(gid, 1, "first"),
			mailboxTestMessage(gid, 2, "second"),
		}
		for _, msg := range msgs {
			if err := st.Put(context.Background(), msg); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if firstMsgs == nil {
			firstMsgs = msgs
		}
	}
	return &globalFlags{data: dataDir}, gids[0], firstMsgs
}

func mailboxTestMessage(gid entmoot.GroupID, ts int64, content string) entmoot.Message {
	msg := entmoot.Message{
		GroupID:   gid,
		Author:    entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("pub")},
		Timestamp: ts,
		Topics:    []string{"test/mailbox"},
		Content:   []byte(content),
	}
	msg.ID = canonical.MessageID(msg)
	return msg
}

func mailboxTestGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

func mailboxTestMessageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}

func captureCommandOutput(t *testing.T, fn func() int) (int, string, string) {
	t.Helper()
	origStdout := os.Stdout
	origStderr := os.Stderr
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stdout = stdoutW
	os.Stderr = stderrW

	code := fn()

	_ = stdoutW.Close()
	_ = stderrW.Close()
	os.Stdout = origStdout
	os.Stderr = origStderr

	var stdoutBuf, stderrBuf bytes.Buffer
	_, _ = io.Copy(&stdoutBuf, stdoutR)
	_, _ = io.Copy(&stderrBuf, stderrR)
	_ = stdoutR.Close()
	_ = stderrR.Close()
	return code, strings.TrimSpace(stdoutBuf.String()), strings.TrimSpace(stderrBuf.String())
}
