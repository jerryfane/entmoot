package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/mailbox"
	"entmoot/pkg/entmoot/store"
)

func cmdMailbox(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "mailbox: expected pull, ack, or cursor")
		return exitInvalidArgument
	}
	switch args[0] {
	case "pull":
		return cmdMailboxPull(gf, args[1:])
	case "ack":
		return cmdMailboxAck(gf, args[1:])
	case "cursor":
		return cmdMailboxCursor(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "mailbox: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdMailboxPull(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("mailbox pull", flag.ContinueOnError)
	clientID := fs.String("client", "", "mailbox client id")
	groupStr := fs.String("group", "", "base64 group id (required if multiple groups are joined)")
	limit := fs.Int("limit", 50, "maximum messages to return")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *clientID == "" {
		fmt.Fprintln(os.Stderr, "mailbox pull: -client is required")
		return exitInvalidArgument
	}
	if *limit < 0 {
		fmt.Fprintln(os.Stderr, "mailbox pull: -limit must be non-negative")
		return exitInvalidArgument
	}

	ctx, cancel := withTimeout(30 * time.Second)
	defer cancel()
	resources, code, ok := openMailboxResources(gf, *groupStr)
	if !ok {
		return code
	}
	defer resources.close()

	out, err := resources.service.Pull(ctx, resources.groupID, *clientID, *limit)
	if err != nil {
		return mailboxError("mailbox pull", err)
	}
	if err := emitJSON(out); err != nil {
		slog.Error("mailbox pull: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	return exitOK
}

func cmdMailboxAck(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("mailbox ack", flag.ContinueOnError)
	clientID := fs.String("client", "", "mailbox client id")
	groupStr := fs.String("group", "", "base64 group id (required if multiple groups are joined)")
	messageStr := fs.String("message", "", "base64 message id to acknowledge")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *clientID == "" {
		fmt.Fprintln(os.Stderr, "mailbox ack: -client is required")
		return exitInvalidArgument
	}
	if *messageStr == "" {
		fmt.Fprintln(os.Stderr, "mailbox ack: -message is required")
		return exitInvalidArgument
	}
	messageID, err := decodeMessageID(*messageStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mailbox ack: %v\n", err)
		return exitInvalidArgument
	}

	ctx, cancel := withTimeout(30 * time.Second)
	defer cancel()
	resources, code, ok := openMailboxResources(gf, *groupStr)
	if !ok {
		return code
	}
	defer resources.close()

	out, err := resources.service.AckMessage(ctx, resources.groupID, *clientID, messageID)
	if errors.Is(err, store.ErrNotFound) {
		fmt.Fprintf(os.Stderr, "mailbox ack: message %s not found\n", messageID)
		return exitInvalidArgument
	}
	if err != nil {
		return mailboxError("mailbox ack", err)
	}
	if err := emitJSON(out); err != nil {
		slog.Error("mailbox ack: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	return exitOK
}

func cmdMailboxCursor(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("mailbox cursor", flag.ContinueOnError)
	clientID := fs.String("client", "", "mailbox client id")
	groupStr := fs.String("group", "", "base64 group id (required if multiple groups are joined)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *clientID == "" {
		fmt.Fprintln(os.Stderr, "mailbox cursor: -client is required")
		return exitInvalidArgument
	}

	ctx, cancel := withTimeout(30 * time.Second)
	defer cancel()
	resources, code, ok := openMailboxResources(gf, *groupStr)
	if !ok {
		return code
	}
	defer resources.close()

	out, err := resources.service.CursorStatus(ctx, resources.groupID, *clientID)
	if err != nil {
		return mailboxError("mailbox cursor", err)
	}
	if err := emitJSON(out); err != nil {
		slog.Error("mailbox cursor: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	return exitOK
}

type mailboxResources struct {
	groupID     entmoot.GroupID
	store       store.MessageStore
	cursorStore mailbox.CursorStore
	service     *mailbox.Service
}

func openMailboxResources(gf *globalFlags, groupStr string) (*mailboxResources, int, bool) {
	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		slog.Error("mailbox: mkdir data", slog.String("err", err.Error()))
		return nil, exitTransport, false
	}
	var gid entmoot.GroupID
	var err error
	if groupStr != "" {
		gid, err = decodeGroupID(groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mailbox: %v\n", err)
			return nil, exitInvalidArgument, false
		}
	} else {
		gid, err = resolveGroupID(gf.data, nil, slog.Default())
		if err != nil {
			fmt.Fprintf(os.Stderr, "mailbox: %v\n", err)
			return nil, exitInvalidArgument, false
		}
	}
	gids, err := listGroupIDs(gf.data, slog.Default())
	if err != nil {
		fmt.Fprintf(os.Stderr, "mailbox: %v\n", err)
		return nil, exitTransport, false
	}
	found := false
	for _, g := range gids {
		if g == gid {
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "mailbox: group %s not joined\n", gid)
		return nil, exitGroupNotFound, false
	}

	st, err := store.OpenSQLite(gf.data)
	if err != nil {
		slog.Error("mailbox: open store", slog.String("err", err.Error()))
		return nil, exitTransport, false
	}
	cursors, err := mailbox.OpenSQLiteCursorStore(gf.data)
	if err != nil {
		_ = st.Close()
		slog.Error("mailbox: open cursor store", slog.String("err", err.Error()))
		return nil, exitTransport, false
	}
	svc, err := mailbox.NewWithCursorStore(st, cursors, nil)
	if err != nil {
		_ = cursors.Close()
		_ = st.Close()
		slog.Error("mailbox: create service", slog.String("err", err.Error()))
		return nil, exitTransport, false
	}
	return &mailboxResources{
		groupID:     gid,
		store:       st,
		cursorStore: cursors,
		service:     svc,
	}, exitOK, true
}

func (r *mailboxResources) close() {
	_ = r.cursorStore.Close()
	_ = r.store.Close()
}

func emitJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func mailboxError(prefix string, err error) int {
	if errors.Is(err, mailbox.ErrInvalidClient) {
		fmt.Fprintf(os.Stderr, "%s: %v\n", prefix, err)
		return exitInvalidArgument
	}
	slog.Error(prefix, slog.String("err", err.Error()))
	return exitTransport
}
