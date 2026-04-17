package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/topic"
)

// cmdTail emits historical backfill from SQLite (when -n is set) then
// subscribes to the control socket for live events. Blocks until EOF on
// stdin or SIGINT/SIGTERM.
func cmdTail(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	topicPattern := fs.String("topic", "#", "MQTT-style filter (default: #)")
	groupStr := fs.String("group", "", "base64 group id (optional; all groups when absent)")
	n := fs.Int("n", 0, "emit last N matching messages before going live; 0 = live only, -1 = all")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *n < -1 {
		fmt.Fprintln(os.Stderr, "tail: -n must be >= -1")
		return exitInvalidArgument
	}
	if err := topic.ValidPattern(*topicPattern); err != nil {
		fmt.Fprintf(os.Stderr, "tail: -topic: %v\n", err)
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("tail: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	// Resolve target groups. An explicit -group narrows to one; absence
	// means "all joined groups."
	var targetGroups []entmoot.GroupID
	if *groupStr != "" {
		gid, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "tail: %v\n", err)
			return exitInvalidArgument
		}
		gids, err := listGroupIDs(s.dataDir, slog.Default())
		if err != nil {
			fmt.Fprintf(os.Stderr, "tail: %v\n", err)
			return exitTransport
		}
		found := false
		for _, g := range gids {
			if g == gid {
				found = true
				break
			}
		}
		if !found {
			fmt.Fprintf(os.Stderr, "tail: group %s not joined\n", gid)
			return exitGroupNotFound
		}
		targetGroups = []entmoot.GroupID{gid}
	} else {
		all, err := listGroupIDs(s.dataDir, slog.Default())
		if err != nil {
			fmt.Fprintf(os.Stderr, "tail: %v\n", err)
			return exitTransport
		}
		targetGroups = all
	}

	// Backfill phase (when -n != 0).
	if *n != 0 && len(targetGroups) > 0 {
		if err := backfillFromSQLite(s.dataDir, targetGroups, *topicPattern, *n); err != nil {
			fmt.Fprintf(os.Stderr, "tail: backfill: %v\n", err)
			return exitTransport
		}
	}

	// Live phase: subscribe via IPC.
	sockPath := controlSocketPath(gf.data)
	if !controlSocketAlive(sockPath, 500*time.Millisecond) {
		fmt.Fprintln(os.Stderr, noJoinHelp)
		return exitControlUnavail
	}

	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		fmt.Fprintln(os.Stderr, noJoinHelp)
		return exitControlUnavail
	}
	defer conn.Close()

	var subGID *entmoot.GroupID
	if len(targetGroups) == 1 && *groupStr != "" {
		g := targetGroups[0]
		subGID = &g
	}
	sub := &ipc.TailSubscribe{
		GroupID: subGID,
		Topic:   *topicPattern,
	}
	if err := ipc.EncodeAndWrite(conn, sub); err != nil {
		slog.Error("tail: write subscribe", slog.String("err", err.Error()))
		return exitTransport
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Watch stdin for EOF so pipe-based consumers can drop the tail
	// cleanly by closing their end.
	go func() {
		buf := make([]byte, 256)
		for {
			_, err := os.Stdin.Read(buf)
			if err != nil {
				cancel()
				return
			}
		}
	}()

	// Close the connection when context is cancelled, which unblocks
	// ReadAndDecode.
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	for {
		_, payload, err := ipc.ReadAndDecode(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return exitOK
			}
			select {
			case <-ctx.Done():
				return exitOK
			default:
			}
			slog.Error("tail: read event", slog.String("err", err.Error()))
			return exitTransport
		}
		switch v := payload.(type) {
		case *ipc.TailEvent:
			if err := emitMessageJSON(v.Message); err != nil {
				slog.Error("tail: marshal event", slog.String("err", err.Error()))
				return exitTransport
			}
		case *ipc.ErrorFrame:
			fmt.Fprintf(os.Stderr, "tail: %s: %s\n", v.Code, v.Message)
			return ipc.ExitCode(v.Code)
		default:
			slog.Warn("tail: unexpected payload", slog.String("type", fmt.Sprintf("%T", payload)))
		}
	}
}

// backfillFromSQLite reads recent messages from each target group's
// SQLite store, applies the topic filter in Go, sorts the combined set
// oldest-to-newest, and optionally trims to the trailing -n entries.
// n < 0 means "everything"; n > 0 means "last N."
func backfillFromSQLite(dataDir string, groups []entmoot.GroupID, pattern string, n int) error {
	st, err := store.OpenSQLite(dataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = st.Close() }()

	ctx, cancel := withTimeout(30 * time.Second)
	defer cancel()

	var all []entmoot.Message
	for _, gid := range groups {
		msgs, err := st.Range(ctx, gid, 0, 0)
		if err != nil {
			return fmt.Errorf("range %s: %w", gid, err)
		}
		for _, m := range msgs {
			if pattern == "" || matchAnyTopic(pattern, m.Topics) {
				all = append(all, m)
			}
		}
	}

	// Oldest-to-newest.
	sort.SliceStable(all, func(i, j int) bool {
		return all[i].Timestamp < all[j].Timestamp
	})

	// Trim to trailing N when n > 0.
	if n > 0 && len(all) > n {
		all = all[len(all)-n:]
	}

	for _, m := range all {
		if err := emitMessageJSON(m); err != nil {
			return err
		}
	}
	return nil
}

// matchAnyTopic applies the MQTT matcher across a message's topics.
// Returns true when any topic matches, including the "#" default.
func matchAnyTopic(pattern string, topics []string) bool {
	if len(topics) == 0 {
		// An untopic'd message matches only the all-matching pattern.
		return pattern == "#"
	}
	for _, t := range topics {
		if topic.Match(pattern, t) {
			return true
		}
	}
	return false
}
