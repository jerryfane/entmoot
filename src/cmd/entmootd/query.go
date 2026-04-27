package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/topic"
)

// cmdQuery runs a historical SQLite query against a single group and
// emits one JSON message per line. Reads SQLite directly; does not
// require a running join.
func cmdQuery(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required if multiple groups are joined)")
	authorStr := fs.String("author", "", "exact Pilot node id (optional)")
	topicPattern := fs.String("topic", "", "MQTT-style filter (optional)")
	sinceStr := fs.String("since", "", "RFC3339 or unix-ms lower bound, inclusive (optional)")
	untilStr := fs.String("until", "", "RFC3339 or unix-ms upper bound, exclusive (optional)")
	limit := fs.Int("limit", 50, "maximum messages to return")
	order := fs.String("order", "desc", "asc|desc")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	if *limit < 0 {
		fmt.Fprintln(os.Stderr, "query: -limit must be non-negative")
		return exitInvalidArgument
	}
	if *order != "asc" && *order != "desc" {
		fmt.Fprintln(os.Stderr, "query: -order must be asc or desc")
		return exitInvalidArgument
	}
	if *topicPattern != "" {
		if err := topic.ValidPattern(*topicPattern); err != nil {
			fmt.Fprintf(os.Stderr, "query: -topic: %v\n", err)
			return exitInvalidArgument
		}
	}
	var sinceMs int64
	if *sinceStr != "" {
		v, err := parseTimeBound(*sinceStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "query: -since: %v\n", err)
			return exitInvalidArgument
		}
		sinceMs = v
	}
	var untilMs int64
	if *untilStr != "" {
		v, err := parseTimeBound(*untilStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "query: -until: %v\n", err)
			return exitInvalidArgument
		}
		untilMs = v
	}
	var authorFilter *entmoot.NodeID
	if *authorStr != "" {
		n, err := strconv.ParseUint(*authorStr, 10, 32)
		if err != nil {
			fmt.Fprintf(os.Stderr, "query: -author: %v\n", err)
			return exitInvalidArgument
		}
		id := entmoot.NodeID(uint32(n))
		authorFilter = &id
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("query: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	var gid entmoot.GroupID
	if *groupStr != "" {
		gid, err = decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "query: %v\n", err)
			return exitInvalidArgument
		}
	} else {
		// Fall back to auto-pick when exactly one group is joined.
		gid, err = resolveGroupID(s.dataDir, nil, slog.Default())
		if err != nil {
			fmt.Fprintf(os.Stderr, "query: %v\n", err)
			return exitInvalidArgument
		}
	}

	// Verify the group is known locally before opening the store, so we
	// can surface a clean GROUP_NOT_FOUND instead of an empty result.
	gids, err := listGroupIDs(s.dataDir, slog.Default())
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: %v\n", err)
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
		fmt.Fprintf(os.Stderr, "query: group %s not joined\n", gid)
		return exitGroupNotFound
	}

	st, err := store.OpenSQLite(s.dataDir)
	if err != nil {
		slog.Error("query: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = st.Close() }()

	ctx, cancel := withTimeout(30 * time.Second)
	defer cancel()

	// Range already honors sinceMs/untilMs via the interface contract.
	msgs, err := st.Range(ctx, gid, sinceMs, untilMs)
	if err != nil {
		slog.Error("query: range", slog.String("err", err.Error()))
		return exitTransport
	}

	// Apply in-Go filters (author, topic).
	filtered := msgs[:0]
	for _, m := range msgs {
		if authorFilter != nil && m.Author.PilotNodeID != *authorFilter {
			continue
		}
		if *topicPattern != "" {
			matched := false
			for _, t := range m.Topics {
				if topic.Match(*topicPattern, t) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		filtered = append(filtered, m)
	}

	// Order: Range returns topological (asc by timestamp). Reverse for
	// desc.
	if *order == "desc" {
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].Timestamp > filtered[j].Timestamp
		})
	}

	// Apply limit.
	if *limit > 0 && len(filtered) > *limit {
		filtered = filtered[:*limit]
	}

	for _, m := range filtered {
		if err := emitMessageJSON(m); err != nil {
			slog.Error("query: marshal", slog.String("err", err.Error()))
			return exitTransport
		}
	}
	return exitOK
}

// parseTimeBound parses either an RFC3339 timestamp or a decimal string
// of unix milliseconds.
func parseTimeBound(s string) (int64, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixMilli(), nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not RFC3339 nor unix-ms: %s", s)
	}
	return n, nil
}

// emitMessageJSON prints one JSON object representing m to stdout with
// a trailing newline. Schema matches CLI_DESIGN §3.3 / §3.5.
func emitMessageJSON(m entmoot.Message) error {
	data, err := json.Marshal(messageJSON(m))
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func messageJSON(m entmoot.Message) map[string]any {
	return map[string]any{
		"message_id":   m.ID,
		"group_id":     m.GroupID,
		"author":       uint32(m.Author.PilotNodeID),
		"topic":        normTopics(m.Topics),
		"content":      string(m.Content),
		"timestamp_ms": m.Timestamp,
	}
}

// normTopics returns topics unchanged when non-nil, or an empty slice
// when nil so the JSON encoder produces [] rather than null.
func normTopics(t []string) []string {
	if t == nil {
		return []string{}
	}
	return t
}
