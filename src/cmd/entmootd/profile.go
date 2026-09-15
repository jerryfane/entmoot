package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"context"
	"net"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/profile"
)

// defaultProfileTTL bounds how long a name is shown without being republished.
// A member that leaves for good should stop being displayed eventually, and a
// name that is never refreshed is probably stale; 30 days is long enough that
// an ordinary member never thinks about it.
const defaultProfileTTL = 30 * 24 * time.Hour

func cmdProfile(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "profile: missing op (want: set, clear, or show)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "set":
		return cmdProfileSet(gf, args[1:], false)
	case "clear":
		return cmdProfileSet(gf, args[1:], true)
	case "show":
		return cmdProfileShow(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "profile: unknown op %q (want: set, clear, or show)\n", args[0])
		return exitInvalidArgument
	}
}

// cmdProfileSet publishes this node's display name into a group.
//
// The name travels as an ordinary signed message, so it needs the daemon
// running for the same reason `publish` does: the daemon holds the identity,
// the store, and the GossipSub topic.
func cmdProfileSet(gf *globalFlags, args []string, clear bool) int {
	fs := flag.NewFlagSet("profile set", flag.ContinueOnError)
	nameFlag := fs.String("name", "", "display name to publish (required unless clearing)")
	groupStr := fs.String("group", "", "base64 group id (optional when exactly one group is joined)")
	ttlFlag := fs.Duration("ttl", defaultProfileTTL, "how long the name stays current; 0 means no expiry")
	timeoutFlag := fs.Duration("timeout", 30*time.Second, "IPC response deadline")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	name := *nameFlag
	if clear {
		if name != "" {
			fmt.Fprintln(os.Stderr, "profile clear: -name is not accepted; clear withdraws the published name")
			return exitInvalidArgument
		}
	} else if name == "" {
		fmt.Fprintln(os.Stderr, "profile set: -name is required")
		return exitInvalidArgument
	}
	normalized, err := profile.NormalizeDisplayName(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "profile set: %v\n", err)
		return exitInvalidArgument
	}
	if *ttlFlag < 0 {
		fmt.Fprintln(os.Stderr, "profile set: -ttl must not be negative")
		return exitInvalidArgument
	}

	var gidPtr *entmoot.GroupID
	if *groupStr != "" {
		gid, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "profile set: %v\n", err)
			return exitInvalidArgument
		}
		gidPtr = &gid
	}

	now := time.Now()
	payload := profile.Profile{DisplayName: normalized, IssuedAtMS: now.UnixMilli()}
	if *ttlFlag > 0 {
		payload.ExpiresAtMS = now.Add(*ttlFlag).UnixMilli()
	}
	content, err := profile.Encode(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "profile set: %v\n", err)
		return exitInvalidArgument
	}

	sockPath := controlSocketPath(gf.data)
	if !controlSocketAlive(sockPath, 500*time.Millisecond) {
		fmt.Fprintln(os.Stderr, runtimeNoDaemonHelp(gf, gf.data))
		return exitControlUnavail
	}
	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		fmt.Fprintln(os.Stderr, runtimeNoDaemonHelp(gf, gf.data))
		return exitControlUnavail
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(*timeoutFlag)); err != nil {
		slog.Error("profile set: set deadline", slog.String("err", err.Error()))
		return exitTransport
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.PublishReq{
		GroupID: gidPtr,
		Topics:  []string{profile.Topic},
		Content: content,
	}); err != nil {
		slog.Error("profile set: write publish_req", slog.String("err", err.Error()))
		return exitTransport
	}
	_, resp, err := ipc.ReadAndDecode(conn)
	if err != nil {
		slog.Error("profile set: read response", slog.String("err", err.Error()))
		return exitTransport
	}
	switch v := resp.(type) {
	case *ipc.PublishResp:
		out := map[string]any{
			"display_name": normalized,
			"group_id":     v.GroupID,
			"message_id":   v.MessageID,
			"timestamp_ms": v.TimestampMS,
		}
		if payload.ExpiresAtMS > 0 {
			out["expires_at_ms"] = payload.ExpiresAtMS
		}
		if normalized == "" {
			out["cleared"] = true
		}
		data, err := json.Marshal(out)
		if err != nil {
			slog.Error("profile set: encode output", slog.String("err", err.Error()))
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	case *ipc.ErrorFrame:
		fmt.Fprintf(os.Stderr, "profile set: %s: %s\n", v.Code, v.Message)
		return ipc.ExitCode(v.Code)
	default:
		fmt.Fprintf(os.Stderr, "profile set: unexpected response %T\n", resp)
		return exitTransport
	}
}

// cmdProfileShow prints the names this node has observed for a group's
// members. It reads local state, so it works with the daemon stopped.
func cmdProfileShow(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("profile show", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (optional when exactly one group is joined)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	var gid entmoot.GroupID
	if *groupStr != "" {
		decoded, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "profile show: %v\n", err)
			return exitInvalidArgument
		}
		gid = decoded
	} else {
		gids, err := listGroupIDs(gf.data, nil)
		if err != nil {
			slog.Error("profile show: list groups", slog.String("err", err.Error()))
			return exitTransport
		}
		if len(gids) != 1 {
			fmt.Fprintf(os.Stderr, "profile show: -group is required when %d groups are joined\n", len(gids))
			return exitInvalidArgument
		}
		gid = gids[0]
	}

	state, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		slog.Error("profile show: open state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer state.Close()

	members, err := localGroupCatalog{dataDir: gf.data, state: state}.ListMembers(context.Background(), gid)
	if err != nil {
		slog.Error("profile show: list members", slog.String("err", err.Error()))
		return exitTransport
	}
	enriched, err := esphttp.EnrichMemberDisplayNames(context.Background(), state, gid, members)
	if err != nil {
		slog.Error("profile show: read profiles", slog.String("err", err.Error()))
		return exitTransport
	}
	rows := make([]map[string]any, 0, len(enriched))
	for _, m := range enriched {
		rows = append(rows, map[string]any{
			"member_id":    m.MemberID.String(),
			"display_name": m.DisplayName,
			"hostname":     m.Hostname,
		})
	}
	data, err := json.Marshal(map[string]any{"group_id": gid.String(), "members": rows})
	if err != nil {
		slog.Error("profile show: encode output", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}
