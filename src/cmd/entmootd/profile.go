package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"context"

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

// profileClaim is a validated display-name claim, encoded as the content of a
// message on profile.Topic.
type profileClaim struct {
	// name is the normalized display name; empty withdraws the published one.
	name    string
	ttl     time.Duration
	payload profile.Profile
	content []byte
}

// newProfileClaim validates the -name and -ttl a set or clear was given and
// encodes the payload. `profile set` and `esp profile set` both publish
// exactly this content, so a name means the same thing whichever path carried
// it. cmd ("profile" or "esp profile") prefixes the messages; on failure the
// problem has already been reported and the exit code is returned.
func newProfileClaim(cmd, name string, clear bool, ttl time.Duration, now time.Time) (profileClaim, int, bool) {
	op := cmd + " set"
	if clear {
		op = cmd + " clear"
		if name != "" {
			fmt.Fprintf(os.Stderr, "%s: -name is not accepted; clear withdraws the published name\n", op)
			return profileClaim{}, exitInvalidArgument, false
		}
	} else if name == "" {
		fmt.Fprintf(os.Stderr, "%s: -name is required\n", op)
		return profileClaim{}, exitInvalidArgument, false
	}
	normalized, err := profile.NormalizeDisplayName(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", op, err)
		return profileClaim{}, exitInvalidArgument, false
	}
	// A name that is only whitespace normalizes to empty, which is the
	// withdrawal payload. Setting must never clear by accident: clearing is
	// what `profile clear` is for, and it says so.
	if !clear && normalized == "" {
		fmt.Fprintf(os.Stderr, "%s: -name is only whitespace; use %q to withdraw a published name\n", op, cmd+" clear")
		return profileClaim{}, exitInvalidArgument, false
	}
	if ttl < 0 {
		fmt.Fprintf(os.Stderr, "%s: -ttl must not be negative\n", op)
		return profileClaim{}, exitInvalidArgument, false
	}
	// A receiving node clamps an expiry beyond its own lifetime bound, so echo
	// the value that will actually be honoured rather than the one asked for.
	if ttl == 0 || ttl > maxProfileLifetime {
		if ttl > maxProfileLifetime {
			fmt.Fprintf(os.Stderr, "%s: -ttl %s exceeds the %s limit a node will honour; using %s\n", op, ttl, maxProfileLifetime, maxProfileLifetime)
		}
		ttl = maxProfileLifetime
	}
	payload := profile.Profile{DisplayName: normalized, IssuedAtMS: now.UnixMilli(), ExpiresAtMS: now.Add(ttl).UnixMilli()}
	content, err := profile.Encode(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", op, err)
		return profileClaim{}, exitInvalidArgument, false
	}
	return profileClaim{name: normalized, ttl: ttl, payload: payload, content: content}, exitOK, true
}

// report adds what was published to a command's JSON output.
func (c profileClaim) report(out map[string]any) {
	out["display_name"] = c.name
	out["expires_at_ms"] = c.payload.ExpiresAtMS
	out["ttl"] = c.ttl.String()
	if c.name == "" {
		out["cleared"] = true
	}
}

// profileNoDaemonHelp is what `profile set` says when no daemon answers. The
// name is published by the daemon, so without one nothing is sent; the ESP
// path publishes the same claim with no daemon at all.
func profileNoDaemonHelp(gf *globalFlags) string {
	return runtimeNoDaemonHelp(gf, gf.data) + "\n" +
		`profile: nothing was published. Start "entmootd serve" and retry, or, with no daemon, ` +
		`run "entmootd esp connect -esp URL -group GID" once and then "entmootd esp profile set -group GID -name NAME".`
}

// cmdProfileSet publishes this node's display name into a group.
//
// The name travels as an ordinary signed message, so it needs the daemon
// running for the same reason `publish` does: the daemon holds the identity,
// the store, and the GossipSub topic. `esp profile set` is the daemonless path.
func cmdProfileSet(gf *globalFlags, args []string, clear bool) int {
	fs := flag.NewFlagSet("profile set", flag.ContinueOnError)
	nameFlag := fs.String("name", "", "display name to publish (required unless clearing)")
	groupStr := fs.String("group", "", "base64 group id (optional when exactly one group is joined)")
	ttlFlag := fs.Duration("ttl", defaultProfileTTL, "how long the name stays current; 0 or a longer value uses the "+maxProfileLifetime.String()+" maximum")
	timeoutFlag := fs.Duration("timeout", 30*time.Second, "IPC response deadline")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	claim, code, ok := newProfileClaim("profile", *nameFlag, clear, *ttlFlag, time.Now())
	if !ok {
		return code
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

	sockPath := controlSocketPath(gf.data)
	if !controlSocketAlive(sockPath, 500*time.Millisecond) {
		fmt.Fprintln(os.Stderr, profileNoDaemonHelp(gf))
		return exitControlUnavail
	}
	conn, err := ipc.DialTimeout(sockPath, 500*time.Millisecond)
	if err != nil {
		fmt.Fprintln(os.Stderr, profileNoDaemonHelp(gf))
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
		Content: claim.content,
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
			"group_id":     v.GroupID,
			"message_id":   v.MessageID,
			"timestamp_ms": v.TimestampMS,
		}
		claim.report(out)
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

// cmdESPProfile is `esp profile set|clear`: the same display-name claim as
// `profile set|clear`, signed locally and posted through the connected ESP
// (`esp connect`), so a member with no running daemon can publish its name.
func cmdESPProfile(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "esp profile: missing op (want: set, clear, or show)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "set":
		return cmdESPProfileSet(gf, args[1:], false)
	case "clear":
		return cmdESPProfileSet(gf, args[1:], true)
	case "show":
		return cmdESPProfileShow(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "esp profile: unknown op %q (want: set, clear, or show)\n", args[0])
		return exitInvalidArgument
	}
}

func cmdESPProfileSet(gf *globalFlags, args []string, clear bool) int {
	op := "esp profile set"
	if clear {
		op = "esp profile clear"
	}
	fs := flag.NewFlagSet(op, flag.ContinueOnError)
	nameFlag := fs.String("name", "", "display name to publish (required unless clearing)")
	groupStr := fs.String("group", "", "group id (required)")
	ttlFlag := fs.Duration("ttl", defaultProfileTTL, "how long the name stays current; 0 or a longer value uses the "+maxProfileLifetime.String()+" maximum")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := parseESPClientGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -group: %v\n", op, err)
		return exitInvalidArgument
	}
	claim, code, ok := newProfileClaim("esp profile", *nameFlag, clear, *ttlFlag, time.Now())
	if !ok {
		return code
	}
	raw, code, err := publishThroughESP(context.Background(), gf, gid, []string{profile.Topic}, claim.content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", op, err)
		return code
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		fmt.Fprintf(os.Stderr, "%s: decode ESP response: %v\n", op, err)
		return exitTransport
	}
	claim.report(out)
	data, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: encode output: %v\n", op, err)
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
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

	members, err := localGroupCatalog{dataDir: gf.data}.ListMembers(context.Background(), gid)
	if err != nil {
		slog.Error("profile show: list members", slog.String("err", err.Error()))
		return exitTransport
	}
	enriched, err := esphttp.EnrichMemberDisplayNames(context.Background(), state, gid, members)
	if err != nil {
		slog.Error("profile show: read profiles", slog.String("err", err.Error()))
		return exitTransport
	}
	return printProfileRows("profile show", gid, enriched)
}

// printProfileRows prints the member/display-name rows `profile show` and
// `esp profile show` share.
func printProfileRows(op string, gid entmoot.GroupID, members []esphttp.MemberSummary) int {
	rows := make([]map[string]any, 0, len(members))
	for _, m := range members {
		rows = append(rows, map[string]any{
			"member_id":    m.MemberID.String(),
			"display_name": m.DisplayName,
			"hostname":     m.Hostname,
		})
	}
	data, err := json.Marshal(map[string]any{"group_id": gid.String(), "members": rows})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: encode output: %v\n", op, err)
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdESPProfileShow prints the names the connected ESP shows for a group's
// members: what the website and apps display. It is how a member with no
// daemon checks that `esp profile set` arrived.
func cmdESPProfileShow(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("esp profile show", flag.ContinueOnError)
	groupStr := fs.String("group", "", "group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := parseESPClientGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp profile show: -group: %v\n", err)
		return exitInvalidArgument
	}
	client, err := openESPClient(gf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "esp profile show: %v\n", err)
		return exitInvalidArgument
	}
	var listing struct {
		Members []esphttp.MemberSummary `json:"members"`
	}
	if err := client.do(context.Background(), http.MethodGet, espGroupPath(gid, "members"), nil, &listing); err != nil {
		fmt.Fprintf(os.Stderr, "esp profile show: %v\n", err)
		return exitTransport
	}
	return printProfileRows("esp profile show", gid, listing.Members)
}
