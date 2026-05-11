package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/roster"
)

// cmdRoster dispatches `roster <op>`.
func cmdRoster(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "roster: missing op (want: add or remove)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "add":
		return cmdRosterAdd(gf, args[1:])
	case "remove":
		return cmdRosterRemove(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "roster: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

type rosterMemberFlags struct {
	groupStr  *string
	nodeStr   *string
	pubkeyStr *string
}

func addRosterMemberFlags(fs *flag.FlagSet) rosterMemberFlags {
	return rosterMemberFlags{
		groupStr:  fs.String("group", "", "base64 group id (required)"),
		nodeStr:   fs.String("node", "", "Pilot node id of the member, uint32 (required)"),
		pubkeyStr: fs.String("pubkey", "", "base64 Ed25519 public key of the member (required)"),
	}
}

func parseRosterMemberFlags(command string, flags rosterMemberFlags) (entmoot.GroupID, entmoot.NodeInfo, int, bool) {
	gid, err := decodeGroupID(*flags.groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	if *flags.nodeStr == "" {
		fmt.Fprintf(os.Stderr, "%s: -node is required\n", command)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	nodeU64, err := strconv.ParseUint(*flags.nodeStr, 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -node %q: %v\n", command, *flags.nodeStr, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	if *flags.pubkeyStr == "" {
		fmt.Fprintf(os.Stderr, "%s: -pubkey is required\n", command)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	pubkey, err := decodePubkey(*flags.pubkeyStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -pubkey: %v\n", command, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	return gid, entmoot.NodeInfo{
		PilotNodeID:   entmoot.NodeID(uint32(nodeU64)),
		EntmootPubKey: pubkey,
	}, exitOK, true
}

type founderRosterContext struct {
	setup   *setupResult
	roster  *roster.RosterLog
	founder entmoot.NodeInfo
	close   func()
}

func setupFounderRoster(gf *globalFlags, command string, gid entmoot.GroupID) (founderRosterContext, int, bool) {
	s, err := setup(gf)
	if err != nil {
		slog.Error(command+": setup", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}

	// We need the local Pilot node id to verify we're the founder. Open
	// Pilot only for NodeID(); the accept loop on port 1004 is not used
	// but Pilot.Open binds it anyway.
	tr, err := openPilot(gf)
	if err != nil {
		slog.Error(command+": pilot", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}

	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		tr.Close()
		slog.Error(command+": open roster", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}

	founder, ok := r.Founder()
	if !ok {
		_ = r.Close()
		tr.Close()
		fmt.Fprintf(os.Stderr, "%s: group has no founder (empty roster)\n", command)
		return founderRosterContext{}, exitGroupNotFound, false
	}
	if founder.PilotNodeID != tr.NodeID() {
		_ = r.Close()
		tr.Close()
		fmt.Fprintf(os.Stderr,
			"%s: local node %d is not founder %d of group %s\n",
			command, tr.NodeID(), founder.PilotNodeID, gid.String())
		return founderRosterContext{}, exitNotMember, false
	}

	return founderRosterContext{
		setup:   s,
		roster:  r,
		founder: founder,
		close: func() {
			_ = r.Close()
			tr.Close()
		},
	}, exitOK, true
}

// cmdRosterAdd admits a new member to a group's roster. Founder-only: the
// local identity must match the roster's declared founder, and the signed
// entry is applied to the on-disk roster JSONL. The new member can then
// run `entmootd join <invite>` and participate in gossip.
func cmdRosterAdd(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster add", flag.ContinueOnError)
	memberFlags := addRosterMemberFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	gid, subject, code, ok := parseRosterMemberFlags("roster add", memberFlags)
	if !ok {
		return code
	}
	ctx, code, ok := setupFounderRoster(gf, "roster add", gid)
	if !ok {
		return code
	}
	defer ctx.close()

	// Build the add entry, matching the signing form used by the canary's
	// mkRosterAdd and by roster.Apply's validate path.
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   subject,
		Actor:     ctx.founder.PilotNodeID,
		Timestamp: time.Now().UnixMilli(),
		Parents:   []entmoot.RosterEntryID{ctx.roster.Head()},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		slog.Error("roster add: canonical encode", slog.String("err", err.Error()))
		return exitTransport
	}
	entry.Signature = ctx.setup.identity.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	if err := ctx.roster.Apply(entry); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			fmt.Fprintf(os.Stderr, "roster add: %v\n", err)
			return exitInvalidArgument
		}
		slog.Error("roster add: apply", slog.String("err", err.Error()))
		return exitTransport
	}

	slog.Info("roster add: member admitted",
		slog.String("group_id", gid.String()),
		slog.Uint64("node", uint64(subject.PilotNodeID)),
		slog.String("entry_id", entry.ID.String()))

	out := map[string]any{
		"entry_id": entry.ID,
		"group_id": gid,
		"members":  len(ctx.roster.Members()),
		"added": map[string]any{
			"pilot_node_id":  subject.PilotNodeID,
			"entmoot_pubkey": encodeBase64(subject.EntmootPubKey),
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("roster add: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdRosterRemove removes an existing member from a group's roster. Founder
// only, matching roster add and the ESP member_remove operation.
func cmdRosterRemove(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster remove", flag.ContinueOnError)
	memberFlags := addRosterMemberFlags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	gid, target, code, ok := parseRosterMemberFlags("roster remove", memberFlags)
	if !ok {
		return code
	}
	ctx, code, ok := setupFounderRoster(gf, "roster remove", gid)
	if !ok {
		return code
	}
	defer ctx.close()

	existing, ok := ctx.roster.MemberInfo(target.PilotNodeID)
	if !ok {
		fmt.Fprintln(os.Stderr, "roster remove: target is not a member")
		return exitNotMember
	}
	if !bytes.Equal(existing.EntmootPubKey, target.EntmootPubKey) {
		fmt.Fprintln(os.Stderr, "roster remove: target identity does not match current roster")
		return exitInvalidArgument
	}
	if existing.PilotNodeID == ctx.founder.PilotNodeID {
		fmt.Fprintln(os.Stderr, "roster remove: cannot remove group founder")
		return exitInvalidArgument
	}
	if err := applyFounderRosterRemove(ctx.setup.identity, ctx.roster, ctx.founder, existing); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			fmt.Fprintf(os.Stderr, "roster remove: %v\n", err)
			return exitInvalidArgument
		}
		slog.Error("roster remove: apply", slog.String("err", err.Error()))
		return exitTransport
	}

	slog.Info("roster remove: member removed",
		slog.String("group_id", gid.String()),
		slog.Uint64("node", uint64(target.PilotNodeID)))

	out := map[string]any{
		"group_id": gid,
		"members":  len(ctx.roster.Members()),
		"removed": map[string]any{
			"pilot_node_id":  target.PilotNodeID,
			"entmoot_pubkey": encodeBase64(target.EntmootPubKey),
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("roster remove: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// decodePubkey parses a base64 Ed25519 public key. Accepts both std and
// raw-std encodings. Rejects anything that isn't 32 bytes.
func decodePubkey(s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("base64 decode: %w", err)
		}
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("expected 32 bytes, got %d", len(raw))
	}
	return raw, nil
}
