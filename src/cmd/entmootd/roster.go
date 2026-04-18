package main

import (
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

// cmdRoster dispatches `roster <op>`. v1.0.1 only recognizes `roster add`.
func cmdRoster(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "roster: missing op (want: add)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "add":
		return cmdRosterAdd(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "roster: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdRosterAdd admits a new member to a group's roster. Founder-only: the
// local identity must match the roster's declared founder, and the signed
// entry is applied to the on-disk roster JSONL. The new member can then
// run `entmootd join <invite>` and participate in gossip.
func cmdRosterAdd(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster add", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	nodeStr := fs.String("node", "", "Pilot node id of the new member, uint32 (required)")
	pubkeyStr := fs.String("pubkey", "", "base64 Ed25519 public key of the new member (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster add: %v\n", err)
		return exitInvalidArgument
	}
	if *nodeStr == "" {
		fmt.Fprintln(os.Stderr, "roster add: -node is required")
		return exitInvalidArgument
	}
	nodeU64, err := strconv.ParseUint(*nodeStr, 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster add: -node %q: %v\n", *nodeStr, err)
		return exitInvalidArgument
	}
	nodeID := entmoot.NodeID(uint32(nodeU64))
	if *pubkeyStr == "" {
		fmt.Fprintln(os.Stderr, "roster add: -pubkey is required")
		return exitInvalidArgument
	}
	pubkey, err := decodePubkey(*pubkeyStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster add: -pubkey: %v\n", err)
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("roster add: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	// We need the local Pilot node id to verify we're the founder. Open
	// Pilot only for NodeID(); the accept loop on port 1004 is not used
	// but Pilot.Open binds it anyway.
	tr, err := openPilot(gf)
	if err != nil {
		slog.Error("roster add: pilot", slog.String("err", err.Error()))
		return exitTransport
	}
	defer tr.Close()
	localNode := tr.NodeID()

	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error("roster add: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = r.Close() }()

	founder, ok := r.Founder()
	if !ok {
		fmt.Fprintln(os.Stderr, "roster add: group has no founder (empty roster)")
		return exitGroupNotFound
	}
	if founder.PilotNodeID != localNode {
		fmt.Fprintf(os.Stderr,
			"roster add: local node %d is not founder %d of group %s\n",
			localNode, founder.PilotNodeID, gid.String())
		return exitNotMember
	}

	// Build the add entry, matching the signing form used by the canary's
	// mkRosterAdd and by roster.Apply's validate path.
	subject := entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: pubkey,
	}
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   subject,
		Actor:     founder.PilotNodeID,
		Timestamp: time.Now().UnixMilli(),
		Parents:   []entmoot.RosterEntryID{r.Head()},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		slog.Error("roster add: canonical encode", slog.String("err", err.Error()))
		return exitTransport
	}
	entry.Signature = s.identity.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)

	if err := r.Apply(entry); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			fmt.Fprintf(os.Stderr, "roster add: %v\n", err)
			return exitInvalidArgument
		}
		slog.Error("roster add: apply", slog.String("err", err.Error()))
		return exitTransport
	}

	slog.Info("roster add: member admitted",
		slog.String("group_id", gid.String()),
		slog.Uint64("node", uint64(nodeID)),
		slog.String("entry_id", entry.ID.String()))

	out := map[string]any{
		"entry_id": entry.ID,
		"group_id": gid,
		"members":  len(r.Members()),
		"added": map[string]any{
			"pilot_node_id":  nodeID,
			"entmoot_pubkey": encodeBase64(pubkey),
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
