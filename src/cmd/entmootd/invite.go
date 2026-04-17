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
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// cmdInvite dispatches `invite <op>`; v1 only recognizes `invite create`.
func cmdInvite(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "invite: missing op (want: create)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "create":
		return cmdInviteCreate(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "invite: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdInviteCreate reads the roster for -group, signs a fresh Invite
// bundle with a ValidUntil window, and prints it to stdout as JSON.
func cmdInviteCreate(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite create", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	peers := fs.String("peers", "", "comma-separated bootstrap peer node ids (optional)")
	validFor := fs.String("valid-for", "24h", "invite TTL (time.ParseDuration or <N>d)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: %v\n", err)
		return exitInvalidArgument
	}
	ttl, err := parseDurationDays(*validFor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: -valid-for: %v\n", err)
		return exitInvalidArgument
	}
	if ttl <= 0 {
		fmt.Fprintln(os.Stderr, "invite create: -valid-for must be positive")
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("invite create: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	tr, err := openPilot(gf)
	if err != nil {
		slog.Error("invite create: pilot", slog.String("err", err.Error()))
		return exitTransport
	}
	defer tr.Close()
	nodeID := tr.NodeID()

	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error("invite create: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = r.Close() }()

	founder, ok := r.Founder()
	if !ok {
		fmt.Fprintln(os.Stderr, "invite create: group has no founder (empty roster)")
		return exitGroupNotFound
	}

	st, err := store.OpenSQLite(s.dataDir)
	if err != nil {
		slog.Error("invite create: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = st.Close() }()

	ctx, cancel := withTimeout(10 * time.Second)
	defer cancel()
	merkleRoot, err := st.MerkleRoot(ctx, gid)
	if err != nil {
		slog.Error("invite create: merkle root", slog.String("err", err.Error()))
		return exitTransport
	}

	peerIDs, err := parsePeerList(*peers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: %v\n", err)
		return exitInvalidArgument
	}
	if len(peerIDs) == 0 {
		peerIDs = defaultBootstrapPeers(r, founder.PilotNodeID, 5)
	}
	bootstrap := make([]entmoot.BootstrapPeer, 0, len(peerIDs))
	for _, p := range peerIDs {
		bootstrap = append(bootstrap, entmoot.BootstrapPeer{NodeID: p})
	}

	issuerInfo := entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	issuedAt := time.Now().UnixMilli()
	invite := entmoot.Invite{
		GroupID:        gid,
		Founder:        founder,
		RosterHead:     r.Head(),
		MerkleRoot:     merkleRoot,
		BootstrapPeers: bootstrap,
		IssuedAt:       issuedAt,
		ValidUntil:     issuedAt + ttl.Milliseconds(),
		Issuer:         issuerInfo,
	}

	signing := invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		slog.Error("invite create: canonical encode", slog.String("err", err.Error()))
		return exitTransport
	}
	invite.Signature = s.identity.Sign(sigInput)

	data, err := json.MarshalIndent(&invite, "", "  ")
	if err != nil {
		slog.Error("invite create: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// defaultBootstrapPeers returns founder + up to max-1 other random
// members. Member ordering from RosterLog.Members is deterministic
// (sorted ascending by node id).
func defaultBootstrapPeers(r *roster.RosterLog, founder entmoot.NodeID, max int) []entmoot.NodeID {
	members := r.Members()
	out := []entmoot.NodeID{founder}
	added := 1
	for _, m := range members {
		if added >= max {
			break
		}
		if m == founder {
			continue
		}
		out = append(out, m)
		added++
	}
	return out
}
