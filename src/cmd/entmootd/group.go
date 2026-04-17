package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// cmdGroup dispatches `group <op>`. v1 only recognizes `group create`.
func cmdGroup(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "group: missing op (want: create)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "create":
		return cmdGroupCreate(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "group: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdGroupCreate generates a fresh GroupID, opens an empty roster and
// SQLite store for it, and writes the genesis entry. Emits a JSON object
// on stdout carrying the new group id plus founder NodeInfo.
func cmdGroupCreate(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("group create", flag.ContinueOnError)
	name := fs.String("name", "", "informational group name (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *name == "" {
		fmt.Fprintln(os.Stderr, "group create: -name is required")
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("group create: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	// group create is a founder-local operation; it does NOT require a
	// Pilot transport (no peer traffic). We still need a node id for the
	// founder NodeInfo. We fetch it via Pilot Info only — no Listen.
	// To keep this minimal we invoke the full Pilot.Open (which Listens)
	// but shut it down immediately on return; the alternative would be
	// driver.Connect directly, which duplicates work done by pilot.Open.
	tr, err := openPilot(gf)
	if err != nil {
		slog.Error("group create: pilot", slog.String("err", err.Error()))
		return exitTransport
	}
	defer tr.Close()
	nodeID := tr.NodeID()

	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		slog.Error("group create: rand", slog.String("err", err.Error()))
		return exitTransport
	}

	// Open (initialize) the SQLite store so the group directory is
	// present on disk even before any messages are written; then open the
	// roster and write the genesis entry.
	st, err := store.OpenSQLite(s.dataDir)
	if err != nil {
		slog.Error("group create: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = st.Close() }()

	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error("group create: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = r.Close() }()

	founderInfo := entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	if err := r.Genesis(s.identity, founderInfo, time.Now().UnixMilli()); err != nil {
		slog.Error("group create: genesis", slog.String("err", err.Error()))
		return exitTransport
	}

	// Touch the SQLite store so the database file exists on disk from
	// group-create time onward. MerkleRoot on an empty group is a cheap
	// way to open the DB lazily.
	ctx, cancel := withTimeout(5 * time.Second)
	if _, err := st.MerkleRoot(ctx, gid); err != nil {
		slog.Warn("group create: merkle touch", slog.String("err", err.Error()))
	}
	cancel()

	slog.Info("group created",
		slog.String("group_id", gid.String()),
		slog.String("name", *name),
		slog.Uint64("founder", uint64(nodeID)))

	out := map[string]any{
		"group_id": gid,
		"founder": map[string]any{
			"pilot_node_id":  nodeID,
			"entmoot_pubkey": encodeBase64(s.identity.PublicKey),
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("group create: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}
