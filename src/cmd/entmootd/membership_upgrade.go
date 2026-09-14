package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
)

// cmdMembership dispatches `membership <op>`.
func cmdMembership(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "membership: missing op (want: upgrade)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "upgrade":
		return cmdMembershipUpgrade(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "membership: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdMembershipUpgrade turns a group's linear roster chain into its first
// checkpoint. Only the founder can do it, because only the founder's signature
// anchors a group: every other node adopts the checkpoint once it sees it, and
// refuses one whose membership disagrees with the chain it already holds.
//
// The chain is left on disk. Messages that cite one of its entries are still
// verified against it until retention ages those messages out.
func cmdMembershipUpgrade(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("membership upgrade", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership upgrade: %v\n", err)
		return exitInvalidArgument
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("membership upgrade: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	if membership.Exists(s.dataDir, gid) {
		group, err := membership.Open(s.dataDir, gid)
		if err != nil {
			slog.Error("membership upgrade: open membership", slog.String("err", err.Error()))
			return exitTransport
		}
		defer group.Close()
		canonical := group.Canonical()
		fmt.Fprintf(os.Stderr, "membership upgrade: group %s already has checkpoint %d\n", gid.String(), canonical.Sequence)
		data, err := json.Marshal(map[string]any{
			"status":     "already_upgraded",
			"group_id":   gid,
			"checkpoint": canonical.ID,
			"sequence":   canonical.Sequence,
		})
		if err != nil {
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}

	legacy, err := membership.LoadLegacyChain(s.dataDir, gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership upgrade: %v\n", err)
		return exitGroupNotFound
	}
	founder := legacy.Founder()
	if founder.MemberID == nil || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		fmt.Fprintf(os.Stderr, "membership upgrade: this identity is not the founder of group %s; run it on the founder\n", gid.String())
		return exitNotMember
	}

	head := legacy.Head()
	policy := membership.DefaultPolicy()
	policy.Admins = membership.SortAdmins(legacy.Admins())
	state := membership.State{
		Founder:        founder,
		Members:        make(map[entmoot.MemberID]entmoot.NodeInfo),
		Policy:         policy,
		Banned:         make(map[entmoot.MemberID]struct{}),
		RevokedInvites: make(map[[32]byte]struct{}),
		InviteUses:     make(map[[32]byte]int),
	}
	for _, member := range legacy.Members() {
		id, err := entmoot.ResolvedMemberID(member)
		if err != nil {
			fmt.Fprintf(os.Stderr, "membership upgrade: chain member is unresolvable: %v\n", err)
			return exitTransport
		}
		state.Members[id] = member
	}
	body := state.Checkpoint(gid, 0, entmoot.RosterEntryID{}, 0, time.Now().UnixMilli())
	body.LegacyHead = &head
	signed, err := membership.SignCheckpoint(s.identity, founder, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership upgrade: sign checkpoint: %v\n", err)
		return exitTransport
	}
	group, err := membership.Adopt(s.dataDir, signed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership upgrade: %v\n", err)
		return exitTransport
	}
	defer group.Close()

	slog.Info("membership upgrade: checkpoint 0 signed",
		slog.String("group_id", gid.String()),
		slog.String("checkpoint", signed.ID.String()),
		slog.String("legacy_head", head.String()),
		slog.Int("members", len(signed.Members)))
	data, err := json.Marshal(map[string]any{
		"status":      "upgraded",
		"group_id":    gid,
		"checkpoint":  signed.ID,
		"legacy_head": head,
		"members":     len(signed.Members),
		"admins":      len(policy.Admins),
	})
	if err != nil {
		slog.Error("membership upgrade: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}
