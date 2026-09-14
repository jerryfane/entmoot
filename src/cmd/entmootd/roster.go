package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/membership"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

// espOpenInviteLister is the part of the ESP state store that reports
// open-invite tokens. Removal paths hold narrower interfaces, so they assert
// for this when telling an operator what bearer credentials remain.
type espOpenInviteLister interface {
	ListOpenInvitesByGroup(context.Context, entmoot.GroupID) ([]esphttp.OpenInviteRecord, error)
}

// cmdRoster dispatches `roster <op>`.
func cmdRoster(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "roster: missing op (want: remove, ban, unban, leave, checkpoint, status, or admin)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "remove":
		return cmdRosterRemove(gf, args[1:])
	case "ban":
		return cmdRosterBan(gf, args[1:])
	case "unban":
		return cmdRosterUnban(gf, args[1:])
	case "leave":
		return cmdRosterLeave(gf, args[1:])
	case "checkpoint":
		return cmdRosterCheckpoint(gf, args[1:])
	case "status":
		return cmdRosterStatus(gf, args[1:])
	case "admin":
		return cmdRosterAdmin(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "roster: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdRosterAdmin dispatches `roster admin <op>`. Delegated admins may invite
// and evict members while the founder is away; only the founder changes who
// holds that authority.
func cmdRosterAdmin(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "roster admin: missing op (want: list, grant, or revoke)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "list":
		return cmdRosterAdminList(gf, args[1:])
	case "grant":
		return cmdRosterAdminChange(gf, args[1:], true)
	case "revoke":
		return cmdRosterAdminChange(gf, args[1:], false)
	default:
		fmt.Fprintf(os.Stderr, "roster admin: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdRosterAdminList(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster admin list", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster admin list: %v\n", err)
		return exitInvalidArgument
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("roster admin list: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	group, err := membership.Open(s.dataDir, gid)
	if err != nil {
		if errors.Is(err, membership.ErrLegacyOnly) {
			fmt.Fprintf(os.Stderr, "roster admin list: group %s has no checkpoint yet\n", gid.String())
			return exitGroupNotFound
		}
		slog.Error("roster admin list: open membership", slog.String("err", err.Error()))
		return exitTransport
	}
	defer group.Close()
	founder := group.Founder()
	admins := group.Admins()
	encoded := make([]string, 0, len(admins))
	for _, admin := range admins {
		encoded = append(encoded, admin.String())
	}
	out := map[string]any{
		"group_id": gid,
		"founder":  founder.MemberID,
		"admins":   encoded,
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("roster admin list: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdRosterAdminChange rewrites the delegated-admin set by adding or removing
// one member. The whole set travels in one founder-signed policy entry, so the
// change is atomic and readable from a single entry.
func cmdRosterAdminChange(gf *globalFlags, args []string, grant bool) int {
	command := "roster admin revoke"
	if grant {
		command = "roster admin grant"
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	memberStr := fs.String("member", "", "base64 MemberID of an existing member (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return exitInvalidArgument
	}
	memberID, err := decodeMemberID(*memberStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -member: %v\n", command, err)
		return exitInvalidArgument
	}
	ctx, code, ok := setupFounderRoster(gf, command, gid)
	if !ok {
		return code
	}
	defer ctx.close()
	if ctx.founder.MemberID != nil && memberID == *ctx.founder.MemberID {
		fmt.Fprintf(os.Stderr, "%s: the founder always administers the group\n", command)
		return exitInvalidArgument
	}
	if grant && !ctx.group.IsMemberID(memberID) {
		fmt.Fprintf(os.Stderr, "%s: %s is not a member of this group\n", command, memberID.String())
		return exitNotMember
	}
	next := make([]entmoot.MemberID, 0, len(ctx.group.Admins())+1)
	changed := false
	for _, admin := range ctx.group.Admins() {
		if admin == memberID {
			if grant {
				return reportAdminSet(ctx, gid, false)
			}
			changed = true
			continue
		}
		next = append(next, admin)
	}
	if grant {
		next = append(next, memberID)
		changed = true
	}
	if !changed {
		return reportAdminSet(ctx, gid, false)
	}
	// The policy record states the complete set, so reading one record is
	// enough to know who may sign after it.
	policy := ctx.group.Policy()
	policy.Admins = membership.SortAdmins(next)
	if _, err := ctx.group.SignRecord(ctx.setup.identity, membership.Record{Kind: membership.KindPolicy, Policy: &policy}); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
			return exitInvalidArgument
		}
		slog.Error(command+": apply", slog.String("err", err.Error()))
		return exitTransport
	}
	return reportAdminSet(ctx, gid, true)
}

func reportAdminSet(ctx founderRosterContext, gid entmoot.GroupID, changed bool) int {
	admins := ctx.group.Admins()
	encoded := make([]string, 0, len(admins))
	for _, admin := range admins {
		encoded = append(encoded, admin.String())
	}
	status := "unchanged"
	if changed {
		status = "updated"
	}
	data, err := json.Marshal(map[string]any{
		"status":     status,
		"group_id":   gid,
		"checkpoint": ctx.group.Canonical().ID,
		"admins":     encoded,
	})
	if err != nil {
		slog.Error("roster admin: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// decodeMemberID parses a base64 MemberID.
func decodeMemberID(encoded string) (entmoot.MemberID, error) {
	var memberID entmoot.MemberID
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return memberID, fmt.Errorf("base64 decode: %w", err)
	}
	if len(raw) != len(memberID) {
		return memberID, fmt.Errorf("member id must be %d bytes, got %d", len(memberID), len(raw))
	}
	copy(memberID[:], raw)
	return memberID, nil
}

type rosterMemberFlags struct {
	groupStr  *string
	memberStr *string
	peerStr   *string
	pubkeyStr *string
}

func addRosterMemberFlags(fs *flag.FlagSet) rosterMemberFlags {
	return rosterMemberFlags{
		groupStr:  fs.String("group", "", "base64 group id (required)"),
		memberStr: fs.String("member", "", "base64 MemberID (required)"),
		peerStr:   fs.String("peer", "", "same-key libp2p PeerID (required)"),
		pubkeyStr: fs.String("pubkey", "", "base64 Ed25519 public key (required)"),
	}
}

func parseRosterMemberFlags(command string, flags rosterMemberFlags) (entmoot.GroupID, entmoot.NodeInfo, int, bool) {
	gid, err := decodeGroupID(*flags.groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	if *flags.memberStr == "" || *flags.peerStr == "" || *flags.pubkeyStr == "" {
		fmt.Fprintf(os.Stderr, "%s: -member, -peer, and -pubkey are required\n", command)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	pubkey, err := decodePubkey(*flags.pubkeyStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -pubkey: %v\n", command, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	binding, err := libp2ptransport.BindingFromPublicKey(pubkey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: identity binding: %v\n", command, err)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	if binding.MemberID.String() != *flags.memberStr {
		fmt.Fprintf(os.Stderr, "%s: -member does not match -pubkey\n", command)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	peerID, err := peer.Decode(*flags.peerStr)
	if err != nil || peerID != binding.PeerID {
		fmt.Fprintf(os.Stderr, "%s: -peer does not match -pubkey\n", command)
		return entmoot.GroupID{}, entmoot.NodeInfo{}, exitInvalidArgument, false
	}
	memberID := binding.MemberID
	return gid, entmoot.NodeInfo{MemberID: &memberID, PeerID: binding.PeerID.String(), EntmootPubKey: pubkey}, exitOK, true
}

// founderRosterContext is an open, write-leased membership store plus the
// identity that will sign. localMemberID is the signer; it is the founder or,
// for membership changes, a delegated admin.
type founderRosterContext struct {
	setup         *setupResult
	group         *membership.Group
	founder       entmoot.NodeInfo
	localMemberID entmoot.MemberID
	close         func()
}

// setupFounderRoster opens the roster for a founder-only operation.
func setupFounderRoster(gf *globalFlags, command string, gid entmoot.GroupID) (founderRosterContext, int, bool) {
	return setupRosterWriter(gf, command, gid, true)
}

// setupAdminRoster opens the roster for an operation any current admin may
// sign: adding and removing ordinary members.
func setupAdminRoster(gf *globalFlags, command string, gid entmoot.GroupID) (founderRosterContext, int, bool) {
	return setupRosterWriter(gf, command, gid, false)
}

func setupRosterWriter(gf *globalFlags, command string, gid entmoot.GroupID, founderOnly bool) (founderRosterContext, int, bool) {
	s, err := setup(gf)
	if err != nil {
		slog.Error(command+": setup", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	memberID, err := entmoot.MemberIDFromPublicKey(s.identity.PublicKey)
	if err != nil {
		slog.Error(command+": local identity", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	g, err := membership.Open(s.dataDir, gid)
	if err != nil {
		if errors.Is(err, membership.ErrLegacyOnly) {
			fmt.Fprintf(os.Stderr, "%s: group %s has no checkpoint yet; run `entmootd membership upgrade -group %s` on the founder\n",
				command, gid.String(), gid.String())
			return founderRosterContext{}, exitGroupNotFound, false
		}
		slog.Error(command+": open membership", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	if err := g.ClaimWriter(); err != nil {
		_ = g.Close()
		slog.Error(command+": membership writer", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	founder := g.Founder()
	isFounder := founder.MemberID != nil && *founder.MemberID == memberID && bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey)
	if founderOnly && !isFounder {
		_ = g.Close()
		fmt.Fprintf(os.Stderr, "%s: local member is not founder of group %s\n", command, gid.String())
		return founderRosterContext{}, exitNotMember, false
	}
	if !isFounder && !g.CanAdminister(memberID) {
		_ = g.Close()
		fmt.Fprintf(os.Stderr, "%s: local member is neither founder nor a delegated admin of group %s\n", command, gid.String())
		return founderRosterContext{}, exitNotMember, false
	}
	return founderRosterContext{
		setup:         s,
		group:         g,
		founder:       founder,
		localMemberID: memberID,
		close:         func() { _ = g.Close() },
	}, exitOK, true
}

// cmdRosterLeave records that the local member is leaving. It needs no admin:
// a member's own departure is a statement about itself, which is the point of
// self-signed records.
func cmdRosterLeave(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster leave", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster leave: %v\n", err)
		return exitInvalidArgument
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("roster leave: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	memberID, err := entmoot.MemberIDFromPublicKey(s.identity.PublicKey)
	if err != nil {
		slog.Error("roster leave: local identity", slog.String("err", err.Error()))
		return exitTransport
	}
	group, err := membership.Open(s.dataDir, gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster leave: %v\n", err)
		return exitGroupNotFound
	}
	defer group.Close()
	if err := group.ClaimWriter(); err != nil {
		slog.Error("roster leave: membership writer", slog.String("err", err.Error()))
		return exitTransport
	}
	if !group.IsMemberID(memberID) {
		fmt.Fprintln(os.Stderr, "roster leave: this identity is not a member of that group")
		return exitNotMember
	}
	record, err := group.SignRecord(s.identity, membership.Record{Kind: membership.KindLeave})
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster leave: %v\n", err)
		return exitInvalidArgument
	}
	data, err := json.Marshal(map[string]any{
		"status":    "left",
		"group_id":  gid,
		"record_id": record.ID,
		"member_id": memberID,
		"members":   len(group.MemberIDs()),
	})
	if err != nil {
		slog.Error("roster leave: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdRosterCheckpoint signs a checkpoint now, rather than waiting for the
// cadence. It is how an operator retires history on demand.
func cmdRosterCheckpoint(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster checkpoint", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster checkpoint: %v\n", err)
		return exitInvalidArgument
	}
	ctx, code, ok := setupAdminRoster(gf, "roster checkpoint", gid)
	if !ok {
		return code
	}
	defer ctx.close()
	checkpoint, signed, err := ctx.group.SignCheckpoint(ctx.setup.identity, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster checkpoint: %v\n", err)
		return exitInvalidArgument
	}
	if !signed {
		fmt.Fprintln(os.Stderr, "roster checkpoint: nothing to fold in")
		return exitOK
	}
	data, err := json.Marshal(map[string]any{
		"status":     "signed",
		"group_id":   gid,
		"checkpoint": checkpoint.ID,
		"sequence":   checkpoint.Sequence,
		"covered":    checkpoint.Covered,
		"members":    len(checkpoint.Members),
	})
	if err != nil {
		slog.Error("roster checkpoint: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdRosterStatus prints what this node holds: the checkpoint it projects
// from, the membership, and how many records are still outside a checkpoint.
func cmdRosterStatus(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("roster status", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster status: %v\n", err)
		return exitInvalidArgument
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("roster status: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	group, err := membership.Open(s.dataDir, gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "roster status: %v\n", err)
		return exitGroupNotFound
	}
	defer group.Close()
	canonical := group.Canonical()
	policy := group.Policy()
	members := make([]string, 0, len(canonical.Members))
	for _, id := range group.MemberIDs() {
		members = append(members, id.String())
	}
	admins := make([]string, 0, len(policy.Admins))
	for _, admin := range policy.Admins {
		admins = append(admins, admin.String())
	}
	banned := make([]string, 0, len(canonical.Banned))
	for _, id := range canonical.Banned {
		banned = append(banned, id.String())
	}
	data, err := json.Marshal(map[string]any{
		"group_id":   gid,
		"founder":    group.Founder().MemberID,
		"checkpoint": canonical.ID,
		"sequence":   canonical.Sequence,
		"pending":    group.EffectivePendingCount(),
		"members":    members,
		"admins":     admins,
		"banned":     banned,
		"policy": map[string]any{
			"join_rule":        policy.JoinRule,
			"checkpoint_every": policy.CheckpointEvery,
		},
	})
	if err != nil {
		slog.Error("roster status: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// cmdRosterRemove removes an existing member from a group's roster. The
// founder or a delegated admin may sign it, matching roster add and the ESP
// member_remove operation; only the founder may remove an admin.
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
	ctx, code, ok := setupAdminRoster(gf, "roster remove", gid)
	if !ok {
		return code
	}
	defer ctx.close()

	existing, ok := ctx.group.MemberInfoByID(*target.MemberID)
	if !ok {
		fmt.Fprintln(os.Stderr, "roster remove: target is not a member")
		return exitNotMember
	}
	if !bytes.Equal(existing.EntmootPubKey, target.EntmootPubKey) {
		fmt.Fprintln(os.Stderr, "roster remove: target identity does not match current roster")
		return exitInvalidArgument
	}
	if existing.MemberID != nil && ctx.founder.MemberID != nil && *existing.MemberID == *ctx.founder.MemberID {
		fmt.Fprintln(os.Stderr, "roster remove: cannot remove group founder")
		return exitInvalidArgument
	}
	if err := applyRosterRemove(ctx.setup.identity, ctx.group, existing); err != nil {
		if errors.Is(err, entmoot.ErrRosterReject) {
			fmt.Fprintf(os.Stderr, "roster remove: %v\n", err)
			return exitInvalidArgument
		}
		slog.Error("roster remove: apply", slog.String("err", err.Error()))
		return exitTransport
	}

	slog.Info("roster remove: member removed",
		slog.String("group_id", gid.String()),
		slog.String("member_id", target.MemberID.String()))

	// Invites the removed member issued are void already: an invite carries
	// its issuer's current authority, and that authority is gone. Invites from
	// other admins are not affected, so list what is still live.
	openInvites, listErr := outstandingOpenInvites(ctx.setup.dataDir, gid)
	if listErr != nil {
		slog.Error("roster remove: read invite ledger", slog.String("err", listErr.Error()))
		fmt.Fprintf(os.Stderr, "roster remove: warning: the member was removed but this node's invite ledger could not be read: %v\n"+
			"Run: entmootd invite list -group %s\n", listErr, gid.String())
	}
	if len(openInvites) > 0 {
		fmt.Fprintf(os.Stderr, "roster remove: warning: %d open bearer invite(s) remain for this group; anyone holding one can still join. Revoke with: entmootd invite revoke -group %s -nonce <NONCE>\n",
			len(openInvites), gid.String())
		for _, nonce := range openInvites {
			fmt.Fprintf(os.Stderr, "  nonce %s\n", nonce)
		}
	}
	espOpen, espErr := liveESPOpenInvites(ctx.setup.dataDir, gid)
	if espErr != nil {
		// Reporting zero when the store could not be read would understate
		// what is outstanding, so report that it is unknown instead.
		slog.Error("roster remove: read esp open invites", slog.String("err", espErr.Error()))
		fmt.Fprintf(os.Stderr, "roster remove: warning: could not read ESP open-invite tokens for this group: %v\n", espErr)
	}
	if espOpen > 0 {
		fmt.Fprintf(os.Stderr, "roster remove: warning: %d ESP open-invite token(s) remain for this group; revoke them through the ESP API\n", espOpen)
	}

	binding, _ := libp2ptransport.BindingFromPublicKey(target.EntmootPubKey)
	out := map[string]any{
		"group_id":                     gid,
		"members":                      len(ctx.group.MemberIDs()),
		"outstanding_open_invites":     openInvites,
		"outstanding_esp_open_invites": espOpen,
		"removed": map[string]any{
			"member_id":      target.MemberID,
			"peer_id":        binding.PeerID.String(),
			"entmoot_pubkey": encodeBase64(target.EntmootPubKey),
		},
	}
	if espErr != nil {
		out["outstanding_esp_open_invites"] = nil
		out["esp_open_invites_error"] = espErr.Error()
	}
	if listErr != nil {
		out["invite_ledger_error"] = listErr.Error()
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("roster remove: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	if listErr != nil {
		return exitTransport
	}
	return exitOK
}

// outstandingOpenInvites lists this node's live bearer invites after a
// removal. The removed member's own invites need no cleanup: an invite is
// worth its issuer's current authority, so losing membership voids them
// everywhere at once. Invites from other admins are unaffected, and an
// operator may want to see them.
func outstandingOpenInvites(dataDir string, groupID entmoot.GroupID) ([]string, error) {
	ledger, err := libp2ptransport.OpenInviteLedger(dataDir)
	if err != nil {
		return nil, err
	}
	defer ledger.Close()
	live, err := ledger.LiveOpenInvites(groupID)
	if err != nil {
		return nil, err
	}
	nonces := make([]string, 0, len(live))
	for _, record := range live {
		nonces = append(nonces, base64.StdEncoding.EncodeToString(record.Nonce[:]))
	}
	return nonces, nil
}

// liveESPOpenInvites counts the ESP-hosted open-invite tokens still redeemable
// for a group. They are a second bearer path into the same group, so a removal
// that only reported daemon-issued invites would understate what is still
// outstanding.
func liveESPOpenInvites(dataDir string, groupID entmoot.GroupID) (int, error) {
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		return 0, err
	}
	defer state.Close()
	records, err := state.ListOpenInvitesByGroup(context.Background(), groupID)
	if err != nil {
		return 0, err
	}
	nowMS := time.Now().UnixMilli()
	live := 0
	for _, record := range records {
		if record.Revoked {
			continue
		}
		if record.ExpiresAtMS > 0 && record.ExpiresAtMS <= nowMS {
			continue
		}
		if record.MaxUses > 0 && record.UseCount >= record.MaxUses {
			continue
		}
		live++
	}
	return live, nil
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

// cmdRosterBan removes a member and bars it from rejoining. A plain removal
// lets the member back in with a fresh invite, which is right for "left the
// team" and wrong for "must not come back".
func cmdRosterBan(gf *globalFlags, args []string) int {
	return rosterBanChange(gf, args, true)
}

// cmdRosterUnban lifts a ban. Founder-only: an admin that could unban could
// undo the founder's decision.
func cmdRosterUnban(gf *globalFlags, args []string) int {
	return rosterBanChange(gf, args, false)
}

func rosterBanChange(gf *globalFlags, args []string, ban bool) int {
	command := "roster unban"
	if ban {
		command = "roster ban"
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	memberStr := fs.String("member", "", "base64 MemberID (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return exitInvalidArgument
	}
	memberID, err := decodeMemberID(*memberStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: -member: %v\n", command, err)
		return exitInvalidArgument
	}

	var ctx founderRosterContext
	var code int
	var ok bool
	if ban {
		ctx, code, ok = setupAdminRoster(gf, command, gid)
	} else {
		ctx, code, ok = setupFounderRoster(gf, command, gid)
	}
	if !ok {
		return code
	}
	defer ctx.close()

	subject := entmoot.NodeInfo{MemberID: &memberID}
	if info, present := ctx.group.MemberInfoByID(memberID); present {
		subject = info
	} else if !ban && !ctx.group.IsBanned(memberID) {
		fmt.Fprintf(os.Stderr, "%s: %s is neither a member nor banned\n", command, memberID.String())
		return exitNotMember
	}
	kind := membership.KindUnban
	record := membership.Record{Kind: kind, Subject: subject}
	if ban {
		record = membership.Record{Kind: membership.KindRemove, Subject: subject, Banned: true}
	}
	signed, err := ctx.group.SignRecord(ctx.setup.identity, record)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return exitInvalidArgument
	}
	status := "unbanned"
	if ban {
		status = "banned"
	}
	data, err := json.Marshal(map[string]any{
		"status":    status,
		"group_id":  gid,
		"record_id": signed.ID,
		"member_id": memberID,
		"members":   len(ctx.group.MemberIDs()),
		"banned":    ctx.group.IsBanned(memberID),
	})
	if err != nil {
		slog.Error(command+": marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}
