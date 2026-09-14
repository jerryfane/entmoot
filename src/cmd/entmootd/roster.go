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
	"entmoot/pkg/entmoot/roster"
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
		fmt.Fprintln(os.Stderr, "roster: missing op (want: add, remove, or admin)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "add":
		return cmdRosterAdd(gf, args[1:])
	case "remove":
		return cmdRosterRemove(gf, args[1:])
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
	rlog, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error("roster admin list: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer rlog.Close()
	founder, ok := rlog.Founder()
	if !ok {
		fmt.Fprintln(os.Stderr, "roster admin list: group has no founder")
		return exitGroupNotFound
	}
	admins := rlog.Admins()
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
	if grant && !ctx.roster.IsMemberID(memberID) {
		fmt.Fprintf(os.Stderr, "%s: %s is not a member of this group\n", command, memberID.String())
		return exitNotMember
	}
	next := make([]entmoot.MemberID, 0, len(ctx.roster.Admins())+1)
	changed := false
	for _, admin := range ctx.roster.Admins() {
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
	payload, err := roster.MarshalAdminPolicy(next)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", command, err)
		return exitInvalidArgument
	}
	timestamp := time.Now().UnixMilli()
	if head := ctx.roster.HeadTimestamp(); timestamp <= head {
		timestamp = head + 1
	}
	entry, err := ctx.roster.SignEntry(ctx.setup.identity, "policy_change", entmoot.NodeInfo{}, payload, timestamp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: sign policy: %v\n", command, err)
		return exitInvalidArgument
	}
	if err := ctx.roster.Apply(entry); err != nil {
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
	admins := ctx.roster.Admins()
	encoded := make([]string, 0, len(admins))
	for _, admin := range admins {
		encoded = append(encoded, admin.String())
	}
	status := "unchanged"
	if changed {
		status = "updated"
	}
	data, err := json.Marshal(map[string]any{
		"status":      status,
		"group_id":    gid,
		"roster_head": ctx.roster.Head(),
		"admins":      encoded,
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

// founderRosterContext is an open, write-leased roster plus the identity that
// will sign. localMemberID is the signer; it is the founder or, for membership
// changes, a delegated admin.
type founderRosterContext struct {
	setup         *setupResult
	roster        *roster.RosterLog
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
	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error(command+": open roster", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	if err := r.ClaimWriter(); err != nil {
		_ = r.Close()
		slog.Error(command+": roster writer", slog.String("err", err.Error()))
		return founderRosterContext{}, exitTransport, false
	}
	founder, ok := r.Founder()
	if !ok {
		_ = r.Close()
		fmt.Fprintf(os.Stderr, "%s: group has no founder (empty roster)\n", command)
		return founderRosterContext{}, exitGroupNotFound, false
	}
	isFounder := founder.MemberID != nil && *founder.MemberID == memberID && bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey)
	if founderOnly && !isFounder {
		_ = r.Close()
		fmt.Fprintf(os.Stderr, "%s: local member is not founder of group %s\n", command, gid.String())
		return founderRosterContext{}, exitNotMember, false
	}
	if !isFounder && !r.CanAdminister(memberID) {
		_ = r.Close()
		fmt.Fprintf(os.Stderr, "%s: local member is neither founder nor a delegated admin of group %s\n", command, gid.String())
		return founderRosterContext{}, exitNotMember, false
	}
	return founderRosterContext{
		setup:         s,
		roster:        r,
		founder:       founder,
		localMemberID: memberID,
		close:         func() { _ = r.Close() },
	}, exitOK, true
}

// cmdRosterAdd admits a new member to a group's roster. Founder-only: the
// local identity must match the declared founder. This offline maintenance
// command acquires the roster writer lease and fails promptly while the daemon
// owns it.
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
	ctx, code, ok := setupAdminRoster(gf, "roster add", gid)
	if !ok {
		return code
	}
	defer ctx.close()

	entry, err := ctx.roster.SignEntry(ctx.setup.identity, "add", subject, nil, time.Now().UnixMilli())
	if err != nil {
		slog.Error("roster add: sign entry", slog.String("err", err.Error()))
		return exitTransport
	}

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
		slog.String("member_id", subject.MemberID.String()),
		slog.String("entry_id", entry.ID.String()))

	binding, _ := libp2ptransport.BindingFromPublicKey(subject.EntmootPubKey)
	out := map[string]any{
		"entry_id": entry.ID,
		"group_id": gid,
		"members":  len(ctx.roster.MemberIDs()),
		"added": map[string]any{
			"member_id":      subject.MemberID,
			"peer_id":        binding.PeerID.String(),
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
	ctx, code, ok := setupAdminRoster(gf, "roster remove", gid)
	if !ok {
		return code
	}
	defer ctx.close()

	existing, ok := ctx.roster.MemberInfoByID(*target.MemberID)
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
		slog.String("member_id", target.MemberID.String()))

	// The removal is already committed, so a revocation failure must not
	// swallow the result: report what happened and what the operator still has
	// to do by hand.
	revoked, openInvites, revokeErr := revokeInvitesAfterRemoval(ctx.setup.dataDir, gid, *target.MemberID)
	if revokeErr != nil {
		slog.Error("roster remove: revoke invites", slog.String("err", revokeErr.Error()))
		fmt.Fprintf(os.Stderr, "roster remove: warning: the member was removed but its invites could not be revoked: %v\n"+
			"Run: entmootd invite list -group %s, then entmootd invite revoke -group %s -nonce <NONCE>\n",
			revokeErr, gid.String(), gid.String())
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
		"members":                      len(ctx.roster.MemberIDs()),
		"revoked_invites":              revoked,
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
	if revokeErr != nil {
		out["invite_revocation_error"] = revokeErr.Error()
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("roster remove: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	if revokeErr != nil {
		return exitTransport
	}
	return exitOK
}

// revokeInvitesAfterRemoval voids every invite bound to the removed member and
// returns the nonces of the group's remaining open bearer invites, which no
// removal can attribute to anyone.
func revokeInvitesAfterRemoval(dataDir string, groupID entmoot.GroupID, memberID entmoot.MemberID) (int, []string, error) {
	admission, err := libp2ptransport.OpenPersistentBootstrapAdmission(dataDir)
	if err != nil {
		return 0, nil, err
	}
	defer admission.Close()
	revoked, err := admission.RevokeInvitesForMember(groupID, memberID)
	if err != nil {
		return 0, nil, err
	}
	live, err := admission.LiveOpenInvites(groupID)
	if err != nil {
		return revoked, nil, err
	}
	nonces := make([]string, 0, len(live))
	for _, record := range live {
		nonces = append(nonces, base64.StdEncoding.EncodeToString(record.Nonce[:]))
	}
	return revoked, nonces, nil
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
