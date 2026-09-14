package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/roster"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// maxInviteUses bounds a multi-use invite. It keeps one leaked link from
// admitting an unbounded crowd while still covering "five people from my
// team".
const maxInviteUses = 64

func cmdInvite(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: entmootd invite <create|list|revoke> [flags]")
		return exitInvalidArgument
	}
	switch args[0] {
	case "create":
		return cmdInviteCreate(gf, args[1:])
	case "list":
		return cmdInviteList(gf, args[1:])
	case "revoke":
		return cmdInviteRevoke(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "invite: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdInviteCreate emits a bootstrap capability. It is bound to one target
// identity unless -open is given, which mints a bearer invite any holder may
// redeem while uses remain. Bootstrap addresses must be full multiaddrs
// ending in /p2p/<founder-peer-id>.
func cmdInviteCreate(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite create", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	targetKey := fs.String("target-pubkey", "", "base64 Ed25519 public key of the joining identity (required unless -open)")
	open := fs.Bool("open", false, "mint a bearer invite with no target identity; any holder may redeem it")
	maxUses := fs.Int("max-uses", 1, "how many distinct identities may join with this invite")
	validFor := fs.String("valid-for", "24h", "capability TTL (time.ParseDuration or <N>d)")
	var bootstrap stringListFlag
	fs.Var(&bootstrap, "bootstrap", "issuing node's libp2p multiaddr ending in /p2p/<peer-id>; repeatable")
	var relays stringListFlag
	fs.Var(&relays, "relay", "controlled-relay multiaddr the joiner should adopt, ending in /p2p/<relay-peer-id>; repeatable; defaults to this data root's own relays")
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
	if err != nil || ttl <= 0 {
		fmt.Fprintf(os.Stderr, "invite create: -valid-for must be positive: %v\n", err)
		return exitInvalidArgument
	}
	if len(bootstrap) == 0 {
		fmt.Fprintln(os.Stderr, "invite create: at least one -bootstrap multiaddr is required")
		return exitInvalidArgument
	}
	if *maxUses < 1 || *maxUses > maxInviteUses {
		fmt.Fprintf(os.Stderr, "invite create: -max-uses must be between 1 and %d\n", maxInviteUses)
		return exitInvalidArgument
	}
	switch {
	case *open && *targetKey != "":
		fmt.Fprintln(os.Stderr, "invite create: -open and -target-pubkey are mutually exclusive")
		return exitInvalidArgument
	case !*open && *targetKey == "":
		fmt.Fprintln(os.Stderr, "invite create: -target-pubkey is required; pass -open to mint a bearer invite any holder can redeem")
		return exitInvalidArgument
	}
	var publicKey []byte
	var targetMemberID entmoot.MemberID
	targetPeerID := ""
	if *targetKey != "" {
		decoded, err := base64.StdEncoding.DecodeString(*targetKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invite create: -target-pubkey: %v\n", err)
			return exitInvalidArgument
		}
		target, err := libp2ptransport.BindingFromPublicKey(decoded)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invite create: -target-pubkey: %v\n", err)
			return exitInvalidArgument
		}
		publicKey = decoded
		targetMemberID = target.MemberID
		targetPeerID = target.PeerID.String()
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("invite create: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	rlog, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		slog.Error("invite create: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer rlog.Close()
	founder, ok := rlog.Founder()
	if !ok {
		fmt.Fprintln(os.Stderr, "invite create: group has no founder")
		return exitGroupNotFound
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founder.EntmootPubKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: founder identity: %v\n", err)
		return exitTransport
	}
	founder.MemberID = &founderBinding.MemberID
	// The founder or any delegated admin may invite. The issuer's own host is
	// what serves enrollment, so the bootstrap addresses must name it.
	localMemberID := mustMemberID(s.identity.PublicKey)
	if !rlog.CanAdminister(localMemberID) {
		fmt.Fprintln(os.Stderr, "invite create: local identity is neither the group founder nor a delegated admin")
		return exitNotMember
	}
	localBinding, err := libp2ptransport.BindingFromPublicKey(s.identity.PublicKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: local identity: %v\n", err)
		return exitTransport
	}
	var issuer *entmoot.NodeInfo
	if localMemberID != founderBinding.MemberID {
		info, found := rlog.MemberInfoByID(localMemberID)
		if !found {
			fmt.Fprintln(os.Stderr, "invite create: local identity is not a member of this group")
			return exitNotMember
		}
		memberID := localMemberID
		info.MemberID = &memberID
		info.PeerID = localBinding.PeerID.String()
		issuer = &info
	}
	allowedPeerIDs := make([]string, 0, len(bootstrap))
	allowedAddresses := make([]string, 0, len(bootstrap))
	seen := make(map[peer.ID]struct{})
	for _, raw := range bootstrap {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invite create: invalid -bootstrap %q: %v\n", raw, err)
			return exitInvalidArgument
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil || info.ID != localBinding.PeerID {
			fmt.Fprintf(os.Stderr, "invite create: bootstrap must end in the issuing node's peer id %s\n", localBinding.PeerID)
			return exitInvalidArgument
		}
		allowedAddresses = append(allowedAddresses, address.String())
		if _, ok := seen[info.ID]; !ok {
			seen[info.ID] = struct{}{}
			allowedPeerIDs = append(allowedPeerIDs, info.ID.String())
		}
	}
	// Relay hints default to whatever this node itself relays through, since
	// that is the set already known to accept it.
	relayHints := []string(relays)
	if len(relayHints) == 0 {
		relayHints = gf.controlledRelays
	}
	if len(relayHints) == 0 {
		if stored, err := loadRelayHints(s.dataDir); err == nil {
			relayHints = stored
		}
	}
	relayHints, err = validateRelayHints(relayHints)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: -relay: %v\n", err)
		return exitInvalidArgument
	}
	if len(relays) > 0 && len(relayHints) != len(relays) {
		fmt.Fprintln(os.Stderr, "invite create: every -relay must be a multiaddr ending in /p2p/<relay-peer-id>")
		return exitInvalidArgument
	}
	now := time.Now()
	capability := entmoot.BootstrapCapability{
		GroupID:           gid,
		TargetPublicKey:   append([]byte(nil), publicKey...),
		TargetMemberID:    targetMemberID,
		TargetPeerID:      targetPeerID,
		Founder:           founder,
		Issuer:            issuer,
		RosterHead:        rlog.Head(),
		AllowedPeerIDs:    allowedPeerIDs,
		AllowedMultiaddrs: allowedAddresses,
		Relays:            relayHints,
		MaxUses:           *maxUses,
		IssuedAtMS:        now.UnixMilli(),
		ExpiresAtMS:       now.Add(ttl).UnixMilli(),
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		slog.Error("invite create: nonce", slog.String("err", err.Error()))
		return exitTransport
	}
	if err := libp2ptransport.SignBootstrapCapability(s.identity, &capability); err != nil {
		slog.Error("invite create: sign", slog.String("err", err.Error()))
		return exitTransport
	}
	admission, err := libp2ptransport.OpenPersistentBootstrapAdmission(s.dataDir)
	if err != nil {
		slog.Error("invite create: open admission", slog.String("err", err.Error()))
		return exitTransport
	}
	defer admission.Close()
	if err := admission.RecordIssuedInvite(capability); err != nil {
		slog.Error("invite create: record invite", slog.String("err", err.Error()))
		return exitTransport
	}
	encoded, err := json.MarshalIndent(capability, "", "  ")
	if err != nil {
		slog.Error("invite create: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	binding := "bound to target " + capability.TargetMemberID.String()
	if capability.IsOpenInvite() {
		binding = "OPEN bearer invite: any holder can redeem it"
	}
	fmt.Fprintf(os.Stderr, "invite create: %s; max uses %d; expires %s; nonce %s\n",
		binding, capability.Uses(), time.UnixMilli(capability.ExpiresAtMS).Format(time.RFC3339),
		base64.StdEncoding.EncodeToString(capability.Nonce[:]))
	fmt.Println(string(encoded))
	return exitOK
}

// cmdInviteList prints the invites this data root issued together with how
// many uses each has spent, so an operator can see what is still outstanding
// before revoking anything.
func cmdInviteList(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite list", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id; omit to list every group")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	var filter *entmoot.GroupID
	if *groupStr != "" {
		gid, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invite list: %v\n", err)
			return exitInvalidArgument
		}
		filter = &gid
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("invite list: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	admission, err := libp2ptransport.OpenPersistentBootstrapAdmission(s.dataDir)
	if err != nil {
		slog.Error("invite list: open admission", slog.String("err", err.Error()))
		return exitTransport
	}
	defer admission.Close()
	records, err := admission.ListInvites(filter)
	if err != nil {
		slog.Error("invite list: read invites", slog.String("err", err.Error()))
		return exitTransport
	}
	type inviteJSON struct {
		GroupID        string `json:"group_id"`
		Nonce          string `json:"nonce"`
		Open           bool   `json:"open"`
		TargetMemberID string `json:"target_member_id,omitempty"`
		MaxUses        int    `json:"max_uses"`
		UsesCommitted  int    `json:"uses_committed"`
		UsesReserved   int    `json:"uses_reserved"`
		IssuedAtMS     int64  `json:"issued_at_ms"`
		ExpiresAtMS    int64  `json:"expires_at_ms"`
		RevokedAtMS    int64  `json:"revoked_at_ms,omitempty"`
		State          string `json:"state"`
	}
	nowMS := time.Now().UnixMilli()
	out := make([]inviteJSON, 0, len(records))
	for _, record := range records {
		item := inviteJSON{
			GroupID:       record.GroupID.String(),
			Nonce:         base64.StdEncoding.EncodeToString(record.Nonce[:]),
			Open:          record.Open(),
			MaxUses:       record.MaxUses,
			UsesCommitted: record.UsesCommitted,
			UsesReserved:  record.UsesReserved,
			IssuedAtMS:    record.IssuedAtMS,
			ExpiresAtMS:   record.ExpiresAtMS,
			RevokedAtMS:   record.RevokedAtMS,
		}
		if record.TargetMemberID != nil {
			item.TargetMemberID = record.TargetMemberID.String()
		}
		switch {
		case record.RevokedAtMS > 0:
			item.State = "revoked"
		case record.ExpiresAtMS > 0 && record.ExpiresAtMS <= nowMS:
			item.State = "expired"
		case record.MaxUses > 0 && record.UsesCommitted >= record.MaxUses:
			item.State = "spent"
		default:
			item.State = "open"
		}
		out = append(out, item)
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		slog.Error("invite list: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(encoded))
	return exitOK
}

// cmdInviteRevoke withdraws an outstanding invite before it expires. Revoking
// blocks every remaining use, including uses of an invite this data root never
// recorded.
func cmdInviteRevoke(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite revoke", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	nonceStr := fs.String("nonce", "", "base64 invite nonce from the invite file (required)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite revoke: %v\n", err)
		return exitInvalidArgument
	}
	nonce, err := libp2ptransport.DecodeInviteNonce(*nonceStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite revoke: -nonce: %v\n", err)
		return exitInvalidArgument
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error("invite revoke: setup", slog.String("err", err.Error()))
		return exitTransport
	}
	admission, err := libp2ptransport.OpenPersistentBootstrapAdmission(s.dataDir)
	if err != nil {
		slog.Error("invite revoke: open admission", slog.String("err", err.Error()))
		return exitTransport
	}
	defer admission.Close()
	revoked, err := admission.RevokeInvite(gid, nonce)
	if err != nil {
		slog.Error("invite revoke: revoke", slog.String("err", err.Error()))
		return exitTransport
	}
	status := "already_revoked"
	if revoked {
		status = "revoked"
	}
	encoded, err := json.Marshal(map[string]any{
		"status":   status,
		"group_id": gid.String(),
		"nonce":    base64.StdEncoding.EncodeToString(nonce[:]),
	})
	if err != nil {
		slog.Error("invite revoke: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(encoded))
	return exitOK
}

func mustMemberID(publicKey []byte) entmoot.MemberID {
	memberID, err := entmoot.MemberIDFromPublicKey(publicKey)
	if err != nil {
		panic(err)
	}
	return memberID
}
