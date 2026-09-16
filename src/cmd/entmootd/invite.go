package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
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
// ending in /p2p/<peer-id>, naming this node or another current member.
func cmdInviteCreate(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite create", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	targetKey := fs.String("target-pubkey", "", "base64 Ed25519 public key of the joining identity (required unless -open)")
	open := fs.Bool("open", false, "mint a bearer invite with no target identity; any holder may redeem it")
	maxUses := fs.Int("max-uses", 1, "how many distinct identities may join with this invite")
	validFor := fs.String("valid-for", "24h", "capability TTL (time.ParseDuration or <N>d)")
	var bootstrap stringListFlag
	fs.Var(&bootstrap, "bootstrap", "libp2p multiaddr of this node or another current member, ending in /p2p/<peer-id>; repeatable")
	noFallback := fs.Bool("no-fallback-peers", false, "do not attach other members' known addresses as fallback bootstrap peers")
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
	group, err := membership.Open(s.dataDir, gid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: %v\n", err)
		return exitGroupNotFound
	}
	defer group.Close()
	founder := group.Founder()
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founder.EntmootPubKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: founder identity: %v\n", err)
		return exitTransport
	}
	founder.MemberID = &founderBinding.MemberID
	// The founder or any delegated admin may invite. Whichever member the
	// bootstrap addresses name is what serves the checkpoint a joiner reads —
	// not necessarily this node, which is what lets an invite outlive its
	// issuer's uptime.
	localMemberID := mustMemberID(s.identity.PublicKey)
	if !group.CanAdminister(localMemberID) {
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
		info, found := group.MemberInfoByID(localMemberID)
		if !found {
			fmt.Fprintln(os.Stderr, "invite create: local identity is not a member of this group")
			return exitNotMember
		}
		memberID := localMemberID
		info.MemberID = &memberID
		info.PeerID = localBinding.PeerID.String()
		issuer = &info
	}
	// A bootstrap address may name ANY current member, not only the issuer.
	// The newcomer pins the founder's key from this capability and verifies
	// the checkpoint it is served against that key, so a named peer cannot
	// forge membership — it can only serve or fail. Restricting the list to
	// the issuer meant the issuer had to be running for its own invite to be
	// usable, which is the one thing self-signed admission was supposed to
	// remove.
	memberPeers, err := groupMemberPeerIDs(group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: read group members: %v\n", err)
		return exitTransport
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
		if err != nil {
			fmt.Fprintf(os.Stderr, "invite create: -bootstrap must end in /p2p/<peer-id>: %s\n", raw)
			return exitInvalidArgument
		}
		if _, ok := memberPeers[info.ID]; !ok && info.ID != localBinding.PeerID {
			fmt.Fprintf(os.Stderr, "invite create: bootstrap peer %s is not a member of this group; name this node or another member\n", info.ID)
			return exitInvalidArgument
		}
		allowedAddresses = append(allowedAddresses, address.String())
		if _, ok := seen[info.ID]; !ok {
			seen[info.ID] = struct{}{}
			allowedPeerIDs = append(allowedPeerIDs, info.ID.String())
		}
	}
	if !*noFallback {
		var privateFallbacks int
		allowedAddresses, allowedPeerIDs, privateFallbacks = addKnownMemberPeers(s.dataDir, gid, memberPeers,
			localBinding.PeerID, allowedAddresses, allowedPeerIDs, seen)
		if privateFallbacks > 0 {
			fmt.Fprintf(os.Stderr, "invite create: %d fallback peer(s) are known only on a private address; the invite carries it, which works on the same network and not beyond it\n", privateFallbacks)
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
	// An invite carries a bounded number of relay BYTES, so a large hint set
	// is trimmed here rather than silently later. Say so: dropping a relay an
	// operator named without a word is how a joiner ends up unable to reach
	// the group through the path the operator intended.
	if bounded := boundInviteRelays(relayHints); len(bounded) != len(relayHints) {
		fmt.Fprintf(os.Stderr, "invite create: carrying %d of %d relay hints; the rest do not fit the invite's %d-byte relay budget\n",
			len(bounded), len(relayHints), maxInviteFallbackBytes)
		relayHints = bounded
	}
	now := time.Now()
	capability := entmoot.BootstrapCapability{
		GroupID:           gid,
		TargetPublicKey:   append([]byte(nil), publicKey...),
		TargetMemberID:    targetMemberID,
		TargetPeerID:      targetPeerID,
		Founder:           founder,
		Issuer:            issuer,
		RosterHead:        group.Canonical().ID,
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
	// Refuse here rather than let the mint succeed and the redemption fail
	// with a size error the joiner cannot act on.
	if size, tooLarge := libp2ptransport.CapabilityTooLarge(capability); tooLarge {
		fmt.Fprintf(os.Stderr, "invite create: the invite is %d bytes, over the %d-byte limit a joiner can send; name fewer -bootstrap addresses or pass -no-fallback-peers\n",
			size, libp2ptransport.MaxCapabilityBytes)
		return exitInvalidArgument
	}
	admission, err := libp2ptransport.OpenInviteLedger(s.dataDir)
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
	admission, err := libp2ptransport.OpenInviteLedger(s.dataDir)
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
	// Uses and revocations are group state, not ledger columns: the ledger
	// only records what this node handed out. Open each group once and read
	// the counts every node agrees on.
	groups := make(map[entmoot.GroupID]*membership.Group)
	defer func() {
		for _, group := range groups {
			_ = group.Close()
		}
	}()
	groupFor := func(gid entmoot.GroupID) *membership.Group {
		if group, seen := groups[gid]; seen {
			return group
		}
		group, ok, err := openExistingGroup(s.dataDir, gid)
		if err != nil || !ok {
			groups[gid] = nil
			return nil
		}
		groups[gid] = group
		return group
	}
	uses := func(record libp2ptransport.InviteRecord) int {
		if group := groupFor(record.GroupID); group != nil {
			return group.InviteUses(record.Nonce)
		}
		return 0
	}
	revoked := func(record libp2ptransport.InviteRecord) bool {
		if group := groupFor(record.GroupID); group != nil {
			return group.IsInviteRevoked(record.Nonce)
		}
		return record.RevokedAtMS > 0
	}
	type inviteJSON struct {
		GroupID        string `json:"group_id"`
		Nonce          string `json:"nonce"`
		Open           bool   `json:"open"`
		TargetMemberID string `json:"target_member_id,omitempty"`
		MaxUses        int    `json:"max_uses"`
		Uses           int    `json:"uses"`
		IssuedAtMS     int64  `json:"issued_at_ms"`
		ExpiresAtMS    int64  `json:"expires_at_ms"`
		RevokedAtMS    int64  `json:"revoked_at_ms,omitempty"`
		State          string `json:"state"`
	}
	nowMS := time.Now().UnixMilli()
	out := make([]inviteJSON, 0, len(records))
	for _, record := range records {
		item := inviteJSON{
			GroupID:     record.GroupID.String(),
			Nonce:       base64.StdEncoding.EncodeToString(record.Nonce[:]),
			Open:        record.Open(),
			MaxUses:     record.MaxUses,
			Uses:        uses(record),
			IssuedAtMS:  record.IssuedAtMS,
			ExpiresAtMS: record.ExpiresAtMS,
			RevokedAtMS: record.RevokedAtMS,
		}
		if record.TargetMemberID != nil {
			item.TargetMemberID = record.TargetMemberID.String()
		}
		switch {
		case record.RevokedAtMS > 0:
			item.State = "revoked"
		case record.ExpiresAtMS > 0 && record.ExpiresAtMS <= nowMS:
			item.State = "expired"
		case revoked(record):
			item.State = "revoked"
		case record.MaxUses > 0 && uses(record) >= record.MaxUses:
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
	admission, err := libp2ptransport.OpenInviteLedger(s.dataDir)
	if err != nil {
		slog.Error("invite revoke: open admission", slog.String("err", err.Error()))
		return exitTransport
	}
	defer admission.Close()
	// A local ledger row stops this node from advertising the invite. What
	// stops every other node honouring it is a signed record, so sign one:
	// without it a revoked invite still works anywhere it is presented.
	ctx, code, ok := setupAdminRoster(gf, "invite revoke", gid)
	if !ok {
		return code
	}
	defer ctx.close()
	record, err := ctx.group.SignRecord(ctx.setup.identity, membership.Record{
		Kind:        membership.KindRevokeInvite,
		InviteNonce: nonce,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite revoke: %v\n", err)
		return exitInvalidArgument
	}
	noted, err := admission.MarkRevoked(gid, nonce)
	if err != nil {
		slog.Error("invite revoke: ledger", slog.String("err", err.Error()))
		return exitTransport
	}
	status := "revoked"
	if !noted {
		status = "revoked_not_issued_here"
	}
	encoded, err := json.Marshal(map[string]any{
		"status":    status,
		"record_id": record.ID,
		"group_id":  gid.String(),
		"nonce":     base64.StdEncoding.EncodeToString(nonce[:]),
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

// groupMemberPeerIDs maps the group's current members to their transport peer
// ids. Both are derived from the same Ed25519 key, so membership is what makes
// a peer id serveable: no separate list has to be maintained.
func groupMemberPeerIDs(group *membership.Group) (map[peer.ID]struct{}, error) {
	out := make(map[peer.ID]struct{})
	for _, memberID := range group.MemberIDs() {
		info, ok := group.MemberInfoByID(memberID)
		if !ok || len(info.EntmootPubKey) == 0 {
			continue
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			return nil, err
		}
		out[binding.PeerID] = struct{}{}
	}
	return out, nil
}

// Auto-attached fallback bounds. The cap that matters is on ADDRESSES, not
// members: a multi-homed node can hold thirty of them, and a capability is
// carried in one request frame with an 8 KiB ceiling, so bounding members
// alone let an invite grow past the size at which it can be redeemed at all.
const (
	maxInviteFallbackPeers     = 4
	maxInviteFallbackAddrs     = 8
	maxInviteFallbackAddrsPeer = 2
	// maxInviteAddrBytes bounds ONE attached address, fallback or relay, and
	// maxInviteFallbackBytes bounds the fallback set TOGETHER. Counts alone
	// were not enough twice over: peer caches hold long forms — a
	// quic-v1/webtransport address with two certhashes runs past 190 bytes —
	// so eight slots at full width plus eight relay hints consumed the whole
	// capability budget before the operator named anything. Bounding bytes is
	// what makes the minted size predictable to a caller that must estimate
	// it before the capability exists.
	maxInviteAddrBytes     = 256
	maxInviteFallbackBytes = 1 << 10
	// maxPeerIDBytes bounds a base58 libp2p peer id string; ed25519 identity
	// peer ids are 52 characters, and this leaves room for other key types.
	maxPeerIDBytes = 64
	// maxInviteCapabilityOverhead charges every field of a capability that is
	// not an address or a peer id: both identities, the checkpoint id, nonce,
	// signature, timestamps, target fields and the JSON structure itself.
	// TestCapabilityOverheadIsBounded measures a capability built the way the
	// mint builds one and fails if this stops being an upper bound, so the
	// number is checked against the encoder rather than argued for.
	maxInviteCapabilityOverhead = 1536
)

// routableInviteAddress reports whether an address is reachable from outside
// this host. It is a PREFERENCE, not a filter: the caller attaches routable
// addresses first and gives a member known only on a non-routable address a
// single slot, because on a LAN or an overlay that address is the one that
// works. What the predicate buys is that the byte budget goes to addresses
// likely to work, and that an invite carries one of a member's internal
// addresses rather than its whole network.
func routableInviteAddress(addr multiaddr.Multiaddr) bool {
	value, err := addr.ValueForProtocol(multiaddr.P_IP4)
	if err != nil {
		if value, err = addr.ValueForProtocol(multiaddr.P_IP6); err != nil {
			// A DNS or relay address carries no literal to judge; keep it.
			return true
		}
	}
	ip := net.ParseIP(value)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsPrivate() {
		return false
	}
	// 100.64.0.0/10, carrier-grade NAT: reachable only inside one provider.
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return false
	}
	return true
}

// addKnownMemberPeers appends addresses of OTHER current members this node has
// seen, so an invite keeps working when the issuer is down. It never fails the
// invite: fewer addresses just means the newcomer has fewer doors to try, and
// the operator can always name peers explicitly, or pass -no-fallback-peers to
// attach none.
//
// Routable addresses are preferred, but a member reachable ONLY on a private
// address still gets one slot: on a LAN or an overlay network that private
// address is exactly the door that works, and silently attaching nothing there
// would leave the invite depending on the issuer's uptime while the operator
// believed otherwise. It reports how many of each kind it attached so the
// caller can say so.
func addKnownMemberPeers(dataDir string, groupID entmoot.GroupID, memberPeers map[peer.ID]struct{}, self peer.ID,
	addresses []string, peerIDs []string, seen map[peer.ID]struct{}) ([]string, []string, int) {
	cached, err := loadGroupPeers(dataDir, groupID)
	if err != nil {
		return addresses, peerIDs, 0
	}
	peers, addrs, bytes, private := 0, 0, 0, 0
	for _, info := range cached {
		if peers >= maxInviteFallbackPeers || addrs >= maxInviteFallbackAddrs || bytes >= maxInviteFallbackBytes {
			break
		}
		if info.ID == self {
			continue
		}
		if _, ok := memberPeers[info.ID]; !ok {
			continue
		}
		if _, ok := seen[info.ID]; ok {
			continue
		}
		routable, fallback := make([]string, 0, maxInviteFallbackAddrsPeer), ""
		for _, addr := range info.Addrs {
			full := addr.String() + "/p2p/" + info.ID.String()
			// The width bound is what makes the minted size predictable to a
			// caller estimating it before the capability exists.
			if len(full) > maxInviteAddrBytes {
				continue
			}
			if routableInviteAddress(addr) {
				if len(routable) < maxInviteFallbackAddrsPeer {
					routable = append(routable, full)
				}
				continue
			}
			if fallback == "" && !multiaddrIsLoopback(addr) {
				fallback = full
			}
		}
		chosen := routable
		if len(chosen) == 0 {
			if fallback == "" {
				continue
			}
			chosen = []string{fallback}
			private++
		}
		for _, addr := range chosen {
			if addrs >= maxInviteFallbackAddrs || bytes+len(addr) > maxInviteFallbackBytes {
				break
			}
			addresses = append(addresses, addr)
			addrs++
			bytes += len(addr)
		}
		seen[info.ID] = struct{}{}
		peerIDs = append(peerIDs, info.ID.String())
		peers++
	}
	return addresses, peerIDs, private
}

// boundInviteRelays trims a relay set to what a capability can carry: each
// address at most maxInviteAddrBytes, the set at most maxInviteFallbackBytes.
// Count alone was not enough — MaxCapabilityRelays bounds how many, and eight
// webtransport relay addresses are over 1.7 KiB — so a caller estimating the
// minted size before the capability exists undershot by that whole margin.
// Both mint paths trim here, which is what makes the estimate an upper bound.
func boundInviteRelays(hints []string) []string {
	out := make([]string, 0, len(hints))
	total := 0
	for _, hint := range hints {
		if len(hint) > maxInviteAddrBytes || total+len(hint) > maxInviteFallbackBytes {
			continue
		}
		out = append(out, hint)
		total += len(hint)
	}
	return out
}

// multiaddrIsLoopback reports a literal loopback address, which is the one
// class never worth attaching: it names the newcomer's own machine, not a
// member.
func multiaddrIsLoopback(addr multiaddr.Multiaddr) bool {
	for _, code := range []int{multiaddr.P_IP4, multiaddr.P_IP6} {
		if value, err := addr.ValueForProtocol(code); err == nil {
			if ip := net.ParseIP(value); ip != nil {
				return ip.IsLoopback() || ip.IsUnspecified()
			}
		}
	}
	return false
}
