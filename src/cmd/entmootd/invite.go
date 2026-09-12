package main

import (
	"bytes"
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

func cmdInvite(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "invite: missing op (want: create)")
		return exitInvalidArgument
	}
	if args[0] != "create" {
		fmt.Fprintf(os.Stderr, "invite: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
	return cmdInviteCreate(gf, args[1:])
}

// cmdInviteCreate emits a target-bound capability. Bootstrap addresses must be
// full multiaddrs ending in /p2p/<founder-peer-id>.
func cmdInviteCreate(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("invite create", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	targetKey := fs.String("target-pubkey", "", "base64 Ed25519 public key of the joining identity (required)")
	validFor := fs.String("valid-for", "24h", "capability TTL (time.ParseDuration or <N>d)")
	var bootstrap stringListFlag
	fs.Var(&bootstrap, "bootstrap", "founder libp2p multiaddr ending in /p2p/<peer-id>; repeatable")
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
	publicKey, err := base64.StdEncoding.DecodeString(*targetKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: -target-pubkey: %v\n", err)
		return exitInvalidArgument
	}
	target, err := libp2ptransport.BindingFromPublicKey(publicKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invite create: -target-pubkey: %v\n", err)
		return exitInvalidArgument
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
	if err != nil || founderBinding.MemberID != mustMemberID(s.identity.PublicKey) || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		fmt.Fprintln(os.Stderr, "invite create: local identity is not the group founder")
		return exitNotMember
	}
	founder.MemberID = &founderBinding.MemberID
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
		if err != nil || info.ID != founderBinding.PeerID {
			fmt.Fprintf(os.Stderr, "invite create: bootstrap must end in founder peer id %s\n", founderBinding.PeerID)
			return exitInvalidArgument
		}
		allowedAddresses = append(allowedAddresses, address.String())
		if _, ok := seen[info.ID]; !ok {
			seen[info.ID] = struct{}{}
			allowedPeerIDs = append(allowedPeerIDs, info.ID.String())
		}
	}
	now := time.Now()
	capability := entmoot.BootstrapCapability{
		GroupID:           gid,
		TargetPublicKey:   append([]byte(nil), publicKey...),
		TargetMemberID:    target.MemberID,
		TargetPeerID:      target.PeerID.String(),
		Founder:           founder,
		RosterHead:        rlog.Head(),
		AllowedPeerIDs:    allowedPeerIDs,
		AllowedMultiaddrs: allowedAddresses,
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
	encoded, err := json.MarshalIndent(capability, "", "  ")
	if err != nil {
		slog.Error("invite create: marshal", slog.String("err", err.Error()))
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
