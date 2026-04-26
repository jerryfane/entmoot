package gossip

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/wire"
)

// ErrJoinFailed is returned by Join when every bootstrap strategy has been
// exhausted without producing a synced roster. Callers decide whether to
// retry (ARCHITECTURE §5 calls for a 30s→10m exponential backoff).
var ErrJoinFailed = errors.New("gossip: join failed: all bootstrap candidates unreachable")

// Join executes the three-stage bootstrap described in ARCHITECTURE §5:
//
//  1. Verify invite.Signature against invite.Issuer.EntmootPubKey. Return an
//     error wrapping entmoot.ErrSigInvalid on mismatch; no dial is attempted.
//  2. If invite.ValidUntil is set (non-zero) and has elapsed relative to
//     the gossiper's configured clock, return an error wrapping
//     entmoot.ErrInviteExpired. This
//     check runs AFTER signature verification so a tampered ValidUntil
//     cannot be evaluated before its signature is trusted. ValidUntil == 0
//     is treated as "no expiry asserted" for backwards compatibility with
//     pre-v1 invite bundles and test fixtures.
//  3. Try invite.BootstrapPeers in listed order. First peer that answers
//     RosterReq with applicable entries wins.
//  4. Fallback: Transport.TrustedPeers() ∩ invite.BootstrapPeers.NodeID.
//  5. Last resort: dial invite.Founder.PilotNodeID (guaranteed in the
//     roster because it was the genesis signer).
//
// On success, entries returned by the peer's RosterResp are applied to
// g.cfg.Roster in order. Join does NOT start the accept loop — callers run
// Start themselves after Join returns nil so they can own the accept-loop
// context lifetime.
//
// Roster seeding flow:
//
//   - If the local Roster is empty, the first entry in the peer's RosterResp
//     is treated as a self-signed genesis and seeded via
//     roster.AcceptGenesis. This verifies the entry's signature against the
//     pubkey declared in its Subject, so a joiner without the founder's
//     private key can still bootstrap from an empty state. Any remaining
//     entries are then applied in order.
//   - If the local Roster already has a genesis (e.g., the joiner IS the
//     founder, or pre-seeded out-of-band), AcceptGenesis is skipped and
//     entries at or before the current head are filtered out so Join is
//     idempotent under retry.
//
// Apply requires Parents to reference the current head, so any entry already
// at or before head is skipped and only strictly-newer entries are appended.
func (g *Gossiper) Join(ctx context.Context, invite *entmoot.Invite) error {
	if invite == nil {
		return errors.New("gossip: Join: invite is nil")
	}
	if invite.GroupID != g.cfg.GroupID {
		return fmt.Errorf("gossip: Join: invite is for group %s, want %s",
			invite.GroupID, g.cfg.GroupID)
	}
	if err := verifyInvite(invite); err != nil {
		return err
	}

	// Expiry check runs after signature verification so a tampered
	// ValidUntil can never short-circuit verification. ValidUntil == 0
	// means "no expiry asserted" (pre-v1 bundles and test fixtures); any
	// strictly-positive ValidUntil that has elapsed rejects the invite.
	if invite.ValidUntil > 0 {
		now := g.clk.Now().UnixMilli()
		if now > invite.ValidUntil {
			return fmt.Errorf("%w: valid_until=%d now=%d",
				entmoot.ErrInviteExpired, invite.ValidUntil, now)
		}
	}

	// Early-return for the "already synced" case. The founder's own
	// roster already contains the invite's advertised head (by
	// construction: the founder issued the invite), so there is nothing
	// to fetch. Without this shortcut, the founder would try to dial
	// every BootstrapPeer — including peers that have not started yet —
	// and fail ErrJoinFailed. Any re-invite on a node that is already a
	// fully-synced member hits the same path.
	if g.cfg.Roster.Head() == invite.RosterHead &&
		g.cfg.Roster.IsMember(g.cfg.LocalNode) {
		return nil
	}

	// v1.2.0: pre-seed the Pilot transport with invite-embedded
	// endpoints so the first Join dial can use TCP fallback if UDP
	// is blocked. Invites are signed by the founder, and
	// verifyInvite above has already verified that signature, so
	// endpoints carried here are authenticated end-to-end. A bad
	// SetPeerEndpoints (pilot restarting, IPC hiccup) is
	// non-fatal — we log at Debug and continue, falling through to
	// the same Dial path we had in v1.1.x.
	for _, bp := range invite.BootstrapPeers {
		if len(bp.Endpoints) == 0 {
			continue
		}
		if bp.NodeID == g.cfg.LocalNode {
			continue
		}
		if err := g.cfg.Transport.SetPeerEndpoints(ctx, bp.NodeID, bp.Endpoints); err != nil {
			g.logger.Debug("gossip: set invite endpoints",
				slog.Uint64("peer", uint64(bp.NodeID)),
				slog.String("err", err.Error()))
		}
	}

	// Strategy 1: invite.BootstrapPeers in listed order.
	for _, bp := range invite.BootstrapPeers {
		if bp.NodeID == g.cfg.LocalNode {
			continue
		}
		if err := g.tryRosterSync(ctx, bp.NodeID); err == nil {
			// v1.2.0: snapshot the bootstrap peer's current
			// transport-ad table. This populates Pilot's peerTCP for
			// every group member (not just our bootstrap peer) so
			// TCP fallback is available for the very first dial of
			// any peer post-Join, not just after the next
			// anti-entropy cycle. Failures are non-fatal — the
			// ongoing gossip path will catch up.
			if err := g.pullTransportSnapshot(ctx, bp.NodeID); err != nil {
				g.logger.Debug("gossip: transport snapshot",
					slog.Uint64("peer", uint64(bp.NodeID)),
					slog.String("err", err.Error()))
			}
			return nil
		} else {
			g.logger.Warn("gossip: bootstrap peer failed",
				slog.Uint64("peer", uint64(bp.NodeID)),
				slog.String("err", err.Error()))
		}
	}

	// Strategy 2: Transport.TrustedPeers() ∩ invite.BootstrapPeers.NodeID.
	trusted, err := g.cfg.Transport.TrustedPeers(ctx)
	if err != nil {
		g.logger.Warn("gossip: trusted peers", slog.String("err", err.Error()))
	} else {
		bootstrapSet := make(map[entmoot.NodeID]struct{}, len(invite.BootstrapPeers))
		for _, bp := range invite.BootstrapPeers {
			bootstrapSet[bp.NodeID] = struct{}{}
		}
		for _, peer := range trusted {
			if peer == g.cfg.LocalNode {
				continue
			}
			if _, ok := bootstrapSet[peer]; !ok {
				continue
			}
			if err := g.tryRosterSync(ctx, peer); err == nil {
				if err := g.pullTransportSnapshot(ctx, peer); err != nil {
					g.logger.Debug("gossip: transport snapshot",
						slog.Uint64("peer", uint64(peer)),
						slog.String("err", err.Error()))
				}
				return nil
			} else {
				g.logger.Warn("gossip: trusted-intersect peer failed",
					slog.Uint64("peer", uint64(peer)),
					slog.String("err", err.Error()))
			}
		}
	}

	// Strategy 3: founder fallback. Skip if the founder is us.
	if invite.Founder.PilotNodeID != 0 && invite.Founder.PilotNodeID != g.cfg.LocalNode {
		if err := g.tryRosterSync(ctx, invite.Founder.PilotNodeID); err == nil {
			if err := g.pullTransportSnapshot(ctx, invite.Founder.PilotNodeID); err != nil {
				g.logger.Debug("gossip: transport snapshot",
					slog.Uint64("peer", uint64(invite.Founder.PilotNodeID)),
					slog.String("err", err.Error()))
			}
			return nil
		} else {
			g.logger.Warn("gossip: founder fallback failed",
				slog.Uint64("peer", uint64(invite.Founder.PilotNodeID)),
				slog.String("err", err.Error()))
		}
	}

	return ErrJoinFailed
}

// tryRosterSync dials peer, sends RosterReq, reads RosterResp, and applies
// entries to g.cfg.Roster in order.
func (g *Gossiper) tryRosterSync(ctx context.Context, peer entmoot.NodeID) error {
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	req := &wire.RosterReq{GroupID: g.cfg.GroupID}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		return fmt.Errorf("write roster_req: %w", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		return fmt.Errorf("read roster_resp: %w", err)
	}
	resp, ok := payload.(*wire.RosterResp)
	if !ok {
		return fmt.Errorf("roster sync: unexpected response type")
	}
	if resp.GroupID != g.cfg.GroupID {
		return fmt.Errorf("roster sync: wrong group in response")
	}
	if len(resp.Entries) == 0 {
		return fmt.Errorf("roster sync: empty entries")
	}

	return g.applyEntries(resp.Entries)
}

// applyEntries applies roster entries from a RosterResp to the local log.
// If the local log is empty, the first entry is treated as a self-signed
// genesis and seeded via roster.AcceptGenesis. Entries already at or before
// the current head are skipped so Join is idempotent under retry.
func (g *Gossiper) applyEntries(entries []entmoot.RosterEntry) error {
	var zero entmoot.RosterEntryID

	// Empty-log path: seed genesis from the first entry, then fall through
	// to the normal "skip-up-to-head" logic to apply any remaining entries.
	if g.cfg.Roster.Head() == zero {
		if err := g.cfg.Roster.AcceptGenesis(entries[0]); err != nil {
			return fmt.Errorf("roster sync: accept genesis: %w", err)
		}
	}

	// Find the index of the current head in the response's entry list. Every
	// entry AFTER head is new and must be applied in order. Entries up to
	// and including head are skipped.
	head := g.cfg.Roster.Head()
	startIdx := -1
	for i, e := range entries {
		if e.ID == head {
			startIdx = i
			break
		}
	}
	if startIdx < 0 {
		// Response does not contain our head — maybe a fork, maybe the peer
		// hasn't seen us yet. v0 cannot reconcile forks (founder-only admin
		// precludes them), so treat this as a soft error and log; we apply
		// nothing.
		return fmt.Errorf("roster sync: response does not include local head %s", head)
	}
	for i := startIdx + 1; i < len(entries); i++ {
		if err := g.cfg.Roster.Apply(entries[i]); err != nil {
			return fmt.Errorf("roster sync: apply entry %s: %w", entries[i].ID, err)
		}
	}
	return nil
}

// pullTransportSnapshot fetches the current unexpired TransportAd table
// from peer and feeds each entry through onTransportAd, which performs
// full validation (signature, allowlist, seq LWW, size) and installs the
// endpoints into Pilot's peerTCP map via SetPeerEndpoints. Used at
// Join time to warm up the newcomer's peerTCP for every group member
// the bootstrap peer has seen an ad for, not just the bootstrap peer
// itself. Errors are returned so the caller can log at Debug and move
// on — the ongoing gossip path picks up any missing ads within one
// advertiser refresh cycle. (v1.2.0)
func (g *Gossiper) pullTransportSnapshot(ctx context.Context, peer entmoot.NodeID) error {
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	req := &wire.TransportSnapshotReq{GroupID: g.cfg.GroupID}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		return fmt.Errorf("write transport_snapshot_req: %w", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		return fmt.Errorf("read transport_snapshot_resp: %w", err)
	}
	resp, ok := payload.(*wire.TransportSnapshotResp)
	if !ok {
		return fmt.Errorf("transport snapshot: unexpected response type %T", payload)
	}
	if resp.GroupID != g.cfg.GroupID {
		return fmt.Errorf("transport snapshot: group_id mismatch")
	}
	// onTransportAd performs full validation (schema, size, trusted
	// forwarder, membership, signature, LWW seq check) before installing.
	// The `remote` argument is the snapshot peer that delivered the table;
	// authenticity of each ad comes from the author's signature.
	for i := range resp.Ads {
		ad := &resp.Ads[i]
		g.onTransportAd(ctx, peer, ad)
	}
	return nil
}

// verifyInvite checks invite.Signature against invite.Issuer.EntmootPubKey
// over the canonical encoding of the invite with Signature zeroed. Returns
// an error wrapping entmoot.ErrSigInvalid on mismatch.
func verifyInvite(invite *entmoot.Invite) error {
	if len(invite.Issuer.EntmootPubKey) == 0 {
		return fmt.Errorf("%w: invite issuer has no pubkey", entmoot.ErrSigInvalid)
	}
	signing := *invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("gossip: canonical encode invite: %w", err)
	}
	if !keystore.Verify(invite.Issuer.EntmootPubKey, sigInput, invite.Signature) {
		return fmt.Errorf("%w: invite signature does not verify", entmoot.ErrSigInvalid)
	}
	return nil
}
