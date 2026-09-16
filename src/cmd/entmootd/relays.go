package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	libpeer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
)

// relayHintsFileName holds the relays this data root adopted from an invite.
// A new node has no way to discover a relay on its own: relays are named by
// their operator, never found. Carrying the inviter's relays in the invite and
// remembering them here is what lets a joiner stay reachable across restarts
// without the operator retyping them.
const relayHintsFileName = "relays.json"

// maxRelayHints bounds what one invite can install, so a leaked or hostile
// invite cannot fan a joiner out across an unbounded relay set.
const maxRelayHints = 8

type relayHintsFile struct {
	Version uint8    `json:"version"`
	Relays  []string `json:"relays"`
}

func relayHintsPath(dataDir string) string {
	return filepath.Join(dataDir, relayHintsFileName)
}

// loadRelayHints returns the adopted relay multiaddrs for this data root.
func loadRelayHints(dataDir string) ([]string, error) {
	data, err := os.ReadFile(relayHintsPath(dataDir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var stored relayHintsFile
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("decode relay hints: %w", err)
	}
	if stored.Version != 1 {
		return nil, fmt.Errorf("unsupported relay hints version %d", stored.Version)
	}
	return validateRelayHints(stored.Relays)
}

// saveRelayHints replaces the adopted relay set, merging with what is already
// stored so joining a second group does not drop the first group's relay.
func saveRelayHints(dataDir string, relays []string) error {
	existing, err := loadRelayHints(dataDir)
	if err != nil {
		// A corrupt or unreadable file must not block adopting a working
		// relay; it is a cache of operator-visible hints, not signed state.
		existing = nil
	}
	merged := make([]string, 0, len(existing)+len(relays))
	seen := make(map[string]struct{}, len(existing)+len(relays))
	for _, address := range append(existing, relays...) {
		if _, duplicate := seen[address]; duplicate {
			continue
		}
		seen[address] = struct{}{}
		merged = append(merged, address)
	}
	sort.Strings(merged)
	if len(merged) > maxRelayHints {
		merged = merged[:maxRelayHints]
	}
	data, err := json.Marshal(relayHintsFile{Version: 1, Relays: merged})
	if err != nil {
		return err
	}
	path := relayHintsPath(dataDir)
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// validateRelayHints keeps only well-formed relay multiaddrs that name a peer,
// bounded by maxRelayHints. Anything else is dropped rather than failing the
// caller: these are hints, and a joiner with one usable relay should proceed.
func validateRelayHints(relays []string) ([]string, error) {
	out := make([]string, 0, len(relays))
	seen := make(map[string]struct{}, len(relays))
	relayBytes := 0
	for _, raw := range relays {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			continue
		}
		info, err := libpeer.AddrInfoFromP2pAddr(address)
		if err != nil || info.ID == "" || len(info.Addrs) == 0 {
			continue
		}
		normalized := address.String()
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		// Width and total bound as well as count: a capability's size has to
		// be predictable to a caller estimating it before it exists.
		if len(normalized) > maxInviteAddrBytes || relayBytes+len(normalized) > maxInviteFallbackBytes {
			continue
		}
		relayBytes += len(normalized)
		seen[normalized] = struct{}{}
		out = append(out, normalized)
		if len(out) == maxRelayHints {
			break
		}
	}
	return out, nil
}
