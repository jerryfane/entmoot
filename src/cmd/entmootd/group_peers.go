package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
)

const groupPeersFileName = "libp2p-peers.json"

type persistedGroupPeers struct {
	Version uint8           `json:"version"`
	Peers   []persistedPeer `json:"peers"`
}

type persistedPeer struct {
	PeerID     string   `json:"peer_id"`
	Multiaddrs []string `json:"multiaddrs"`
}

func groupPeersPath(dataDir string, groupID entmoot.GroupID) string {
	name := base64.RawURLEncoding.EncodeToString(groupID[:])
	return filepath.Join(dataDir, "groups", name, groupPeersFileName)
}

func loadGroupPeers(dataDir string, groupID entmoot.GroupID) ([]peer.AddrInfo, error) {
	path := groupPeersPath(dataDir, groupID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read group peers: %w", err)
	}
	var stored persistedGroupPeers
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("decode group peers: %w", err)
	}
	if stored.Version != 1 {
		return nil, fmt.Errorf("unsupported group peer cache version %d", stored.Version)
	}
	out := make([]peer.AddrInfo, 0, len(stored.Peers))
	for _, item := range stored.Peers {
		peerID, err := peer.Decode(item.PeerID)
		if err != nil {
			return nil, fmt.Errorf("decode cached peer id: %w", err)
		}
		info := peer.AddrInfo{ID: peerID}
		for _, raw := range item.Multiaddrs {
			address, err := multiaddr.NewMultiaddr(raw)
			if err != nil {
				return nil, fmt.Errorf("decode cached multiaddr: %w", err)
			}
			info.Addrs = append(info.Addrs, address)
		}
		out = append(out, info)
	}
	return out, nil
}

// persistGroupPeer records the peer's current address set, replacing whatever
// was cached before. Unioning would keep a member reachable at an address it
// abandoned after a NAT change, and the cache has no expiry of its own.
func persistGroupPeer(dataDir string, groupID entmoot.GroupID, info peer.AddrInfo) error {
	if info.ID == "" || len(info.Addrs) == 0 {
		return nil
	}
	peers, err := loadGroupPeers(dataDir, groupID)
	if err != nil {
		return err
	}
	byID := make(map[peer.ID]peer.AddrInfo, len(peers)+1)
	for _, current := range peers {
		byID[current.ID] = current
	}
	current := peer.AddrInfo{ID: info.ID}
	seen := make(map[string]bool, len(info.Addrs))
	for _, address := range info.Addrs {
		if !seen[address.String()] {
			current.Addrs = append(current.Addrs, address)
			seen[address.String()] = true
		}
	}
	byID[info.ID] = current

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id.String())
	}
	sort.Strings(ids)
	stored := persistedGroupPeers{Version: 1, Peers: make([]persistedPeer, 0, len(ids))}
	for _, rawID := range ids {
		id, _ := peer.Decode(rawID)
		addresses := byID[id].Addrs
		rawAddresses := make([]string, 0, len(addresses))
		for _, address := range addresses {
			rawAddresses = append(rawAddresses, address.String())
		}
		sort.Strings(rawAddresses)
		stored.Peers = append(stored.Peers, persistedPeer{PeerID: rawID, Multiaddrs: rawAddresses})
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	path := groupPeersPath(dataDir, groupID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}
