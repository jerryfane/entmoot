package membership

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

// legacyAdminPolicyType is the discriminator the linear roster used for an
// admin-set policy entry.
const legacyAdminPolicyType = "admins/v1"

type legacyAdminPolicy struct {
	Type   string             `json:"type"`
	Admins []entmoot.MemberID `json:"admins"`
}

// LegacyChain is a read-only view of the linear roster chain a group used
// before its first checkpoint.
//
// It deliberately does not re-check authority or signatures: these entries are
// this node's own store, and they were validated when they were accepted. What
// it does check is that the stored bytes still match what they claim to be and
// that the chain links up, which is what catches a corrupted or tampered file.
// Two things need it: minting checkpoint 0 during the upgrade, and verifying a
// historical message that cites a chain entry rather than a checkpoint.
type LegacyChain struct {
	groupID entmoot.GroupID
	entries []entmoot.RosterEntry
	byID    map[entmoot.RosterEntryID]int
	founder entmoot.NodeInfo
	members map[entmoot.MemberID]entmoot.NodeInfo
	admins  []entmoot.MemberID
	head    entmoot.RosterEntryID
	// legacyRoot and legacyLeaves carry the identity-upgrade commitment a v0
	// history proof is checked against, when the chain has one.
	legacyRoot    [32]byte
	legacyHasRoot bool
	legacyLeaves  int
}

// LoadLegacyChain reads a group's linear roster chain, if one is on disk.
func LoadLegacyChain(root string, groupID entmoot.GroupID) (*LegacyChain, error) {
	dir, err := groupDir(root, groupID)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, legacyDBName)
	if info, statErr := os.Stat(path); statErr != nil || info.IsDir() {
		return nil, fmt.Errorf("%w: no legacy chain for %s", ErrLegacyOnly, groupID.String())
	}
	q := url.Values{}
	q.Set("mode", "ro")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("membership: open legacy chain: %w", err)
	}
	defer db.Close()
	entries, err := readLegacyEntries(context.Background(), db, groupID)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: legacy chain for %s is empty", ErrLegacyOnly, groupID.String())
	}
	return newLegacyChain(groupID, entries)
}

func readLegacyEntries(ctx context.Context, db *sql.DB, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT canonical_bytes FROM roster_entries WHERE group_id = ? ORDER BY sequence;`, groupID[:])
	if err != nil {
		return nil, fmt.Errorf("membership: read legacy entries: %w", err)
	}
	defer rows.Close()
	var out []entmoot.RosterEntry
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("membership: scan legacy entry: %w", err)
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("membership: decode legacy entry: %w", err)
		}
		encoded, err := canonical.Encode(entry)
		if err != nil || !bytes.Equal(encoded, raw) {
			return nil, errors.New("membership: legacy entry bytes are corrupt")
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("membership: iterate legacy entries: %w", err)
	}
	return out, nil
}

func newLegacyChain(groupID entmoot.GroupID, entries []entmoot.RosterEntry) (*LegacyChain, error) {
	chain := &LegacyChain{
		groupID: groupID,
		entries: entries,
		byID:    make(map[entmoot.RosterEntryID]int, len(entries)),
		members: make(map[entmoot.MemberID]entmoot.NodeInfo, len(entries)),
	}
	for i, entry := range entries {
		if canonical.RosterEntryID(entry) != entry.ID {
			return nil, fmt.Errorf("membership: legacy entry %d id does not match its contents", i)
		}
		if i == 0 {
			if len(entry.Parents) != 0 {
				return nil, errors.New("membership: legacy genesis entry names a parent")
			}
			chain.founder = entry.Subject
		} else if len(entry.Parents) != 1 || entry.Parents[0] != entries[i-1].ID {
			return nil, fmt.Errorf("membership: legacy entry %d does not follow its predecessor", i)
		}
		chain.byID[entry.ID] = i
		chain.applyEntry(entry)
	}
	chain.head = entries[len(entries)-1].ID
	return chain, nil
}

func (c *LegacyChain) applyEntry(entry entmoot.RosterEntry) {
	switch entry.Op {
	case "add":
		if id, err := entmoot.ResolvedMemberID(entry.Subject); err == nil {
			c.members[id] = cloneNodeInfo(entry.Subject)
		}
	case "remove":
		if id, err := entmoot.ResolvedMemberID(entry.Subject); err == nil {
			delete(c.members, id)
			c.admins = removeMemberID(c.admins, id)
		}
	case "policy_change":
		if policy, ok := parseLegacyAdminPolicy(entry.Policy); ok {
			c.admins = SortAdmins(policy.Admins)
			return
		}
		if root, leaves, ok := parseLegacyUpgradePolicy(entry.Policy); ok {
			c.legacyRoot, c.legacyLeaves, c.legacyHasRoot = root, leaves, true
		}
	}
}

func parseLegacyAdminPolicy(payload []byte) (legacyAdminPolicy, bool) {
	if len(payload) == 0 {
		return legacyAdminPolicy{}, false
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var policy legacyAdminPolicy
	if err := decoder.Decode(&policy); err != nil || policy.Type != legacyAdminPolicyType {
		return legacyAdminPolicy{}, false
	}
	return policy, true
}

// parseLegacyUpgradePolicy reads the founder-signed identity-upgrade
// commitment a v0 history proof is verified against. The type tag and field
// names are the ones the linear roster wrote, so they cannot be renamed.
func parseLegacyUpgradePolicy(payload []byte) ([32]byte, int, bool) {
	var root [32]byte
	if len(payload) == 0 {
		return root, 0, false
	}
	var marker struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &marker); err != nil || marker.Type != "legacy_identity_upgrade" {
		return root, 0, false
	}
	var policy entmoot.LegacyIdentityUpgradePolicy
	if err := json.Unmarshal(payload, &policy); err != nil {
		return root, 0, false
	}
	decoded, err := hex.DecodeString(policy.LegacyHistoryRoot)
	if err != nil || len(decoded) != len(root) || policy.LegacyHistoryCount < 0 {
		return root, 0, false
	}
	copy(root[:], decoded)
	return root, policy.LegacyHistoryCount, true
}

func removeMemberID(ids []entmoot.MemberID, id entmoot.MemberID) []entmoot.MemberID {
	out := make([]entmoot.MemberID, 0, len(ids))
	for _, candidate := range ids {
		if candidate == id {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

// Head is the last entry of the chain, which checkpoint 0 records so a node
// can tell a genuine upgrade from a fabricated one.
func (c *LegacyChain) Head() entmoot.RosterEntryID { return c.head }

// Founder is the identity the chain's genesis entry named.
func (c *LegacyChain) Founder() entmoot.NodeInfo { return cloneNodeInfo(c.founder) }

// Members is the membership the chain projects to, sorted by member id.
func (c *LegacyChain) Members() []entmoot.NodeInfo {
	ids := make([]entmoot.MemberID, 0, len(c.members))
	for id := range c.members {
		ids = append(ids, id)
	}
	sortMemberIDs(ids)
	out := make([]entmoot.NodeInfo, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneNodeInfo(c.members[id]))
	}
	return out
}

// Admins is the delegated-admin set at the head of the chain.
func (c *LegacyChain) Admins() []entmoot.MemberID {
	return append([]entmoot.MemberID(nil), c.admins...)
}

// Entries exposes the chain for the v0 history paths that scan it.
func (c *LegacyChain) Entries() []entmoot.RosterEntry {
	return append([]entmoot.RosterEntry(nil), c.entries...)
}

// HasEntry reports whether an id names an entry of this chain.
func (c *LegacyChain) HasEntry(id entmoot.RosterEntryID) bool {
	_, ok := c.byID[id]
	return ok
}

// MemberAt answers whether a member was in the group at a chain entry, which
// is what a historical message citing that entry needs.
func (c *LegacyChain) MemberAt(id entmoot.MemberID, entry entmoot.RosterEntryID) (entmoot.NodeInfo, bool, bool) {
	index, known := c.byID[entry]
	if !known {
		return entmoot.NodeInfo{}, false, false
	}
	for i := index; i >= 0; i-- {
		candidate := c.entries[i]
		candidateID, err := entmoot.ResolvedMemberID(candidate.Subject)
		if err != nil || candidateID != id {
			continue
		}
		switch candidate.Op {
		case "remove":
			return entmoot.NodeInfo{}, false, true
		case "add":
			return cloneNodeInfo(candidate.Subject), true, true
		}
	}
	return entmoot.NodeInfo{}, false, true
}

// LegacyHistoryCommitment returns the identity-upgrade commitment a v0
// history proof is verified against, and whether the chain carries one.
func (c *LegacyChain) LegacyHistoryCommitment() ([32]byte, int, bool) {
	return c.legacyRoot, c.legacyLeaves, c.legacyHasRoot
}

func sortMemberIDs(ids []entmoot.MemberID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && bytes.Compare(ids[j-1][:], ids[j][:]) > 0; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
