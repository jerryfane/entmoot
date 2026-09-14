package main

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	"entmoot/pkg/entmoot/roster"
)

func daemonTestGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = seed + byte(i)
	}
	return gid
}

// mustOpenGroup opens a group for direct inspection. Commands claim the writer
// lease, so a test must close its own handle before running one.
func mustOpenGroup(t *testing.T, dataDir string, gid entmoot.GroupID) *membership.Group {
	t.Helper()
	group, err := membership.Open(dataDir, gid)
	if err != nil {
		t.Fatalf("membership.Open: %v", err)
	}
	return group
}

func mustCloseGroup(t *testing.T, group *membership.Group) {
	t.Helper()
	if err := group.Close(); err != nil {
		t.Fatalf("membership close: %v", err)
	}
}

// runRosterCommand runs a roster subcommand and returns its exit code with the
// JSON object it printed, if any.
func runRosterCommand(t *testing.T, gf *globalFlags, args ...string) (int, map[string]any, string) {
	t.Helper()
	code, stdout, stderr := captureCommandOutput(t, func() int { return cmdRoster(gf, args) })
	var out map[string]any
	if stdout != "" {
		if err := json.Unmarshal([]byte(stdout), &out); err != nil {
			t.Fatalf("roster %v stdout is not JSON: %v\n%s", args, err, stdout)
		}
	}
	return code, out, stderr
}

// A ban must do more than remove: the removed key must not be able to walk
// back in with a fresh invite. Lifting the ban must let the same key back,
// which is the whole difference between `roster remove` and `roster ban`.
func TestRosterBanBlocksRejoinUntilUnbanned(t *testing.T) {
	dataDir := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	joiner, joinerInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x71)
	mustCreateGroup(t, dataDir, gid, founder, membership.DefaultPolicy())

	group := mustOpenGroup(t, dataDir, gid)
	mustJoinWithInvite(t, group, joiner, mustDaemonInvite(t, group, founder, joinerInfo, 4))
	mustCloseGroup(t, group)

	gf := daemonFlags(t, dataDir, founder)
	code, out, stderr := runRosterCommand(t, gf, "ban", "-group", gid.String(), "-member", joinerInfo.MemberID.String())
	if code != exitOK {
		t.Fatalf("roster ban code = %d (%s)", code, stderr)
	}
	if out["status"] != "banned" || out["banned"] != true {
		t.Fatalf("roster ban output = %v, want banned", out)
	}

	group = mustOpenGroup(t, dataDir, gid)
	if group.IsMemberID(*joinerInfo.MemberID) || !group.IsBanned(*joinerInfo.MemberID) {
		t.Fatal("banned member is still a member, or is not recorded as banned")
	}
	// A banned key can still sign a join and hand it to a peer; what must not
	// happen is that the group accepts it.
	invite := mustDaemonInvite(t, group, founder, joinerInfo, 4)
	rejoin, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin, Invite: &invite})
	if err != nil {
		t.Fatalf("rejoin record: %v", err)
	}
	if group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("a banned identity rejoined with a fresh invite")
	}
	mustCloseGroup(t, group)

	code, out, stderr = runRosterCommand(t, gf, "unban", "-group", gid.String(), "-member", joinerInfo.MemberID.String())
	if code != exitOK {
		t.Fatalf("roster unban code = %d (%s)", code, stderr)
	}
	if out["status"] != "unbanned" || out["banned"] != false {
		t.Fatalf("roster unban output = %v, want unbanned", out)
	}

	group = mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if group.IsBanned(*joinerInfo.MemberID) {
		t.Fatal("identity is still banned after unban")
	}
	// Lifting the ban does not retroactively admit the join it refused: a
	// record is judged at its own point in the order, so the joiner asks
	// again.
	if !group.HasRecord(rejoin.ID) {
		t.Fatal("the refused join record was dropped")
	}
	if group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("lifting the ban silently readmitted the refused join")
	}
	mustJoinWithInvite(t, group, joiner, mustDaemonInvite(t, group, founder, joinerInfo, 1))
}

// Removing a member closes one door and leaves another open: a bearer invite
// minted earlier still admits whoever holds it. The removal has to say so, or
// an operator believes the group is closed when it is not.
func TestRosterRemoveReportsOutstandingOpenInvites(t *testing.T) {
	dataDir := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	joiner, joinerInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x5c)
	mustCreateGroup(t, dataDir, gid, founder, membership.DefaultPolicy())

	group := mustOpenGroup(t, dataDir, gid)
	mustJoinWithInvite(t, group, joiner, mustDaemonInvite(t, group, founder, joinerInfo, 1))
	mustCloseGroup(t, group)

	gf := daemonFlags(t, dataDir, founder)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdInvite(gf, []string{"create", "-group", gid.String(), "-open",
			"-bootstrap", "/ip4/127.0.0.1/tcp/41998/p2p/" + founderInfo.PeerID})
	})
	if code != exitOK {
		t.Fatalf("invite create code = %d (%s)", code, stderr)
	}
	var bearer entmoot.BootstrapCapability
	if err := json.Unmarshal([]byte(stdout), &bearer); err != nil {
		t.Fatalf("invite create stdout: %v\n%s", err, stdout)
	}

	code, out, stderr := runRosterCommand(t, gf, "remove", "-group", gid.String(),
		"-member", joinerInfo.MemberID.String(), "-peer", joinerInfo.PeerID,
		"-pubkey", base64.StdEncoding.EncodeToString(joinerInfo.EntmootPubKey))
	if code != exitOK {
		t.Fatalf("roster remove code = %d (%s)", code, stderr)
	}
	if out["members"] != float64(1) {
		t.Fatalf("roster remove output = %v, want 1 remaining member", out)
	}
	outstanding, _ := out["outstanding_open_invites"].([]any)
	if len(outstanding) != 1 || outstanding[0] != base64.StdEncoding.EncodeToString(bearer.Nonce[:]) {
		t.Fatalf("outstanding open invites = %v, want the bearer invite nonce", out["outstanding_open_invites"])
	}
	if out["outstanding_esp_open_invites"] != float64(0) {
		t.Fatalf("esp open invites = %v, want 0", out["outstanding_esp_open_invites"])
	}

	group = mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("removed member is still in the group")
	}
	// A plain removal is not a ban: the same key may be invited back.
	if group.IsBanned(*joinerInfo.MemberID) {
		t.Fatal("roster remove banned the member")
	}
}

// Leaving is the one membership change a member makes about itself with no
// admin involved, which is the point of self-signed records.
func TestRosterLeaveRemovesTheCallerWithoutAnAdmin(t *testing.T) {
	dataDir := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	joiner, joinerInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x35)
	mustCreateGroup(t, dataDir, gid, founder, membership.DefaultPolicy())

	group := mustOpenGroup(t, dataDir, gid)
	mustJoinWithInvite(t, group, joiner, mustDaemonInvite(t, group, founder, joinerInfo, 1))
	mustCloseGroup(t, group)

	code, out, stderr := runRosterCommand(t, daemonFlags(t, dataDir, joiner), "leave", "-group", gid.String())
	if code != exitOK {
		t.Fatalf("roster leave code = %d (%s)", code, stderr)
	}
	if out["status"] != "left" || out["members"] != float64(1) {
		t.Fatalf("roster leave output = %v, want left with 1 member", out)
	}

	group = mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("member is still in the group after leaving")
	}
	if !group.IsMemberID(*founderInfo.MemberID) {
		t.Fatal("leaving removed the wrong member")
	}
	if group.IsBanned(*joinerInfo.MemberID) {
		t.Fatal("leaving banned the member")
	}

	// A stranger has nothing to leave, and must not be able to write a record
	// claiming otherwise.
	stranger, _ := mustDaemonIdentity(t)
	code, _, _ = runRosterCommand(t, daemonFlags(t, dataDir, stranger), "leave", "-group", gid.String())
	if code != exitNotMember {
		t.Fatalf("stranger leave code = %d, want %d", code, exitNotMember)
	}
}

// Checkpointing is how a group stops replaying its whole history to every new
// member: the records before the checkpoint are retired and the state they
// project is carried by one signed object.
func TestRosterCheckpointFoldsPendingRecordsAndStatusReportsIt(t *testing.T) {
	dataDir := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	first, firstInfo := mustDaemonIdentity(t)
	second, secondInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x28)
	policy := membership.DefaultPolicy()
	policy.CheckpointEvery = 64
	mustCreateGroup(t, dataDir, gid, founder, policy)

	group := mustOpenGroup(t, dataDir, gid)
	mustJoinWithInvite(t, group, first, mustDaemonInvite(t, group, founder, firstInfo, 1))
	mustJoinWithInvite(t, group, second, mustDaemonInvite(t, group, founder, secondInfo, 1))
	mustCloseGroup(t, group)

	gf := daemonFlags(t, dataDir, founder)
	code, before, stderr := runRosterCommand(t, gf, "status", "-group", gid.String())
	if code != exitOK {
		t.Fatalf("roster status code = %d (%s)", code, stderr)
	}
	if before["sequence"] != float64(0) || before["pending"] != float64(2) {
		t.Fatalf("status before checkpoint = %v, want sequence 0 with 2 pending", before)
	}

	code, signed, stderr := runRosterCommand(t, gf, "checkpoint", "-group", gid.String())
	if code != exitOK {
		t.Fatalf("roster checkpoint code = %d (%s)", code, stderr)
	}
	if signed["status"] != "signed" || signed["sequence"] != float64(1) ||
		signed["covered"] != float64(2) || signed["members"] != float64(3) {
		t.Fatalf("roster checkpoint output = %v, want sequence 1 covering 2 records over 3 members", signed)
	}

	code, after, stderr := runRosterCommand(t, gf, "status", "-group", gid.String())
	if code != exitOK {
		t.Fatalf("roster status code = %d (%s)", code, stderr)
	}
	if after["sequence"] != float64(1) || after["pending"] != float64(0) {
		t.Fatalf("status after checkpoint = %v, want sequence 1 with 0 pending", after)
	}
	if after["checkpoint"] == before["checkpoint"] {
		t.Fatalf("status still reports the old checkpoint %v", after["checkpoint"])
	}
	members, _ := after["members"].([]any)
	if len(members) != 3 {
		t.Fatalf("status members = %v, want 3", after["members"])
	}
}

// The admin set travels in a policy record that restates the whole policy, so
// the risk worth testing is a grant that quietly resets the join rule or the
// checkpoint cadence along with it.
func TestRosterAdminGrantAndRevokePreserveThePolicy(t *testing.T) {
	dataDir := t.TempDir()
	founder, _ := mustDaemonIdentity(t)
	admin, adminInfo := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x4a)
	policy := membership.DefaultPolicy()
	policy.CheckpointEvery = 9
	mustCreateGroup(t, dataDir, gid, founder, policy)

	group := mustOpenGroup(t, dataDir, gid)
	mustJoinWithInvite(t, group, admin, mustDaemonInvite(t, group, founder, adminInfo, 1))
	mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	mustCloseGroup(t, group)

	founderFlags := daemonFlags(t, dataDir, founder)
	code, out, stderr := runRosterCommand(t, founderFlags, "admin", "grant", "-group", gid.String(), "-member", adminInfo.MemberID.String())
	if code != exitOK {
		t.Fatalf("roster admin grant code = %d (%s)", code, stderr)
	}
	if out["status"] != "updated" {
		t.Fatalf("roster admin grant output = %v, want updated", out)
	}

	code, listed, stderr := runRosterCommand(t, founderFlags, "admin", "list", "-group", gid.String())
	if code != exitOK {
		t.Fatalf("roster admin list code = %d (%s)", code, stderr)
	}
	admins, _ := listed["admins"].([]any)
	if len(admins) != 1 || admins[0] != adminInfo.MemberID.String() {
		t.Fatalf("admins = %v, want %s", listed["admins"], adminInfo.MemberID)
	}

	group = mustOpenGroup(t, dataDir, gid)
	if got := group.Policy(); got.CheckpointEvery != 9 || got.JoinRule != membership.JoinRuleInvite {
		t.Fatalf("policy after grant = %+v, want checkpoint cadence 9 and the invite join rule", got)
	}
	mustCloseGroup(t, group)

	// A delegated admin evicts members; only the founder lifts a ban, because
	// an admin that could unban could undo the founder's decision.
	adminFlags := daemonFlags(t, dataDir, admin)
	code, _, stderr = runRosterCommand(t, adminFlags, "ban", "-group", gid.String(), "-member", memberInfo.MemberID.String())
	if code != exitOK {
		t.Fatalf("admin ban code = %d (%s)", code, stderr)
	}
	code, _, _ = runRosterCommand(t, adminFlags, "unban", "-group", gid.String(), "-member", memberInfo.MemberID.String())
	if code != exitNotMember {
		t.Fatalf("admin unban code = %d, want %d", code, exitNotMember)
	}

	code, out, stderr = runRosterCommand(t, founderFlags, "admin", "revoke", "-group", gid.String(), "-member", adminInfo.MemberID.String())
	if code != exitOK {
		t.Fatalf("roster admin revoke code = %d (%s)", code, stderr)
	}
	admins, _ = out["admins"].([]any)
	if out["status"] != "updated" || len(admins) != 0 {
		t.Fatalf("roster admin revoke output = %v, want updated with no admins", out)
	}
	group = mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if group.CanAdminister(*adminInfo.MemberID) {
		t.Fatal("revoked admin still administers the group")
	}
	if got := group.Policy(); got.CheckpointEvery != 9 || got.JoinRule != membership.JoinRuleInvite {
		t.Fatalf("policy after revoke = %+v, want checkpoint cadence 9 and the invite join rule", got)
	}
}

// Revoking must reach every node, not only the ledger of the node that issued
// the invite: without a signed record the invite still works anywhere else it
// is presented.
func TestInviteRevokeSignsARecordThatStopsTheInvite(t *testing.T) {
	dataDir := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	joiner, joinerInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x63)
	mustCreateGroup(t, dataDir, gid, founder, membership.DefaultPolicy())

	gf := daemonFlags(t, dataDir, founder)
	bootstrap := "/ip4/127.0.0.1/tcp/41999/p2p/" + founderInfo.PeerID
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdInvite(gf, []string{"create", "-group", gid.String(),
			"-target-pubkey", base64.StdEncoding.EncodeToString(joiner.PublicKey),
			"-bootstrap", bootstrap})
	})
	if code != exitOK {
		t.Fatalf("invite create code = %d (%s)", code, stderr)
	}
	var capability entmoot.BootstrapCapability
	if err := json.Unmarshal([]byte(stdout), &capability); err != nil {
		t.Fatalf("invite create stdout: %v\n%s", err, stdout)
	}

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdInvite(gf, []string{"revoke", "-group", gid.String(),
			"-nonce", base64.StdEncoding.EncodeToString(capability.Nonce[:])})
	})
	if code != exitOK {
		t.Fatalf("invite revoke code = %d (%s)", code, stderr)
	}
	var revoked map[string]any
	if err := json.Unmarshal([]byte(stdout), &revoked); err != nil {
		t.Fatalf("invite revoke stdout: %v\n%s", err, stdout)
	}
	if revoked["status"] != "revoked" || revoked["record_id"] == nil {
		t.Fatalf("invite revoke output = %v, want a revoked status with a record id", revoked)
	}

	group := mustOpenGroup(t, dataDir, gid)
	defer mustCloseGroup(t, group)
	if !group.IsInviteRevoked(capability.Nonce) {
		t.Fatal("group state does not report the invite as revoked")
	}
	if err := group.CheckInvite(capability, time.Now().UnixMilli()); err == nil {
		t.Fatal("a revoked invite still authorises its holder")
	}
	if _, err := group.SignRecord(joiner, membership.Record{Kind: membership.KindJoin, Invite: &capability}); err != nil {
		t.Fatalf("join record: %v", err)
	}
	if group.IsMemberID(*joinerInfo.MemberID) {
		t.Fatal("a revoked invite admitted a joiner")
	}

	// `invite list` reads uses and revocation from group state, so the ledger
	// row must be reported as revoked too.
	code, stdout, stderr = captureCommandOutput(t, func() int { return cmdInvite(gf, []string{"list"}) })
	if code != exitOK {
		t.Fatalf("invite list code = %d (%s)", code, stderr)
	}
	var listed []map[string]any
	if err := json.Unmarshal([]byte(stdout), &listed); err != nil {
		t.Fatalf("invite list stdout: %v\n%s", err, stdout)
	}
	if len(listed) != 1 || listed[0]["state"] != "revoked" || listed[0]["uses"] != float64(0) {
		t.Fatalf("invite list = %v, want one revoked invite with no uses", listed)
	}
}

// Upgrading is the migration every existing group goes through: the linear
// chain becomes checkpoint 0, and nothing about who is in the group or who may
// administer it may change in the process.
func TestMembershipUpgradeMintsCheckpointZeroFromTheChain(t *testing.T) {
	dataDir := t.TempDir()
	founder, founderInfo := mustDaemonIdentity(t)
	_, memberInfo := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0x1f)
	seedLegacyChain(t, dataDir, gid, founder, founderInfo, memberInfo)

	stranger, _ := mustDaemonIdentity(t)
	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdMembership(daemonFlags(t, dataDir, stranger), []string{"upgrade", "-group", gid.String()})
	})
	if code != exitNotMember {
		t.Fatalf("non-founder upgrade code = %d (%s), want %d", code, stderr, exitNotMember)
	}
	if membership.Exists(dataDir, gid) {
		t.Fatal("a non-founder minted checkpoint 0")
	}

	gf := daemonFlags(t, dataDir, founder)
	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdMembership(gf, []string{"upgrade", "-group", gid.String()})
	})
	if code != exitOK {
		t.Fatalf("membership upgrade code = %d (%s)", code, stderr)
	}
	var upgraded map[string]any
	if err := json.Unmarshal([]byte(stdout), &upgraded); err != nil {
		t.Fatalf("membership upgrade stdout: %v\n%s", err, stdout)
	}
	if upgraded["status"] != "upgraded" || upgraded["members"] != float64(2) {
		t.Fatalf("membership upgrade output = %v, want an upgrade covering 2 members", upgraded)
	}

	group := mustOpenGroup(t, dataDir, gid)
	canonical := group.Canonical()
	if canonical.Sequence != 0 || canonical.LegacyHead == nil {
		t.Fatalf("checkpoint = sequence %d legacy head %v, want sequence 0 naming the chain head", canonical.Sequence, canonical.LegacyHead)
	}
	if !group.IsMemberID(*founderInfo.MemberID) || !group.IsMemberID(*memberInfo.MemberID) {
		t.Fatalf("members after upgrade = %v, want the founder and the chain member", group.MemberIDs())
	}
	admins := group.Admins()
	if len(admins) != 1 || admins[0] != *memberInfo.MemberID {
		t.Fatalf("admins after upgrade = %v, want the chain's delegated admin %s", admins, memberInfo.MemberID)
	}
	if !group.CanAdminister(*memberInfo.MemberID) {
		t.Fatal("the chain's delegated admin lost its authority")
	}
	// The chain stays readable, because messages still cite its entries.
	legacy := group.Legacy()
	if legacy == nil || *canonical.LegacyHead != legacy.Head() {
		t.Fatal("checkpoint 0 does not name the chain head it replaced")
	}
	mustCloseGroup(t, group)

	code, stdout, stderr = captureCommandOutput(t, func() int {
		return cmdMembership(gf, []string{"upgrade", "-group", gid.String()})
	})
	if code != exitOK {
		t.Fatalf("second upgrade code = %d (%s)", code, stderr)
	}
	var again map[string]any
	if err := json.Unmarshal([]byte(stdout), &again); err != nil {
		t.Fatalf("second upgrade stdout: %v\n%s", err, stdout)
	}
	if again["status"] != "already_upgraded" || again["checkpoint"] != upgraded["checkpoint"] {
		t.Fatalf("second upgrade output = %v, want already_upgraded naming checkpoint %v", again, upgraded["checkpoint"])
	}
}

// seedLegacyChain writes the pre-checkpoint chain a group had before this
// release: a founder genesis, one added member, and that member delegated as
// an admin.
func seedLegacyChain(t *testing.T, dataDir string, gid entmoot.GroupID, founder *keystore.Identity, founderInfo, memberInfo entmoot.NodeInfo) {
	t.Helper()
	chain, err := roster.OpenJSONL(dataDir, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL: %v", err)
	}
	defer func() {
		if err := chain.Close(); err != nil {
			t.Fatalf("roster close: %v", err)
		}
	}()
	if err := chain.Genesis(founder, founderInfo, 1_700_000_000_000); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	add, err := chain.SignEntry(founder, "add", memberInfo, nil, 1_700_000_001_000)
	if err != nil {
		t.Fatalf("SignEntry add: %v", err)
	}
	if err := chain.Apply(add); err != nil {
		t.Fatalf("Apply add: %v", err)
	}
	policy, err := roster.MarshalAdminPolicy([]entmoot.MemberID{*memberInfo.MemberID})
	if err != nil {
		t.Fatalf("MarshalAdminPolicy: %v", err)
	}
	grant, err := chain.SignEntry(founder, "policy_change", entmoot.NodeInfo{}, policy, 1_700_000_002_000)
	if err != nil {
		t.Fatalf("SignEntry policy_change: %v", err)
	}
	if err := chain.Apply(grant); err != nil {
		t.Fatalf("Apply policy_change: %v", err)
	}
}
