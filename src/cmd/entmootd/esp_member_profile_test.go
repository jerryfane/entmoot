package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// A member with no daemon names itself through the ESP: `esp connect`, then
// `esp profile set`. The founder daemon stores the signed profile for history,
// records it as the member's display name, and the ESP members listing (what
// the website shows) returns it. `esp profile clear` withdraws it the same way.
// Plain `profile set` on that machine refuses and points at the ESP path.
func TestESPMemberProfileSetNamesMemberInESPListing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	gid := daemonTestGroupID(0x6a)
	memberB, memberBInfo := mustDaemonIdentity(t)
	memberC, _ := mustDaemonIdentity(t)
	node := startESPHistoryMoot(t, ctx, gid, nil, memberC, memberB).node
	flagsB := node.connectESPMember(t, memberB)

	code, _, stderr := captureCommandOutput(t, func() int {
		return cmdProfile(flagsB, []string{"set", "-group", gid.String(), "-name", "Quickbeam"})
	})
	if code != exitControlUnavail || !strings.Contains(stderr, "esp profile set") {
		t.Fatalf("profile set without a daemon: exit = %d stderr=%s, want control unavailable pointing at esp profile set", code, stderr)
	}

	set := espProfileCLI(t, flagsB, "set", "-group", gid.String(), "-name", "  Quickbeam  ")
	if set["delivery"] != string(libp2ptransport.DeliveryPendingHistory) || set["display_name"] != "Quickbeam" {
		t.Fatalf("esp profile set = %v, want pending_history delivery of Quickbeam", set)
	}
	named := espListedMember(t, ctx, flagsB, gid, *memberBInfo.MemberID)
	if named.GlobalHostname != "Quickbeam" || named.DisplayName != esphttp.NodeDisplayName(named.MemberID, "Quickbeam") {
		t.Fatalf("ESP members listing = %+v, want B named Quickbeam", named)
	}

	// A clear issued in the same millisecond would tie with the set.
	time.Sleep(5 * time.Millisecond)
	cleared := espProfileCLI(t, flagsB, "clear", "-group", gid.String())
	if cleared["cleared"] != true {
		t.Fatalf("esp profile clear = %v, want cleared", cleared)
	}
	unnamed := espListedMember(t, ctx, flagsB, gid, *memberBInfo.MemberID)
	if unnamed.GlobalHostname != "" || unnamed.DisplayName != esphttp.NodeDisplayName(unnamed.MemberID, "") {
		t.Fatalf("ESP members listing after clear = %+v, want B unnamed", unnamed)
	}
}

// espProfileCLI runs `entmootd esp profile <args>` and decodes its output.
func espProfileCLI(t *testing.T, flags *globalFlags, args ...string) map[string]any {
	t.Helper()
	code, stdout, stderr := captureCommandOutput(t, func() int { return cmdESP(flags, append([]string{"profile"}, args...)) })
	if code != exitOK {
		t.Fatalf("esp profile %v exit = %d stdout=%s stderr=%s", args, code, stdout, stderr)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("decode esp profile output %q: %v", stdout, err)
	}
	return out
}

// espListedMember reads the ESP members listing as the connected member and
// returns memberID's entry; `esp profile show` must print the same name.
func espListedMember(t *testing.T, ctx context.Context, flags *globalFlags, gid entmoot.GroupID, memberID entmoot.MemberID) esphttp.MemberSummary {
	t.Helper()
	client, err := openESPClient(flags)
	if err != nil {
		t.Fatal(err)
	}
	var listing struct {
		Members []esphttp.MemberSummary `json:"members"`
	}
	if err := client.do(ctx, http.MethodGet, espGroupPath(gid, "members"), nil, &listing); err != nil {
		t.Fatalf("list members: %v", err)
	}
	var listed esphttp.MemberSummary
	found := false
	for _, m := range listing.Members {
		if m.MemberID == memberID {
			listed, found = m, true
		}
	}
	if !found {
		t.Fatalf("member %s missing from ESP listing %+v", memberID, listing.Members)
	}

	shown := espProfileCLI(t, flags, "show", "-group", gid.String())
	rows, _ := shown["members"].([]any)
	for _, row := range rows {
		r, _ := row.(map[string]any)
		if r["member_id"] == memberID.String() {
			if r["display_name"] != listed.DisplayName {
				t.Fatalf("esp profile show display_name = %v, listing has %q", r["display_name"], listed.DisplayName)
			}
			return listed
		}
	}
	t.Fatalf("member %s missing from esp profile show %v", memberID, shown)
	return esphttp.MemberSummary{}
}
