package esphttp

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestNodeProfileHostnameNormalizeAndDisplayName(t *testing.T) {
	member := testMemberID(133053)
	if name, ok := NormalizeNodeProfileHostname("  hermes  "); !ok || name != "hermes" {
		t.Fatalf("normalized hostname: %q/%v", name, ok)
	}
	for _, name := range []string{"", "   ", "bad\nname", strings.Repeat("a", MaxNodeProfileHostnameBytes+1)} {
		if _, ok := NormalizeNodeProfileHostname(name); ok {
			t.Fatalf("accepted invalid hostname %q", name)
		}
	}
	if name := NodeDisplayName(member, " deimos "); name != "deimos#"+member.String() {
		t.Fatalf("display name: %q", name)
	}
	if name := NodeDisplayName(member, " "); name != "member-"+member.String() {
		t.Fatalf("fallback display name: %q", name)
	}
}

func TestStateStoresNodeProfilesScopeAndExpiry(t *testing.T) {
	ctx := context.Background()
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.close()
			member := testMobileNode(t, 133053)
			id, key := *member.MemberID, base64.StdEncoding.EncodeToString(member.EntmootPubKey)
			gid, other := testGroupID(1), testGroupID(2)
			put := func(rec NodeProfileRecord) {
				t.Helper()
				if _, _, err := tc.store.UpsertNodeProfile(ctx, rec); err != nil {
					t.Fatal(err)
				}
			}
			hostname := func(group entmoot.GroupID, publicKey string) string {
				t.Helper()
				rows, err := EnrichMemberDisplayNames(ctx, tc.store, group, []MemberSummary{{MemberID: id, EntmootPubKey: publicKey}})
				if err != nil {
					t.Fatal(err)
				}
				return rows[0].GlobalHostname
			}
			profile := func(host string, observedAtMS, expiresAtMS int64) NodeProfileRecord {
				return NodeProfileRecord{MemberID: id, EntmootPubKey: key, Hostname: host, Source: NodeProfileSourceMemberProfile, SourceGroupID: &gid, ObservedAtMS: observedAtMS, ExpiresAtMS: expiresAtMS}
			}
			put(profile("hermes", 20, 4_102_444_800_000))
			if got := hostname(gid, key); got != "hermes" {
				t.Fatalf("scoped profile: %q", got)
			}
			put(profile("older", 19, 4_102_444_800_000))
			put(profile("same-time", 20, 4_102_444_800_000))
			if got := hostname(gid, key); got != "hermes" {
				t.Fatalf("older/equal observation replaced profile: %q", got)
			}
			put(profile("deimos", 30, 4_102_444_800_000))
			if got := hostname(gid, key); got != "deimos" {
				t.Fatalf("newer observation ignored: %q", got)
			}
			if got := hostname(other, key); got != "" {
				t.Fatalf("profile crossed group: %q", got)
			}
			if got := hostname(gid, "another-key"); got != "" {
				t.Fatalf("profile crossed key binding: %q", got)
			}
			put(profile("expired", 31, 1))
			if got := hostname(gid, key); got != "" {
				t.Fatalf("expired profile still served: %q", got)
			}
		})
	}
}

func TestStateStoresNodeProfilesRejectUnknownSource(t *testing.T) {
	for _, tc := range openInviteStateStores(t) {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.close()
			id := testMemberID(1)
			if _, _, err := tc.store.UpsertNodeProfile(context.Background(), NodeProfileRecord{MemberID: id, Hostname: "unknown", Source: "unknown"}); err == nil {
				t.Fatal("accepted unknown profile source")
			}
			if _, found, err := tc.store.GetNodeProfile(context.Background(), id); err != nil || found {
				t.Fatalf("invalid profile persisted: found=%v err=%v", found, err)
			}
		})
	}
}
