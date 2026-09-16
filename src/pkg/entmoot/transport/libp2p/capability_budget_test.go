package libp2ptransport

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	entmoot "entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
)

// TestJoinRequestFitsTheFrame pins the reserve behind MaxCapabilityBytes. A
// joiner sends the capability inside a membership-sync READ request, which the
// server refuses over maxSyncRequestBytes; the join record travels separately
// on the push protocol, which decodes against a far larger limit. So the
// budget is correct exactly when a capability at the limit still leaves room
// for the rest of a read request, and a push carrying both fits its own limit.
//
// Guessing this produced two failures already: half the frame made a
// worst-case estimate refuse a single address, and measuring only the address
// list let invites be minted that no joiner could redeem.
func TestJoinRequestFitsTheFrame(t *testing.T) {
	identity := mustIdentity(t)
	_, group := mustInviteOnlyGroup(t, t.TempDir(), identity)
	record, err := group.SignRecord(identity, membership.Record{Kind: membership.KindJoin})
	if err != nil {
		t.Fatalf("SignRecord: %v", err)
	}

	// A capability filled to exactly the budget.
	capability := entmoot.BootstrapCapability{
		GroupID:     group.GroupID(),
		Founder:     group.Founder(),
		RosterHead:  group.Canonical().ID,
		IssuedAtMS:  time.Now().UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli(),
	}
	for {
		encoded, err := json.Marshal(capability)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		if len(encoded) >= MaxCapabilityBytes-300 {
			break
		}
		capability.AllowedMultiaddrs = append(capability.AllowedMultiaddrs,
			"/ip4/203.0.113.7/tcp/1004/p2p/"+strings.Repeat("Q", 52))
	}
	if size, tooLarge := CapabilityTooLarge(capability); tooLarge {
		t.Fatalf("the fixture overshot the budget: %d bytes", size)
	}

	read := MembershipSyncRequest{
		Version:        1,
		RequestID:      strings.Repeat("r", 36),
		GroupID:        group.GroupID(),
		Capability:     &capability,
		HaveSequence:   1 << 40,
		HaveCheckpoint: entmoot.RosterEntryID{3},
		AfterTimestamp: 1 << 40,
		AfterID:        entmoot.RosterEntryID{4},
		Limit:          maxMembershipRecords,
	}
	encoded, err := json.Marshal(read)
	if err != nil {
		t.Fatalf("Marshal read request: %v", err)
	}
	if len(encoded) > maxSyncRequestBytes {
		t.Fatalf("a capability at the budget makes a %d-byte read request, over the %d-byte frame: the reserve is too small",
			len(encoded), maxSyncRequestBytes)
	}

	push := MembershipPushRequest{
		Version:    1,
		RequestID:  strings.Repeat("r", 36),
		GroupID:    group.GroupID(),
		Capability: &capability,
		Record:     record,
	}
	encoded, err = json.Marshal(push)
	if err != nil {
		t.Fatalf("Marshal push request: %v", err)
	}
	if len(encoded) > maxMembershipResponse {
		t.Fatalf("a capability at the budget plus a join record makes a %d-byte push, over its %d-byte limit",
			len(encoded), maxMembershipResponse)
	}
}
