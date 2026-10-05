package membership

import (
	"crypto/rand"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
)

// benchmarkRecords builds a window of about n records over a fresh group:
// joins, then removals and leaves of some of the members.
func benchmarkRecords(b *testing.B, n int) (Checkpoint, []Record) {
	b.Helper()
	founder, err := keystore.Generate()
	if err != nil {
		b.Fatal(err)
	}
	founderInfo, err := identityInfo(founder)
	if err != nil {
		b.Fatal(err)
	}
	gid := entmoot.GroupID{0x5b}
	group, err := Create(b.TempDir(), founder, founderInfo, gid, DefaultPolicy(), 1_000)
	if err != nil {
		b.Fatal(err)
	}
	base := group.Canonical()
	_ = group.Close()
	clock := int64(1_000)
	sign := func(identity *keystore.Identity, rec Record) Record {
		info, err := identityInfo(identity)
		if err != nil {
			b.Fatal(err)
		}
		clock += 10
		rec.GroupID, rec.Actor, rec.Timestamp = gid, info, clock
		if rec.Kind == KindJoin || rec.Kind == KindLeave {
			rec.Subject = info
		}
		signed, err := SignRecord(identity, rec)
		if err != nil {
			b.Fatal(err)
		}
		return signed
	}
	joins := n * 3 / 5
	members := make([]*keystore.Identity, 0, joins)
	records := make([]Record, 0, n)
	for range joins {
		identity, err := keystore.Generate()
		if err != nil {
			b.Fatal(err)
		}
		info, err := identityInfo(identity)
		if err != nil {
			b.Fatal(err)
		}
		capability := entmoot.BootstrapCapability{
			GroupID: gid, Founder: founderInfo, MaxUses: 1, IssuedAtMS: clock, ExpiresAtMS: clock + 3_600_000,
			TargetPublicKey: info.EntmootPubKey, TargetMemberID: *info.MemberID, TargetPeerID: info.PeerID,
		}
		if _, err := rand.Read(capability.Nonce[:]); err != nil {
			b.Fatal(err)
		}
		if err := SignInvite(founder, &capability); err != nil {
			b.Fatal(err)
		}
		records = append(records, sign(identity, Record{Kind: KindJoin, Invite: &capability}))
		members = append(members, identity)
	}
	for i, member := range members {
		if len(records) >= n {
			break
		}
		if i%2 == 0 {
			info, _ := identityInfo(member)
			records = append(records, sign(founder, Record{Kind: KindRemove, Subject: info}))
		} else {
			records = append(records, sign(member, Record{Kind: KindLeave}))
		}
	}
	return base, records
}

// BenchmarkProject500 is the projection every Apply runs (reproject).
func BenchmarkProject500(b *testing.B) {
	base, records := benchmarkRecords(b, 500)
	b.ResetTimer()
	for range b.N {
		Project(base, records)
	}
}
