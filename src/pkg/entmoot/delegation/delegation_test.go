package delegation

import (
	"errors"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

func TestRegistryCheckAndRevoke(t *testing.T) {
	now := time.Now()
	reg := NewRegistry()
	rec := Record{
		GroupID:     groupID(1),
		User:        entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("pub")},
		ServicePeer: 45981,
		Capabilities: []Capability{
			CapabilitySync,
			CapabilityNotify,
		},
		CreatedAt:  now,
		ValidUntil: now.Add(time.Hour),
	}
	if err := reg.Upsert(rec); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := reg.Check(now, rec.GroupID, 45491, 45981, CapabilitySync); err != nil {
		t.Fatalf("Check sync: %v", err)
	}
	if err := reg.Check(now, rec.GroupID, 45491, 45981, CapabilityRelayPublish); !errors.Is(err, ErrDenied) {
		t.Fatalf("Check relay_publish err = %v, want ErrDenied", err)
	}
	if !reg.Revoke(rec.GroupID, 45491, 45981, now.Add(time.Second)) {
		t.Fatalf("Revoke returned false")
	}
	if err := reg.Check(now.Add(2*time.Second), rec.GroupID, 45491, 45981, CapabilitySync); !errors.Is(err, ErrDenied) {
		t.Fatalf("Check after revoke err = %v, want ErrDenied", err)
	}
}

func TestRecordExpires(t *testing.T) {
	now := time.Now()
	rec := Record{
		GroupID:      groupID(2),
		User:         entmoot.NodeInfo{PilotNodeID: 1, EntmootPubKey: []byte("pub")},
		ServicePeer:  2,
		Capabilities: []Capability{CapabilitySync},
		CreatedAt:    now.Add(-2 * time.Hour),
		ValidUntil:   now.Add(-time.Hour),
	}
	if rec.Valid(now) {
		t.Fatalf("expired record reported valid")
	}
}

func groupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}
