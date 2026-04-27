// Package delegation models local authorization for an always-on service peer
// to sync or notify on behalf of a mobile/user Entmoot identity.
package delegation

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
)

// Capability is one operation a mobile identity delegates to a service peer.
type Capability string

const (
	CapabilitySync         Capability = "sync"
	CapabilityNotify       Capability = "notify"
	CapabilityRelayPublish Capability = "relay_publish"
)

// Record authorizes ServicePeer to perform selected capabilities for User in
// GroupID. It is local policy for the service layer; future work can carry the
// same shape in signed roster/control records.
type Record struct {
	GroupID      entmoot.GroupID  `json:"group_id"`
	User         entmoot.NodeInfo `json:"user"`
	ServicePeer  entmoot.NodeID   `json:"service_peer"`
	Capabilities []Capability     `json:"capabilities"`
	CreatedAt    time.Time        `json:"created_at"`
	ValidUntil   time.Time        `json:"valid_until,omitempty"`
	RevokedAt    time.Time        `json:"revoked_at,omitempty"`
}

// ErrDenied is returned when a delegation does not authorize an operation.
var ErrDenied = errors.New("delegation: denied")

// Valid reports whether rec is active at now.
func (rec Record) Valid(now time.Time) bool {
	if rec.ServicePeer == 0 || rec.User.PilotNodeID == 0 || len(rec.User.EntmootPubKey) == 0 {
		return false
	}
	if !rec.RevokedAt.IsZero() && !rec.RevokedAt.After(now) {
		return false
	}
	if !rec.ValidUntil.IsZero() && !rec.ValidUntil.After(now) {
		return false
	}
	return len(rec.Capabilities) > 0
}

// Allows reports whether rec grants cap to servicePeer for user.
func (rec Record) Allows(now time.Time, user entmoot.NodeID, servicePeer entmoot.NodeID, cap Capability) bool {
	if !rec.Valid(now) || rec.User.PilotNodeID != user || rec.ServicePeer != servicePeer {
		return false
	}
	for _, c := range rec.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// Registry stores delegation records in memory.
type Registry struct {
	mu      sync.RWMutex
	records map[key]Record
}

type key struct {
	group       entmoot.GroupID
	user        entmoot.NodeID
	servicePeer entmoot.NodeID
}

// NewRegistry returns an empty local delegation registry.
func NewRegistry() *Registry {
	return &Registry{records: make(map[key]Record)}
}

// Upsert stores or replaces rec.
func (r *Registry) Upsert(rec Record) error {
	if !rec.Valid(time.Now()) {
		return fmt.Errorf("%w: invalid record", ErrDenied)
	}
	k := key{group: rec.GroupID, user: rec.User.PilotNodeID, servicePeer: rec.ServicePeer}
	r.mu.Lock()
	r.records[k] = cloneRecord(rec)
	r.mu.Unlock()
	return nil
}

// Revoke marks a record revoked if it exists.
func (r *Registry) Revoke(groupID entmoot.GroupID, user entmoot.NodeID, servicePeer entmoot.NodeID, at time.Time) bool {
	k := key{group: groupID, user: user, servicePeer: servicePeer}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[k]
	if !ok {
		return false
	}
	rec.RevokedAt = at
	r.records[k] = rec
	return true
}

// Check returns nil when cap is authorized.
func (r *Registry) Check(now time.Time, groupID entmoot.GroupID, user entmoot.NodeID, servicePeer entmoot.NodeID, cap Capability) error {
	r.mu.RLock()
	rec, ok := r.records[key{group: groupID, user: user, servicePeer: servicePeer}]
	r.mu.RUnlock()
	if !ok || !rec.Allows(now, user, servicePeer, cap) {
		return fmt.Errorf("%w: user=%d service_peer=%d capability=%s", ErrDenied, user, servicePeer, cap)
	}
	return nil
}

func cloneRecord(rec Record) Record {
	rec.User.EntmootPubKey = append([]byte(nil), rec.User.EntmootPubKey...)
	rec.Capabilities = append([]Capability(nil), rec.Capabilities...)
	return rec
}
