package wire

import (
	"fmt"

	entmoot "entmoot/pkg/entmoot"
)

// Collection limits cap decode work independently of the encoded byte caps in
// frame.go. They apply equally to local writers and remote readers.
const (
	MaxHelloGroups          = 64
	MaxRosterEntries        = 256
	MaxGossipIDs            = 256
	MaxRangeIDs             = 1024
	MaxPlumtreeIDs          = 512
	MaxSnapshotRecords      = 256
	MaxReconcileRanges      = 1024
	MaxTransportEndpoints   = 16
	MaxDiagnosticNonceBytes = 256
)

// ValidatePayloadLimits rejects collection shapes that can create excessive
// follow-on work after an otherwise byte-bounded JSON decode.
func ValidatePayloadLimits(v any) error {
	var field string
	var got, max int
	switch p := v.(type) {
	case *Hello:
		field, got, max = "hello groups", len(p.Groups), MaxHelloGroups
	case *RosterResp:
		field, got, max = "roster entries", len(p.Entries), MaxRosterEntries
	case *Gossip:
		field, got, max = "gossip ids", len(p.IDs), MaxGossipIDs
	case *RangeReq:
		if p.Limit < 0 {
			return fmt.Errorf("wire: negative range limit: %w", entmoot.ErrMalformedFrame)
		}
		field, got, max = "range limit", p.Limit, MaxRangeIDs
	case *RangeResp:
		field, got, max = "range ids", len(p.IDs), MaxRangeIDs
	case *IHave:
		field, got, max = "ihave ids", len(p.IDs), MaxPlumtreeIDs
	case *Graft:
		field, got, max = "graft ids", len(p.IDs), MaxPlumtreeIDs
	case *TransportAd:
		field, got, max = "transport endpoints", len(p.Endpoints), MaxTransportEndpoints
	case *TransportSnapshotResp:
		field, got, max = "transport snapshot ads", len(p.Ads), MaxSnapshotRecords
	case *MemberProfileSnapshotResp:
		field, got, max = "member profile snapshot records", len(p.Profiles), MaxSnapshotRecords
	case *Reconcile:
		field, got, max = "reconcile ranges", len(p.Ranges), MaxReconcileRanges
	case *DiagPingReq:
		field, got, max = "diagnostic nonce bytes", len(p.Nonce), MaxDiagnosticNonceBytes
	case *DiagPingResp:
		field, got, max = "diagnostic nonce bytes", len(p.Nonce), MaxDiagnosticNonceBytes
	default:
		return nil
	}
	if got > max {
		return fmt.Errorf("wire: %s count %d exceeds cap %d: %w", field, got, max, entmoot.ErrOversized)
	}
	return nil
}
