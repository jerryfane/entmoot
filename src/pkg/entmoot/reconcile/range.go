package reconcile

import (
	"fmt"

	"entmoot/pkg/entmoot"
)

// Kind identifies what a Range carries on the wire.
type Kind uint8

const (
	// KindEmpty is the "nothing to do" signal for a range: the sender
	// asserts it has no further information to add, either because its
	// fingerprint already matched the peer's or because it has nothing
	// new to offer after a KindIDList exchange.
	KindEmpty Kind = 0
	// KindFingerprint carries a Fingerprint summarising the sender's ids
	// in [Lo, Hi). The receiver recomputes its local fingerprint for the
	// same range: if equal the range is resolved, otherwise the receiver
	// either splits further or falls back to KindIDList.
	KindFingerprint Kind = 1
	// KindIDList carries the sender's full list of ids in [Lo, Hi). Used
	// as the leaf form of the exchange when the range holds few enough
	// items that sending raw ids is cheaper than further recursion.
	KindIDList Kind = 2
)

// String returns a short human-readable name for k, suitable for logs.
func (k Kind) String() string {
	switch k {
	case KindEmpty:
		return "empty"
	case KindFingerprint:
		return "fingerprint"
	case KindIDList:
		return "id_list"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

// Range is one half-open slice of the message-id keyspace together with
// whatever evidence the sender attaches about its contents: a Fingerprint
// (for recursive narrowing) or an explicit IDs slice (for leaf exchanges),
// or nothing at all (KindEmpty).
//
// The interval is half-open: [Lo, Hi). A Hi of all-zero bytes is the
// "max" sentinel meaning "every id greater than or equal to Lo"; see
// Storage for the same convention on the storage side.
type Range struct {
	// Lo is the inclusive lower bound of the range.
	Lo entmoot.MessageID `json:"lo"`
	// Hi is the exclusive upper bound of the range. Zero means "max":
	// every id with Lo <= id.
	Hi entmoot.MessageID `json:"hi"`
	// Kind identifies what evidence is attached.
	Kind Kind `json:"kind"`
	// Fingerprint is the sender's fingerprint for [Lo, Hi). Populated
	// only when Kind == KindFingerprint.
	Fingerprint Fingerprint `json:"fingerprint,omitempty"`
	// IDs is the sender's full id list for [Lo, Hi). Populated only when
	// Kind == KindIDList.
	IDs []entmoot.MessageID `json:"ids,omitempty"`
}

// IsEmpty reports whether r carries the "no further information" signal
// (Kind == KindEmpty).
func (r Range) IsEmpty() bool {
	return r.Kind == KindEmpty
}
