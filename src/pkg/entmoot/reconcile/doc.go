// Package reconcile implements a Range-Based Set Reconciliation (RBSR) state
// machine for peer-to-peer anti-entropy over ordered 32-byte message ids.
//
// The algorithm is a generic RBSR protocol in the family described by
// Meyer et al. ("Range-Based Set Reconciliation", SRDS 2023, arXiv
// 2212.13567), the Nostr Negentropy specification (NIP-77,
// https://nips.nostr.com/77), and the Willow 3d-range RBSR protocol spec
// (https://willowprotocol.org/specs/3d-range-based-set-reconciliation).
// Two peers exchange compact interval fingerprints over a shared totally
// ordered universe; when an interval fingerprint agrees on both sides the
// interval is known to hold the same set with overwhelming probability,
// otherwise each side splits the interval further or — for small intervals —
// exchanges the raw id list so that differences can be named exactly.
//
// This implementation is wire-format-agnostic: callers drive a Session with
// []Range frames and receive []Range frames back, deciding how to serialize
// them. The package only depends on the root entmoot module and the Go
// standard library so it is safe to import from wire, gossip, or store
// without creating cycles.
package reconcile
