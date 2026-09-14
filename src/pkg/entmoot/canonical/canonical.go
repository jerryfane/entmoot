// Package canonical produces a deterministic byte form of values for signing
// and hashing.
//
// Any two peers running this code produce byte-identical encodings for equal
// inputs. Determinism is achieved by:
//
//   - disabling HTML escaping in encoding/json so characters like <, >, & are
//     not rewritten to their \u00xx forms,
//   - recursively sorting map keys (encoding/json already sorts top-level
//     map keys but we also walk nested maps to guarantee the property for
//     arbitrary payloads),
//   - relying on encoding/json's stable struct-field ordering, which follows
//     the definition order of fields in the Go source,
//   - stripping the trailing newline that encoding/json.Encoder appends after
//     each value so the output is exactly the encoded value's bytes.
//
// MessageID computes sha256 over MessageSigningBytes. Legacy messages keep
// their exact canonical bytes; version-2 messages use a domain-separated form
// that binds version, group, roster head, author, and content fields. ID and
// Signature never contribute.
package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"entmoot/pkg/entmoot"
)

const (
	messageV2Domain     = "entmoot/message/v2\x00"
	rosterEntryV2Domain = "entmoot/roster-entry/v2\x00"
)

// Encode returns the deterministic canonical JSON encoding of v.
//
// For struct values, encoding/json's stable field order is used. For values
// that contain map[string]any branches, those maps are re-encoded with their
// keys sorted recursively. HTML escaping is disabled.
func Encode(v any) ([]byte, error) {
	normalized, err := normalize(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normalized); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	// json.Encoder.Encode appends a trailing newline; strip it so the result
	// is exactly the encoded value.
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}

// normalize walks v, round-tripping any struct/typed value through
// encoding/json so we end up with a tree of plain json types
// (map[string]any / []any / string / float64 / bool / nil), then sorts every
// map's keys recursively.
//
// The round trip is what lets us handle named types and custom
// MarshalJSON implementations uniformly: after Marshal + Unmarshal into
// any, every object is a map[string]any whose keys we can sort.
func normalize(v any) (any, error) {
	// Round-trip through encoding/json to collapse named types, custom
	// MarshalJSON, etc, to the plain json value tree.
	raw, err := marshalNoEscape(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("canonical: normalize decode: %w", err)
	}
	return sortRecursive(decoded), nil
}

// marshalNoEscape marshals v with HTML escaping disabled.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if n := len(out); n > 0 && out[n-1] == '\n' {
		out = out[:n-1]
	}
	return out, nil
}

// sortRecursive walks a json-native value tree and returns a tree in which
// every map[string]any has its keys presented in sorted order by re-building
// the map via an ordered marshaller-friendly structure.
func sortRecursive(v any) any {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(sortedObject, 0, len(t))
		for _, k := range keys {
			out = append(out, sortedEntry{Key: k, Value: sortRecursive(t[k])})
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = sortRecursive(t[i])
		}
		return out
	default:
		return v
	}
}

// sortedEntry is one key/value pair in a deterministic object.
type sortedEntry struct {
	Key   string
	Value any
}

// sortedObject is a slice that marshals as a JSON object with keys in slice
// order. Because sortRecursive only ever builds it with pre-sorted keys, the
// encoded form is deterministic.
type sortedObject []sortedEntry

// MarshalJSON implements json.Marshaler for sortedObject.
func (s sortedObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, entry := range s {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := marshalNoEscape(entry.Key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		valBytes, err := marshalNoEscape(entry.Value)
		if err != nil {
			return nil, err
		}
		buf.Write(valBytes)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// MessageSigningBytes returns the exact author-signed form. The identifier and
// the signature itself are excluded so the form is stable.
func MessageSigningBytes(m entmoot.Message) ([]byte, error) {
	signing := m
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	encoded, err := Encode(signing)
	if err != nil {
		return nil, err
	}
	switch signing.Version {
	case 0:
		return encoded, nil
	case 2:
		out := make([]byte, 0, len(messageV2Domain)+len(encoded))
		out = append(out, messageV2Domain...)
		out = append(out, encoded...)
		return out, nil
	default:
		return nil, fmt.Errorf("canonical: unsupported message version %d", signing.Version)
	}
}

// MessageID returns sha256(MessageSigningBytes(m)).
func MessageID(m entmoot.Message) entmoot.MessageID {
	encoded, err := MessageSigningBytes(m)
	if err != nil {
		panic(fmt.Sprintf("canonical.MessageID: encoding message failed: %v", err))
	}
	return entmoot.MessageID(sha256.Sum256(encoded))
}

// RosterEntrySigningBytes returns the exact bytes covered by a roster
// signature. Legacy entries retain their historical canonical JSON bytes.
// Version-2 entries prepend a domain separator before the canonical JSON so
// their signatures cannot be confused with another signed record type.
func RosterEntrySigningBytes(e entmoot.RosterEntry) ([]byte, error) {
	signing := e
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil
	encoded, err := Encode(signing)
	if err != nil {
		return nil, err
	}
	if e.Version != 2 {
		return encoded, nil
	}
	out := make([]byte, 0, len(rosterEntryV2Domain)+len(encoded))
	out = append(out, rosterEntryV2Domain...)
	out = append(out, encoded...)
	return out, nil
}

// RosterEntryID returns sha256 over RosterEntrySigningBytes(e).
func RosterEntryID(e entmoot.RosterEntry) entmoot.RosterEntryID {
	encoded, err := RosterEntrySigningBytes(e)
	if err != nil {
		panic(fmt.Sprintf("canonical.RosterEntryID: encoding roster entry failed: %v", err))
	}
	return entmoot.RosterEntryID(sha256.Sum256(encoded))
}
