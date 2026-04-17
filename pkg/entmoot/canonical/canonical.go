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
// MessageID computes sha256 over the canonical encoding of the signing form
// of a Message: the message with its ID and Signature fields zeroed. Every
// other field (group_id, author, timestamp, topics, parents, content,
// references) contributes to the id.
package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"entmoot/pkg/entmoot"
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

// MessageID returns sha256(Encode(signing form of m)).
//
// The signing form is m with ID and Signature zeroed. Every other field
// contributes: GroupID, Author, Timestamp, Topics, Parents, Content,
// References. A single bit flip in any of those changes the MessageID.
func MessageID(m entmoot.Message) entmoot.MessageID {
	signing := m
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil

	encoded, err := Encode(signing)
	if err != nil {
		// Encoding a Message value should never fail: it contains only
		// types supported by encoding/json. If it somehow does, we do not
		// have a meaningful recovery path for callers and returning a
		// zeroed id would silently mask the failure. Panic is the
		// principled choice here; callers pass well-formed Messages.
		panic(fmt.Sprintf("canonical.MessageID: encoding message failed: %v", err))
	}
	return entmoot.MessageID(sha256.Sum256(encoded))
}

// RosterEntryID returns sha256(Encode(signing form of e)).
//
// The signing form is e with ID and Signature zeroed, matching the convention
// used by MessageID. Every other field (Op, Subject, Policy, Actor, Timestamp,
// Parents) contributes to the id. Callers should populate ID with this result
// after signing so the on-wire and on-disk forms are self-describing.
func RosterEntryID(e entmoot.RosterEntry) entmoot.RosterEntryID {
	signing := e
	signing.ID = entmoot.RosterEntryID{}
	signing.Signature = nil

	encoded, err := Encode(signing)
	if err != nil {
		// Encoding a RosterEntry value should never fail: it contains only
		// types supported by encoding/json. Mirror MessageID's panic for
		// consistency — callers pass well-formed entries.
		panic(fmt.Sprintf("canonical.RosterEntryID: encoding roster entry failed: %v", err))
	}
	return entmoot.RosterEntryID(sha256.Sum256(encoded))
}
