// Package topic implements the MQTT-style topic pattern matcher used by
// Entmoot subscriber filters (see ARCHITECTURE.md §3.4).
//
// Syntax rules (enforced by ValidPattern):
//
//   - Patterns and topics are slash-separated, non-empty, ASCII strings.
//     Every segment must be non-empty: a leading, trailing, or repeated
//     "/" is an invalid pattern / topic.
//   - Concrete segments match literally, case-sensitive.
//   - "+" is a single-segment wildcard; it must occupy the entire segment.
//     "foo+" or "+bar" is not a wildcard — patterns containing those are
//     rejected by ValidPattern. Matching against a literal such segment
//     compares it byte-for-byte with the topic.
//   - "#" is a multi-segment tail wildcard and matches zero or more trailing
//     segments. It must appear only in the final segment and alone in that
//     segment. "a/#" matches "a", "a/b", "a/b/c".
//   - An empty pattern or an empty topic never matches.
//
// Match is lenient: it never panics and returns false for invalid patterns
// or invalid topics. Callers that want strict rejection should call
// ValidPattern up-front. Topic well-formedness is enforced inline by Match
// (no separate ValidTopic helper is exported in v0).
package topic

import (
	"errors"
	"strings"

	entmoot "entmoot/pkg/entmoot"
)

const (
	// segSep is the pattern/topic segment separator.
	segSep = "/"
	// wildSingle matches exactly one segment.
	wildSingle = "+"
	// wildMulti matches zero or more trailing segments.
	wildMulti = "#"
)

// ValidPattern reports nil if pattern is well-formed per the rules in the
// package godoc. It returns a descriptive error otherwise. The empty string
// is not a valid pattern.
func ValidPattern(pattern string) error {
	if pattern == "" {
		return errors.New("topic: empty pattern")
	}
	segs := strings.Split(pattern, segSep)
	for i, seg := range segs {
		if seg == "" {
			return errors.New("topic: empty segment in pattern")
		}
		// '#' must stand alone and only as the final segment.
		if strings.Contains(seg, wildMulti) {
			if seg != wildMulti {
				return errors.New("topic: '#' must occupy an entire segment")
			}
			if i != len(segs)-1 {
				return errors.New("topic: '#' must be the final segment")
			}
		}
		// '+' must stand alone in its segment.
		if strings.Contains(seg, wildSingle) && seg != wildSingle {
			return errors.New("topic: '+' must occupy an entire segment")
		}
	}
	return nil
}

// validTopic reports whether s is a well-formed concrete topic: non-empty,
// slash-separated with every segment non-empty, and no MQTT wildcards
// ('+' or '#') appearing anywhere. Topics are data, not patterns, so a
// wildcard character inside a topic is not treated as a wildcard but does
// indicate a malformed input; we reject outright to avoid surprising
// matches when a mis-encoded topic contains literal '+' or '#'.
func validTopic(s string) bool {
	if s == "" {
		return false
	}
	for _, seg := range strings.Split(s, segSep) {
		if seg == "" {
			return false
		}
		if strings.ContainsAny(seg, "+#") {
			return false
		}
	}
	return true
}

// Match reports whether topic matches pattern per MQTT-style semantics with
// '+' as a single-segment wildcard and '#' as a multi-segment tail wildcard.
//
// Match never panics. An invalid pattern or an invalid topic returns false;
// callers that want strictness should call ValidPattern first.
func Match(pattern, topic string) bool {
	if ValidPattern(pattern) != nil {
		return false
	}
	if !validTopic(topic) {
		return false
	}

	patSegs := strings.Split(pattern, segSep)
	topSegs := strings.Split(topic, segSep)

	for i, p := range patSegs {
		// Multi-segment wildcard: consume all remaining topic segments
		// (including zero). ValidPattern guarantees this is the final
		// pattern segment.
		if p == wildMulti {
			return true
		}
		// If the pattern still has segments but the topic is exhausted,
		// no match.
		if i >= len(topSegs) {
			return false
		}
		if p == wildSingle {
			// '+' matches exactly one segment; any non-empty segment
			// qualifies. validTopic has already guaranteed non-empty.
			continue
		}
		if p != topSegs[i] {
			return false
		}
	}
	// All pattern segments consumed; topic must be exhausted for a match.
	return len(patSegs) == len(topSegs)
}

// MatchAny reports whether topic matches any of the patterns in filter.
// An empty filter never matches. Invalid patterns inside the filter are
// skipped (per Match's lenient contract); they cannot cause a match.
func MatchAny(filter entmoot.Filter, topic string) bool {
	for _, p := range filter {
		if Match(p, topic) {
			return true
		}
	}
	return false
}
