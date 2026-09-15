// Package profile carries a member's self-chosen display name.
//
// A display name is not identity. Identity is the full-width MemberID and the
// Entmoot public key it derives from; a profile is a hint an app may show
// instead of a truncated key. It is therefore published the same way any other
// claim about a member is published: as an ordinary signed message on a
// reserved topic, inside the group. That gives it the properties the display
// layer needs for free — the author is a current member, the signature is over
// the canonical message, and a member who is removed stops being able to
// publish a new one.
//
// Nothing here grants authority. A reader decides whether to show a name; the
// name never decides who may write, publish, or join.
package profile

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Topic is the reserved topic a profile is published on. Members that filter
// their subscription by topic must include it to observe other members' names.
const Topic = "entmoot/profile/1"

// payloadType tags the content so a future profile shape can be added without
// a reader mistaking one for the other.
const payloadType = "entmoot/profile/1"

// MaxDisplayNameLength bounds a name in runes rather than bytes, so a name in
// a non-Latin script is not cut shorter than a Latin one.
const MaxDisplayNameLength = 64

// MaxDisplayNameBytes must not exceed the store's own hostname cap
// (esphttp.MaxNodeProfileHostnameBytes). Both limits are enforced here so a
// name that the store would silently drop is refused at publish time instead
// of being reported as published. 64 four-byte runes are 256 bytes, so without
// this the two limits disagree by exactly one name shape.
const MaxDisplayNameBytes = 255

// ErrNotProfile is returned when content is not a profile payload at all. It
// is distinct from a validation error: an unrecognised payload on the reserved
// topic is ignored, while a malformed profile is worth reporting.
var ErrNotProfile = errors.New("profile: content is not a member profile payload")

// Profile is a member's claim about how it wants to be shown.
type Profile struct {
	// DisplayName is the name to show. Empty means the member is withdrawing
	// its name, and a reader falls back to the MemberID presentation.
	DisplayName string `json:"display_name"`
	// IssuedAtMS is unix milliseconds at compose time. A reader prefers the
	// newest profile it holds for a member.
	IssuedAtMS int64 `json:"issued_at_ms"`
	// ExpiresAtMS is unix milliseconds after which the name must not be
	// shown. Zero means no expiry.
	ExpiresAtMS int64 `json:"expires_at_ms,omitempty"`
}

type wireProfile struct {
	Type string `json:"type"`
	Profile
}

// NormalizeDisplayName trims a name and reports whether it is usable.
//
// Refusals rather than rewrites: a name carrying a control or formatting
// character is either a mistake or an attempt to forge output, and silently
// stripping it would hide both. A name is displayed as "name#MemberID", a bare
// concatenation, so the name must not be able to reach past its own field:
//
//   - Cc control characters, and \n and \r explicitly, would split a line.
//   - Cf format characters include the bidi overrides (U+202E RLO and friends)
//     and zero-width characters; a trailing override reverses the appended
//     "#MemberID" run in any bidi-aware renderer, so the identity that is
//     supposed to travel with the name can be made to read as something else.
//   - Zl and Zp (U+2028, U+2029) are line and paragraph separators in most
//     renderers even though they are not Cc.
//   - '#' is the separator itself. Allowing it lets a name contain a plausible
//     "#<something>" tail, so a reader cannot tell which '#' begins the real
//     MemberID.
func NormalizeDisplayName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", nil
	}
	if !utf8.ValidString(trimmed) {
		return "", errors.New("profile: display name is not valid UTF-8")
	}
	if n := utf8.RuneCountInString(trimmed); n > MaxDisplayNameLength {
		return "", fmt.Errorf("profile: display name is %d runes, limit %d", n, MaxDisplayNameLength)
	}
	if n := len(trimmed); n > MaxDisplayNameBytes {
		return "", fmt.Errorf("profile: display name is %d bytes, limit %d", n, MaxDisplayNameBytes)
	}
	for _, r := range trimmed {
		switch {
		case r == '\n' || r == '\r':
			return "", errors.New("profile: display name contains a line break")
		case r == '#':
			return "", errors.New("profile: display name contains '#', which separates the name from the member id")
		case unicode.IsControl(r):
			return "", errors.New("profile: display name contains a control character")
		case unicode.Is(unicode.Cf, r):
			return "", fmt.Errorf("profile: display name contains the formatting character %U, which can reorder or hide text", r)
		case unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r):
			return "", fmt.Errorf("profile: display name contains the line separator %U", r)
		}
	}
	return trimmed, nil
}

// Encode validates a profile and returns the message content to publish.
func Encode(p Profile) ([]byte, error) {
	name, err := NormalizeDisplayName(p.DisplayName)
	if err != nil {
		return nil, err
	}
	if p.IssuedAtMS <= 0 {
		return nil, errors.New("profile: issued_at_ms is required")
	}
	if p.ExpiresAtMS != 0 && p.ExpiresAtMS <= p.IssuedAtMS {
		return nil, fmt.Errorf("profile: expires_at_ms %d is not after issued_at_ms %d", p.ExpiresAtMS, p.IssuedAtMS)
	}
	return json.Marshal(wireProfile{
		Type: payloadType,
		Profile: Profile{
			DisplayName: name,
			IssuedAtMS:  p.IssuedAtMS,
			ExpiresAtMS: p.ExpiresAtMS,
		},
	})
}

// Decode parses message content published on Topic.
//
// Content that is not a profile payload returns ErrNotProfile so a caller can
// ignore it quietly; content that claims to be one and is not valid returns a
// descriptive error, because that is worth logging.
func Decode(content []byte) (Profile, error) {
	var wire wireProfile
	if err := json.Unmarshal(content, &wire); err != nil {
		return Profile{}, ErrNotProfile
	}
	if wire.Type != payloadType {
		return Profile{}, ErrNotProfile
	}
	name, err := NormalizeDisplayName(wire.DisplayName)
	if err != nil {
		return Profile{}, err
	}
	if wire.IssuedAtMS <= 0 {
		return Profile{}, errors.New("profile: issued_at_ms is required")
	}
	if wire.ExpiresAtMS != 0 && wire.ExpiresAtMS <= wire.IssuedAtMS {
		return Profile{}, fmt.Errorf("profile: expires_at_ms %d is not after issued_at_ms %d", wire.ExpiresAtMS, wire.IssuedAtMS)
	}
	return Profile{DisplayName: name, IssuedAtMS: wire.IssuedAtMS, ExpiresAtMS: wire.ExpiresAtMS}, nil
}

// HasTopic reports whether a message's topics include the reserved profile
// topic.
func HasTopic(topics []string) bool {
	for _, t := range topics {
		if t == Topic {
			return true
		}
	}
	return false
}
