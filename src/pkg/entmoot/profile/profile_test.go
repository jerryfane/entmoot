package profile

import (
	"errors"
	"strings"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	want := Profile{DisplayName: "burj", IssuedAtMS: 1_700_000_000_000, ExpiresAtMS: 1_700_000_900_000}
	content, err := Encode(want)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Decode(content)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// TestDecodeIgnoresForeignContent pins the distinction the daemon depends on:
// an ordinary message that happens to carry the reserved topic must be ignored
// quietly, while a payload claiming to be a profile and failing validation is
// worth reporting.
func TestDecodeIgnoresForeignContent(t *testing.T) {
	for _, content := range []string{
		`hello`,
		`{"hello":"world"}`,
		`{"type":"entmoot/other/1","display_name":"x","issued_at_ms":1}`,
		``,
	} {
		if _, err := Decode([]byte(content)); !errors.Is(err, ErrNotProfile) {
			t.Fatalf("Decode(%q) err = %v, want ErrNotProfile", content, err)
		}
	}
}

// TestDecodeReportsAMalformedProfile is the other half: a payload that claims
// to be a profile and fails validation must not be silently ignored, or a
// member would never learn why its name did not appear.
func TestDecodeReportsAMalformedProfile(t *testing.T) {
	for _, content := range []string{
		`{"type":"entmoot/profile/1","display_name":"x","issued_at_ms":0}`,
		`{"type":"entmoot/profile/1","display_name":"x\nadmin","issued_at_ms":1}`,
		`{"type":"entmoot/profile/1","display_name":"x","issued_at_ms":10,"expires_at_ms":5}`,
	} {
		_, err := Decode([]byte(content))
		if err == nil {
			t.Fatalf("Decode(%q) accepted an invalid profile", content)
		}
		if errors.Is(err, ErrNotProfile) {
			t.Fatalf("Decode(%q) reported a malformed profile as foreign content", content)
		}
	}
}

func TestDisplayNameRules(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "trims", input: "  burj  ", want: "burj"},
		{name: "empty means withdrawn", input: "   ", want: ""},
		{name: "non-latin is fine", input: "бурдж", want: "бурдж"},
		{name: "emoji is fine", input: "pi-tv 📺", want: "pi-tv 📺"},
		{name: "newline refused", input: "burj\nadmin", wantErr: true},
		{name: "carriage return refused", input: "burj\radmin", wantErr: true},
		{name: "control char refused", input: "burj\x07", wantErr: true},
		// A name is displayed as name#MemberID, a bare concatenation, so a
		// name must not be able to reach past its own field.
		{name: "bidi override refused", input: "rev\u202Eevil", wantErr: true},
		{name: "right-to-left mark refused", input: "burj\u200F", wantErr: true},
		{name: "zero width space refused", input: "bu\u200Brj", wantErr: true},
		{name: "byte order mark refused", input: "\uFEFFburj", wantErr: true},
		{name: "line separator refused", input: "a\u2028b", wantErr: true},
		{name: "paragraph separator refused", input: "a\u2029b", wantErr: true},
		{name: "hash refused", input: "pi-burj#AAAA", wantErr: true},
		{name: "byte cap refused", input: strings.Repeat("\U0001F6F0", 64), wantErr: true},
		{name: "at the limit", input: strings.Repeat("ф", MaxDisplayNameLength), want: strings.Repeat("ф", MaxDisplayNameLength)},
		{name: "over the limit", input: strings.Repeat("ф", MaxDisplayNameLength+1), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NormalizeDisplayName(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeDisplayName(%q) = %q, want an error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeDisplayName(%q): %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("NormalizeDisplayName(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// TestLimitCountsRunesNotBytes is the reason the limit is expressed in runes:
// a name in a non-Latin script must not be cut shorter than a Latin one.
func TestLimitCountsRunesNotBytes(t *testing.T) {
	cyrillic := strings.Repeat("ф", MaxDisplayNameLength) // 2 bytes per rune
	if len(cyrillic) <= MaxDisplayNameLength {
		t.Fatalf("fixture is not multi-byte: %d bytes for %d runes", len(cyrillic), MaxDisplayNameLength)
	}
	if _, err := NormalizeDisplayName(cyrillic); err != nil {
		t.Fatalf("a name at the rune limit was refused on byte length: %v", err)
	}
}

func TestEncodeRejectsBadTimestamps(t *testing.T) {
	if _, err := Encode(Profile{DisplayName: "x"}); err == nil {
		t.Fatal("Encode with no issued_at_ms must fail")
	}
	if _, err := Encode(Profile{DisplayName: "x", IssuedAtMS: 100, ExpiresAtMS: 100}); err == nil {
		t.Fatal("Encode with expiry equal to issue time must fail")
	}
	if _, err := Encode(Profile{DisplayName: "x", IssuedAtMS: 100, ExpiresAtMS: 99}); err == nil {
		t.Fatal("Encode with expiry before issue time must fail")
	}
}

func TestHasTopic(t *testing.T) {
	if !HasTopic([]string{"chat", Topic}) {
		t.Fatal("HasTopic missed the reserved topic")
	}
	if HasTopic([]string{"chat", "entmoot/profile"}) {
		t.Fatal("HasTopic matched a near-miss topic")
	}
	if HasTopic(nil) {
		t.Fatal("HasTopic matched an empty topic list")
	}
}
