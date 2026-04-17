package topic

import (
	"testing"

	entmoot "entmoot/pkg/entmoot"
)

func TestValidPattern(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		pattern string
		wantErr bool
	}{
		{"empty", "", true},
		{"concrete single", "foo", false},
		{"concrete multi", "foo/bar/baz", false},
		{"plus alone", "+", false},
		{"plus segment", "foo/+/baz", false},
		{"hash alone", "#", false},
		{"hash trailing", "foo/#", false},
		{"plus and hash", "foo/+/#", false},
		{"hash not trailing", "foo/#/bar", true},
		{"hash in middle segment", "foo/#bar", true},
		{"hash suffix on segment", "foo/bar#", true},
		{"plus as suffix", "foo+", true},
		{"plus as prefix", "+bar", true},
		{"plus mid segment", "a+b", true},
		{"leading slash", "/foo", true},
		{"trailing slash", "foo/", true},
		{"double slash", "foo//bar", true},
		{"only slash", "/", true},
		{"hash twice", "#/#", true}, // first '#' is not trailing
		{"hash with content after", "foo/#/baz/#", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidPattern(tc.pattern)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidPattern(%q) err=%v, wantErr=%v", tc.pattern, err, tc.wantErr)
			}
		})
	}
}

func TestMatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		pattern string
		topic   string
		want    bool
	}{
		// Exact.
		{"exact equal", "foo/bar", "foo/bar", true},
		{"exact diff last", "foo/bar", "foo/baz", false},
		{"exact longer topic", "foo/bar", "foo/bar/baz", false},
		{"exact shorter topic", "foo/bar/baz", "foo/bar", false},
		{"single concrete equal", "foo", "foo", true},
		{"single concrete diff", "foo", "bar", false},

		// '+' single-segment wildcard.
		{"plus middle match", "foo/+/baz", "foo/x/baz", true},
		{"plus middle too long", "foo/+/baz", "foo/x/y/baz", false},
		{"plus middle too short", "foo/+/baz", "foo/baz", false},
		{"plus leading", "+/bar", "foo/bar", true},
		{"plus leading no match", "+/bar", "foo/baz", false},
		{"plus trailing", "foo/+", "foo/bar", true},
		{"plus trailing too long", "foo/+", "foo/bar/baz", false},
		{"plus only", "+", "foo", true},
		{"plus only multi-seg", "+", "foo/bar", false},

		// '#' multi-segment wildcard.
		{"hash bare matches root", "foo/#", "foo", true},
		{"hash bare matches one more", "foo/#", "foo/x", true},
		{"hash bare matches many more", "foo/#", "foo/x/y", true},
		{"hash bare needs prefix", "foo/#", "bar/x", false},
		{"hash only matches all", "#", "anything", true},
		{"hash only matches nested", "#", "a/b/c", true},

		// '+' and '#' combined.
		{"plus hash matches one", "foo/+/#", "foo/x", true},
		{"plus hash matches many", "foo/+/#", "foo/x/y/z", true},
		{"plus hash needs plus match", "foo/+/#", "foo", false},

		// Case sensitivity.
		{"case sensitive mismatch", "Foo/bar", "foo/bar", false},
		{"case sensitive match", "Foo/bar", "Foo/bar", true},

		// Empty inputs — no match, never a panic.
		{"empty pattern", "", "foo", false},
		{"empty topic", "foo", "", false},
		{"both empty", "", "", false},

		// Invalid patterns — Match returns false.
		{"hash not trailing returns false", "foo/#/bar", "foo/x/bar", false},
		{"partial plus is invalid", "foo+", "foo+", false},
		{"partial hash is invalid", "#bar", "#bar", false},
		{"leading slash invalid", "/foo", "/foo", false},
		{"double slash invalid", "foo//bar", "foo//bar", false},

		// Invalid topics — Match returns false even with a matching shape.
		{"topic with plus invalid", "foo/+", "foo/+", false},
		{"topic with hash invalid", "foo/#", "foo/#", false},
		{"topic leading slash invalid", "foo/bar", "/foo/bar", false},
		{"topic trailing slash invalid", "foo/bar", "foo/bar/", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Match(tc.pattern, tc.topic)
			if got != tc.want {
				t.Fatalf("Match(%q, %q) = %v, want %v", tc.pattern, tc.topic, got, tc.want)
			}
		})
	}
}

func TestMatchDoesNotPanicOnJunk(t *testing.T) {
	t.Parallel()
	// Mostly a regression guard for the "never panics" contract.
	junk := []string{
		"", "/", "//", "///", "+", "#", "a/+/b/#", "a/#/b",
		"a+b", "#a", "a#", "foo\x00bar",
	}
	for _, p := range junk {
		for _, tp := range junk {
			_ = Match(p, tp)
		}
	}
}

func TestMatchAny(t *testing.T) {
	t.Parallel()
	filter := entmoot.Filter{
		"entmoot/security/+",
		"logs/#",
		"exact/topic",
	}

	cases := []struct {
		topic string
		want  bool
	}{
		{"entmoot/security/cve", true},
		{"entmoot/security/hotfix", true},
		{"entmoot/security/cve/2026", false}, // '+' is one segment only
		{"logs", true},                       // '#' matches zero tail segments
		{"logs/errors", true},
		{"logs/errors/fatal", true},
		{"exact/topic", true},
		{"exact/topic/extra", false},
		{"unrelated", false},
		{"", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.topic, func(t *testing.T) {
			t.Parallel()
			got := MatchAny(filter, tc.topic)
			if got != tc.want {
				t.Fatalf("MatchAny(filter, %q) = %v, want %v", tc.topic, got, tc.want)
			}
		})
	}
}

func TestMatchAnyEmptyFilter(t *testing.T) {
	t.Parallel()
	if MatchAny(entmoot.Filter{}, "foo") {
		t.Fatalf("empty filter should never match")
	}
	if MatchAny(nil, "foo") {
		t.Fatalf("nil filter should never match")
	}
}

func TestMatchAnySkipsInvalidPatterns(t *testing.T) {
	t.Parallel()
	// Invalid patterns must not cause a match; only the valid one decides.
	f := entmoot.Filter{"foo/#/bar", "foo+", "exact/topic"}
	if !MatchAny(f, "exact/topic") {
		t.Fatalf("valid pattern in filter should still match")
	}
	if MatchAny(f, "foo/x/bar") {
		t.Fatalf("invalid patterns must not match")
	}
}
