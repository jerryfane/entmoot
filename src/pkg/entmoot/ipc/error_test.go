package ipc

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestExitCodeMapping covers every registered ErrorCode per
// CLI_DESIGN §5.4 + §6.
func TestExitCodeMapping(t *testing.T) {
	cases := []struct {
		code ErrorCode
		want int
	}{
		{CodeOK, 0},
		{CodeInternal, 1},
		{CodeNotMember, 2},
		{CodeGroupNotFound, 3},
		{CodeInvalidArgument, 5},
	}
	for _, tc := range cases {
		if got := ExitCode(tc.code); got != tc.want {
			t.Errorf("ExitCode(%q) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

// TestExitCodeUnknown ensures unknown codes fall through to 1 so
// callers get a sane default instead of zero.
func TestExitCodeUnknown(t *testing.T) {
	for _, c := range []ErrorCode{"", "BOGUS", "not-real", "OK "} {
		// ExitCode("OK ") with a trailing space must NOT match CodeOK;
		// the codes are exact strings.
		if got := ExitCode(c); got != 1 {
			t.Errorf("ExitCode(%q) = %d, want 1", c, got)
		}
	}
}

// TestEncodeSetsErrorType verifies that Encode populates
// ErrorFrame.Type = "error" when the caller leaves it empty, and does
// not clobber a caller-supplied value.
func TestEncodeSetsErrorType(t *testing.T) {
	// Empty Type: Encode must fill it in.
	ef := &ErrorFrame{
		Code:    CodeInternal,
		Message: "something went wrong",
	}
	if _, _, err := Encode(ef); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if ef.Type != "error" {
		t.Fatalf("Type = %q, want %q", ef.Type, "error")
	}

	// Caller-supplied non-empty Type: respect it. (We don't need to
	// coerce, just not clobber.)
	ef2 := &ErrorFrame{
		Type:    "error",
		Code:    CodeInternal,
		Message: "x",
	}
	if _, _, err := Encode(ef2); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if ef2.Type != "error" {
		t.Fatalf("pre-filled Type = %q, want %q", ef2.Type, "error")
	}
}

// TestEncodeErrorFrameBodyShape confirms the JSON body carries the
// expected keys: "type": "error", "code", and "message".
func TestEncodeErrorFrameBodyShape(t *testing.T) {
	ef := &ErrorFrame{
		Code:    CodeNotMember,
		Message: "not a member",
	}
	_, body, err := Encode(ef)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if gt, _ := decoded["type"].(string); gt != "error" {
		t.Errorf(`body["type"] = %v, want "error"`, decoded["type"])
	}
	if gc, _ := decoded["code"].(string); gc != "NOT_MEMBER" {
		t.Errorf(`body["code"] = %v, want "NOT_MEMBER"`, decoded["code"])
	}
	if gm, _ := decoded["message"].(string); gm != "not a member" {
		t.Errorf(`body["message"] = %v, want "not a member"`, decoded["message"])
	}
	// GroupID was not set; it must be omitted from the JSON.
	if _, ok := decoded["group_id"]; ok {
		t.Errorf(`body["group_id"] present; want omitted`)
	}
}

// TestEncodeErrorFrameWithGroupID confirms the optional group_id field
// is emitted as a base64 JSON string when set.
func TestEncodeErrorFrameWithGroupID(t *testing.T) {
	gid := mustGroupID(0xAB)
	ef := &ErrorFrame{
		Code:    CodeGroupNotFound,
		GroupID: &gid,
		Message: "unknown group",
	}
	_, body, err := Encode(ef)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Cheap shape check: group_id must be present and must be a
	// base64 string (not an array).
	if !bytes.Contains(body, []byte(`"group_id":"`)) {
		t.Errorf("body missing base64 group_id: %s", body)
	}
}
