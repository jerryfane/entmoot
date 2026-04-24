package ipcclient

import (
	"encoding/json"
	"testing"
)

// TestInfoDecodeWithTURNEndpoint asserts that a daemon response
// containing turn_endpoint decodes into Info.TURNEndpoint. This is
// the v1.4.0 path: pilot-daemon v1.9.0-jf.8+ with a TURN provider
// configured.
func TestInfoDecodeWithTURNEndpoint(t *testing.T) {
	t.Parallel()
	body := []byte(`{"node_id": 42, "hostname": "alice", "turn_endpoint": "relay.example.test:3478"}`)
	var info Info
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if info.NodeID != 42 {
		t.Fatalf("NodeID = %d, want 42", info.NodeID)
	}
	if info.Hostname != "alice" {
		t.Fatalf("Hostname = %q, want alice", info.Hostname)
	}
	if info.TURNEndpoint != "relay.example.test:3478" {
		t.Fatalf("TURNEndpoint = %q, want relay.example.test:3478", info.TURNEndpoint)
	}
}

// TestInfoDecodeWithoutTURNEndpoint asserts backwards-compat with
// jf.7 (and earlier) daemons whose InfoOK payload does not carry
// turn_endpoint: the field decodes to the zero value ("") and
// downstream callers treat empty as "no TURN relay".
func TestInfoDecodeWithoutTURNEndpoint(t *testing.T) {
	t.Parallel()
	body := []byte(`{"node_id": 7, "hostname": "bob"}`)
	var info Info
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if info.NodeID != 7 {
		t.Fatalf("NodeID = %d, want 7", info.NodeID)
	}
	if info.Hostname != "bob" {
		t.Fatalf("Hostname = %q, want bob", info.Hostname)
	}
	if info.TURNEndpoint != "" {
		t.Fatalf("TURNEndpoint = %q, want empty string (jf.7 compat)", info.TURNEndpoint)
	}
}

// TestInfoDecodeEmptyTURNEndpoint covers the path where the daemon
// explicitly sends "turn_endpoint": "" (omitempty should prevent
// this on the encode side, but the decode path must still tolerate
// it without treating "" as a configured relay).
func TestInfoDecodeEmptyTURNEndpoint(t *testing.T) {
	t.Parallel()
	body := []byte(`{"node_id": 9, "turn_endpoint": ""}`)
	var info Info
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if info.TURNEndpoint != "" {
		t.Fatalf("TURNEndpoint = %q, want empty string", info.TURNEndpoint)
	}
}

// TestInfoEncodeOmitsEmptyTURNEndpoint asserts the encode path
// honours omitempty so v1.4.0-produced Info values (e.g. in
// tests that marshal back out) round-trip through older consumers
// without a stray null/empty key.
func TestInfoEncodeOmitsEmptyTURNEndpoint(t *testing.T) {
	t.Parallel()
	info := Info{NodeID: 11, Hostname: "carol"}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	got := string(raw)
	for _, needle := range []string{"turn_endpoint"} {
		if containsSubstring(got, needle) {
			t.Fatalf("encoded Info %q should not contain %q (omitempty)", got, needle)
		}
	}
}

// containsSubstring is a tiny local helper so the test file does
// not pull in strings for a single Contains call.
func containsSubstring(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
