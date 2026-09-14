package main

import (
	"os"
	"path/filepath"
	"testing"
)

const testRelayA = "/ip4/198.51.100.7/tcp/4001/p2p/12D3KooWPXb5rMPAHKYBc5Cwx9dbDhjm2Dsqwt8gFkGe6kGNCDSp"
const testRelayB = "/ip4/203.0.113.9/tcp/4001/p2p/12D3KooWBdvL92Hd76R1LN5qswuXSgQf7ZWZNwHhKeS4tDoHGzuA"

// A joiner that adopts a relay must still have it after a restart, or a NATed
// node comes back unreachable with nobody able to tell it a relay.
func TestAdoptedRelayHintsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	if stored, err := loadRelayHints(dir); err != nil || len(stored) != 0 {
		t.Fatalf("fresh data root hints = %v/%v, want empty/nil", stored, err)
	}
	if err := saveRelayHints(dir, []string{testRelayA}); err != nil {
		t.Fatal(err)
	}
	stored, err := loadRelayHints(dir)
	if err != nil || len(stored) != 1 || stored[0] != testRelayA {
		t.Fatalf("stored hints = %v/%v", stored, err)
	}
	// Joining a second group must not drop the first group's relay.
	if err := saveRelayHints(dir, []string{testRelayB}); err != nil {
		t.Fatal(err)
	}
	stored, err = loadRelayHints(dir)
	if err != nil || len(stored) != 2 {
		t.Fatalf("merged hints = %v/%v, want both relays", stored, err)
	}
	// Re-adopting the same relay must not grow the set.
	if err := saveRelayHints(dir, []string{testRelayA}); err != nil {
		t.Fatal(err)
	}
	stored, _ = loadRelayHints(dir)
	if len(stored) != 2 {
		t.Fatalf("hints after duplicate adoption = %v, want 2", stored)
	}
}

// The daemon must pick up adopted relays when the operator names none, and
// never override an explicit flag with them.
func TestDaemonHostConfigUsesAdoptedRelays(t *testing.T) {
	dir := t.TempDir()
	if err := saveRelayHints(dir, []string{testRelayA}); err != nil {
		t.Fatal(err)
	}
	config, err := daemonHostConfig(&globalFlags{data: dir, listenPort: 1004})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.ControlledRelays) != 1 {
		t.Fatalf("controlled relays = %v, want the adopted relay", config.ControlledRelays)
	}
	explicit, err := daemonHostConfig(&globalFlags{data: dir, listenPort: 1004, controlledRelays: stringListFlag{testRelayB}})
	if err != nil {
		t.Fatal(err)
	}
	if len(explicit.ControlledRelays) != 1 || explicit.ControlledRelays[0].ID.String() == config.ControlledRelays[0].ID.String() {
		t.Fatalf("explicit relays = %v, want only the flag value", explicit.ControlledRelays)
	}
}

// Hints come from a remote invite, so malformed or unbounded input must be
// filtered rather than trusted or fatal.
func TestRelayHintsAreFilteredAndBounded(t *testing.T) {
	junk := []string{
		"not-a-multiaddr",
		"/ip4/198.51.100.7/tcp/4001", // no peer id
		testRelayA,
		testRelayA, // duplicate
	}
	for i := 0; i < maxRelayHints+4; i++ {
		junk = append(junk, "/ip4/198.51.100.7/tcp/"+itoa(5000+i)+"/p2p/12D3KooWPXb5rMPAHKYBc5Cwx9dbDhjm2Dsqwt8gFkGe6kGNCDSp")
	}
	filtered, err := validateRelayHints(junk)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != maxRelayHints {
		t.Fatalf("filtered = %d hints, want the cap of %d", len(filtered), maxRelayHints)
	}
	if filtered[0] != testRelayA {
		t.Fatalf("first kept hint = %q, want the one valid input first", filtered[0])
	}
	for _, hint := range filtered {
		if hint == "not-a-multiaddr" {
			t.Fatal("a malformed hint survived filtering")
		}
	}
}

// A corrupt hints file must not make a node unstartable: the file is a cache
// of operator-visible addresses, not signed state.
func TestCorruptRelayHintsDoNotBlockAdoption(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, relayHintsFileName), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRelayHints(dir); err == nil {
		t.Fatal("corrupt hints file decoded without error")
	}
	if err := saveRelayHints(dir, []string{testRelayA}); err != nil {
		t.Fatalf("adoption over a corrupt file failed: %v", err)
	}
	stored, err := loadRelayHints(dir)
	if err != nil || len(stored) != 1 || stored[0] != testRelayA {
		t.Fatalf("hints after recovery = %v/%v", stored, err)
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
