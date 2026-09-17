package entmoot_test

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestOperationalTreeHasNoPilotDependency keeps Pilot confined to immutable
// legacy readers and the one-way conversion path.
func TestOperationalTreeHasNoPilotDependency(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]string{
		"src/pkg/entmoot/types.go":                       "immutable legacy signed record fields",
		"src/pkg/entmoot/member_identity.go":             "legacy-record identity validation boundary",
		"src/pkg/entmoot/identity_transition.go":         "founder-signed legacy identity mapping",
		"src/pkg/entmoot/conversion/conversion.go":       "one-way durable conversion",
		"src/pkg/entmoot/order/order.go":                 "ordering immutable legacy messages",
		"src/pkg/entmoot/roster/roster.go":               "validating immutable legacy rosters during conversion",
		"src/pkg/entmoot/transport/libp2p/validation.go": "verifying immutable legacy messages",
	}
	// Every file the repository SHIPS, and nothing else. Walking the checkout
	// also read whatever was lying there untracked: a scratch file containing
	// "autopilot" turned this test red while naming a file no release
	// contains. Asking git for the tracked set keeps the release manifest and
	// the shell scripts in scope - narrowing to a few directories silently
	// dropped .goreleaser.yaml - without letting local mess decide.
	tracked, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v (this guard needs the checkout)", root, err)
	}
	// Prose and generated web assets are not the operational tree.
	skipped := []string{"docs/", "paper/", "website/"}
	var residues []string
	for _, rel := range strings.Split(strings.TrimRight(string(tracked), "\x00"), "\x00") {
		if rel == "" {
			continue
		}
		if _, ok := allowed[rel]; ok {
			continue
		}
		if !inventoryFile(rel) {
			continue
		}
		prose := false
		for _, prefix := range skipped {
			if strings.HasPrefix(rel, prefix) {
				prose = true
			}
		}
		if prose {
			continue
		}
		file, err := os.Open(filepath.Join(root, rel))
		if errors.Is(err, os.ErrNotExist) {
			// The index can name a file the working tree does not have: a
			// deletion staged mid-refactor, or a sparse checkout. There is
			// nothing to scan, and failing here would blame this guard for an
			// unrelated state.
			continue
		}
		if err != nil {
			t.Fatalf("open %s: %v", rel, err)
		}
		scanner := bufio.NewScanner(file)
		for line := 1; scanner.Scan(); line++ {
			if strings.Contains(strings.ToLower(scanner.Text()), "pilot") {
				residues = append(residues, rel+":"+itoa(line))
			}
		}
		scanErr := scanner.Err()
		if err := file.Close(); err != nil {
			t.Fatalf("close %s: %v", rel, err)
		}
		if scanErr != nil {
			t.Fatalf("scan %s: %v", rel, scanErr)
		}
	}
	if len(residues) != 0 {
		t.Fatalf("operational Pilot residues outside explicit legacy allowlist: %s", strings.Join(residues, ", "))
	}
}

func inventoryFile(path string) bool {
	if strings.HasSuffix(path, "_test.go") {
		return false
	}
	base := filepath.Base(path)
	if base == "go.mod" || base == "go.sum" || base == "Dockerfile" {
		return true
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".go", ".sh", ".service", ".socket", ".yaml", ".yml", ".toml", ".json", ".env":
		return true
	default:
		return false
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	pos := len(digits)
	for value > 0 {
		pos--
		digits[pos] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[pos:])
}
