package entmoot_test

import (
	"bufio"
	"os"
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
		"src/pkg/entmoot/roster/roster.go":               "validating and projecting immutable legacy rosters",
		"src/pkg/entmoot/transport/libp2p/validation.go": "verifying immutable legacy messages",
	}
	var residues []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			switch rel {
			case ".git", "artifacts", "docs", "paper", "repos", "website":
				return filepath.SkipDir
			}
			return nil
		}
		if _, ok := allowed[rel]; ok {
			return nil
		}
		if !inventoryFile(rel) {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for line := 1; scanner.Scan(); line++ {
			if strings.Contains(strings.ToLower(scanner.Text()), "pilot") {
				residues = append(residues, rel+":"+itoa(line))
			}
		}
		return scanner.Err()
	})
	if err != nil {
		t.Fatal(err)
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
