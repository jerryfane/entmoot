package skills

import (
	"io/fs"
	"testing"
)

func TestEmbeddedEntmootSkillPackage(t *testing.T) {
	content, err := fs.ReadFile(FS, "entmoot/SKILL.md")
	if err != nil {
		t.Fatalf("read embedded SKILL.md: %v", err)
	}
	if len(content) == 0 {
		t.Fatalf("embedded SKILL.md is empty")
	}

	entries, err := fs.ReadDir(FS, "entmoot/references")
	if err != nil {
		t.Fatalf("read embedded references: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("embedded references directory is empty")
	}

	if _, err := fs.Stat(FS, "entmoot/references/JOIN_SERVE.md"); err != nil {
		t.Fatalf("missing embedded JOIN_SERVE reference: %v", err)
	}
}
