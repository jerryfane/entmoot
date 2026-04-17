package roster

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

// rosterFileName is the per-group roster filename under <root>/groups/<gid>/.
const rosterFileName = "roster.jsonl"

// OpenJSONL opens (or creates) a RosterLog backed by the file:
//
//	<root>/groups/<base64url(group_id)>/roster.jsonl
//
// Parent directories are created with 0700; the roster file itself is opened
// with 0600. On open, any existing file is scanned line-by-line and valid
// canonical-JSON entries are loaded into the in-memory state in order. The
// founder is inferred from the first entry (the genesis).
//
// Malformed lines are logged and skipped — the on-disk file is append-only
// and a crash mid-write could truncate the last line, so skipping preserves
// recoverability. The function returns an error only if the file itself
// cannot be opened for append after loading.
//
// This implementation deliberately does not share code with
// pkg/entmoot/store even though the two packages follow the same
// base64url-named directory pattern. Changes here do not affect the message
// store and vice versa.
func OpenJSONL(root string, groupID entmoot.GroupID) (*RosterLog, error) {
	if root == "" {
		return nil, errors.New("roster: OpenJSONL root path is empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("roster: resolve root %q: %w", root, err)
	}
	groupDir := filepath.Join(absRoot, "groups", encodeGroupDirName(groupID))
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		return nil, fmt.Errorf("roster: mkdir %q: %w", groupDir, err)
	}
	path := filepath.Join(groupDir, rosterFileName)

	r := New(groupID)
	// Hydrate from disk before we open the append-mode handle: we don't want
	// a partial load to leave the file truncated/locked.
	if err := loadInto(r, path); err != nil {
		return nil, fmt.Errorf("roster: load %q: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("roster: open %q: %w", path, err)
	}

	// persist appends a canonical-encoded entry followed by '\n' and fsyncs
	// before returning. Called from Apply and Genesis under r.mu.
	r.persist = func(entry entmoot.RosterEntry) error {
		encoded, err := canonical.Encode(entry)
		if err != nil {
			return fmt.Errorf("canonical encode: %w", err)
		}
		line := append(append(make([]byte, 0, len(encoded)+1), encoded...), '\n')
		if _, err := f.Write(line); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync: %w", err)
		}
		return nil
	}
	r.closeFn = f.Close
	return r, nil
}

// encodeGroupDirName mirrors pkg/entmoot/store's base64.RawURLEncoding convention.
// Kept as a private helper here so the two packages don't depend on each
// other.
func encodeGroupDirName(gid entmoot.GroupID) string {
	return base64.RawURLEncoding.EncodeToString(gid[:])
}

// loadInto scans path line-by-line and applies valid canonical JSON entries
// into r's state. Missing files are treated as an empty log. Malformed lines
// are logged and skipped. The first entry in the file is adopted as the
// founder record.
func loadInto(r *RosterLog, path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxLine = 16 << 20
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxLine)

	lineNo := 0
	for scanner.Scan() {
		lineNo++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			r.logger.Warn("roster: skipping malformed jsonl line",
				slog.String("path", path),
				slog.Int("line", lineNo),
				slog.String("err", err.Error()))
			continue
		}
		// First entry is the genesis: record the founder directly; we do
		// not re-validate the on-disk log against itself here (validation
		// happens at Apply time, before persist).
		if len(r.entries) == 0 {
			r.founder = entry.Subject
		}
		r.applyLocked(entry)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("scan: %w", err)
	}
	return nil
}
