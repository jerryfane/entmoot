package roster

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

func readAndValidateLegacy(path string, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.Size() > 0 {
		var final [1]byte
		if _, err := f.ReadAt(final[:], info.Size()-1); err != nil {
			return nil, fmt.Errorf("read final byte: %w", err)
		}
		if final[0] != '\n' {
			return nil, errors.New("truncated legacy log: final entry has no newline")
		}
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind: %w", err)
	}

	candidate := newChain(groupID)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	var entries []entmoot.RosterEntry
	for lineNo := 1; scanner.Scan(); lineNo++ {
		raw := append([]byte(nil), scanner.Bytes()...)
		if len(raw) == 0 {
			return nil, fmt.Errorf("line %d: empty entry", lineNo)
		}
		var entry entmoot.RosterEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("line %d: malformed JSON: %w", lineNo, err)
		}
		canonicalBytes, err := canonical.Encode(entry)
		if err != nil {
			return nil, fmt.Errorf("line %d: canonical encode: %w", lineNo, err)
		}
		if !bytes.Equal(canonicalBytes, raw) {
			return nil, fmt.Errorf("line %d: entry is not exact canonical JSON", lineNo)
		}
		if len(candidate.entries) == 0 {
			err = validateGenesis(entry, groupID)
			if err == nil {
				candidate.founder = entry.Subject
				candidate.apply(entry)
			}
		} else {
			err = candidate.validate(entry)
			if err == nil {
				candidate.apply(entry)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return entries, nil
}

// ValidateLegacyJSONL validates an immutable legacy roster without importing
// or modifying it. Conversion uses this during preflight.
func ValidateLegacyJSONL(path string, groupID entmoot.GroupID) ([]entmoot.RosterEntry, error) {
	return readAndValidateLegacy(path, groupID)
}

// ValidateEntries verifies a decoded roster chain in order without persisting
// it. The caller remains responsible for checking its stored canonical bytes.
func ValidateEntries(groupID entmoot.GroupID, entries []entmoot.RosterEntry) error {
	candidate := newChain(groupID)
	for i, entry := range entries {
		var err error
		if len(candidate.entries) == 0 {
			err = validateGenesis(entry, groupID)
			if err == nil {
				candidate.founder = entry.Subject
				candidate.apply(entry)
			}
		} else {
			err = candidate.validate(entry)
			if err == nil {
				candidate.apply(entry)
			}
		}
		if err != nil {
			return fmt.Errorf("entry %d: %w", i+1, err)
		}
	}
	return nil
}
