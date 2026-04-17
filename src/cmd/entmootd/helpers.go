package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"entmoot/pkg/entmoot"
)

// expandHome returns path with a leading "~" or "~/" expanded to the current
// user's home directory. Any other path is returned unchanged. On platforms
// where os.UserHomeDir fails the input is returned as-is with a warning-shaped
// error surfaced to the caller.
func expandHome(path string) (string, error) {
	if path == "" {
		return path, nil
	}
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path, fmt.Errorf("expand ~: %w", err)
	}
	if path == "~" {
		return home, nil
	}
	return filepath.Join(home, path[2:]), nil
}

// parseLogLevel maps a cli string into an slog.Level. Unknown strings default
// to slog.LevelInfo with an error return so callers can decide whether to
// abort or warn.
func parseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "err":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unknown log level %q", s)
	}
}

// decodeGroupID parses the base64 representation of a GroupID. Accepts both
// std and raw-std encodings so users can paste either.
func decodeGroupID(s string) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	s = strings.TrimSpace(s)
	if s == "" {
		return gid, errors.New("empty group id")
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		// Try raw (no padding) as a fallback.
		raw, err = base64.RawStdEncoding.DecodeString(s)
		if err != nil {
			return gid, fmt.Errorf("group id: %w", err)
		}
	}
	if len(raw) != 32 {
		return gid, fmt.Errorf("group id: expected 32 bytes, got %d", len(raw))
	}
	copy(gid[:], raw)
	return gid, nil
}

// parsePeerList parses "123,456,789" into a slice of NodeIDs. Empty string
// yields nil with no error. Whitespace around entries is trimmed.
func parsePeerList(s string) ([]entmoot.NodeID, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]entmoot.NodeID, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("peer %q: %w", p, err)
		}
		out = append(out, entmoot.NodeID(uint32(n)))
	}
	return out, nil
}

// parseTopicList splits "a,b,c" into a trimmed slice. Empty input yields nil.
func parseTopicList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// groupsDir returns the parent directory under which per-group subdirs live,
// matching the on-disk layout used by store/jsonl.go and roster/jsonl.go
// (both use <dataRoot>/groups/<base64url(gid)>/).
func groupsDir(dataRoot string) string {
	return filepath.Join(dataRoot, "groups")
}

// listGroupIDs scans dataRoot/groups/ and returns every GroupID whose
// directory name decodes cleanly. Missing or empty parent dirs return nil.
//
// The directory-naming convention is base64.RawURLEncoding of the 32-byte
// GroupID (see pkg/entmoot/store/jsonl.go#encodeGroupDirName). Unrecognized
// names are skipped with slog.Warn.
func listGroupIDs(dataRoot string, logger *slog.Logger) ([]entmoot.GroupID, error) {
	dir := groupsDir(dataRoot)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	var out []entmoot.GroupID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		raw, err := base64.RawURLEncoding.DecodeString(e.Name())
		if err != nil || len(raw) != 32 {
			logger.Warn("skipping unrecognized group dir", slog.String("name", e.Name()))
			continue
		}
		var gid entmoot.GroupID
		copy(gid[:], raw)
		out = append(out, gid)
	}
	return out, nil
}
