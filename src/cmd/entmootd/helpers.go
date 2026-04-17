package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/transport/pilot"
)

// setupResult carries resources assembled by setup() — every non-info-only
// subcommand needs identity + data dir; Pilot is opened only by callers
// that actually need a Transport.
type setupResult struct {
	identity *keystore.Identity
	dataDir  string
}

// setup performs the common boilerplate: mkdir the data root, load or
// generate the identity. Subcommands that need Pilot dial it separately.
func setup(gf *globalFlags) (*setupResult, error) {
	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data %q: %w", gf.data, err)
	}
	id, err := keystore.LoadOrGenerate(gf.identity)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}
	return &setupResult{identity: id, dataDir: gf.data}, nil
}

// expandHome returns path with a leading "~" or "~/" expanded to the
// current user's home directory. Any other path is returned unchanged.
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

// parseLogLevel maps a cli string into an slog.Level. Unknown strings
// default to slog.LevelInfo with an error return so callers can decide
// whether to abort or warn.
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

// decodeGroupID parses the base64 representation of a GroupID. Accepts
// both std and raw-std encodings so users can paste either.
func decodeGroupID(s string) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	s = strings.TrimSpace(s)
	if s == "" {
		return gid, errors.New("empty group id")
	}
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
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

// parseTopicList splits "a,b,c" into a trimmed slice. Empty input yields
// nil.
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

// groupsDir returns the parent directory under which per-group subdirs
// live, matching the on-disk layout used by store/sqlite.go and
// roster/jsonl.go (both use <dataRoot>/groups/<base64url(gid)>/).
func groupsDir(dataRoot string) string {
	return filepath.Join(dataRoot, "groups")
}

// controlSocketPath returns the canonical control-socket path under the
// given data root.
func controlSocketPath(dataRoot string) string {
	return filepath.Join(dataRoot, "control.sock")
}

// listGroupIDs scans dataRoot/groups/ and returns every GroupID whose
// directory name decodes cleanly. Missing or empty parent dirs return
// nil.
//
// Naming convention is base64.RawURLEncoding of the 32-byte GroupID.
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
			if logger != nil {
				logger.Warn("skipping unrecognized group dir", slog.String("name", e.Name()))
			}
			continue
		}
		var gid entmoot.GroupID
		copy(gid[:], raw)
		out = append(out, gid)
	}
	return out, nil
}

// resolveGroupID picks the target group for publish/tail. If gid is
// non-nil the caller supplied -group. Otherwise it falls back to
// auto-pick: accept only when exactly one group is joined locally.
func resolveGroupID(dataRoot string, gid *entmoot.GroupID, logger *slog.Logger) (entmoot.GroupID, error) {
	if gid != nil {
		return *gid, nil
	}
	gids, err := listGroupIDs(dataRoot, logger)
	if err != nil {
		return entmoot.GroupID{}, err
	}
	switch len(gids) {
	case 0:
		return entmoot.GroupID{}, errors.New("no joined groups; pass -group")
	case 1:
		return gids[0], nil
	default:
		return entmoot.GroupID{}, fmt.Errorf("%d groups joined; pass -group to disambiguate", len(gids))
	}
}

// controlSocketAlive probes path with a short dial timeout. Returns true
// if an accepted connection can be opened within timeout.
func controlSocketAlive(path string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// parseDurationDays extends time.ParseDuration with a best-effort "d"
// suffix. "24h" parses as time.ParseDuration does; "7d" is translated to
// "168h" before dispatch. Returns the parsed duration on success.
func parseDurationDays(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty duration")
	}
	// Translate a trailing "d" suffix to hours. Only the simple form
	// "<N>d" is supported; compound forms like "1d12h" are not (and are
	// not expected to matter for invite TTLs).
	if strings.HasSuffix(s, "d") {
		num := strings.TrimSuffix(s, "d")
		days, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid days %q: %w", s, err)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d, nil
}

// encodeBase64 returns the standard base64 (with padding) representation
// of b.
func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// openPilot dials the Pilot daemon and binds the listen port. Every
// subcommand that needs a Pilot transport goes through this single
// helper so the Config is consistent.
func openPilot(gf *globalFlags) (*pilot.Transport, error) {
	return pilot.Open(pilot.Config{
		SocketPath: gf.socket,
		ListenPort: uint16(gf.listenPort),
		Logger:     slog.Default(),
	})
}

// withTimeout wraps context.WithTimeout and returns both the ctx and
// its cancel func; it's the same as the stdlib but saves a repeated
// import in every subcommand.
func withTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
