package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
)

// JSONL persists messages as append-only JSON-lines files under:
//
//	<root>/groups/<group_id>/messages.jsonl
//
// One file per group. Each line is the canonical JSON encoding of an
// entmoot.Message (pkg/entmoot/canonical), so what is stored on disk is
// byte-for-byte what would be signed. Writes are line-appended and fsync'd
// before Put returns — we prefer correctness over throughput for v0.
//
// Reads are fully cached in memory after OpenJSONL walks the data root, so
// Get/Has/Range/MerkleRoot operate on the in-memory index and do not hit disk.
//
// # Group directory naming
//
// Group-dir names are base64.RawURLEncoding of the 32-byte GroupID. URL-safe
// base64 is used instead of standard base64 because the latter produces '+',
// '/', and '=' characters that some filesystems disallow or treat specially.
// RawURLEncoding is drop-in filesystem-safe (alphabet is A-Z a-z 0-9 - _, no
// padding).
//
// # Lazy dir creation
//
// <root>/ and <root>/groups/ are created eagerly when OpenJSONL runs. A
// per-group directory is created only on the first Put for that group.
type JSONL struct {
	root      string
	groupsDir string

	mu     sync.RWMutex
	groups map[entmoot.GroupID]*groupState
	logger *slog.Logger
}

// groupState is the in-memory state for one group.
type groupState struct {
	// msgs is the authoritative index of messages keyed by id.
	msgs map[entmoot.MessageID]entmoot.Message
	// file is the open append-mode handle for messages.jsonl. Nil until the
	// first Put; OpenJSONL deliberately does not hold file handles open just
	// to read the initial snapshot.
	file *os.File
}

// OpenJSONL opens (or creates) the data root directory and loads all existing
// group files into memory. The root and <root>/groups/ are created with 0700
// permissions if missing.
//
// Malformed JSONL lines are logged and skipped; they never cause Open to
// fail. This is deliberate: the file is append-only and a partial write on
// crash could leave a truncated last line. Skipping preserves recoverability.
func OpenJSONL(root string) (*JSONL, error) {
	if root == "" {
		return nil, errors.New("store: JSONL root path is empty")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("store: resolve root %q: %w", root, err)
	}
	if err := os.MkdirAll(absRoot, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir root %q: %w", absRoot, err)
	}
	groupsDir := filepath.Join(absRoot, "groups")
	if err := os.MkdirAll(groupsDir, 0o700); err != nil {
		return nil, fmt.Errorf("store: mkdir groups %q: %w", groupsDir, err)
	}

	s := &JSONL{
		root:      absRoot,
		groupsDir: groupsDir,
		groups:    make(map[entmoot.GroupID]*groupState),
		logger:    slog.Default(),
	}

	entries, err := os.ReadDir(groupsDir)
	if err != nil {
		return nil, fmt.Errorf("store: readdir %q: %w", groupsDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		gid, ok := decodeGroupDirName(e.Name())
		if !ok {
			s.logger.Warn("store: skipping unrecognized group dir",
				slog.String("name", e.Name()),
				slog.String("root", absRoot))
			continue
		}
		gs, err := loadGroup(s.logger, filepath.Join(groupsDir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("store: load group %q: %w", e.Name(), err)
		}
		s.groups[gid] = gs
	}
	return s, nil
}

// Put implements MessageStore.Put. Writes the canonical encoding of m as a
// new line in messages.jsonl and fsyncs the file before returning.
func (s *JSONL) Put(_ context.Context, m entmoot.Message) error {
	if isZeroGroupID(m.GroupID) {
		return fmt.Errorf("%w: zero group id", ErrInvalidMessage)
	}
	if isZeroMessageID(m.ID) {
		return fmt.Errorf("%w: zero message id", ErrInvalidMessage)
	}

	encoded, err := canonical.Encode(m)
	if err != nil {
		return fmt.Errorf("store: canonical encode: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	gs, ok := s.groups[m.GroupID]
	if !ok {
		gs = &groupState{msgs: make(map[entmoot.MessageID]entmoot.Message)}
		s.groups[m.GroupID] = gs
	}
	if _, exists := gs.msgs[m.ID]; exists {
		// Idempotent: neither cache nor disk touched on duplicate Put.
		return nil
	}

	if gs.file == nil {
		dir := filepath.Join(s.groupsDir, encodeGroupDirName(m.GroupID))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("store: mkdir group %q: %w", dir, err)
		}
		path := filepath.Join(dir, "messages.jsonl")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("store: open %q: %w", path, err)
		}
		gs.file = f
	}

	line := append(append(make([]byte, 0, len(encoded)+1), encoded...), '\n')
	if _, err := gs.file.Write(line); err != nil {
		return fmt.Errorf("store: write: %w", err)
	}
	if err := gs.file.Sync(); err != nil {
		return fmt.Errorf("store: fsync: %w", err)
	}
	gs.msgs[m.ID] = m
	return nil
}

// Get implements MessageStore.Get.
func (s *JSONL) Get(_ context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gs, ok := s.groups[groupID]
	if !ok {
		return entmoot.Message{}, ErrNotFound
	}
	m, ok := gs.msgs[id]
	if !ok {
		return entmoot.Message{}, ErrNotFound
	}
	return m, nil
}

// Has implements MessageStore.Has.
func (s *JSONL) Has(_ context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gs, ok := s.groups[groupID]
	if !ok {
		return false, nil
	}
	_, ok = gs.msgs[id]
	return ok, nil
}

// Range implements MessageStore.Range.
func (s *JSONL) Range(_ context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error) {
	s.mu.RLock()
	gs := s.groups[groupID]
	var candidates []entmoot.Message
	if gs != nil {
		candidates = make([]entmoot.Message, 0, len(gs.msgs))
		for _, m := range gs.msgs {
			if m.Timestamp < sinceMillis {
				continue
			}
			if untilMillis != 0 && m.Timestamp >= untilMillis {
				continue
			}
			candidates = append(candidates, m)
		}
	}
	s.mu.RUnlock()

	return topoOrder(candidates)
}

// IterMessageIDsInIDRange implements MessageStore.IterMessageIDsInIDRange.
// Scans the in-memory index under the read lock, filters by byte-range, and
// returns byte-sorted ascending. JSONL is test-only, so perf is unimportant.
func (s *JSONL) IterMessageIDsInIDRange(_ context.Context, groupID entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	hiUnbounded := isZeroMessageID(hiID)

	s.mu.RLock()
	gs := s.groups[groupID]
	var out []entmoot.MessageID
	if gs != nil {
		out = make([]entmoot.MessageID, 0, len(gs.msgs))
		for id := range gs.msgs {
			if bytes.Compare(id[:], loID[:]) < 0 {
				continue
			}
			if !hiUnbounded && bytes.Compare(id[:], hiID[:]) >= 0 {
				continue
			}
			out = append(out, id)
		}
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out, nil
}

// MerkleRoot implements MessageStore.MerkleRoot.
func (s *JSONL) MerkleRoot(_ context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	s.mu.RLock()
	gs := s.groups[groupID]
	var all []entmoot.Message
	if gs != nil {
		all = make([]entmoot.Message, 0, len(gs.msgs))
		for _, m := range gs.msgs {
			all = append(all, m)
		}
	}
	s.mu.RUnlock()

	return merkleRootOf(all)
}

// Close closes any open file handles. Safe to call multiple times.
func (s *JSONL) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for _, gs := range s.groups {
		if gs.file == nil {
			continue
		}
		if err := gs.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		gs.file = nil
	}
	return firstErr
}

// encodeGroupDirName returns the on-disk directory name for gid.
func encodeGroupDirName(gid entmoot.GroupID) string {
	return base64.RawURLEncoding.EncodeToString(gid[:])
}

// decodeGroupDirName parses a directory name back into a GroupID. The second
// return is false when the name is not a valid raw-url-base64 encoding of
// exactly 32 bytes.
func decodeGroupDirName(name string) (entmoot.GroupID, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(name)
	if err != nil {
		return entmoot.GroupID{}, false
	}
	if len(raw) != 32 {
		return entmoot.GroupID{}, false
	}
	var gid entmoot.GroupID
	copy(gid[:], raw)
	return gid, true
}

// loadGroup reads messages.jsonl under dir and materializes it into a
// groupState. Malformed lines are logged and skipped; missing files produce
// an empty state. The returned groupState has a nil file handle — Put opens
// it lazily on first write.
func loadGroup(logger *slog.Logger, dir string) (*groupState, error) {
	gs := &groupState{msgs: make(map[entmoot.MessageID]entmoot.Message)}
	path := filepath.Join(dir, "messages.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return gs, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Raise the line cap to allow reasonably large message payloads. The wire
	// layer caps frames at 16 MiB; match that here.
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
		var m entmoot.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			logger.Warn("store: skipping malformed jsonl line",
				slog.String("path", path),
				slog.Int("line", lineNo),
				slog.String("err", err.Error()))
			continue
		}
		if isZeroGroupID(m.GroupID) || isZeroMessageID(m.ID) {
			logger.Warn("store: skipping jsonl line with zero id(s)",
				slog.String("path", path),
				slog.Int("line", lineNo))
			continue
		}
		gs.msgs[m.ID] = m
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("store: scan %q: %w", path, err)
	}
	return gs, nil
}
