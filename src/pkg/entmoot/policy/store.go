package policy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"entmoot/pkg/entmoot"
)

const (
	storeVersion = 1
	storeDir     = "policies"
	storeFile    = "group-policies.json"
	lockFile     = "group-policies.lock"
)

// FileStore persists group policies under the local Entmoot data root.
type FileStore struct {
	path     string
	lockPath string
	mu       sync.Mutex
}

// OpenFileStore returns a policy store rooted at dataRoot.
func OpenFileStore(dataRoot string) (*FileStore, error) {
	if dataRoot == "" {
		return nil, errors.New("policy: data root is empty")
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return nil, fmt.Errorf("policy: resolve data root %q: %w", dataRoot, err)
	}
	dir := filepath.Join(root, storeDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("policy: mkdir %q: %w", dir, err)
	}
	return &FileStore{
		path:     filepath.Join(dir, storeFile),
		lockPath: filepath.Join(dir, lockFile),
	}, nil
}

// Path returns the backing JSON file path.
func (s *FileStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Get returns the stored policy for groupID, if one exists.
func (s *FileStore) Get(ctx context.Context, groupID entmoot.GroupID) (Policy, bool, error) {
	if err := validateStoreCall(ctx, s, groupID); err != nil {
		return Policy{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var (
		p  Policy
		ok bool
	)
	if err := s.withFileLockLocked(false, func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		p, ok = data.Policies[groupID.String()]
		return nil
	}); err != nil {
		return Policy{}, false, err
	}
	return p, ok, nil
}

// Put validates and stores policy for groupID.
func (s *FileStore) Put(ctx context.Context, groupID entmoot.GroupID, policy Policy) error {
	if err := validateStoreCall(ctx, s, groupID); err != nil {
		return err
	}
	if err := policy.Validate(); err != nil {
		return fmt.Errorf("policy: validate: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withFileLockLocked(true, func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		data.Policies[groupID.String()] = policy
		return s.saveLocked(data)
	})
}

// Delete removes any stored policy for groupID.
func (s *FileStore) Delete(ctx context.Context, groupID entmoot.GroupID) error {
	if err := validateStoreCall(ctx, s, groupID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.withFileLockLocked(true, func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		delete(data.Policies, groupID.String())
		return s.saveLocked(data)
	})
}

// List returns a copy of every stored policy keyed by group id.
func (s *FileStore) List(ctx context.Context) (map[entmoot.GroupID]Policy, error) {
	if s == nil {
		return nil, errors.New("policy: nil store")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var out map[entmoot.GroupID]Policy
	if err := s.withFileLockLocked(false, func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		out = make(map[entmoot.GroupID]Policy, len(data.Policies))
		for encoded, p := range data.Policies {
			gid, err := decodeGroupID(encoded)
			if err != nil {
				return err
			}
			out[gid] = p
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

type storeData struct {
	Version  int               `json:"version"`
	Policies map[string]Policy `json:"policies"`
}

func (s *FileStore) loadLocked() (storeData, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return newStoreData(), nil
	}
	if err != nil {
		return storeData{}, fmt.Errorf("policy: read %q: %w", s.path, err)
	}
	var data storeData
	if err := json.Unmarshal(raw, &data); err != nil {
		return storeData{}, fmt.Errorf("policy: decode %q: %w", s.path, err)
	}
	if data.Version == 0 {
		data.Version = storeVersion
	}
	if data.Version != storeVersion {
		return storeData{}, fmt.Errorf("policy: unsupported store version %d", data.Version)
	}
	if data.Policies == nil {
		data.Policies = make(map[string]Policy)
	}
	for encoded, p := range data.Policies {
		if _, err := decodeGroupID(encoded); err != nil {
			return storeData{}, err
		}
		if err := p.Validate(); err != nil {
			return storeData{}, fmt.Errorf("policy: validate group %q: %w", encoded, err)
		}
	}
	return data, nil
}

func (s *FileStore) saveLocked(data storeData) error {
	if data.Version == 0 {
		data.Version = storeVersion
	}
	if data.Policies == nil {
		data.Policies = make(map[string]Policy)
	}
	for encoded, p := range data.Policies {
		if _, err := decodeGroupID(encoded); err != nil {
			return err
		}
		if err := p.Validate(); err != nil {
			return fmt.Errorf("policy: validate group %q: %w", encoded, err)
		}
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("policy: encode: %w", err)
	}
	raw = append(raw, '\n')
	tmp, f, err := createTempPolicyFile(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	n, writeErr := f.Write(raw)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("policy: write temp: %w", writeErr)
	}
	if n != len(raw) {
		_ = os.Remove(tmp)
		return fmt.Errorf("policy: write temp: %w", io.ErrShortWrite)
	}
	if syncErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("policy: sync temp: %w", syncErr)
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("policy: close temp: %w", closeErr)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("policy: replace %q: %w", s.path, err)
	}
	syncDir(filepath.Dir(s.path))
	return nil
}

func (s *FileStore) withFileLockLocked(exclusive bool, fn func() error) error {
	lock, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("policy: open lock %q: %w", s.lockPath, err)
	}
	defer lock.Close()

	how := syscall.LOCK_SH
	if exclusive {
		how = syscall.LOCK_EX
	}
	if err := syscall.Flock(int(lock.Fd()), how); err != nil {
		return fmt.Errorf("policy: lock %q: %w", s.lockPath, err)
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	return fn()
}

func createTempPolicyFile(dir string) (string, *os.File, error) {
	f, err := os.CreateTemp(dir, storeFile+".*.tmp")
	if err != nil {
		return "", nil, fmt.Errorf("policy: create temp in %q: %w", dir, err)
	}
	return f.Name(), f, nil
}

func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	defer f.Close()
	_ = f.Sync()
}

func newStoreData() storeData {
	return storeData{
		Version:  storeVersion,
		Policies: make(map[string]Policy),
	}
}

func validateStoreCall(ctx context.Context, s *FileStore, groupID entmoot.GroupID) error {
	if s == nil {
		return errors.New("policy: nil store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if groupID == (entmoot.GroupID{}) {
		return errors.New("policy: group id is required")
	}
	return nil
}

func decodeGroupID(encoded string) (entmoot.GroupID, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return entmoot.GroupID{}, fmt.Errorf("policy: decode group id %q: %w", encoded, err)
	}
	if len(raw) != 32 {
		return entmoot.GroupID{}, fmt.Errorf("policy: group id %q decoded to %d bytes, want 32", encoded, len(raw))
	}
	var gid entmoot.GroupID
	copy(gid[:], raw)
	return gid, nil
}
