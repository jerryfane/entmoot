package esphttp

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"entmoot/pkg/entmoot"
)

// DeviceRegistryDocument is the stable JSON projection stored in
// esp-devices.json and printed by the operator CLI.
type DeviceRegistryDocument struct {
	Devices []DeviceRecord `json:"devices"`
}

// DeviceRecord is one serializable ESP device registry entry.
type DeviceRecord struct {
	ID        string   `json:"id"`
	PublicKey string   `json:"public_key"`
	Groups    []string `json:"groups"`
	ClientIDs []string `json:"client_ids"`
	Disabled  bool     `json:"disabled"`
}

// LoadDeviceRegistryOrEmpty reads path, returning an empty registry when the
// file does not exist.
func LoadDeviceRegistryOrEmpty(path string) (*DeviceRegistry, error) {
	reg, err := LoadDeviceRegistry(path)
	if err == nil {
		return reg, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return NewDeviceRegistry(nil)
	}
	return nil, err
}

// LoadDeviceRegistry reads the local ESP device registry JSON file.
func LoadDeviceRegistry(path string) (*DeviceRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("esphttp: read device registry %s: %w", path, err)
	}
	var f DeviceRegistryDocument
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("esphttp: parse device registry %s: %w", path, err)
	}
	devices := make([]Device, 0, len(f.Devices))
	for _, in := range f.Devices {
		device, err := DeviceFromRecord(in)
		if err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return NewDeviceRegistry(devices)
}

// SaveDeviceRegistry atomically writes the local ESP device registry JSON file.
func SaveDeviceRegistry(path string, reg *DeviceRegistry) error {
	if reg == nil {
		var err error
		reg, err = NewDeviceRegistry(nil)
		if err != nil {
			return err
		}
	}
	// Re-validate before serializing so callers cannot persist malformed in-memory
	// entries constructed outside LoadDeviceRegistry.
	validated, err := NewDeviceRegistry(reg.Snapshot())
	if err != nil {
		return err
	}
	doc := DeviceRegistryDocumentFromRegistry(validated)
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("esphttp: marshal device registry %s: %w", path, err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("esphttp: create device registry dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".esp-devices-*.json")
	if err != nil {
		return fmt.Errorf("esphttp: create temp device registry %s: %w", dir, err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("esphttp: chmod temp device registry %s: %w", tmpPath, err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("esphttp: write temp device registry %s: %w", tmpPath, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("esphttp: sync temp device registry %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("esphttp: close temp device registry %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("esphttp: replace device registry %s: %w", path, err)
	}
	cleanup = false
	syncDir(dir)
	return nil
}

// DeviceRegistryDocumentFromRegistry converts a registry into its stable JSON
// projection.
func DeviceRegistryDocumentFromRegistry(reg *DeviceRegistry) DeviceRegistryDocument {
	if reg == nil {
		return DeviceRegistryDocument{Devices: []DeviceRecord{}}
	}
	snapshot := reg.Snapshot()
	devices := make([]DeviceRecord, 0, len(snapshot))
	for _, d := range snapshot {
		groups := make([]string, 0, len(d.Groups))
		for _, gid := range d.Groups {
			groups = append(groups, gid.String())
		}
		devices = append(devices, DeviceRecord{
			ID:        d.ID,
			PublicKey: base64.StdEncoding.EncodeToString(d.PublicKey),
			Groups:    groups,
			ClientIDs: append([]string(nil), d.ClientIDs...),
			Disabled:  d.Disabled,
		})
	}
	return DeviceRegistryDocument{Devices: devices}
}

// DeviceFromRecord validates and converts one serializable registry entry.
func DeviceFromRecord(in DeviceRecord) (Device, error) {
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return Device{}, fmt.Errorf("esphttp: device id is required")
	}
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(in.PublicKey))
	if err != nil {
		return Device{}, fmt.Errorf("esphttp: device %q public_key: %w", id, err)
	}
	if len(pub) != ed25519.PublicKeySize {
		return Device{}, fmt.Errorf("esphttp: device %q public_key length %d", id, len(pub))
	}
	groups := make([]entmoot.GroupID, 0, len(in.Groups))
	for _, rawGroup := range in.Groups {
		gid, err := decodeGroupID(strings.TrimSpace(rawGroup))
		if err != nil {
			return Device{}, fmt.Errorf("esphttp: device %q group: %w", id, err)
		}
		groups = append(groups, gid)
	}
	clients := make([]string, 0, len(in.ClientIDs))
	for _, clientID := range in.ClientIDs {
		clientID = strings.TrimSpace(clientID)
		if clientID == "" {
			return Device{}, fmt.Errorf("esphttp: device %q client id is required", id)
		}
		clients = append(clients, clientID)
	}
	return Device{
		ID:        id,
		PublicKey: append(ed25519.PublicKey(nil), pub...),
		Groups:    groups,
		ClientIDs: clients,
		Disabled:  in.Disabled,
	}, nil
}

func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	defer f.Close()
	_ = f.Sync()
}
