package esphttp

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"entmoot/pkg/entmoot"
)

type deviceRegistryFile struct {
	Devices []deviceFile `json:"devices"`
}

type deviceFile struct {
	ID        string   `json:"id"`
	PublicKey string   `json:"public_key"`
	Groups    []string `json:"groups"`
	ClientIDs []string `json:"client_ids"`
	Disabled  bool     `json:"disabled"`
}

// LoadDeviceRegistry reads the local ESP device registry JSON file.
func LoadDeviceRegistry(path string) (*DeviceRegistry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("esphttp: read device registry %s: %w", path, err)
	}
	var f deviceRegistryFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("esphttp: parse device registry %s: %w", path, err)
	}
	devices := make([]Device, 0, len(f.Devices))
	for _, in := range f.Devices {
		pub, err := base64.StdEncoding.DecodeString(in.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("esphttp: device %q public_key: %w", in.ID, err)
		}
		if len(pub) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("esphttp: device %q public_key length %d", in.ID, len(pub))
		}
		groups := make([]entmoot.GroupID, 0, len(in.Groups))
		for _, rawGroup := range in.Groups {
			gid, err := decodeGroupID(rawGroup)
			if err != nil {
				return nil, fmt.Errorf("esphttp: device %q group: %w", in.ID, err)
			}
			groups = append(groups, gid)
		}
		devices = append(devices, Device{
			ID:        in.ID,
			PublicKey: append(ed25519.PublicKey(nil), pub...),
			Groups:    groups,
			ClientIDs: append([]string(nil), in.ClientIDs...),
			Disabled:  in.Disabled,
		})
	}
	return NewDeviceRegistry(devices)
}
