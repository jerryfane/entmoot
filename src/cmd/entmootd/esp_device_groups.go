package main

import (
	"context"
	"fmt"
	"strings"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

type deviceGroupAuthorizer interface {
	GrantDeviceGroup(context.Context, string, entmoot.GroupID) (bool, error)
	RevokeDeviceGroup(context.Context, string, entmoot.GroupID) error
	GrantDeviceAdminGroup(context.Context, string, entmoot.GroupID) (bool, error)
	RevokeDeviceAdminGroup(context.Context, string, entmoot.GroupID) error
	DeviceAllowsGroup(context.Context, string, entmoot.GroupID) (bool, error)
	BindDeviceIdentity(context.Context, string, entmoot.MemberID, string, []byte) (bool, error)
}

// fileBackedDeviceGroupAuthorizer persists operator-approved grants. Every
// write goes through DeviceRegistry.Update, the same serialized cycle member
// self-enrollment uses, so neither writer can drop the other's change.
type fileBackedDeviceGroupAuthorizer struct {
	path     string
	registry *esphttp.DeviceRegistry
}

func (a *fileBackedDeviceGroupAuthorizer) GrantDeviceGroup(_ context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	return a.update(deviceID, gid, true)
}

func (a *fileBackedDeviceGroupAuthorizer) RevokeDeviceGroup(_ context.Context, deviceID string, gid entmoot.GroupID) error {
	_, err := a.update(deviceID, gid, false)
	return err
}

func (a *fileBackedDeviceGroupAuthorizer) GrantDeviceAdminGroup(_ context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	return a.updateAdmin(deviceID, gid, true)
}

func (a *fileBackedDeviceGroupAuthorizer) RevokeDeviceAdminGroup(_ context.Context, deviceID string, gid entmoot.GroupID) error {
	_, err := a.updateAdmin(deviceID, gid, false)
	return err
}

func (a *fileBackedDeviceGroupAuthorizer) DeviceAllowsGroup(_ context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return false, fmt.Errorf("esp device id is required")
	}
	for _, device := range a.registry.Snapshot() {
		if device.ID != deviceID {
			continue
		}
		for _, allowed := range device.Groups {
			if allowed == gid {
				return true, nil
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("esp device %q not found", deviceID)
}

func (a *fileBackedDeviceGroupAuthorizer) BindDeviceIdentity(_ context.Context, deviceID string, memberID entmoot.MemberID, peerID string, entmootPubKey []byte) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	return a.registry.Update(a.path, func(current *esphttp.DeviceRegistry) (*esphttp.DeviceRegistry, bool, error) {
		return current.WithDeviceIdentity(deviceID, memberID, peerID, entmootPubKey)
	})
}

func (a *fileBackedDeviceGroupAuthorizer) update(deviceID string, gid entmoot.GroupID, grant bool) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	return a.registry.Update(a.path, func(current *esphttp.DeviceRegistry) (*esphttp.DeviceRegistry, bool, error) {
		if grant {
			return current.WithGroupGranted(deviceID, gid)
		}
		return current.WithGroupRevoked(deviceID, gid)
	})
}

func (a *fileBackedDeviceGroupAuthorizer) updateAdmin(deviceID string, gid entmoot.GroupID, grant bool) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	return a.registry.Update(a.path, func(current *esphttp.DeviceRegistry) (*esphttp.DeviceRegistry, bool, error) {
		if grant {
			return current.WithAdminGroupGranted(deviceID, gid)
		}
		return current.WithAdminGroupRevoked(deviceID, gid)
	})
}
