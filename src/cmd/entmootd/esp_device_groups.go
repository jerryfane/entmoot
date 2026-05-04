package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

type deviceGroupAuthorizer interface {
	GrantDeviceGroup(context.Context, string, entmoot.GroupID) (bool, error)
	RevokeDeviceGroup(context.Context, string, entmoot.GroupID) error
	GrantDeviceAdminGroup(context.Context, string, entmoot.GroupID) (bool, error)
	RevokeDeviceAdminGroup(context.Context, string, entmoot.GroupID) error
	DeviceAllowsGroup(context.Context, string, entmoot.GroupID) (bool, error)
}

type fileBackedDeviceGroupAuthorizer struct {
	path     string
	registry *esphttp.DeviceRegistry
	mu       sync.Mutex
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
	a.mu.Lock()
	defer a.mu.Unlock()
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

func (a *fileBackedDeviceGroupAuthorizer) update(deviceID string, gid entmoot.GroupID, grant bool) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var (
		next    *esphttp.DeviceRegistry
		changed bool
		err     error
	)
	if grant {
		next, changed, err = a.registry.WithGroupGranted(deviceID, gid)
	} else {
		next, changed, err = a.registry.WithGroupRevoked(deviceID, gid)
	}
	if err != nil || !changed {
		return false, err
	}
	if err := esphttp.SaveDeviceRegistry(a.path, next); err != nil {
		return false, err
	}
	a.registry.Replace(next)
	return true, nil
}

func (a *fileBackedDeviceGroupAuthorizer) updateAdmin(deviceID string, gid entmoot.GroupID, grant bool) (bool, error) {
	if a == nil || a.registry == nil {
		return false, fmt.Errorf("esp device group authorizer is not configured")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var (
		next    *esphttp.DeviceRegistry
		changed bool
		err     error
	)
	if grant {
		next, changed, err = a.registry.WithAdminGroupGranted(deviceID, gid)
	} else {
		next, changed, err = a.registry.WithAdminGroupRevoked(deviceID, gid)
	}
	if err != nil || !changed {
		return false, err
	}
	if err := esphttp.SaveDeviceRegistry(a.path, next); err != nil {
		return false, err
	}
	a.registry.Replace(next)
	return true, nil
}
