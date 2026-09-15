package main

import (
	"testing"

	"entmoot/pkg/entmoot/esphttp"
)

// TestGroupVisibilityHonoursTheLegacyFleetControlMarker pins a surface the
// Fleet removal must not change.
//
// The removed fleet-create path wrote `fleet_control: true` on the group it
// used as a control channel, and that marker kept the group out of
// GET /v1/groups and so out of the phone's moot list. Nothing migrates or
// clears the metadata on an upgraded install, so dropping the check would make
// those groups appear in a user's list — a visible change to a surface with
// nothing to do with the feature.
//
// The marker is deliberately NOT subject to include_hidden, which is how it
// behaved before the removal: `hidden` is a group opting out of listings and an
// operator can ask past it, while the control marker hid the group from every
// listing.
func TestGroupVisibilityHonoursTheLegacyFleetControlMarker(t *testing.T) {
	for _, tc := range []struct {
		name              string
		meta              map[string]interface{}
		wantDefault       bool
		wantIncludeHidden bool
	}{
		{name: "no metadata", meta: nil, wantDefault: true, wantIncludeHidden: true},
		{name: "ordinary group", meta: map[string]interface{}{"name": "moot"}, wantDefault: true, wantIncludeHidden: true},
		{name: "explicitly hidden", meta: map[string]interface{}{"hidden": true}, wantDefault: false, wantIncludeHidden: true},
		{name: "legacy fleet control", meta: map[string]interface{}{"name": "control", "fleet_control": true}, wantDefault: false, wantIncludeHidden: false},
		{name: "hidden and control", meta: map[string]interface{}{"hidden": true, "fleet_control": true}, wantDefault: false, wantIncludeHidden: false},
		{name: "fleet control false", meta: map[string]interface{}{"fleet_control": false}, wantDefault: true, wantIncludeHidden: true},
		{name: "fleet control not a bool", meta: map[string]interface{}{"fleet_control": "true"}, wantDefault: true, wantIncludeHidden: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := groupVisibleForList(tc.meta, esphttp.GroupListOptions{}); got != tc.wantDefault {
				t.Fatalf("visible = %v, want %v", got, tc.wantDefault)
			}
			if got := groupVisibleForList(tc.meta, esphttp.GroupListOptions{IncludeHidden: true}); got != tc.wantIncludeHidden {
				t.Fatalf("visible with IncludeHidden = %v, want %v", got, tc.wantIncludeHidden)
			}
		})
	}
}
