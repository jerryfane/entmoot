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
func TestGroupVisibilityHonoursTheLegacyFleetControlMarker(t *testing.T) {
	for _, tc := range []struct {
		name     string
		meta     map[string]interface{}
		wantList bool
	}{
		{name: "no metadata", meta: nil, wantList: true},
		{name: "ordinary group", meta: map[string]interface{}{"name": "moot"}, wantList: true},
		{name: "explicitly hidden", meta: map[string]interface{}{"hidden": true}, wantList: false},
		{name: "legacy fleet control", meta: map[string]interface{}{"name": "control", "fleet_control": true}, wantList: false},
		{name: "fleet control false", meta: map[string]interface{}{"fleet_control": false}, wantList: true},
		{name: "fleet control not a bool", meta: map[string]interface{}{"fleet_control": "true"}, wantList: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := groupVisibleForList(tc.meta, esphttp.GroupListOptions{}); got != tc.wantList {
				t.Fatalf("visible = %v, want %v", got, tc.wantList)
			}
			// IncludeHidden is the operator escape hatch and must still show
			// everything, or a hidden group could never be inspected.
			if got := groupVisibleForList(tc.meta, esphttp.GroupListOptions{IncludeHidden: true}); !got {
				t.Fatal("visible with IncludeHidden = false, want true")
			}
		})
	}
}
