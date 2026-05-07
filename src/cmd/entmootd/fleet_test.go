package main

import (
	"context"
	"encoding/json"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
)

func TestFleetListHidesArchivedFleets(t *testing.T) {
	dataDir := t.TempDir()
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	ctx := context.Background()
	for _, rec := range []esphttp.FleetRecord{
		{
			FleetID:             "fleet-active",
			Name:                "Active Fleet",
			Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")},
			CoordinatorDeviceID: "ios-1",
			Status:              esphttp.FleetStatusActive,
			CreatedAtMS:         2,
		},
		{
			FleetID:             "fleet-archived",
			Name:                "Archived Fleet",
			Coordinator:         entmoot.NodeInfo{PilotNodeID: 45491, EntmootPubKey: []byte("coordinator")},
			CoordinatorDeviceID: "ios-1",
			Status:              esphttp.FleetStatusArchived,
			CreatedAtMS:         1,
			ArchivedAtMS:        3,
		},
	} {
		if _, err := state.CreateFleet(ctx, rec); err != nil {
			t.Fatalf("CreateFleet %s: %v", rec.FleetID, err)
		}
	}
	if err := state.Close(); err != nil {
		t.Fatalf("Close state: %v", err)
	}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdFleetList(&globalFlags{data: dataDir}, nil)
	})
	if code != exitOK {
		t.Fatalf("fleet list exit = %d stderr=%s", code, stderr)
	}
	var out struct {
		Fleets []esphttp.FleetRecord `json:"fleets"`
	}
	if err := json.Unmarshal([]byte(stdout), &out); err != nil {
		t.Fatalf("fleet list JSON: %v\nstdout=%s", err, stdout)
	}
	if len(out.Fleets) != 1 || out.Fleets[0].FleetID != "fleet-active" {
		t.Fatalf("fleet list fleets = %+v, want only active fleet", out.Fleets)
	}
}
