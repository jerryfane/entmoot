package esphttp

import (
	"context"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestLiveTopicMatches(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		want   bool
	}{
		{filter: "story/collab/#", topic: "story/collab/sky-city", want: true},
		{filter: "story/+/sky-city", topic: "story/collab/sky-city", want: true},
		{filter: "story/+", topic: "story/collab/sky-city", want: false},
		{filter: "chat", topic: "chat", want: true},
		{filter: "chat", topic: "other/chat", want: false},
		{filter: "chat/#/bad", topic: "chat/a/bad", want: false},
	}
	for _, tc := range tests {
		if got := LiveTopicMatches(tc.filter, tc.topic); got != tc.want {
			t.Fatalf("LiveTopicMatches(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.want)
		}
	}
}

func TestLiveAgentStateExpiresLease(t *testing.T) {
	gid := testLiveGroupID(1)
	states := LiveAgentStatesByNode([]LiveAgentConfig{{
		GroupID:      gid,
		NodeID:       7,
		Enabled:      true,
		Mode:         LiveModeConverse,
		TopicFilters: []string{"story/#"},
		UpdatedAtMS:  10,
	}}, []LiveAgentPresence{{
		GroupID:      gid,
		NodeID:       7,
		Status:       LiveStatusOnline,
		Mode:         LiveModeConverse,
		TopicFilters: []string{"story/#"},
		LastSeenAtMS: 20,
		LeaseUntilMS: 30,
		UpdatedAtMS:  20,
	}}, 31)
	if states[7].Status != LiveStatusOffline {
		t.Fatalf("expired live status = %q, want offline", states[7].Status)
	}
}

func TestLiveAgentConfigPersists(t *testing.T) {
	ctx := context.Background()
	gid := testLiveGroupID(2)
	for _, tc := range []struct {
		name  string
		store StateStore
	}{
		{name: "memory", store: NewMemoryStateStore()},
		{name: "sqlite", store: mustOpenLiveStateStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := tc.store.UpsertLiveAgentConfig(ctx, LiveAgentConfig{
				GroupID:           gid,
				NodeID:            9,
				Enabled:           true,
				Mode:              LiveModeOperator,
				TopicFilters:      []string{"story/#", "story/#", "chat"},
				MaxActionsPerScan: 3,
				MaxActionBytes:    128,
			})
			if err != nil {
				t.Fatalf("UpsertLiveAgentConfig: %v", err)
			}
			if len(cfg.AllowedActions) != len(DefaultLiveActions()) {
				t.Fatalf("operator default actions = %d, want %d", len(cfg.AllowedActions), len(DefaultLiveActions()))
			}
			if len(cfg.TopicFilters) != 2 {
				t.Fatalf("topic filters = %v, want deduped two filters", cfg.TopicFilters)
			}
			if cfg.MaxActionsPerScan != 3 || cfg.MaxActionBytes != 128 {
				t.Fatalf("spam controls = %d/%d, want 3/128", cfg.MaxActionsPerScan, cfg.MaxActionBytes)
			}
			presence, err := tc.store.UpsertLiveAgentPresence(ctx, LiveAgentPresence{
				GroupID:      gid,
				NodeID:       9,
				Status:       LiveStatusOnline,
				Mode:         LiveModeOperator,
				TopicFilters: []string{"story/#"},
				LastSeenAtMS: 100,
				LeaseUntilMS: 200,
				UpdatedAtMS:  100,
			})
			if err != nil {
				t.Fatalf("UpsertLiveAgentPresence: %v", err)
			}
			if presence.Status != LiveStatusOnline {
				t.Fatalf("presence status = %q", presence.Status)
			}
			configs, err := tc.store.ListLiveAgentConfigs(ctx, gid)
			if err != nil {
				t.Fatalf("ListLiveAgentConfigs: %v", err)
			}
			nodeConfigs, err := tc.store.ListLiveAgentConfigsForNode(ctx, 9)
			if err != nil {
				t.Fatalf("ListLiveAgentConfigsForNode: %v", err)
			}
			if len(nodeConfigs) != 1 || nodeConfigs[0].GroupID != gid || nodeConfigs[0].NodeID != 9 {
				t.Fatalf("node configs = %+v, want config for node 9 in group %s", nodeConfigs, gid)
			}
			presences, err := tc.store.ListLiveAgentPresence(ctx, gid)
			if err != nil {
				t.Fatalf("ListLiveAgentPresence: %v", err)
			}
			states := LiveAgentStatesByNode(configs, presences, 150)
			if states[9].Status != LiveStatusOnline || states[9].Mode != LiveModeOperator {
				t.Fatalf("live state = %+v", states[9])
			}
			if states[9].MaxActionsPerScan != 3 || states[9].MaxActionBytes != 128 {
				t.Fatalf("live state spam controls = %+v, want 3/128", states[9])
			}
		})
	}
}

func TestListLiveAgentConfigsForNodeOrdersByGroup(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		store StateStore
	}{
		{name: "memory", store: NewMemoryStateStore()},
		{name: "sqlite", store: mustOpenLiveStateStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, gid := range []entmoot.GroupID{testLiveGroupID(9), testLiveGroupID(3), testLiveGroupID(6)} {
				if _, err := tc.store.UpsertLiveAgentConfig(ctx, LiveAgentConfig{
					GroupID:      gid,
					NodeID:       9,
					Enabled:      true,
					Mode:         LiveModeListen,
					TopicFilters: []string{"#"},
				}); err != nil {
					t.Fatalf("UpsertLiveAgentConfig(%s): %v", gid, err)
				}
			}
			configs, err := tc.store.ListLiveAgentConfigsForNode(ctx, 9)
			if err != nil {
				t.Fatalf("ListLiveAgentConfigsForNode: %v", err)
			}
			want := []entmoot.GroupID{testLiveGroupID(3), testLiveGroupID(6), testLiveGroupID(9)}
			if len(configs) != len(want) {
				t.Fatalf("configs = %d, want %d", len(configs), len(want))
			}
			for i, cfg := range configs {
				if cfg.GroupID != want[i] {
					t.Fatalf("configs[%d].GroupID = %s, want %s", i, cfg.GroupID, want[i])
				}
			}
		})
	}
}

func TestLiveAgentConfigRejectsNegativeSpamControls(t *testing.T) {
	ctx := context.Background()
	gid := testLiveGroupID(6)
	for _, tc := range []struct {
		name string
		cfg  LiveAgentConfig
	}{
		{name: "negative actions", cfg: LiveAgentConfig{MaxActionsPerScan: -1}},
		{name: "negative bytes", cfg: LiveAgentConfig{MaxActionBytes: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.GroupID = gid
			cfg.NodeID = 16
			cfg.Enabled = true
			cfg.Mode = LiveModeConverse
			cfg.TopicFilters = []string{"chat"}
			if _, err := NewMemoryStateStore().UpsertLiveAgentConfig(ctx, cfg); err == nil {
				t.Fatal("UpsertLiveAgentConfig accepted negative spam control")
			}
		})
	}
}

func TestLiveAgentCursorPersists(t *testing.T) {
	ctx := context.Background()
	gid := testLiveGroupID(5)
	for _, tc := range []struct {
		name  string
		store StateStore
	}{
		{name: "memory", store: NewMemoryStateStore()},
		{name: "sqlite", store: mustOpenLiveStateStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := tc.store.UpsertLiveAgentCursor(ctx, LiveAgentCursor{
				GroupID:              gid,
				NodeID:               15,
				ScanFloorAtMS:        120,
				LastSeenAtMS:         123,
				LastSeenAuthorNodeID: 16,
				LastSeenMessageID:    testLiveMessageID(17),
				SeenMessageIDs:       []entmoot.MessageID{testLiveMessageID(17), testLiveMessageID(18)},
				UpdatedAtMS:          124,
			})
			if err != nil {
				t.Fatalf("UpsertLiveAgentCursor: %v", err)
			}
			if cursor.LastSeenAtMS != 123 {
				t.Fatalf("cursor.LastSeenAtMS = %d, want 123", cursor.LastSeenAtMS)
			}
			got, ok, err := tc.store.GetLiveAgentCursor(ctx, gid, 15)
			if err != nil || !ok {
				t.Fatalf("GetLiveAgentCursor ok/err = %v/%v", ok, err)
			}
			if got.ScanFloorAtMS != 120 || got.LastSeenAtMS != 123 || got.LastSeenAuthorNodeID != 16 || got.LastSeenMessageID != testLiveMessageID(17) || len(got.SeenMessageIDs) != 2 || got.SeenMessageIDs[1] != testLiveMessageID(18) || got.UpdatedAtMS != 124 {
				t.Fatalf("cursor = %+v", got)
			}
		})
	}
}

func TestLiveAgentConfigRejectsUnknownActions(t *testing.T) {
	ctx := context.Background()
	gid := testLiveGroupID(3)
	for _, tc := range []struct {
		name  string
		store StateStore
	}{
		{name: "memory", store: NewMemoryStateStore()},
		{name: "sqlite", store: mustOpenLiveStateStore(t)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.store.UpsertLiveAgentConfig(ctx, LiveAgentConfig{
				GroupID:        gid,
				NodeID:         11,
				Enabled:        true,
				Mode:           LiveModeOperator,
				AllowedActions: []string{"shell.rnu"},
			})
			if err == nil {
				t.Fatal("UpsertLiveAgentConfig accepted unknown action")
			}
			configs, listErr := tc.store.ListLiveAgentConfigs(ctx, gid)
			if listErr != nil {
				t.Fatalf("ListLiveAgentConfigs: %v", listErr)
			}
			if len(configs) != 0 {
				t.Fatalf("configs after rejected action = %+v, want none", configs)
			}
		})
	}
}

func TestMemoryDeleteLiveAgentConfigDoesNotInsertMissingPresence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	gid := testLiveGroupID(4)
	if _, err := store.UpsertLiveAgentPresence(ctx, LiveAgentPresence{
		GroupID:      gid,
		NodeID:       12,
		Status:       LiveStatusOnline,
		Mode:         LiveModeListen,
		TopicFilters: []string{"chat"},
	}); err != nil {
		t.Fatalf("UpsertLiveAgentPresence: %v", err)
	}
	if err := store.DeleteLiveAgentConfig(ctx, gid, 13, 100); err != nil {
		t.Fatalf("DeleteLiveAgentConfig: %v", err)
	}
	presence, err := store.ListLiveAgentPresence(ctx, gid)
	if err != nil {
		t.Fatalf("ListLiveAgentPresence: %v", err)
	}
	if len(presence) != 1 || presence[0].NodeID != 12 {
		t.Fatalf("presence after deleting missing node = %+v, want only node 12", presence)
	}
}

func mustOpenLiveStateStore(t *testing.T) StateStore {
	t.Helper()
	store, err := OpenSQLiteStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenSQLiteStateStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testLiveGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	gid[0] = seed
	return gid
}

func testLiveMessageID(seed byte) entmoot.MessageID {
	var id entmoot.MessageID
	id[0] = seed
	return id
}
