package policy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestFileStorePersistsPoliciesByGroupID(t *testing.T) {
	ctx := context.Background()
	gid := testGroupID(0x42)
	other := testGroupID(0x43)
	root := t.TempDir()

	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	if want := filepath.Join(root, "policies", "group-policies.json"); store.Path() != want {
		t.Fatalf("Path = %q, want %q", store.Path(), want)
	}
	if _, ok, err := store.Get(ctx, gid); err != nil || ok {
		t.Fatalf("Get before Put = ok %t err %v, want missing nil", ok, err)
	}

	p := TheEntMootDefault()
	p.RetentionDays = 14
	if err := store.Put(ctx, gid, p); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok, err := store.Get(ctx, other); err != nil || ok {
		t.Fatalf("Get other = ok %t err %v, want missing nil", ok, err)
	}

	reopened, err := OpenFileStore(root)
	if err != nil {
		t.Fatalf("reopen OpenFileStore: %v", err)
	}
	got, ok, err := reopened.Get(ctx, gid)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !ok {
		t.Fatal("Get after reopen missing policy")
	}
	if got.RetentionDays != 14 {
		t.Fatalf("RetentionDays = %d, want 14", got.RetentionDays)
	}

	all, err := reopened.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("List length = %d, want 1", len(all))
	}
	if all[gid].RetentionDays != 14 {
		t.Fatalf("List policy = %+v, want retained policy", all[gid])
	}
}

func TestFileStoreRejectsMalformedValues(t *testing.T) {
	ctx := context.Background()
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}

	p := TheEntMootDefault()
	p.MessageRatePerAuthor = "bad"
	if err := store.Put(ctx, testGroupID(0x44), p); err == nil {
		t.Fatal("Put malformed policy returned nil, want error")
	}
}

func TestFileStoreReportsMalformedExistingFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	data := map[string]interface{}{
		"version": storeVersion,
		"policies": map[string]interface{}{
			testGroupID(0x45).String(): map[string]interface{}{
				"message_rate_per_author":   "bad",
				"message_burst_per_author":  12,
				"byte_rate_per_author":      "64KiB/min",
				"byte_burst_per_author":     128 * 1024,
				"max_message_bytes":         8192,
				"live_trigger_rate":         "6/min",
				"live_trigger_burst":        6,
				"live_max_actions_per_scan": 1,
				"live_max_action_bytes":     4096,
				"retention_days":            30,
			},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(store.Path(), raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, _, err := store.Get(ctx, testGroupID(0x45)); err == nil {
		t.Fatal("Get malformed existing file returned nil, want error")
	}
}

func TestFileStoreDeleteAndValidation(t *testing.T) {
	ctx := context.Background()
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	gid := testGroupID(0x46)
	if err := store.Put(ctx, gid, TheEntMootDefault()); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, gid); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := store.Get(ctx, gid); err != nil || ok {
		t.Fatalf("Get after Delete = ok %t err %v, want missing nil", ok, err)
	}

	var zero entmoot.GroupID
	if err := store.Put(ctx, zero, TheEntMootDefault()); err == nil {
		t.Fatal("Put zero group returned nil, want error")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := store.Get(canceled, gid); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get canceled err = %v, want context.Canceled", err)
	}
}

func TestFileStoreConcurrentInstancesPreserveUpdates(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const stores = 12

	var wg sync.WaitGroup
	errs := make(chan error, stores)
	for i := 0; i < stores; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			store, err := OpenFileStore(root)
			if err != nil {
				errs <- err
				return
			}
			p := TheEntMootDefault()
			p.RetentionDays = 30 + i
			errs <- store.Put(ctx, testGroupID(byte(i+1)), p)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	store, err := OpenFileStore(root)
	if err != nil {
		t.Fatalf("OpenFileStore: %v", err)
	}
	all, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != stores {
		t.Fatalf("List length = %d, want %d", len(all), stores)
	}
	for i := 0; i < stores; i++ {
		gid := testGroupID(byte(i + 1))
		got, ok := all[gid]
		if !ok {
			t.Fatalf("missing policy for group %s", gid)
		}
		if got.RetentionDays != 30+i {
			t.Fatalf("RetentionDays for group %s = %d, want %d", gid, got.RetentionDays, 30+i)
		}
	}
}

func testGroupID(seed byte) entmoot.GroupID {
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = seed
	}
	return gid
}
