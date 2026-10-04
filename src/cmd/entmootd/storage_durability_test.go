package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The mount table of the restricted cloud whose message store vanished while
// identity and membership survived: the data root lived on a Kata overlay
// mounted fsync=volatile.
const cloudMountinfo = `22 1 0:20 / / rw,relatime - ext4 /dev/vda1 rw
40 22 0:41 / /workspace/shared rw,relatime - overlay overlay rw,lowerdir=/l1:/l2,upperdir=/u,workdir=/w,fsync=volatile
41 22 0:42 / /workspace/scratch rw,relatime - overlay overlay rw,lowerdir=/l1,upperdir=/u2,workdir=/w2
42 40 0:43 / /workspace/shared/durable rw,relatime - ext4 /dev/vdb1 rw
43 22 0:44 / /run rw,nosuid - tmpfs tmpfs rw,size=1024k
44 22 0:45 / /srv/my\040data rw,relatime - overlay overlay rw,lowerdir=/l1,upperdir=/u3,workdir=/w3,volatile
`

func withMountinfo(t *testing.T, mountinfo string) {
	t.Helper()
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	t.Cleanup(func() { procRoot = oldProcRoot })
	if err := os.MkdirAll(filepath.Join(procRoot, "self"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(procRoot, "self", "mountinfo"), []byte(mountinfo), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeReportWarnsDataDirOnVolatileStorage(t *testing.T) {
	withMountinfo(t, cloudMountinfo)
	dataDir := "/workspace/shared/entmoot"
	report := collectRuntimeReport(&globalFlags{data: dataDir, identity: dataDir + "/identity.json"}, dataDir)
	if !strings.Contains(report.StorageWarning, "fsync=volatile") || !strings.Contains(report.StorageWarning, "/workspace/shared") {
		t.Fatalf("storage warning = %q, want fsync=volatile warning for /workspace/shared", report.StorageWarning)
	}

	for _, tc := range []struct {
		dataDir string
		want    string
	}{
		{dataDir: "/workspace/scratch/entmoot"},
		{dataDir: "/workspace/shared/durable/entmoot"},
		{dataDir: "/workspace/sharedx/entmoot"},
		{dataDir: "/home/agent/.entmoot"},
		{dataDir: "/run/entmoot", want: "memory-backed tmpfs"},
		{dataDir: "/srv/my data/entmoot", want: "fsync=volatile at /srv/my data"},
	} {
		got := dataStorageWarning(tc.dataDir)
		if tc.want == "" && got != "" {
			t.Errorf("dataStorageWarning(%q) = %q, want none", tc.dataDir, got)
		}
		if tc.want != "" && !strings.Contains(got, tc.want) {
			t.Errorf("dataStorageWarning(%q) = %q, want %q", tc.dataDir, got, tc.want)
		}
	}
}

func TestDataStorageWarningResolvesSymlinkedUncreatedDataDir(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	shm := filepath.Join(root, "shm")
	if err := os.Mkdir(shm, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(shm, link); err != nil {
		t.Fatal(err)
	}
	withMountinfo(t, "22 1 0:20 / / rw,relatime - ext4 /dev/vda1 rw\n43 22 0:44 / "+shm+" rw,nosuid - tmpfs tmpfs rw\n")
	dataDir := filepath.Join(link, "entmoot-not-created", "nested")
	if got := dataStorageWarning(dataDir); !strings.Contains(got, "memory-backed tmpfs mounted at "+shm) {
		t.Fatalf("dataStorageWarning(%q) = %q, want tmpfs warning for %s", dataDir, got, shm)
	}
}

func TestMountinfoWithoutSuperOptionsIsRejected(t *testing.T) {
	malformed := "22 1 0:20 / / rw - ext4 /dev/vda"
	if entry, ok := parseMountinfoLine(malformed); ok {
		t.Fatalf("parseMountinfoLine(%q) = %+v, want rejection", malformed, entry)
	}
	withMountinfo(t, malformed+"\n43 22 0:44 / /run rw,nosuid - tmpfs tmpfs rw\n")
	if got := dataStorageWarning("/run/entmoot"); !strings.Contains(got, "memory-backed tmpfs") {
		t.Fatalf("dataStorageWarning after malformed line = %q, want tmpfs warning", got)
	}
}
