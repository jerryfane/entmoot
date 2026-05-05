package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func TestParseUpdateSemver(t *testing.T) {
	got, err := parseUpdateSemver("v1.2.3-rc1")
	if err != nil {
		t.Fatalf("parseUpdateSemver: %v", err)
	}
	if got.String() != "v1.2.3" {
		t.Fatalf("version = %s, want v1.2.3", got.String())
	}
	newer, _ := parseUpdateSemver("v1.2.4")
	if !newer.NewerThan(got) {
		t.Fatalf("v1.2.4 should be newer than %s", got.String())
	}
}

func TestUpdateReleaseVersionNewerThanPreservesPrereleasePrecedence(t *testing.T) {
	tests := []struct {
		target  string
		current string
		want    bool
	}{
		{"v1.2.3", "v1.2.3-rc1", true},
		{"v1.2.3-rc2", "v1.2.3-rc1", true},
		{"v1.2.3+build2", "v1.2.3+build1", false},
		{"v1.2.3-rc1", "v1.2.3", false},
	}
	for _, tt := range tests {
		t.Run(tt.target+"_over_"+tt.current, func(t *testing.T) {
			target, err := parseUpdateReleaseVersion(tt.target)
			if err != nil {
				t.Fatalf("target parse: %v", err)
			}
			current, err := parseUpdateReleaseVersion(tt.current)
			if err != nil {
				t.Fatalf("current parse: %v", err)
			}
			if got := target.NewerThan(current); got != tt.want {
				t.Fatalf("%s newer than %s = %v, want %v", tt.target, tt.current, got, tt.want)
			}
		})
	}
}

func TestRunEntmootUpdateAllowsDevCurrentVersion(t *testing.T) {
	current, latest, available, err := entmootUpdateAvailability("dev", "v1.2.3", false)
	if err != nil {
		t.Fatalf("entmootUpdateAvailability: %v", err)
	}
	if !available {
		t.Fatalf("dev current version should update to release")
	}
	if current != "dev" {
		t.Fatalf("current = %q, want dev", current)
	}
	if latest != "v1.2.3" {
		t.Fatalf("latest = %q, want v1.2.3", latest)
	}
}

func TestRunEntmootUpdateAllowsPrereleaseToStable(t *testing.T) {
	_, _, available, err := entmootUpdateAvailability("v1.2.3-rc1", "v1.2.3", false)
	if err != nil {
		t.Fatalf("entmootUpdateAvailability: %v", err)
	}
	if !available {
		t.Fatalf("prerelease should update to stable release")
	}
}

func TestRunEntmootUpdateExplicitTagUsesFullTag(t *testing.T) {
	_, _, available, err := entmootUpdateAvailability("v1.2.3-rc1", "v1.2.3", true)
	if err != nil {
		t.Fatalf("entmootUpdateAvailability: %v", err)
	}
	if !available {
		t.Fatalf("explicit stable tag should update from prerelease")
	}
}

func TestCmdUpdateHelpReturnsOK(t *testing.T) {
	tests := [][]string{{"-h"}, {"--help"}}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, _, stderr := captureCommandOutput(t, func() int {
				return cmdUpdate(&globalFlags{}, args)
			})
			if code != exitOK {
				t.Fatalf("cmdUpdate(%v) exit = %d, want %d; stderr=%q", args, code, exitOK, stderr)
			}
			if !strings.Contains(stderr, "Usage: entmootd update") {
				t.Fatalf("stderr = %q, want usage", stderr)
			}
		})
	}
}

func TestCmdUpdateInvalidFlagReturnsInvalidArgument(t *testing.T) {
	code, _, _ := captureCommandOutput(t, func() int {
		return cmdUpdate(&globalFlags{}, []string{"--definitely-not-a-real-flag"})
	})
	if code != exitInvalidArgument {
		t.Fatalf("cmdUpdate invalid flag exit = %d, want %d", code, exitInvalidArgument)
	}
}

func TestVerifyUpdateChecksum(t *testing.T) {
	dir := t.TempDir()
	archiveName := "entmoot-linux-amd64.tar.gz"
	archivePath := filepath.Join(dir, archiveName)
	content := []byte("archive")
	if err := os.WriteFile(archivePath, content, 0644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	checksumsPath := filepath.Join(dir, "checksums.txt")
	if err := os.WriteFile(checksumsPath, []byte(fmt.Sprintf("%x  %s\n", sum, archiveName)), 0644); err != nil {
		t.Fatal(err)
	}
	if err := verifyUpdateChecksum(archivePath, archiveName, checksumsPath); err != nil {
		t.Fatalf("verifyUpdateChecksum: %v", err)
	}
}

func TestExtractUpdateTarGzSkipsUnexpectedFiles(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "entmoot.tar.gz")
	createEntmootUpdateTestTarGz(t, archivePath, map[string]string{
		"entmootd":     "new-bin",
		"README.md":    "docs",
		"not-entmootd": "bad",
	})
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	if err := extractUpdateTarGz(archivePath, out, map[string]bool{"entmootd": true}); err != nil {
		t.Fatalf("extractUpdateTarGz: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "evil")); err == nil {
		t.Fatalf("unexpected path traversal output")
	}
	if _, err := os.Stat(filepath.Join(out, "not-entmootd")); err == nil {
		t.Fatalf("unexpected non-binary output")
	}
	if _, err := os.Stat(filepath.Join(out, "entmootd")); err != nil {
		t.Fatalf("missing entmootd: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "entmootd"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "new-bin" {
		t.Fatalf("entmootd = %q, want new-bin", string(data))
	}
}

func TestExtractUpdateTarGzRejectsUnsafeEntmootdEntries(t *testing.T) {
	tests := []string{
		"nested/entmootd",
		"../entmootd",
		"../evil",
		"/entmootd",
	}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			archivePath := filepath.Join(dir, "entmoot.tar.gz")
			createEntmootUpdateTestTarGz(t, archivePath, map[string]string{name: "bad"})
			out := filepath.Join(dir, "out")
			if err := os.MkdirAll(out, 0755); err != nil {
				t.Fatal(err)
			}
			if err := extractUpdateTarGz(archivePath, out, map[string]bool{"entmootd": true}); err == nil {
				t.Fatalf("extractUpdateTarGz succeeded for unsafe entry %q", name)
			}
			if _, err := os.Stat(filepath.Join(out, "entmootd")); err == nil {
				t.Fatalf("unsafe entry %q wrote entmootd", name)
			}
		})
	}
}

func TestExtractUpdateTarGzRejectsDuplicateEntmootd(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "entmoot.tar.gz")
	createEntmootUpdateTestTarGzEntries(t, archivePath, []entmootUpdateTestTarEntry{
		{name: "entmootd", content: "first"},
		{name: "./entmootd", content: "second"},
	})
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	if err := extractUpdateTarGz(archivePath, out, map[string]bool{"entmootd": true}); err == nil {
		t.Fatalf("extractUpdateTarGz succeeded for duplicate entmootd")
	}
}

func TestCanonicalEntmootUpdateInstallDir(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(filepath.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)

	got, err := canonicalEntmootUpdateInstallDir(filepath.Base(dir))
	if err != nil {
		t.Fatalf("canonicalEntmootUpdateInstallDir: %v", err)
	}
	want, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("install dir = %q, want %q", got, want)
	}
}

func TestCanonicalEntmootUpdateInstallDirResolvesSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "link")
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	got, err := canonicalEntmootUpdateInstallDir(link)
	if err != nil {
		t.Fatalf("canonicalEntmootUpdateInstallDir: %v", err)
	}
	want, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("install dir = %q, want %q", got, want)
	}
}

func TestRestartEntmootUnsupportedPlatformRequiresManualRestart(t *testing.T) {
	oldGOOS := updateRuntimeGOOS
	updateRuntimeGOOS = "darwin"
	defer func() { updateRuntimeGOOS = oldGOOS }()

	got := restartEntmootAfterUpdate(filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd"))
	if got.handled {
		t.Fatalf("restart handled on unsupported platform")
	}
	if got.warning == "" {
		t.Fatalf("missing unsupported platform warning")
	}
}

func TestRestartEntmootRescansBeforeSignaling(t *testing.T) {
	oldGOOS := updateRuntimeGOOS
	oldSignal := updateSignalProcess
	oldFind := updateFindEntmootRestartProcessIDs
	updateRuntimeGOOS = "linux"
	defer func() {
		updateRuntimeGOOS = oldGOOS
		updateSignalProcess = oldSignal
		updateFindEntmootRestartProcessIDs = oldFind
	}()

	entmootdPath := filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd")
	updateFindEntmootRestartProcessIDs = func(path string) []int {
		if path != entmootdPath {
			t.Fatalf("restart scan path = %q, want %q", path, entmootdPath)
		}
		return []int{101, 202}
	}

	var gotPIDs []int
	updateSignalProcess = func(pid int, sig syscall.Signal) error {
		if sig != syscall.SIGTERM {
			t.Fatalf("signal = %v, want SIGTERM", sig)
		}
		gotPIDs = append(gotPIDs, pid)
		return nil
	}

	got := restartEntmootAfterUpdate(entmootdPath)
	if got.handled || got.warning == "" {
		t.Fatalf("restart = %+v, want manual restart warning", got)
	}
	if fmt.Sprint(gotPIDs) != "[101 202]" {
		t.Fatalf("signaled pids = %v, want [101 202]", gotPIDs)
	}
}

func TestRestartEntmootNoRunningDaemonIsHandled(t *testing.T) {
	oldGOOS := updateRuntimeGOOS
	oldFind := updateFindEntmootRestartProcessIDs
	updateRuntimeGOOS = "linux"
	updateFindEntmootRestartProcessIDs = func(string) []int { return nil }
	defer func() {
		updateRuntimeGOOS = oldGOOS
		updateFindEntmootRestartProcessIDs = oldFind
	}()

	got := restartEntmootAfterUpdate(filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd"))
	if !got.handled || got.warning != "" {
		t.Fatalf("restart = %+v, want handled without warning", got)
	}
}

func TestFindEntmootProcessIDsExcludesCurrentProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires /proc")
	}
	exe, err := os.Readlink(filepath.Join("/proc", fmt.Sprint(os.Getpid()), "exe"))
	if err != nil {
		t.Skipf("read current exe: %v", err)
	}
	for _, pid := range findEntmootProcessIDs(exe) {
		if pid == os.Getpid() {
			t.Fatalf("findEntmootProcessIDs returned current pid %d", pid)
		}
	}
}

func TestMatchesEntmootDaemonExecutable(t *testing.T) {
	entmootdPath := filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd")
	tests := []struct {
		name string
		exe  string
		want bool
	}{
		{name: "exact", exe: entmootdPath, want: true},
		{name: "exact deleted", exe: entmootdPath + " (deleted)", want: true},
		{name: "backup deleted", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd.bak") + " (deleted)", want: true},
		{name: "backup live", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd.bak"), want: true},
		{name: "different dir", exe: filepath.Join(string(filepath.Separator), "tmp", "entmootd"), want: false},
		{name: "other file same dir", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "tail") + " (deleted)", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesEntmootDaemonExecutable(tt.exe, entmootdPath); got != tt.want {
				t.Fatalf("matchesEntmootDaemonExecutable = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesStaleEntmootDaemonExecutable(t *testing.T) {
	entmootdPath := filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd")
	tests := []struct {
		name string
		exe  string
		want bool
	}{
		{name: "fresh daemon", exe: entmootdPath, want: false},
		{name: "deleted daemon", exe: entmootdPath + " (deleted)", want: true},
		{name: "backup deleted", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd.bak") + " (deleted)", want: true},
		{name: "backup live", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "entmootd.bak"), want: true},
		{name: "different dir deleted", exe: filepath.Join(string(filepath.Separator), "tmp", "entmootd") + " (deleted)", want: false},
		{name: "other file deleted", exe: filepath.Join(string(filepath.Separator), "opt", "entmoot", "tail") + " (deleted)", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesStaleEntmootDaemonExecutable(tt.exe, entmootdPath); got != tt.want {
				t.Fatalf("matchesStaleEntmootDaemonExecutable = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEntmootTopLevelSubcommandDetectsDaemonModes(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "serve", args: []string{"entmootd", "serve"}, want: "serve"},
		{name: "join", args: []string{"entmootd", "join", "invite.json"}, want: "join"},
		{name: "global flag value", args: []string{"entmootd", "-data", "/tmp/e", "serve"}, want: "serve"},
		{name: "global flag equals", args: []string{"entmootd", "-data=/tmp/e", "join"}, want: "join"},
		{name: "global bool flag", args: []string{"entmootd", "-hide-ip", "serve"}, want: "serve"},
		{name: "tail", args: []string{"entmootd", "tail", "-topic", "join"}, want: "tail"},
		{name: "esp serve", args: []string{"entmootd", "esp", "serve"}, want: "esp"},
		{name: "missing", args: []string{"entmootd", "-hide-ip"}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := entmootTopLevelSubcommand(tt.args); got != tt.want {
				t.Fatalf("subcommand = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsEntmootDaemonProcessFiltersCmdline(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]struct {
		cmdline []byte
		want    bool
	}{
		"serve":     {cmdline: []byte("entmootd\x00serve\x00"), want: true},
		"join":      {cmdline: []byte("entmootd\x00-data\x00/tmp/e\x00join\x00"), want: true},
		"tail":      {cmdline: []byte("entmootd\x00tail\x00-topic\x00join\x00"), want: false},
		"esp_serve": {cmdline: []byte("entmootd\x00esp\x00serve\x00"), want: false},
		"update":    {cmdline: []byte("entmootd\x00update\x00--restart\x00"), want: false},
		"empty":     {cmdline: nil, want: false},
		"no_subcmd": {cmdline: []byte("entmootd\x00-hide-ip\x00"), want: false},
		"truncated": {cmdline: []byte("entmootd"), want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, tc.cmdline, 0644); err != nil {
				t.Fatal(err)
			}
			if got := isEntmootDaemonProcess(path); got != tc.want {
				t.Fatalf("isEntmootDaemonProcess = %v, want %v", got, tc.want)
			}
		})
	}
}

func createEntmootUpdateTestTarGz(t *testing.T, path string, files map[string]string) {
	t.Helper()
	entries := make([]entmootUpdateTestTarEntry, 0, len(files))
	for name, content := range files {
		entries = append(entries, entmootUpdateTestTarEntry{name: name, content: content})
	}
	createEntmootUpdateTestTarGzEntries(t, path, entries)
}

type entmootUpdateTestTarEntry struct {
	name    string
	content string
}

func createEntmootUpdateTestTarGzEntries(t *testing.T, path string, entries []entmootUpdateTestTarEntry) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	for _, entry := range entries {
		hdr := &tar.Header{Name: entry.name, Mode: 0755, Size: int64(len(entry.content))}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(entry.content)); err != nil {
			t.Fatal(err)
		}
	}
}
