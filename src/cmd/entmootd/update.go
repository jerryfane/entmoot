package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const defaultEntmootUpdateRepo = "jerryfane/entmoot"

type updateRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []updateAsset `json:"assets"`
}

type updateAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

type updateResult struct {
	CurrentVersion  string   `json:"current_version"`
	LatestVersion   string   `json:"latest_version"`
	UpdateAvailable bool     `json:"update_available"`
	Updated         bool     `json:"updated"`
	Asset           string   `json:"asset,omitempty"`
	InstallDir      string   `json:"install_dir"`
	RestartRequired bool     `json:"restart_required"`
	Warnings        []string `json:"warnings,omitempty"`
}

type updateSemver struct {
	major int
	minor int
	patch int
}

type updateReleaseVersion struct {
	major      int
	minor      int
	patch      int
	prerelease []string
}

type updateRestartResult struct {
	handled bool
	warning string
}

var updateRuntimeGOOS = runtime.GOOS
var updateSignalProcess = syscall.Kill
var updateReadFile = os.ReadFile
var updateFindEntmootRestartProcessIDs = findEntmootRestartProcessIDs

func cmdUpdate(_ *globalFlags, args []string) int {
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	checkOnly := fs.Bool("check", false, "check for an update without installing it")
	jsonOut := fs.Bool("json", false, "print JSON output")
	repo := fs.String("repo", defaultEntmootUpdateRepo, "GitHub owner/repo")
	tag := fs.String("tag", "", "explicit release tag to install")
	prerelease := fs.Bool("prerelease", false, "use the newest release including prereleases")
	installDir := fs.String("install-dir", "", "directory containing entmootd")
	restart := fs.Bool("restart", false, "send SIGTERM to a running entmootd from the install dir after update")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd update [--check] [--restart] [--json] [--repo owner/repo] [--tag vX.Y.Z] [--prerelease] [--install-dir DIR]")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if fs.NArg() != 0 {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "entmootd update: unexpected arguments")
		return exitInvalidArgument
	}
	if *installDir == "" {
		*installDir = defaultEntmootUpdateInstallDir()
	}

	result, err := runEntmootUpdate(updateConfig{
		repo:       *repo,
		tag:        *tag,
		prerelease: *prerelease,
		installDir: *installDir,
		currentTag: version,
		checkOnly:  *checkOnly,
		restart:    *restart,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	})
	if err != nil {
		if *jsonOut {
			_ = emitJSON(map[string]any{"error": map[string]any{"code": "update_failed", "message": err.Error()}})
		} else {
			fmt.Fprintf(os.Stderr, "entmootd update: %v\n", err)
		}
		return exitTransport
	}
	if *jsonOut {
		if err := emitJSON(result); err != nil {
			fmt.Fprintf(os.Stderr, "entmootd update: %v\n", err)
			return exitTransport
		}
		return exitOK
	}
	if !result.UpdateAvailable {
		fmt.Printf("Entmoot is up to date (%s)\n", result.CurrentVersion)
		return exitOK
	}
	if *checkOnly {
		fmt.Printf("Entmoot update available: %s -> %s\n", result.CurrentVersion, result.LatestVersion)
		fmt.Println("Run: entmootd update --restart")
		return exitOK
	}
	fmt.Printf("Updated Entmoot: %s -> %s\n", result.CurrentVersion, result.LatestVersion)
	for _, warning := range result.Warnings {
		fmt.Printf("Warning: %s\n", warning)
	}
	if result.RestartRequired {
		fmt.Println("Restart required: start entmootd with your usual service manager or command.")
	}
	return exitOK
}

type updateConfig struct {
	repo       string
	tag        string
	prerelease bool
	installDir string
	currentTag string
	checkOnly  bool
	restart    bool
	httpClient *http.Client
}

func runEntmootUpdate(cfg updateConfig) (*updateResult, error) {
	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	installDir, err := canonicalEntmootUpdateInstallDir(cfg.installDir)
	if err != nil {
		return nil, err
	}
	cfg.installDir = installDir
	release, err := fetchEntmootRelease(cfg)
	if err != nil {
		return nil, err
	}
	currentTag, latestTag, updateAvailable, err := entmootUpdateAvailability(cfg.currentTag, release.TagName, cfg.tag != "")
	if err != nil {
		return nil, err
	}
	asset := entmootArchiveName()
	result := &updateResult{
		CurrentVersion:  currentTag,
		LatestVersion:   latestTag,
		UpdateAvailable: updateAvailable,
		Asset:           asset,
		InstallDir:      cfg.installDir,
	}
	if !result.UpdateAvailable {
		return result, nil
	}
	if cfg.checkOnly {
		return result, nil
	}
	if err := applyEntmootUpdate(cfg, release, asset); err != nil {
		return nil, err
	}
	result.Updated = true
	if cfg.restart {
		restartResult := restartEntmootAfterUpdate(filepath.Join(cfg.installDir, "entmootd"))
		result.RestartRequired = !restartResult.handled
		if restartResult.warning != "" {
			result.Warnings = append(result.Warnings, restartResult.warning)
		}
	} else {
		result.RestartRequired = true
	}
	return result, nil
}

func canonicalEntmootUpdateInstallDir(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("install dir is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolve install dir %q: %w", dir, err)
	}
	if _, err := os.Stat(abs); err != nil {
		if os.IsNotExist(err) {
			return abs, nil
		}
		return "", fmt.Errorf("stat install dir %q: %w", abs, err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve install dir symlinks %q: %w", abs, err)
	}
	return real, nil
}

func fetchEntmootRelease(cfg updateConfig) (*updateRelease, error) {
	if cfg.tag != "" {
		return fetchEntmootReleaseURL(cfg, fmt.Sprintf("https://api.github.com/repos/%s/releases/tags/%s", cfg.repo, cfg.tag))
	}
	if cfg.prerelease {
		url := fmt.Sprintf("https://api.github.com/repos/%s/releases?per_page=1", cfg.repo)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("User-Agent", "entmootd/"+version)
		resp, err := cfg.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return nil, fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
		}
		var releases []updateRelease
		if err := json.NewDecoder(resp.Body).Decode(&releases); err != nil {
			return nil, err
		}
		if len(releases) == 0 {
			return nil, fmt.Errorf("no releases found")
		}
		return &releases[0], nil
	}
	return fetchEntmootReleaseURL(cfg, fmt.Sprintf("https://api.github.com/repos/%s/releases/latest", cfg.repo))
}

func fetchEntmootReleaseURL(cfg updateConfig, url string) (*updateRelease, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "entmootd/"+version)
	resp, err := cfg.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, string(body))
	}
	var release updateRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}
	return &release, nil
}

func applyEntmootUpdate(cfg updateConfig, release *updateRelease, archiveName string) error {
	var archiveURL, checksumsURL string
	for _, a := range release.Assets {
		switch a.Name {
		case archiveName:
			archiveURL = a.BrowserDownloadURL
		case "checksums.txt":
			checksumsURL = a.BrowserDownloadURL
		}
	}
	if archiveURL == "" {
		return fmt.Errorf("no asset %q in release %s", archiveName, release.TagName)
	}
	if checksumsURL == "" {
		return fmt.Errorf("release %s has no checksums.txt", release.TagName)
	}
	tmpDir, err := os.MkdirTemp("", "entmoot-update-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	archivePath := filepath.Join(tmpDir, archiveName)
	checksumsPath := filepath.Join(tmpDir, "checksums.txt")
	if err := downloadUpdateFile(cfg.httpClient, archiveURL, archivePath); err != nil {
		return fmt.Errorf("download archive: %w", err)
	}
	if err := downloadUpdateFile(cfg.httpClient, checksumsURL, checksumsPath); err != nil {
		return fmt.Errorf("download checksums: %w", err)
	}
	if err := verifyUpdateChecksum(archivePath, archiveName, checksumsPath); err != nil {
		return err
	}
	stagingDir := filepath.Join(tmpDir, "staging")
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return err
	}
	if err := extractUpdateTarGz(archivePath, stagingDir, map[string]bool{"entmootd": true}); err != nil {
		return err
	}
	src := filepath.Join(stagingDir, "entmootd")
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("archive missing entmootd: %w", err)
	}
	if err := os.MkdirAll(cfg.installDir, 0755); err != nil {
		return err
	}
	dst := filepath.Join(cfg.installDir, "entmootd")
	if err := replaceUpdateBinary(src, dst); err != nil {
		return err
	}
	if runtime.GOOS == "darwin" {
		_ = exec.Command("codesign", "--force", "--sign", "-", dst).Run()
		_ = exec.Command("xattr", "-cr", dst).Run()
	}
	_ = os.WriteFile(filepath.Join(cfg.installDir, ".entmoot-version"), []byte(release.TagName+"\n"), 0644)
	return nil
}

func downloadUpdateFile(client *http.Client, url, dst string) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d for %s", resp.StatusCode, url)
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func verifyUpdateChecksum(archivePath, archiveName, checksumsPath string) error {
	data, err := os.ReadFile(checksumsPath)
	if err != nil {
		return err
	}
	var expected string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == archiveName {
			expected = fields[0]
			break
		}
	}
	if expected == "" {
		return fmt.Errorf("checksums.txt has no entry for %s", archiveName)
	}
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if actual != expected {
		return fmt.Errorf("checksum mismatch for %s", archiveName)
	}
	return nil
}

func extractUpdateTarGz(archivePath, destDir string, allowed map[string]bool) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	seen := make(map[string]bool)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		name, ok, err := updateArchiveEntryName(hdr.Name, allowed)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if seen[name] {
			return fmt.Errorf("archive contains duplicate %s", name)
		}
		seen[name] = true
		dst := filepath.Join(destDir, name)
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, tr)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
}

func updateArchiveEntryName(raw string, allowed map[string]bool) (string, bool, error) {
	clean := path.Clean(raw)
	if raw == "" || path.IsAbs(raw) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", false, fmt.Errorf("unsafe archive path %q", raw)
	}
	base := path.Base(clean)
	if allowed[base] && clean != base {
		return "", false, fmt.Errorf("archive contains nested %s at %q", base, raw)
	}
	if !allowed[clean] {
		return "", false, nil
	}
	return clean, true, nil
}

func replaceUpdateBinary(src, dst string) error {
	backup := dst + ".bak"
	_ = os.Remove(backup)
	if _, err := os.Stat(dst); err == nil {
		if err := os.Rename(dst, backup); err != nil {
			return err
		}
	}
	srcFile, err := os.Open(src)
	if err != nil {
		_ = os.Rename(backup, dst)
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		_ = os.Rename(backup, dst)
		return err
	}
	if _, err := io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		_ = os.Remove(dst)
		_ = os.Rename(backup, dst)
		return err
	}
	if err := dstFile.Close(); err != nil {
		_ = os.Remove(dst)
		_ = os.Rename(backup, dst)
		return err
	}
	if err := os.Chmod(dst, 0755); err != nil {
		_ = os.Remove(dst)
		_ = os.Rename(backup, dst)
		return err
	}
	_ = os.Remove(backup)
	return nil
}

func defaultEntmootUpdateInstallDir() string {
	if exe, err := os.Executable(); err == nil {
		if resolved, err := filepath.EvalSymlinks(exe); err == nil {
			exe = resolved
		}
		if filepath.Base(exe) == "entmootd" {
			return filepath.Dir(exe)
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".entmoot", "bin")
}

func entmootArchiveName() string {
	return fmt.Sprintf("entmoot-%s-%s.tar.gz", runtime.GOOS, runtime.GOARCH)
}

func parseUpdateSemver(s string) (updateSemver, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	if idx := strings.IndexByte(s, '-'); idx >= 0 {
		s = s[:idx]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return updateSemver{}, fmt.Errorf("invalid semver %q", s)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return updateSemver{}, err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return updateSemver{}, err
	}
	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return updateSemver{}, err
	}
	return updateSemver{major: major, minor: minor, patch: patch}, nil
}

func normalizeUpdateTag(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "dev" {
		return s
	}
	if !strings.HasPrefix(s, "v") {
		return "v" + s
	}
	return s
}

func entmootUpdateAvailability(currentTag, releaseTag string, explicitTag bool) (string, string, bool, error) {
	latest, err := parseUpdateReleaseVersion(releaseTag)
	if err != nil {
		return "", "", false, fmt.Errorf("parse release tag %q: %w", releaseTag, err)
	}
	normalizedCurrent := normalizeUpdateTag(currentTag)
	normalizedLatest := normalizeUpdateTag(releaseTag)
	current, currentErr := parseUpdateReleaseVersion(normalizedCurrent)
	if currentErr != nil {
		return normalizedCurrent, normalizedLatest, true, nil
	}
	return normalizedCurrent, normalizedLatest, updateReleaseAvailable(current, normalizedCurrent, latest, releaseTag, explicitTag), nil
}

func parseUpdateReleaseVersion(s string) (updateReleaseVersion, error) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	if idx := strings.IndexByte(s, '+'); idx >= 0 {
		s = s[:idx]
	}
	var prerelease []string
	if idx := strings.IndexByte(s, '-'); idx >= 0 {
		prerelease = strings.Split(s[idx+1:], ".")
		s = s[:idx]
	}
	base, err := parseUpdateSemver(s)
	if err != nil {
		return updateReleaseVersion{}, err
	}
	return updateReleaseVersion{
		major:      base.major,
		minor:      base.minor,
		patch:      base.patch,
		prerelease: prerelease,
	}, nil
}

func updateReleaseAvailable(current updateReleaseVersion, currentTag string, target updateReleaseVersion, targetTag string, explicitTag bool) bool {
	if explicitTag {
		return normalizeUpdateTag(currentTag) != normalizeUpdateTag(targetTag)
	}
	return target.NewerThan(current)
}

func (v updateReleaseVersion) NewerThan(other updateReleaseVersion) bool {
	if v.major != other.major {
		return v.major > other.major
	}
	if v.minor != other.minor {
		return v.minor > other.minor
	}
	if v.patch != other.patch {
		return v.patch > other.patch
	}
	return compareUpdatePrerelease(v.prerelease, other.prerelease) > 0
}

func compareUpdatePrerelease(a, b []string) int {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	if len(a) == 0 {
		return 1
	}
	if len(b) == 0 {
		return -1
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		cmp := compareUpdatePrereleaseIdentifier(a[i], b[i])
		if cmp != 0 {
			return cmp
		}
	}
	if len(a) > len(b) {
		return 1
	}
	if len(a) < len(b) {
		return -1
	}
	return 0
}

func compareUpdatePrereleaseIdentifier(a, b string) int {
	aNum, aErr := strconv.Atoi(a)
	bNum, bErr := strconv.Atoi(b)
	aIsNum := aErr == nil
	bIsNum := bErr == nil
	switch {
	case aIsNum && bIsNum:
		if aNum > bNum {
			return 1
		}
		if aNum < bNum {
			return -1
		}
		return 0
	case aIsNum:
		return -1
	case bIsNum:
		return 1
	default:
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	}
}

func (v updateSemver) NewerThan(other updateSemver) bool {
	if v.major != other.major {
		return v.major > other.major
	}
	if v.minor != other.minor {
		return v.minor > other.minor
	}
	return v.patch > other.patch
}

func (v updateSemver) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.major, v.minor, v.patch)
}

func restartEntmootAfterUpdate(entmootdPath string) updateRestartResult {
	if updateRuntimeGOOS != "linux" {
		return updateRestartResult{
			warning: fmt.Sprintf("restart is not supported on %s; restart entmootd manually", updateRuntimeGOOS),
		}
	}
	pids := updateFindEntmootRestartProcessIDs(entmootdPath)
	if len(pids) == 0 {
		return updateRestartResult{handled: true}
	}
	for _, pid := range pids {
		if err := updateSignalProcess(pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
			return updateRestartResult{
				warning: fmt.Sprintf("failed to signal entmootd pid %d: %v; restart entmootd manually", pid, err),
			}
		}
	}
	return updateRestartResult{
		warning: "entmootd was stopped after update; start it with your usual service manager or command",
	}
}

func findEntmootRestartProcessIDs(entmootdPath string) []int {
	return findEntmootProcessIDsMatching(entmootdPath, matchesStaleEntmootDaemonExecutable)
}

func findEntmootProcessIDs(entmootdPath string) []int {
	return findEntmootProcessIDsMatching(entmootdPath, matchesEntmootDaemonExecutable)
}

func findEntmootProcessIDsMatching(entmootdPath string, matches func(string, string) bool) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		exe, err := os.Readlink(filepath.Join("/proc", entry.Name(), "exe"))
		if err != nil || !matches(exe, entmootdPath) {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == os.Getpid() {
			continue
		}
		if isEntmootDaemonProcess(filepath.Join("/proc", entry.Name(), "cmdline")) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func matchesEntmootDaemonExecutable(exe, entmootdPath string) bool {
	exe = strings.TrimSuffix(exe, " (deleted)")
	if exe == entmootdPath {
		return true
	}
	return filepath.Dir(exe) == filepath.Dir(entmootdPath) && filepath.Base(exe) == "entmootd.bak"
}

func matchesStaleEntmootDaemonExecutable(exe, entmootdPath string) bool {
	deleted := strings.HasSuffix(exe, " (deleted)")
	exe = strings.TrimSuffix(exe, " (deleted)")
	if filepath.Dir(exe) != filepath.Dir(entmootdPath) {
		return false
	}
	switch filepath.Base(exe) {
	case "entmootd.bak":
		return true
	case "entmootd":
		return deleted
	default:
		return false
	}
}

func isEntmootDaemonProcess(cmdlinePath string) bool {
	data, err := updateReadFile(cmdlinePath)
	if err != nil || len(data) == 0 {
		return false
	}
	args := splitProcCmdline(data)
	subcmd := entmootTopLevelSubcommand(args)
	return subcmd == "join" || subcmd == "serve"
}

func splitProcCmdline(data []byte) []string {
	parts := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func entmootTopLevelSubcommand(args []string) string {
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(arg, "-") {
			if updateFlagTakesValue(arg) && !strings.Contains(arg, "=") {
				i++
			}
			continue
		}
		return arg
	}
	return ""
}

func updateFlagTakesValue(flagName string) bool {
	flagName = strings.TrimLeft(flagName, "-")
	if idx := strings.IndexByte(flagName, '='); idx >= 0 {
		flagName = flagName[:idx]
	}
	switch flagName {
	case "socket", "identity", "data", "listen-port", "log-level",
		"pilot-wait-timeout", "pilot-wait-base-delay", "pilot-wait-max-delay":
		return true
	default:
		return false
	}
}
