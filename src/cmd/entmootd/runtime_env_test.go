package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCmdEnvDoesNotCreateIdentityOrData(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "missing-data")
	identity := filepath.Join(dir, "missing-identity.json")
	gf := &globalFlags{socket: filepath.Join(dir, "pilot.sock"), data: dataDir, identity: identity}

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdEnv(gf, []string{"--json"})
	})
	if code != exitOK {
		t.Fatalf("cmdEnv code = %d stderr=%s", code, stderr)
	}
	var report runtimeReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal env report: %v\n%s", err, stdout)
	}
	if report.DataDir != dataDir || report.IdentityPath != identity {
		t.Fatalf("paths = data %q identity %q", report.DataDir, report.IdentityPath)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("env created data dir or stat err = %v", err)
	}
	if _, err := os.Stat(identity); !os.IsNotExist(err) {
		t.Fatalf("env created identity or stat err = %v", err)
	}
}

func TestRuntimeNoDaemonHelpReportsNamespaceMismatch(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	pidRoot := filepath.Join(procRoot, "12345")
	if err := os.MkdirAll(filepath.Join(pidRoot, "root", "data", ".entmoot"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(pidRoot, "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/data/.entmoot/bin/entmootd",
		"-socket", "/tmp/pilot.sock",
		"-identity", "/data/.entmoot/identity.json",
		"-data", "/data/.entmoot",
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pidRoot, "root", ".dockerenv"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sockPath := filepath.Join(pidRoot, "root", "data", ".entmoot", "control.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen fake control socket: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	gf := &globalFlags{
		socket:   "/tmp/pilot.sock",
		data:     "/data/.entmoot",
		identity: "/data/.entmoot/identity.json",
	}
	help := runtimeNoDaemonHelp(gf, gf.data)
	if !strings.Contains(help, "runtime warning:") {
		t.Fatalf("help missing runtime warning:\n%s", help)
	}
	if !strings.Contains(help, "pid=12345") {
		t.Fatalf("help missing daemon pid:\n%s", help)
	}
	if !strings.Contains(help, "docker exec -u node") {
		t.Fatalf("help missing docker suggestion:\n%s", help)
	}
}

func TestRuntimeReportClassifiesHalfAliveDaemon(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	dataDir := filepath.Join(dir, "data")
	pidRoot := filepath.Join(procRoot, "12345")
	if err := os.MkdirAll(pidRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/usr/local/bin/entmootd",
		"-socket", filepath.Join(dir, "pilot.sock"),
		"-identity", filepath.Join(dataDir, "identity.json"),
		"-data", dataDir,
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}

	report := collectRuntimeReport(&globalFlags{
		socket:   filepath.Join(dir, "pilot.sock"),
		data:     dataDir,
		identity: filepath.Join(dataDir, "identity.json"),
	}, dataDir)
	if report.RuntimeStatus != runtimeStatusHalfAlive {
		t.Fatalf("runtime status = %q, want half_alive; report=%+v", report.RuntimeStatus, report)
	}
	if report.PublishPathHealthy {
		t.Fatalf("publish path unexpectedly healthy: %+v", report)
	}
	if report.RunningDaemon == nil {
		t.Fatalf("running daemon missing: %+v", report)
	}
	if !strings.Contains(strings.Join(report.Suggestions, "\n"), "direct publish path is not healthy") {
		t.Fatalf("half-alive suggestion missing: %#v", report.Suggestions)
	}
}

func TestRuntimeReportClassifiesHealthyPublishPath(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-runtime-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	dataDir := filepath.Join(dir, "data")
	pilotSock := filepath.Join(dir, "missing-pilot.sock")
	controlSock := controlSocketPath(dataDir)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	controlLn, err := net.Listen("unix", controlSock)
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	defer controlLn.Close()
	go func() {
		conn, err := controlLn.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	report := collectRuntimeReport(&globalFlags{
		socket:   pilotSock,
		data:     dataDir,
		identity: filepath.Join(dataDir, "identity.json"),
	}, dataDir)
	if report.RuntimeStatus != runtimeStatusHealthy {
		t.Fatalf("runtime status = %q, want healthy; report=%+v", report.RuntimeStatus, report)
	}
	if !report.PublishPathHealthy {
		t.Fatalf("publish path not healthy: %+v", report)
	}
	if report.PilotSocketReachable {
		t.Fatalf("pilot socket unexpectedly reachable: %+v", report)
	}
	if !report.ControlSocketReachable {
		t.Fatalf("control socket not reachable: %+v", report)
	}
}

func TestRuntimeSocketProbeDistinguishesStaleSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "entmoot-runtime-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	missing := filepath.Join(dir, "missing.sock")
	missingHealth := runtimeSocketProbe(missing, runtimeProcScanTimeout)
	if missingHealth.Exists || missingHealth.Reachable || missingHealth.Stale {
		t.Fatalf("missing socket health = %+v, want all false", missingHealth)
	}

	stale := filepath.Join(dir, "stale.sock")
	if err := os.WriteFile(stale, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	staleHealth := runtimeSocketProbe(stale, runtimeProcScanTimeout)
	if !staleHealth.Exists || staleHealth.Reachable || !staleHealth.Stale {
		t.Fatalf("stale socket health = %+v, want exists stale unreachable", staleHealth)
	}

	live := filepath.Join(dir, "live.sock")
	ln, err := net.Listen("unix", live)
	if err != nil {
		t.Fatalf("listen live socket fixture: %v", err)
	}
	defer ln.Close()
	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
		close(accepted)
	}()
	liveHealth := runtimeSocketProbe(live, runtimeProcScanTimeout)
	if !liveHealth.Exists || !liveHealth.Reachable || liveHealth.Stale {
		t.Fatalf("live socket health = %+v, want exists reachable not stale", liveHealth)
	}
	<-accepted
}

func TestRuntimePIDFileProbeClassifiesLiveAndStale(t *testing.T) {
	dir := t.TempDir()
	missing := runtimePIDFileProbe(filepath.Join(dir, "missing.pid"), nil)
	if missing.PID != 0 || missing.Live || missing.Stale {
		t.Fatalf("missing pidfile health = %+v, want empty", missing)
	}

	badPath := filepath.Join(dir, "bad.pid")
	if err := os.WriteFile(badPath, []byte("not-a-pid"), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := runtimePIDFileProbe(badPath, nil)
	if bad.PID != 0 || bad.Live || !bad.Stale {
		t.Fatalf("bad pidfile health = %+v, want stale", bad)
	}

	livePath := filepath.Join(dir, "live.pid")
	if err := os.WriteFile(livePath, []byte("42\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	live := runtimePIDFileProbe(livePath, func(pid int) bool { return pid == 42 })
	if live.PID != 42 || !live.Live || live.Stale {
		t.Fatalf("live pidfile health = %+v, want live pid 42", live)
	}

	stale := runtimePIDFileProbe(livePath, func(int) bool { return false })
	if stale.PID != 42 || stale.Live || !stale.Stale {
		t.Fatalf("stale pidfile health = %+v, want stale pid 42", stale)
	}
}

func TestRuntimeDiscoveryExpandsDefaultDaemonDataDir(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()
	t.Setenv("HOME", filepath.Join(dir, "home"))

	homeDataDir := filepath.Join(os.Getenv("HOME"), ".entmoot")
	pidRoot := filepath.Join(procRoot, "34567")
	if err := os.MkdirAll(filepath.Join(pidRoot, "root"+homeDataDir), 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/usr/local/bin/entmootd",
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}
	sockPath := filepath.Join(pidRoot, "root"+homeDataDir, "control.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen fake default control socket: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	gf := &globalFlags{
		socket:   "/tmp/pilot.sock",
		data:     homeDataDir,
		identity: filepath.Join(homeDataDir, "identity.json"),
	}
	report := collectRuntimeReport(gf, gf.data)
	if report.RunningDaemon == nil {
		t.Fatal("expected default-data running daemon")
	}
	daemon := report.RunningDaemon
	if daemon.DataDir != homeDataDir {
		t.Fatalf("daemon data dir = %q, want %q", daemon.DataDir, homeDataDir)
	}
	if daemon.ControlSocket != filepath.Join(homeDataDir, "control.sock") {
		t.Fatalf("daemon control socket = %q", daemon.ControlSocket)
	}
	if daemon.ControlSocketViaPID == "" {
		t.Fatalf("expected control socket via pid, daemon=%+v", daemon)
	}
	if report.NamespaceWarning == "" {
		t.Fatalf("expected namespace warning, report=%+v", report)
	}
}

func TestRuntimeDiscoveryIgnoresUnrelatedDaemon(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	unrelatedDataDir := "/data/.entmoot-b"
	pidRoot := filepath.Join(procRoot, "45678")
	if err := os.MkdirAll(filepath.Join(pidRoot, "root", "data", ".entmoot-b"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/data/.entmoot-b/bin/entmootd",
		"-data", unrelatedDataDir,
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}
	sockPath := filepath.Join(pidRoot, "root", "data", ".entmoot-b", "control.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unrelated fake control socket: %v", err)
	}
	defer ln.Close()

	gf := &globalFlags{
		socket:   "/tmp/pilot.sock",
		data:     "/data/.entmoot-a",
		identity: "/data/.entmoot-a/identity.json",
	}
	report := collectRuntimeReport(gf, gf.data)
	if report.RunningDaemon != nil {
		t.Fatalf("expected no matched daemon, got %+v", report.RunningDaemon)
	}
	help := runtimeNoDaemonHelp(gf, gf.data)
	for _, bad := range []string{"runtime warning:", "nsenter", "docker exec", "45678", unrelatedDataDir} {
		if strings.Contains(help, bad) {
			t.Fatalf("help contains unrelated daemon detail %q:\n%s", bad, help)
		}
	}
}

func TestRuntimeDiscoveryIgnoresNestedESPServe(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	pidRoot := filepath.Join(procRoot, "56789")
	if err := os.MkdirAll(pidRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/data/.entmoot/bin/entmootd",
		"-data", "/data/.entmoot",
		"esp",
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}

	gf := &globalFlags{
		socket:   "/tmp/pilot.sock",
		data:     "/data/.entmoot",
		identity: "/data/.entmoot/identity.json",
	}
	report := collectRuntimeReport(gf, gf.data)
	if report.RunningDaemon != nil {
		t.Fatalf("expected esp serve to be ignored, got %+v", report.RunningDaemon)
	}
}

func TestRuntimeDiscoveryParsesDoubleDashGlobalFlags(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	dataDir := "/data/.entmoot"
	pidRoot := filepath.Join(procRoot, "67890")
	if err := os.MkdirAll(filepath.Join(pidRoot, "root", "data", ".entmoot"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmdline := strings.Join([]string{
		"/data/.entmoot/bin/entmootd",
		"--socket", "/data/.pilot/pilot.sock",
		"--identity", "/data/.entmoot/identity.json",
		"--data", dataDir,
		"serve",
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}

	daemons := discoverRuntimeDaemons()
	if len(daemons) != 1 {
		t.Fatalf("daemons = %d, want 1: %+v", len(daemons), daemons)
	}
	daemon := daemons[0]
	if daemon.DataDir != dataDir {
		t.Fatalf("data dir = %q, want %q", daemon.DataDir, dataDir)
	}
	if daemon.IdentityPath != "/data/.entmoot/identity.json" {
		t.Fatalf("identity = %q", daemon.IdentityPath)
	}
	if daemon.PilotSocket != "/data/.pilot/pilot.sock" {
		t.Fatalf("pilot socket = %q", daemon.PilotSocket)
	}
}

func TestRuntimeReportDoesNotExposeDaemonCommandLine(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc runtime discovery is linux-only")
	}
	dir := t.TempDir()
	oldProcRoot := procRoot
	procRoot = filepath.Join(dir, "proc")
	defer func() { procRoot = oldProcRoot }()

	pidRoot := filepath.Join(procRoot, "23456")
	if err := os.MkdirAll(filepath.Join(pidRoot, "root", "data", ".entmoot"), 0o755); err != nil {
		t.Fatal(err)
	}
	secretInvite := "entmoot://open-invite?issuer=https://esp.example&token=secret-token"
	cmdline := strings.Join([]string{
		"/data/.entmoot/bin/entmootd",
		"-socket", "/data/.pilot/pilot.sock",
		"-identity", "/data/.entmoot/identity.json",
		"-data", "/data/.entmoot",
		"join",
		secretInvite,
	}, "\x00") + "\x00"
	if err := os.WriteFile(filepath.Join(pidRoot, "cmdline"), []byte(cmdline), 0o644); err != nil {
		t.Fatal(err)
	}

	gf := &globalFlags{
		socket:   "/data/.pilot/pilot.sock",
		data:     "/data/.entmoot",
		identity: "/data/.entmoot/identity.json",
	}
	report := collectRuntimeReport(gf, gf.data)
	if report.RunningDaemon == nil {
		t.Fatal("expected running daemon")
	}
	if report.RunningDaemon.Subcommand != "join" {
		t.Fatalf("subcommand = %q", report.RunningDaemon.Subcommand)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-token") || strings.Contains(string(data), "open-invite") {
		t.Fatalf("runtime report leaked daemon argv: %s", data)
	}
	if strings.Contains(string(data), `"command"`) {
		t.Fatalf("runtime report still exposes command field: %s", data)
	}
}

func TestRedactDoctorReportOmitsRuntime(t *testing.T) {
	report := &doctorReport{
		Runtime: &runtimeReport{
			Binary:        "/data/.entmoot/bin/entmootd",
			DataDir:       "/data/.entmoot",
			IdentityPath:  "/data/.entmoot/identity.json",
			PilotSocket:   "/data/.pilot/pilot.sock",
			ControlSocket: "/data/.entmoot/control.sock",
			RunningDaemon: &runtimeDaemonReport{
				PID:                 12345,
				ControlSocketViaPID: "/proc/12345/root/data/.entmoot/control.sock",
			},
		},
		Entmoot: doctorEntmootReport{DataDir: "/data/.entmoot"},
	}
	redactDoctorReport(report)
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	if strings.Contains(out, `"runtime"`) || strings.Contains(out, "/data/.entmoot") || strings.Contains(out, "/proc/12345") {
		t.Fatalf("redacted doctor report leaked runtime fields: %s", out)
	}
	if strings.Contains(out, "/data/.entmoot") {
		t.Fatalf("redacted doctor report leaked entmoot data dir: %s", out)
	}
}
