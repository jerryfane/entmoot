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
