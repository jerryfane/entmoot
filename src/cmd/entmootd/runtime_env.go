package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	defaultEntmootDataDir  = "~/.entmoot"
	agentPilotSocketPath   = "/data/.pilot/pilot.sock"
	agentEntmootDataPath   = "/data/.entmoot"
	agentEntmootWrapper    = "/data/.entmoot/entmoot"
	agentPilotWrapper      = "/data/.pilot/pilot"
	agentStackHelper       = "/data/.pilot/start-entmoot-stack.sh"
	runtimeProcScanTimeout = 250 * time.Millisecond
)

const (
	runtimeStatusHealthy   = "healthy"
	runtimeStatusHalfAlive = "half_alive"
	runtimeStatusDown      = "down"
)

var procRoot = "/proc"

type runtimeReport struct {
	Binary                 string               `json:"binary,omitempty"`
	UID                    int                  `json:"uid"`
	GID                    int                  `json:"gid"`
	RuntimeStatus          string               `json:"runtime_status"`
	RuntimeStatusReason    string               `json:"runtime_status_reason,omitempty"`
	DataDir                string               `json:"data_dir"`
	IdentityPath           string               `json:"identity_path"`
	PilotSocket            string               `json:"pilot_socket"`
	PilotSocketReachable   bool                 `json:"pilot_socket_reachable"`
	ControlSocket          string               `json:"control_socket"`
	ControlSocketReachable bool                 `json:"control_socket_reachable"`
	PublishPathHealthy     bool                 `json:"publish_path_healthy"`
	AgentWrapper           string               `json:"agent_wrapper,omitempty"`
	PilotWrapper           string               `json:"pilot_wrapper,omitempty"`
	StackHelper            string               `json:"stack_helper,omitempty"`
	RunningDaemon          *runtimeDaemonReport `json:"running_daemon,omitempty"`
	NamespaceWarning       string               `json:"namespace_warning,omitempty"`
	Suggestions            []string             `json:"suggestions,omitempty"`
	Recommended            map[string]string    `json:"recommended,omitempty"`
	Platform               map[string]string    `json:"platform,omitempty"`
}

type runtimeDaemonReport struct {
	PID                 int    `json:"pid"`
	Binary              string `json:"binary,omitempty"`
	DataDir             string `json:"data_dir,omitempty"`
	IdentityPath        string `json:"identity_path,omitempty"`
	PilotSocket         string `json:"pilot_socket,omitempty"`
	ControlSocket       string `json:"control_socket,omitempty"`
	ControlSocketViaPID string `json:"control_socket_via_pid,omitempty"`
	MountNamespace      string `json:"mount_namespace,omitempty"`
	NetNamespace        string `json:"net_namespace,omitempty"`
	Subcommand          string `json:"subcommand,omitempty"`
	ContainerLike       bool   `json:"container_like,omitempty"`
}

type runtimeSocketHealth struct {
	Path      string
	Exists    bool
	Reachable bool
	Stale     bool
}

func runtimeSocketProbe(path string, timeout time.Duration) runtimeSocketHealth {
	health := runtimeSocketHealth{Path: path, Exists: pathExists(path)}
	health.Reachable = controlSocketAlive(path, timeout)
	health.Stale = health.Exists && !health.Reachable
	return health
}

type runtimePIDFileHealth struct {
	Path  string
	PID   int
	Live  bool
	Stale bool
}

func runtimePIDFileProbe(path string, live func(int) bool) runtimePIDFileHealth {
	health := runtimePIDFileHealth{Path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		return health
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		health.Stale = true
		return health
	}
	health.PID = pid
	if live != nil && live(pid) {
		health.Live = true
		return health
	}
	health.Stale = true
	return health
}

func cmdEnv(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("env", flag.ContinueOnError)
	jsonOut := fs.Bool("json", false, "emit JSON instead of a human summary")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	report := collectRuntimeReport(gf, gf.data)
	if *jsonOut {
		data, err := json.Marshal(report)
		if err != nil {
			fmt.Fprintf(os.Stderr, "env: marshal: %v\n", err)
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}
	printRuntimeReport(report)
	return exitOK
}

func collectRuntimeReport(gf *globalFlags, dataDir string) runtimeReport {
	bin, _ := os.Executable()
	controlSock := controlSocketPath(dataDir)
	pilotSocket := runtimeSocketProbe(gf.socket, runtimeProcScanTimeout)
	controlSocket := runtimeSocketProbe(controlSock, runtimeProcScanTimeout)
	report := runtimeReport{
		Binary:                 bin,
		UID:                    os.Getuid(),
		GID:                    os.Getgid(),
		DataDir:                dataDir,
		IdentityPath:           gf.identity,
		PublishPathHealthy:     controlSocket.Reachable,
		PilotSocket:            gf.socket,
		PilotSocketReachable:   pilotSocket.Reachable,
		ControlSocket:          controlSock,
		ControlSocketReachable: controlSocket.Reachable,
		Recommended: map[string]string{
			"agent_pilot_socket": agentPilotSocketPath,
			"agent_data_dir":     agentEntmootDataPath,
		},
		Platform: map[string]string{
			"goos":   runtime.GOOS,
			"goarch": runtime.GOARCH,
		},
	}
	if fileExists(agentEntmootWrapper) {
		report.AgentWrapper = agentEntmootWrapper
	}
	if fileExists(agentPilotWrapper) {
		report.PilotWrapper = agentPilotWrapper
	}
	if fileExists(agentStackHelper) {
		report.StackHelper = agentStackHelper
	}
	if daemon := findBestRuntimeDaemon(dataDir, controlSock); daemon != nil {
		report.RunningDaemon = daemon
		if !report.ControlSocketReachable && daemon.ControlSocketViaPID != "" && controlSocketAlive(daemon.ControlSocketViaPID, runtimeProcScanTimeout) {
			report.NamespaceWarning = fmt.Sprintf("a running entmootd daemon was found at pid %d, but its control socket is only reachable through that process namespace", daemon.PID)
			report.Suggestions = append(report.Suggestions,
				fmt.Sprintf("run commands in the daemon namespace: nsenter --target %d --mount -- entmootd -socket %s -identity %s -data %s <command>", daemon.PID, nonEmpty(daemon.PilotSocket, gf.socket), nonEmpty(daemon.IdentityPath, gf.identity), nonEmpty(daemon.DataDir, dataDir)),
			)
			if daemon.ContainerLike {
				report.Suggestions = append(report.Suggestions, "if this daemon is inside Docker/OpenClaw, prefer: docker exec -u node <container> /data/.entmoot/entmoot <command>")
			}
		}
	}
	report.RuntimeStatus, report.RuntimeStatusReason = classifyRuntimeStatus(report)
	if report.RuntimeStatus == runtimeStatusHalfAlive {
		report.Suggestions = append(report.Suggestions, "process discovery found entmootd, but the direct publish path is not healthy; trust socket probes over process lists")
		if report.StackHelper != "" {
			report.Suggestions = append(report.Suggestions, fmt.Sprintf("check the managed stack without mutation: %s check", report.StackHelper))
		}
	}
	if strings.HasPrefix(dataDir, "/data/") && gf.socket == "/tmp/pilot.sock" {
		report.Suggestions = append(report.Suggestions, "for containerized agents, prefer -socket /data/.pilot/pilot.sock and keep /tmp/pilot.sock as a compatibility symlink")
	}
	return report
}

func classifyRuntimeStatus(report runtimeReport) (string, string) {
	if report.PublishPathHealthy {
		return runtimeStatusHealthy, ""
	}
	if report.RunningDaemon != nil {
		return runtimeStatusHalfAlive, "running entmootd process found, but the control socket is not reachable"
	}
	return runtimeStatusDown, "control socket is not reachable"
}

func printRuntimeReport(report runtimeReport) {
	fmt.Printf("binary: %s\n", report.Binary)
	fmt.Printf("uid: %d gid: %d\n", report.UID, report.GID)
	fmt.Printf("runtime_status: %s\n", report.RuntimeStatus)
	if report.RuntimeStatusReason != "" {
		fmt.Printf("runtime_status_reason: %s\n", report.RuntimeStatusReason)
	}
	fmt.Printf("publish_path_healthy: %t\n", report.PublishPathHealthy)
	fmt.Printf("data: %s\n", report.DataDir)
	fmt.Printf("identity: %s\n", report.IdentityPath)
	fmt.Printf("pilot_socket: %s reachable=%t\n", report.PilotSocket, report.PilotSocketReachable)
	fmt.Printf("control_socket: %s reachable=%t\n", report.ControlSocket, report.ControlSocketReachable)
	if report.AgentWrapper != "" {
		fmt.Printf("agent_wrapper: %s\n", report.AgentWrapper)
	}
	if d := report.RunningDaemon; d != nil {
		fmt.Printf("running_daemon: pid=%d data=%s socket=%s\n", d.PID, d.DataDir, d.ControlSocket)
		if d.ControlSocketViaPID != "" {
			fmt.Printf("running_daemon_socket_via_pid: %s\n", d.ControlSocketViaPID)
		}
	}
	if report.NamespaceWarning != "" {
		fmt.Printf("warning: %s\n", report.NamespaceWarning)
	}
	for _, suggestion := range report.Suggestions {
		fmt.Printf("suggestion: %s\n", suggestion)
	}
}

func runtimeNoDaemonHelp(gf *globalFlags, dataDir string) string {
	report := collectRuntimeReport(gf, dataDir)
	if report.NamespaceWarning == "" {
		return noJoinHelp
	}
	var b strings.Builder
	b.WriteString(noJoinHelp)
	b.WriteString("\n")
	b.WriteString("runtime warning: ")
	b.WriteString(report.NamespaceWarning)
	b.WriteString("\n")
	b.WriteString("current control socket: ")
	b.WriteString(report.ControlSocket)
	if d := report.RunningDaemon; d != nil {
		b.WriteString("\ndetected daemon: pid=")
		b.WriteString(strconv.Itoa(d.PID))
		b.WriteString(" data=")
		b.WriteString(d.DataDir)
		b.WriteString(" control_socket=")
		b.WriteString(d.ControlSocket)
	}
	for _, suggestion := range report.Suggestions {
		b.WriteString("\nsuggestion: ")
		b.WriteString(suggestion)
	}
	return b.String()
}

func findBestRuntimeDaemon(dataDir, controlSock string) *runtimeDaemonReport {
	daemons := discoverRuntimeDaemons()
	if len(daemons) == 0 {
		return nil
	}
	normalizedDataDir := normalizeRuntimeDataDir(dataDir)
	normalizedControlSock := controlSocketPath(normalizedDataDir)
	for i := range daemons {
		if runtimeDaemonMatches(daemons[i], normalizedDataDir, controlSock, normalizedControlSock) {
			return &daemons[i]
		}
	}
	return nil
}

func runtimeDaemonMatches(daemon runtimeDaemonReport, dataDir, controlSock, normalizedControlSock string) bool {
	return daemon.DataDir == dataDir || daemon.ControlSocket == controlSock || daemon.ControlSocket == normalizedControlSock
}

func discoverRuntimeDaemons() []runtimeDaemonReport {
	if runtime.GOOS != "linux" {
		return nil
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return nil
	}
	out := make([]runtimeDaemonReport, 0, 2)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		args, err := readProcCmdline(pid)
		if err != nil || !isEntmootDaemonCmd(args) {
			continue
		}
		daemon := runtimeDaemonReport{
			PID:            pid,
			Binary:         firstArg(args),
			DataDir:        parseFlagValue(args, "-data"),
			IdentityPath:   parseFlagValue(args, "-identity"),
			PilotSocket:    parseFlagValue(args, "-socket"),
			Subcommand:     daemonSubcommand(args),
			MountNamespace: readlinkQuiet(procPath(pid, "ns/mnt")),
			NetNamespace:   readlinkQuiet(procPath(pid, "ns/net")),
			ContainerLike:  fileExists(procPath(pid, "root/.dockerenv")),
		}
		daemon.DataDir = normalizeRuntimeDataDir(daemon.DataDir)
		if daemon.PilotSocket == "" {
			daemon.PilotSocket = "/tmp/pilot.sock"
		}
		daemon.ControlSocket = controlSocketPath(daemon.DataDir)
		if strings.HasPrefix(daemon.ControlSocket, "/") {
			viaPID := procPath(pid, "root"+daemon.ControlSocket)
			if fileExists(viaPID) {
				daemon.ControlSocketViaPID = viaPID
			}
		}
		out = append(out, daemon)
	}
	return out
}

func normalizeRuntimeDataDir(path string) string {
	if path == "" {
		path = defaultEntmootDataDir
	}
	expanded, err := expandHome(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(expanded)
}

func readProcCmdline(pid int) ([]string, error) {
	data, err := os.ReadFile(procPath(pid, "cmdline"))
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
	if len(parts) == 1 && parts[0] == "" {
		return nil, errors.New("empty cmdline")
	}
	return parts, nil
}

func isEntmootDaemonCmd(args []string) bool {
	if len(args) == 0 || filepath.Base(args[0]) != "entmootd" {
		return false
	}
	return daemonSubcommand(args) != ""
}

func daemonSubcommand(args []string) string {
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i+1 < len(args) && isRuntimeDaemonSubcommand(args[i+1]) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(arg, "-") && arg != "-" {
			name := strings.TrimLeft(arg, "-")
			if cut := strings.IndexByte(name, '='); cut >= 0 {
				name = name[:cut]
			}
			if runtimeGlobalFlagTakesValue(name) && !strings.Contains(arg, "=") {
				i++
			}
			continue
		}
		if isRuntimeDaemonSubcommand(arg) {
			return arg
		}
		return ""
	}
	return ""
}

func isRuntimeDaemonSubcommand(arg string) bool {
	return arg == "serve" || arg == "join"
}

func runtimeGlobalFlagTakesValue(name string) bool {
	switch name {
	case "socket", "identity", "data", "listen-port", "log-level", "pilot-wait-timeout", "pilot-wait-base-delay", "pilot-wait-max-delay":
		return true
	default:
		return false
	}
}

func parseFlagValue(args []string, name string) string {
	names := []string{name}
	if strings.HasPrefix(name, "-") && !strings.HasPrefix(name, "--") {
		names = append(names, "-"+name)
	}
	for i, arg := range args {
		for _, candidate := range names {
			prefix := candidate + "="
			if strings.HasPrefix(arg, prefix) {
				return strings.TrimPrefix(arg, prefix)
			}
			if arg == candidate && i+1 < len(args) {
				return args[i+1]
			}
		}
	}
	return ""
}

func procPath(pid int, suffix string) string {
	return filepath.Join(procRoot, strconv.Itoa(pid), suffix)
}

func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func readlinkQuiet(path string) string {
	v, _ := os.Readlink(path)
	return v
}

func nonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
