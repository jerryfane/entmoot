//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// denyUnixSocketsOnThisThread makes socket(AF_UNIX, ...) on the calling OS
// thread, and in processes it forks, fail with EPERM, as the seccomp-confined
// cloud runtime did. The caller locks its goroutine to the thread and exits
// without unlocking it, so the runtime discards the filtered thread.
func denyUnixSocketsOnThisThread() error {
	var arch uint32
	switch runtime.GOARCH {
	case "amd64":
		arch = unix.AUDIT_ARCH_X86_64
	case "arm64":
		arch = unix.AUDIT_ARCH_AARCH64
	default:
		return fmt.Errorf("no seccomp audit arch for %s", runtime.GOARCH)
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4}, // seccomp_data.arch
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 5, K: arch},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}, // seccomp_data.nr
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 3, K: unix.SYS_SOCKET},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16}, // args[0], little endian
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, Jf: 1, K: unix.AF_UNIX},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	return unix.Prctl(unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&prog)), 0, 0)
}

// startWithoutUnixSockets starts cmd in a process that cannot create Unix
// sockets; the seccomp filter is inherited across fork and exec.
func startWithoutUnixSockets(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		if err := denyUnixSocketsOnThisThread(); err != nil {
			done <- fmt.Errorf("deny Unix sockets: %w", err)
			return
		}
		done <- cmd.Start()
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// An agent in a runtime that forbids Unix sockets follows the plain
// instructions: `serve` with no transport flag, then plain `info` and
// `publish`. Explicit `-control-transport unix` still refuses to fall back.
func TestServeWithoutUnixSocketsUsesAuthenticatedTCPControl(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the daemon")
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "entmootd")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data := filepath.Join(dir, "data")
	if err := os.MkdirAll(data, 0700); err != nil {
		t.Fatal(err)
	}
	global := []string{"-identity", filepath.Join(dir, "identity.json"), "-data", data}
	env := append(os.Environ(), "HERDR_SOCKET_PATH="+filepath.Join(dir, "no-herdr.sock"))
	command := func(args ...string) *exec.Cmd {
		cmd := exec.Command(binary, append(append([]string{}, global...), args...)...)
		cmd.Env = env
		return cmd
	}
	run := func(args ...string) (string, string, error) {
		cmd := command(args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		startWithoutUnixSockets(t, cmd)
		err := cmd.Wait()
		return stdout.String(), stderr.String(), err
	}
	if _, stderr, err := run("-allow-new-identity", "group", "create", "-name", "unix-denied"); err != nil {
		t.Fatalf("group create: %v\n%s", err, stderr)
	}
	serveArgs := []string{"-p2p-listen", "/ip4/127.0.0.1/tcp/0", "serve"}

	_, stderr, err := run(append([]string{"-control-transport", "unix"}, serveArgs...)...)
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != exitTransport {
		t.Fatalf("strict unix serve: %v, want exit %d\n%s", err, exitTransport, stderr)
	}
	if !strings.Contains(stderr, "operation not permitted") {
		t.Fatalf("strict unix serve did not hit the denied socket:\n%s", stderr)
	}

	serve := func(name string) *exec.Cmd {
		log, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer log.Close()
		cmd := command(serveArgs...)
		cmd.Stdout, cmd.Stderr = log, log
		startWithoutUnixSockets(t, cmd)
		t.Cleanup(func() {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		})
		return cmd
	}
	waitRunning := func(log string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			stdout, _, err := run("info")
			var info struct {
				Running bool `json:"running"`
			}
			if err == nil && json.Unmarshal([]byte(stdout), &info) == nil && info.Running {
				return
			}
			if time.Now().After(deadline) {
				raw, _ := os.ReadFile(filepath.Join(dir, log))
				t.Fatalf("plain info never saw a running daemon: %v %s\nserve log:\n%s", err, stdout, raw)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	first := serve("serve-1.log")
	waitRunning("serve-1.log")
	if info, err := os.Lstat(filepath.Join(data, "control.sock")); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("control endpoint is not a private credential file: %v %v", info, err)
	}
	if stdout, stderr, err := run("publish", "-topic", "unix/denied", "-content", "hello"); err != nil || !strings.Contains(stdout, `"message_id"`) {
		t.Fatalf("plain publish: %v\n%s%s", err, stdout, stderr)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "serve-1.log")); !bytes.Contains(raw, []byte("unix control socket forbidden; serving authenticated loopback tcp control")) {
		t.Fatalf("serve did not report the control fallback:\n%s", raw)
	}

	// A killed daemon leaves its endpoint file; the next default serve replaces it.
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	serve("serve-2.log")
	waitRunning("serve-2.log")
	if stdout, stderr, err := run("publish", "-topic", "unix/denied", "-content", "after restart"); err != nil || !strings.Contains(stdout, `"message_id"`) {
		t.Fatalf("plain publish after restart: %v\n%s%s", err, stdout, stderr)
	}

	// A group created while the daemon runs is started in it over the same
	// tcp control endpoint, and the daemon then publishes to it.
	stdout, stderr, err := run("group", "create", "-name", "unix-denied-second", "-json")
	var second struct {
		GroupID          string `json:"group_id"`
		DaemonActivation string `json:"daemon_activation"`
	}
	if err != nil || json.Unmarshal([]byte(stdout), &second) != nil || second.DaemonActivation != "activated" {
		t.Fatalf("group create under the tcp-control daemon: %v %+v\n%s%s", err, second, stdout, stderr)
	}
	if stdout, stderr, err := run("publish", "-group", second.GroupID, "-topic", "unix/denied", "-content", "new group"); err != nil || !strings.Contains(stdout, `"message_id"`) {
		t.Fatalf("publish to the new group: %v\n%s%s", err, stdout, stderr)
	}
}
