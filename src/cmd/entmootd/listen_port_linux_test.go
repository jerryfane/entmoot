//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// documentedDefaultPort is the -listen-port default agents get without the
// flag: privileged, so a non-root agent cannot bind it.
const documentedDefaultPort = 1004

// A non-root agent follows the plain instructions - `serve` and `join` with no
// -listen-port - on a host where the default port 1004 cannot be bound. Both
// start on an OS-assigned port and report it, and the founder's reported port
// is the one the joiner dials. An explicit -listen-port that cannot be bound
// still fails.
func TestDefaultListenPortFallsBackWhenUnbindable(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the daemon")
	}
	binary := filepath.Join(t.TempDir(), "entmootd")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	// The permission case runs the binary as another user: t.TempDir and
	// its parent are created 0700.
	for _, path := range []string{filepath.Dir(binary), filepath.Dir(filepath.Dir(binary))} {
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("in use", func(t *testing.T) {
		// Hold the port without SO_REUSEPORT. If something else already
		// holds it, or it is privileged for this user, it is unbindable all
		// the same.
		holder, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", documentedDefaultPort))
		switch {
		case err == nil:
			t.Cleanup(func() { _ = holder.Close() })
		case errors.Is(err, syscall.EADDRINUSE), errors.Is(err, syscall.EACCES):
		default:
			t.Skipf("cannot make port %d unbindable: %v", documentedDefaultPort, err)
		}
		checkDefaultListenPortFallback(t, binary, nil, "")
	})

	t.Run("permission denied", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("needs root to run the daemon as an unprivileged user")
		}
		raw, err := os.ReadFile("/proc/sys/net/ipv4/ip_unprivileged_port_start")
		if err != nil {
			t.Skip(err)
		}
		if start, err := strconv.Atoi(strings.TrimSpace(string(raw))); err != nil || start <= documentedDefaultPort {
			t.Skipf("port %d is unprivileged here (ip_unprivileged_port_start=%s)", documentedDefaultPort, strings.TrimSpace(string(raw)))
		}
		nobody := &syscall.Credential{Uid: 65534, Gid: 65534}
		checkDefaultListenPortFallback(t, binary, nobody, "permission denied")
	})
}

func checkDefaultListenPortFallback(t *testing.T, binary string, user *syscall.Credential, bindErr string) {
	t.Helper()
	dir := t.TempDir()
	for _, node := range []string{"founder", "joiner"} {
		data := filepath.Join(dir, node)
		if err := os.Mkdir(data, 0o700); err != nil {
			t.Fatal(err)
		}
		if user != nil {
			if err := os.Chown(data, int(user.Uid), int(user.Gid)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if user != nil {
		for _, path := range []string{dir, filepath.Dir(dir)} {
			if err := os.Chmod(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	// No SO_REUSEPORT: libp2p must not share the port with whatever holds it.
	env := append(os.Environ(), "LIBP2P_TCP_REUSEPORT=false", "HERDR_SOCKET_PATH="+filepath.Join(dir, "no-herdr.sock"))
	command := func(node string, args ...string) *exec.Cmd {
		data := filepath.Join(dir, node)
		global := []string{"-identity", filepath.Join(data, "identity.json"), "-data", data}
		cmd := exec.Command(binary, append(global, args...)...)
		cmd.Env = env
		if user != nil {
			cmd.SysProcAttr = &syscall.SysProcAttr{Credential: user}
		}
		return cmd
	}
	run := func(node string, args ...string) (string, string, error) {
		cmd := command(node, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		return stdout.String(), stderr.String(), err
	}
	mustRun := func(node string, args ...string) string {
		t.Helper()
		stdout, stderr, err := run(node, args...)
		if err != nil {
			t.Fatalf("%s %v: %v\n%s%s", node, args, err, stdout, stderr)
		}
		return stdout
	}
	type info struct {
		PeerID        string `json:"peer_id"`
		EntmootPubKey string `json:"entmoot_pubkey"`
		ListenPort    int    `json:"listen_port"`
		Running       bool   `json:"running"`
	}
	readInfo := func(node string) (info, error) {
		stdout, stderr, err := run(node, "info")
		if err != nil {
			return info{}, fmt.Errorf("%v: %s", err, stderr)
		}
		var got info
		return got, json.Unmarshal([]byte(stdout), &got)
	}

	var group struct {
		GroupID string `json:"group_id"`
	}
	created := mustRun("founder", "-allow-new-identity", "group", "create", "-name", "listen-port", "-policy", "none", "-json")
	if err := json.Unmarshal([]byte(created), &group); err != nil {
		t.Fatalf("group create output %q: %v", created, err)
	}
	mustRun("joiner", "-allow-new-identity", "info")

	// An explicit port is the operator's choice: it is not replaced.
	explicit := []string{"-listen-port", strconv.Itoa(documentedDefaultPort)}
	_, stderr, err := run("founder", append(explicit, "serve")...)
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != exitTransport {
		t.Fatalf("explicit -listen-port serve: %v, want exit %d\n%s", err, exitTransport, stderr)
	}
	if !strings.Contains(stderr, "libp2p host") || !strings.Contains(stderr, bindErr) {
		t.Fatalf("explicit -listen-port serve did not fail binding the port:\n%s", stderr)
	}

	logPath := filepath.Join(dir, "serve.log")
	log, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	serve := command("founder", "serve")
	serve.Stdout, serve.Stderr = log, log
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	_ = log.Close()
	t.Cleanup(func() {
		_ = serve.Process.Signal(syscall.SIGTERM)
		_ = serve.Wait()
	})
	// serve prints its event once the host is up and the control socket is
	// listening.
	var serving struct {
		Event      string `json:"event"`
		ListenPort int    `json:"listen_port"`
	}
	var raw []byte
	for deadline := time.Now().Add(30 * time.Second); serving.Event == ""; {
		raw, _ = os.ReadFile(logPath)
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &serving) == nil {
				break
			}
		}
		if serving.Event == "" && time.Now().After(deadline) {
			t.Fatalf("default serve never started:\n%s", raw)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if serving.Event != "serving" || serving.ListenPort == 0 || serving.ListenPort == documentedDefaultPort {
		t.Fatalf("serve event %+v, want serving on an OS-assigned port\nserve log:\n%s", serving, raw)
	}
	if !bytes.Contains(raw, []byte("default listen port unavailable")) {
		t.Fatalf("serve did not log the fallback:\n%s", raw)
	}
	founder, err := readInfo("founder")
	if err != nil || !founder.Running || founder.ListenPort != serving.ListenPort {
		t.Fatalf("founder info %+v (%v), want running on listen_port %d", founder, err, serving.ListenPort)
	}

	joiner, err := readInfo("joiner")
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", founder.ListenPort, founder.PeerID)
	invite := filepath.Join(dir, "invite.json")
	if err := os.WriteFile(invite, []byte(mustRun("founder", "invite", "create", "-group", group.GroupID,
		"-target-pubkey", joiner.EntmootPubKey, "-bootstrap", bootstrap, "-no-fallback-peers")), 0o644); err != nil {
		t.Fatal(err)
	}

	_, stderr, err = run("joiner", append(explicit, "join", invite)...)
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != exitTransport {
		t.Fatalf("explicit -listen-port join: %v, want exit %d\n%s", err, exitTransport, stderr)
	}

	stdout := mustRun("joiner", "join", invite)
	var joined struct {
		Event      string `json:"event"`
		GroupID    string `json:"group_id"`
		ListenPort int    `json:"listen_port"`
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &joined); err != nil {
		t.Fatalf("join output %q: %v", stdout, err)
	}
	if joined.Event != "joined" || joined.GroupID != group.GroupID {
		t.Fatalf("join event %+v, want joined %s", joined, group.GroupID)
	}
	if joined.ListenPort == 0 || joined.ListenPort == documentedDefaultPort {
		t.Fatalf("join reports listen_port %d, want the OS-assigned port", joined.ListenPort)
	}
}
