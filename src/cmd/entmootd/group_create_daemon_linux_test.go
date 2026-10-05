//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// An operator whose daemon is already serving creates another group and
// invites someone into it. The running daemon serves the new group at once:
// the invitee joins without anyone restarting the founder (#214).
func TestGroupCreateWhileServingActivatesGroup(t *testing.T) {
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
	for _, node := range []string{"founder", "joiner"} {
		if err := os.Mkdir(filepath.Join(dir, node), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	env := append(os.Environ(), "HOME="+dir, "HERDR_SOCKET_PATH="+filepath.Join(dir, "no-herdr.sock"))
	// Both nodes listen on loopback only. The founder takes a port picked
	// here, so the joiner's invite can name it.
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	founderPort := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()
	listen := map[string]string{
		"founder": fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", founderPort),
		"joiner":  "/ip4/127.0.0.1/tcp/0",
	}
	command := func(node string, args ...string) *exec.Cmd {
		data := filepath.Join(dir, node)
		global := []string{"-identity", filepath.Join(data, "identity.json"), "-data", data, "-p2p-listen", listen[node]}
		cmd := exec.Command(binary, append(global, args...)...)
		cmd.Env = env
		cmd.Dir = dir
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
	type created struct {
		GroupID          string `json:"group_id"`
		DaemonActivation string `json:"daemon_activation"`
	}
	createGroup := func(name string) created {
		t.Helper()
		var out created
		raw := mustRun("founder", "-allow-new-identity", "group", "create", "-name", name, "-policy", "none", "-json")
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatalf("group create output %q: %v", raw, err)
		}
		return out
	}
	type info struct {
		PeerID        string `json:"peer_id"`
		EntmootPubKey string `json:"entmoot_pubkey"`
		Running       bool   `json:"running"`
	}
	readInfo := func(node string) info {
		t.Helper()
		var got info
		if err := json.Unmarshal([]byte(mustRun(node, "-allow-new-identity", "info")), &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	// The founder already serves one group.
	first := createGroup("first")
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
	var founder info
	for deadline := time.Now().Add(30 * time.Second); !founder.Running; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(logPath)
			t.Fatalf("founder daemon never came up\nserve log:\n%s", raw)
		}
		founder = readInfo("founder")
	}

	// While it runs, the founder creates a second group and invites the joiner.
	second := createGroup("second")
	joiner := readInfo("joiner")
	invite := filepath.Join(dir, "invite.json")
	bootstrap := fmt.Sprintf("%s/p2p/%s", listen["founder"], founder.PeerID)
	if err := os.WriteFile(invite, []byte(mustRun("founder", "invite", "create", "-group", second.GroupID,
		"-target-pubkey", joiner.EntmootPubKey, "-bootstrap", bootstrap, "-no-fallback-peers")), 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, err := run("joiner", "join", invite)
	if err != nil {
		raw, _ := os.ReadFile(logPath)
		t.Fatalf("join a group created under the running daemon: %v\n%s%s\nfounder serve log:\n%s", err, stdout, stderr, raw)
	}
	var joined struct {
		Event   string `json:"event"`
		GroupID string `json:"group_id"`
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &joined); err != nil {
		t.Fatalf("join output %q: %v", stdout, err)
	}
	if joined.Event != "joined" || joined.GroupID != second.GroupID {
		t.Fatalf("join event %+v, want joined %s", joined, second.GroupID)
	}
	// Each create says what it did about the daemon.
	if first.DaemonActivation != "daemon_not_running" || second.DaemonActivation != "activated" {
		t.Fatalf("daemon_activation without/with a running daemon: %q/%q, want daemon_not_running/activated", first.DaemonActivation, second.DaemonActivation)
	}
}
