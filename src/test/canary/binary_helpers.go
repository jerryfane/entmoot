package canary

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// buildEntmootdBinary compiles cmd/entmootd to a fresh file inside the
// test's tempdir and returns the absolute path. Fatal on build failure —
// a broken binary should be a loud test failure, not a skip.
func buildEntmootdBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "entmootd")
	// The working directory is the canary package; cmd/entmootd lives three
	// levels up (src/cmd/entmootd). Spell it out as an absolute path so we
	// do not depend on Go's relative package resolution past the go list
	// walk.
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("buildEntmootdBinary: os.Getwd: %v", err)
	}
	// test/canary → test → src. src/cmd/entmootd is the target package.
	srcRoot := filepath.Clean(filepath.Join(cwd, "..", ".."))
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/entmootd")
	cmd.Dir = srcRoot
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("buildEntmootdBinary: go build failed: %v\noutput:\n%s", err, out.String())
	}
	return bin
}

// runEntmootd runs `entmootd <args...>` as a one-shot subprocess and waits
// for exit. envExtra entries are appended to os.Environ(); pass an empty
// slice when no extra env is needed. Returns stdout, stderr, and the
// process exit code.
//
// Subprocess output is also appended to t.Log on non-zero exit so failures
// are immediately diagnosable. A 30-second hard deadline prevents a
// hanging subprocess from blocking the test suite.
func runEntmootd(t *testing.T, binary string, envExtra []string, args ...string) (string, string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), envExtra...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			// Could be a timeout or exec failure; report as -1 and log.
			t.Logf("runEntmootd: %v (args=%v)\nstderr=%s", err, args, stderr.String())
			code = -1
		}
	}
	if code != 0 {
		t.Logf("entmootd %v exited with code %d\nstdout=%s\nstderr=%s",
			args, code, stdout.String(), stderr.String())
	}
	return stdout.String(), stderr.String(), code
}

// joinHandle owns a backgrounded `entmootd join` subprocess and the
// goroutines draining its stdout/stderr.
type joinHandle struct {
	name    string
	cmd     *exec.Cmd
	stdout  *bufio.Scanner
	stderr  io.ReadCloser
	stdoutR io.ReadCloser

	// extraStdout holds post-joined-event stdout lines; readers can pop
	// them off as JSON frames arrive (the daemon writes nothing on stdout
	// after "joined" in v1, but reserve it for future diagnostics).
	mu         sync.Mutex
	extraLines []string

	done chan struct{}

	// stderrLines are appended to t.Log on Stop for post-mortem.
	stderrBuf strings.Builder
	stderrMu  sync.Mutex
}

// startEntmootdJoin launches `entmootd -socket SOCK -identity ID -data DIR
// -listen-port PORT join <invite>` in the background, waits for the
// `{"event":"joined",...}` line on stdout (with a 15-second timeout), and
// returns a handle the caller can Stop.
//
// This mirrors the blocking-process contract in CLI_DESIGN §3.1: join
// emits exactly one JSON stdout line and then nothing further during
// normal operation.
func startEntmootdJoin(
	t *testing.T,
	name string,
	binary string,
	pilotSocket string,
	dataDir string,
	identityPath string,
	listenPort int,
	invitePath string,
) *joinHandle {
	t.Helper()

	args := []string{
		"-socket", pilotSocket,
		"-identity", identityPath,
		"-data", dataDir,
		"-listen-port", fmt.Sprintf("%d", listenPort),
		"-log-level", "info",
		"join",
		invitePath,
	}

	cmd := exec.Command(binary, args...)
	// Give the child its own process group so Stop's SIGTERM does not
	// race signal delivery with the parent.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutR, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("startEntmootdJoin %s: StdoutPipe: %v", name, err)
	}
	stderrR, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("startEntmootdJoin %s: StderrPipe: %v", name, err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("startEntmootdJoin %s: start: %v", name, err)
	}

	h := &joinHandle{
		name:    name,
		cmd:     cmd,
		stderr:  stderrR,
		stdoutR: stdoutR,
		done:    make(chan struct{}),
	}
	h.stdout = bufio.NewScanner(stdoutR)
	// Lift the default 64 KB cap in case a future publish_resp or
	// tail_event gets pushed to stdout.
	h.stdout.Buffer(make([]byte, 0, 128*1024), 1024*1024)

	// Drain stderr into the handle buffer (with t.Log forwarding).
	go func() {
		scan := bufio.NewScanner(stderrR)
		scan.Buffer(make([]byte, 0, 128*1024), 1024*1024)
		for scan.Scan() {
			line := scan.Text()
			h.stderrMu.Lock()
			h.stderrBuf.WriteString(line)
			h.stderrBuf.WriteByte('\n')
			h.stderrMu.Unlock()
			// Tagged forward to t.Log. Keep slog format compact so test
			// logs stay readable when three peers run in parallel.
			t.Logf("[join %s stderr] %s", name, line)
		}
	}()

	// Block on the first stdout line for up to 15s. CLI_DESIGN §3.1 says
	// join emits exactly one JSON line before going quiet.
	type readResult struct {
		line string
		ok   bool
	}
	ch := make(chan readResult, 1)
	go func() {
		if h.stdout.Scan() {
			ch <- readResult{line: h.stdout.Text(), ok: true}
			return
		}
		ch <- readResult{ok: false}
	}()

	select {
	case r := <-ch:
		if !r.ok {
			// Wait briefly for the process to exit so we can print its
			// stderr — otherwise the test fails without context.
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			h.stderrMu.Lock()
			stderr := h.stderrBuf.String()
			h.stderrMu.Unlock()
			t.Fatalf("join %s: stdout closed before joined-event; stderr=\n%s", name, stderr)
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(r.line), &ev); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("join %s: malformed first stdout line %q: %v", name, r.line, err)
		}
		if ev["event"] != "joined" {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("join %s: first stdout event = %v, want \"joined\"; line=%s",
				name, ev["event"], r.line)
		}
		t.Logf("[join %s] joined event: %s", name, r.line)
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		h.stderrMu.Lock()
		stderr := h.stderrBuf.String()
		h.stderrMu.Unlock()
		t.Fatalf("join %s: no joined-event within 15s; stderr=\n%s", name, stderr)
	}

	// Continue reading post-joined stdout in the background so a stdout
	// pipe backpressure does not block the daemon. v1 join does not emit
	// further stdout, but we capture anything that shows up for
	// diagnostics.
	go func() {
		for h.stdout.Scan() {
			line := h.stdout.Text()
			h.mu.Lock()
			h.extraLines = append(h.extraLines, line)
			h.mu.Unlock()
			t.Logf("[join %s stdout] %s", name, line)
		}
	}()

	// Register teardown so a test exit always reaches Stop.
	t.Cleanup(func() {
		h.Stop(t)
	})

	return h
}

// Stop sends SIGTERM, waits 3s for exit, then SIGKILL. Safe to call
// multiple times; second call is a no-op.
func (h *joinHandle) Stop(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
		return
	default:
	}
	defer close(h.done)
	if h.cmd == nil || h.cmd.Process == nil {
		return
	}
	// Send SIGTERM to the process group so nothing leaks.
	pgid, err := syscall.Getpgid(h.cmd.Process.Pid)
	if err == nil {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	} else {
		_ = h.cmd.Process.Signal(syscall.SIGTERM)
	}

	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		} else {
			_ = h.cmd.Process.Kill()
		}
		<-done
	}
}

// tailHandle is the equivalent of joinHandle for `entmootd tail`. It
// exposes a channel of parsed TailEvent maps; the caller Stops it to
// terminate the subprocess.
type tailHandle struct {
	name    string
	cmd     *exec.Cmd
	stdout  *bufio.Scanner
	stderr  io.ReadCloser
	stdoutR io.ReadCloser
	// stdinW is the piped stdin; held open for the tail's lifetime so
	// cmdTail's stdin-EOF shutdown path does not fire prematurely.
	stdinW io.WriteCloser

	Events   chan map[string]any
	readDone chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	stderrBuf strings.Builder
	stderrMu  sync.Mutex
}

// startEntmootdTail runs `entmootd tail -group G -n 0` against the given
// peer's control socket. Emits one map per JSON line on Events. Caller
// Stops it when done; never blocks more than the subprocess's own
// shutdown window.
func startEntmootdTail(
	t *testing.T,
	name string,
	binary string,
	pilotSocket string,
	dataDir string,
	identityPath string,
	listenPort int,
	groupB64 string,
) *tailHandle {
	t.Helper()

	args := []string{
		"-socket", pilotSocket,
		"-identity", identityPath,
		"-data", dataDir,
		"-listen-port", fmt.Sprintf("%d", listenPort),
		"-log-level", "info",
		"tail",
		"-group", groupB64,
		"-n", "0",
	}

	cmd := exec.Command(binary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// cmdTail watches os.Stdin for EOF as a shutdown signal (see
	// cmd/entmootd/tail.go). An unconnected Stdin defaults to
	// /dev/null, whose first Read returns EOF immediately — that would
	// cancel the ctx and tear down the subscription before any
	// publishes land. Pipe stdin from us and keep it open until Stop.
	stdinW, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("startEntmootdTail %s: StdinPipe: %v", name, err)
	}

	stdoutR, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("startEntmootdTail %s: StdoutPipe: %v", name, err)
	}
	stderrR, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("startEntmootdTail %s: StderrPipe: %v", name, err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("startEntmootdTail %s: start: %v", name, err)
	}

	h := &tailHandle{
		name:     name,
		cmd:      cmd,
		stderr:   stderrR,
		stdoutR:  stdoutR,
		stdinW:   stdinW,
		Events:   make(chan map[string]any, 16),
		readDone: make(chan struct{}),
		done:     make(chan struct{}),
	}
	h.stdout = bufio.NewScanner(stdoutR)
	h.stdout.Buffer(make([]byte, 0, 128*1024), 1024*1024)

	go func() {
		scan := bufio.NewScanner(stderrR)
		scan.Buffer(make([]byte, 0, 128*1024), 1024*1024)
		for scan.Scan() {
			line := scan.Text()
			h.stderrMu.Lock()
			h.stderrBuf.WriteString(line)
			h.stderrBuf.WriteByte('\n')
			h.stderrMu.Unlock()
			t.Logf("[tail %s stderr] %s", name, line)
		}
	}()

	go func() {
		defer close(h.readDone)
		for h.stdout.Scan() {
			line := h.stdout.Text()
			var ev map[string]any
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				t.Logf("[tail %s] malformed stdout line: %s", name, line)
				continue
			}
			select {
			case h.Events <- ev:
			case <-h.done:
				return
			}
		}
	}()

	t.Cleanup(func() { h.Stop(t) })
	return h
}

// Stop terminates the tail process with SIGTERM → SIGKILL fallback.
// Closing stdinW first gives cmdTail's graceful-shutdown path (it
// cancels on stdin EOF) a chance to win against the signal.
func (h *tailHandle) Stop(t *testing.T) {
	t.Helper()
	h.stopOnce.Do(func() {
		close(h.done)
		if h.stdinW != nil {
			_ = h.stdinW.Close()
		}
		if h.cmd == nil || h.cmd.Process == nil {
			return
		}
		pgid, err := syscall.Getpgid(h.cmd.Process.Pid)
		if err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGTERM)
		} else {
			_ = h.cmd.Process.Signal(syscall.SIGTERM)
		}
		doneW := make(chan error, 1)
		go func() { doneW <- h.cmd.Wait() }()
		select {
		case <-doneW:
		case <-time.After(3 * time.Second):
			if err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = h.cmd.Process.Kill()
			}
			<-doneW
		}
	})
}

// writeInviteFile writes invite JSON (as produced by `entmootd invite
// create`) to a uniquely-named file inside dir, and returns its path. The
// file is registered with t.Cleanup for best-effort teardown.
func writeInviteFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name+".invite.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writeInviteFile %s: %v", name, err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
	return path
}
