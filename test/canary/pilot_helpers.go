package canary

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
)

// pilotNode bundles the runtime state of a single sandboxed Pilot daemon
// subprocess: where its IPC socket is, what its data dir is, its Pilot NodeID
// (once discovered), and the *exec.Cmd / stdout-pipe / stderr-pipe so we can
// stop it cleanly at test teardown.
type pilotNode struct {
	Name   string          // "a", "b", "c"
	Socket string          // /tmp/entmoot-canary-<name>.sock
	Dir    string          // per-node identity dir (t.TempDir()/pilot-<name>)
	NodeID entmoot.NodeID  // populated after daemon registers
	Cmd    *exec.Cmd       // subprocess
	LogBuf *syncBuffer     // captured daemon stdout/stderr (for debug)
	Cancel context.CancelFunc
	Ctx    context.Context
}

// syncBuffer is a tiny goroutine-safe buffer used to capture daemon output so
// test failures can dump it at the end. It intentionally mirrors bytes.Buffer
// so callers can do .String() on it.
type syncBuffer struct {
	b  bytes.Buffer
	mu struct {
		sync chan struct{}
	}
}

func newSyncBuffer() *syncBuffer {
	s := &syncBuffer{}
	s.mu.sync = make(chan struct{}, 1)
	s.mu.sync <- struct{}{}
	return s
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	<-s.mu.sync
	defer func() { s.mu.sync <- struct{}{} }()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	<-s.mu.sync
	defer func() { s.mu.sync <- struct{}{} }()
	return s.b.String()
}

// findRepoRoot walks up from the test file's cwd until it finds a directory
// with go.mod named "entmoot". Tests run from their package dir by default, so
// this is two parents up (test/canary → test → repoRoot). We walk rather than
// assume in case Go moves the CWD semantics in a future version.
func findRepoRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("findRepoRoot: os.Getwd: %v", err)
	}
	dir := cwd
	for i := 0; i < 10; i++ {
		goMod := filepath.Join(dir, "go.mod")
		if data, err := os.ReadFile(goMod); err == nil {
			if bytes.Contains(data, []byte("module entmoot")) {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("findRepoRoot: could not locate entmoot module root from %s", cwd)
	return ""
}

// mustExist fails the test if path does not exist. Used to verify the Pilot
// daemon/pilotctl binaries have been built before the test tries to launch
// them; a missing binary should be a fail, not a skip.
func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("required binary missing: %s (run: make -C repos/pilotprotocol build): %v", path, err)
	}
}

// cleanupPilotSockets removes any leftover /tmp/entmoot-canary-*.sock files
// from a prior aborted run. Safe to call repeatedly.
func cleanupPilotSockets(t *testing.T) {
	t.Helper()
	matches, err := filepath.Glob("/tmp/entmoot-canary-*.sock")
	if err != nil {
		return
	}
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// killStrayPilotDaemons sends SIGKILL to any orphaned canary daemons from a
// prior aborted run so the new run starts from a clean slate.
func killStrayPilotDaemons(t *testing.T) {
	t.Helper()
	_ = exec.Command("pkill", "-f", "bin/daemon.*entmoot-canary").Run()
	// pkill returns non-zero when nothing matched; not an error for us.
}

// startPilotDaemon spawns a single Pilot daemon subprocess for the given
// logical name (a, b, c). If `public` is true the daemon is launched with
// -public (required for the founder so private peers can resolve its
// endpoint). Returns a populated pilotNode; on any launch error the test
// fails immediately.
//
// The daemon logs are captured to an in-memory buffer and also written to
// <tempdir>/daemon.log so humans inspecting a failure can see them.
func startPilotDaemon(t *testing.T, name string, public bool, pilotBin string, logger *slog.Logger) *pilotNode {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "pilot-"+name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	socket := fmt.Sprintf("/tmp/entmoot-canary-%s.sock", name)
	identity := filepath.Join(dir, "identity.json")
	email := fmt.Sprintf("entmoot-canary-%s@example.com", name)

	args := []string{
		"-socket", socket,
		"-identity", identity,
		"-email", email,
		"-listen", ":0",
		// -trust-auto-approve: every incoming handshake is auto-approved. The
		// canary test only needs pairwise trust edges between three daemons we
		// control; the manual handshake/approve dance is slow and flaky
		// (pending queues can take seconds to propagate, reverse direction
		// needs its own round-trip). With auto-approve each `pilotctl
		// handshake` call results in a two-way trust edge once accepted by the
		// remote daemon.
		"-trust-auto-approve",
	}
	if public {
		args = append(args, "-public")
	}
	// Speed: disable services we do not need. Keep handshake (:444) — that is
	// mandatory for pairwise trust — but the others just add startup noise.
	args = append(args,
		"-no-echo",
		"-no-dataexchange",
		"-no-eventstream",
		"-no-tasksubmit",
	)

	ctx, cancel := context.WithCancel(context.Background())
	daemon := filepath.Join(pilotBin, "daemon")
	cmd := exec.CommandContext(ctx, daemon, args...)
	// Point HOME at the dir so any defaults the daemon resolves land inside
	// the temp sandbox. Also pin PILOT_SOCKET so any accidental pilotctl
	// invocation from inside the daemon uses the right socket.
	cmd.Env = append(os.Environ(),
		"HOME="+dir,
		"PILOT_SOCKET="+socket,
	)
	logBuf := newSyncBuffer()

	logPath := filepath.Join(dir, "daemon.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		cancel()
		t.Fatalf("create daemon log %s: %v", logPath, err)
	}
	// A MultiWriter so the file and in-memory buffer both see the output.
	cmd.Stdout = io.MultiWriter(logBuf, logFile)
	cmd.Stderr = io.MultiWriter(logBuf, logFile)

	logger.Info("starting pilot daemon",
		slog.String("node", name),
		slog.Bool("public", public),
		slog.String("socket", socket),
		slog.String("log", logPath))

	if err := cmd.Start(); err != nil {
		cancel()
		_ = logFile.Close()
		t.Fatalf("start pilot daemon %s: %v", name, err)
	}

	return &pilotNode{
		Name:   name,
		Socket: socket,
		Dir:    dir,
		Cmd:    cmd,
		LogBuf: logBuf,
		Cancel: cancel,
		Ctx:    ctx,
	}
}

// stopPilotDaemon terminates the daemon via its cancel func, then waits up to
// 3 seconds for it to exit, then SIGKILLs if it hasn't. Socket files are
// removed after exit.
func stopPilotDaemon(t *testing.T, p *pilotNode, logger *slog.Logger) {
	t.Helper()
	if p == nil || p.Cmd == nil {
		return
	}
	logger.Info("stopping pilot daemon", slog.String("node", p.Name))
	p.Cancel() // sends SIGKILL via exec.CommandContext when the context fires.

	done := make(chan error, 1)
	go func() { done <- p.Cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = p.Cmd.Process.Kill()
		<-done
	}
	_ = os.Remove(p.Socket)

	// If the test failed, dump the daemon log to t.Log so CI can see what
	// went wrong. On pass we keep it silent to avoid noise.
	if t.Failed() {
		t.Logf("--- pilot daemon %s log (tail) ---", p.Name)
		s := p.LogBuf.String()
		// Trim to the last ~4KB so we do not blow out test output.
		const maxOut = 4096
		if len(s) > maxOut {
			s = s[len(s)-maxOut:]
		}
		t.Logf("%s", s)
	}
}

// waitPilotRegistered polls `pilotctl info` until the daemon reports an
// `addr` field that is not empty. Pilot's daemon is not usable until the
// registry has assigned it an address, so every other pilotctl call that
// depends on the daemon being on the network must come after this.
func waitPilotRegistered(t *testing.T, p *pilotNode, timeout time.Duration, logger *slog.Logger) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	interval := 500 * time.Millisecond
	var lastErr error
	for {
		info, err := pilotctlJSON(p, "info")
		if err == nil {
			// pilotctl info JSON uses `address` (not `addr`) — pilotctl's CLI
			// pretty-printer uses "addr" but the JSON envelope key is different.
			if addr, ok := info["address"].(string); ok && addr != "" && addr != "0:0000.0000.0000" {
				logger.Info("pilot daemon registered",
					slog.String("node", p.Name),
					slog.String("address", addr))
				return
			}
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("pilot daemon %s did not register within %s (last err: %v)", p.Name, timeout, lastErr)
		}
		time.Sleep(interval)
	}
}

// pilotctlNodeID reads `node_id` off `pilotctl info --json`. The same field
// pilot.Transport.Open will read out of driver.Info; we duplicate it here so
// the test can set up rosters (which need numeric NodeIDs) before building
// the pilot adapter.
func pilotctlNodeID(t *testing.T, p *pilotNode, _ string) entmoot.NodeID {
	t.Helper()
	info, err := pilotctlJSON(p, "info")
	if err != nil {
		t.Fatalf("pilotctl info %s: %v", p.Name, err)
	}
	raw, ok := info["node_id"]
	if !ok {
		t.Fatalf("pilotctl info %s: no node_id field", p.Name)
	}
	f, ok := raw.(float64)
	if !ok {
		t.Fatalf("pilotctl info %s: node_id is %T", p.Name, raw)
	}
	return entmoot.NodeID(uint32(f))
}

// pilotctl runs `pilotctl <args...>` against p's socket (passed via the
// PILOT_SOCKET env var, which is the only way pilotctl picks up a non-default
// socket — it has no `-socket` flag) and returns stdout on success.
//
// If the caller includes "--json" in args, this helper assumes the invocation
// wants a JSON-wrapped envelope: `{"status":"ok","data":{...}}`. Callers that
// want the raw `.data` payload should use pilotctlJSON which unwraps.
func pilotctl(p *pilotNode, args ...string) ([]byte, error) {
	bin := pilotctlPath()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "PILOT_SOCKET="+p.Socket)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("pilotctl %v: %w (stderr=%s)", args, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// pilotctlJSON runs pilotctl --json ... and returns the "data" field from the
// response envelope. Errors surface the "message"/"code" from the envelope
// when the CLI produces a status=error wrapper.
func pilotctlJSON(p *pilotNode, args ...string) (map[string]any, error) {
	fullArgs := append([]string{"--json"}, args...)
	raw, err := pilotctl(p, fullArgs...)
	if err != nil {
		// Try to unmarshal stderr-like responses; some exit paths still write
		// a JSON envelope to stdout even on failure. Fall through otherwise.
		return nil, err
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("pilotctl %v: json decode: %w (raw=%s)", args, err, string(raw))
	}
	if status, ok := env["status"].(string); ok && status != "ok" {
		return nil, fmt.Errorf("pilotctl %v: status=%s msg=%v", args, status, env["message"])
	}
	data, ok := env["data"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("pilotctl %v: no data object in envelope: %s", args, string(raw))
	}
	return data, nil
}

// pilotctlPath returns the canonical path to the pilotctl binary.
// Invariant: this function is only called after mustExist has run at test
// entry, so a missing binary here is a programmer error (not an operator
// error).
func pilotctlPath() string {
	// Look up relative to CWD first (tests normally run from their package
	// dir). Fall back to searching upward like findRepoRoot does.
	candidates := []string{
		"repos/pilotprotocol/bin/pilotctl",
		"../../repos/pilotprotocol/bin/pilotctl",
		"../repos/pilotprotocol/bin/pilotctl",
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			abs, err := filepath.Abs(c)
			if err == nil {
				return abs
			}
		}
	}
	// Final fallback: walk up looking for entmoot go.mod.
	cwd, _ := os.Getwd()
	dir := cwd
	for i := 0; i < 10; i++ {
		p := filepath.Join(dir, "repos", "pilotprotocol", "bin", "pilotctl")
		if _, err := os.Stat(p); err == nil {
			return p
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Will be caught by pilotctl() as an exec failure if we never find it.
	return "pilotctl"
}

// handshakeApprove establishes a two-way trust edge between initiator and
// target. With the canary daemons started with -trust-auto-approve, each
// pilotctl-handshake call results in the remote side automatically approving
// the request. We then issue a reverse handshake so both sides have a trust
// record (Entmoot's gossip needs outbound dials in both directions).
//
// Fails the test on the initial dial failure. A failed reverse handshake is
// logged but does not fail the test (it is non-fatal — once trust is
// established one-way, many operations work; convergence will expose any
// real issue).
func handshakeApprove(t *testing.T, initiator, target *pilotNode, pilotBin string, logger *slog.Logger) {
	t.Helper()
	logger.Info("pilot handshake",
		slog.String("initiator", initiator.Name),
		slog.String("target", target.Name))
	// initiator → target
	if _, err := pilotctl(initiator, "handshake", fmt.Sprintf("%d", target.NodeID), "entmoot-canary"); err != nil {
		t.Fatalf("handshake %s→%s: %v", initiator.Name, target.Name, err)
	}
	// target → initiator (reverse direction). With -trust-auto-approve the
	// target auto-approves initiator on the first handshake; this second
	// handshake establishes initiator's trust of target for outbound dials.
	if _, err := pilotctl(target, "handshake", fmt.Sprintf("%d", initiator.NodeID), "entmoot-canary-reverse"); err != nil {
		logger.Warn("reverse handshake failed (non-fatal)",
			slog.String("target", target.Name),
			slog.String("initiator", initiator.Name),
			slog.String("err", err.Error()))
	}
	// Give trust a moment to settle — auto-approve is fast but the tunnel
	// handshake itself is a multi-RTT flow.
	waitForTrust(t, initiator, target.NodeID, 15*time.Second, logger)
	waitForTrust(t, target, initiator.NodeID, 15*time.Second, logger)
	logger.Info("pilot handshake complete",
		slog.String("a", initiator.Name),
		slog.String("b", target.Name))
}

// handshakeApproveBestEffort is the B↔C counterpart: both sides are private
// so the registry may hide endpoints initially. Pilot relays handshakes via
// the registry when direct resolution fails, so B↔C will usually establish;
// if it does not, we log and let convergence surface the gossip-layer
// impact.
func handshakeApproveBestEffort(t *testing.T, initiator, target *pilotNode, pilotBin string, logger *slog.Logger) {
	t.Helper()
	logger.Info("pilot handshake (best-effort)",
		slog.String("initiator", initiator.Name),
		slog.String("target", target.Name))
	if _, err := pilotctl(initiator, "handshake", fmt.Sprintf("%d", target.NodeID), "entmoot-canary-pp"); err != nil {
		logger.Warn("best-effort handshake failed",
			slog.String("initiator", initiator.Name),
			slog.String("target", target.Name),
			slog.String("err", err.Error()))
		return
	}
	if _, err := pilotctl(target, "handshake", fmt.Sprintf("%d", initiator.NodeID), "entmoot-canary-pp-rev"); err != nil {
		logger.Warn("best-effort reverse handshake failed",
			slog.String("target", target.Name),
			slog.String("initiator", initiator.Name),
			slog.String("err", err.Error()))
	}
	_ = waitForTrustSoft(initiator, target.NodeID, 15*time.Second)
	_ = waitForTrustSoft(target, initiator.NodeID, 15*time.Second)
	logger.Info("pilot handshake (best-effort) complete",
		slog.String("a", initiator.Name),
		slog.String("b", target.Name))
}

// waitForTrust polls `pilotctl trust` on p until it reports a trust edge
// with `peer`, or timeout elapses. Test-fatal on timeout.
func waitForTrust(t *testing.T, p *pilotNode, peer entmoot.NodeID, timeout time.Duration, logger *slog.Logger) {
	t.Helper()
	if err := waitForTrustSoft(p, peer, timeout); err != nil {
		logger.Warn("pilot trust edge did not appear",
			slog.String("node", p.Name),
			slog.Uint64("peer", uint64(peer)),
			slog.String("err", err.Error()))
	}
}

// waitForTrustSoft is the non-fatal variant; returns an error rather than
// calling t.Fatalf. Used by best-effort B↔C.
func waitForTrustSoft(p *pilotNode, peer entmoot.NodeID, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		data, err := pilotctlJSON(p, "trust")
		if err == nil {
			if trusted, ok := data["trusted"].([]any); ok {
				for _, item := range trusted {
					entry, ok := item.(map[string]any)
					if !ok {
						continue
					}
					if idf, ok := entry["node_id"].(float64); ok {
						if entmoot.NodeID(uint32(idf)) == peer {
							return nil
						}
					}
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no trust edge to %d within %s", peer, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

