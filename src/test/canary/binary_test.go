package canary

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

// TestCanaryBinary spawns real `entmootd` subprocesses against three
// sandboxed Pilot daemons and exercises the v1 CLI surface end-to-end:
//
//	group create → invite create → join (×3) → publish → query (×3)
//	→ tail (backgrounded on B) → publish (gossip propagation check).
//
// The test uses library-level helpers to add B and C to A's roster after
// `group create` but before `invite create`; the v1 CLI does not yet
// expose an admin "add member" subcommand, so this seam bridges the gap.
// Everything else is genuine CLI invocation.
//
// Gating mirrors TestCanaryPilot: skipped under `-short` and when
// ENTMOOT_SKIP_PILOT is set, because it needs real Pilot daemons.
func TestCanaryBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Pilot daemons and entmootd binary; skipping in -short mode")
	}
	if os.Getenv("ENTMOOT_SKIP_PILOT") != "" {
		t.Skip("ENTMOOT_SKIP_PILOT is set; skipping binary canary")
	}

	start := time.Now()
	defer func() {
		t.Logf("TestCanaryBinary wall clock: %s", time.Since(start))
	}()

	// Locate the repo root and verify the Pilot binaries exist.
	repoRoot := findRepoRoot(t)
	pilotBin := filepath.Join(repoRoot, "repos", "pilotprotocol", "bin")
	mustExist(t, filepath.Join(pilotBin, "daemon"))
	mustExist(t, filepath.Join(pilotBin, "pilotctl"))

	// Build entmootd once.
	entmootdBin := buildEntmootdBinary(t)
	t.Logf("built entmootd at %s", entmootdBin)

	// Fresh Pilot sockets; kill any strays from prior runs.
	cleanupPilotSockets(t)
	killStrayPilotDaemons(t)

	logger := slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelInfo}))

	// --- Pilot daemon setup (same pattern as TestCanaryPilot) ---

	a := startPilotDaemon(t, "a", true, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, a, logger) })
	b := startPilotDaemon(t, "b", false, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, b, logger) })
	c := startPilotDaemon(t, "c", false, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, c, logger) })

	waitPilotRegistered(t, a, 20*time.Second, logger)
	waitPilotRegistered(t, b, 20*time.Second, logger)
	waitPilotRegistered(t, c, 20*time.Second, logger)

	a.NodeID = pilotctlNodeID(t, a, pilotBin)
	b.NodeID = pilotctlNodeID(t, b, pilotBin)
	c.NodeID = pilotctlNodeID(t, c, pilotBin)
	logger.Info("binary canary: pilot node ids",
		slog.Uint64("a", uint64(a.NodeID)),
		slog.Uint64("b", uint64(b.NodeID)),
		slog.Uint64("c", uint64(c.NodeID)))

	handshakeApprove(t, b, a, pilotBin, logger) // B→A
	handshakeApprove(t, c, a, pilotBin, logger) // C→A
	handshakeApproveBestEffort(t, b, c, pilotBin, logger)

	// --- entmootd data + identity layout ---

	dataA := filepath.Join(t.TempDir(), "entmoot-a")
	dataB := filepath.Join(t.TempDir(), "entmoot-b")
	dataC := filepath.Join(t.TempDir(), "entmoot-c")
	for _, d := range []string{dataA, dataB, dataC} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	idPathA := filepath.Join(dataA, "identity.json")
	idPathB := filepath.Join(dataB, "identity.json")
	idPathC := filepath.Join(dataC, "identity.json")

	// Pre-generate B and C identities so the founder can add them to the
	// roster before `invite create` runs. `entmootd` will reuse these on
	// disk via LoadOrGenerate.
	idB, err := keystore.LoadOrGenerate(idPathB)
	if err != nil {
		t.Fatalf("LoadOrGenerate idB: %v", err)
	}
	idC, err := keystore.LoadOrGenerate(idPathC)
	if err != nil {
		t.Fatalf("LoadOrGenerate idC: %v", err)
	}

	// --- Founder: create the group via CLI ---

	envBase := func(sock, dataDir string) []string {
		// entmootd reads only its flag arguments; HOME/PILOT_SOCKET are
		// supplied for parity with the pilotctl helpers in case the
		// binary ever grows a default resolver that looks at them.
		return []string{
			"HOME=" + dataDir,
			"PILOT_SOCKET=" + sock,
		}
	}

	globalA := []string{
		"-socket", a.Socket,
		"-identity", idPathA,
		"-data", dataA,
		"-listen-port", "1004",
		"-log-level", "info",
	}
	globalB := []string{
		"-socket", b.Socket,
		"-identity", idPathB,
		"-data", dataB,
		"-listen-port", "1004",
		"-log-level", "info",
	}
	globalC := []string{
		"-socket", c.Socket,
		"-identity", idPathC,
		"-data", dataC,
		"-listen-port", "1004",
		"-log-level", "info",
	}

	// `entmootd group create` outputs {"group_id":"<b64>", "founder":{...}}.
	groupArgs := append(append([]string{}, globalA...), "group", "create", "-name", "demo")
	stdout, stderr, code := runEntmootd(t, entmootdBin, envBase(a.Socket, dataA), groupArgs...)
	if code != 0 {
		t.Fatalf("group create: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
	var groupCreateResp struct {
		GroupID entmoot.GroupID `json:"group_id"`
	}
	if err := json.Unmarshal([]byte(stdout), &groupCreateResp); err != nil {
		t.Fatalf("group create: parse stdout %q: %v", stdout, err)
	}
	gid := groupCreateResp.GroupID
	t.Logf("group created: %s", gid)

	// --- Library seam: founder adds B and C to the roster ---
	//
	// The v1 CLI has no `group add-member` yet; the v0 canary's
	// TestCanaryPilot seeds roster entries in-process for the same reason.
	// We load the founder's identity from the on-disk path that
	// `entmootd group create` just created.
	idA, err := keystore.LoadOrGenerate(idPathA)
	if err != nil {
		t.Fatalf("LoadOrGenerate idA: %v", err)
	}

	// Open A's roster (already contains the genesis) and append add(B),
	// add(C) entries signed by the founder.
	rosterA, err := roster.OpenJSONL(dataA, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL A: %v", err)
	}
	founderID := a.NodeID
	infoB := entmoot.NodeInfo{PilotNodeID: b.NodeID, EntmootPubKey: []byte(idB.PublicKey)}
	infoC := entmoot.NodeInfo{PilotNodeID: c.NodeID, EntmootPubKey: []byte(idC.PublicKey)}
	nowMs := time.Now().UnixMilli()
	addB := mkRosterAdd(t, idA, founderID, rosterA.Head(), infoB, nowMs+1)
	if err := rosterA.Apply(addB); err != nil {
		t.Fatalf("rosterA.Apply add B: %v", err)
	}
	addC := mkRosterAdd(t, idA, founderID, rosterA.Head(), infoC, nowMs+2)
	if err := rosterA.Apply(addC); err != nil {
		t.Fatalf("rosterA.Apply add C: %v", err)
	}
	// Close before the invite-create subprocess opens it — JSONL is
	// append-only and safe, but closing avoids holding an unnecessary
	// file handle.
	_ = rosterA.Close()

	// --- Founder: generate two invite bundles ---
	//
	// Invite bundles are identical in content; we write them to separate
	// files so each member can be given a dedicated path. The invite
	// includes founder + bootstrap peers; BootstrapPeers defaults to the
	// founder + other current members (so B and C appear in the list
	// because we just added them). That makes the join path resilient to
	// founder-first, member-first, and B↔C peer discovery.
	gidB64 := gid.String()
	inviteArgs := append(append([]string{}, globalA...),
		"invite", "create", "-group", gidB64, "-valid-for", "1h")

	var inviteBodies [2]string
	for i := 0; i < 2; i++ {
		stdout, stderr, code = runEntmootd(t, entmootdBin, envBase(a.Socket, dataA), inviteArgs...)
		if code != 0 {
			t.Fatalf("invite create #%d: exit=%d stdout=%s stderr=%s", i, code, stdout, stderr)
		}
		inviteBodies[i] = stdout
	}

	// Quick schema sanity on the first invite so a regression surfaces
	// here rather than in "join".
	var inviteHead entmoot.Invite
	if err := json.Unmarshal([]byte(inviteBodies[0]), &inviteHead); err != nil {
		t.Fatalf("invite #0: parse: %v\nraw=%s", err, inviteBodies[0])
	}
	if inviteHead.ValidUntil == 0 {
		t.Fatalf("invite #0: ValidUntil not set; v1 issuers must set it")
	}
	if inviteHead.IssuedAt == 0 {
		t.Fatalf("invite #0: IssuedAt not set")
	}
	if inviteHead.ValidUntil <= inviteHead.IssuedAt {
		t.Fatalf("invite #0: ValidUntil %d <= IssuedAt %d", inviteHead.ValidUntil, inviteHead.IssuedAt)
	}

	inviteDirB := t.TempDir()
	inviteDirC := t.TempDir()
	invitePathB := writeInviteFile(t, inviteDirB, "b", inviteBodies[0])
	invitePathC := writeInviteFile(t, inviteDirC, "c", inviteBodies[1])
	// A needs its own invite too; the founder still goes through `join`
	// to own the control socket + accept loop.
	inviteDirA := t.TempDir()
	invitePathA := writeInviteFile(t, inviteDirA, "a", inviteBodies[0])

	// --- Start the three `join` subprocesses ---
	//
	// Start A first so B and C find a bootstrap peer already listening.
	// All three listen on port 1004 per their own Pilot daemon; the
	// transport layer's DialAddr uses the local ListenPort when dialing
	// peers, so the three values must match (see
	// pkg/entmoot/transport/pilot/pilot.go).
	hA := startEntmootdJoin(t, "a", entmootdBin, a.Socket, dataA, idPathA, 1004, invitePathA)
	hB := startEntmootdJoin(t, "b", entmootdBin, b.Socket, dataB, idPathB, 1004, invitePathB)
	hC := startEntmootdJoin(t, "c", entmootdBin, c.Socket, dataC, idPathC, 1004, invitePathC)
	_ = hA // handles are auto-cleanup via t.Cleanup in startEntmootdJoin
	_ = hB
	_ = hC

	// Give the three join processes a moment to open their control
	// sockets. Poll rather than Sleep so we do not sit longer than we
	// need to.
	waitForSocket(t, filepath.Join(dataA, "control.sock"), 5*time.Second)
	waitForSocket(t, filepath.Join(dataB, "control.sock"), 5*time.Second)
	waitForSocket(t, filepath.Join(dataC, "control.sock"), 5*time.Second)

	// --- Publish three messages from A, B, A ---

	publish := func(global []string, env []string, content string) entmoot.MessageID {
		args := append(append([]string{}, global...),
			"publish", "-group", gidB64, "-topic", "chat", "-content", content)
		out, err, code := runEntmootd(t, entmootdBin, env, args...)
		if code != 0 {
			t.Fatalf("publish %q: exit=%d stdout=%s stderr=%s", content, code, out, err)
		}
		var resp struct {
			MessageID entmoot.MessageID `json:"message_id"`
			GroupID   entmoot.GroupID   `json:"group_id"`
		}
		if uerr := json.Unmarshal([]byte(out), &resp); uerr != nil {
			t.Fatalf("publish %q: parse stdout %q: %v", content, out, uerr)
		}
		if resp.GroupID != gid {
			t.Fatalf("publish %q: unexpected group_id %s (want %s)", content, resp.GroupID, gid)
		}
		// message_id must be non-zero.
		var zeroID entmoot.MessageID
		if resp.MessageID == zeroID {
			t.Fatalf("publish %q: zero message_id", content)
		}
		return resp.MessageID
	}

	msg1 := publish(globalA, envBase(a.Socket, dataA), "hi from A")
	// Space publishes slightly: gossip picks fanout=2; if two messages
	// race out instantly the first Pilot DialAddr can occasionally race
	// trust-edge installation on B↔C (it's the private↔private path).
	time.Sleep(500 * time.Millisecond)
	msg2 := publish(globalB, envBase(b.Socket, dataB), "hi from B")
	time.Sleep(500 * time.Millisecond)
	msg3 := publish(globalA, envBase(a.Socket, dataA), "third message")

	expectedContent := map[entmoot.MessageID]string{
		msg1: "hi from A",
		msg2: "hi from B",
		msg3: "third message",
	}
	t.Logf("published 3 messages: %s %s %s", msg1, msg2, msg3)

	// --- Convergence: every peer's query returns all three messages ---

	queryOnce := func(global []string) ([]map[string]any, error) {
		args := append(append([]string{}, global...),
			"query", "-group", gidB64, "-order", "asc", "-limit", "100")
		out, errOut, code := runEntmootd(t, entmootdBin, nil, args...)
		if code != 0 {
			return nil, fmt.Errorf("query exit=%d stdout=%s stderr=%s", code, out, errOut)
		}
		var events []map[string]any
		for _, line := range splitJSONLines(out) {
			var ev map[string]any
			if err := json.Unmarshal([]byte(line), &ev); err != nil {
				return nil, fmt.Errorf("query: parse %q: %w", line, err)
			}
			events = append(events, ev)
		}
		return events, nil
	}

	peers := []struct {
		name   string
		global []string
	}{
		{"a", globalA},
		{"b", globalB},
		{"c", globalC},
	}

	deadline := time.Now().Add(25 * time.Second)
	lastCounts := make(map[string]int)
	for {
		allGood := true
		for _, p := range peers {
			events, err := queryOnce(p.global)
			if err != nil {
				t.Fatalf("query %s: %v", p.name, err)
			}
			lastCounts[p.name] = len(events)
			if len(events) != 3 {
				allGood = false
			} else {
				// Assert every expected content is present.
				got := make(map[string]bool, 3)
				for _, e := range events {
					if s, ok := e["content"].(string); ok {
						got[s] = true
					}
				}
				for _, want := range expectedContent {
					if !got[want] {
						allGood = false
					}
				}
			}
		}
		if allGood {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("query convergence: timed out; last counts = %v", lastCounts)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("convergence reached; per-peer counts = %v", lastCounts)

	// --- Tail subscription: B tails, A publishes, B sees the event ---

	tailH := startEntmootdTail(t, "b", entmootdBin, b.Socket, dataB, idPathB, 1004, gidB64)

	// Let the tail subscription register before publishing. The control
	// socket accept handler is up when join emitted "joined", so this is
	// effectively immediate; a tiny drain lets the subscribe frame round-
	// trip.
	time.Sleep(300 * time.Millisecond)

	msg4 := publish(globalA, envBase(a.Socket, dataA), "fourth message (tail)")

	// Block on the tail event with a 10s budget.
	var tailEv map[string]any
	select {
	case ev := <-tailH.Events:
		tailEv = ev
	case <-time.After(10 * time.Second):
		t.Fatalf("tail: no event received within 10s")
	}

	// tail_event JSON wraps the full message as "message"; emitMessageJSON
	// on the daemon side for direct tail output flattens it. The daemon's
	// handleTail emits *ipc.TailEvent, which the tail CLI re-renders via
	// emitMessageJSON — so the stdout shape on the client side mirrors
	// `query`'s output.
	content, _ := tailEv["content"].(string)
	if content != "fourth message (tail)" {
		// One failure mode: stale backlog leaked onto the live stream.
		// Drain any extra events to assert bounded; fail with rich diag.
		extra := []map[string]any{tailEv}
		for {
			select {
			case e := <-tailH.Events:
				extra = append(extra, e)
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("tail: expected \"fourth message (tail)\" but saw %d events; first content=%q", len(extra), content)
				return
			}
		}
	}
	// msg4 is the id we expect; confirm the tail carried the same id.
	if gotID, _ := tailEv["message_id"].(string); gotID != msg4.String() {
		t.Fatalf("tail: message_id %q != published %q", gotID, msg4.String())
	}

	// Stop the tail now so the next phase (teardown) doesn't race.
	tailH.Stop(t)

	// --- Teardown invariant: each peer's control.sock is removed after
	// its join process exits. We stop the handles explicitly so the
	// deferred t.Cleanup does not hide a premature socket removal.

	hA.Stop(t)
	hB.Stop(t)
	hC.Stop(t)

	// Give the OS a beat to unlink the sockets; `control.sock` removal
	// happens as the last step of cmdJoin's shutdown path.
	for _, dir := range []string{dataA, dataB, dataC} {
		sock := filepath.Join(dir, "control.sock")
		deadline := time.Now().Add(3 * time.Second)
		for {
			if _, err := os.Stat(sock); os.IsNotExist(err) {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("control.sock not removed after shutdown: %s", sock)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// waitForSocket polls for the unix socket at path to exist within
// timeout. Fatal on timeout — a missing control socket after the
// joined-event line means cmdJoin is not in its steady state yet.
func waitForSocket(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("control socket %s not present within %s", path, timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// splitJSONLines splits s on newlines, trimming empty lines. Entmootd's
// query emits one JSON object per line terminated by '\n'; this helper
// is robust to both Unix and Windows line endings.
func splitJSONLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\n' {
			line := s[start:i]
			// Trim trailing \r.
			if n := len(line); n > 0 && line[n-1] == '\r' {
				line = line[:n-1]
			}
			if len(line) > 0 {
				out = append(out, line)
			}
			start = i + 1
		}
	}
	return out
}
