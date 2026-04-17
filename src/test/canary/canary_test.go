package canary

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/transport/pilot"
)

// TestCanaryInMemory exercises the full canary flow against the in-memory
// gossip.Transport mock (gossip.NewMemTransports). The test must run fast
// (well under 5s wall clock) and is the logic gate for the whole integration;
// TestCanaryPilot then proves the same flow works over a real Pilot network.
//
// Flow (ARCHITECTURE §12):
//   1. Generate 3 Entmoot identities (A, B, C).
//   2. Assign Pilot NodeIDs 1, 2, 3 respectively.
//   3. A opens a JSONL RosterLog, calls Genesis, then applies two signed
//      add(subject) entries for B and C (in-process; there is no CLI for
//      admin-side roster mutation in v0).
//   4. Snapshot the resulting 3 entries onto B and C via AcceptGenesis +
//      Apply so every node agrees on membership — this exercises the Join
//      code path without running the accept loops yet.
//   5. Open a JSONL MessageStore per node; wire a gossip.Gossiper around
//      each (fanout = 2).
//   6. Start all three accept loops.
//   7. Publish 3 messages: A publishes #1, B publishes #2, A publishes #3.
//   8. Poll for convergence: all three stores have 3 messages AND identical
//      Merkle roots.
//   9. Cancel the gossipers, close stores/rosters/transports.
func TestCanaryInMemory(t *testing.T) {
	t.Parallel()

	const (
		nodeA entmoot.NodeID = 1
		nodeB entmoot.NodeID = 2
		nodeC entmoot.NodeID = 3
	)

	// Per-node tempdirs so JSONL rosters/stores don't collide.
	dataA := t.TempDir()
	dataB := t.TempDir()
	dataC := t.TempDir()

	// Identities.
	idA, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate A: %v", err)
	}
	idB, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate B: %v", err)
	}
	idC, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate C: %v", err)
	}

	infoA := entmoot.NodeInfo{PilotNodeID: nodeA, EntmootPubKey: []byte(idA.PublicKey)}
	infoB := entmoot.NodeInfo{PilotNodeID: nodeB, EntmootPubKey: []byte(idB.PublicKey)}
	infoC := entmoot.NodeInfo{PilotNodeID: nodeC, EntmootPubKey: []byte(idC.PublicKey)}

	// Group id: pick a fixed pattern so test failures are easy to read in the
	// JSONL dumps. In production this would come from crypto/rand.
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(0xA0 + i)
	}

	// ---- Step 3: A opens roster, Genesis, then Apply add(B), add(C).
	rosterA, err := roster.OpenJSONL(dataA, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL A: %v", err)
	}
	t.Cleanup(func() { _ = rosterA.Close() })

	const tsBase int64 = 2_000_000
	if err := rosterA.Genesis(idA, infoA, tsBase); err != nil {
		t.Fatalf("rosterA.Genesis: %v", err)
	}
	addB := mkRosterAdd(t, idA, nodeA, rosterA.Head(), infoB, tsBase+100)
	if err := rosterA.Apply(addB); err != nil {
		t.Fatalf("rosterA.Apply add B: %v", err)
	}
	addC := mkRosterAdd(t, idA, nodeA, rosterA.Head(), infoC, tsBase+200)
	if err := rosterA.Apply(addC); err != nil {
		t.Fatalf("rosterA.Apply add C: %v", err)
	}

	entries := rosterA.Entries() // genesis, add(B), add(C) — 3 entries.
	if len(entries) != 3 {
		t.Fatalf("rosterA.Entries = %d, want 3", len(entries))
	}

	// ---- Step 4: seed B's and C's rosters from the same entries, via
	// AcceptGenesis + Apply on fresh JSONL rosters. This exercises the Join
	// path rather than just writing the JSONL file directly.
	rosterB, err := roster.OpenJSONL(dataB, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL B: %v", err)
	}
	t.Cleanup(func() { _ = rosterB.Close() })
	if err := rosterB.AcceptGenesis(entries[0]); err != nil {
		t.Fatalf("rosterB.AcceptGenesis: %v", err)
	}
	if err := rosterB.Apply(entries[1]); err != nil {
		t.Fatalf("rosterB.Apply add B: %v", err)
	}
	if err := rosterB.Apply(entries[2]); err != nil {
		t.Fatalf("rosterB.Apply add C: %v", err)
	}

	rosterC, err := roster.OpenJSONL(dataC, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL C: %v", err)
	}
	t.Cleanup(func() { _ = rosterC.Close() })
	if err := rosterC.AcceptGenesis(entries[0]); err != nil {
		t.Fatalf("rosterC.AcceptGenesis: %v", err)
	}
	if err := rosterC.Apply(entries[1]); err != nil {
		t.Fatalf("rosterC.Apply add B: %v", err)
	}
	if err := rosterC.Apply(entries[2]); err != nil {
		t.Fatalf("rosterC.Apply add C: %v", err)
	}

	// ---- Step 5: JSONL message stores per node.
	storeA, err := store.OpenJSONL(dataA)
	if err != nil {
		t.Fatalf("store.OpenJSONL A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenJSONL(dataB)
	if err != nil {
		t.Fatalf("store.OpenJSONL B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	storeC, err := store.OpenJSONL(dataC)
	if err != nil {
		t.Fatalf("store.OpenJSONL C: %v", err)
	}
	t.Cleanup(func() { _ = storeC.Close() })

	// ---- Transports: one per node, all wired together in a single hub.
	transports := gossip.NewMemTransports([]entmoot.NodeID{nodeA, nodeB, nodeC})
	t.Cleanup(func() {
		for _, tr := range transports {
			_ = tr.Close()
		}
	})

	// ---- Gossipers.
	gossiperFor := func(n entmoot.NodeID, id *keystore.Identity, r *roster.RosterLog, s store.MessageStore) *gossip.Gossiper {
		t.Helper()
		g, err := gossip.New(gossip.Config{
			LocalNode: n,
			Identity:  id,
			Roster:    r,
			Store:     s,
			Transport: transports[n],
			GroupID:   gid,
			Fanout:    2,
			Logger:    slog.Default(),
		})
		if err != nil {
			t.Fatalf("gossip.New for %d: %v", n, err)
		}
		return g
	}
	gA := gossiperFor(nodeA, idA, rosterA, storeA)
	gB := gossiperFor(nodeB, idB, rosterB, storeB)
	gC := gossiperFor(nodeC, idC, rosterC, storeC)

	// ---- Step 6: accept loops.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(g *gossip.Gossiper) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.Start(ctx)
		}()
	}
	start(gA)
	start(gB)
	start(gC)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	// ---- Step 7: three messages.
	msg1 := mkMessage(t, idA, infoA, gid, "canary", "hello from A", tsBase+1_000)
	if err := gA.Publish(ctx, msg1); err != nil {
		t.Fatalf("gA.Publish msg1: %v", err)
	}
	msg2 := mkMessage(t, idB, infoB, gid, "canary", "hello from B", tsBase+1_100)
	if err := gB.Publish(ctx, msg2); err != nil {
		t.Fatalf("gB.Publish msg2: %v", err)
	}
	msg3 := mkMessage(t, idA, infoA, gid, "canary", "A again", tsBase+1_200)
	if err := gA.Publish(ctx, msg3); err != nil {
		t.Fatalf("gA.Publish msg3: %v", err)
	}

	// ---- Step 8: converge.
	stores := map[entmoot.NodeID]store.MessageStore{
		nodeA: storeA, nodeB: storeB, nodeC: storeC,
	}
	root, err := waitForConvergence(t, stores, gid, 3, 5*time.Second, 25*time.Millisecond, false)
	if err != nil {
		t.Fatalf("converge: %v", err)
	}
	t.Logf("canary: in-memory converged; root=%x", root[:8])

	// ---- Final asserts: every message retrievable on every node.
	for _, msg := range []entmoot.Message{msg1, msg2, msg3} {
		for n, s := range stores {
			getCtx, getCancel := context.WithTimeout(context.Background(), time.Second)
			got, err := s.Get(getCtx, gid, msg.ID)
			getCancel()
			if err != nil {
				t.Fatalf("store %d Get %s: %v", n, msg.ID, err)
			}
			if got.ID != msg.ID {
				t.Fatalf("store %d id mismatch: got %s want %s", n, got.ID, msg.ID)
			}
			if string(got.Content) != string(msg.Content) {
				t.Fatalf("store %d content mismatch: got %q want %q",
					n, got.Content, msg.Content)
			}
		}
	}
}

// TestCanaryPilot reproduces the in-memory canary against three real Pilot
// daemons. It spawns three sandboxed daemon subprocesses, performs the
// handshake+approve dance so everyone trusts everyone, wires entmoot gossip
// transport over the Pilot IPC socket, and then runs the identical A/B/C +
// 3-message flow with a longer convergence budget (Pilot's first-dial tunnel
// setup is seconds, not milliseconds).
//
// Skipped under `-short` and when ENTMOOT_SKIP_PILOT is set so `go test
// -short ./...` stays offline-friendly for CI.
func TestCanaryPilot(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Pilot daemons; skipping in -short mode")
	}
	if os.Getenv("ENTMOOT_SKIP_PILOT") != "" {
		t.Skip("ENTMOOT_SKIP_PILOT is set; skipping Pilot canary")
	}

	// Locate the repo root (…/entmoot) so we can find repos/pilotprotocol/bin.
	repoRoot := findRepoRoot(t)

	// Ensure daemon and pilotctl exist; fail loudly (not skip) if they are
	// missing — the task description is explicit.
	pilotBin := filepath.Join(repoRoot, "repos", "pilotprotocol", "bin")
	mustExist(t, filepath.Join(pilotBin, "daemon"))
	mustExist(t, filepath.Join(pilotBin, "pilotctl"))

	// Clean any leftover sockets from a prior run.
	cleanupPilotSockets(t)
	// Kill any leftover canary daemons from an aborted run.
	killStrayPilotDaemons(t)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Start daemon A (the -public founder), then B and C as private.
	a := startPilotDaemon(t, "a", true, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, a, logger) })
	b := startPilotDaemon(t, "b", false, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, b, logger) })
	c := startPilotDaemon(t, "c", false, pilotBin, logger)
	t.Cleanup(func() { stopPilotDaemon(t, c, logger) })

	// Wait for each daemon to register (have an addr). Up to 20s per node; a
	// fresh daemon on a cold path occasionally takes >10s.
	waitPilotRegistered(t, a, 20*time.Second, logger)
	waitPilotRegistered(t, b, 20*time.Second, logger)
	waitPilotRegistered(t, c, 20*time.Second, logger)

	// Populate NodeIDs via `pilotctl info` (the Entmoot pilot adapter also
	// reads them on Open, but we need them now so the three rosters can
	// reference each other before the adapters are even built).
	a.NodeID = pilotctlNodeID(t, a, pilotBin)
	b.NodeID = pilotctlNodeID(t, b, pilotBin)
	c.NodeID = pilotctlNodeID(t, c, pilotBin)
	logger.Info("pilot canary: node ids discovered",
		slog.Uint64("a", uint64(a.NodeID)),
		slog.Uint64("b", uint64(b.NodeID)),
		slog.Uint64("c", uint64(c.NodeID)))

	// Pairwise handshakes. A is public so we initiate from B and C toward A
	// (private→public resolves), plus B↔C (both private but we discover
	// through A — actually: once B has handshaken A, B's hostname lookup for
	// C still goes through the registry which hides C's endpoint because C
	// is private. So B→C handshake cannot be delivered until C is on B's
	// trust edge AND C's endpoint is reachable. Our pragmatic workaround:
	// do all three pairs; if any pair can't complete we surface the error
	// and continue — the test will fail convergence and the deviation will
	// be reported.
	handshakeApprove(t, b, a, pilotBin, logger) // B→A
	handshakeApprove(t, c, a, pilotBin, logger) // C→A
	// B↔C depends on a public endpoint being discoverable. We still attempt
	// it; on failure the test continues and convergence will report.
	handshakeApproveBestEffort(t, b, c, pilotBin, logger) // B↔C

	// ---- Entmoot-layer setup identical in shape to TestCanaryInMemory.
	dataA := t.TempDir()
	dataB := t.TempDir()
	dataC := t.TempDir()

	idA, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate A: %v", err)
	}
	idB, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate B: %v", err)
	}
	idC, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate C: %v", err)
	}

	infoA := entmoot.NodeInfo{PilotNodeID: a.NodeID, EntmootPubKey: []byte(idA.PublicKey)}
	infoB := entmoot.NodeInfo{PilotNodeID: b.NodeID, EntmootPubKey: []byte(idB.PublicKey)}
	infoC := entmoot.NodeInfo{PilotNodeID: c.NodeID, EntmootPubKey: []byte(idC.PublicKey)}

	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(0xB0 + i)
	}

	rosterA, err := roster.OpenJSONL(dataA, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL A: %v", err)
	}
	t.Cleanup(func() { _ = rosterA.Close() })

	const tsBase int64 = 3_000_000
	if err := rosterA.Genesis(idA, infoA, tsBase); err != nil {
		t.Fatalf("rosterA.Genesis: %v", err)
	}
	addB := mkRosterAdd(t, idA, a.NodeID, rosterA.Head(), infoB, tsBase+100)
	if err := rosterA.Apply(addB); err != nil {
		t.Fatalf("rosterA.Apply add B: %v", err)
	}
	addC := mkRosterAdd(t, idA, a.NodeID, rosterA.Head(), infoC, tsBase+200)
	if err := rosterA.Apply(addC); err != nil {
		t.Fatalf("rosterA.Apply add C: %v", err)
	}
	entries := rosterA.Entries()

	rosterB, err := roster.OpenJSONL(dataB, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL B: %v", err)
	}
	t.Cleanup(func() { _ = rosterB.Close() })
	if err := rosterB.AcceptGenesis(entries[0]); err != nil {
		t.Fatalf("rosterB.AcceptGenesis: %v", err)
	}
	if err := rosterB.Apply(entries[1]); err != nil {
		t.Fatalf("rosterB.Apply: %v", err)
	}
	if err := rosterB.Apply(entries[2]); err != nil {
		t.Fatalf("rosterB.Apply: %v", err)
	}

	rosterC, err := roster.OpenJSONL(dataC, gid)
	if err != nil {
		t.Fatalf("roster.OpenJSONL C: %v", err)
	}
	t.Cleanup(func() { _ = rosterC.Close() })
	if err := rosterC.AcceptGenesis(entries[0]); err != nil {
		t.Fatalf("rosterC.AcceptGenesis: %v", err)
	}
	if err := rosterC.Apply(entries[1]); err != nil {
		t.Fatalf("rosterC.Apply: %v", err)
	}
	if err := rosterC.Apply(entries[2]); err != nil {
		t.Fatalf("rosterC.Apply: %v", err)
	}

	storeA, err := store.OpenJSONL(dataA)
	if err != nil {
		t.Fatalf("store.OpenJSONL A: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	storeB, err := store.OpenJSONL(dataB)
	if err != nil {
		t.Fatalf("store.OpenJSONL B: %v", err)
	}
	t.Cleanup(func() { _ = storeB.Close() })
	storeC, err := store.OpenJSONL(dataC)
	if err != nil {
		t.Fatalf("store.OpenJSONL C: %v", err)
	}
	t.Cleanup(func() { _ = storeC.Close() })

	// ---- Pilot-backed transports. Each Open call connects to the
	// corresponding daemon socket, reads NodeID via driver.Info, and binds
	// :1004 for inbound.
	trA, err := pilot.Open(pilot.Config{SocketPath: a.Socket, ListenPort: 1004, Logger: logger})
	if err != nil {
		t.Fatalf("pilot.Open A: %v", err)
	}
	t.Cleanup(func() { _ = trA.Close() })
	trB, err := pilot.Open(pilot.Config{SocketPath: b.Socket, ListenPort: 1004, Logger: logger})
	if err != nil {
		t.Fatalf("pilot.Open B: %v", err)
	}
	t.Cleanup(func() { _ = trB.Close() })
	trC, err := pilot.Open(pilot.Config{SocketPath: c.Socket, ListenPort: 1004, Logger: logger})
	if err != nil {
		t.Fatalf("pilot.Open C: %v", err)
	}
	t.Cleanup(func() { _ = trC.Close() })

	// Sanity-check: NodeID we got from pilotctl matches what the driver
	// reports. If this fails the whole test is compromised and we bail now.
	if trA.NodeID() != a.NodeID {
		t.Fatalf("pilot adapter A NodeID %d != pilotctl %d", trA.NodeID(), a.NodeID)
	}
	if trB.NodeID() != b.NodeID {
		t.Fatalf("pilot adapter B NodeID %d != pilotctl %d", trB.NodeID(), b.NodeID)
	}
	if trC.NodeID() != c.NodeID {
		t.Fatalf("pilot adapter C NodeID %d != pilotctl %d", trC.NodeID(), c.NodeID)
	}

	// ---- Gossipers.
	newGossiper := func(n entmoot.NodeID, id *keystore.Identity, r *roster.RosterLog, s store.MessageStore, tr *pilot.Transport) *gossip.Gossiper {
		t.Helper()
		g, err := gossip.New(gossip.Config{
			LocalNode: n,
			Identity:  id,
			Roster:    r,
			Store:     s,
			Transport: tr,
			GroupID:   gid,
			Fanout:    2,
			Logger:    logger,
		})
		if err != nil {
			t.Fatalf("gossip.New for %d: %v", n, err)
		}
		return g
	}
	gA := newGossiper(a.NodeID, idA, rosterA, storeA, trA)
	gB := newGossiper(b.NodeID, idB, rosterB, storeB, trB)
	gC := newGossiper(c.NodeID, idC, rosterC, storeC, trC)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(g *gossip.Gossiper, name string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.Start(ctx); err != nil {
				logger.Warn("pilot canary: gossiper exit", slog.String("node", name), slog.String("err", err.Error()))
			}
		}()
	}
	start(gA, "a")
	start(gB, "b")
	start(gC, "c")
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	logger.Info("pilot canary: gossipers started; publishing 3 messages")
	msg1 := mkMessage(t, idA, infoA, gid, "canary", "hello from A", tsBase+1_000)
	if err := gA.Publish(ctx, msg1); err != nil {
		t.Fatalf("gA.Publish msg1: %v", err)
	}
	// Pilot's first dial is slow: space the sends slightly so each Publish
	// gets at least a moment to fan out before the next one races it.
	time.Sleep(500 * time.Millisecond)
	msg2 := mkMessage(t, idB, infoB, gid, "canary", "hello from B", tsBase+1_100)
	if err := gB.Publish(ctx, msg2); err != nil {
		t.Fatalf("gB.Publish msg2: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	msg3 := mkMessage(t, idA, infoA, gid, "canary", "A again", tsBase+1_200)
	if err := gA.Publish(ctx, msg3); err != nil {
		t.Fatalf("gA.Publish msg3: %v", err)
	}

	stores := map[entmoot.NodeID]store.MessageStore{
		a.NodeID: storeA, b.NodeID: storeB, c.NodeID: storeC,
	}
	root, err := waitForConvergence(t, stores, gid, 3, 45*time.Second, 500*time.Millisecond, true)
	if err != nil {
		t.Fatalf("converge: %v", err)
	}
	logger.Info("pilot canary: converged",
		slog.String("root_prefix", formatRootPrefix(root)))

	// Final checks.
	for _, msg := range []entmoot.Message{msg1, msg2, msg3} {
		for n, s := range stores {
			getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
			got, err := s.Get(getCtx, gid, msg.ID)
			getCancel()
			if err != nil {
				t.Fatalf("store %d Get %s: %v", n, msg.ID, err)
			}
			if got.ID != msg.ID {
				t.Fatalf("store %d id mismatch", n)
			}
			if string(got.Content) != string(msg.Content) {
				t.Fatalf("store %d content mismatch", n)
			}
		}
	}
}

// formatRootPrefix renders the first 8 hex chars of a Merkle root for logs.
func formatRootPrefix(root [32]byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i := 0; i < 8; i++ {
		out[i*2] = hex[root[i]>>4]
		out[i*2+1] = hex[root[i]&0x0F]
	}
	return string(out)
}
