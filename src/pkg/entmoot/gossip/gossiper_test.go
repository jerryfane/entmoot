package gossip

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

// --- test fixture ---------------------------------------------------------

// fixture bundles the per-node state every multi-node test needs: identity,
// NodeInfo, roster log, message store, and the in-memory transport. The
// fixture's Gossipers share one group id and a single genesis entry signed
// by the founder identity; every member is added via Apply so roster state
// is identical across nodes without a real bootstrap flow.
type fixture struct {
	t *testing.T

	groupID    entmoot.GroupID
	founder    *keystore.Identity
	founderInf entmoot.NodeInfo
	founderTS  int64 // most recently-applied roster entry timestamp

	transports map[entmoot.NodeID]Transport
	nodes      map[entmoot.NodeID]*nodeState
}

// nodeState is the per-peer state inside a fixture.
type nodeState struct {
	id      *keystore.Identity
	info    entmoot.NodeInfo
	rost    *roster.RosterLog
	storeM  store.MessageStore
	gossip  *Gossiper
	running atomic.Bool
}

type failFirstDialTransport struct {
	Transport
	mu        sync.Mutex
	remaining int
}

func (t *failFirstDialTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	if t.remaining > 0 {
		t.remaining--
		t.mu.Unlock()
		return nil, errors.New("scripted transient dial failure")
	}
	t.mu.Unlock()
	return t.Transport.Dial(ctx, peer)
}

type rosterRespTransport struct {
	Transport
	mu    sync.Mutex
	dials int
	resp  *wire.RosterResp
}

func (t *rosterRespTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	t.dials++
	t.mu.Unlock()

	client, server := net.Pipe()
	go func() {
		defer server.Close()
		_, _, _ = wire.ReadAndDecode(server)
		_ = wire.EncodeAndWrite(server, t.resp)
	}()
	return client, nil
}

func (t *rosterRespTransport) dialCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dials
}

type rosterRespOnceTransport struct {
	Transport
	mu       sync.Mutex
	peer     entmoot.NodeID
	used     bool
	response *wire.RosterResp
}

func (t *rosterRespOnceTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	t.mu.Lock()
	if peer == t.peer && !t.used {
		t.used = true
		t.mu.Unlock()
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			_, _, _ = wire.ReadAndDecode(server)
			_ = wire.EncodeAndWrite(server, t.response)
		}()
		return client, nil
	}
	t.mu.Unlock()
	return t.Transport.Dial(ctx, peer)
}

type rosterStallTransport struct {
	Transport
}

func (t rosterStallTransport) Dial(ctx context.Context, peer entmoot.NodeID) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		_, _, _ = wire.ReadAndDecode(server)
		_, _ = io.Copy(io.Discard, server)
	}()
	return client, nil
}

// newFixture builds a fixture with nodes named by the supplied NodeIDs. The
// first NodeID is treated as the founder. Every node gets its own Roster log
// and its own Memory store, but they all share a single in-memory hub, so
// any node can Dial any other.
func newFixture(t *testing.T, nodeIDs []entmoot.NodeID) *fixture {
	t.Helper()

	// Group id: deterministic per-test so log output is readable.
	var gid entmoot.GroupID
	for i := range gid {
		gid[i] = byte(i + 1)
	}

	founderID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate founder: %v", err)
	}
	founderInfo := entmoot.NodeInfo{
		PilotNodeID:   nodeIDs[0],
		EntmootPubKey: []byte(founderID.PublicKey),
	}

	f := &fixture{
		t:          t,
		groupID:    gid,
		founder:    founderID,
		founderInf: founderInfo,
		founderTS:  1_000,
		transports: NewMemTransports(nodeIDs),
		nodes:      make(map[entmoot.NodeID]*nodeState, len(nodeIDs)),
	}

	// Seed each node with an identity + empty roster + memory store.
	for _, n := range nodeIDs {
		var id *keystore.Identity
		var info entmoot.NodeInfo
		if n == nodeIDs[0] {
			id = founderID
			info = founderInfo
		} else {
			fresh, err := keystore.Generate()
			if err != nil {
				t.Fatalf("keystore.Generate %d: %v", n, err)
			}
			id = fresh
			info = entmoot.NodeInfo{
				PilotNodeID:   n,
				EntmootPubKey: []byte(fresh.PublicKey),
			}
		}
		r := roster.New(gid)
		// Seed genesis on every roster — shares a founder identity.
		if err := r.Genesis(founderID, founderInfo, f.founderTS); err != nil {
			t.Fatalf("roster.Genesis on %d: %v", n, err)
		}
		s := store.NewMemory()
		f.nodes[n] = &nodeState{id: id, info: info, rost: r, storeM: s}
	}

	// Add non-founder nodes to every roster so membership is globally
	// consistent. Apply entries in a fixed order and bump founderTS so
	// monotonicity holds.
	for _, n := range nodeIDs {
		if n == nodeIDs[0] {
			continue
		}
		f.founderTS += 100
		subject := f.nodes[n].info
		for _, rn := range nodeIDs {
			r := f.nodes[rn].rost
			entry := f.buildAddEntry(subject, f.founderTS, r.Head())
			if err := r.Apply(entry); err != nil {
				t.Fatalf("roster.Apply add %d to roster %d: %v", n, rn, err)
			}
		}
	}

	// Build a Gossiper per node now that rosters are consistent.
	fakeClk := clock.NewFake(time.UnixMilli(f.founderTS))
	for _, n := range nodeIDs {
		ns := f.nodes[n]
		g, err := New(Config{
			LocalNode: n,
			Identity:  ns.id,
			Roster:    ns.rost,
			Store:     ns.storeM,
			Transport: f.transports[n],
			GroupID:   gid,
			Fanout:    defaultFanout,
			Clock:     fakeClk,
			Logger:    slog.Default(),
		})
		if err != nil {
			t.Fatalf("gossip.New for %d: %v", n, err)
		}
		ns.gossip = g
	}
	return f
}

// buildAddEntry signs and returns an add(subject) roster entry with the
// given timestamp and parents. The entry is signed by the fixture's
// founder identity so it passes roster.Apply.
func (f *fixture) buildAddEntry(subject entmoot.NodeInfo, ts int64, parent entmoot.RosterEntryID) entmoot.RosterEntry {
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   subject,
		Actor:     f.founderInf.PilotNodeID,
		Timestamp: ts,
		Parents:   []entmoot.RosterEntryID{parent},
	}
	sig, err := canonical.Encode(entry)
	if err != nil {
		f.t.Fatalf("canonical encode roster entry: %v", err)
	}
	entry.Signature = f.founder.Sign(sig)
	entry.ID = canonical.RosterEntryID(entry)
	return entry
}

// startAll launches each node's accept loop on the supplied context.
func (f *fixture) startAll(ctx context.Context) {
	for _, ns := range f.nodes {
		ns.running.Store(true)
		ns := ns
		go func() {
			defer ns.running.Store(false)
			_ = ns.gossip.Start(ctx)
		}()
	}
	waitUntil(f.t, time.Second, "gossip start contexts installed", func() bool {
		for _, ns := range f.nodes {
			ns.gossip.lifeMu.Lock()
			lifeCtx := ns.gossip.lifeCtx
			ns.gossip.lifeMu.Unlock()
			if lifeCtx == nil {
				return false
			}
		}
		return true
	})
}

// closeTransports closes every transport.
func (f *fixture) closeTransports() {
	for _, tr := range f.transports {
		_ = tr.Close()
	}
}

// buildMessage builds and signs a message authored by the supplied node.
// The message has a single topic, no parents, and a deterministic timestamp.
func (f *fixture) buildMessage(author entmoot.NodeID, content string, ts int64) entmoot.Message {
	ns, ok := f.nodes[author]
	if !ok {
		f.t.Fatalf("unknown author %d", author)
	}
	msg := entmoot.Message{
		GroupID:   f.groupID,
		Author:    ns.info,
		Timestamp: ts,
		Topics:    []string{"test"},
		Content:   []byte(content),
	}
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		f.t.Fatalf("canonical encode message: %v", err)
	}
	msg.Signature = ns.id.Sign(sigInput)
	msg.ID = canonical.MessageID(msg)
	return msg
}

func (f *fixture) replaceGossiperWithPolicy(node entmoot.NodeID, p *policy.Policy, clk clock.Clock) {
	f.t.Helper()
	ns := f.nodes[node]
	if clk == nil {
		clk = ns.gossip.clk
	}
	g, err := New(Config{
		LocalNode:   node,
		Identity:    ns.id,
		Roster:      ns.rost,
		Store:       ns.storeM,
		Transport:   f.transports[node],
		GroupID:     f.groupID,
		Fanout:      defaultFanout,
		Clock:       clk,
		Logger:      slog.Default(),
		GroupPolicy: p,
	})
	if err != nil {
		f.t.Fatalf("gossip.New with policy for %d: %v", node, err)
	}
	ns.gossip = g
}

func (f *fixture) signedInlineGossip(relay entmoot.NodeID, msg entmoot.Message) *wire.Gossip {
	f.t.Helper()
	frame := &wire.Gossip{
		GroupID:   f.groupID,
		IDs:       []entmoot.MessageID{msg.ID},
		Body:      &msg,
		Timestamp: msg.Timestamp,
	}
	if err := signGossip(frame, f.nodes[relay].id); err != nil {
		f.t.Fatalf("signGossip: %v", err)
	}
	return frame
}

// waitUntil polls fn at a small interval until it returns true or timeout
// elapses. Returns true on success and fails the test with msg on timeout.
func waitUntil(t *testing.T, timeout time.Duration, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for: %s", msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- tests ----------------------------------------------------------------

// 1. Single-node gossip: Publish stores the message locally; no outbound
// attempted because there are no other members.
func TestPublishSingleNode(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	msg := f.buildMessage(10, "hello", 2_000)
	if err := f.nodes[10].gossip.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	has, err := f.nodes[10].storeM.Has(context.Background(), f.groupID, msg.ID)
	if err != nil {
		t.Fatalf("store.Has: %v", err)
	}
	if !has {
		t.Fatalf("expected local store to contain published message")
	}
}

// 2. Two-node: A publishes, B receives via push and fetches the body.
func TestPublishTwoNode(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "from A", 2_000)
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}

	waitUntil(t, time.Second, "B stores A's message", func() bool {
		has, _ := f.nodes[20].storeM.Has(ctx, f.groupID, msg.ID)
		return has
	})
}

func TestPolicyInboundContentLimitUsesAuthorAcrossRelays(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30, 40})
	defer f.closeTransports()
	p := policy.TheEntMootDefault()
	p.MessageRatePerAuthor = "1/hour"
	p.MessageBurstPerAuthor = 1
	f.replaceGossiperWithPolicy(40, &p, nil)

	first := f.buildMessage(10, "first", 2_000)
	second := f.buildMessage(10, "second", 2_001)
	f.nodes[40].gossip.onGossip(context.Background(), 20, f.signedInlineGossip(20, first))
	f.nodes[40].gossip.onGossip(context.Background(), 30, f.signedInlineGossip(30, second))

	if has, err := f.nodes[40].storeM.Has(context.Background(), f.groupID, first.ID); err != nil || !has {
		t.Fatalf("first stored has/err = %v/%v, want true/nil", has, err)
	}
	if has, err := f.nodes[40].storeM.Has(context.Background(), f.groupID, second.ID); err != nil || has {
		t.Fatalf("second stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestPolicyOversizedInboundMessageRejectedBeforeStorage(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()
	p := policy.TheEntMootDefault()
	p.MaxMessageBytes = 3
	f.replaceGossiperWithPolicy(20, &p, nil)

	msg := f.buildMessage(10, "too-large", 2_000)
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, msg))
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, msg.ID); err != nil || has {
		t.Fatalf("oversized stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestSetGroupPolicyAppliesToRunningGossiper(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	beforePolicy := f.buildMessage(10, "allowed-before-policy", 2_000)
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, beforePolicy))
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, beforePolicy.ID); err != nil || !has {
		t.Fatalf("before policy stored has/err = %v/%v, want true/nil", has, err)
	}

	p := policy.TheEntMootDefault()
	p.MaxMessageBytes = 3
	if err := f.nodes[20].gossip.SetGroupPolicy(p); err != nil {
		t.Fatalf("SetGroupPolicy: %v", err)
	}
	afterPolicy := f.buildMessage(10, "too-large-after-policy", 2_001)
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, afterPolicy))
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, afterPolicy.ID); err != nil || has {
		t.Fatalf("after policy stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestPolicyRetentionPrunesConfiguredGroup(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()
	const dayMS = int64(24 * time.Hour / time.Millisecond)
	clk := clock.NewFake(time.UnixMilli(3 * dayMS))
	p := policy.TheEntMootDefault()
	p.RetentionDays = 1
	f.replaceGossiperWithPolicy(20, &p, clk)

	old := f.buildMessage(10, "old", dayMS)
	newer := f.buildMessage(10, "new", 3*dayMS)
	if err := f.nodes[20].storeM.Put(context.Background(), old); err != nil {
		t.Fatalf("Put old: %v", err)
	}
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, newer))

	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, old.ID); err != nil || has {
		t.Fatalf("old retained has/err = %v/%v, want false/nil", has, err)
	}
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, newer.ID); err != nil || !has {
		t.Fatalf("new retained has/err = %v/%v, want true/nil", has, err)
	}
}

func TestPolicyExpiredInboundDoesNotConsumeAuthorQuota(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()
	const dayMS = int64(24 * time.Hour / time.Millisecond)
	clk := clock.NewFake(time.UnixMilli(3 * dayMS))
	p := policy.TheEntMootDefault()
	p.MessageRatePerAuthor = "1/hour"
	p.MessageBurstPerAuthor = 1
	p.RetentionDays = 1
	f.replaceGossiperWithPolicy(20, &p, clk)

	expired := f.buildMessage(10, "expired", dayMS)
	fresh := f.buildMessage(10, "fresh", 3*dayMS)
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, expired))
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, fresh))

	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, expired.ID); err != nil || has {
		t.Fatalf("expired stored has/err = %v/%v, want false/nil", has, err)
	}
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, fresh.ID); err != nil || !has {
		t.Fatalf("fresh stored has/err = %v/%v, want true/nil", has, err)
	}
}

func TestPolicyExpiredLocalPublishRejectedBeforeStorage(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()
	const dayMS = int64(24 * time.Hour / time.Millisecond)
	clk := clock.NewFake(time.UnixMilli(3 * dayMS))
	p := policy.TheEntMootDefault()
	p.RetentionDays = 1
	f.replaceGossiperWithPolicy(10, &p, clk)

	expired := f.buildMessage(10, "expired local", dayMS)
	if err := f.nodes[10].gossip.Publish(context.Background(), expired); err == nil {
		t.Fatal("Publish expired returned nil, want error")
	}
	if has, err := f.nodes[10].storeM.Has(context.Background(), f.groupID, expired.ID); err != nil || has {
		t.Fatalf("expired local stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestPolicyOversizedLocalPublishRejectedBeforeStorage(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()
	p := policy.TheEntMootDefault()
	p.MaxMessageBytes = 3
	f.replaceGossiperWithPolicy(10, &p, nil)

	msg := f.buildMessage(10, "too-large", 2_000)
	if err := f.nodes[10].gossip.Publish(context.Background(), msg); err == nil {
		t.Fatal("Publish oversized returned nil, want error")
	}
	if has, err := f.nodes[10].storeM.Has(context.Background(), f.groupID, msg.ID); err != nil || has {
		t.Fatalf("oversized local stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestPolicyLocalPublishConsumesAuthorQuota(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()
	p := policy.TheEntMootDefault()
	p.MessageRatePerAuthor = "1/hour"
	p.MessageBurstPerAuthor = 1
	f.replaceGossiperWithPolicy(10, &p, nil)

	first := f.buildMessage(10, "first", 2_000)
	second := f.buildMessage(10, "second", 2_001)
	if err := f.nodes[10].gossip.Publish(context.Background(), first); err != nil {
		t.Fatalf("Publish first: %v", err)
	}
	if err := f.nodes[10].gossip.Publish(context.Background(), second); err == nil {
		t.Fatal("Publish second returned nil, want rate limit error")
	}
	if has, err := f.nodes[10].storeM.Has(context.Background(), f.groupID, first.ID); err != nil || !has {
		t.Fatalf("first local stored has/err = %v/%v, want true/nil", has, err)
	}
	if has, err := f.nodes[10].storeM.Has(context.Background(), f.groupID, second.ID); err != nil || has {
		t.Fatalf("second local stored has/err = %v/%v, want false/nil", has, err)
	}
}

func TestNoGroupPolicyPreservesExistingInboundBehavior(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	msg := f.buildMessage(10, strings.Repeat("x", policy.DefaultMaxMessageBytes+1), 2_000)
	f.nodes[20].gossip.onGossip(context.Background(), 10, f.signedInlineGossip(10, msg))
	if has, err := f.nodes[20].storeM.Has(context.Background(), f.groupID, msg.ID); err != nil || !has {
		t.Fatalf("message stored has/err = %v/%v, want true/nil", has, err)
	}
}

// 3. Three-node: A publishes, both B and C receive (fanout covers both).
func TestPublishThreeNodeFanout(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20, 30})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	msg := f.buildMessage(10, "everyone", 2_000)
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}
	for _, id := range []entmoot.NodeID{20, 30} {
		id := id
		waitUntil(t, time.Second, "node has A's message", func() bool {
			has, _ := f.nodes[id].storeM.Has(ctx, f.groupID, msg.ID)
			return has
		})
	}
}

// TestPublishReturnsBeforeFanout asserts the v1.0.3 contract: Publish
// returns on local-durable accept, not on peer delivery. Peer B's Start
// loop is intentionally NOT running, so any push to B writes to a
// net.Pipe whose other side nobody reads — pushGossip would block
// forever under the pre-v1.0.3 code path. Publish on A must still
// return fast (target: well under 200ms; the async fanout goroutine
// stays hung in the background, which is fine — it's isolated from
// the caller).
func TestPublishReturnsBeforeFanout(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start A only — we need Publish to see g.lifeCtx != nil so it
	// takes the async path. B's Start is deliberately NOT called, so
	// B never drains its acceptCh and the net.Pipe write hangs.
	f.nodes[10].running.Store(true)
	go func() {
		defer f.nodes[10].running.Store(false)
		_ = f.nodes[10].gossip.Start(ctx)
	}()
	// Let Start set lifeCtx. Tests are fast; 50ms is comfortable.
	time.Sleep(50 * time.Millisecond)

	msg := f.buildMessage(10, "fast-return test", 2_000)
	start := time.Now()
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Publish took %v — expected fast return (<200ms) even with hung peer", elapsed)
	}

	// Local store must have the message immediately.
	has, err := f.nodes[10].storeM.Has(ctx, f.groupID, msg.ID)
	if err != nil {
		t.Fatalf("store.Has: %v", err)
	}
	if !has {
		t.Fatalf("expected A's local store to contain published message after fast return")
	}
}

// TestPublishReturnsBeforeTrustedPeerLookup asserts the stricter publish
// contract introduced for service/mobile clients: local durable accept must not
// wait on Pilot trust IPC. The trust oracle is only a fanout optimization, so a
// stuck TrustedPeers call must not make the CLI report failure after the store
// has already accepted the message.
func TestPublishReturnsBeforeTrustedPeerLookup(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	f.nodes[10].gossip.cfg.Transport = blockingTrustedPeersTransport{
		Transport: f.transports[10],
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f.nodes[10].running.Store(true)
	go func() {
		defer f.nodes[10].running.Store(false)
		_ = f.nodes[10].gossip.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	msg := f.buildMessage(10, "fast-return despite stuck trust IPC", 2_000)
	start := time.Now()
	if err := f.nodes[10].gossip.Publish(ctx, msg); err != nil {
		t.Fatalf("Publish on A: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("Publish took %v — expected fast return (<200ms) even with blocked TrustedPeers", elapsed)
	}

	has, err := f.nodes[10].storeM.Has(ctx, f.groupID, msg.ID)
	if err != nil {
		t.Fatalf("store.Has: %v", err)
	}
	if !has {
		t.Fatalf("expected A's local store to contain published message after fast return")
	}
}

type blockingTrustedPeersTransport struct {
	Transport
}

func (t blockingTrustedPeersTransport) TrustedPeers(ctx context.Context) ([]entmoot.NodeID, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// 4. Publish from a non-member: forge a message with an author that is not
// in the roster. Publish must reject with entmoot.ErrNotMember.
func TestPublishNonMemberRejected(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	// Craft a message whose Author.PilotNodeID (99) is NOT a roster member.
	outsider, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	msg := entmoot.Message{
		GroupID: f.groupID,
		Author: entmoot.NodeInfo{
			PilotNodeID:   99,
			EntmootPubKey: []byte(outsider.PublicKey),
		},
		Timestamp: 2_000,
		Content:   []byte("ghost"),
	}
	signing := msg
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		t.Fatalf("canonical encode: %v", err)
	}
	msg.Signature = outsider.Sign(sigInput)
	msg.ID = canonical.MessageID(msg)

	err = f.nodes[10].gossip.Publish(context.Background(), msg)
	if !errors.Is(err, entmoot.ErrNotMember) {
		t.Fatalf("Publish of non-member: got %v, want ErrNotMember", err)
	}
}

func TestPublishCanonicalIDMismatchRejected(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	msg := f.buildMessage(10, "signed but wrong id", 2_000)
	msg.ID[0] ^= 0xff
	err := f.nodes[10].gossip.Publish(context.Background(), msg)
	if !errors.Is(err, entmoot.ErrSigInvalid) {
		t.Fatalf("Publish canonical ID mismatch: got %v, want ErrSigInvalid", err)
	}
}

// 5. FetchReq for unknown id returns FetchResp{NotFound: true}.
func TestFetchReqUnknown(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	// Dial A from B and ask for a random id A does not have.
	var id entmoot.MessageID
	for i := range id {
		id[i] = 0xFE
	}
	conn, err := f.transports[20].Dial(ctx, 10)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	req := &wire.FetchReq{GroupID: f.groupID, ID: id}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		t.Fatalf("write fetch_req: %v", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		t.Fatalf("read fetch_resp: %v", err)
	}
	resp, ok := payload.(*wire.FetchResp)
	if !ok {
		t.Fatalf("unexpected payload type %T", payload)
	}
	if !resp.NotFound {
		t.Fatalf("expected NotFound=true, got %+v", resp)
	}
	if resp.Message != nil {
		t.Fatalf("expected nil Message, got %+v", resp.Message)
	}
}

// 6. Forged FetchResp at the receiver: the receiver dials the peer, the peer
// replies with a message whose signature does not verify. We simulate this by
// hand-crafting the exchange — rather than running Publish, we drive the
// fetch/verify path through a low-level interaction. The receiver must NOT
// store the forged body.
func TestFetchForgedBodyRejected(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// B accepts connections: we hijack B's accept loop to respond manually.
	// Drive by running only A's accept loop and not B's.
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	// Build a message whose claimed author is 10 but signed with the WRONG
	// key. Insert directly into A's store so FetchReq returns it; then call
	// B.fetchFrom(10, id) and assert the body is NOT stored on B.
	victim := f.buildMessage(10, "real", 2_000)
	// Replace victim.Signature with garbage while keeping the ID.
	forged := victim
	forged.Signature = make([]byte, len(victim.Signature))
	for i := range forged.Signature {
		forged.Signature[i] = 0xAB
	}
	// Inject the forged message directly into A's store via Put. The Memory
	// store accepts any non-zero ID/GroupID.
	if err := f.nodes[10].storeM.Put(ctx, forged); err != nil {
		t.Fatalf("seed forged in A: %v", err)
	}

	// Now have B fetch from A. fetchFrom verifies the signature against
	// A's entmoot pubkey (known via the roster); the garbage signature
	// will fail and the body will not be stored on B.
	if err := f.nodes[20].gossip.fetchFrom(ctx, 10, forged.ID); err == nil {
		t.Fatalf("expected error fetching forged body")
	}
	has, err := f.nodes[20].storeM.Has(ctx, f.groupID, forged.ID)
	if err != nil {
		t.Fatalf("B store.Has: %v", err)
	}
	if has {
		t.Fatalf("B stored forged body; expected rejection")
	}
	cancel()
}

// 7. RosterReq: joiner with only genesis calls Join and catches up via
// RosterResp from A. After Join, joiner.Members() matches A.Members().
func TestRosterReqSyncViaJoin(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 30, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start A, B, C accept loops but NOT 99 (the joiner).
	for _, n := range []entmoot.NodeID{10, 20, 30} {
		ns := f.nodes[n]
		go func() { _ = ns.gossip.Start(ctx) }()
	}

	invite := f.buildInvite([]entmoot.NodeID{10, 20, 30})
	if err := f.nodes[99].gossip.Join(ctx, invite); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got := f.nodes[99].rost.Members()
	want := f.nodes[10].rost.Members()
	if len(got) != len(want) {
		t.Fatalf("member count mismatch: got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("member[%d]: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestJoinRetriesBootstrapUntilDeadline(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	joiner := f.nodes[99].gossip
	joiner.cfg.Transport = &failFirstDialTransport{
		Transport: joiner.cfg.Transport,
		remaining: 3,
	}

	joinCtx, joinCancel := context.WithTimeout(ctx, 2*time.Second)
	defer joinCancel()
	invite := f.buildInvite([]entmoot.NodeID{10})
	if err := joiner.Join(joinCtx, invite); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got := f.nodes[99].rost.Members()
	want := f.nodes[10].rost.Members()
	if len(got) != len(want) {
		t.Fatalf("member count mismatch: got %v want %v", got, want)
	}
}

func TestJoinRetryDelayBacksOffAndCaps(t *testing.T) {
	if got := nextJoinRetryDelay(joinRetryBase); got != time.Second {
		t.Fatalf("nextJoinRetryDelay(base) = %s, want 1s", got)
	}
	if got := nextJoinRetryDelay(2 * time.Second); got != joinRetryCap {
		t.Fatalf("nextJoinRetryDelay(2s) = %s, want %s", got, joinRetryCap)
	}
	if got := nextJoinRetryDelay(joinRetryCap); got != joinRetryCap {
		t.Fatalf("nextJoinRetryDelay(cap) = %s, want %s", got, joinRetryCap)
	}
}

func TestTryRosterSyncCancelsBlockedRead(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 99}, 99)
	defer f.closeTransports()

	joiner := f.nodes[99].gossip
	joiner.cfg.Transport = rosterStallTransport{Transport: joiner.cfg.Transport}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- joiner.tryRosterSync(ctx, 10)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("tryRosterSync unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("tryRosterSync did not return after context deadline")
	}
}

func TestJoinDoesNotRetryPermanentRosterError(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 99}, 99)
	defer f.closeTransports()

	var wrongGroup entmoot.GroupID
	wrongGroup[0] = 99
	tr := &rosterRespTransport{
		Transport: f.nodes[99].gossip.cfg.Transport,
		resp: &wire.RosterResp{
			GroupID: wrongGroup,
			Entries: f.nodes[10].rost.Entries(),
		},
	}
	f.nodes[99].gossip.cfg.Transport = tr

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	invite := f.buildInvite([]entmoot.NodeID{10})
	err := f.nodes[99].gossip.Join(ctx, invite)
	if err == nil {
		t.Fatal("Join unexpectedly succeeded")
	}
	if errors.Is(err, ErrJoinFailed) {
		t.Fatalf("Join returned retryable ErrJoinFailed for permanent error: %v", err)
	}
	if got := tr.dialCount(); got != 3 {
		t.Fatalf("dials = %d, want one pass through the available join strategies", got)
	}
}

func TestJoinContinuesAfterPermanentPeerError(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()

	var wrongGroup entmoot.GroupID
	wrongGroup[0] = 99
	joiner := f.nodes[99].gossip
	joiner.cfg.Transport = &rosterRespOnceTransport{
		Transport: joiner.cfg.Transport,
		peer:      20,
		response: &wire.RosterResp{
			GroupID: wrongGroup,
			Entries: f.nodes[10].rost.Entries(),
		},
	}

	joinCtx, joinCancel := context.WithTimeout(ctx, 2*time.Second)
	defer joinCancel()
	invite := f.buildInvite([]entmoot.NodeID{20, 10})
	if err := joiner.Join(joinCtx, invite); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got := f.nodes[99].rost.Members()
	want := f.nodes[10].rost.Members()
	if len(got) != len(want) {
		t.Fatalf("member count mismatch: got %v want %v", got, want)
	}
}

func TestJoinContinuesAfterStaleRosterHead(t *testing.T) {
	t.Parallel()
	f := newFixtureWithGenesisOnly(t, []entmoot.NodeID{10, 20, 99}, 99)
	defer f.closeTransports()

	entries := f.nodes[10].rost.Entries()
	if len(entries) < 2 {
		t.Fatalf("fixture entries = %d, want at least 2", len(entries))
	}
	if err := f.nodes[99].rost.AcceptGenesis(entries[0]); err != nil {
		t.Fatalf("joiner AcceptGenesis: %v", err)
	}
	if err := f.nodes[99].rost.Apply(entries[1]); err != nil {
		t.Fatalf("joiner Apply partial head: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[10].gossip.Start(ctx) }()
	go func() { _ = f.nodes[20].gossip.Start(ctx) }()

	joiner := f.nodes[99].gossip
	joiner.cfg.Transport = &rosterRespOnceTransport{
		Transport: joiner.cfg.Transport,
		peer:      20,
		response: &wire.RosterResp{
			GroupID: f.groupID,
			Entries: []entmoot.RosterEntry{
				entries[0],
			},
		},
	}

	joinCtx, joinCancel := context.WithTimeout(ctx, 2*time.Second)
	defer joinCancel()
	invite := f.buildInvite([]entmoot.NodeID{20})
	if err := joiner.Join(joinCtx, invite); err != nil {
		t.Fatalf("Join: %v", err)
	}
	got := f.nodes[99].rost.Members()
	want := f.nodes[10].rost.Members()
	if len(got) != len(want) {
		t.Fatalf("member count mismatch: got %v want %v", got, want)
	}
}

func TestUnsolicitedRosterRespAppliesUpdate(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = f.nodes[20].gossip.Start(ctx) }()
	waitUntil(t, time.Second, "node 20 accept loop started", func() bool {
		f.nodes[20].gossip.lifeMu.Lock()
		defer f.nodes[20].gossip.lifeMu.Unlock()
		return f.nodes[20].gossip.lifeCtx != nil
	})

	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	target := entmoot.NodeInfo{PilotNodeID: 99, EntmootPubKey: append([]byte(nil), targetID.PublicKey...)}
	f.founderTS += 100
	entry := f.buildAddEntry(target, f.founderTS, f.nodes[10].rost.Head())
	if err := f.nodes[10].rost.Apply(entry); err != nil {
		t.Fatalf("founder roster apply: %v", err)
	}

	f.nodes[10].gossip.FanoutRoster(ctx, []entmoot.NodeID{20}, 99)
	waitUntil(t, time.Second, "node 20 received roster update", func() bool {
		info, ok := f.nodes[20].rost.MemberInfo(99)
		return ok && bytes.Equal(info.EntmootPubKey, targetID.PublicKey)
	})
}

func TestUnsolicitedRosterRespWrongGroupIgnored(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	target := entmoot.NodeInfo{PilotNodeID: 99, EntmootPubKey: append([]byte(nil), targetID.PublicKey...)}
	f.founderTS += 100
	entry := f.buildAddEntry(target, f.founderTS, f.nodes[10].rost.Head())
	var wrong entmoot.GroupID
	for i := range wrong {
		wrong[i] = 0xFE
	}
	f.nodes[20].gossip.onRosterResp(context.Background(), 10, &wire.RosterResp{
		GroupID: wrong,
		Entries: append(f.nodes[10].rost.Entries(), entry),
	})
	if _, ok := f.nodes[20].rost.MemberInfo(99); ok {
		t.Fatal("wrong-group roster_resp added target member")
	}
}

func TestUnsolicitedRosterRespDuplicateIsIdempotent(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	targetID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target: %v", err)
	}
	target := entmoot.NodeInfo{PilotNodeID: 99, EntmootPubKey: append([]byte(nil), targetID.PublicKey...)}
	f.founderTS += 100
	entry := f.buildAddEntry(target, f.founderTS, f.nodes[10].rost.Head())
	if err := f.nodes[10].rost.Apply(entry); err != nil {
		t.Fatalf("founder roster apply: %v", err)
	}
	resp := &wire.RosterResp{GroupID: f.groupID, Entries: f.nodes[10].rost.Entries()}
	f.nodes[20].gossip.onRosterResp(context.Background(), 10, resp)
	firstLen := len(f.nodes[20].rost.Entries())
	f.nodes[20].gossip.onRosterResp(context.Background(), 10, resp)
	if got := len(f.nodes[20].rost.Entries()); got != firstLen {
		t.Fatalf("len(entries) = %d after duplicate, want %d", got, firstLen)
	}
}

func TestUnsolicitedRosterRespConcurrentIsSerialized(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	targetAID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target A: %v", err)
	}
	targetA := entmoot.NodeInfo{PilotNodeID: 99, EntmootPubKey: append([]byte(nil), targetAID.PublicKey...)}
	f.founderTS += 100
	entryA := f.buildAddEntry(targetA, f.founderTS, f.nodes[10].rost.Head())
	if err := f.nodes[10].rost.Apply(entryA); err != nil {
		t.Fatalf("founder roster apply A: %v", err)
	}
	entriesA := f.nodes[10].rost.Entries()

	targetBID, err := keystore.Generate()
	if err != nil {
		t.Fatalf("Generate target B: %v", err)
	}
	targetB := entmoot.NodeInfo{PilotNodeID: 100, EntmootPubKey: append([]byte(nil), targetBID.PublicKey...)}
	f.founderTS += 100
	entryB := f.buildAddEntry(targetB, f.founderTS, f.nodes[10].rost.Head())
	if err := f.nodes[10].rost.Apply(entryB); err != nil {
		t.Fatalf("founder roster apply B: %v", err)
	}
	entriesB := f.nodes[10].rost.Entries()

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, entries := range [][]entmoot.RosterEntry{entriesA, entriesB} {
		entries := entries
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f.nodes[20].gossip.onRosterResp(context.Background(), 10, &wire.RosterResp{
				GroupID: f.groupID,
				Entries: entries,
			})
		}()
	}
	close(start)
	wg.Wait()

	if info, ok := f.nodes[20].rost.MemberInfo(99); !ok || !bytes.Equal(info.EntmootPubKey, targetAID.PublicKey) {
		t.Fatalf("target A member = %+v ok=%v, want target A pubkey", info, ok)
	}
	if info, ok := f.nodes[20].rost.MemberInfo(100); !ok || !bytes.Equal(info.EntmootPubKey, targetBID.PublicKey) {
		t.Fatalf("target B member = %+v ok=%v, want target B pubkey", info, ok)
	}
	if got, want := len(f.nodes[20].rost.Entries()), len(entriesB); got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
}

// 8. MerkleReq: A and B have the same messages → same root; different
// messages → different roots. Uses the accept-loop MerkleReq handler rather
// than Store.MerkleRoot directly.
func TestMerkleReq(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.startAll(ctx)

	ma := f.buildMessage(10, "one", 2_000)
	mb := f.buildMessage(10, "two", 2_100)
	// Put the same messages on both nodes.
	for _, m := range []entmoot.Message{ma, mb} {
		if err := f.nodes[10].storeM.Put(ctx, m); err != nil {
			t.Fatalf("seed A: %v", err)
		}
		if err := f.nodes[20].storeM.Put(ctx, m); err != nil {
			t.Fatalf("seed B: %v", err)
		}
	}
	rootA := f.requestMerkle(ctx, 20, 10) // B asks A
	rootB := f.requestMerkle(ctx, 10, 20) // A asks B
	if rootA.Root != rootB.Root {
		t.Fatalf("same messages yielded different roots: %x vs %x", rootA.Root, rootB.Root)
	}
	if rootA.MessageCount != 0 {
		t.Fatalf("expected MessageCount=0 on hot-path MerkleResp, got %d", rootA.MessageCount)
	}

	// Diverge B: add a third message only on B. Roots must now differ.
	extra := f.buildMessage(10, "only-B", 2_200)
	if err := f.nodes[20].storeM.Put(ctx, extra); err != nil {
		t.Fatalf("seed extra on B: %v", err)
	}
	rootBafter := f.requestMerkle(ctx, 10, 20)
	if rootBafter.Root == rootA.Root {
		t.Fatalf("expected roots to diverge after extra message on B")
	}
}

func TestInboundReconcileWaitsForOneShotResponse(t *testing.T) {
	t.Parallel()
	f := newFixture(t, []entmoot.NodeID{10, 20})
	defer f.closeTransports()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entered := make(chan struct{})
	release := make(chan struct{})
	base := f.nodes[20].storeM
	f.nodes[20].storeM = &blockingMerkleStore{
		MessageStore: base,
		entered:      entered,
		release:      release,
	}
	f.nodes[20].gossip.cfg.Store = f.nodes[20].storeM
	f.startAll(ctx)

	aG := f.nodes[20].gossip
	aG.pendMu.Lock()
	delete(aG.lastReconciled, 10)
	aG.pendMu.Unlock()

	conn, err := f.transports[10].Dial(ctx, 20)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.MerkleReq{GroupID: f.groupID}); err != nil {
		t.Fatalf("write merkle_req: %v", err)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("MerkleRoot was not called")
	}

	aG.pendMu.Lock()
	_, reconciledBeforeResponse := aG.lastReconciled[10]
	aG.pendMu.Unlock()
	if reconciledBeforeResponse {
		t.Fatal("inbound reconcile fired before one-shot response completed")
	}

	close(release)
	if _, _, err := wire.ReadAndDecode(conn); err != nil {
		t.Fatalf("read merkle_resp: %v", err)
	}
	waitUntil(t, 2*time.Second, "reconcile fires after one-shot response", func() bool {
		aG.pendMu.Lock()
		defer aG.pendMu.Unlock()
		_, ok := aG.lastReconciled[10]
		return ok
	})
}

type blockingMerkleStore struct {
	store.MessageStore
	entered chan struct{}
	release chan struct{}

	enteredOnce atomic.Bool
}

func (s *blockingMerkleStore) MerkleRoot(ctx context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	if s.enteredOnce.CompareAndSwap(false, true) {
		close(s.entered)
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return [32]byte{}, ctx.Err()
	}
	return s.MessageStore.MerkleRoot(ctx, groupID)
}

// requestMerkle issues a MerkleReq from fromNode to toNode via their shared
// transport and returns the decoded MerkleResp.
func (f *fixture) requestMerkle(ctx context.Context, fromNode, toNode entmoot.NodeID) *wire.MerkleResp {
	f.t.Helper()
	conn, err := f.transports[fromNode].Dial(ctx, toNode)
	if err != nil {
		f.t.Fatalf("Dial %d→%d: %v", fromNode, toNode, err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, &wire.MerkleReq{GroupID: f.groupID}); err != nil {
		f.t.Fatalf("write merkle_req: %v", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		f.t.Fatalf("read merkle_resp: %v", err)
	}
	resp, ok := payload.(*wire.MerkleResp)
	if !ok {
		f.t.Fatalf("unexpected payload %T", payload)
	}
	return resp
}
