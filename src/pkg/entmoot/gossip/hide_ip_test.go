package gossip

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ratelimit"
)

// TestTransportAdPublisher_HideIP_WithTurnEndpoint wires a LocalEndpoints
// callback that returns UDP + TCP + TURN and asserts the published ad
// contains ONLY the TURN entry when HideIP=true. This is the privacy
// path: an operator with -hide-ip set must never leak UDP/TCP IPs in a
// transport-ad regardless of what Pilot exposes locally.
func TestTransportAdPublisher_HideIP_WithTurnEndpoint(t *testing.T) {
	t.Parallel()
	// Single-node fixture: tryPublishTransportAd calls fanoutTransportAd
	// which would block on a net.Pipe write if peers existed in the hub
	// but their accept loops weren't running. One-node keeps the eager
	// set empty and the fanout is a no-op.
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	allEps := []entmoot.NodeEndpoint{
		{Network: "udp", Addr: "198.51.100.10:4001"},
		{Network: "tcp", Addr: "198.51.100.10:4001"},
		{Network: "turn", Addr: "relay.example.test:3478"},
	}

	ns := f.nodes[10]
	ns.gossip.cfg.TransportAdStore = newMemTransportAdStore()
	ns.gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)
	ns.gossip.cfg.LocalEndpoints = func() []entmoot.NodeEndpoint {
		return append([]entmoot.NodeEndpoint(nil), allEps...)
	}
	ns.gossip.cfg.HideIP = true

	ns.gossip.tryPublishTransportAd(context.Background(), "test")

	store := ns.gossip.cfg.TransportAdStore.(*memTransportAdStore)
	ad, ok := store.adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("no ad stored; expected one TURN-only ad")
	}
	if len(ad.Endpoints) != 1 {
		t.Fatalf("ad has %d endpoints, want exactly 1 (turn only); got=%+v",
			len(ad.Endpoints), ad.Endpoints)
	}
	if ad.Endpoints[0].Network != "turn" || ad.Endpoints[0].Addr != "relay.example.test:3478" {
		t.Fatalf("ad endpoint = %+v, want {turn relay.example.test:3478}", ad.Endpoints[0])
	}
}

// TestTransportAdPublisher_HideIP_NoTurnEndpoint asserts that a
// HideIP-enabled gossiper whose LocalEndpoints() returns UDP+TCP but NO
// turn entry publishes nothing (empty ad, no IP fallback) and emits a
// warning through the configured slog.Logger. The warning is the
// operator's only signal that the node is unreachable — a silent
// fallback to IP advertisement would defeat the privacy goal.
func TestTransportAdPublisher_HideIP_NoTurnEndpoint(t *testing.T) {
	t.Parallel()
	// Single-node fixture: tryPublishTransportAd calls fanoutTransportAd
	// which would block on a net.Pipe write if peers existed in the hub
	// but their accept loops weren't running. One-node keeps the eager
	// set empty and the fanout is a no-op.
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	ipOnlyEps := []entmoot.NodeEndpoint{
		{Network: "udp", Addr: "198.51.100.10:4001"},
		{Network: "tcp", Addr: "198.51.100.10:4001"},
	}

	var logBuf bytes.Buffer
	handler := slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	ns := f.nodes[10]
	ns.gossip.cfg.TransportAdStore = newMemTransportAdStore()
	ns.gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)
	ns.gossip.cfg.LocalEndpoints = func() []entmoot.NodeEndpoint {
		return append([]entmoot.NodeEndpoint(nil), ipOnlyEps...)
	}
	ns.gossip.cfg.HideIP = true
	// Swap the gossiper's logger so we can scrape the warning.
	ns.gossip.logger = logger

	ns.gossip.tryPublishTransportAd(context.Background(), "startup")

	store := ns.gossip.cfg.TransportAdStore.(*memTransportAdStore)
	if _, ok := store.adFor(f.groupID, 10); ok {
		t.Fatalf("ad was published despite HideIP=true && no TURN endpoint; privacy violated")
	}
	if got := store.putCallCount(); got != 0 {
		t.Fatalf("PutTransportAd called %d times; want 0 (silent no-op with warning)", got)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "hide-ip set but no TURN relay available") {
		t.Fatalf("missing warning log; got logs=%q", logs)
	}
	// Bonus: the reason tag should be present so operators can tell
	// startup-time warnings from refresh-time ones.
	if !strings.Contains(logs, "startup") {
		t.Fatalf("warning log missing reason tag %q; got logs=%q", "startup", logs)
	}
}

// TestTransportAdPublisher_NormalMode_WithTurn exercises the regression
// guard: HideIP=false with a mixed UDP+TCP+TURN slice must publish ALL
// three entries. The "turn" network string being free-form on the wire
// format means this has always been possible; the test pins the
// behavior so a future change to normalise the slice to IP-only cannot
// regress silently.
func TestTransportAdPublisher_NormalMode_WithTurn(t *testing.T) {
	t.Parallel()
	// Single-node fixture: tryPublishTransportAd calls fanoutTransportAd
	// which would block on a net.Pipe write if peers existed in the hub
	// but their accept loops weren't running. One-node keeps the eager
	// set empty and the fanout is a no-op.
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	allEps := []entmoot.NodeEndpoint{
		{Network: "udp", Addr: "198.51.100.10:4001"},
		{Network: "tcp", Addr: "198.51.100.10:4001"},
		{Network: "turn", Addr: "relay.example.test:3478"},
	}

	ns := f.nodes[10]
	ns.gossip.cfg.TransportAdStore = newMemTransportAdStore()
	ns.gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)
	ns.gossip.cfg.LocalEndpoints = func() []entmoot.NodeEndpoint {
		return append([]entmoot.NodeEndpoint(nil), allEps...)
	}
	ns.gossip.cfg.HideIP = false

	ns.gossip.tryPublishTransportAd(context.Background(), "test")

	store := ns.gossip.cfg.TransportAdStore.(*memTransportAdStore)
	ad, ok := store.adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("no ad stored; expected one UDP+TCP+TURN ad")
	}
	if len(ad.Endpoints) != 3 {
		t.Fatalf("ad has %d endpoints, want 3 (udp+tcp+turn); got=%+v",
			len(ad.Endpoints), ad.Endpoints)
	}
	nets := map[string]bool{}
	for _, ep := range ad.Endpoints {
		nets[ep.Network] = true
	}
	for _, want := range []string{"udp", "tcp", "turn"} {
		if !nets[want] {
			t.Fatalf("ad missing %q network; endpoints=%+v", want, ad.Endpoints)
		}
	}
}

// TestTransportAdPublisher_NormalMode_NoTurn is the pre-v1.4.0 regression
// guard: HideIP=false with only UDP+TCP must publish those two entries
// unchanged. This is the default behaviour for every v1.2.0+ node that
// doesn't opt into hide-IP mode; the TURN code path must be invisible
// on this path.
func TestTransportAdPublisher_NormalMode_NoTurn(t *testing.T) {
	t.Parallel()
	// Single-node fixture: tryPublishTransportAd calls fanoutTransportAd
	// which would block on a net.Pipe write if peers existed in the hub
	// but their accept loops weren't running. One-node keeps the eager
	// set empty and the fanout is a no-op.
	f := newFixture(t, []entmoot.NodeID{10})
	defer f.closeTransports()

	ipEps := []entmoot.NodeEndpoint{
		{Network: "udp", Addr: "198.51.100.10:4001"},
		{Network: "tcp", Addr: "198.51.100.10:4001"},
	}

	ns := f.nodes[10]
	ns.gossip.cfg.TransportAdStore = newMemTransportAdStore()
	ns.gossip.cfg.RateLimiter = ratelimit.New(ratelimit.DefaultLimits(), ns.gossip.clk)
	ns.gossip.cfg.LocalEndpoints = func() []entmoot.NodeEndpoint {
		return append([]entmoot.NodeEndpoint(nil), ipEps...)
	}
	ns.gossip.cfg.HideIP = false

	ns.gossip.tryPublishTransportAd(context.Background(), "test")

	store := ns.gossip.cfg.TransportAdStore.(*memTransportAdStore)
	ad, ok := store.adFor(f.groupID, 10)
	if !ok {
		t.Fatalf("no ad stored; expected one UDP+TCP ad")
	}
	if len(ad.Endpoints) != 2 {
		t.Fatalf("ad has %d endpoints, want 2 (udp+tcp); got=%+v",
			len(ad.Endpoints), ad.Endpoints)
	}
	for _, ep := range ad.Endpoints {
		if ep.Network == "turn" {
			t.Fatalf("normal-mode ad unexpectedly contains a turn endpoint: %+v", ep)
		}
	}
}

// TestFilterTurnOnly is a direct unit test for the HideIP filter
// helper. Covers the empty, all-IP, all-turn, and mixed cases so
// future callers of filterTurnOnly see the invariant pinned.
func TestFilterTurnOnly(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []entmoot.NodeEndpoint
		want []entmoot.NodeEndpoint
	}{
		{
			name: "empty input",
			in:   nil,
			want: []entmoot.NodeEndpoint{},
		},
		{
			name: "all ip",
			in: []entmoot.NodeEndpoint{
				{Network: "tcp", Addr: "198.51.100.1:4001"},
				{Network: "udp", Addr: "198.51.100.1:4001"},
			},
			want: []entmoot.NodeEndpoint{},
		},
		{
			name: "all turn",
			in: []entmoot.NodeEndpoint{
				{Network: "turn", Addr: "relay.example.test:3478"},
			},
			want: []entmoot.NodeEndpoint{
				{Network: "turn", Addr: "relay.example.test:3478"},
			},
		},
		{
			name: "mixed keeps turn only",
			in: []entmoot.NodeEndpoint{
				{Network: "tcp", Addr: "198.51.100.1:4001"},
				{Network: "turn", Addr: "relay.example.test:3478"},
				{Network: "udp", Addr: "198.51.100.1:4001"},
			},
			want: []entmoot.NodeEndpoint{
				{Network: "turn", Addr: "relay.example.test:3478"},
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := filterTurnOnly(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("len = %d, want %d; got=%+v", len(got), len(tc.want), got)
			}
			for i, ep := range got {
				if ep != tc.want[i] {
					t.Fatalf("entry %d = %+v, want %+v", i, ep, tc.want[i])
				}
			}
		})
	}
}
