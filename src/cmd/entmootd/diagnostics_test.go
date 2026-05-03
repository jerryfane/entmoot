package main

import (
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

func TestDiagnosePeerPrecedence(t *testing.T) {
	cases := []struct {
		name   string
		peer   doctorPeerReport
		probed bool
		want   string
	}{
		{
			name: "trust before profile",
			peer: doctorPeerReport{Roster: true, Trust: "missing", Profile: "missing", Transport: "missing", Route: "not_checked"},
			want: "trust_missing",
		},
		{
			name: "pending trust",
			peer: doctorPeerReport{Roster: true, Trust: "pending", Profile: "ok", Transport: "ok", Route: "not_checked"},
			want: "trust_pending",
		},
		{
			name:   "route timeout after passive prerequisites",
			peer:   doctorPeerReport{Roster: true, Trust: "trusted", Profile: "ok", Transport: "ok", Route: "route_timeout"},
			probed: true,
			want:   "route_timeout",
		},
		{
			name: "ok passive",
			peer: doctorPeerReport{Roster: true, Trust: "trusted", Profile: "ok", Transport: "ok", Route: "not_checked"},
			want: "ok",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diagnosePeer(tc.peer, tc.probed); got != tc.want {
				t.Fatalf("diagnosePeer = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseTrustedNodeIDs(t *testing.T) {
	got := parseTrustedNodeIDs(map[string]interface{}{
		"trusted": []interface{}{
			map[string]interface{}{"node_id": float64(42)},
			map[string]interface{}{"node_id": float64(7)},
			map[string]interface{}{"node_id": "bad"},
		},
	})
	want := []entmoot.NodeID{7, 42}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %d, want %d (all %v)", i, got[i], want[i], got)
		}
	}
}

func TestDoctorPeerTrustUnknownWhenTrustQueriesFail(t *testing.T) {
	trusted := map[entmoot.NodeID]struct{}{7: {}}
	pending := map[entmoot.NodeID]struct{}{8: {}}
	cases := []struct {
		name   string
		local  entmoot.NodeID
		node   entmoot.NodeID
		report doctorPilotReport
		want   string
	}{
		{
			name:   "self wins",
			local:  1,
			node:   1,
			report: doctorPilotReport{Reachable: true},
			want:   "self",
		},
		{
			name:   "trusted wins when pending query failed",
			node:   7,
			report: doctorPilotReport{Reachable: true, TrustedQueryOK: true, PendingQueryOK: false},
			want:   "trusted",
		},
		{
			name:   "pending wins when trusted query failed",
			node:   8,
			report: doctorPilotReport{Reachable: true, TrustedQueryOK: false, PendingQueryOK: true},
			want:   "pending",
		},
		{
			name:   "trusted query failure makes absent peer unknown",
			node:   9,
			report: doctorPilotReport{Reachable: true, TrustedQueryOK: false, PendingQueryOK: true},
			want:   "unknown",
		},
		{
			name:   "pending query failure makes absent peer unknown",
			node:   9,
			report: doctorPilotReport{Reachable: true, TrustedQueryOK: true, PendingQueryOK: false},
			want:   "unknown",
		},
		{
			name:   "both queries succeed and absent means missing",
			node:   9,
			report: doctorPilotReport{Reachable: true, TrustedQueryOK: true, PendingQueryOK: true},
			want:   "missing",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := doctorPeerTrust(tc.local, tc.node, tc.report, trusted, pending)
			if got != tc.want {
				t.Fatalf("doctorPeerTrust = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDoctorPilotReportTrustQueryErrors(t *testing.T) {
	report := doctorPilotReport{
		Reachable:      true,
		TrustedQueryOK: false,
		TrustedError:   "trusted unavailable",
		PendingQueryOK: false,
		PendingError:   "pending unavailable",
	}
	if report.TrustedQueryOK || report.PendingQueryOK {
		t.Fatalf("query ok flags unexpectedly true: %+v", report)
	}
	if report.TrustedError == "" || report.PendingError == "" {
		t.Fatalf("query errors not recorded: %+v", report)
	}
}

func TestDoctorLocalMembershipRequiresCurrentPilotNode(t *testing.T) {
	localID := mustGenerateIdentity(t)
	otherID := mustGenerateIdentity(t)
	cases := []struct {
		name      string
		rosterID  entmoot.NodeID
		rosterKey *keystore.Identity
		localNode entmoot.NodeID
		localKey  []byte
		wantOK    bool
		wantState string
	}{
		{
			name:      "current node and key match",
			rosterID:  7,
			rosterKey: localID,
			localNode: 7,
			localKey:  []byte(localID.PublicKey),
			wantOK:    true,
			wantState: doctorLocalMemberOK,
		},
		{
			name:      "same key under different pilot node is not local",
			rosterID:  8,
			rosterKey: localID,
			localNode: 7,
			localKey:  []byte(localID.PublicKey),
			wantState: doctorLocalMemberNotInRoster,
		},
		{
			name:      "current node with different key is mismatch",
			rosterID:  7,
			rosterKey: otherID,
			localNode: 7,
			localKey:  []byte(localID.PublicKey),
			wantState: doctorLocalMemberIdentityMismatch,
		},
		{
			name:      "missing current pilot node",
			rosterID:  7,
			rosterKey: localID,
			localNode: 0,
			localKey:  []byte(localID.PublicKey),
			wantState: doctorLocalMemberPilotUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newDoctorTestRoster(t, tc.rosterID, tc.rosterKey)
			gotOK, gotState := doctorLocalMembership(r, tc.localNode, tc.localKey)
			if gotOK != tc.wantOK || gotState != tc.wantState {
				t.Fatalf("doctorLocalMembership = (%t, %q), want (%t, %q)", gotOK, gotState, tc.wantOK, tc.wantState)
			}
		})
	}
}

func TestApplyDoctorProbesKeepsSelfPassiveDiagnosis(t *testing.T) {
	cases := []struct {
		name  string
		group doctorGroupReport
		peer  doctorPeerReport
		want  string
	}{
		{
			name:  "profile missing remains visible",
			group: doctorGroupReport{LocalMember: true, LocalMemberStatus: doctorLocalMemberOK},
			peer: doctorPeerReport{
				NodeID:    7,
				Roster:    true,
				Profile:   "missing",
				Transport: "ok",
				Trust:     "self",
				Route:     "not_checked",
				Diagnosis: "profile_missing",
			},
			want: "profile_missing",
		},
		{
			name:  "local membership mismatch wins",
			group: doctorGroupReport{LocalMember: false, LocalMemberStatus: doctorLocalMemberIdentityMismatch},
			peer: doctorPeerReport{
				NodeID:    7,
				Roster:    true,
				Profile:   "ok",
				Transport: "ok",
				Trust:     "self",
				Route:     "not_checked",
				Diagnosis: "ok",
			},
			want: doctorLocalMemberIdentityMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := &doctorReport{
				Entmoot: doctorEntmootReport{
					Running: true,
					NodeID:  7,
				},
				Groups: []doctorGroupReport{
					tc.group,
				},
			}
			report.Groups[0].Peers = []doctorPeerReport{tc.peer}

			applyDoctorProbes(report, t.TempDir(), time.Second)
			peer := report.Groups[0].Peers[0]
			if peer.Route != "self" {
				t.Fatalf("self route = %q, want self", peer.Route)
			}
			if peer.Diagnosis != tc.want {
				t.Fatalf("self diagnosis = %q, want %q", peer.Diagnosis, tc.want)
			}
		})
	}
}

func TestDoctorNextCommandPreservesGlobalPaths(t *testing.T) {
	gid := entmoot.GroupID{0x42}
	gf := &globalFlags{
		socket:   "/tmp/pilot custom.sock",
		identity: "/Users/jerryfane/.entmoot/agent's key.json",
		data:     "/Users/jerryfane/Entmoot Data",
	}

	got := doctorNextCommand(gf, gid)
	want := "entmootd -socket '/tmp/pilot custom.sock' -identity '/Users/jerryfane/.entmoot/agent'\"'\"'s key.json' -data '/Users/jerryfane/Entmoot Data' doctor -group QgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= --probe"
	if got != want {
		t.Fatalf("doctorNextCommand = %q, want %q", got, want)
	}
}

func newDoctorTestRoster(t *testing.T, nodeID entmoot.NodeID, id *keystore.Identity) *roster.RosterLog {
	t.Helper()
	info := entmoot.NodeInfo{
		PilotNodeID:   nodeID,
		EntmootPubKey: append([]byte(nil), id.PublicKey...),
	}
	r := roster.New(entmoot.GroupID{1})
	if err := r.Genesis(id, info, 1); err != nil {
		t.Fatalf("Genesis: %v", err)
	}
	return r
}

func mustGenerateIdentity(t *testing.T) *keystore.Identity {
	t.Helper()
	id, err := keystore.Generate()
	if err != nil {
		t.Fatalf("keystore.Generate: %v", err)
	}
	return id
}

func TestClassifyProbeError(t *testing.T) {
	if got := classifyProbeError("read: i/o timeout"); got != "route_timeout" {
		t.Fatalf("timeout classified as %q", got)
	}
	if got := classifyProbeError("wire: type 0xff: unknown message"); got != "probe_unsupported" {
		t.Fatalf("unknown classified as %q", got)
	}
}

func TestClampDiagProbeTimeout(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero uses default", in: 0, want: diagProbeDefaultTimeout},
		{name: "negative uses default", in: -time.Second, want: diagProbeDefaultTimeout},
		{name: "sub minimum clamps up", in: time.Nanosecond, want: diagProbeMinTimeout},
		{name: "normal preserved", in: 5 * time.Second, want: 5 * time.Second},
		{name: "over maximum clamps down", in: time.Hour, want: diagProbeMaxTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clampDiagProbeTimeout(tc.in); got != tc.want {
				t.Fatalf("clampDiagProbeTimeout(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestDiagProbeBudgetUsesConcurrencyBatches(t *testing.T) {
	timeout := time.Second
	cases := []struct {
		peers int
		want  time.Duration
	}{
		{peers: 0, want: timeout + diagProbeServerSlack},
		{peers: 1, want: timeout + diagProbeServerSlack},
		{peers: 4, want: timeout + diagProbeServerSlack},
		{peers: 5, want: 2*timeout + diagProbeServerSlack},
		{peers: 256, want: 64*timeout + diagProbeServerSlack},
	}
	for _, tc := range cases {
		if got := diagProbeBudget(timeout, tc.peers); got != tc.want {
			t.Fatalf("diagProbeBudget(%d peers) = %s, want %s", tc.peers, got, tc.want)
		}
	}
}

func TestChunkDiagProbePeers(t *testing.T) {
	peers := make([]entmoot.NodeID, diagProbeMaxPeersPerRequest+1)
	for i := range peers {
		peers[i] = entmoot.NodeID(i + 1)
	}
	chunks := chunkDiagProbePeers(peers)
	if len(chunks) != 2 {
		t.Fatalf("chunks len = %d, want 2", len(chunks))
	}
	if len(chunks[0]) != diagProbeMaxPeersPerRequest {
		t.Fatalf("first chunk len = %d, want %d", len(chunks[0]), diagProbeMaxPeersPerRequest)
	}
	if len(chunks[1]) != 1 || chunks[1][0] != entmoot.NodeID(diagProbeMaxPeersPerRequest+1) {
		t.Fatalf("second chunk = %v, want final peer", chunks[1])
	}
	if got := chunkDiagProbePeers(nil); got != nil {
		t.Fatalf("nil peers chunks = %v, want nil", got)
	}
}
