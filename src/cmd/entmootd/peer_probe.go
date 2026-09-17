package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

const (
	// defaultProbeBudget bounds a whole probe, not one peer. An operator
	// running doctor waits for the total, so a thirty-member group must not
	// cost thirty timeouts.
	defaultProbeBudget = 5 * time.Second
	maxProbeBudget     = 60 * time.Second
	// minProbeSlice is the least time worth giving one peer: below this a
	// healthy peer on a slow path would be reported unreachable for
	// arithmetic reasons. It is a floor on all three of the budget, the
	// per-peer slice and the remaining-time clamp, so no caller can ask for
	// an answer cheaper than one honest attempt.
	// TestProbeGivesEachPeerTheFloor pins it against a listener that accepts
	// and never writes, where the elapsed time is the deadline handed out.
	minProbeSlice = 500 * time.Millisecond
	// maxProbeParallel bounds concurrent dials so a large group does not open
	// a connection per member at once.
	maxProbeParallel = 8
	// maxProbeErrorBytes bounds one peer's error in the report.
	maxProbeErrorBytes = 200
	// maxProbeRefusalBytes bounds the peer's OWN text. A refusal is a short
	// code - "unauthorized", "not_member" - but the field is remote-controlled
	// and the response frame allows megabytes, so without a cap a member
	// decides how large this daemon's answer is, and a few of them push it
	// past the control socket's frame limit and destroy the whole probe.
	maxProbeRefusalBytes = 64
)

// probePeers dials every other member of a group and reports which of them
// answer a membership read for it.
//
// A read, not a connection: a reachable address proves only that something
// listens there, while an answered membership request proves the peer is up,
// speaks this version of the protocol, and serves this group to this node.
// That is the question an operator is asking when a group will not converge.
//
// Nothing here changes local state. The response is measured and discarded,
// so a probe cannot be a back door for applying a peer's records.
func (r *groupRuntime) probePeers(ctx context.Context, groupID entmoot.GroupID, budget time.Duration) ([]ipc.PeerProbeResult, bool, error) {
	session, ok := r.Get(groupID)
	if !ok {
		return nil, false, errors.New("group not joined")
	}
	if budget <= 0 {
		budget = defaultProbeBudget
	}
	if budget < minProbeSlice {
		// The floor binds the budget, not only the slices carved out of it.
		// Clamping the slice alone left the incoming budget free to defeat
		// it: `doctor -probe-timeout 1ms` (join.go passes the operator's
		// value straight through) set a deadline that was already spent by
		// the time the workers ran, so every member was refused with
		// arithmetic - "not attempted: probe budget spent" - without one
		// dial being made. One peer's worth of time is the least a probe can
		// honestly cost, so a smaller request buys a slower answer, not a
		// false one.
		budget = minProbeSlice
	}
	if budget > maxProbeBudget {
		budget = maxProbeBudget
	}

	targets := r.probeTargets(session)
	results := make([]ipc.PeerProbeResult, 0, len(targets))
	if len(targets) == 0 {
		return results, false, nil
	}

	deadline := time.Now().Add(budget)
	slice := budget / time.Duration(len(targets))
	if slice < minProbeSlice {
		slice = minProbeSlice
	}

	var (
		mu         sync.Mutex
		incomplete bool
		wg         sync.WaitGroup
	)
	gate := make(chan struct{}, maxProbeParallel)
	out := make([]ipc.PeerProbeResult, len(targets))
	for i := range targets {
		target := targets[i]
		index := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate <- struct{}{}
			defer func() { <-gate }()

			remaining := time.Until(deadline)
			if remaining <= 0 {
				mu.Lock()
				incomplete = true
				mu.Unlock()
				out[index] = ipc.PeerProbeResult{
					MemberID:  target.memberID,
					PeerID:    target.info.ID.String(),
					Addresses: len(target.info.Addrs),
					Error:     "not attempted: probe budget spent",
				}
				return
			}
			attempt := slice
			if attempt > remaining {
				// Never below the floor: a peer given a sub-millisecond
				// deadline is reported unreachable for arithmetic reasons,
				// which is a wrong answer rather than a slow one. Going over
				// the budget by one slice is the lesser fault, and the
				// remaining peers are reported as not attempted.
				attempt = remaining
				if attempt < minProbeSlice {
					attempt = minProbeSlice
				}
			}
			out[index] = r.probeOne(ctx, session, target, attempt)
		}()
	}
	wg.Wait()
	results = append(results, out...)
	return results, incomplete, nil
}

type probeTarget struct {
	memberID entmoot.MemberID
	info     peer.AddrInfo
}

// probeTargets lists every member except this node, with whatever addresses
// this host knows. A member with no known address is still listed, with zero
// addresses: "we have never learned where it is" is a different answer from
// "it did not reply", and an operator needs to tell them apart.
//
// This node is not a target. Probing yourself answers nothing and would pad
// the reachable count an operator reads.
func (r *groupRuntime) probeTargets(session *groupSession) []probeTarget {
	cached, _ := loadGroupPeers(r.dataDir, session.groupID)
	addrsFor := func(id peer.ID) []multiaddr.Multiaddr {
		if addrs := r.host.Peerstore().Addrs(id); len(addrs) > 0 {
			return addrs
		}
		for _, candidate := range cached {
			if candidate.ID == id {
				return candidate.Addrs
			}
		}
		return nil
	}
	memberIDs := session.group.MemberIDs()
	sort.Slice(memberIDs, func(i, j int) bool { return memberIDs[i].String() < memberIDs[j].String() })

	targets := make([]probeTarget, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		info, found := session.group.MemberInfoByID(memberID)
		if !found {
			continue
		}
		binding, err := libp2ptransport.BindingFromPublicKey(info.EntmootPubKey)
		if err != nil {
			targets = append(targets, probeTarget{memberID: memberID})
			continue
		}
		if binding.PeerID == r.host.ID() {
			continue
		}
		targets = append(targets, probeTarget{
			memberID: memberID,
			info:     peer.AddrInfo{ID: binding.PeerID, Addrs: addrsFor(binding.PeerID)},
		})
	}
	return targets
}

func (r *groupRuntime) probeOne(ctx context.Context, session *groupSession, target probeTarget, budget time.Duration) ipc.PeerProbeResult {
	result := ipc.PeerProbeResult{
		MemberID:  target.memberID,
		PeerID:    target.info.ID.String(),
		Addresses: len(target.info.Addrs),
	}
	if target.info.ID == "" {
		result.Error = "member key does not yield a peer id"
		return result
	}
	if len(target.info.Addrs) == 0 && len(r.host.Peerstore().Addrs(target.info.ID)) == 0 {
		result.Error = "no address known for this member"
		return result
	}

	probeCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	canonical := session.group.Canonical()
	request := libp2ptransport.MembershipSyncRequest{
		Version:        1,
		RequestID:      probeRequestID(),
		GroupID:        session.groupID,
		HaveSequence:   canonical.Sequence,
		HaveCheckpoint: canonical.ID,
	}
	started := time.Now()
	// The response body is discarded on purpose: a probe reports reachability
	// and must not be a second path for adopting a peer's records. Only its
	// error code is read, to tell a refusal from silence.
	response, err := libp2ptransport.RequestMembership(probeCtx, r.host, target.info, request)
	result.LatencyMS = time.Since(started).Milliseconds()
	if err != nil {
		if response.Error != "" {
			// The peer replied. It served the stream and refused us, which is
			// an answer about membership, not about the network.
			result.Answered = true
			result.Refusal = boundRefusal(string(response.Error))
			result.Relayed = connectionIsRelayed(r, target.info.ID)
			return result
		}
		result.Error = summarizeProbeError(err, len(target.info.Addrs))
		return result
	}
	result.Reachable = true
	result.Answered = true
	result.Relayed = connectionIsRelayed(r, target.info.ID)
	return result
}

// connectionIsRelayed reports whether every live connection to the peer runs
// through a circuit. It is worth surfacing: a relayed path explains latency an
// operator would otherwise read as a fault, and it is what hide-IP expects.
func connectionIsRelayed(r *groupRuntime, id peer.ID) bool {
	conns := r.host.Network().ConnsToPeer(id)
	if len(conns) == 0 {
		return false
	}
	for _, conn := range conns {
		if !multiaddrIsCircuit(conn.RemoteMultiaddr()) {
			return false
		}
	}
	return true
}

func multiaddrIsCircuit(addr multiaddr.Multiaddr) bool {
	if addr == nil {
		return false
	}
	found := false
	multiaddr.ForEach(addr, func(component multiaddr.Component) bool {
		if component.Protocol().Code == multiaddr.P_CIRCUIT {
			found = true
			return false
		}
		return true
	})
	return found
}

func probeRequestID() string {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "probe"
	}
	return "probe-" + base64.RawURLEncoding.EncodeToString(raw[:])
}

// probePeersOverIPC asks the running daemon to probe, because only the daemon
// holds the libp2p host. A second process dialling with a fresh identity
// would answer a different question.
func probePeersOverIPC(ctx context.Context, sockPath string, req *ipc.PeerProbeReq, budget time.Duration) (*ipc.PeerProbeResp, error) {
	conn, err := net.DialTimeout("unix", sockPath, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(budget + 10*time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.After(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.PeerProbeResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, errors.New(v.Message)
	default:
		return nil, fmt.Errorf("unexpected probe reply %T", payload)
	}
}

// applyPeerProbe fills each peer row with the daemon's probe result. A row
// without a Probe means the probe never reached that member, which the group's
// ProbeStatus explains; the rows are never silently left looking healthy.
func applyPeerProbe(ctx context.Context, group *doctorGroupReport, sockPath string, running bool, budget time.Duration) {
	if !running {
		group.ProbeStatus = "runtime_unavailable: a probe needs the running daemon, which owns the libp2p host"
		return
	}
	if budget <= 0 {
		budget = defaultProbeBudget
	}
	resp, err := probePeersOverIPC(ctx, sockPath, &ipc.PeerProbeReq{
		GroupID:  group.GroupID,
		BudgetMS: budget.Milliseconds(),
	}, budget)
	if err != nil {
		group.ProbeStatus = "failed: " + err.Error()
		return
	}
	byMember := make(map[entmoot.MemberID]ipc.PeerProbeResult, len(resp.Peers))
	for _, result := range resp.Peers {
		byMember[result.MemberID] = result
	}
	for i := range group.Peers {
		result, ok := byMember[group.Peers[i].MemberID]
		if !ok {
			continue
		}
		group.Peers[i].Probe = &doctorPeerProbe{
			Reachable: result.Reachable,
			Answered:  result.Answered,
			Refusal:   result.Refusal,
			Relayed:   result.Relayed,
			LatencyMS: result.LatencyMS,
			Addresses: result.Addresses,
			Error:     result.Error,
		}
	}
	group.ProbeStatus = "ok"
	if resp.Incomplete {
		group.ProbeStatus = "incomplete: the probe budget was spent before every member was tried"
	}
}

// summarizeProbeError keeps a probe answer readable. A failed dial to a
// multi-homed member carries one line per address - thirty of them on a host
// with many interfaces - and that whole wall lands in JSON an operator greps
// and in a report they paste. The first line names the failure; the count
// says how much was tried.
func summarizeProbeError(err error, addresses int) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if idx := strings.IndexByte(text, '\n'); idx >= 0 {
		text = strings.TrimSpace(text[:idx])
	}
	if len(text) > maxProbeErrorBytes {
		text = text[:maxProbeErrorBytes] + "..."
	}
	if addresses > 1 {
		return fmt.Sprintf("%s (%d addresses tried)", text, addresses)
	}
	return text
}

// boundRefusal trims a peer's refusal text to something this daemon is willing
// to put in its own answer. Whitespace goes too: a code with a newline in it
// would break the one-line-per-peer shape an operator reads.
func boundRefusal(text string) string {
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > maxProbeRefusalBytes {
		return text[:maxProbeRefusalBytes] + "..."
	}
	return text
}
