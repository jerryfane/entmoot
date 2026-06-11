package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/reconcile"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

type benchNode struct {
	id     *keystore.Identity
	info   entmoot.NodeInfo
	roster *roster.RosterLog
	store  *countingStore
	g      *gossip.Gossiper
}

type countingStore struct {
	inner store.MessageStore

	mu         sync.Mutex
	putCalls   int64
	uniquePuts int64
	duplicates int64
}

func newCountingStore() *countingStore {
	return &countingStore{inner: store.NewMemory()}
}

func (s *countingStore) Put(ctx context.Context, m entmoot.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	exists, err := s.inner.Has(ctx, m.GroupID, m.ID)
	if err != nil {
		return err
	}
	if err := s.inner.Put(ctx, m); err != nil {
		return err
	}
	s.putCalls++
	if exists {
		s.duplicates++
	} else {
		s.uniquePuts++
	}
	return nil
}

func (s *countingStore) Get(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	return s.inner.Get(ctx, groupID, id)
}

func (s *countingStore) Has(ctx context.Context, groupID entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return s.inner.Has(ctx, groupID, id)
}

func (s *countingStore) Range(ctx context.Context, groupID entmoot.GroupID, sinceMillis, untilMillis int64) ([]entmoot.Message, error) {
	return s.inner.Range(ctx, groupID, sinceMillis, untilMillis)
}

func (s *countingStore) Latest(ctx context.Context, groupID entmoot.GroupID, limit int) ([]entmoot.Message, error) {
	return s.inner.Latest(ctx, groupID, limit)
}

func (s *countingStore) LatestBefore(ctx context.Context, groupID entmoot.GroupID, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return s.inner.LatestBefore(ctx, groupID, limit, boundary)
}

func (s *countingStore) Topics(ctx context.Context, groupID entmoot.GroupID, limit int) ([]store.TopicSummary, error) {
	return s.inner.Topics(ctx, groupID, limit)
}

func (s *countingStore) LatestByTopic(ctx context.Context, groupID entmoot.GroupID, topic string, limit int) ([]entmoot.Message, error) {
	return s.inner.LatestByTopic(ctx, groupID, topic, limit)
}

func (s *countingStore) LatestByTopicBefore(ctx context.Context, groupID entmoot.GroupID, topic string, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return s.inner.LatestByTopicBefore(ctx, groupID, topic, limit, boundary)
}

func (s *countingStore) MerkleRoot(ctx context.Context, groupID entmoot.GroupID) ([32]byte, error) {
	return s.inner.MerkleRoot(ctx, groupID)
}

func (s *countingStore) IterMessageIDsInIDRange(ctx context.Context, groupID entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	return s.inner.IterMessageIDsInIDRange(ctx, groupID, loID, hiID)
}

func (s *countingStore) Close() error {
	return s.inner.Close()
}

func (s *countingStore) counts() (putCalls, uniquePuts, duplicates int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putCalls, s.uniquePuts, s.duplicates
}

type trustTransport struct {
	gossip.Transport
	trusted []entmoot.NodeID
}

func (t trustTransport) TrustedPeers(context.Context) ([]entmoot.NodeID, error) {
	return append([]entmoot.NodeID(nil), t.trusted...), nil
}

type broadcastResult struct {
	nodes              int
	trustDegree        int
	messages           int
	expectedDeliveries int
	delivered          int
	lost               int
	p50Millis          float64
	p90Millis          float64
	p95Millis          float64
	p99Millis          float64
	duplicatePuts      int64
	uniquePuts         int64
	duplicateRatio     float64
}

type rbsrResult struct {
	sharedIDs      int
	extraIDs       int
	rounds         int
	missingAtA     int
	missingAtB     int
	frameBytes     int
	bytesPerExtra  float64
	rangesSent     int
	idListEntries  int
	fingerprintMsg int
}

type joinResult struct {
	historyMessages int
	joinMillis      float64
	catchupMillis   float64
	totalMillis     float64
	finalMessages   int
	lost            int
}

func main() {
	outDir := flag.String("out", filepath.Join("..", "paper", "generated", "benchmarks"), "directory for TSV outputs")
	broadcastNodes := flag.String("broadcast-n", "50,100,300", "comma-separated broadcast network sizes")
	broadcastMessages := flag.Int("messages", 1, "messages per broadcast network size")
	joinHistories := flag.String("join-history", "0,100,1000", "comma-separated history lengths for join/catch-up")
	flag.Parse()

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	nodes, err := parseInts(*broadcastNodes)
	if err != nil {
		log.Fatal(err)
	}
	histories, err := parseInts(*joinHistories)
	if err != nil {
		log.Fatal(err)
	}

	var bResults []broadcastResult
	for _, n := range nodes {
		res, err := runBroadcast(context.Background(), n, *broadcastMessages)
		if err != nil {
			log.Fatalf("broadcast N=%d: %v", n, err)
		}
		bResults = append(bResults, res)
	}
	if err := writeBroadcast(filepath.Join(*outDir, "broadcast.tsv"), bResults); err != nil {
		log.Fatal(err)
	}

	var rResults []rbsrResult
	for _, extras := range []int{0, 1, 10, 100, 1000} {
		res, err := runRBSR(context.Background(), 1000, extras)
		if err != nil {
			log.Fatalf("rbsr extra=%d: %v", extras, err)
		}
		rResults = append(rResults, res)
	}
	if err := writeRBSR(filepath.Join(*outDir, "rbsr.tsv"), rResults); err != nil {
		log.Fatal(err)
	}

	var jResults []joinResult
	for _, h := range histories {
		res, err := runJoinCatchup(context.Background(), h)
		if err != nil {
			log.Fatalf("join history=%d: %v", h, err)
		}
		jResults = append(jResults, res)
	}
	if err := writeJoin(filepath.Join(*outDir, "join.tsv"), jResults); err != nil {
		log.Fatal(err)
	}
}

func parseInts(s string) ([]int, error) {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(p, "%d", &n); err != nil {
			return nil, fmt.Errorf("parse %q: %w", p, err)
		}
		out = append(out, n)
	}
	return out, nil
}

func runBroadcast(parent context.Context, n, messages int) (broadcastResult, error) {
	const trustDegree = 6
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	nodes, groupID, closeFn, err := newBroadcastNetwork(n, trustDegree)
	if err != nil {
		return broadcastResult{}, err
	}
	defer closeFn()
	startGossip(ctx, nodes)

	var samples []float64
	delivered := 0
	lost := 0
	for i := 0; i < messages; i++ {
		author := nodes[(i*17)%len(nodes)]
		msg, err := buildMessage(author, groupID, fmt.Sprintf("broadcast-%d", i), int64(2_000_000+i))
		if err != nil {
			return broadcastResult{}, err
		}
		t0 := time.Now()
		if err := author.g.Publish(ctx, msg); err != nil {
			return broadcastResult{}, err
		}
		seenAt := make(map[*benchNode]time.Time, len(nodes))
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) && len(seenAt) < len(nodes) {
			for _, node := range nodes {
				if _, ok := seenAt[node]; ok {
					continue
				}
				has, err := node.store.Has(ctx, groupID, msg.ID)
				if err != nil {
					return broadcastResult{}, err
				}
				if has {
					seenAt[node] = time.Now()
				}
			}
			if len(seenAt) < len(nodes) {
				time.Sleep(2 * time.Millisecond)
			}
		}
		for _, node := range nodes {
			t, ok := seenAt[node]
			if !ok {
				lost++
				continue
			}
			delivered++
			if node == author {
				continue
			}
			samples = append(samples, float64(t.Sub(t0).Microseconds())/1000)
		}
	}
	cancel()

	var unique, dup int64
	for _, node := range nodes {
		_, u, d := node.store.counts()
		unique += u
		dup += d
	}
	duplicateRatio := 0.0
	if unique > 0 {
		duplicateRatio = float64(dup) / float64(unique)
	}
	return broadcastResult{
		nodes:              n,
		trustDegree:        trustDegree,
		messages:           messages,
		expectedDeliveries: n * messages,
		delivered:          delivered,
		lost:               lost,
		p50Millis:          percentile(samples, 50),
		p90Millis:          percentile(samples, 90),
		p95Millis:          percentile(samples, 95),
		p99Millis:          percentile(samples, 99),
		duplicatePuts:      dup,
		uniquePuts:         unique,
		duplicateRatio:     duplicateRatio,
	}, nil
}

func newBroadcastNetwork(n, trustDegree int) ([]*benchNode, entmoot.GroupID, func(), error) {
	ids := make([]entmoot.NodeID, n)
	for i := range ids {
		ids[i] = entmoot.NodeID(10_000 + i)
	}
	transports := gossip.NewMemTransports(ids)
	groupID := groupIDFromSeed("broadcast")
	founderID, err := keystore.Generate()
	if err != nil {
		return nil, groupID, nil, err
	}
	founderInfo := entmoot.NodeInfo{PilotNodeID: ids[0], EntmootPubKey: []byte(founderID.PublicKey)}
	nodes := make([]*benchNode, n)
	for i, id := range ids {
		ident := founderID
		info := founderInfo
		if i != 0 {
			ident, err = keystore.Generate()
			if err != nil {
				return nil, groupID, nil, err
			}
			info = entmoot.NodeInfo{PilotNodeID: id, EntmootPubKey: []byte(ident.PublicKey)}
		}
		r := roster.New(groupID)
		if err := r.Genesis(founderID, founderInfo, 1_000); err != nil {
			return nil, groupID, nil, err
		}
		nodes[i] = &benchNode{id: ident, info: info, roster: r, store: newCountingStore()}
	}
	ts := int64(1_000)
	for i := 1; i < n; i++ {
		ts += 100
		for _, node := range nodes {
			entry, err := buildAddEntry(founderID, founderInfo, nodes[i].info, ts, node.roster.Head())
			if err != nil {
				return nil, groupID, nil, err
			}
			if err := node.roster.Apply(entry); err != nil {
				return nil, groupID, nil, err
			}
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeClock := clock.NewFake(time.UnixMilli(ts))
	for i, node := range nodes {
		tr := trustTransport{Transport: transports[node.info.PilotNodeID], trusted: ringTrusted(ids, i, trustDegree)}
		g, err := gossip.New(gossip.Config{
			LocalNode: node.info.PilotNodeID,
			Identity:  node.id,
			Roster:    node.roster,
			Store:     node.store,
			Transport: tr,
			GroupID:   groupID,
			Clock:     fakeClock,
			Logger:    logger,
		})
		if err != nil {
			return nil, groupID, nil, err
		}
		node.g = g
	}
	closeFn := func() {
		for _, tr := range transports {
			_ = tr.Close()
		}
	}
	return nodes, groupID, closeFn, nil
}

func ringTrusted(ids []entmoot.NodeID, idx, degree int) []entmoot.NodeID {
	if degree >= len(ids)-1 {
		out := make([]entmoot.NodeID, 0, len(ids)-1)
		for i, id := range ids {
			if i != idx {
				out = append(out, id)
			}
		}
		return out
	}
	radius := degree / 2
	seen := make(map[entmoot.NodeID]struct{}, degree)
	out := make([]entmoot.NodeID, 0, degree)
	for d := 1; d <= radius; d++ {
		for _, j := range []int{(idx + d) % len(ids), (idx - d + len(ids)) % len(ids)} {
			id := ids[j]
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out
}

func startGossip(ctx context.Context, nodes []*benchNode) {
	for _, node := range nodes {
		node := node
		go func() { _ = node.g.Start(ctx) }()
	}
	time.Sleep(100 * time.Millisecond)
}

func buildAddEntry(founder *keystore.Identity, founderInfo, subject entmoot.NodeInfo, ts int64, parent entmoot.RosterEntryID) (entmoot.RosterEntry, error) {
	entry := entmoot.RosterEntry{
		Op:        "add",
		Subject:   subject,
		Actor:     founderInfo.PilotNodeID,
		Timestamp: ts,
		Parents:   []entmoot.RosterEntryID{parent},
	}
	sigInput, err := canonical.Encode(entry)
	if err != nil {
		return entmoot.RosterEntry{}, err
	}
	entry.Signature = founder.Sign(sigInput)
	entry.ID = canonical.RosterEntryID(entry)
	return entry, nil
}

func buildMessage(author *benchNode, groupID entmoot.GroupID, content string, ts int64) (entmoot.Message, error) {
	msg := entmoot.Message{
		GroupID:   groupID,
		Author:    author.info,
		Timestamp: ts,
		Topics:    []string{"bench"},
		Content:   []byte(content),
	}
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return entmoot.Message{}, err
	}
	msg.Signature = author.id.Sign(sigInput)
	msg.ID = canonical.MessageID(msg)
	return msg, nil
}

func runRBSR(ctx context.Context, sharedCount, extraCount int) (rbsrResult, error) {
	a := newIDSetStorage()
	b := newIDSetStorage()
	for _, id := range deterministicIDs("shared", sharedCount) {
		a.add(id)
		b.add(id)
	}
	for _, id := range deterministicIDs("extra", extraCount) {
		b.add(id)
	}
	initiator, out, err := reconcile.NewInitiator(reconcile.DefaultConfig(), a)
	if err != nil {
		return rbsrResult{}, err
	}
	responder := reconcile.NewResponder(reconcile.DefaultConfig(), b)
	bytesSent := encodedLen(out)
	rangesSent, idListEntries, fingerprintMsgs := rangeStats(out)
	var missingA, missingB []entmoot.MessageID
	var aMissing []entmoot.MessageID
	rounds := 0
	aOut := out
	for rounds < 64 {
		rounds++
		bOut, bMissing, _, err := responder.Next(ctx, aOut)
		if err != nil {
			return rbsrResult{}, err
		}
		missingB = append(missingB, bMissing...)
		bytesSent += encodedLen(bOut)
		r, ids, fps := rangeStats(bOut)
		rangesSent += r
		idListEntries += ids
		fingerprintMsgs += fps

		aOut, aMissing, _, err = initiator.Next(ctx, bOut)
		if err != nil {
			return rbsrResult{}, err
		}
		missingA = append(missingA, aMissing...)
		bytesSent += encodedLen(aOut)
		r, ids, fps = rangeStats(aOut)
		rangesSent += r
		idListEntries += ids
		fingerprintMsgs += fps
		if initiator.Done() && responder.Done() {
			break
		}
	}
	perExtra := 0.0
	if extraCount > 0 {
		perExtra = float64(bytesSent) / float64(extraCount)
	}
	return rbsrResult{
		sharedIDs:      sharedCount,
		extraIDs:       extraCount,
		rounds:         rounds,
		missingAtA:     len(uniqueIDs(missingA)),
		missingAtB:     len(uniqueIDs(missingB)),
		frameBytes:     bytesSent,
		bytesPerExtra:  perExtra,
		rangesSent:     rangesSent,
		idListEntries:  idListEntries,
		fingerprintMsg: fingerprintMsgs,
	}, nil
}

func encodedLen(ranges []reconcile.Range) int {
	raw, _ := json.Marshal(ranges)
	return len(raw)
}

func rangeStats(ranges []reconcile.Range) (count, idListEntries, fingerprintMsgs int) {
	for _, r := range ranges {
		count++
		if r.Kind == reconcile.KindIDList {
			idListEntries += len(r.IDs)
		}
		if r.Kind == reconcile.KindFingerprint {
			fingerprintMsgs++
		}
	}
	return count, idListEntries, fingerprintMsgs
}

type idSetStorage struct {
	ids []entmoot.MessageID
}

func newIDSetStorage() *idSetStorage {
	return &idSetStorage{}
}

func (s *idSetStorage) add(id entmoot.MessageID) {
	s.ids = append(s.ids, id)
	sort.Slice(s.ids, func(i, j int) bool {
		return bytes.Compare(s.ids[i][:], s.ids[j][:]) < 0
	})
}

func (s *idSetStorage) IterIDs(_ context.Context, lo, hi entmoot.MessageID) ([]entmoot.MessageID, error) {
	out := make([]entmoot.MessageID, 0)
	for _, id := range s.ids {
		if inIDRange(id, lo, hi) {
			out = append(out, id)
		}
	}
	return out, nil
}

func (s *idSetStorage) CountInRange(ctx context.Context, lo, hi entmoot.MessageID) (int, error) {
	ids, err := s.IterIDs(ctx, lo, hi)
	return len(ids), err
}

func (s *idSetStorage) HasID(_ context.Context, id entmoot.MessageID) (bool, error) {
	i := sort.Search(len(s.ids), func(i int) bool {
		return bytes.Compare(s.ids[i][:], id[:]) >= 0
	})
	return i < len(s.ids) && s.ids[i] == id, nil
}

func inIDRange(id, lo, hi entmoot.MessageID) bool {
	if lo != (entmoot.MessageID{}) && bytes.Compare(id[:], lo[:]) < 0 {
		return false
	}
	if hi != (entmoot.MessageID{}) && bytes.Compare(id[:], hi[:]) >= 0 {
		return false
	}
	return true
}

func deterministicIDs(prefix string, n int) []entmoot.MessageID {
	out := make([]entmoot.MessageID, n)
	for i := range out {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s-%08d", prefix, i)))
		out[i] = entmoot.MessageID(h)
	}
	return out
}

func uniqueIDs(in []entmoot.MessageID) []entmoot.MessageID {
	seen := make(map[entmoot.MessageID]struct{}, len(in))
	out := make([]entmoot.MessageID, 0, len(in))
	for _, id := range in {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func runJoinCatchup(parent context.Context, historyMessages int) (joinResult, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	nodes, groupID, invite, closeFn, err := newJoinNetwork(historyMessages)
	if err != nil {
		return joinResult{}, err
	}
	defer closeFn()
	founder := nodes[0]
	joiner := nodes[1]
	startGossip(ctx, []*benchNode{founder})

	t0 := time.Now()
	joinCtx, joinCancel := context.WithTimeout(ctx, 10*time.Second)
	if err := joiner.g.Join(joinCtx, invite); err != nil {
		joinCancel()
		return joinResult{}, err
	}
	joinDone := time.Now()
	joinCancel()
	startGossip(ctx, []*benchNode{joiner})

	trigger, err := buildMessage(joiner, groupID, "join-catchup-trigger", 9_000_000)
	if err != nil {
		return joinResult{}, err
	}
	if err := joiner.g.Publish(ctx, trigger); err != nil {
		return joinResult{}, err
	}
	want := historyMessages + 1
	deadline := time.Now().Add(20 * time.Second)
	final := 0
	for time.Now().Before(deadline) {
		msgs, err := joiner.store.Range(ctx, groupID, 0, 0)
		if err != nil {
			return joinResult{}, err
		}
		final = len(msgs)
		if final >= want {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	done := time.Now()
	lost := want - final
	if lost < 0 {
		lost = 0
	}
	return joinResult{
		historyMessages: historyMessages,
		joinMillis:      float64(joinDone.Sub(t0).Microseconds()) / 1000,
		catchupMillis:   float64(done.Sub(joinDone).Microseconds()) / 1000,
		totalMillis:     float64(done.Sub(t0).Microseconds()) / 1000,
		finalMessages:   final,
		lost:            lost,
	}, nil
}

func newJoinNetwork(historyMessages int) ([]*benchNode, entmoot.GroupID, *entmoot.Invite, func(), error) {
	ids := []entmoot.NodeID{42_001, 42_002}
	transports := gossip.NewMemTransports(ids)
	groupID := groupIDFromSeed(fmt.Sprintf("join-%d", historyMessages))
	founderID, err := keystore.Generate()
	if err != nil {
		return nil, groupID, nil, nil, err
	}
	joinerID, err := keystore.Generate()
	if err != nil {
		return nil, groupID, nil, nil, err
	}
	founderInfo := entmoot.NodeInfo{PilotNodeID: ids[0], EntmootPubKey: []byte(founderID.PublicKey)}
	joinerInfo := entmoot.NodeInfo{PilotNodeID: ids[1], EntmootPubKey: []byte(joinerID.PublicKey)}

	founderRoster := roster.New(groupID)
	if err := founderRoster.Genesis(founderID, founderInfo, 1_000); err != nil {
		return nil, groupID, nil, nil, err
	}
	addJoiner, err := buildAddEntry(founderID, founderInfo, joinerInfo, 1_100, founderRoster.Head())
	if err != nil {
		return nil, groupID, nil, nil, err
	}
	if err := founderRoster.Apply(addJoiner); err != nil {
		return nil, groupID, nil, nil, err
	}

	founder := &benchNode{id: founderID, info: founderInfo, roster: founderRoster, store: newCountingStore()}
	joiner := &benchNode{id: joinerID, info: joinerInfo, roster: roster.New(groupID), store: newCountingStore()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeClock := clock.NewFake(time.UnixMilli(1_100))
	for _, cfg := range []struct {
		node    *benchNode
		trusted []entmoot.NodeID
	}{
		{founder, []entmoot.NodeID{joiner.info.PilotNodeID}},
		{joiner, []entmoot.NodeID{founder.info.PilotNodeID}},
	} {
		g, err := gossip.New(gossip.Config{
			LocalNode: cfg.node.info.PilotNodeID,
			Identity:  cfg.node.id,
			Roster:    cfg.node.roster,
			Store:     cfg.node.store,
			Transport: trustTransport{Transport: transports[cfg.node.info.PilotNodeID], trusted: cfg.trusted},
			GroupID:   groupID,
			Clock:     fakeClock,
			Logger:    logger,
		})
		if err != nil {
			return nil, groupID, nil, nil, err
		}
		cfg.node.g = g
	}
	for i := 0; i < historyMessages; i++ {
		msg, err := buildMessage(founder, groupID, fmt.Sprintf("history-%d", i), int64(2_000_000+i))
		if err != nil {
			return nil, groupID, nil, nil, err
		}
		if err := founder.store.Put(context.Background(), msg); err != nil {
			return nil, groupID, nil, nil, err
		}
	}
	invite, err := buildInvite(founderID, founderInfo, groupID, founderRoster.Head(), []entmoot.NodeID{founderInfo.PilotNodeID}, 1_100, 1_100+24*60*60*1000)
	if err != nil {
		return nil, groupID, nil, nil, err
	}
	closeFn := func() {
		for _, tr := range transports {
			_ = tr.Close()
		}
	}
	return []*benchNode{founder, joiner}, groupID, invite, closeFn, nil
}

func buildInvite(founder *keystore.Identity, founderInfo entmoot.NodeInfo, groupID entmoot.GroupID, head entmoot.RosterEntryID, bootstrap []entmoot.NodeID, issuedAt, validUntil int64) (*entmoot.Invite, error) {
	bps := make([]entmoot.BootstrapPeer, 0, len(bootstrap))
	for _, n := range bootstrap {
		bps = append(bps, entmoot.BootstrapPeer{NodeID: n})
	}
	inv := &entmoot.Invite{
		GroupID:        groupID,
		Founder:        founderInfo,
		Issuer:         founderInfo,
		RosterHead:     head,
		BootstrapPeers: bps,
		IssuedAt:       issuedAt,
		ValidUntil:     validUntil,
	}
	signing := *inv
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return nil, err
	}
	inv.Signature = founder.Sign(sigInput)
	return inv, nil
}

func groupIDFromSeed(seed string) entmoot.GroupID {
	return entmoot.GroupID(sha256.Sum256([]byte(seed)))
}

func percentile(samples []float64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)
	rank := (p / 100) * float64(len(cp)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return cp[lo]
	}
	frac := rank - float64(lo)
	return cp[lo]*(1-frac) + cp[hi]*frac
}

func writeBroadcast(path string, rows []broadcastResult) error {
	var b strings.Builder
	b.WriteString("nodes\ttrust_degree\tmessages\texpected_deliveries\tdelivered\tlost\tloss_rate\tp50_ms\tp90_ms\tp95_ms\tp99_ms\tunique_puts\tduplicate_puts\tduplicate_ratio\n")
	for _, r := range rows {
		lossRate := 0.0
		if r.expectedDeliveries > 0 {
			lossRate = float64(r.lost) / float64(r.expectedDeliveries)
		}
		fmt.Fprintf(&b, "%d\t%d\t%d\t%d\t%d\t%d\t%.6f\t%.3f\t%.3f\t%.3f\t%.3f\t%d\t%d\t%.6f\n",
			r.nodes, r.trustDegree, r.messages, r.expectedDeliveries, r.delivered, r.lost, lossRate,
			r.p50Millis, r.p90Millis, r.p95Millis, r.p99Millis, r.uniquePuts, r.duplicatePuts, r.duplicateRatio)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeRBSR(path string, rows []rbsrResult) error {
	var b strings.Builder
	b.WriteString("shared_ids\textra_ids\trounds\tmissing_at_a\tmissing_at_b\tframe_bytes\tbytes_per_extra\tranges_sent\tid_list_entries\tfingerprint_ranges\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "%d\t%d\t%d\t%d\t%d\t%d\t%.3f\t%d\t%d\t%d\n",
			r.sharedIDs, r.extraIDs, r.rounds, r.missingAtA, r.missingAtB, r.frameBytes,
			r.bytesPerExtra, r.rangesSent, r.idListEntries, r.fingerprintMsg)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeJoin(path string, rows []joinResult) error {
	var b strings.Builder
	b.WriteString("history_messages\tjoin_ms\tcatchup_ms\ttotal_ms\tfinal_messages\tlost\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "%d\t%.3f\t%.3f\t%.3f\t%d\t%d\n",
			r.historyMessages, r.joinMillis, r.catchupMillis, r.totalMillis, r.finalMessages, r.lost)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
