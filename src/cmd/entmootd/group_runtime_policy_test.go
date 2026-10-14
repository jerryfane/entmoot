package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

// Unlike the command's acknowledgement-only fixture, this runs the actual IPC
// signer and live session. A successful report must already be enforceable.
func TestGroupRuntimeFounderPolicyCLISetAndClear(t *testing.T) {
	ctx := withTestContext(t)
	root, err := os.MkdirTemp("/tmp", "entmoot-policy-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	founder, _ := mustDaemonIdentity(t)
	gid := daemonTestGroupID(0xb1)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	node := startPolicyRuntimeNode(t, ctx, root, founder, gid)
	listener, err := net.Listen("unix", controlSocketPath(root))
	if err != nil {
		t.Fatal(err)
	}
	server := &ipcServer{
		memberID: node.runtime.binding.MemberID, peerID: node.host.ID().String(),
		identity: founder, dataDir: root, runtime: node.runtime,
		store: node.runtime.store, notify: node.runtime.notify,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.acceptLoop(ctx, listener)
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })

	p := restrictiveRuntimePolicy()
	policyPath := root + "/restrictive.json"
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(policyPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	gf := &globalFlags{data: root}
	var sequence uint64
	for _, operation := range []string{"set", "clear"} {
		args := []string{operation, "-group", gid.String(), "--json"}
		if operation == "set" {
			args = append(args, "-file", policyPath)
		}
		code, stdout, stderr := captureCommandOutput(t, func() int { return cmdGroupPolicy(gf, args) })
		if code != exitOK || stderr != "" {
			t.Fatalf("%s code/stderr = %d/%q", operation, code, stderr)
		}
		var report groupPolicyReport
		if err := json.Unmarshal([]byte(stdout), &report); err != nil {
			t.Fatalf("%s report: %v: %s", operation, err, stdout)
		}
		if !report.Published || !report.RuntimeAppliedKnown || report.RuntimeApplied == nil || !*report.RuntimeApplied || report.Sequence <= sequence {
			t.Fatalf("%s report does not confirm a newer applied policy: %+v", operation, report)
		}
		sequence = report.Sequence
		var expected *entpolicy.Policy
		if operation == "set" {
			expected = &p
		}
		assertRuntimePolicy(t, ctx, node, sequence, expected)
		messages, err := node.runtime.store.Range(ctx, gid, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, message := range messages {
			update, err := entpolicy.ParseUpdate(message.Content)
			if err == nil && update.Sequence == sequence {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s returned success without a durable policy message", operation)
		}
		assertRuntimeContentPolicy(t, ctx, node, operation == "set", operation)
		if operation == "set" {
			// Exhaust the ordinary author's burst as well as imposing a size
			// limit smaller than the clear payload. Neither may trap the founder.
			first := node.message(t, ctx, []string{"chat"}, []byte("a"))
			if _, err := node.session.live.Publish(ctx, first); err != nil {
				t.Fatalf("first ordinary message: %v", err)
			}
			second := node.message(t, ctx, []string{"chat"}, []byte("b"))
			if _, err := node.session.live.Publish(ctx, second); err == nil {
				t.Fatal("ordinary author burst was not enforced")
			}
		}
	}
}

func TestGroupRuntimeFounderPolicyLivePeer(t *testing.T) {
	ctx := withTestContext(t)
	founder, member := policyRuntimePair(t, ctx, daemonTestGroupID(0xb2))
	// Hold both catch-up gates before connecting: this test must receive via
	// live gossip, not be rescued by startup or periodic history sync.
	founder.session.catchup.Lock()
	defer founder.session.catchup.Unlock()
	member.session.catchup.Lock()
	defer member.session.catchup.Unlock()
	connectBothWays(t, ctx, founder.host, member.host)
	waitPolicyLiveLink(t, ctx, founder, member)
	p := restrictiveRuntimePolicy()
	for i, policy := range []*entpolicy.Policy{&p, nil} {
		sequence := uint64(i + 1)
		message := founder.policyMessage(t, ctx, entpolicy.NewUpdate(founder.session.groupID, policy, time.Now().UnixMilli(), sequence))
		if _, err := founder.session.live.Publish(ctx, message); err != nil {
			t.Fatalf("publish policy sequence %d: %v", sequence, err)
		}
		waitPolicyMessage(t, ctx, member, message, false)
		assertRuntimePolicy(t, ctx, member, sequence, policy)
		assertRuntimeContentPolicy(t, ctx, member, policy != nil, fmt.Sprint(sequence))
	}
	stale := founder.policyMessage(t, ctx, entpolicy.NewUpdate(founder.session.groupID, &p, time.Now().UnixMilli()+1, 1))
	if _, err := founder.session.live.Publish(ctx, stale); err != nil {
		t.Fatalf("publish stale signed history: %v", err)
	}
	assertRuntimePolicy(t, ctx, founder, 2, nil)
	waitPolicyMessage(t, ctx, member, stale, false)
	assertRuntimePolicy(t, ctx, member, 2, nil)
}

func TestGroupRuntimeFounderPolicyOfflineHistory(t *testing.T) {
	ctx := withTestContext(t)
	founder, member := policyRuntimePair(t, ctx, daemonTestGroupID(0xb3))
	p := restrictiveRuntimePolicy()
	// Seed the keeper's durable history without broadcasting, as if it had
	// accepted these publications before the receiver connected.
	for i, policy := range []*entpolicy.Policy{&p, nil, &p} {
		sequence := uint64(i + 1)
		if i == 2 {
			sequence = 1 // newer timestamp, older sequence must not roll back clear
		}
		message := founder.policyMessage(t, ctx, entpolicy.NewUpdate(founder.session.groupID, policy, time.Now().UnixMilli()+int64(i), sequence))
		if _, err := founder.runtime.store.Put(ctx, founder.session.groupID, message); err != nil {
			t.Fatalf("store offline policy: %v", err)
		}
		if i == 0 {
			connectBothWays(t, ctx, founder.host, member.host)
		}
		waitPolicyMessage(t, ctx, member, message, true)
		if i == 0 {
			assertRuntimePolicy(t, ctx, member, 1, &p)
			assertRuntimeContentPolicy(t, ctx, member, true, "history-set")
		} else {
			assertRuntimePolicy(t, ctx, member, 2, nil)
			assertRuntimeContentPolicy(t, ctx, member, false, fmt.Sprintf("history-%d", i))
		}
	}
}

func TestGroupRuntimePolicyHistorySkipsInvalidUpdates(t *testing.T) {
	ctx := withTestContext(t)
	founder, member := policyRuntimePair(t, ctx, daemonTestGroupID(0xb6))
	gid := founder.session.groupID
	p := entpolicy.Standard()
	invalid := []entmoot.Message{
		member.policyMessage(t, ctx, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 100)),
		founder.message(t, ctx, []string{entpolicy.UpdateTopic}, []byte("{")),
		founder.policyMessage(t, ctx, entpolicy.NewUpdate(daemonTestGroupID(0xb7), &p, time.Now().UnixMilli(), 101)),
	}
	// Older daemons accepted these signed messages as ordinary content.
	for _, message := range invalid {
		if _, err := founder.runtime.store.Put(ctx, gid, message); err != nil {
			t.Fatal(err)
		}
	}
	valid := founder.policyMessage(t, ctx, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 1))
	if _, err := founder.runtime.store.Put(ctx, gid, valid); err != nil {
		t.Fatal(err)
	}
	chat := founder.message(t, ctx, []string{"chat"}, []byte("history after invalid policy"))
	if _, err := founder.runtime.store.Put(ctx, gid, chat); err != nil {
		t.Fatal(err)
	}
	connectBothWays(t, ctx, founder.host, member.host)
	waitPolicyMessage(t, ctx, member, valid, true)
	if held, err := member.runtime.store.Has(ctx, gid, chat.ID); err != nil || !held {
		t.Fatalf("ordinary history missing after invalid updates: held=%v err=%v", held, err)
	}
	assertRuntimePolicy(t, ctx, member, 1, &p)
	for _, message := range invalid {
		held, err := member.runtime.store.Has(ctx, gid, message.ID)
		if err != nil || held {
			t.Fatalf("invalid history stored=%v err=%v", held, err)
		}
	}
}

func TestGroupRuntimePolicyRejectsUnauthorizedAndInvalidUpdates(t *testing.T) {
	ctx := withTestContext(t)
	founder, member := policyRuntimePair(t, ctx, daemonTestGroupID(0xb4))
	gid := founder.session.groupID
	p := restrictiveRuntimePolicy()
	invalid := p
	invalid.MaxMessageBytes = -1
	cases := []struct {
		name   string
		author *policyRuntimeNode
		update entpolicy.Update
	}{
		{"nonfounder", member, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 100)},
		{"wrong-payload-group", founder, entpolicy.NewUpdate(daemonTestGroupID(0xf4), &p, time.Now().UnixMilli(), 100)},
		{"invalid-policy", founder, entpolicy.NewUpdate(gid, &invalid, time.Now().UnixMilli(), 100)},
		{"malformed-json", founder, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 100)},
		{"invalid-signature", founder, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			message := tc.author.policyMessage(t, ctx, tc.update)
			switch tc.name {
			case "malformed-json":
				message = tc.author.message(t, ctx, []string{entpolicy.UpdateTopic}, []byte("{"))
			case "invalid-signature":
				message.Signature[0] ^= 1
			}
			if _, err := tc.author.session.live.Publish(ctx, message); err == nil {
				t.Fatal("invalid policy publish succeeded")
			}
			// Also exercise the receiving node's authenticated ingestion path;
			// this must check founder authority independently of the sender.
			if _, err := founder.session.live.StoreForHistory(ctx, message); err == nil {
				t.Fatal("invalid policy accepted from another member")
			}
			for _, node := range []*policyRuntimeNode{founder, member} {
				if held, err := node.runtime.store.Has(ctx, gid, message.ID); err != nil || held {
					t.Fatalf("rejected policy stored: held=%v err=%v", held, err)
				}
				assertRuntimePolicy(t, ctx, node, 0, nil)
			}
		})
	}
	valid := founder.policyMessage(t, ctx, entpolicy.NewUpdate(gid, &p, time.Now().UnixMilli(), 1))
	if _, err := founder.session.live.Publish(ctx, valid); err != nil {
		t.Fatal(err)
	}
	assertRuntimePolicy(t, ctx, founder, 1, &p)
}

func TestGroupRuntimeFounderPolicyRestartReplay(t *testing.T) {
	ctx := withTestContext(t)
	root := t.TempDir()
	gid := daemonTestGroupID(0xb5)
	founder, founderInfo := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	mustCreateGroup(t, root, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, root, gid)
	mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	head := group.Canonical().ID
	mustCloseGroup(t, group)
	p := restrictiveRuntimePolicy()
	invalid := p
	invalid.MessageRatePerAuthor = "not-a-rate"
	// Reproduce an old daemon's durable log with no derived policy state.
	// Timestamp order deliberately disagrees with policy sequence order.
	messages, err := store.OpenSQLite(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = messages.Close() })
	now := time.Now().UnixMilli()
	for i, item := range []struct {
		identity *keystore.Identity
		author   entmoot.NodeInfo
		update   entpolicy.Update
	}{
		{founder, founderInfo, entpolicy.NewUpdate(gid, &p, now, 2)},
		{founder, founderInfo, entpolicy.NewUpdate(gid, nil, now+1, 1)},
		{member, memberInfo, entpolicy.NewUpdate(gid, nil, now+2, 100)},
		{founder, founderInfo, entpolicy.NewUpdate(daemonTestGroupID(0xf5), nil, now+3, 101)},
		{founder, founderInfo, entpolicy.NewUpdate(gid, &invalid, now+4, 102)},
		{founder, founderInfo, entpolicy.NewUpdate(gid, nil, now+5, 103)},
	} {
		raw, err := json.Marshal(item.update)
		if err != nil {
			t.Fatal(err)
		}
		message := entmoot.Message{
			Version: 2, GroupID: gid, Author: item.author, Timestamp: now + int64(i),
			RosterHead: &head, Topics: []string{entpolicy.UpdateTopic}, Content: raw,
		}
		signer, err := signing.NewLocalSigner(item.author, item.identity)
		if err != nil {
			t.Fatal(err)
		}
		message, err = signing.SignMessage(ctx, signer, message)
		if err != nil {
			t.Fatal(err)
		}
		if item.update.Sequence == 103 {
			message.Signature[0] ^= 1 // a forged high sequence must not poison replay
		}
		if _, err := messages.Put(ctx, gid, message); err != nil {
			t.Fatal(err)
		}
	}
	// Push the authoritative update beyond the first history page. These
	// newer messages carry an older policy sequence and must not win.
	signer, err := signing.NewLocalSigner(founderInfo, founder)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 130; i++ {
		raw, err := json.Marshal(entpolicy.NewUpdate(gid, nil, now+int64(i)+10, 1))
		if err != nil {
			t.Fatal(err)
		}
		message, err := signing.SignMessage(ctx, signer, entmoot.Message{
			Version: 2, GroupID: gid, Author: founderInfo, Timestamp: now + int64(i) + 10,
			RosterHead: &head, Topics: []string{entpolicy.UpdateTopic}, Content: raw,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := messages.Put(ctx, gid, message); err != nil {
			t.Fatal(err)
		}
	}
	if err := messages.Close(); err != nil {
		t.Fatal(err)
	}
	node := startPolicyRuntimeNode(t, ctx, root, founder, gid)
	assertRuntimePolicy(t, ctx, node, 2, &p)
	assertRuntimeContentPolicy(t, ctx, node, true, "replayed-set")
	clear := node.policyMessage(t, ctx, entpolicy.NewUpdate(gid, nil, now+5, 3))
	if _, err := node.session.live.Publish(ctx, clear); err != nil {
		t.Fatalf("clear replayed restriction: %v", err)
	}
	node.runtime.Close()
	if err := node.host.Close(); err != nil {
		t.Fatal(err)
	}
	// Discard the derived cache so the second start has to replay the clear,
	// rather than merely loading the state written before shutdown.
	if err := os.Remove(node.runtime.policyStore.Path()); err != nil {
		t.Fatal(err)
	}
	restarted := startPolicyRuntimeNode(t, ctx, root, founder, gid)
	assertRuntimePolicy(t, ctx, restarted, 3, nil)
	assertRuntimeContentPolicy(t, ctx, restarted, false, "replayed-clear")
}

type policyRuntimeNode struct {
	runtime  *groupRuntime
	session  *groupSession
	host     host.Host
	identity *keystore.Identity
}

func startPolicyRuntimeNode(t *testing.T, ctx context.Context, root string, identity *keystore.Identity, gid entmoot.GroupID) *policyRuntimeNode {
	t.Helper()
	runtime, session, h := startTestRuntime(t, ctx, root, identity, gid)
	t.Cleanup(func() { runtime.Close(); _ = h.Close() })
	return &policyRuntimeNode{runtime: runtime, session: session, host: h, identity: identity}
}

func policyRuntimePair(t *testing.T, ctx context.Context, gid entmoot.GroupID) (*policyRuntimeNode, *policyRuntimeNode) {
	t.Helper()
	founder, _ := mustDaemonIdentity(t)
	member, memberInfo := mustDaemonIdentity(t)
	founderRoot, memberRoot := t.TempDir(), t.TempDir()
	mustCreateGroup(t, founderRoot, gid, founder, membership.DefaultPolicy())
	group := mustOpenGroup(t, founderRoot, gid)
	join := mustJoinWithInvite(t, group, member, mustDaemonInvite(t, group, founder, memberInfo, 1))
	checkpoint := group.Canonical()
	mustCloseGroup(t, group)
	adopted, err := membership.Adopt(memberRoot, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adopted.Apply(join); err != nil {
		t.Fatal(err)
	}
	mustCloseGroup(t, adopted)
	return startPolicyRuntimeNode(t, ctx, founderRoot, founder, gid), startPolicyRuntimeNode(t, ctx, memberRoot, member, gid)
}

func restrictiveRuntimePolicy() entpolicy.Policy {
	p := entpolicy.Standard()
	p.MaxMessageBytes = 8
	p.MessageRatePerAuthor = "1/hour"
	p.MessageBurstPerAuthor = 1
	return p
}

func (n *policyRuntimeNode) message(t *testing.T, ctx context.Context, topics []string, content []byte) entmoot.Message {
	t.Helper()
	author, ok := n.session.group.MemberInfoByID(n.runtime.binding.MemberID)
	if !ok {
		t.Fatal("fixture identity missing from group")
	}
	return signRuntimePolicyMessage(t, ctx, n.session.groupID, n.session.group.Canonical().ID, n.identity, author, topics, content)
}

func signRuntimePolicyMessage(t *testing.T, ctx context.Context, gid entmoot.GroupID, head entmoot.RosterEntryID, identity *keystore.Identity, author entmoot.NodeInfo, topics []string, content []byte) entmoot.Message {
	t.Helper()
	signer, err := signing.NewLocalSigner(author, identity)
	if err != nil {
		t.Fatal(err)
	}
	message, err := signing.SignMessage(ctx, signer, entmoot.Message{
		Version: 2, GroupID: gid, Author: author, Timestamp: time.Now().UnixMilli(),
		RosterHead: &head, Topics: topics, Content: content,
	})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func (n *policyRuntimeNode) policyMessage(t *testing.T, ctx context.Context, update entpolicy.Update) entmoot.Message {
	t.Helper()
	raw, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	return n.message(t, ctx, []string{entpolicy.UpdateTopic}, raw)
}

func assertRuntimePolicy(t *testing.T, ctx context.Context, node *policyRuntimeNode, sequence uint64, expected *entpolicy.Policy) {
	t.Helper()
	actual, configured, err := node.runtime.policyStore.Get(ctx, node.session.groupID)
	if err != nil || configured != (expected != nil) || (expected != nil && actual != *expected) {
		t.Fatalf("runtime policy = %+v configured=%v err=%v; want %+v", actual, configured, err, expected)
	}
	got, known, err := node.runtime.policyStore.Sequence(ctx, node.session.groupID)
	if err != nil || got != sequence || known != (sequence != 0) {
		t.Fatalf("runtime policy sequence = %d known=%v err=%v; want %d", got, known, err, sequence)
	}
}

func assertRuntimeContentPolicy(t *testing.T, ctx context.Context, node *policyRuntimeNode, restricted bool, label string) {
	t.Helper()
	message := node.message(t, ctx, []string{"chat"}, []byte("larger than eight bytes: "+label))
	_, err := node.session.live.Publish(ctx, message)
	if restricted && err == nil {
		t.Fatal("ordinary oversized publish succeeded under restrictive policy")
	}
	if !restricted && err != nil {
		t.Fatalf("ordinary publish remained restricted after clear: %v", err)
	}
}

func waitPolicyMessage(t *testing.T, ctx context.Context, node *policyRuntimeNode, message entmoot.Message, catchup bool) {
	t.Helper()
	update, err := entpolicy.ParseUpdate(message.Content)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if catchup {
			node.runtime.catchUp(ctx, node.session)
		}
		held, err := node.runtime.store.Has(ctx, node.session.groupID, message.ID)
		if err != nil {
			t.Fatal(err)
		}
		// The insert precedes policy application, so observing the message
		// alone is not enough to know the receiving worker has applied it.
		sequence, _, err := node.runtime.policyStore.Sequence(ctx, node.session.groupID)
		if err != nil {
			t.Fatal(err)
		}
		if held && sequence >= update.Sequence {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("policy message was not received and applied (catchup=%v): %v", catchup, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitPolicyLiveLink(t *testing.T, ctx context.Context, from, to *policyRuntimeNode) {
	t.Helper()
	// Gossip subscriptions propagate asynchronously. Probe with fresh signed
	// messages instead of a fixed sleep or relying on private pubsub fields.
	for attempt := 0; ; attempt++ {
		message := from.message(t, ctx, []string{"chat"}, []byte(fmt.Sprintf("live-ready-%d", attempt)))
		if _, err := from.session.live.Publish(ctx, message); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("live subscriptions never became ready: %v", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
		messages, err := to.runtime.store.Range(ctx, to.session.groupID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, received := range messages {
			if strings.HasPrefix(string(received.Content), "live-ready-") {
				return
			}
		}
	}
}
