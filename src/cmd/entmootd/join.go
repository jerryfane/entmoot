package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/ratelimit"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/topic"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// warnIfPilotNotFullyHideIP queries the local pilot-daemon's Info and
// logs a WARN-level message naming every privacy gap between Entmoot's
// -hide-ip (gossip-layer IP suppression) and Pilot's three privacy
// flags (-turn-provider, -outbound-turn-only, -no-registry-endpoint).
// Best-effort: IPC failures degrade to a Debug log so startup isn't
// blocked when the daemon is slow to respond.
//
// Entmoot's -hide-ip alone suppresses endpoints in transport-ads; it
// can't prevent registry leaks (pilot-daemon publishes the real IP)
// or outbound source-IP leaks (pilot-daemon dials peers direct). This
// cross-layer check surfaces those gaps loudly at startup so the
// operator knows whether they're actually achieving the privacy
// posture they asked for.
//
// Added in Entmoot v1.4.3, matches pilot-daemon v1.9.0-jf.11a.
func warnIfPilotNotFullyHideIP(d *ipcclient.Driver) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := d.InfoStruct(ctx)
	if err != nil {
		slog.Debug("hide-ip check: could not query pilot Info",
			slog.String("err", err.Error()))
		return
	}
	var issues []string
	if info.TURNEndpoint == "" {
		issues = append(issues, "pilot-daemon has no TURN allocation "+
			"(set pilot-daemon -turn-provider=cloudflare or "+
			"-turn-provider=static; without it, Entmoot hide-ip "+
			"publishes empty transport-ads and the peer is unreachable)")
	}
	if !info.OutboundTURNOnly {
		issues = append(issues, "pilot-daemon not in outbound-turn-only "+
			"mode (outbound tunnel frames may reveal source IP to "+
			"peers direct; set pilot-daemon -outbound-turn-only)")
	}
	if !info.NoRegistryEndpoint {
		issues = append(issues, "pilot-daemon still publishes endpoint "+
			"to registry (peers can registry.Lookup your IP; set "+
			"pilot-daemon -no-registry-endpoint)")
	}
	if len(issues) > 0 {
		slog.Warn("entmootd -hide-ip is only partially supported by the "+
			"local pilot-daemon; app-layer IP suppression is incomplete",
			slog.Any("missing", issues),
			slog.String("remedy", "run pilot-daemon with -hide-ip "+
				"(preset for -no-registry-endpoint + -outbound-turn-only; "+
				"requires -turn-provider) or set the sub-flags "+
				"individually"))
	} else {
		slog.Info("entmootd -hide-ip configuration verified: " +
			"pilot-daemon is in full hide-ip mode " +
			"(turn-provider + outbound-turn-only + no-registry-endpoint)")
	}
}

// cmdJoin is the blocking top-level command. It reads or fetches an
// invite, applies it, opens the control socket, and serves IPC traffic
// until the process is signalled.
func cmdJoin(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	// v1.2.0: repeatable -advertise-endpoint flag feeds the gossiper's
	// LocalEndpoints callback. Format: "-advertise-endpoint
	// tcp=37.27.59.89:4443 -advertise-endpoint udp=37.27.59.89:37736".
	// Zero flags ships a gossiper with LocalEndpoints=nil, which is the
	// documented "no change from v1.1.x" behavior — we still accept
	// inbound TransportAds from other peers, we just don't publish one
	// of our own. Auto-discovery from Pilot (the pilot-daemon's own
	// configured listen endpoints) is deferred to v1.3 — the jf.7
	// driver.Info response does not expose listen addresses today.
	var advertiseEndpoints endpointFlag
	fs.Var(&advertiseEndpoints, "advertise-endpoint",
		"advertise this node's endpoint (network=host:port); repeatable (v1.2.0)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "join: missing invite (file path or http(s) URL)")
		return exitInvalidArgument
	}
	if len(rest) > 1 {
		fmt.Fprintln(os.Stderr, "join: too many positional arguments")
		return exitInvalidArgument
	}
	inviteArg := rest[0]

	invite, err := loadInvite(inviteArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "join: invite: %v\n", err)
		// A local-parse failure (bad file, bad JSON, expired ValidUntil)
		// is INVALID_ARGUMENT per CLI_DESIGN §3.1. A network-fetch
		// failure is a transport error (exit 1).
		if errors.Is(err, errFetchFailed) {
			return exitTransport
		}
		if errors.Is(err, entmoot.ErrInviteExpired) ||
			errors.Is(err, entmoot.ErrSigInvalid) ||
			errors.Is(err, errInviteMalformed) {
			return exitInvalidArgument
		}
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("join: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	// Early-check the control socket: if another join is already live,
	// exit 6 before we touch Pilot.
	sockPath := controlSocketPath(s.dataDir)
	if controlSocketAlive(sockPath, 200*time.Millisecond) {
		fmt.Fprintf(os.Stderr, "join: another join is already running at %s\n", sockPath)
		return exitControlUnavail
	}
	// Stale socket left behind by a previous crash: unlink so we can
	// bind. If a process is listening we'd have taken the branch above.
	if _, err := os.Stat(sockPath); err == nil {
		if err := os.Remove(sockPath); err != nil {
			fmt.Fprintf(os.Stderr, "join: remove stale socket: %v\n", err)
			return exitTransport
		}
	}

	tr, err := openPilot(gf)
	if err != nil {
		slog.Error("join: pilot", slog.String("err", err.Error()))
		return exitTransport
	}
	defer tr.Close()
	nodeID := tr.NodeID()

	// v1.4.3: when -hide-ip is set, verify the local pilot-daemon is
	// in full hide-ip mode (turn-provider + outbound-turn-only +
	// no-registry-endpoint). Entmoot's -hide-ip hides IP at the
	// gossip layer only; without matching pilot-daemon flags, peers
	// can still learn our IP via registry.Lookup or direct outbound
	// source-IP leaks. Best-effort: failures to query Info() don't
	// block startup, just log at Debug.
	if gf.hideIP {
		warnIfPilotNotFullyHideIP(tr.Driver())
	}

	rawStore, err := store.OpenSQLite(s.dataDir)
	if err != nil {
		slog.Error("join: open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = rawStore.Close() }()

	// Wrap the store so IPC tail subscribers see new messages as they
	// land. The gossip/publish path writes through this wrapper.
	notifyStore := newNotifyingStore(rawStore)

	rlog, err := roster.OpenJSONL(s.dataDir, invite.GroupID)
	if err != nil {
		slog.Error("join: open roster", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = rlog.Close() }()

	// Establish the signal-bound root context before anything starts
	// long-running goroutines (v1.4.4's TURN-endpoint poller is the
	// first). Previously this was set up just before gossiper.Join;
	// the poller needs it earlier so its lifetime tracks the daemon's.
	rootCtx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// v1.2.0: snapshot the -advertise-endpoint values once at startup.
	// v1.4.4: merge a live-polled TURN endpoint from pilot-daemon on
	// top of the CLI snapshot, so Cloudflare allocation port rotation
	// (port changes on restart or credential refresh) gets observed
	// and re-advertised within ~30 s. Without this poller, LocalEndpoints
	// returns a fixed CLI snapshot; remote peers keep using the stale
	// TURN relay addr in their cached transport_ad and their outbound
	// frames get silently dropped by Cloudflare's edge.
	cliEps := advertiseEndpoints.Snapshot()
	turnPoller := newTURNEndpointPoller(tr.Driver(), turnEndpointPollInterval)
	// Prime the poller synchronously so the advertiser's startup
	// publish sees the current TURN addr on the very first call to
	// LocalEndpoints. Failure degrades to empty — the ticker will
	// pick it up on the next 30 s boundary.
	turnPoller.pollOnce(rootCtx)

	localEndpointsFn := func() []entmoot.NodeEndpoint {
		out := make([]entmoot.NodeEndpoint, 0, len(cliEps)+1)
		// Carry non-TURN CLI entries (tcp=, udp=) verbatim.
		for _, e := range cliEps {
			if e.Network != "turn" {
				out = append(out, e)
			}
		}
		// Prefer the live-polled TURN addr. Fall back to any turn=
		// CLI entries if the poller has no value yet (e.g. the
		// startup poll failed and the ticker hasn't fired).
		if live := turnPoller.CurrentTURN(); live != "" {
			out = append(out, entmoot.NodeEndpoint{Network: "turn", Addr: live})
		} else {
			for _, e := range cliEps {
				if e.Network == "turn" {
					out = append(out, e)
				}
			}
		}
		return out
	}
	// Background polling runs for the lifetime of the daemon.
	go turnPoller.Run(rootCtx)

	g, err := gossip.New(gossip.Config{
		LocalNode: nodeID,
		Identity:  s.identity,
		Roster:    rlog,
		Store:     notifyStore,
		Transport: tr,
		GroupID:   invite.GroupID,
		Logger:    slog.Default(),
		// v1.2.0 wiring: rawStore (the concrete *store.SQLite)
		// satisfies the gossip.TransportAdStore interface; notifyStore
		// is a thin wrapper around it for the message surface only.
		TransportAdStore: rawStore,
		RateLimiter:      ratelimit.New(ratelimit.DefaultLimits(), nil),
		LocalEndpoints:   localEndpointsFn,
		// v1.4.4: wire the TURN-rotation signal so the advertiser
		// re-publishes immediately on change instead of waiting for
		// the 6-day safety-net refresh.
		EndpointsChanged: turnPoller.Changed(),
		// v1.4.0: when set, the advertiser publishes only turn
		// endpoints from LocalEndpoints and drops UDP/TCP entries,
		// or emits an empty ad + warning when no TURN endpoint
		// exists. See gossip.Config.HideIP for the rationale.
		HideIP: gf.hideIP,
	})
	if err != nil {
		slog.Error("join: new gossiper", slog.String("err", err.Error()))
		return exitTransport
	}

	// Run Join with a bounded deadline so an unreachable peer set fails
	// loudly rather than hanging the process before the control socket
	// is even up.
	joinCtx, joinCancel := context.WithTimeout(rootCtx, 30*time.Second)
	err = g.Join(joinCtx, invite)
	joinCancel()
	if err != nil {
		// Expired / bad-sig invite surfaces as INVALID_ARGUMENT here
		// too (Join re-verifies signature and ValidUntil).
		if errors.Is(err, entmoot.ErrInviteExpired) || errors.Is(err, entmoot.ErrSigInvalid) {
			fmt.Fprintf(os.Stderr, "join: %v\n", err)
			return exitInvalidArgument
		}
		slog.Error("join: gossiper.Join", slog.String("err", err.Error()))
		return exitTransport
	}

	// Bind the control socket with 0600 permissions. net.Listen uses
	// the process umask, so explicitly chmod afterwards.
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		slog.Error("join: listen control socket", slog.String("err", err.Error()))
		return exitTransport
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		slog.Warn("join: chmod control socket", slog.String("err", err.Error()))
	}
	// Ensure the socket file is removed on every return path, even
	// panics / Close errors.
	removeSocket := func() {
		if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("join: remove control socket", slog.String("err", err.Error()))
		}
	}

	// v1.2.0: replay persisted TransportAds into Pilot so peerTCP is
	// warm before the accept loop begins. Handles the "pilot-daemon
	// restart but entmootd still up" case and the "entmootd restart
	// but pilot still up" case symmetrically: on either boot, state
	// converges from disk. Failures are non-fatal — the ongoing
	// advertiser/refanout loops rebuild peerTCP within one refresh
	// cycle if we can't replay now. Own-author ads are skipped
	// because Pilot doesn't need (and on jf.7 actively rejects) a
	// self entry in its peerTCP map.
	replayCtx, replayCancel := context.WithTimeout(rootCtx, 10*time.Second)
	if ads, err := rawStore.GetAllTransportAds(replayCtx, invite.GroupID, time.Now(), false); err != nil {
		slog.Warn("join: replay transport ads",
			slog.String("err", err.Error()))
	} else {
		for _, ad := range ads {
			if ad.Author.PilotNodeID == nodeID {
				continue
			}
			if err := tr.SetPeerEndpoints(replayCtx, ad.Author.PilotNodeID, ad.Endpoints); err != nil {
				slog.Debug("join: replay endpoint install failed",
					slog.Uint64("peer", uint64(ad.Author.PilotNodeID)),
					slog.String("err", err.Error()))
			}
		}
	}
	replayCancel()

	// Start the gossiper accept loop and the IPC accept loop in
	// separate goroutines. Wait for both to return before emitting the
	// final shutdown log and removing the control socket.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := g.Start(rootCtx); err != nil {
			slog.Warn("join: gossiper stopped", slog.String("err", err.Error()))
		}
	}()

	// IPC server state: shared across handlers via closure.
	srv := &ipcServer{
		nodeID:     nodeID,
		identity:   s.identity,
		dataDir:    s.dataDir,
		listenPort: uint16(gf.listenPort),
		gossiper:   g,
		roster:     rlog,
		groupID:    invite.GroupID,
		store:      rawStore,
		notify:     notifyStore,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			<-rootCtx.Done()
			_ = listener.Close()
		}()
		srv.acceptLoop(rootCtx, listener)
	}()

	// Emit the one-line "joined" event on stdout.
	joinedEvent := map[string]any{
		"event":          "joined",
		"group_id":       invite.GroupID,
		"members":        len(rlog.Members()),
		"listen_port":    gf.listenPort,
		"control_socket": sockPath,
	}
	if data, err := json.Marshal(joinedEvent); err == nil {
		fmt.Println(string(data))
	}

	// Block until a signal fires and all goroutines unwind.
	<-rootCtx.Done()
	wg.Wait()

	// WAL checkpoint + DB close is handled by rawStore.Close in the
	// deferred teardown above. Remove the control socket last.
	removeSocket()
	slog.Info("entmootd shutting down")
	return exitOK
}

// errInviteMalformed is the local sentinel used to distinguish a
// malformed/unsigned invite from a fetch error.
var errInviteMalformed = errors.New("invite malformed")

// errFetchFailed marks a network-fetch failure for a URL invite.
var errFetchFailed = errors.New("invite fetch failed")

// loadInvite reads an invite JSON bundle from arg (file path or
// http(s) URL) and verifies ValidUntil at parse time (CLI_DESIGN §3.1
// says an expired invite is rejected at invite-parse time with exit 5).
// Signature verification happens in gossip.Join; this function only
// catches structurally-bad invites and expired ones.
func loadInvite(arg string) (*entmoot.Invite, error) {
	var raw []byte
	if len(arg) > 0 && (hasPrefix(arg, "http://") || hasPrefix(arg, "https://")) {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(arg)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errFetchFailed, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("%w: %s", errFetchFailed, resp.Status)
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errFetchFailed, err)
		}
		raw = b
	} else {
		b, err := os.ReadFile(arg)
		if err != nil {
			return nil, fmt.Errorf("%w: read %s: %v", errInviteMalformed, arg, err)
		}
		raw = b
	}
	var invite entmoot.Invite
	if err := json.Unmarshal(raw, &invite); err != nil {
		return nil, fmt.Errorf("%w: parse: %v", errInviteMalformed, err)
	}
	if invite.ValidUntil > 0 {
		now := time.Now().UnixMilli()
		if now > invite.ValidUntil {
			return nil, fmt.Errorf("%w: valid_until=%d now=%d",
				entmoot.ErrInviteExpired, invite.ValidUntil, now)
		}
	}
	return &invite, nil
}

// hasPrefix is a tiny alias for strings.HasPrefix so the import
// surface in this file stays minimal.
func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}

// endpointFlag implements flag.Value for a repeatable
// -advertise-endpoint argument whose value is "network=host:port".
// Networks are restricted to "tcp", "udp", and "turn" (what Pilot's
// driver understands today; "turn" added in v1.4.0 / jf.8). The addr
// half is parsed with net.SplitHostPort so we catch a malformed value
// at flag-parse time rather than at advertiser-publish time. (v1.2.0)
type endpointFlag struct {
	entries []entmoot.NodeEndpoint
}

// Set parses one -advertise-endpoint value of the form
// "network=host:port". Repeated invocations append to the slice.
// Empty values are rejected so `-advertise-endpoint ""` surfaces at
// parse time rather than as a silent no-op.
func (e *endpointFlag) Set(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("empty -advertise-endpoint")
	}
	eq := strings.IndexByte(s, '=')
	if eq <= 0 || eq == len(s)-1 {
		return fmt.Errorf("-advertise-endpoint %q: want network=host:port", s)
	}
	network := strings.TrimSpace(s[:eq])
	addr := strings.TrimSpace(s[eq+1:])
	switch network {
	case "tcp", "udp", "turn":
	default:
		return fmt.Errorf("-advertise-endpoint %q: unsupported network %q (want tcp, udp, or turn)", s, network)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("-advertise-endpoint %q: parse addr: %w", s, err)
	}
	e.entries = append(e.entries, entmoot.NodeEndpoint{Network: network, Addr: addr})
	return nil
}

// String implements flag.Value. Concatenates the parsed entries in
// the canonical "network=addr" form joined with commas so help text
// prints something readable for a partially-parsed state.
func (e *endpointFlag) String() string {
	if e == nil || len(e.entries) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, ep := range e.entries {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(ep.Network)
		sb.WriteByte('=')
		sb.WriteString(ep.Addr)
	}
	return sb.String()
}

// Snapshot returns a defensive copy of the parsed entries. The
// gossiper's LocalEndpoints callback captures this snapshot in a
// closure so later flag mutations (shouldn't happen, but we're
// safe-by-default) don't race the advertiser-loop reads.
func (e *endpointFlag) Snapshot() []entmoot.NodeEndpoint {
	if e == nil {
		return nil
	}
	return append([]entmoot.NodeEndpoint(nil), e.entries...)
}

// notifyingStore wraps a MessageStore and publishes newly-stored
// messages to in-process subscribers. The gossip layer's Store writes
// through the wrapper; IPC tail_subscribe handlers register channels
// against it. Kept local to cmd/entmootd so neither the gossip nor
// store packages need to grow a subscribe method.
type notifyingStore struct {
	inner store.MessageStore

	mu     sync.Mutex
	subs   map[int]chan<- entmoot.Message
	nextID int
}

func newNotifyingStore(inner store.MessageStore) *notifyingStore {
	return &notifyingStore{
		inner: inner,
		subs:  make(map[int]chan<- entmoot.Message),
	}
}

// subscribe registers ch to receive future Put events. Returns an
// unregister fn.
func (n *notifyingStore) subscribe(ch chan<- entmoot.Message) func() {
	n.mu.Lock()
	id := n.nextID
	n.nextID++
	n.subs[id] = ch
	n.mu.Unlock()
	return func() {
		n.mu.Lock()
		delete(n.subs, id)
		n.mu.Unlock()
	}
}

// broadcast delivers m to every current subscriber, non-blocking: a
// subscriber whose buffer is full misses the event.
func (n *notifyingStore) broadcast(m entmoot.Message) {
	n.mu.Lock()
	subs := make([]chan<- entmoot.Message, 0, len(n.subs))
	for _, c := range n.subs {
		subs = append(subs, c)
	}
	n.mu.Unlock()
	for _, c := range subs {
		select {
		case c <- m:
		default:
			// Subscriber too slow; drop. Matches roster.emit's
			// philosophy: loss > stall of writers.
		}
	}
}

// Put implements store.MessageStore. On success we broadcast; on
// failure subscribers see nothing (consistent with "only delivered
// messages are visible").
func (n *notifyingStore) Put(ctx context.Context, m entmoot.Message) error {
	if err := n.inner.Put(ctx, m); err != nil {
		return err
	}
	n.broadcast(m)
	return nil
}

func (n *notifyingStore) Get(ctx context.Context, gid entmoot.GroupID, id entmoot.MessageID) (entmoot.Message, error) {
	return n.inner.Get(ctx, gid, id)
}
func (n *notifyingStore) Has(ctx context.Context, gid entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return n.inner.Has(ctx, gid, id)
}
func (n *notifyingStore) Range(ctx context.Context, gid entmoot.GroupID, since, until int64) ([]entmoot.Message, error) {
	return n.inner.Range(ctx, gid, since, until)
}
func (n *notifyingStore) MerkleRoot(ctx context.Context, gid entmoot.GroupID) ([32]byte, error) {
	return n.inner.MerkleRoot(ctx, gid)
}
func (n *notifyingStore) IterMessageIDsInIDRange(ctx context.Context, gid entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	return n.inner.IterMessageIDsInIDRange(ctx, gid, loID, hiID)
}
func (n *notifyingStore) Close() error { return n.inner.Close() }

// ipcServer bundles the state IPC handlers need. All fields are
// read-only after cmdJoin finishes setup so handlers may access them
// without locking.
type ipcServer struct {
	nodeID     entmoot.NodeID
	identity   *keystore.Identity
	dataDir    string
	listenPort uint16
	gossiper   *gossip.Gossiper
	roster     *roster.RosterLog
	groupID    entmoot.GroupID
	store      *store.SQLite
	notify     *notifyingStore
}

// acceptLoop accepts IPC connections until the listener is closed.
// Each connection is handled in its own goroutine; the loop waits for
// all connection handlers to return before itself returning.
func (s *ipcServer) acceptLoop(ctx context.Context, l net.Listener) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Warn("ipc: accept", slog.String("err", err.Error()))
			return
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			s.handleConn(ctx, c)
		}(conn)
	}
}

// handleConn reads one frame and dispatches on type. publish_req and
// info_req are one-shot; tail_subscribe keeps the connection open and
// streams tail_event frames until the client closes or the context
// fires.
func (s *ipcServer) handleConn(ctx context.Context, c net.Conn) {
	// A short read deadline for the first frame defends against a
	// caller that opens the socket and stalls; live subscribers reset
	// the deadline once they've registered.
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))

	t, payload, err := ipc.ReadAndDecode(c)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		slog.Warn("ipc: read frame", slog.String("err", err.Error()))
		return
	}

	switch v := payload.(type) {
	case *ipc.PublishReq:
		s.handlePublish(ctx, c, v)
	case *ipc.InfoReq:
		s.handleInfo(ctx, c)
	case *ipc.TailSubscribe:
		// Subscription stays open indefinitely; clear the short read
		// deadline we set above.
		_ = c.SetReadDeadline(time.Time{})
		s.handleTail(ctx, c, v)
	default:
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: fmt.Sprintf("unexpected ipc type %s", t),
		})
	}
}

// handlePublish resolves the target group, signs the supplied Topics+
// Content as a full Message, and gossips it. Emits PublishResp on
// success or a structured ErrorFrame on failure.
func (s *ipcServer) handlePublish(ctx context.Context, c net.Conn, req *ipc.PublishReq) {
	// Resolve group: the daemon only serves its own groupID in v1
	// (single-group per join process). nil means "the single group we
	// know"; any explicit value must match.
	gid := s.groupID
	if req.GroupID != nil && *req.GroupID != s.groupID {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: req.GroupID,
			Message: "group not joined",
		})
		return
	}
	if len(req.Topics) == 0 {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: "no topics supplied",
		})
		return
	}
	for _, t := range req.Topics {
		// We don't call topic.ValidPattern here because topics on a
		// message are concrete strings, not patterns. We just reject
		// empty topics defensively.
		if t == "" {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
				Type:    "error",
				Code:    ipc.CodeInvalidArgument,
				Message: "empty topic",
			})
			return
		}
	}
	if !s.roster.IsMember(s.nodeID) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: fmt.Sprintf("local node %d is not a member", s.nodeID),
		})
		return
	}

	// Build and sign the Message. Author pubkey comes from the
	// roster's own record of the local node so it matches what every
	// peer (and our own verifyMessage) expects.
	now := time.Now().UnixMilli()
	author := entmoot.NodeInfo{
		PilotNodeID:   s.nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	if info, ok := s.roster.MemberInfo(s.nodeID); ok {
		author = info
	}

	msg := entmoot.Message{
		GroupID:   gid,
		Author:    author,
		Timestamp: now,
		Topics:    append([]string(nil), req.Topics...),
		Content:   append([]byte(nil), req.Content...),
	}

	// Parent selection mirrors v0's cmdPublish: include up to 3 of
	// the most-recent messages we have.
	if existing, err := s.store.Range(ctx, gid, 0, 0); err == nil && len(existing) > 0 {
		start := len(existing) - 3
		if start < 0 {
			start = 0
		}
		for _, e := range existing[start:] {
			msg.Parents = append(msg.Parents, e.ID)
		}
	}

	// Compute ID, then sign, then put the ID back in.
	msg.ID = canonical.MessageID(msg)
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "canonical encode: " + err.Error(),
		})
		return
	}
	msg.Signature = s.identity.Sign(sigInput)

	if err := s.gossiper.Publish(ctx, msg); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "gossiper.Publish: " + err.Error(),
		})
		return
	}

	_ = ipc.EncodeAndWrite(c, &ipc.PublishResp{
		MessageID:   msg.ID,
		GroupID:     gid,
		TimestampMS: now,
	})
}

// handleInfo assembles a full InfoResp snapshot from live state.
func (s *ipcServer) handleInfo(ctx context.Context, c net.Conn) {
	pub, _ := pubkeyFromRoster(s.roster, s.nodeID, s.identity.PublicKey)

	// Enumerate <dataDir>/groups/* for consistency with the standalone
	// info command, but the authoritative group is always s.groupID
	// for a single-group join. Other groups found on disk are reported
	// with zero message counts unless they share our SQLite handle.
	gids, err := listGroupIDs(s.dataDir, slog.Default())
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "list groups: " + err.Error(),
		})
		return
	}

	groups := make([]ipc.GroupInfo, 0, len(gids))
	for _, gid := range gids {
		members := 0
		if gid == s.groupID {
			members = len(s.roster.Members())
		} else {
			// For groups outside the daemon's active roster, peek at
			// the roster file directly.
			if r, err := roster.OpenJSONL(s.dataDir, gid); err == nil {
				members = len(r.Members())
				_ = r.Close()
			}
		}
		msgs, err := s.store.Range(ctx, gid, 0, 0)
		msgCount := 0
		if err == nil {
			msgCount = len(msgs)
		}
		var mr *[32]byte
		if root, err := s.store.MerkleRoot(ctx, gid); err == nil {
			r := root
			mr = &r
		}
		groups = append(groups, ipc.GroupInfo{
			GroupID:    gid,
			Members:    members,
			Messages:   msgCount,
			MerkleRoot: mr,
		})
	}

	resp := &ipc.InfoResp{
		PilotNodeID:   s.nodeID,
		EntmootPubKey: pub,
		ListenPort:    s.listenPort,
		DataDir:       s.dataDir,
		Groups:        groups,
		Running:       true,
	}
	_ = ipc.EncodeAndWrite(c, resp)
}

// pubkeyFromRoster returns the locally-stored pubkey from the
// membership projection, falling back to fallback if absent.
func pubkeyFromRoster(r *roster.RosterLog, id entmoot.NodeID, fallback []byte) ([]byte, bool) {
	if info, ok := r.MemberInfo(id); ok && len(info.EntmootPubKey) > 0 {
		return info.EntmootPubKey, true
	}
	return fallback, false
}

// handleTail registers a subscriber channel with the notifying store
// and streams matching messages as tail_event frames until the client
// closes or ctx fires.
func (s *ipcServer) handleTail(ctx context.Context, c net.Conn, sub *ipc.TailSubscribe) {
	pattern := sub.Topic
	if pattern == "" {
		pattern = "#"
	}
	if err := topic.ValidPattern(pattern); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: "invalid topic pattern: " + err.Error(),
		})
		return
	}
	if sub.GroupID != nil && *sub.GroupID != s.groupID {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: sub.GroupID,
			Message: "group not joined",
		})
		return
	}

	ch := make(chan entmoot.Message, 32)
	unsub := s.notify.subscribe(ch)
	defer unsub()

	// Detect client-close so we can drop the subscription promptly.
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := c.Read(buf); err != nil {
				close(done)
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case m := <-ch:
			if sub.GroupID != nil && m.GroupID != *sub.GroupID {
				continue
			}
			if !matchAnyTopic(pattern, m.Topics) {
				continue
			}
			ev := &ipc.TailEvent{Message: m}
			if err := ipc.EncodeAndWrite(c, ev); err != nil {
				return
			}
		}
	}
}
