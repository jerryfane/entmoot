// Command entmootd is the Entmoot v0 daemon + CLI. A single binary that:
//
//   - runs the long-running gossip accept loop against a Pilot daemon (`run`);
//   - creates groups, issues invites, joins, and publishes messages.
//
// Global flags configure where to find the Pilot socket and where Entmoot
// stores its identity / group data. Subcommands select behavior. See
// ARCHITECTURE.md §2 (single-binary stance) and the Phase E plan for scope.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/transport/pilot"
)

// subcommandTimeout is the default deadline applied to every one-shot
// subcommand (group create, invite create, join, publish, info). The `run`
// subcommand does NOT use this — it installs its own signal-driven cancel.
const subcommandTimeout = 30 * time.Second

// globalFlags is the set of flags shared by every subcommand. The struct is
// populated by the top-level FlagSet before dispatch.
type globalFlags struct {
	socket     string
	identity   string
	data       string
	listenPort uint
	logLevel   string
}

func main() {
	if err := run(); err != nil {
		// run() logs on error already; we exit 1 without re-logging to avoid
		// noise.
		_ = err
		os.Exit(1)
	}
}

// run is the real entry. It parses global flags, sets up slog, dispatches the
// subcommand, and returns a non-nil error on any subcommand failure (which
// main translates into exit 1).
func run() error {
	fs := flag.NewFlagSet("entmootd", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd [flags] <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Subcommands:")
		fmt.Fprintln(os.Stderr, "  run                      Start the long-running daemon.")
		fmt.Fprintln(os.Stderr, "  group create -name NAME  Create a new group.")
		fmt.Fprintln(os.Stderr, "  invite create -group GID [-peers N1,N2,...]")
		fmt.Fprintln(os.Stderr, "                           Print a signed invite bundle.")
		fmt.Fprintln(os.Stderr, "  join -invite FILE        Load invite and call gossip.Join.")
		fmt.Fprintln(os.Stderr, "  publish -group GID -topic T -content STR")
		fmt.Fprintln(os.Stderr, "                           Publish a message into the group.")
		fmt.Fprintln(os.Stderr, "  info                     Print node id, pubkey, groups, roster sizes.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Global flags:")
		fs.PrintDefaults()
	}

	gf := &globalFlags{}
	fs.StringVar(&gf.socket, "socket", "/tmp/pilot.sock", "Pilot daemon IPC socket path")
	fs.StringVar(&gf.identity, "identity", "~/.entmoot/identity.json", "Entmoot identity file")
	fs.StringVar(&gf.data, "data", "~/.entmoot", "Entmoot data root")
	fs.UintVar(&gf.listenPort, "listen-port", 1004, "Entmoot listen port")
	fs.StringVar(&gf.logLevel, "log-level", "info", "slog level: debug|info|warn|error")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	// Configure slog before anything logs.
	level, err := parseLogLevel(gf.logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "entmootd: %v\n", err)
		return err
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))

	if gf.listenPort == 0 || gf.listenPort > 0xFFFF {
		return fmt.Errorf("listen-port %d out of uint16 range", gf.listenPort)
	}

	// Expand ~ in path flags once so every subcommand sees the same real paths.
	if v, err := expandHome(gf.identity); err == nil {
		gf.identity = v
	} else {
		return err
	}
	if v, err := expandHome(gf.data); err == nil {
		gf.data = v
	} else {
		return err
	}

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		return errors.New("missing subcommand")
	}

	switch args[0] {
	case "run":
		return cmdRun(gf, args[1:])
	case "group":
		return cmdGroup(gf, args[1:])
	case "invite":
		return cmdInvite(gf, args[1:])
	case "join":
		return cmdJoin(gf, args[1:])
	case "publish":
		return cmdPublish(gf, args[1:])
	case "info":
		return cmdInfo(gf, args[1:])
	default:
		fs.Usage()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

// setupCtx bundles the shared resources every (non-info-only) subcommand
// needs. openGroup loads a (roster, store) pair for a given group id from
// the shared on-disk data root; the same JSONL store handle is reused across
// groups so concurrent Puts across the canary's three-peer demo share one
// file-handle pool.
type setupCtx struct {
	logger    *slog.Logger
	identity  *keystore.Identity
	transport *pilot.Transport
	nodeID    entmoot.NodeID
	dataDir   string
	store     *store.JSONL

	// mu guards openRosters; OpenJSONL is idempotent for the store but the
	// roster.RosterLog is not sharable across concurrent open calls.
	mu           sync.Mutex
	openRosters  map[entmoot.GroupID]*roster.RosterLog
	extraClosers []func() error
}

// openGroup returns the (roster, store) pair for gid, creating the on-disk
// directory structure via the JSONL implementations if missing. The store is
// shared across all groups; the roster is per-group and cached on first open.
func (s *setupCtx) openGroup(gid entmoot.GroupID) (*roster.RosterLog, store.MessageStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.openRosters[gid]; ok {
		return r, s.store, nil
	}
	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		return nil, nil, fmt.Errorf("open roster: %w", err)
	}
	s.openRosters[gid] = r
	return r, s.store, nil
}

// Close tears down every resource held by the setup context. Safe to call
// multiple times in defers.
func (s *setupCtx) Close() {
	s.mu.Lock()
	for _, r := range s.openRosters {
		if err := r.Close(); err != nil {
			s.logger.Warn("close roster", slog.String("err", err.Error()))
		}
	}
	s.openRosters = map[entmoot.GroupID]*roster.RosterLog{}
	s.mu.Unlock()
	if s.store != nil {
		if err := s.store.Close(); err != nil {
			s.logger.Warn("close store", slog.String("err", err.Error()))
		}
	}
	for _, fn := range s.extraClosers {
		_ = fn()
	}
	if s.transport != nil {
		if err := s.transport.Close(); err != nil {
			s.logger.Warn("close transport", slog.String("err", err.Error()))
		}
	}
}

// setup performs the common bootstrap: load-or-generate the identity, open
// the Pilot transport, open the shared JSONL message store. It returns a
// setupCtx that owns everything; callers defer Close on the returned value.
func setup(gf *globalFlags) (*setupCtx, error) {
	logger := slog.Default()

	if err := os.MkdirAll(gf.data, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data %q: %w", gf.data, err)
	}

	id, err := keystore.LoadOrGenerate(gf.identity)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}

	tr, err := pilot.Open(pilot.Config{
		SocketPath: gf.socket,
		ListenPort: uint16(gf.listenPort),
		Logger:     logger,
	})
	if err != nil {
		return nil, fmt.Errorf("open pilot: %w", err)
	}

	st, err := store.OpenJSONL(gf.data)
	if err != nil {
		_ = tr.Close()
		return nil, fmt.Errorf("open store: %w", err)
	}

	return &setupCtx{
		logger:      logger,
		identity:    id,
		transport:   tr,
		nodeID:      tr.NodeID(),
		dataDir:     gf.data,
		store:       st,
		openRosters: make(map[entmoot.GroupID]*roster.RosterLog),
	}, nil
}

// cmdGroup dispatches the `group <op>` subcommands. v0 only recognizes
// `group create`.
func cmdGroup(gf *globalFlags, args []string) error {
	if len(args) == 0 {
		return errors.New("group: missing op (want: create)")
	}
	switch args[0] {
	case "create":
		return cmdGroupCreate(gf, args[1:])
	default:
		return fmt.Errorf("group: unknown op %q", args[0])
	}
}

// cmdGroupCreate generates a fresh GroupID, opens an empty roster for it, and
// writes the genesis entry. Prints the base64-encoded group id on stdout.
func cmdGroupCreate(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("group create", flag.ContinueOnError)
	name := fs.String("name", "", "informational group name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("group create: -name is required")
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("group create: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		return fmt.Errorf("group create: rand: %w", err)
	}

	r, _, err := s.openGroup(gid)
	if err != nil {
		return fmt.Errorf("group create: %w", err)
	}

	founderInfo := entmoot.NodeInfo{
		PilotNodeID:   s.nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	if err := r.Genesis(s.identity, founderInfo, time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("group create: genesis: %w", err)
	}
	s.logger.Info("group created",
		slog.String("group_id", gid.String()),
		slog.String("name", *name),
		slog.Uint64("founder", uint64(s.nodeID)))

	fmt.Println(gid.String())
	return nil
}

// cmdInvite dispatches `invite <op>`; v0 only recognizes `invite create`.
func cmdInvite(gf *globalFlags, args []string) error {
	if len(args) == 0 {
		return errors.New("invite: missing op (want: create)")
	}
	switch args[0] {
	case "create":
		return cmdInviteCreate(gf, args[1:])
	default:
		return fmt.Errorf("invite: unknown op %q", args[0])
	}
}

// cmdInviteCreate reads the roster for -group, signs a fresh Invite bundle,
// and prints it to stdout as JSON.
func cmdInviteCreate(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("invite create", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	peers := fs.String("peers", "", "comma-separated bootstrap peer node ids (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		return fmt.Errorf("invite create: %w", err)
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("invite create: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	r, st, err := s.openGroup(gid)
	if err != nil {
		return fmt.Errorf("invite create: %w", err)
	}

	founder, ok := r.Founder()
	if !ok {
		return errors.New("invite create: group has no founder (empty roster)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), subcommandTimeout)
	defer cancel()

	merkleRoot, err := st.MerkleRoot(ctx, gid)
	if err != nil {
		return fmt.Errorf("invite create: merkle root: %w", err)
	}

	// Build bootstrap peer list. Explicit -peers wins. Otherwise include
	// founder + up to 5 random other members.
	peerIDs, err := parsePeerList(*peers)
	if err != nil {
		return fmt.Errorf("invite create: %w", err)
	}
	if len(peerIDs) == 0 {
		peerIDs = defaultBootstrapPeers(r, founder.PilotNodeID, 5)
	}
	bootstrap := make([]entmoot.BootstrapPeer, 0, len(peerIDs))
	for _, p := range peerIDs {
		bootstrap = append(bootstrap, entmoot.BootstrapPeer{NodeID: p})
	}

	issuerInfo := entmoot.NodeInfo{
		PilotNodeID:   s.nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	invite := entmoot.Invite{
		GroupID:        gid,
		Founder:        founder,
		RosterHead:     r.Head(),
		MerkleRoot:     merkleRoot,
		BootstrapPeers: bootstrap,
		IssuedAt:       time.Now().UnixMilli(),
		Issuer:         issuerInfo,
	}

	signing := invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("invite create: canonical encode: %w", err)
	}
	invite.Signature = s.identity.Sign(sigInput)

	data, err := json.MarshalIndent(&invite, "", "  ")
	if err != nil {
		return fmt.Errorf("invite create: marshal: %w", err)
	}
	fmt.Println(string(data))
	return nil
}

// defaultBootstrapPeers returns founder + up to max-1 other random members.
// It does not invoke crypto/rand; member ordering from RosterLog.Members is
// deterministic (sorted ascending by node id) and that is good enough for
// "pick some members" in v0.
func defaultBootstrapPeers(r *roster.RosterLog, founder entmoot.NodeID, max int) []entmoot.NodeID {
	members := r.Members()
	out := []entmoot.NodeID{founder}
	added := 1
	for _, m := range members {
		if added >= max {
			break
		}
		if m == founder {
			continue
		}
		out = append(out, m)
		added++
	}
	return out
}

// cmdJoin reads an invite bundle from -invite and calls Gossiper.Join against
// it. On success the roster is written to disk and the process exits; the
// caller is expected to run `entmootd run` afterwards to actually participate
// in gossip.
func cmdJoin(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	inviteFile := fs.String("invite", "", "path to invite JSON file (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inviteFile == "" {
		return errors.New("join: -invite is required")
	}
	data, err := os.ReadFile(*inviteFile)
	if err != nil {
		return fmt.Errorf("join: read invite: %w", err)
	}
	var invite entmoot.Invite
	if err := json.Unmarshal(data, &invite); err != nil {
		return fmt.Errorf("join: parse invite: %w", err)
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("join: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	r, st, err := s.openGroup(invite.GroupID)
	if err != nil {
		return fmt.Errorf("join: open group: %w", err)
	}

	g, err := gossip.New(gossip.Config{
		LocalNode: s.nodeID,
		Identity:  s.identity,
		Roster:    r,
		Store:     st,
		Transport: s.transport,
		GroupID:   invite.GroupID,
		Logger:    s.logger,
	})
	if err != nil {
		return fmt.Errorf("join: new gossiper: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), subcommandTimeout)
	defer cancel()

	if err := g.Join(ctx, &invite); err != nil {
		return fmt.Errorf("join: %w", err)
	}
	members := r.Members()
	s.logger.Info("join succeeded",
		slog.String("group_id", invite.GroupID.String()),
		slog.Int("members", len(members)))
	for _, m := range members {
		fmt.Printf("member %d\n", uint32(m))
	}
	return nil
}

// cmdPublish signs and publishes a message into the target group using a
// transient Gossiper. The accept-loop side is assumed to be running in a
// sibling `entmootd run` process.
func cmdPublish(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	groupStr := fs.String("group", "", "base64 group id (required)")
	topic := fs.String("topic", "", "comma-separated topics (optional)")
	content := fs.String("content", "", "message content (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	gid, err := decodeGroupID(*groupStr)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	if *content == "" {
		return errors.New("publish: -content is required")
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("publish: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	r, st, err := s.openGroup(gid)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	g, err := gossip.New(gossip.Config{
		LocalNode: s.nodeID,
		Identity:  s.identity,
		Roster:    r,
		Store:     st,
		Transport: s.transport,
		GroupID:   gid,
		Logger:    s.logger,
	})
	if err != nil {
		return fmt.Errorf("publish: new gossiper: %w", err)
	}

	author := entmoot.NodeInfo{
		PilotNodeID:   s.nodeID,
		EntmootPubKey: s.identity.PublicKey,
	}
	msg := entmoot.Message{
		GroupID:   gid,
		Author:    author,
		Timestamp: time.Now().UnixMilli(),
		Topics:    parseTopicList(*topic),
		Content:   []byte(*content),
	}

	// v0 parents rule (ARCHITECTURE §3.2): include the 3 highest-timestamp
	// message ids we have seen for the group. We do the range-all query here
	// so publish can be used without a sibling `run`.
	ctx, cancel := context.WithTimeout(context.Background(), subcommandTimeout)
	defer cancel()
	existing, err := st.Range(ctx, gid, 0, 0)
	if err != nil {
		return fmt.Errorf("publish: range: %w", err)
	}
	if n := len(existing); n > 0 {
		// Range returns in topological order; take the last up-to-3 ids as a
		// reasonable approximation of "highest-timestamp seen."
		start := n - 3
		if start < 0 {
			start = 0
		}
		for _, e := range existing[start:] {
			msg.Parents = append(msg.Parents, e.ID)
		}
	}

	// Compute id, then sign, then put id back in.
	msg.ID = canonical.MessageID(msg)
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("publish: canonical encode: %w", err)
	}
	msg.Signature = s.identity.Sign(sigInput)

	// Sanity: author must be a current roster member.
	if !r.IsMember(s.nodeID) {
		return fmt.Errorf("publish: local node %d is not a member of %s", s.nodeID, gid)
	}

	if err := g.Publish(ctx, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	s.logger.Info("published",
		slog.String("group_id", gid.String()),
		slog.String("message_id", msg.ID.String()))
	fmt.Println(msg.ID.String())
	return nil
}

// cmdInfo prints node id (from Pilot), identity pubkey, and per-group roster
// + message counts. Does NOT run the accept loop.
func cmdInfo(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("info: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	fmt.Printf("pilot_node_id:  %d\n", uint32(s.nodeID))
	fmt.Printf("entmoot_pubkey: %s\n", encodeBase64(s.identity.PublicKey))
	fmt.Printf("data_dir:       %s\n", s.dataDir)
	fmt.Printf("identity_file:  %s\n", gf.identity)

	gids, err := listGroupIDs(s.dataDir, s.logger)
	if err != nil {
		return fmt.Errorf("info: %w", err)
	}
	if len(gids) == 0 {
		fmt.Println("groups:         (none)")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), subcommandTimeout)
	defer cancel()
	fmt.Println("groups:")
	for _, gid := range gids {
		r, st, err := s.openGroup(gid)
		if err != nil {
			s.logger.Warn("info: open group", slog.String("group", gid.String()), slog.String("err", err.Error()))
			continue
		}
		msgs, err := st.Range(ctx, gid, 0, 0)
		msgCount := 0
		if err == nil {
			msgCount = len(msgs)
		}
		fmt.Printf("  - %s  members=%d  messages=%d\n",
			gid.String(), len(r.Members()), msgCount)
	}
	return nil
}

// cmdRun opens every group on disk, spawns one Gossiper accept loop per
// group, and blocks until SIGINT/SIGTERM. Gossipers share the single Pilot
// transport; driver-level demultiplexing matches accepted connections to the
// correct accept channel via the per-port registration inside the Pilot
// driver, so all Gossipers Accept() on the same listener.
//
// # Multi-group accept gotcha
//
// The Pilot driver exposes a single Listener per port. We only call
// driver.Listen once (inside pilot.Open). Each gossip.Gossiper.Start calls
// transport.Accept; the transport hands out accepted connections one at a
// time. If there are multiple groups, the gossip package itself gates
// messages by group id at the frame-type level — so whichever Gossiper
// happens to accept a given frame first will see it, check the group id in
// the frame, and either handle or warn. That is good enough for the canary
// (one group per node); multi-group ergonomics are a v1+ concern tracked in
// the plan.
func cmdRun(gf *globalFlags, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("run: setup", slog.String("err", err.Error()))
		return err
	}
	defer s.Close()

	gids, err := listGroupIDs(s.dataDir, s.logger)
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	if len(gids) == 0 {
		s.logger.Warn("run: no groups found; waiting for signal anyway",
			slog.String("data_dir", s.dataDir))
	}

	rootCtx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Build gossipers first so we can surface config errors up front.
	gossipers := make([]*gossip.Gossiper, 0, len(gids))
	for _, gid := range gids {
		r, st, err := s.openGroup(gid)
		if err != nil {
			s.logger.Error("run: open group", slog.String("group", gid.String()), slog.String("err", err.Error()))
			return err
		}
		g, err := gossip.New(gossip.Config{
			LocalNode: s.nodeID,
			Identity:  s.identity,
			Roster:    r,
			Store:     st,
			Transport: s.transport,
			GroupID:   gid,
			Logger:    s.logger,
		})
		if err != nil {
			return fmt.Errorf("run: new gossiper for %s: %w", gid, err)
		}
		gossipers = append(gossipers, g)
	}

	s.logger.Info("entmootd up",
		slog.Uint64("pilot_node_id", uint64(s.nodeID)),
		slog.Uint64("listen_port", uint64(gf.listenPort)),
		slog.Int("groups", len(gossipers)))

	var wg sync.WaitGroup
	for _, g := range gossipers {
		wg.Add(1)
		go func(g *gossip.Gossiper) {
			defer wg.Done()
			if err := g.Start(rootCtx); err != nil {
				s.logger.Warn("gossiper stopped", slog.String("err", err.Error()))
			}
		}(g)
	}

	// If no gossipers are running, still block until signal so `run` always
	// terminates on a clean Ctrl-C rather than immediately.
	if len(gossipers) == 0 {
		<-rootCtx.Done()
	} else {
		<-rootCtx.Done()
		wg.Wait()
	}
	s.logger.Info("entmootd shutting down")
	return nil
}

// encodeBase64 returns the standard base64 (with padding) representation of b.
// Used by the `info` subcommand to print the local Entmoot public key.
func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
