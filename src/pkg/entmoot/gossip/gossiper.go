package gossip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/clock"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/wire"
)

// defaultFanout is the number of random peers a Publish pushes a new message
// id to when Config.Fanout is zero. Chosen for the canary (3-node group); the
// real gossip math is O(log N + k) and we'll revisit for v1.
const defaultFanout = 3

// Config parameterizes a Gossiper. Every field except Fanout, Clock, and
// Logger is required; New returns an error if a required field is nil or
// zero. Construct one Gossiper per group per process.
type Config struct {
	// LocalNode is the Pilot node id of this peer. Used to exclude self from
	// peer sampling, to label outbound Gossip frames, and to sanity-check
	// inbound FetchResp signatures.
	LocalNode entmoot.NodeID
	// Identity signs outbound Gossip frames. Must be the same Ed25519 key that
	// the roster pairs with LocalNode.
	Identity *keystore.Identity
	// Roster is the group membership log. Required for signature verification
	// (inbound messages, inbound FetchResp bodies) and for peer sampling.
	Roster *roster.RosterLog
	// Store is where incoming message bodies and locally-published bodies are
	// persisted.
	Store store.MessageStore
	// Transport is the dial / listen / trust surface. In tests this is the
	// in-memory mock from NewMemTransports; in production Phase E wires it
	// to Pilot's driver.
	Transport Transport
	// GroupID identifies the single group this Gossiper serves.
	GroupID entmoot.GroupID
	// Fanout is the number of random peers to push each Publish to. Zero
	// selects defaultFanout.
	Fanout int
	// Clock supplies wall-clock time for Gossip frame timestamps. Nil
	// selects clock.System.
	Clock clock.Clock
	// Logger is used for slog.Warn / slog.Error surfaces. Nil selects
	// slog.Default().
	Logger *slog.Logger
}

// Gossiper runs the accept loop, publishes local messages, and fetches
// unseen ids announced by peers. Every Gossiper instance is scoped to a
// single group (Config.GroupID); v0 makes no attempt to multiplex.
//
// Concurrency: Publish is safe to call concurrently from any goroutine after
// Start has been invoked. The accept loop runs in the goroutine that calls
// Start and spawns one short-lived goroutine per inbound connection. Every
// goroutine respects the context passed to Start.
type Gossiper struct {
	cfg    Config
	logger *slog.Logger
	clk    clock.Clock
	fanout int

	// picker is rebuilt lazily on first Sample so a freshly-Joined Gossiper
	// (whose roster may grow between New and Start) always samples from the
	// latest membership view.
	pickerOnce sync.Once
	picker     *PeerPicker

	// wg tracks in-flight connection handlers so Start can wait for clean
	// shutdown when its context is cancelled.
	wg sync.WaitGroup
}

// New constructs a Gossiper. Does not start the accept loop; callers must
// invoke Start (typically after a successful Join). Returns an error if the
// Config is missing a required field.
func New(cfg Config) (*Gossiper, error) {
	if cfg.LocalNode == 0 {
		return nil, errors.New("gossip: Config.LocalNode is required")
	}
	if cfg.Identity == nil {
		return nil, errors.New("gossip: Config.Identity is required")
	}
	if cfg.Roster == nil {
		return nil, errors.New("gossip: Config.Roster is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("gossip: Config.Store is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("gossip: Config.Transport is required")
	}
	var zeroGroup entmoot.GroupID
	if cfg.GroupID == zeroGroup {
		return nil, errors.New("gossip: Config.GroupID is required")
	}
	fanout := cfg.Fanout
	if fanout == 0 {
		fanout = defaultFanout
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.System{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Gossiper{cfg: cfg, logger: logger, clk: clk, fanout: fanout}, nil
}

// Start runs the accept loop. It blocks until ctx is cancelled or the
// transport is closed. Any inbound connection is handled in its own
// goroutine; Start waits for those goroutines to return before it itself
// returns. The returned error is nil on clean shutdown (ctx cancelled or
// transport closed); it is a wrapped Accept error otherwise.
func (g *Gossiper) Start(ctx context.Context) error {
	for {
		conn, remote, err := g.cfg.Transport.Accept(ctx)
		if err != nil {
			// Accept returning is the signal to shut down: wait for any
			// in-flight handlers before surfacing.
			g.wg.Wait()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("gossip: accept: %w", err)
		}
		// Server-side membership gate (replaces the v0-dropped Hello).
		if !g.cfg.Roster.IsMember(remote) {
			g.logger.Warn("gossip: reject non-member",
				slog.Uint64("remote", uint64(remote)),
				slog.String("group_id", g.cfg.GroupID.String()))
			_ = conn.Close()
			continue
		}
		g.wg.Add(1)
		go func(c net.Conn, r entmoot.NodeID) {
			defer g.wg.Done()
			defer c.Close()
			g.handleConn(ctx, c, r)
		}(conn, remote)
	}
}

// handleConn reads one frame from c and dispatches on type. v0 is stateless
// per connection: exactly one request-response, then close. Errors are
// logged and the connection is dropped (hard-disconnect per the plan).
func (g *Gossiper) handleConn(ctx context.Context, c net.Conn, remote entmoot.NodeID) {
	t, payload, err := wire.ReadAndDecode(c)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		g.logger.Warn("gossip: read frame",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	switch v := payload.(type) {
	case *wire.RosterReq:
		g.onRosterReq(c, remote, v)
	case *wire.FetchReq:
		g.onFetchReq(ctx, c, remote, v)
	case *wire.MerkleReq:
		g.onMerkleReq(ctx, c, remote, v)
	case *wire.Gossip:
		g.onGossip(ctx, remote, v)
	case *wire.Hello:
		// v0 drops Hello: Pilot's tunnel already authenticates the remote.
		// The codec still recognizes the type (kept for future use), so we
		// simply log and move on.
		g.logger.Debug("gossip: ignoring hello frame (v0 skips handshake)",
			slog.Uint64("remote", uint64(remote)))
	default:
		g.logger.Warn("gossip: unexpected frame type",
			slog.Uint64("remote", uint64(remote)),
			slog.String("type", t.String()))
	}
}

// onRosterReq responds with the full roster entries. SinceHead is accepted
// but ignored (v0 responders always return the full log).
func (g *Gossiper) onRosterReq(c net.Conn, remote entmoot.NodeID, req *wire.RosterReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: roster_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	resp := &wire.RosterResp{
		GroupID: g.cfg.GroupID,
		Entries: g.cfg.Roster.Entries(),
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write roster_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onFetchReq looks up the requested id in the local store and responds with
// either the message body or NotFound=true.
func (g *Gossiper) onFetchReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.FetchReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: fetch_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	resp := &wire.FetchResp{GroupID: g.cfg.GroupID, ID: req.ID}
	msg, err := g.cfg.Store.Get(ctx, req.GroupID, req.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			resp.NotFound = true
		} else {
			g.logger.Warn("gossip: store.Get",
				slog.Uint64("remote", uint64(remote)),
				slog.String("err", err.Error()))
			resp.NotFound = true
		}
	} else {
		m := msg
		resp.Message = &m
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write fetch_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onMerkleReq responds with the current Merkle root plus the number of
// messages in the local store for this group.
func (g *Gossiper) onMerkleReq(ctx context.Context, c net.Conn, remote entmoot.NodeID, req *wire.MerkleReq) {
	if req.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: merkle_req for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", req.GroupID.String()))
		return
	}
	root, err := g.cfg.Store.MerkleRoot(ctx, req.GroupID)
	if err != nil {
		g.logger.Warn("gossip: merkle root",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	// Count via Range with the widest possible window. v0's Range uses
	// [sinceMillis, untilMillis) with untilMillis=0 meaning no upper bound.
	msgs, err := g.cfg.Store.Range(ctx, req.GroupID, 0, 0)
	if err != nil {
		g.logger.Warn("gossip: range for count",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
		return
	}
	resp := &wire.MerkleResp{
		GroupID:      g.cfg.GroupID,
		Root:         wire.MerkleRoot(root),
		MessageCount: len(msgs),
	}
	if err := wire.EncodeAndWrite(c, resp); err != nil {
		g.logger.Warn("gossip: write merkle_resp",
			slog.Uint64("remote", uint64(remote)),
			slog.String("err", err.Error()))
	}
}

// onGossip validates the sender's signature, then for each advertised id
// that we have not already seen, dials the sender back on a fresh connection
// to fetch the body. Bodies are verified against the author's roster pubkey
// before being stored.
//
// Signature-vs-roster race: if the sender is in our roster but we have not
// yet learned about the author's membership (e.g. a roster_sync is still in
// flight), FetchResp verification below will fail and the message is
// dropped. That is the desired safe behavior — we prefer false negatives
// (re-delivery on the next gossip push) over accepting an unverifiable body.
// A Phase-E integration that wants to bridge this gap would gate Start on a
// completed Join rather than running both in parallel.
func (g *Gossiper) onGossip(ctx context.Context, remote entmoot.NodeID, gos *wire.Gossip) {
	if gos.GroupID != g.cfg.GroupID {
		g.logger.Warn("gossip: gossip for wrong group",
			slog.Uint64("remote", uint64(remote)),
			slog.String("got", gos.GroupID.String()))
		return
	}
	senderInfo, ok := g.cfg.Roster.MemberInfo(remote)
	if !ok {
		g.logger.Warn("gossip: gossip from non-member",
			slog.Uint64("remote", uint64(remote)))
		return
	}
	if !verifyGossipSig(gos, senderInfo.EntmootPubKey) {
		g.logger.Warn("gossip: gossip signature invalid",
			slog.Uint64("remote", uint64(remote)))
		return
	}

	for _, id := range gos.IDs {
		has, err := g.cfg.Store.Has(ctx, g.cfg.GroupID, id)
		if err != nil {
			g.logger.Warn("gossip: store.Has",
				slog.String("err", err.Error()))
			continue
		}
		if has {
			continue
		}
		if err := g.fetchFrom(ctx, remote, id); err != nil {
			// v0: best-effort. NAT, transient errors, peer gone — we drop
			// and rely on the next gossip push to re-advertise. No retries.
			g.logger.Warn("gossip: fetch",
				slog.Uint64("remote", uint64(remote)),
				slog.String("id", id.String()),
				slog.String("err", err.Error()))
		}
	}
}

// fetchFrom opens a fresh connection to peer, sends FetchReq for id, and
// stores the response body after verifying its signature against the
// author's pubkey from the local roster. Returns an error on dial failure,
// codec failure, NotFound, or signature verification failure.
func (g *Gossiper) fetchFrom(ctx context.Context, peer entmoot.NodeID, id entmoot.MessageID) error {
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		return fmt.Errorf("dial %d: %w", peer, err)
	}
	defer conn.Close()

	req := &wire.FetchReq{GroupID: g.cfg.GroupID, ID: id}
	if err := wire.EncodeAndWrite(conn, req); err != nil {
		return fmt.Errorf("write fetch_req: %w", err)
	}
	_, payload, err := wire.ReadAndDecode(conn)
	if err != nil {
		return fmt.Errorf("read fetch_resp: %w", err)
	}
	resp, ok := payload.(*wire.FetchResp)
	if !ok {
		return fmt.Errorf("fetch: unexpected response type")
	}
	if resp.NotFound || resp.Message == nil {
		return fmt.Errorf("fetch: peer %d reports not-found for %s", peer, id)
	}
	if resp.Message.ID != id {
		return fmt.Errorf("fetch: response id mismatch")
	}
	if err := g.verifyMessage(*resp.Message); err != nil {
		return err
	}
	if err := g.cfg.Store.Put(ctx, *resp.Message); err != nil {
		return fmt.Errorf("store put: %w", err)
	}
	return nil
}

// Publish validates, stores, and gossips a new local message. The message
// must already be signed by Config.Identity (Publish re-verifies before
// storing). Fan-out errors are logged but do not fail Publish — a failed
// push is just less fan-out; the next peer that gossips us this id will
// pull the body back.
func (g *Gossiper) Publish(ctx context.Context, msg entmoot.Message) error {
	if msg.GroupID != g.cfg.GroupID {
		return fmt.Errorf("gossip: publish for wrong group %s", msg.GroupID.String())
	}
	if err := g.verifyMessage(msg); err != nil {
		return err
	}
	if err := g.cfg.Store.Put(ctx, msg); err != nil {
		return fmt.Errorf("gossip: store put: %w", err)
	}

	peers := g.getPicker().Sample(g.fanout)
	if len(peers) == 0 {
		return nil
	}

	// Build a signed Gossip frame once; reuse across fan-out targets.
	frame := &wire.Gossip{
		GroupID:   g.cfg.GroupID,
		IDs:       []entmoot.MessageID{msg.ID},
		Timestamp: g.clk.Now().UnixMilli(),
	}
	if err := signGossip(frame, g.cfg.Identity); err != nil {
		return fmt.Errorf("gossip: sign: %w", err)
	}

	for _, p := range peers {
		if err := g.pushGossip(ctx, p, frame); err != nil {
			g.logger.Warn("gossip: push",
				slog.Uint64("peer", uint64(p)),
				slog.String("id", msg.ID.String()),
				slog.String("err", err.Error()))
		}
	}
	return nil
}

// pushGossip opens a connection to peer, writes the frame, and closes. v0 is
// stateless: we never expect a response to a gossip push.
func (g *Gossiper) pushGossip(ctx context.Context, peer entmoot.NodeID, frame *wire.Gossip) error {
	conn, err := g.cfg.Transport.Dial(ctx, peer)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	if err := wire.EncodeAndWrite(conn, frame); err != nil {
		return fmt.Errorf("write gossip: %w", err)
	}
	return nil
}

// getPicker returns a cached PeerPicker, constructing it on first use. The
// picker shares the Gossiper's rand source; the source is seeded off the
// current time when Config.Clock is nil (production) and off a fixed seed
// when Fake clocks are injected (tests) so deterministic coverage stays
// possible.
func (g *Gossiper) getPicker() *PeerPicker {
	g.pickerOnce.Do(func() {
		// Seed off the clock. Fake clocks produce a stable pair for tests
		// that want determinism; the System clock produces a moving seed
		// across restarts, which is fine for gossip fan-out.
		now := g.clk.Now().UnixNano()
		src := rand.New(rand.NewPCG(uint64(now), uint64(now>>1)^0x9E3779B97F4A7C15))
		g.picker = NewPicker(g.cfg.Roster, g.cfg.LocalNode, src)
	})
	return g.picker
}

// verifyMessage checks that msg.Author is a current roster member, that
// msg.Signature verifies against the author's roster pubkey, and that
// msg.ID matches canonical.MessageID of msg (id/sig zeroed). Returns
// entmoot.ErrNotMember or entmoot.ErrSigInvalid on failure.
func (g *Gossiper) verifyMessage(msg entmoot.Message) error {
	author, ok := g.cfg.Roster.MemberInfo(msg.Author.PilotNodeID)
	if !ok {
		return fmt.Errorf("%w: author %d", entmoot.ErrNotMember, msg.Author.PilotNodeID)
	}
	// The roster's stored pubkey wins over whatever the message carries in
	// Author.EntmootPubKey: a forged message could put a valid-looking key
	// in the author slot and sign with its matching private key.
	signing := msg
	signing.ID = entmoot.MessageID{}
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return fmt.Errorf("gossip: canonical encode: %w", err)
	}
	if !keystore.Verify(author.EntmootPubKey, sigInput, msg.Signature) {
		return fmt.Errorf("%w: message %s", entmoot.ErrSigInvalid, msg.ID)
	}
	if canonical.MessageID(msg) != msg.ID {
		return fmt.Errorf("gossip: message id does not match canonical hash")
	}
	return nil
}

// signGossip canonicalizes frame with Signature zeroed, signs with id, and
// stores the resulting signature back into frame.
func signGossip(frame *wire.Gossip, id *keystore.Identity) error {
	signing := *frame
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return err
	}
	frame.Signature = id.Sign(sigInput)
	return nil
}

// verifyGossipSig verifies a Gossip frame against pubKey using the canonical
// encoding of the signing form (Signature zeroed).
func verifyGossipSig(frame *wire.Gossip, pubKey []byte) bool {
	signing := *frame
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return false
	}
	return keystore.Verify(pubKey, sigInput, frame.Signature)
}
