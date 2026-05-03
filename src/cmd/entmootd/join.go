package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
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

func localPilotHostname(socketPath, command string) (string, bool) {
	drv, err := ipcclient.Connect(socketPath)
	if err != nil {
		slog.Debug(command+": local pilot hostname lookup failed",
			slog.String("err", err.Error()))
		return "", false
	}
	defer func() { _ = drv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	info, err := drv.InfoStruct(ctx)
	if err != nil {
		slog.Debug(command+": local pilot hostname lookup failed",
			slog.String("err", err.Error()))
		return "", false
	}
	hostname, ok := normalizeLocalPilotHostname(info.Hostname)
	if !ok {
		slog.Debug(command + ": local pilot hostname unavailable")
	}
	return hostname, ok
}

func pilotDriverHasCapability(ctx context.Context, d *ipcclient.Driver, want string) bool {
	if d == nil {
		return false
	}
	infoCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	info, err := d.InfoStruct(infoCtx)
	if err != nil {
		slog.Debug("pilot capability lookup failed", slog.String("capability", want), slog.String("err", err.Error()))
		return false
	}
	for _, cap := range info.Capabilities {
		if cap == want {
			return true
		}
	}
	return false
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
	inputs := make([]joinInput, 0, len(rest))
	for _, inviteArg := range rest {
		input, err := loadJoinInput(inviteArg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "join: invite %s: %v\n", inviteArg, err)
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
		inputs = append(inputs, input)
	}

	return runGroupDaemon(gf, groupDaemonOptions{
		command:            "join",
		event:              "joined",
		advertiseEndpoints: advertiseEndpoints,
		loadGroups: func(ctx context.Context, runtime *groupRuntime, loadCtx groupDaemonLoadContext) (int, error) {
			for _, input := range inputs {
				invite := input.invite
				if input.openInvite != nil {
					redeemed, err := redeemJoinOpenInvite(ctx, input.openInvite, loadCtx)
					if err != nil {
						return classifyJoinOpenInviteError(err), fmt.Errorf("redeem open invite %s: %w", input.source, err)
					}
					invite = redeemed
				}
				if invite == nil {
					return exitInvalidArgument, fmt.Errorf("invite %s: no signed invite produced", input.source)
				}
				if _, _, err := runtime.AddInvite(ctx, *invite); err != nil {
					code := classifyJoinAddInviteError(err)
					if code == exitInvalidArgument {
						return code, err
					}
					return code, fmt.Errorf("add group %s: %w", invite.GroupID.String(), err)
				}
			}
			return exitOK, nil
		},
	})
}

func classifyJoinOpenInviteError(err error) int {
	var opErr *esphttp.OperationError
	if errors.As(err, &opErr) {
		switch {
		case opErr.HTTPStatus == http.StatusBadRequest,
			opErr.HTTPStatus == http.StatusNotFound,
			opErr.HTTPStatus == http.StatusConflict:
			return exitInvalidArgument
		default:
			return exitTransport
		}
	}
	return exitTransport
}

func classifyJoinAddInviteError(err error) int {
	switch {
	case errors.Is(err, entmoot.ErrInviteExpired), errors.Is(err, entmoot.ErrSigInvalid):
		return exitInvalidArgument
	case errors.Is(err, errLocalGroupNotMember), errors.Is(err, errLocalGroupIdentityMismatch):
		return exitNotMember
	default:
		return exitTransport
	}
}

type groupDaemonOptions struct {
	command            string
	event              string
	advertiseEndpoints endpointFlag
	loadGroups         func(context.Context, *groupRuntime, groupDaemonLoadContext) (int, error)
}

type groupDaemonLoadContext struct {
	identity        *keystore.Identity
	pilot           pilotInfoSigner
	pilotSocketPath string
}

type pilotInfoSigner interface {
	InfoStruct(context.Context) (ipcclient.Info, error)
	SignChallenge(context.Context, []byte) (ipcclient.ChallengeSignature, error)
}

const pilotCapabilityLookupNode = "lookup_node"

var errPilotLookupUnavailable = errors.New("pilot identity lookup unavailable")

func runGroupDaemon(gf *globalFlags, opts groupDaemonOptions) int {
	if opts.command == "" {
		opts.command = "daemon"
	}
	if opts.event == "" {
		opts.event = "started"
	}
	s, err := setup(gf)
	if err != nil {
		slog.Error(opts.command+": setup", slog.String("err", err.Error()))
		return exitTransport
	}

	// Early-check the control socket: if another daemon is already live,
	// exit 6 before we touch Pilot.
	sockPath := controlSocketPath(s.dataDir)
	if controlSocketAlive(sockPath, 200*time.Millisecond) {
		fmt.Fprintf(os.Stderr, "%s: another entmoot daemon is already running at %s\n", opts.command, sockPath)
		return exitControlUnavail
	}
	// Stale socket left behind by a previous crash: unlink so we can
	// bind. If a process is listening we'd have taken the branch above.
	if _, err := os.Stat(sockPath); err == nil {
		if err := os.Remove(sockPath); err != nil {
			fmt.Fprintf(os.Stderr, "%s: remove stale socket: %v\n", opts.command, err)
			return exitTransport
		}
	}

	tr, err := openPilotForJoin(gf)
	if err != nil {
		slog.Error(opts.command+": pilot", slog.String("err", err.Error()))
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
		slog.Error(opts.command+": open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = rawStore.Close() }()

	// Wrap the store so IPC tail subscribers and service integrations see
	// new messages as they land. The gossip/publish path writes through this
	// wrapper.
	serviceEvents := events.NewBus()
	notifyStore := newNotifyingStore(rawStore, serviceEvents)

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
	cliEps := opts.advertiseEndpoints.Snapshot()
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
	localHostnameFn := func() (string, bool) {
		return localPilotHostname(gf.socket, opts.command)
	}
	// Background polling runs for the lifetime of the daemon.
	go turnPoller.Run(rootCtx)
	pilotLookupNodeSupported := pilotDriverHasCapability(rootCtx, tr.Driver(), pilotCapabilityLookupNode)
	if !pilotLookupNodeSupported {
		slog.Warn(opts.command + ": pilot-daemon does not advertise lookup_node; ESP invite creation and open-invite redemption are unavailable until Pilot is upgraded")
	}

	runtime, err := newGroupRuntime(groupRuntimeConfig{
		NodeID:           nodeID,
		Identity:         s.identity,
		DataDir:          s.dataDir,
		Store:            rawStore,
		Notify:           notifyStore,
		Transport:        tr,
		PilotDriver:      tr.Driver(),
		LocalEndpoints:   localEndpointsFn,
		LocalHostname:    localHostnameFn,
		EndpointsChanged: turnPoller.Changed(),
		HideIP:           gf.hideIP,
		TraceReconcile:   gf.traceReconcile,
		Logger:           slog.Default(),
	})
	if err != nil {
		slog.Error(opts.command+": new group runtime", slog.String("err", err.Error()))
		return exitTransport
	}

	if opts.loadGroups == nil {
		runtime.Close()
		fmt.Fprintf(os.Stderr, "%s: no group loader configured\n", opts.command)
		return exitInvalidArgument
	}
	if code, err := opts.loadGroups(rootCtx, runtime, groupDaemonLoadContext{
		identity:        s.identity,
		pilot:           tr.Driver(),
		pilotSocketPath: gf.socket,
	}); err != nil {
		if code == exitInvalidArgument || code == exitNotMember || code == exitGroupNotFound {
			fmt.Fprintf(os.Stderr, "%s: %v\n", opts.command, err)
		} else {
			slog.Error(opts.command+": load groups", slog.String("err", err.Error()))
		}
		runtime.Close()
		return code
	}
	if runtime.Count() == 0 {
		runtime.Close()
		fmt.Fprintf(os.Stderr, "%s: no active groups\n", opts.command)
		return exitGroupNotFound
	}

	// Bind the control socket with 0600 permissions. net.Listen uses
	// the process umask, so explicitly chmod afterwards.
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		slog.Error(opts.command+": listen control socket", slog.String("err", err.Error()))
		runtime.Close()
		return exitTransport
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		slog.Warn(opts.command+": chmod control socket", slog.String("err", err.Error()))
	}
	// Ensure the socket file is removed on every return path, even
	// panics / Close errors.
	removeSocket := func() {
		if err := os.Remove(sockPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn(opts.command+": remove control socket", slog.String("err", err.Error()))
		}
	}

	// Start the shared transport demux and the IPC accept loop in separate
	// goroutines. Each group session has already started its own gossiper
	// workers inside runtime.AddInvite.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runtime.Start(rootCtx); err != nil {
			slog.Warn(opts.command+": group runtime stopped", slog.String("err", err.Error()))
		}
	}()

	// IPC server state: shared across handlers via closure.
	srv := &ipcServer{
		nodeID:                   nodeID,
		identity:                 s.identity,
		dataDir:                  s.dataDir,
		listenPort:               uint16(gf.listenPort),
		runtime:                  runtime,
		store:                    rawStore,
		notify:                   notifyStore,
		pilot:                    tr.Driver(),
		pilotLookupNodeSupported: pilotLookupNodeSupported,
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
	groups := runtime.ActiveGroupIDs()
	members := 0
	for _, gid := range groups {
		if sess, ok := runtime.Get(gid); ok {
			members += len(sess.roster.Members())
		}
	}
	joinedEvent := map[string]any{
		"event":          opts.event,
		"group_id":       groups[0],
		"group_ids":      groups,
		"members":        members,
		"listen_port":    gf.listenPort,
		"control_socket": sockPath,
	}
	if data, err := json.Marshal(joinedEvent); err == nil {
		fmt.Println(string(data))
	}

	// Block until a signal fires and all goroutines unwind.
	<-rootCtx.Done()
	runtime.Close()
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

type joinInput struct {
	source     string
	invite     *entmoot.Invite
	openInvite *openInviteAcceptPayload
}

// loadJoinInput reads a join input from arg (file path, http(s) URL, or
// entmoot://open-invite link) and classifies it as either a signed invite or an
// open invite descriptor that must be redeemed after Pilot startup.
func loadJoinInput(arg string) (joinInput, error) {
	if payload, ok, err := parseOpenInviteLinkArg(arg); ok || err != nil {
		if err != nil {
			return joinInput{}, err
		}
		return joinInput{source: arg, openInvite: payload}, nil
	}
	raw, err := readJoinInputBytes(arg)
	if err != nil {
		return joinInput{}, err
	}
	if payload, ok, err := parseOpenInviteDescriptor(raw); ok || err != nil {
		if err != nil {
			return joinInput{}, err
		}
		return joinInput{source: arg, openInvite: payload}, nil
	}
	invite, err := parseSignedInvite(raw)
	if err != nil {
		return joinInput{}, err
	}
	return joinInput{source: arg, invite: invite}, nil
}

// loadInvite reads a signed invite JSON bundle from arg. Kept as a small
// compatibility helper for tests and older internal call sites; cmdJoin uses
// loadJoinInput so app-generated open invites can be redeemed automatically.
func loadInvite(arg string) (*entmoot.Invite, error) {
	input, err := loadJoinInput(arg)
	if err != nil {
		return nil, err
	}
	if input.invite == nil {
		return nil, fmt.Errorf("%w: open invite descriptor requires redemption", errInviteMalformed)
	}
	return input.invite, nil
}

// readJoinInputBytes reads a join input JSON bundle from arg (file path or
// http(s) URL). Signed-invite structural checks happen after classification.
func readJoinInputBytes(arg string) ([]byte, error) {
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
		return b, nil
	} else {
		b, err := os.ReadFile(arg)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && looksLikeOpenInviteToken(arg) {
				return nil, fmt.Errorf("%w: raw open invite token is not enough; provide the app descriptor JSON or entmoot://open-invite link with issuer and token", errInviteMalformed)
			}
			return nil, fmt.Errorf("%w: read %s: %v", errInviteMalformed, arg, err)
		}
		return b, nil
	}
}

func parseSignedInvite(raw []byte) (*entmoot.Invite, error) {
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

func parseOpenInviteDescriptor(raw []byte) (*openInviteAcceptPayload, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, false, nil
	}
	if !hasJSONField(fields, "issuer_url") && !hasJSONField(fields, "token") && !hasJSONField(fields, "link") {
		return nil, false, nil
	}
	var desc struct {
		IssuerURL string `json:"issuer_url"`
		Token     string `json:"token"`
		Link      string `json:"link"`
	}
	if err := json.Unmarshal(raw, &desc); err != nil {
		return nil, true, fmt.Errorf("%w: open invite descriptor: %v", errInviteMalformed, err)
	}
	var out *openInviteAcceptPayload
	if strings.TrimSpace(desc.IssuerURL) != "" || strings.TrimSpace(desc.Token) != "" {
		payload, err := normalizeOpenInviteAcceptPayload(openInviteAcceptPayload{IssuerURL: desc.IssuerURL, Token: desc.Token})
		if err != nil {
			return nil, true, fmt.Errorf("%w: open invite descriptor: %v", errInviteMalformed, err)
		}
		out = payload
	}
	if strings.TrimSpace(desc.Link) != "" {
		payload, err := parseOpenInviteLink(desc.Link)
		if err != nil {
			return nil, true, fmt.Errorf("%w: open invite link: %v", errInviteMalformed, err)
		}
		if out != nil && (out.IssuerURL != payload.IssuerURL || out.Token != payload.Token) {
			return nil, true, fmt.Errorf("%w: open invite link does not match issuer_url/token fields", errInviteMalformed)
		}
		out = payload
	}
	if out == nil {
		return nil, true, fmt.Errorf("%w: open invite descriptor requires issuer_url and token", errInviteMalformed)
	}
	return out, true, nil
}

func parseOpenInviteLinkArg(arg string) (*openInviteAcceptPayload, bool, error) {
	if !strings.HasPrefix(strings.TrimSpace(arg), "entmoot:") {
		return nil, false, nil
	}
	payload, err := parseOpenInviteLink(arg)
	return payload, true, err
}

func parseOpenInviteLink(raw string) (*openInviteAcceptPayload, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	if u.Scheme != "entmoot" {
		return nil, fmt.Errorf("expected entmoot://open-invite link")
	}
	linkKind := strings.Trim(strings.TrimSpace(u.Host+u.Path), "/")
	if linkKind != "open-invite" {
		return nil, fmt.Errorf("unsupported entmoot link %q", linkKind)
	}
	q := u.Query()
	return normalizeOpenInviteAcceptPayload(openInviteAcceptPayload{
		IssuerURL: q.Get("issuer"),
		Token:     q.Get("token"),
	})
}

func normalizeOpenInviteAcceptPayload(payload openInviteAcceptPayload) (*openInviteAcceptPayload, error) {
	issuer, token, err := parseOpenInviteAcceptPayload(payload)
	if err != nil {
		return nil, err
	}
	return &openInviteAcceptPayload{IssuerURL: issuer.String(), Token: token}, nil
}

func hasJSONField(fields map[string]json.RawMessage, name string) bool {
	_, ok := fields[name]
	return ok
}

func looksLikeOpenInviteToken(arg string) bool {
	if len(arg) < 32 || strings.ContainsAny(arg, `/\.`) {
		return false
	}
	for _, r := range arg {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func redeemJoinOpenInvite(ctx context.Context, payload *openInviteAcceptPayload, loadCtx groupDaemonLoadContext) (*entmoot.Invite, error) {
	if payload == nil {
		return nil, fmt.Errorf("%w: open invite payload is missing", errInviteMalformed)
	}
	issuer, token, err := parseOpenInviteAcceptPayload(*payload)
	if err != nil {
		return nil, err
	}
	exec := espOperationExecutor{
		identity:        loadCtx.identity,
		pilotSocketPath: loadCtx.pilotSocketPath,
		timeout:         30 * time.Second,
	}
	if loadCtx.pilot != nil {
		exec.pilotIdentity = func(ctx context.Context) (entmoot.NodeID, string, error) {
			infoCtx, cancel := context.WithTimeout(ctx, exec.timeout)
			defer cancel()
			info, err := loadCtx.pilot.InfoStruct(infoCtx)
			if err != nil {
				return 0, "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot identity is unavailable: " + err.Error()}
			}
			if info.NodeID == 0 || strings.TrimSpace(info.PublicKey) == "" {
				return 0, "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot identity is incomplete"}
			}
			return entmoot.NodeID(info.NodeID), strings.TrimSpace(info.PublicKey), nil
		}
		exec.pilotSignChallenge = func(ctx context.Context, challenge []byte) (string, error) {
			signCtx, cancel := context.WithTimeout(ctx, exec.timeout)
			defer cancel()
			sig, err := loadCtx.pilot.SignChallenge(signCtx, challenge)
			if err != nil {
				return "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot challenge signing failed: " + err.Error()}
			}
			if strings.TrimSpace(sig.Signature) == "" {
				return "", &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "pilot_unavailable", Message: "local Pilot challenge signature is empty"}
			}
			return strings.TrimSpace(sig.Signature), nil
		}
	}
	invite, _, err := exec.redeemOpenInviteFromIssuer(ctx, issuer, token)
	if err != nil {
		return nil, err
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
	sink  events.Sink

	mu     sync.Mutex
	subs   map[int]chan<- entmoot.Message
	nextID int
}

func newNotifyingStore(inner store.MessageStore, sink events.Sink) *notifyingStore {
	if sink == nil {
		sink = events.NopSink{}
	}
	return &notifyingStore{
		inner: inner,
		sink:  sink,
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
	n.sink.Emit(events.Event{
		Type:      events.TypeMessageIngested,
		GroupID:   m.GroupID,
		MessageID: m.ID,
		PeerID:    m.Author.PilotNodeID,
		At:        time.Now(),
	})
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
func (n *notifyingStore) Latest(ctx context.Context, gid entmoot.GroupID, limit int) ([]entmoot.Message, error) {
	return n.inner.Latest(ctx, gid, limit)
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
	nodeID                   entmoot.NodeID
	identity                 *keystore.Identity
	dataDir                  string
	listenPort               uint16
	runtime                  *groupRuntime
	store                    *store.SQLite
	notify                   *notifyingStore
	pilot                    *ipcclient.Driver
	pilotLookupNodeSupported bool
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
	case *ipc.SignedPublishReq:
		s.handleSignedPublish(ctx, c, v)
	case *ipc.JoinGroupReq:
		s.handleJoinGroup(ctx, c, v)
	case *ipc.InviteCreateReq:
		s.handleInviteCreate(ctx, c, v)
	case *ipc.InviteAuthorityCheckReq:
		s.handleInviteAuthorityCheck(ctx, c, v)
	case *ipc.MemberRemoveReq:
		s.handleMemberRemove(ctx, c, v)
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

// handleSignedPublish accepts a message whose author already signed it. This
// is the ESP/mobile write path: the daemon owns durable storage and gossip
// fanout, but does not hold the author's signing key.
func (s *ipcServer) handleSignedPublish(ctx context.Context, c net.Conn, req *ipc.SignedPublishReq) {
	msg := req.Message
	gid := msg.GroupID
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group not joined",
		})
		return
	}
	if err := sess.gossip.Publish(ctx, msg); err != nil {
		code := ipc.CodeInternal
		switch {
		case errors.Is(err, entmoot.ErrNotMember):
			code = ipc.CodeNotMember
		case errors.Is(err, entmoot.ErrSigInvalid):
			code = ipc.CodeInvalidArgument
		}
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    code,
			GroupID: &gid,
			Message: "signed publish: " + err.Error(),
		})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.SignedPublishResp{
		Status:      "accepted",
		MessageID:   msg.ID,
		GroupID:     gid,
		Author:      msg.Author.PilotNodeID,
		TimestampMS: msg.Timestamp,
	})
}

// handlePublish resolves the target group, signs the supplied Topics+
// Content as a full Message, and gossips it. Emits PublishResp on
// success or a structured ErrorFrame on failure.
func (s *ipcServer) handlePublish(ctx context.Context, c net.Conn, req *ipc.PublishReq) {
	gid, ok := s.resolvePublishGroup(c, req.GroupID)
	if !ok {
		return
	}
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
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
	if !sess.roster.IsMember(s.nodeID) {
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
	if info, ok := sess.roster.MemberInfo(s.nodeID); ok {
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

	signer, err := signing.NewLocalSigner(author, s.identity)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "local signer: " + err.Error(),
		})
		return
	}
	msg, err = signing.SignMessage(ctx, signer, msg)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "sign message: " + err.Error(),
		})
		return
	}

	if err := sess.gossip.Publish(ctx, msg); err != nil {
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

func (s *ipcServer) resolvePublishGroup(c net.Conn, requested *entmoot.GroupID) (entmoot.GroupID, bool) {
	if requested != nil {
		return *requested, true
	}
	gid, ok := s.runtime.SingleGroup()
	if ok {
		return gid, true
	}
	code := ipc.CodeInvalidArgument
	message := "group_id is required when multiple groups are active"
	if s.runtime.Count() == 0 {
		code = ipc.CodeGroupNotFound
		message = "no active groups"
	}
	_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
		Type:    "error",
		Code:    code,
		Message: message,
	})
	return entmoot.GroupID{}, false
}

func (s *ipcServer) handleJoinGroup(ctx context.Context, c net.Conn, req *ipc.JoinGroupReq) {
	sess, created, err := s.runtime.AddInvite(ctx, req.Invite)
	if err != nil {
		code := ipc.CodeInternal
		if errors.Is(err, entmoot.ErrInviteExpired) || errors.Is(err, entmoot.ErrSigInvalid) {
			code = ipc.CodeInvalidArgument
		}
		gid := req.Invite.GroupID
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    code,
			GroupID: &gid,
			Message: "join group: " + err.Error(),
		})
		return
	}
	status := "already_joined"
	if created {
		status = "joined"
	}
	_ = ipc.EncodeAndWrite(c, &ipc.JoinGroupResp{
		Status:  status,
		GroupID: sess.groupID,
		Members: len(sess.roster.Members()),
	})
}

func (s *ipcServer) handleInviteCreate(ctx context.Context, c net.Conn, req *ipc.InviteCreateReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: "invite_create requires group_id",
		})
		return
	}
	if req.Target.PilotNodeID == 0 || len(req.Target.EntmootPubKey) != ed25519.PublicKeySize {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			GroupID: &gid,
			Message: "invite_create requires target agent identity",
		})
		return
	}
	if inviteCreateNeedsPilotLookup(req) && !s.pilotLookupNodeSupported {
		_ = ipc.EncodeAndWrite(c, s.pilotLookupUnavailableError(gid))
		return
	}
	if err := s.verifyTargetPilotIdentity(ctx, req); err != nil {
		code := ipc.CodeInvalidArgument
		if errors.Is(err, errPilotLookupUnavailable) {
			code = ipc.CodeUnavailable
		}
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    code,
			GroupID: &gid,
			Message: err.Error(),
		})
		return
	}
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group not joined",
		})
		return
	}
	founder, ok := sess.roster.Founder()
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group has no founder",
		})
		return
	}
	if founder.PilotNodeID != s.nodeID || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: "invite_create requires the local founder identity",
		})
		return
	}
	root, err := s.store.MerkleRoot(ctx, gid)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			GroupID: &gid,
			Message: "merkle root: " + err.Error(),
		})
		return
	}

	explicitPeers := append([]entmoot.NodeID(nil), req.BootstrapPeers...)
	var peers []entmoot.NodeID
	var fanoutPeers []entmoot.NodeID
	added := false
	unlock := lockESPInviteRoster(gid)
	if len(explicitPeers) > 0 {
		peers = explicitPeers
	} else {
		peers = defaultBootstrapPeers(sess.roster, founder.PilotNodeID, 5)
	}
	if existing, ok := sess.roster.MemberInfo(req.Target.PilotNodeID); ok {
		if !bytes.Equal(existing.EntmootPubKey, req.Target.EntmootPubKey) {
			unlock()
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
				Type:    "error",
				Code:    ipc.CodeConflict,
				GroupID: &gid,
				Message: "target node already exists with a different Entmoot pubkey",
			})
			return
		}
	} else if err := applyFounderRosterAdd(s.identity, sess.roster, founder, req.Target); err != nil {
		unlock()
		code := ipc.CodeInternal
		if errors.Is(err, entmoot.ErrRosterReject) {
			code = ipc.CodeInvalidArgument
		}
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    code,
			GroupID: &gid,
			Message: err.Error(),
		})
		return
	} else {
		added = true
		fanoutPeers = sess.roster.Members()
	}
	unlock()
	if added {
		sess.gossip.FanoutRoster(ctx, fanoutPeers, req.Target.PilotNodeID)
	}
	bootstrap := make([]entmoot.BootstrapPeer, 0, len(peers))
	for _, p := range peers {
		if p == req.Target.PilotNodeID {
			continue
		}
		bootstrap = append(bootstrap, entmoot.BootstrapPeer{NodeID: p})
	}
	issuedAt := time.Now().UnixMilli()
	validUntil := issuedAt + (24 * time.Hour).Milliseconds()
	if req.ValidForMS > 0 {
		validUntil = issuedAt + req.ValidForMS
	}
	if req.ValidUntilMS > 0 {
		validUntil = req.ValidUntilMS
	}
	invite := entmoot.Invite{
		GroupID:        gid,
		Founder:        founder,
		RosterHead:     sess.roster.Head(),
		MerkleRoot:     root,
		BootstrapPeers: bootstrap,
		IssuedAt:       issuedAt,
		ValidUntil:     validUntil,
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   s.nodeID,
			EntmootPubKey: append([]byte(nil), s.identity.PublicKey...),
		},
	}
	if err := signInvite(s.identity, &invite); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			GroupID: &gid,
			Message: "sign invite: " + err.Error(),
		})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.InviteCreateResp{
		Status:     "created",
		GroupID:    gid,
		Invite:     invite,
		RosterHead: sess.roster.Head(),
		Members:    len(sess.roster.Members()),
	})
}

func (s *ipcServer) handleInviteAuthorityCheck(ctx context.Context, c net.Conn, req *ipc.InviteAuthorityCheckReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: "invite_authority_check requires group_id",
		})
		return
	}
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group not joined",
		})
		return
	}
	founder, ok := sess.roster.Founder()
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group has no founder",
		})
		return
	}
	if founder.PilotNodeID != s.nodeID || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: "invite_create requires the local founder identity",
		})
		return
	}
	if _, err := s.store.MerkleRoot(ctx, gid); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			GroupID: &gid,
			Message: "merkle root: " + err.Error(),
		})
		return
	}
	if !s.pilotLookupNodeSupported {
		_ = ipc.EncodeAndWrite(c, s.pilotLookupUnavailableError(gid))
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.InviteAuthorityCheckResp{
		Status:     "ok",
		GroupID:    gid,
		RosterHead: sess.roster.Head(),
		Members:    len(sess.roster.Members()),
	})
}

func inviteCreateNeedsPilotLookup(req *ipc.InviteCreateReq) bool {
	if req == nil {
		return false
	}
	return req.RequirePilotIdentity || req.RequirePilotProof ||
		len(req.TargetPilotPubKey) > 0 ||
		len(req.TargetPilotProof) > 0 ||
		len(req.TargetPilotSignature) > 0
}

func (s *ipcServer) pilotLookupUnavailableError(gid entmoot.GroupID) *ipc.ErrorFrame {
	return &ipc.ErrorFrame{
		Type:    "error",
		Code:    ipc.CodeUnavailable,
		GroupID: &gid,
		Message: "pilot-daemon does not advertise lookup_node; upgrade Pilot to create invites",
	}
}

func (s *ipcServer) verifyTargetPilotIdentity(ctx context.Context, req *ipc.InviteCreateReq) error {
	if len(req.TargetPilotPubKey) == 0 {
		if req.RequirePilotIdentity {
			return fmt.Errorf("target pilot_pubkey is required")
		}
		return nil
	}
	if len(req.TargetPilotPubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("target pilot_pubkey must be 32 bytes")
	}
	if s.pilot == nil {
		return errPilotLookupUnavailable
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	got, err := s.pilot.LookupNode(lookupCtx, uint32(req.Target.PilotNodeID))
	if err != nil {
		return fmt.Errorf("%w: %v", errPilotLookupUnavailable, err)
	}
	if got.NodeID != uint32(req.Target.PilotNodeID) {
		return fmt.Errorf("pilot identity mismatch: lookup returned node_id %d", got.NodeID)
	}
	if got.PublicKey == "" {
		return fmt.Errorf("pilot identity lookup returned no public key")
	}
	want := base64.StdEncoding.EncodeToString(req.TargetPilotPubKey)
	if got.PublicKey != want {
		return fmt.Errorf("pilot identity mismatch for node %d", req.Target.PilotNodeID)
	}
	if req.RequirePilotProof || len(req.TargetPilotProof) > 0 || len(req.TargetPilotSignature) > 0 {
		if len(req.TargetPilotProof) == 0 {
			return fmt.Errorf("target pilot proof is required")
		}
		if len(req.TargetPilotSignature) != ed25519.SignatureSize {
			return fmt.Errorf("target pilot signature must be 64 bytes")
		}
		if !ed25519.Verify(ed25519.PublicKey(req.TargetPilotPubKey), pilotChallengeSigningBytes(req.TargetPilotProof), req.TargetPilotSignature) {
			return fmt.Errorf("target pilot proof signature does not verify")
		}
	}
	return nil
}

func (s *ipcServer) handleMemberRemove(ctx context.Context, c net.Conn, req *ipc.MemberRemoveReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "member_remove requires group_id"})
		return
	}
	if req.Target.PilotNodeID == 0 || len(req.Target.EntmootPubKey) != ed25519.PublicKeySize {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "member_remove requires target agent identity"})
		return
	}
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	founder, ok := sess.roster.Founder()
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group has no founder"})
		return
	}
	if founder.PilotNodeID != s.nodeID || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "member_remove requires the local founder identity"})
		return
	}
	if req.Target.PilotNodeID == founder.PilotNodeID {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "cannot remove group founder"})
		return
	}
	unlock := lockESPInviteRoster(gid)
	existing, ok := sess.roster.MemberInfo(req.Target.PilotNodeID)
	if !ok {
		unlock()
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "target is not a member"})
		return
	}
	if !bytes.Equal(existing.EntmootPubKey, req.Target.EntmootPubKey) {
		unlock()
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeConflict, GroupID: &gid, Message: "target identity does not match current roster"})
		return
	}
	if err := applyFounderRosterRemove(s.identity, sess.roster, founder, existing); err != nil {
		unlock()
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: err.Error()})
		return
	}
	fanoutPeers := append(sess.roster.Members(), req.Target.PilotNodeID)
	head := sess.roster.Head()
	members := len(sess.roster.Members())
	unlock()
	sess.gossip.FanoutRoster(ctx, fanoutPeers, 0)
	_ = ipc.EncodeAndWrite(c, &ipc.MemberRemoveResp{Status: "removed", GroupID: gid, RosterHead: head, Members: members})
}

// handleInfo assembles a full InfoResp snapshot from live state.
func (s *ipcServer) handleInfo(ctx context.Context, c net.Conn) {
	pub := append([]byte(nil), s.identity.PublicKey...)
	if gid, ok := s.runtime.SingleGroup(); ok {
		if sess, ok := s.runtime.Get(gid); ok {
			pub, _ = pubkeyFromRoster(sess.roster, s.nodeID, s.identity.PublicKey)
		}
	}

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
		if sess, ok := s.runtime.Get(gid); ok {
			members = len(sess.roster.Members())
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
	if sub.GroupID != nil {
		if _, ok := s.runtime.Get(*sub.GroupID); !ok {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
				Type:    "error",
				Code:    ipc.CodeGroupNotFound,
				GroupID: sub.GroupID,
				Message: "group not joined",
			})
			return
		}
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

func normalizeLocalPilotHostname(hostname string) (string, bool) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", false
	}
	return hostname, true
}
