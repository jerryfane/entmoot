package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	libpeer "github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/defaultmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/events"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/topic"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

const defaultJoinTimeout = 90 * time.Second

func cmdJoin(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	serveAfterJoin := fs.Bool("serve", false, "after joining, keep running as the Entmoot daemon")
	timeout := fs.Duration("timeout", defaultJoinTimeout, "deadline per capability redeemed")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "join: missing target-bound bootstrap capability")
		return exitInvalidArgument
	}
	inputs, code := loadJoinInputs(fs.Args())
	if code != exitOK {
		return code
	}
	sockPath := controlSocketPath(gf.data)
	if controlSocketAlive(sockPath, 200*time.Millisecond) {
		fmt.Fprintln(os.Stderr, "join: stop the running daemon before joining a new group")
		return exitControlUnavail
	}
	// A brand-new node has no way to find a relay, so adopt the ones the
	// inviter uses when the operator named none. Explicit -controlled-relay
	// flags always win.
	adoptedRelays := []string(nil)
	if len(gf.controlledRelays) == 0 {
		for _, input := range inputs {
			if input.capability == nil {
				continue
			}
			adoptedRelays = append(adoptedRelays, input.capability.Relays...)
		}
		adoptedRelays, _ = validateRelayHints(adoptedRelays)
		if len(adoptedRelays) > 0 {
			gf.controlledRelays = append(gf.controlledRelays, adoptedRelays...)
			fmt.Fprintf(os.Stderr, "join: adopting %d relay(s) named by the invite:\n", len(adoptedRelays))
			for _, relay := range adoptedRelays {
				fmt.Fprintf(os.Stderr, "  %s\n", relay)
			}
		}
	}
	return runGroupDaemon(gf, groupDaemonOptions{
		command:       "join",
		event:         "joined",
		exitAfterLoad: !*serveAfterJoin,
		loadGroups: func(ctx context.Context, runtime *groupRuntime, loadCtx groupDaemonLoadContext) (int, error) {
			for _, input := range inputs {
				capability, code, err := resolveJoinInput(ctx, input, loadCtx)
				if err != nil {
					return code, err
				}
				joinCtx, cancel := context.WithTimeout(ctx, *timeout)
				_, _, err = runtime.AddCapability(joinCtx, *capability)
				cancel()
				if err != nil {
					return exitTransport, fmt.Errorf("join group %s: %w", capability.GroupID.String(), err)
				}
				if err := persistJoinGroupMetadata(ctx, loadCtx.metadataStore, capability.GroupID, input.groupMetadata); err != nil {
					return exitTransport, fmt.Errorf("persist group metadata %s: %w", capability.GroupID.String(), err)
				}
				// Remember the adopted relays, or a restart of `serve` would
				// come back with no way to be reached.
				if len(adoptedRelays) > 0 {
					if err := saveRelayHints(gf.data, adoptedRelays); err != nil {
						slog.Warn("join: persist adopted relays", slog.String("err", err.Error()))
					}
				}
			}
			return exitOK, nil
		},
	})
}

func loadJoinInputs(args []string) ([]joinInput, int) {
	inputs := make([]joinInput, 0, len(args))
	for _, inviteArg := range args {
		input, err := loadJoinInput(inviteArg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "join: invite %s: %v\n", inviteArg, err)
			// A local-parse failure (bad file, bad JSON, expired ValidUntil)
			// is INVALID_ARGUMENT, registered in CLI_DESIGN §5.4 and mapped to an
			// exit code by §6. A network-fetch
			// failure is a transport error (exit 1).
			if errors.Is(err, errFetchFailed) {
				return nil, exitTransport
			}
			return nil, exitInvalidArgument
		}
		inputs = append(inputs, input)
	}
	return inputs, exitOK
}

func resolveJoinInput(ctx context.Context, input joinInput, loadCtx groupDaemonLoadContext) (*entmoot.BootstrapCapability, int, error) {
	capability := input.capability
	if input.openInvite != nil {
		redeemed, err := redeemJoinOpenInvite(ctx, input.openInvite, loadCtx)
		if err != nil {
			return nil, classifyJoinOpenInviteError(err), fmt.Errorf("redeem open invite %s: %w", input.source, err)
		}
		capability = redeemed
	}
	if capability == nil {
		return nil, exitInvalidArgument, fmt.Errorf("invite %s: legacy signed invites are not accepted; use a bootstrap capability", input.source)
	}
	if input.expectedGroup != nil && capability.GroupID != *input.expectedGroup {
		return nil, exitInvalidArgument, fmt.Errorf("invite %s: group does not match signed descriptor", input.source)
	}
	if input.expectedIssuer != nil && !nodeInfoEqual(capability.Founder, *input.expectedIssuer) {
		return nil, exitInvalidArgument, fmt.Errorf("invite %s: founder does not match signed descriptor", input.source)
	}
	return capability, exitOK, nil
}

func joinGroupReqOverIPC(ctx context.Context, sockPath string, req *ipc.JoinGroupReq, timeout time.Duration) (*ipc.JoinGroupResp, *ipc.ErrorFrame, error) {
	if timeout <= 0 {
		timeout = defaultJoinTimeout
	}
	req.TimeoutMS = timeout.Milliseconds()
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", sockPath)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(joinIPCResponseTimeout(timeout))); err != nil {
		return nil, nil, err
	}
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		return nil, nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, nil, err
	}
	switch v := payload.(type) {
	case *ipc.JoinGroupResp:
		return v, nil, nil
	case *ipc.ErrorFrame:
		return nil, v, nil
	default:
		return nil, nil, fmt.Errorf("unexpected join response %T", payload)
	}
}

func joinIPCResponseTimeout(bootstrapTimeout time.Duration) time.Duration {
	if bootstrapTimeout <= 0 {
		bootstrapTimeout = defaultJoinTimeout
	}
	margin := bootstrapTimeout / 10
	if margin < 5*time.Second {
		margin = 5 * time.Second
	}
	if margin > 30*time.Second {
		margin = 30 * time.Second
	}
	return bootstrapTimeout + margin
}

func classifyJoinOpenInviteError(err error) int {
	if errors.Is(err, errInviteMalformed) {
		return exitInvalidArgument
	}
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

type groupDaemonOptions struct {
	command       string
	event         string
	exitAfterLoad bool
	loadGroups    func(context.Context, *groupRuntime, groupDaemonLoadContext) (int, error)
}

type groupDaemonLoadContext struct {
	identity      *keystore.Identity
	metadataStore esphttp.GroupMetadataStore
}

func daemonHostConfig(gf *globalFlags) (libp2ptransport.HostConfig, error) {
	config := libp2ptransport.HostConfig{Mode: libp2ptransport.DirectConnectivity}
	configured := gf.controlledRelays
	if len(configured) == 0 {
		// Relays adopted from an invite have to survive a restart, otherwise a
		// NATed node comes back unreachable and nobody can tell it a relay.
		stored, err := loadRelayHints(gf.data)
		if err != nil {
			slog.Warn("daemon: read adopted relays", slog.String("err", err.Error()))
		} else if len(stored) > 0 {
			configured = stored
			slog.Info("daemon: using relays adopted from an invite", slog.Int("relays", len(stored)))
		}
	}
	relays, err := parseControlledRelays(configured)
	if err != nil {
		return libp2ptransport.HostConfig{}, err
	}
	config.ControlledRelays = relays
	service, err := daemonRelayService(gf)
	if err != nil {
		return libp2ptransport.HostConfig{}, err
	}
	config.RelayService = service
	switch gf.connectivity {
	case "", "direct":
		config.ListenAddrs = []string{fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", gf.listenPort)}
	case "relay-only":
		config.Mode = libp2ptransport.RelayOnlyConnectivity
		if len(config.ControlledRelays) == 0 {
			return libp2ptransport.HostConfig{}, errors.New("relay-only connectivity requires at least one -controlled-relay")
		}
	default:
		return libp2ptransport.HostConfig{}, fmt.Errorf("unsupported connectivity profile %q", gf.connectivity)
	}
	return config, nil
}

// daemonRelayService reads -relay-service and its allowlist. The caps match
// `relay serve`'s defaults, because the service is the same one: this only
// decides whether it runs on the daemon's host instead of its own.
//
// Two things it deliberately does not do. It does not invent an allowlist -
// an open relay is a service for strangers, and the flag exists so an
// operator can relay for their OWN peers. And it does not touch the announced
// address list, so the addresses this daemon puts in its invites stay its own
// (see RelayServiceOptions).
func daemonRelayService(gf *globalFlags) (*libp2ptransport.RelayServerConfig, error) {
	if !gf.relayService {
		if len(gf.relayAllowPeers) > 0 {
			return nil, errors.New("-relay-allow-peer requires -relay-service")
		}
		return nil, nil
	}
	if len(gf.relayAllowPeers) == 0 {
		return nil, errors.New("-relay-service requires at least one -relay-allow-peer")
	}
	// The transport refuses this pairing too, but that refusal surfaces as a
	// transport failure. Flag misuse has to read as flag misuse, or a
	// supervisor retries a configuration error as if it were a network fault.
	if gf.connectivity == "relay-only" {
		return nil, errors.New("-relay-service cannot be used with -connectivity relay-only")
	}
	allowed := make([]libpeer.ID, 0, len(gf.relayAllowPeers))
	for _, raw := range gf.relayAllowPeers {
		id, err := libpeer.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("relay allow peer %q: %w", raw, err)
		}
		allowed = append(allowed, id)
	}
	return &libp2ptransport.RelayServerConfig{
		AllowedPeers:          allowed,
		ReservationTTL:        time.Hour,
		CircuitDuration:       15 * time.Minute,
		CircuitBytes:          64 << 20,
		MaxReservations:       128,
		MaxCircuitsPerPeer:    16,
		MaxReservationsPerIP:  8,
		MaxReservationsPerASN: 32,
	}, nil
}

// parseControlledRelays resolves repeatable -controlled-relay multiaddrs. In the
// direct profile they are hole-punch rendezvous points, so a NATed peer stays
// reachable while DCUtR upgrades the connection; relay-only requires them.
func parseControlledRelays(values []string) ([]libpeer.AddrInfo, error) {
	var relays []libpeer.AddrInfo
	for _, raw := range values {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			return nil, fmt.Errorf("controlled relay %q: %w", raw, err)
		}
		info, err := libpeer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return nil, fmt.Errorf("controlled relay %q: %w", raw, err)
		}
		relays = append(relays, *info)
	}
	return relays, nil
}

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

	// Refuse a second daemon before mutating local runtime state.
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

	rootCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	hostConfig, err := daemonHostConfig(gf)
	if err != nil {
		slog.Error(opts.command+": connectivity", slog.String("err", err.Error()))
		return exitInvalidArgument
	}
	libp2pHost, binding, err := libp2ptransport.NewConfiguredHost(rootCtx, s.identity, hostConfig)
	if err != nil {
		slog.Error(opts.command+": libp2p host", slog.String("err", err.Error()))
		return exitTransport
	}
	rawStore, err := store.OpenSQLite(s.dataDir)
	if err != nil {
		slog.Error(opts.command+": open store", slog.String("err", err.Error()))
		return exitTransport
	}
	defer func() { _ = rawStore.Close() }()
	espState, err := esphttp.OpenSQLiteStateStore(s.dataDir)
	if err != nil {
		slog.Error(opts.command+": open esp state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer espState.Close()

	// Wrap the store so IPC tail subscribers and service integrations see
	// new messages as they land. The gossip/publish path writes through this
	// wrapper.
	serviceEvents := events.NewBus()
	notifyStore := newNotifyingStore(rawStore, serviceEvents)

	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity:         s.identity,
		DataDir:          s.dataDir,
		Store:            rawStore,
		Notify:           notifyStore,
		Host:             libp2pHost,
		Binding:          binding,
		Logger:           slog.Default(),
		Profiles:         espState,
		Mode:             hostConfig.Mode,
		ControlledRelays: hostConfig.ControlledRelays,
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
		identity:      s.identity,
		metadataStore: espState,
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

	if opts.exitAfterLoad {
		groups := runtime.ActiveGroupIDs()
		members := groupRuntimeMemberCount(runtime, groups)
		joinedEvent := groupDaemonEvent(opts.event, gf, groups, members, buildJoinHealthSummary(rootCtx, runtime, rawStore, s.identity.PublicKey), sockPath)
		if data, err := json.Marshal(joinedEvent); err == nil {
			fmt.Println(string(data))
		}
		runtime.Close()
		return exitOK
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

	// Start the shared libp2p transport and local IPC loop independently.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runtime.Start(rootCtx); err != nil {
			slog.Warn(opts.command+": group runtime stopped", slog.String("err", err.Error()))
		}
	}()

	srv := &ipcServer{
		memberID:          binding.MemberID,
		peerID:            binding.PeerID.String(),
		identity:          s.identity,
		identityPath:      gf.identity,
		dataDir:           s.dataDir,
		controlSocketPath: sockPath,
		listenPort:        uint16(gf.listenPort),
		runtime:           runtime,
		store:             rawStore,
		notify:            notifyStore,
		metadataStore:     espState,
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
	members := groupRuntimeMemberCount(runtime, groups)
	joinedEvent := groupDaemonEvent(opts.event, gf, groups, members, buildJoinHealthSummary(rootCtx, runtime, rawStore, s.identity.PublicKey), sockPath)
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

func groupRuntimeMemberCount(runtime *groupRuntime, groups []entmoot.GroupID) int {
	members := 0
	for _, gid := range groups {
		if sess, ok := runtime.Get(gid); ok {
			members += len(sess.group.MemberIDs())
		}
	}
	return members
}

func groupDaemonEvent(event string, gf *globalFlags, groups []entmoot.GroupID, members int, health joinHealthSummary, sockPath string) map[string]any {
	return map[string]any{
		"event":          event,
		"group_id":       groups[0],
		"group_ids":      groups,
		"members":        members,
		"health":         health,
		"listen_port":    gf.listenPort,
		"control_socket": sockPath,
		"next_command":   doctorNextCommand(gf, groups[0]),
	}
}

func doctorNextCommand(gf *globalFlags, gid entmoot.GroupID) string {
	args := []string{
		"entmootd",
		"-identity", gf.identity,
		"-data", gf.data,
		"doctor",
		"-group", gid.String(),
		"--probe",
	}
	for i, arg := range args {
		args[i] = shellQuoteArg(arg)
	}
	return strings.Join(args, " ")
}

func shellQuoteArg(arg string) string {
	if arg == "" {
		return "''"
	}
	if strings.IndexFunc(arg, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' ||
			r == '_' || r == '-' || r == '.' || r == '/' || r == ':' || r == '=' || r == '+' || r == ',')
	}) == -1 {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", "'\"'\"'") + "'"
}

// errInviteMalformed is the local sentinel used to distinguish a
// malformed/unsigned invite from a fetch error.
var errInviteMalformed = errors.New("invite malformed")

// errFetchFailed marks a network-fetch failure for a URL invite.
var errFetchFailed = errors.New("invite fetch failed")

type joinInput struct {
	source         string
	capability     *entmoot.BootstrapCapability
	openInvite     *openInviteAcceptPayload
	expectedGroup  *entmoot.GroupID
	expectedIssuer *entmoot.NodeInfo
	groupPolicy    *entpolicy.Policy
	groupMetadata  json.RawMessage
}

// loadJoinInput reads a join input from arg (file path, http(s) URL, or
// entmoot://open-invite link) and classifies it as either a bootstrap
// capability or an open invite descriptor that must be redeemed.
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
	if input, ok, err := parseDefaultMootDescriptor(raw); ok || err != nil {
		if err != nil {
			return joinInput{}, err
		}
		input.source = arg
		return input, nil
	}
	if payload, ok, err := parseOpenInviteDescriptor(raw); ok || err != nil {
		if err != nil {
			return joinInput{}, err
		}
		return joinInput{source: arg, openInvite: payload}, nil
	}
	var capability entmoot.BootstrapCapability
	if err := json.Unmarshal(raw, &capability); err != nil {
		return joinInput{}, fmt.Errorf("%w: parse bootstrap capability: %v", errInviteMalformed, err)
	}
	if capability.GroupID == (entmoot.GroupID{}) || capability.Signature == nil ||
		(!capability.IsOpenInvite() && len(capability.TargetPublicKey) != ed25519.PublicKeySize) {
		return joinInput{}, fmt.Errorf("%w: unsupported join input; provide a signed bootstrap capability", errInviteMalformed)
	}
	return joinInput{source: arg, capability: &capability}, nil
}

// readJoinInputBytes reads a join input JSON bundle from arg (file path or
// http(s) URL). Bootstrap capability checks happen after classification.
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

func parseDefaultMootDescriptor(raw []byte) (joinInput, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return joinInput{}, false, nil
	}
	if !hasJSONField(fields, "type") {
		return joinInput{}, false, nil
	}
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return joinInput{}, false, nil
	}
	if probe.Type != defaultmoot.DescriptorType {
		return joinInput{}, false, nil
	}
	desc, err := defaultmoot.Parse(raw)
	if err != nil {
		return joinInput{}, true, err
	}
	pinned, err := defaultMootPinnedPublicKeys()
	if err != nil {
		return joinInput{}, true, err
	}
	if err := defaultmoot.VerifyAny(desc, pinned); err != nil {
		return joinInput{}, true, err
	}
	payload, err := openInvitePayloadFromDefaultMootDescriptor(desc)
	if err != nil {
		return joinInput{}, true, err
	}
	metadata, err := defaultMootGroupMetadata(desc)
	if err != nil {
		return joinInput{}, true, err
	}
	return joinInput{openInvite: payload, expectedGroup: &desc.GroupID, expectedIssuer: &desc.Issuer, groupPolicy: &desc.Policy, groupMetadata: metadata}, true, nil
}

// defaultMootPinnedPublicKeys returns every key a default-moot descriptor may
// be signed by. It is a set while the signer key rotates; see
// defaultmoot.DefaultDescriptorPubKeysBase64.
func defaultMootPinnedPublicKeys() ([]ed25519.PublicKey, error) {
	cfg, err := defaultmoot.LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return cfg.PinnedPublicKeys, nil
}

func openInvitePayloadFromDefaultMootDescriptor(desc defaultmoot.Descriptor) (*openInviteAcceptPayload, error) {
	var out *openInviteAcceptPayload
	if strings.TrimSpace(desc.OpenInvite.IssuerURL) != "" || strings.TrimSpace(desc.OpenInvite.Token) != "" {
		payload, err := normalizeOpenInviteAcceptPayload(openInviteAcceptPayload{
			IssuerURL: desc.OpenInvite.IssuerURL,
			Token:     desc.OpenInvite.Token,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: default moot open invite: %v", errInviteMalformed, err)
		}
		out = payload
	}
	for _, rawLink := range []string{desc.OpenInvite.Link, desc.OpenInviteLink} {
		if strings.TrimSpace(rawLink) == "" {
			continue
		}
		payload, err := parseOpenInviteLink(rawLink)
		if err != nil {
			return nil, fmt.Errorf("%w: default moot open invite link: %v", errInviteMalformed, err)
		}
		if out != nil && (out.IssuerURL != payload.IssuerURL || out.Token != payload.Token) {
			return nil, fmt.Errorf("%w: default moot open invite link does not match issuer_url/token fields", errInviteMalformed)
		}
		out = payload
	}
	if out == nil {
		return nil, fmt.Errorf("%w: default moot descriptor has no open invite", errInviteMalformed)
	}
	return out, nil
}

func defaultMootGroupMetadata(desc defaultmoot.Descriptor) (json.RawMessage, error) {
	raw, err := json.Marshal(map[string]any{
		"name":         desc.Name,
		"description":  "Official default public Entmoot moot",
		"tags":         []string{"default", "public", "entmoot"},
		"default_moot": true,
	})
	if err != nil {
		return nil, err
	}
	return esphttp.NormalizeGroupMetadata(raw)
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

func redeemJoinOpenInvite(ctx context.Context, payload *openInviteAcceptPayload, loadCtx groupDaemonLoadContext) (*entmoot.BootstrapCapability, error) {
	if payload == nil {
		return nil, fmt.Errorf("%w: open invite payload is missing", errInviteMalformed)
	}
	issuer, token, err := parseOpenInviteAcceptPayload(*payload)
	if err != nil {
		return nil, err
	}
	exec := espOperationExecutor{identity: loadCtx.identity, timeout: 30 * time.Second}
	capability, _, err := exec.redeemOpenInviteFromIssuer(ctx, issuer, token)
	if err != nil {
		return nil, err
	}
	return &capability, nil
}

// hasPrefix is a tiny alias for strings.HasPrefix so the import
// surface in this file stays minimal.
func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
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

// Put implements store.MessageStore. A newly inserted message is broadcast;
// duplicates and failures are not observable to subscribers.
func (n *notifyingStore) Put(ctx context.Context, expectedGroup entmoot.GroupID, m entmoot.Message) (bool, error) {
	inserted, err := n.inner.Put(ctx, expectedGroup, m)
	if err != nil || !inserted {
		return inserted, err
	}
	n.sink.Emit(events.Event{
		Type:           events.TypeMessageIngested,
		GroupID:        m.GroupID,
		MessageID:      m.ID,
		AuthorMemberID: messageAuthorMemberID(m),
		At:             time.Now(),
	})
	n.broadcast(m)
	return true, nil
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
func (n *notifyingStore) LatestBefore(ctx context.Context, gid entmoot.GroupID, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return n.inner.LatestBefore(ctx, gid, limit, boundary)
}
func (n *notifyingStore) Topics(ctx context.Context, gid entmoot.GroupID, limit int) ([]store.TopicSummary, error) {
	return n.inner.Topics(ctx, gid, limit)
}
func (n *notifyingStore) LatestByTopic(ctx context.Context, gid entmoot.GroupID, topic string, limit int) ([]entmoot.Message, error) {
	return n.inner.LatestByTopic(ctx, gid, topic, limit)
}
func (n *notifyingStore) LatestByTopicBefore(ctx context.Context, gid entmoot.GroupID, topic string, limit int, boundary *store.PageBoundary) ([]entmoot.Message, error) {
	return n.inner.LatestByTopicBefore(ctx, gid, topic, limit, boundary)
}
func (n *notifyingStore) MerkleRoot(ctx context.Context, gid entmoot.GroupID) ([32]byte, error) {
	return n.inner.MerkleRoot(ctx, gid)
}
func (n *notifyingStore) MessageIDsPage(ctx context.Context, gid entmoot.GroupID, sinceMillis int64, after *store.RangeCursor, expectedGeneration uint64, limit int) (store.MessageIDPage, error) {
	paged, ok := n.inner.(store.PagedMessageIDStore)
	if !ok {
		return store.MessageIDPage{}, errors.New("message store does not support paged history")
	}
	return paged.MessageIDsPage(ctx, gid, sinceMillis, after, expectedGeneration, limit)
}
func (n *notifyingStore) MessageIDsPageWindow(ctx context.Context, gid entmoot.GroupID, sinceMillis, untilMillis int64, after *store.RangeCursor, expectedGeneration uint64, limit int) (store.MessageIDPage, error) {
	paged, ok := n.inner.(store.WindowedPagedMessageIDStore)
	if !ok {
		return store.MessageIDPage{}, errors.New("message store does not support windowed paged history")
	}
	return paged.MessageIDsPageWindow(ctx, gid, sinceMillis, untilMillis, after, expectedGeneration, limit)
}
func (n *notifyingStore) PruneBefore(ctx context.Context, gid entmoot.GroupID, beforeMillis int64) (int64, error) {
	return store.PruneBefore(ctx, n.inner, gid, beforeMillis)
}
func (n *notifyingStore) PruneBeforeExceptTopics(ctx context.Context, gid entmoot.GroupID, beforeMillis int64, exemptTopics []string) (int64, error) {
	return store.PruneBeforeExceptTopics(ctx, n.inner, gid, beforeMillis, exemptTopics)
}

// HasTombstone and CoverageFloor must be forwarded, or history sync sees a
// store that never pruned anything: it would keep re-fetching identifiers
// retention deliberately dropped.
func (n *notifyingStore) HasTombstone(ctx context.Context, gid entmoot.GroupID, id entmoot.MessageID) (bool, error) {
	return store.HasTombstone(ctx, n.inner, gid, id)
}
func (n *notifyingStore) CoverageFloor(ctx context.Context, gid entmoot.GroupID) (int64, error) {
	return store.CoverageFloor(ctx, n.inner, gid)
}
func (n *notifyingStore) Close() error { return n.inner.Close() }

// ipcServer bundles the state IPC handlers need. All fields are
// read-only after cmdJoin finishes setup so handlers may access them
// without locking.
type ipcServer struct {
	memberID          entmoot.MemberID
	peerID            string
	identity          *keystore.Identity
	identityPath      string
	dataDir           string
	controlSocketPath string
	listenPort        uint16
	runtime           *groupRuntime
	store             *store.SQLite
	notify            *notifyingStore
	metadataStore     esphttp.GroupMetadataStore
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
	case *ipc.PeerProbeReq:
		s.handlePeerProbe(ctx, c, v)
	case *ipc.GroupDeactivateReq:
		s.handleGroupDeactivate(c, v)
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
	if _, err := sess.live.Publish(ctx, msg); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    publishErrorCode(err),
			GroupID: &gid,
			Message: "signed publish: " + err.Error(),
		})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.SignedPublishResp{
		Status:         "accepted",
		MessageID:      msg.ID,
		GroupID:        gid,
		AuthorMemberID: messageAuthorMemberID(msg),
		TimestampMS:    msg.Timestamp,
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
	resp, frame := s.publishLocalMessage(ctx, gid, req.Topics, req.Content)
	if frame != nil {
		_ = ipc.EncodeAndWrite(c, frame)
		return
	}
	_ = ipc.EncodeAndWrite(c, resp)
}

func (s *ipcServer) publishLocalMessage(ctx context.Context, gid entmoot.GroupID, topics []string, content []byte) (*ipc.PublishResp, *ipc.ErrorFrame) {
	sess, ok := s.runtime.Get(gid)
	if !ok {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeGroupNotFound,
			GroupID: &gid,
			Message: "group not joined",
		}
	}
	if len(topics) == 0 {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInvalidArgument,
			Message: "no topics supplied",
		}
	}
	for _, t := range topics {
		// We don't call topic.ValidPattern here because topics on a
		// message are concrete strings, not patterns. We just reject
		// empty topics defensively.
		if t == "" {
			return nil, &ipc.ErrorFrame{
				Type:    "error",
				Code:    ipc.CodeInvalidArgument,
				Message: "empty topic",
			}
		}
	}
	if !sess.group.IsMemberID(s.memberID) {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: fmt.Sprintf("local member %s is not in the roster", s.memberID.String()),
		}
	}

	// Build and sign the message with the same-key application and transport
	// identity recorded in the roster.
	now := time.Now().UnixMilli()
	author := entmoot.NodeInfo{
		MemberID:      &s.memberID,
		PeerID:        s.peerID,
		EntmootPubKey: s.identity.PublicKey,
	}
	if info, ok := sess.group.MemberInfoByID(s.memberID); ok {
		author = info
		if author.PeerID == "" {
			author.PeerID = s.peerID
		}
	}

	msg := entmoot.Message{
		Version:   2,
		GroupID:   gid,
		Author:    author,
		Timestamp: now,
		Topics:    append([]string(nil), topics...),
		Content:   append([]byte(nil), content...),
	}
	head := sess.group.Canonical().ID
	msg.RosterHead = &head

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
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "local signer: " + err.Error(),
		}
	}
	msg, err = signing.SignMessage(ctx, signer, msg)
	if err != nil {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeInternal,
			Message: "sign message: " + err.Error(),
		}
	}
	if _, err := sess.live.Publish(ctx, msg); err != nil {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    publishErrorCode(err),
			Message: "gossiper.Publish: " + err.Error(),
		}
	}

	return &ipc.PublishResp{
		MessageID:   msg.ID,
		GroupID:     gid,
		TimestampMS: now,
	}, nil
}

func publishErrorCode(err error) ipc.ErrorCode {
	switch {
	case errors.Is(err, entmoot.ErrNotMember):
		return ipc.CodeNotMember
	case errors.Is(err, entmoot.ErrSigInvalid):
		return ipc.CodeInvalidArgument
	default:
		return ipc.CodeInternal
	}
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
	joinTimeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if joinTimeout <= 0 {
		joinTimeout = defaultJoinTimeout
	}
	joinCtx, cancel := context.WithTimeout(ctx, joinTimeout)
	defer cancel()
	var (
		session *groupSession
		created bool
		err     error
		issuer  *entmoot.NodeInfo
	)
	switch {
	case req.Capability != nil && req.LocalGroupID == nil:
		session, created, err = s.runtime.AddCapability(joinCtx, *req.Capability)
		founder := req.Capability.Founder
		issuer = &founder
	case req.Capability == nil && req.LocalGroupID != nil:
		session, created, err = s.runtime.AddLocalGroup(joinCtx, *req.LocalGroupID)
	default:
		err = errors.New("exactly one capability or local_group_id is required")
	}
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "join group: " + err.Error()})
		return
	}
	if req.GroupPolicy != nil {
		if err := s.runtime.policyStore.Put(joinCtx, session.groupID, *req.GroupPolicy); err != nil {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &session.groupID, Message: "join group: persist policy: " + err.Error()})
			return
		}
	}
	if err := persistJoinGroupMetadata(joinCtx, s.metadataStore, session.groupID, req.GroupMetadata); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &session.groupID, Message: "join group: persist group metadata: " + err.Error()})
		return
	}
	status := "already_joined"
	if created {
		status = "joined"
	}
	_ = ipc.EncodeAndWrite(c, &ipc.JoinGroupResp{Status: status, GroupID: session.groupID, Issuer: issuer, Members: len(session.group.MemberIDs()), Readiness: s.joinReadinessEvent(ctx)})
}

func persistJoinGroupMetadata(ctx context.Context, metadataStore esphttp.GroupMetadataStore, groupID entmoot.GroupID, metadata json.RawMessage) error {
	if metadataStore == nil || len(bytes.TrimSpace(metadata)) == 0 {
		return nil
	}
	if _, err := esphttp.NormalizeGroupMetadata(metadata); err != nil {
		return err
	}
	return metadataStore.SetGroupMetadata(ctx, groupID, metadata)
}

func clonePolicyPtr(in *entpolicy.Policy) *entpolicy.Policy {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func nodeInfoEqual(a, b entmoot.NodeInfo) bool {
	if !bytes.Equal(a.EntmootPubKey, b.EntmootPubKey) || a.PeerID != b.PeerID {
		return false
	}
	if a.MemberID == nil || b.MemberID == nil {
		return a.MemberID == nil && b.MemberID == nil
	}
	return *a.MemberID == *b.MemberID
}

func (s *ipcServer) joinReadinessEvent(ctx context.Context) json.RawMessage {
	groups := s.runtime.ActiveGroupIDs()
	if len(groups) == 0 {
		return nil
	}
	gf := &globalFlags{
		identity:   s.identityPath,
		data:       s.dataDir,
		listenPort: uint(s.listenPort),
	}
	event := groupDaemonEvent(
		"joined",
		gf,
		groups,
		groupRuntimeMemberCount(s.runtime, groups),
		buildJoinHealthSummary(ctx, s.runtime, s.store, s.identity.PublicKey),
		s.controlSocketPath,
	)
	data, err := json.Marshal(event)
	if err != nil {
		return nil
	}
	return data
}

func (s *ipcServer) handleInviteCreate(_ context.Context, c net.Conn, req *ipc.InviteCreateReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "group_id is required"})
		return
	}
	if len(req.TargetPublicKey) != 0 && len(req.TargetPublicKey) != ed25519.PublicKeySize {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "target_public_key must be an Ed25519 key"})
		return
	}
	if req.Open == (len(req.TargetPublicKey) != 0) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "provide target_public_key, or set open for a bearer invite; not both"})
		return
	}
	if req.MaxUses < 0 || req.MaxUses > maxInviteUses {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: fmt.Sprintf("max_uses must be between 0 and %d", maxInviteUses)})
		return
	}
	session, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	founder := session.group.Founder()
	if !session.group.CanAdminister(s.memberID) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "invite_create requires the founder or a delegated admin identity"})
		return
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founder.EntmootPubKey)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: "founder identity: " + err.Error()})
		return
	}
	founder.MemberID = &founderBinding.MemberID
	founder.PeerID = founderBinding.PeerID.String()
	// Whichever named peer is entitled to serve does so — a current member, or
	// this node as the capability's own issuer. So the invite carries those
	// addresses and, when it is not the founder, this node as the issuer.
	localBinding, err := libp2ptransport.BindingFromPublicKey(s.identity.PublicKey)
	if err != nil || localBinding.PeerID != s.runtime.host.ID() {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "local identity does not match libp2p host"})
		return
	}
	var issuer *entmoot.NodeInfo
	if s.memberID != founderBinding.MemberID {
		info, found := session.group.MemberInfoByID(s.memberID)
		if !found {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "local identity is not a member of this group"})
			return
		}
		memberID := s.memberID
		info.MemberID = &memberID
		info.PeerID = localBinding.PeerID.String()
		issuer = &info
	}
	var targetMemberID entmoot.MemberID
	targetPeerID := ""
	if len(req.TargetPublicKey) != 0 {
		targetBinding, err := libp2ptransport.BindingFromPublicKey(req.TargetPublicKey)
		if err != nil {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: err.Error()})
			return
		}
		targetMemberID = targetBinding.MemberID
		targetPeerID = targetBinding.PeerID.String()
	}
	if len(req.BootstrapMultiaddrs) == 0 {
		// A request that names nothing gets this node's own addresses — and a
		// libp2p host on a multi-homed machine reports dozens, so this fill
		// has to obey the same bounds as everything else the daemon attaches
		// without being asked. It did not, which is how an ESP open invite
		// created with no address list (the group_create open-invite mode
		// never supplies one) minted a capability too large to redeem.
		own := make([]string, 0, len(s.runtime.host.Addrs()))
		for _, address := range s.runtime.host.Addrs() {
			own = append(own, address.Encapsulate(multiaddr.StringCast("/p2p/"+localBinding.PeerID.String())).String())
		}
		req.BootstrapMultiaddrs = boundInviteAddresses(own)
	}
	// Any current member may be named as a bootstrap peer, so an invite stays
	// usable while its issuer is down. See the comment in cmdInviteCreate.
	memberPeers, err := groupMemberPeerIDs(session.group)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: "read group members"})
		return
	}
	allowedAddresses := make([]string, 0, len(req.BootstrapMultiaddrs))
	allowedPeerIDs := make([]string, 0, len(req.BootstrapMultiaddrs)+1)
	seenPeers := make(map[libpeer.ID]struct{})
	for _, raw := range req.BootstrapMultiaddrs {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "invalid bootstrap multiaddr"})
			return
		}
		info, err := libpeer.AddrInfoFromP2pAddr(address)
		if err != nil {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "bootstrap address must end in /p2p/<peer-id>"})
			return
		}
		if _, ok := memberPeers[info.ID]; !ok && info.ID != localBinding.PeerID {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "bootstrap address does not name a member of this group"})
			return
		}
		allowedAddresses = append(allowedAddresses, address.String())
		if _, ok := seenPeers[info.ID]; !ok {
			seenPeers[info.ID] = struct{}{}
			allowedPeerIDs = append(allowedPeerIDs, info.ID.String())
		}
	}
	if len(allowedPeerIDs) == 0 {
		allowedPeerIDs = append(allowedPeerIDs, localBinding.PeerID.String())
		seenPeers[localBinding.PeerID] = struct{}{}
	}
	if !req.NoFallbackPeers {
		var privateFallbacks int
		allowedAddresses, allowedPeerIDs, privateFallbacks = addKnownMemberPeers(s.dataDir, gid, memberPeers,
			localBinding.PeerID, allowedAddresses, allowedPeerIDs, seenPeers)
		if privateFallbacks > 0 {
			// The CLI prints this; over IPC the operator is elsewhere, so it
			// goes to the daemon log rather than being dropped. An invite that
			// carries a member's LAN address should not do so silently.
			slog.Warn("invite create: attached a private fallback address",
				slog.String("group_id", gid.String()),
				slog.Int("private_fallback_peers", privateFallbacks))
		}
	}
	now := time.Now()
	expires := now.Add(24 * time.Hour)
	if req.ValidForMS > 0 {
		expires = now.Add(time.Duration(req.ValidForMS) * time.Millisecond)
	}
	if req.ValidUntilMS > 0 {
		expires = time.UnixMilli(req.ValidUntilMS)
	}
	if !expires.After(now) || expires.After(now.Add(7*24*time.Hour)) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "capability validity must be within seven days"})
		return
	}
	capability := entmoot.BootstrapCapability{
		GroupID:           gid,
		TargetPublicKey:   append([]byte(nil), req.TargetPublicKey...),
		TargetMemberID:    targetMemberID,
		TargetPeerID:      targetPeerID,
		Founder:           founder,
		Issuer:            issuer,
		RosterHead:        session.group.Canonical().ID,
		AllowedPeerIDs:    allowedPeerIDs,
		AllowedMultiaddrs: allowedAddresses,
		Relays:            s.runtime.relayHints(),
		MaxUses:           req.MaxUses,
		IssuedAtMS:        now.UnixMilli(),
		ExpiresAtMS:       expires.UnixMilli(),
	}
	if _, err := rand.Read(capability.Nonce[:]); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: "nonce generation failed"})
		return
	}
	if err := libp2ptransport.SignBootstrapCapability(s.identity, &capability); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: err.Error()})
		return
	}
	// The signature is part of what the joiner must send, so measure after it.
	if size, tooLarge := libp2ptransport.CapabilityTooLarge(capability); tooLarge {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid,
			Message: fmt.Sprintf("invite is %d bytes, over the %d-byte limit a joiner can send", size, libp2ptransport.MaxCapabilityBytes)})
		return
	}
	if err := s.runtime.invites.RecordIssuedInvite(capability); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: "record issued invite: " + err.Error()})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.InviteCreateResp{Status: "created", GroupID: gid, Capability: capability, RosterHead: session.group.Canonical().ID, Members: len(session.group.MemberIDs())})
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
	if !sess.group.CanAdminister(s.memberID) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: "invite creation requires the founder or a delegated admin identity",
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
	memberPeers, peersErr := groupMemberPeerIDs(sess.group)
	peerIDs := make([]string, 0, len(memberPeers))
	if peersErr == nil {
		for id := range memberPeers {
			peerIDs = append(peerIDs, id.String())
		}
		sort.Strings(peerIDs)
	}
	_ = ipc.EncodeAndWrite(c, &ipc.InviteAuthorityCheckResp{
		Status:        "ok",
		GroupID:       gid,
		RosterHead:    sess.group.Canonical().ID,
		Members:       len(sess.group.MemberIDs()),
		MemberPeerIDs: peerIDs,
		LocalPeerID:   s.peerID,
	})
}

// handlePeerProbe answers a reachability probe. The budget is clamped to
// maxProbeBudget and the write deadline is set past it, because handleConn's
// 10s READ deadline does not bound the write and a probe may legitimately run
// longer than it. Nothing here clamps the budget below that read deadline;
// saying otherwise would describe a mechanism this code does not have.
func (s *ipcServer) handlePeerProbe(ctx context.Context, c net.Conn, req *ipc.PeerProbeReq) {
	gid := req.GroupID
	if s.runtime == nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "no group runtime"})
		return
	}
	if gid == (entmoot.GroupID{}) {
		resolved, ok := s.runtime.SingleGroup()
		if !ok {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "peer_probe requires group_id unless exactly one group is joined"})
			return
		}
		gid = resolved
	}
	budget := time.Duration(req.BudgetMS) * time.Millisecond
	if budget <= 0 {
		budget = defaultProbeBudget
	}
	if budget > maxProbeBudget {
		budget = maxProbeBudget
	}
	// The client is waiting on one socket read; give the answer room to land.
	_ = c.SetWriteDeadline(time.Now().Add(budget + 5*time.Second))
	probeCtx, cancel := context.WithTimeout(ctx, budget+time.Second)
	defer cancel()
	peers, incomplete, err := s.runtime.probePeers(probeCtx, gid, budget)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: err.Error()})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.PeerProbeResp{Status: "probed", GroupID: gid, Peers: peers, Incomplete: incomplete})
}

func (s *ipcServer) handleGroupDeactivate(c net.Conn, req *ipc.GroupDeactivateReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "group_deactivate requires group_id"})
		return
	}
	if s.runtime == nil || !s.runtime.RemoveGroup(gid) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	_ = ipc.EncodeAndWrite(c, &ipc.GroupDeactivateResp{Status: "deactivated", GroupID: gid})
}

func (s *ipcServer) handleMemberRemove(ctx context.Context, c net.Conn, req *ipc.MemberRemoveReq) {
	gid := req.GroupID
	if gid == (entmoot.GroupID{}) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, Message: "member_remove requires group_id"})
		return
	}
	if req.Target.MemberID == nil || req.Target.PeerID == "" || len(req.Target.EntmootPubKey) != ed25519.PublicKeySize {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "member_remove requires target member_id, peer_id, and public key"})
		return
	}
	if err := entmoot.ValidateMemberInfo(req.Target); err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: err.Error()})
		return
	}
	sess, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	founder := sess.group.Founder()
	if !sess.group.CanAdminister(s.memberID) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "member_remove requires the founder or a delegated admin identity"})
		return
	}
	if founder.MemberID != nil && *req.Target.MemberID == *founder.MemberID {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "cannot remove group founder"})
		return
	}
	unlock := lockESPInviteRoster(gid)
	existing, ok := sess.group.MemberInfoByID(*req.Target.MemberID)
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
	if err := applyRosterRemove(s.identity, sess.group, existing); err != nil {
		unlock()
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInternal, GroupID: &gid, Message: err.Error()})
		return
	}
	head := sess.group.Canonical().ID
	members := len(sess.group.MemberIDs())
	unlock()
	// A removed member's outstanding invites stop working by rule: an invite
	// is worth exactly its issuer's current authority, which every node
	// projects from the same records. There is nothing to revoke and so
	// nothing that can fail to be revoked.
	//
	// Invites issued by whoever is still an admin are unaffected, and an
	// operator may want to see them, so they are reported.
	var ledgerError string
	var nonces []string
	live, err := s.runtime.invites.LiveOpenInvites(gid)
	if err != nil {
		ledgerError = "read open invites: " + err.Error()
		slog.Error("member_remove: read open invites", slog.String("err", err.Error()))
	} else {
		nonces = make([]string, 0, len(live))
		for _, record := range live {
			nonces = append(nonces, base64.StdEncoding.EncodeToString(record.Nonce[:]))
		}
	}
	// metadataStore is the narrow interface the join path needs; the concrete
	// ESP state store also lists open-invite tokens. A count of zero must mean
	// "none outstanding", never "could not look", so an unreadable or absent
	// store leaves the count nil and names the reason.
	var espOpen *int
	espError := ""
	lister, ok := s.metadataStore.(espOpenInviteLister)
	switch {
	case !ok || lister == nil:
		espError = "esp open-invite store is unavailable on this daemon"
	default:
		records, err := lister.ListOpenInvitesByGroup(ctx, gid)
		if err != nil {
			espError = "read esp open invites: " + err.Error()
			slog.Error("member_remove: read esp open invites", slog.String("err", err.Error()))
			break
		}
		nowMS := time.Now().UnixMilli()
		espLive := 0
		for _, record := range records {
			if record.Revoked ||
				(record.ExpiresAtMS > 0 && record.ExpiresAtMS <= nowMS) ||
				(record.MaxUses > 0 && record.UseCount >= record.MaxUses) {
				continue
			}
			espLive++
		}
		espOpen = &espLive
	}
	_ = ipc.EncodeAndWrite(c, &ipc.MemberRemoveResp{
		Status: "removed", GroupID: gid, RosterHead: head, Members: members,
		OutstandingOpenInvites:    nonces,
		OutstandingESPOpenInvites: espOpen,
		ESPOpenInvitesError:       espError,
		InviteLedgerError:         ledgerError,
	})
}

// handleInfo assembles a full InfoResp snapshot from live state.
func (s *ipcServer) handleInfo(ctx context.Context, c net.Conn) {
	pub := append([]byte(nil), s.identity.PublicKey...)
	if gid, ok := s.runtime.SingleGroup(); ok {
		if sess, ok := s.runtime.Get(gid); ok {
			pub, _ = pubkeyFromGroup(sess.group, s.memberID, s.identity.PublicKey)
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
			members = len(sess.group.MemberIDs())
		} else {
			// For groups outside the daemon's active roster, peek at
			// the existing roster file directly. Empty/orphan roster
			// shells are not joined groups and should not leak through
			// info after a failed live join attempt.
			r, ok, err := openExistingGroup(s.dataDir, gid)
			if err != nil {
				slog.Warn("info: open roster",
					slog.String("group", gid.String()),
					slog.String("err", err.Error()))
				continue
			}
			if !ok {
				continue
			}
			if !groupHasLocalMemberIdentity(r, s.memberID, s.identity.PublicKey) {
				_ = r.Close()
				continue
			}
			members = len(r.MemberIDs())
			_ = r.Close()
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
		MemberID:      s.memberID,
		PeerID:        s.peerID,
		EntmootPubKey: pub,
		ListenPort:    s.listenPort,
		DataDir:       s.dataDir,
		Groups:        groups,
		Running:       true,
	}
	_ = ipc.EncodeAndWrite(c, resp)
}

// pubkeyFromGroup returns the locally-stored pubkey from the membership
// projection, falling back to fallback if absent.
func pubkeyFromGroup(group *membership.Group, id entmoot.MemberID, fallback []byte) ([]byte, bool) {
	if info, ok := group.MemberInfoByID(id); ok && len(info.EntmootPubKey) > 0 {
		return append([]byte(nil), info.EntmootPubKey...), true
	}
	return append([]byte(nil), fallback...), false
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

func messageAuthorMemberID(message entmoot.Message) entmoot.MemberID {
	if message.Author.MemberID != nil {
		return *message.Author.MemberID
	}
	memberID, _ := entmoot.MemberIDFromPublicKey(message.Author.EntmootPubKey)
	return memberID
}
