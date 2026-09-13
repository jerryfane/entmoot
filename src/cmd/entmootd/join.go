package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/topic"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

const defaultJoinTimeout = 90 * time.Second

func cmdJoin(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	serveAfterJoin := fs.Bool("serve", false, "after joining, keep running as the Entmoot daemon")
	timeout := fs.Duration("timeout", defaultJoinTimeout, "enrollment deadline")
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
		fmt.Fprintln(os.Stderr, "join: stop the running daemon before enrolling a new group")
		return exitControlUnavail
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
				enrollCtx, cancel := context.WithTimeout(ctx, *timeout)
				_, _, err = runtime.AddCapability(enrollCtx, *capability)
				cancel()
				if err != nil {
					return exitTransport, fmt.Errorf("enroll group %s: %w", capability.GroupID.String(), err)
				}
				if err := persistJoinGroupMetadata(ctx, loadCtx.metadataStore, capability.GroupID, input.groupMetadata); err != nil {
					return exitTransport, fmt.Errorf("persist group metadata %s: %w", capability.GroupID.String(), err)
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
			// is INVALID_ARGUMENT per CLI_DESIGN §3.1. A network-fetch
			// failure is a transport error (exit 1).
			if errors.Is(err, errFetchFailed) {
				return nil, exitTransport
			}
			if errors.Is(err, entmoot.ErrInviteExpired) ||
				errors.Is(err, entmoot.ErrSigInvalid) ||
				errors.Is(err, errInviteMalformed) {
				return nil, exitInvalidArgument
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

func joinInputsOverIPC(sockPath string, inputs []joinInput, timeout time.Duration) int {
	ctx := context.Background()
	var lastReadiness []byte
	for _, input := range inputs {
		resp, frame, err := joinInputOverIPC(ctx, sockPath, input, timeout)
		if err != nil {
			slog.Error("join: live daemon join", slog.String("err", err.Error()))
			if errors.Is(err, errInviteMalformed) {
				return exitInvalidArgument
			}
			return exitTransport
		}
		if frame != nil {
			fmt.Fprintf(os.Stderr, "join: %s: %s\n", frame.Code, frame.Message)
			return ipc.ExitCode(frame.Code)
		}
		if len(resp.Readiness) > 0 {
			lastReadiness = append(lastReadiness[:0], resp.Readiness...)
		}
	}
	if len(lastReadiness) > 0 {
		fmt.Println(string(lastReadiness))
	}
	return exitOK
}

func joinInputOverIPC(ctx context.Context, sockPath string, input joinInput, timeout time.Duration) (*ipc.JoinGroupResp, *ipc.ErrorFrame, error) {
	if input.capability == nil {
		return nil, nil, fmt.Errorf("invite %s: a bootstrap capability is required", input.source)
	}
	req := &ipc.JoinGroupReq{Capability: input.capability}
	resp, frame, err := joinGroupReqOverIPC(ctx, sockPath, req, timeout)
	if err != nil || frame != nil {
		return resp, frame, err
	}
	if resp == nil {
		return nil, nil, fmt.Errorf("invite %s: no join response", input.source)
	}
	if input.expectedGroup != nil && resp.GroupID != *input.expectedGroup {
		return nil, nil, fmt.Errorf("%w: %s redeemed group %s, want signed descriptor group %s", errInviteMalformed, input.source, resp.GroupID.String(), input.expectedGroup.String())
	}
	if input.expectedIssuer != nil && (resp.Issuer == nil || !nodeInfoEqual(*resp.Issuer, *input.expectedIssuer)) {
		return nil, nil, fmt.Errorf("%w: %s redeemed founder does not match signed descriptor founder", errInviteMalformed, input.source)
	}
	return resp, nil, nil
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

func remainingJoinBootstrapTimeout(ctx context.Context, fallback time.Duration, now time.Time) (time.Duration, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		if fallback <= 0 {
			fallback = defaultJoinTimeout
		}
		return fallback, nil
	}
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return 0, context.DeadlineExceeded
	}
	return remaining, nil
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
	relays, err := parseControlledRelays(gf.controlledRelays)
	if err != nil {
		return libp2ptransport.HostConfig{}, err
	}
	config.ControlledRelays = relays
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
	fleetState, err := esphttp.OpenSQLiteStateStore(s.dataDir)
	if err != nil {
		slog.Error(opts.command+": open fleet state", slog.String("err", err.Error()))
		return exitTransport
	}
	defer fleetState.Close()

	// Wrap the store so IPC tail subscribers and service integrations see
	// new messages as they land. The gossip/publish path writes through this
	// wrapper.
	serviceEvents := events.NewBus()
	notifyStore := newNotifyingStore(rawStore, serviceEvents)

	runtime, err := newGroupRuntime(groupRuntimeConfig{
		Identity: s.identity,
		DataDir:  s.dataDir,
		Store:    rawStore,
		Notify:   notifyStore,
		Host:     libp2pHost,
		Binding:  binding,
		Logger:   slog.Default(),
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
		metadataStore: fleetState,
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
		metadataStore:     fleetState,
	}
	commandRunner := newFleetCommandRunner(srv, fleetState, notifyStore, slog.Default())

	wg.Add(1)
	go func() {
		defer wg.Done()
		go func() {
			<-rootCtx.Done()
			_ = listener.Close()
		}()
		srv.acceptLoop(rootCtx, listener)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		commandRunner.run(rootCtx)
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
			members += len(sess.roster.MemberIDs())
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
	if input, ok, err := parseFleetInviteDescriptor(raw); ok || err != nil {
		if err != nil {
			return joinInput{}, err
		}
		input.source = arg
		return input, nil
	}
	var capability entmoot.BootstrapCapability
	if err := json.Unmarshal(raw, &capability); err != nil {
		return joinInput{}, fmt.Errorf("%w: parse bootstrap capability: %v", errInviteMalformed, err)
	}
	if capability.GroupID == (entmoot.GroupID{}) || len(capability.TargetPublicKey) != ed25519.PublicKeySize {
		return joinInput{}, fmt.Errorf("%w: unsupported join input; provide a target-bound bootstrap capability", errInviteMalformed)
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

const fleetInviteDescriptorType = "entmoot.fleet_invite.v2"

type fleetInviteDescriptor struct {
	Type           string                      `json:"type,omitempty"`
	FleetID        string                      `json:"fleet_id"`
	FleetName      string                      `json:"fleet_name,omitempty"`
	ControlGroupID entmoot.GroupID             `json:"control_group_id,omitempty"`
	Capability     entmoot.BootstrapCapability `json:"capability"`
	GroupMetadata  json.RawMessage             `json:"group_metadata,omitempty"`
}

func newFleetInviteDescriptor(fleet esphttp.FleetRecord, capability entmoot.BootstrapCapability) (fleetInviteDescriptor, error) {
	metadata, err := fleetControlGroupMetadata(fleet.FleetID, fleet.Name)
	if err != nil {
		return fleetInviteDescriptor{}, err
	}
	return fleetInviteDescriptor{
		Type:           fleetInviteDescriptorType,
		FleetID:        fleet.FleetID,
		FleetName:      fleet.Name,
		ControlGroupID: fleet.ControlGroupID,
		Capability:     capability,
		GroupMetadata:  metadata,
	}, nil
}

func parseFleetInviteDescriptor(raw []byte) (joinInput, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return joinInput{}, false, nil
	}
	if !hasJSONField(fields, "fleet_id") && !hasJSONField(fields, "group_metadata") {
		return joinInput{}, false, nil
	}
	if !hasJSONField(fields, "capability") {
		return joinInput{}, false, nil
	}
	var desc fleetInviteDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor: %v", errInviteMalformed, err)
	}
	if desc.Type != "" && desc.Type != fleetInviteDescriptorType {
		return joinInput{}, true, fmt.Errorf("%w: unsupported fleet invite descriptor type %q", errInviteMalformed, desc.Type)
	}
	desc.FleetID = strings.TrimSpace(desc.FleetID)
	if desc.FleetID == "" {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor requires fleet_id", errInviteMalformed)
	}
	if desc.Capability.GroupID == (entmoot.GroupID{}) {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor requires a bootstrap capability", errInviteMalformed)
	}
	if desc.ControlGroupID != (entmoot.GroupID{}) && desc.ControlGroupID != desc.Capability.GroupID {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor control_group_id does not match capability", errInviteMalformed)
	}
	metadata := desc.GroupMetadata
	if len(bytes.TrimSpace(metadata)) == 0 {
		var err error
		metadata, err = fleetControlGroupMetadata(desc.FleetID, desc.FleetName)
		if err != nil {
			return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor metadata: %v", errInviteMalformed, err)
		}
	} else if _, err := esphttp.NormalizeGroupMetadata(metadata); err != nil {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor metadata: %v", errInviteMalformed, err)
	}
	if !fleetControlMetadataMatches(metadata, desc.FleetID) {
		return joinInput{}, true, fmt.Errorf("%w: fleet invite descriptor metadata does not match fleet_id", errInviteMalformed)
	}
	capability := desc.Capability
	return joinInput{capability: &capability, groupMetadata: metadata}, true, nil
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
	pub, err := defaultMootPinnedPublicKey()
	if err != nil {
		return joinInput{}, true, err
	}
	if err := defaultmoot.Verify(desc, pub); err != nil {
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

func defaultMootPinnedPublicKey() (ed25519.PublicKey, error) {
	cfg, err := defaultmoot.LoadConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return cfg.PinnedPublicKey, nil
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
func (n *notifyingStore) IterMessageIDsInIDRange(ctx context.Context, gid entmoot.GroupID, loID, hiID entmoot.MessageID) ([]entmoot.MessageID, error) {
	return n.inner.IterMessageIDsInIDRange(ctx, gid, loID, hiID)
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
	if !sess.roster.IsMemberID(s.memberID) {
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
	if info, ok := sess.roster.MemberInfoByID(s.memberID); ok {
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
	head := sess.roster.Head()
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
	acceptance, err := s.runtime.AcceptMessage(ctx, gid, msg)
	if err != nil {
		return nil, &ipc.ErrorFrame{
			Type:    "error",
			Code:    ipc.CodeNotMember,
			GroupID: &gid,
			Message: "message acceptance: " + err.Error(),
		}
	}
	msg.Acceptance = &acceptance

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
	_ = ipc.EncodeAndWrite(c, &ipc.JoinGroupResp{Status: status, GroupID: session.groupID, Issuer: issuer, Members: len(session.roster.MemberIDs()), Readiness: s.joinReadinessEvent(ctx)})
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

func cloneGroupIDPtr(in *entmoot.GroupID) *entmoot.GroupID {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneNodeInfoPtr(in *entmoot.NodeInfo) *entmoot.NodeInfo {
	if in == nil {
		return nil
	}
	out := *in
	out.EntmootPubKey = append([]byte(nil), in.EntmootPubKey...)
	if in.MemberID != nil {
		memberID := *in.MemberID
		out.MemberID = &memberID
	}
	return &out
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

func ipcCodeForJoinResolveError(err error) ipc.ErrorCode {
	switch classifyJoinOpenInviteError(err) {
	case exitInvalidArgument:
		return ipc.CodeInvalidArgument
	case exitNotMember:
		return ipc.CodeNotMember
	case exitGroupNotFound:
		return ipc.CodeGroupNotFound
	default:
		return ipc.CodeUnavailable
	}
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
	if gid == (entmoot.GroupID{}) || len(req.TargetPublicKey) != ed25519.PublicKeySize {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "group_id and target_public_key are required"})
		return
	}
	session, ok := s.runtime.Get(gid)
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group not joined"})
		return
	}
	founder, ok := session.roster.Founder()
	if !ok || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "invite_create requires the local founder identity"})
		return
	}
	founderBinding, err := libp2ptransport.BindingFromPublicKey(founder.EntmootPubKey)
	if err != nil || founderBinding.PeerID != s.runtime.host.ID() {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "founder identity does not match libp2p host"})
		return
	}
	founder.MemberID = &founderBinding.MemberID
	founder.PeerID = founderBinding.PeerID.String()
	targetBinding, err := libp2ptransport.BindingFromPublicKey(req.TargetPublicKey)
	if err != nil {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: err.Error()})
		return
	}
	if len(req.BootstrapMultiaddrs) == 0 {
		req.BootstrapMultiaddrs = make([]string, 0, len(s.runtime.host.Addrs()))
		for _, address := range s.runtime.host.Addrs() {
			req.BootstrapMultiaddrs = append(req.BootstrapMultiaddrs, address.Encapsulate(multiaddr.StringCast("/p2p/"+founderBinding.PeerID.String())).String())
		}
	}
	allowedAddresses := make([]string, 0, len(req.BootstrapMultiaddrs))
	for _, raw := range req.BootstrapMultiaddrs {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "invalid bootstrap multiaddr"})
			return
		}
		info, err := libpeer.AddrInfoFromP2pAddr(address)
		if err != nil || info.ID != founderBinding.PeerID {
			_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "bootstrap address does not name the founder host"})
			return
		}
		allowedAddresses = append(allowedAddresses, address.String())
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
		TargetMemberID:    targetBinding.MemberID,
		TargetPeerID:      targetBinding.PeerID.String(),
		Founder:           founder,
		RosterHead:        session.roster.Head(),
		AllowedPeerIDs:    []string{founderBinding.PeerID.String()},
		AllowedMultiaddrs: allowedAddresses,
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
	_ = ipc.EncodeAndWrite(c, &ipc.InviteCreateResp{Status: "created", GroupID: gid, Capability: capability, RosterHead: session.roster.Head(), Members: len(session.roster.MemberIDs())})
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
	if founder.MemberID == nil || *founder.MemberID != s.memberID || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
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
	_ = ipc.EncodeAndWrite(c, &ipc.InviteAuthorityCheckResp{
		Status:     "ok",
		GroupID:    gid,
		RosterHead: sess.roster.Head(),
		Members:    len(sess.roster.MemberIDs()),
	})
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
	founder, ok := sess.roster.Founder()
	if !ok {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeGroupNotFound, GroupID: &gid, Message: "group has no founder"})
		return
	}
	if founder.MemberID == nil || *founder.MemberID != s.memberID || !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeNotMember, GroupID: &gid, Message: "member_remove requires the local founder identity"})
		return
	}
	if founder.MemberID != nil && *req.Target.MemberID == *founder.MemberID {
		_ = ipc.EncodeAndWrite(c, &ipc.ErrorFrame{Type: "error", Code: ipc.CodeInvalidArgument, GroupID: &gid, Message: "cannot remove group founder"})
		return
	}
	unlock := lockESPInviteRoster(gid)
	existing, ok := sess.roster.MemberInfoByID(*req.Target.MemberID)
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
	head := sess.roster.Head()
	members := len(sess.roster.MemberIDs())
	unlock()
	_ = ipc.EncodeAndWrite(c, &ipc.MemberRemoveResp{Status: "removed", GroupID: gid, RosterHead: head, Members: members})
}

// handleInfo assembles a full InfoResp snapshot from live state.
func (s *ipcServer) handleInfo(ctx context.Context, c net.Conn) {
	pub := append([]byte(nil), s.identity.PublicKey...)
	if gid, ok := s.runtime.SingleGroup(); ok {
		if sess, ok := s.runtime.Get(gid); ok {
			pub, _ = pubkeyFromRoster(sess.roster, s.memberID, s.identity.PublicKey)
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
			members = len(sess.roster.MemberIDs())
		} else {
			// For groups outside the daemon's active roster, peek at
			// the existing roster file directly. Empty/orphan roster
			// shells are not joined groups and should not leak through
			// info after a failed live join attempt.
			r, ok, err := openExistingRosterLog(s.dataDir, gid)
			if err != nil {
				slog.Warn("info: open roster",
					slog.String("group", gid.String()),
					slog.String("err", err.Error()))
				continue
			}
			if !ok {
				continue
			}
			if !rosterHasLocalMemberIdentity(r, s.memberID, s.identity.PublicKey) {
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

// pubkeyFromRoster returns the locally-stored pubkey from the membership
// projection, falling back to fallback if absent.
func pubkeyFromRoster(r *roster.RosterLog, id entmoot.MemberID, fallback []byte) ([]byte, bool) {
	if info, ok := r.MemberInfoByID(id); ok && len(info.EntmootPubKey) > 0 {
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
