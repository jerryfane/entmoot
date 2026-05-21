package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

const (
	groupVisibilityPrivate  = "private"
	groupVisibilityUnlisted = "unlisted"
	groupVisibilityPublic   = "public"

	groupJoinModeInviteOnly = "invite_only"
	groupJoinModeOpenInvite = "open_invite"
)

type stringListFlag []string

func (f *stringListFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

type groupCreateOptions struct {
	Name         string
	Description  string
	Tags         []string
	Visibility   string
	JoinMode     string
	Policy       *policy.Policy
	PolicySource string
	JSONOutput   bool
	Help         bool
}

type groupCreateState struct {
	GroupID       entmoot.GroupID
	Founder       entmoot.NodeInfo
	Metadata      json.RawMessage
	Policy        *policy.Policy
	PolicySource  string
	PolicySummary string
	Invite        entmoot.Invite
}

type groupCreateOpenInviteOutput struct {
	Token       string `json:"token"`
	TokenHash   string `json:"token_hash"`
	Link        string `json:"link,omitempty"`
	IssuerURL   string `json:"issuer_url,omitempty"`
	MaxUses     int    `json:"max_uses"`
	ExpiresAtMS int64  `json:"expires_at_ms,omitempty"`
}

type groupCreateOutput struct {
	GroupID                     entmoot.GroupID              `json:"group_id"`
	Founder                     entmoot.NodeInfo             `json:"founder"`
	Name                        string                       `json:"name"`
	Description                 string                       `json:"description,omitempty"`
	Tags                        []string                     `json:"tags,omitempty"`
	Visibility                  string                       `json:"visibility"`
	JoinMode                    string                       `json:"join_mode"`
	PolicyConfigured            bool                         `json:"policy_configured"`
	PolicySource                string                       `json:"policy_source"`
	PolicySummary               string                       `json:"policy_summary,omitempty"`
	OpenInvite                  *groupCreateOpenInviteOutput `json:"open_invite,omitempty"`
	NextPublicDescriptorCommand string                       `json:"next_public_descriptor_command,omitempty"`
}

// cmdGroup dispatches `group <op>`.
func cmdGroup(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "group: missing op (want: create, policy, public)")
		return exitInvalidArgument
	}
	switch args[0] {
	case "create":
		return cmdGroupCreate(gf, args[1:])
	case "policy":
		return cmdGroupPolicy(gf, args[1:])
	case "public":
		return cmdGroupPublic(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "group: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

// cmdGroupCreate generates a fresh GroupID, opens an empty roster and
// SQLite store for it, and writes the genesis entry. Emits a JSON object
// on stdout carrying the new group id plus founder NodeInfo.
func cmdGroupCreate(gf *globalFlags, args []string) int {
	opts, code := parseGroupCreateOptions(args)
	if code != exitOK {
		return code
	}
	if opts.Help {
		return exitOK
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("group create: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	nodeCtx, nodeCancel := withTimeout(5 * time.Second)
	nodeID, err := groupCreatePilotNodeID(nodeCtx, gf.socket)
	nodeCancel()
	if err != nil {
		slog.Error("group create: pilot", slog.String("err", err.Error()))
		return exitTransport
	}

	var gid entmoot.GroupID
	if _, err := rand.Read(gid[:]); err != nil {
		slog.Error("group create: rand", slog.String("err", err.Error()))
		return exitTransport
	}

	ctx, cancel := withTimeout(10 * time.Second)
	defer cancel()
	state, rollback, err := createGroupLocalState(ctx, groupCreateLocalStateInput{
		DataDir:      s.dataDir,
		Identity:     s.identity,
		FounderNode:  nodeID,
		GroupID:      gid,
		Name:         opts.Name,
		Description:  opts.Description,
		Tags:         opts.Tags,
		Visibility:   opts.Visibility,
		JoinMode:     opts.JoinMode,
		Policy:       opts.Policy,
		PolicySource: opts.PolicySource,
		NowMS:        time.Now().UnixMilli(),
	})
	if err != nil {
		if rollback != nil {
			rollback()
		}
		slog.Error("group create: local state", slog.String("err", err.Error()))
		return exitTransport
	}
	committed := false
	defer func() {
		if !committed {
			rollback()
		}
	}()

	openInvite, err := maybeCreateGroupOpenInvite(ctx, gf, state)
	if err != nil {
		fmt.Fprintf(os.Stderr, "group create: %v\n", err)
		return exitTransport
	}

	slog.Info("group created",
		slog.String("group_id", state.GroupID.String()),
		slog.String("name", opts.Name),
		slog.String("visibility", opts.Visibility),
		slog.String("join_mode", opts.JoinMode),
		slog.Uint64("founder", uint64(nodeID)))

	out := groupCreateOutput{
		GroupID:          state.GroupID,
		Founder:          state.Founder,
		Name:             opts.Name,
		Description:      opts.Description,
		Tags:             normalizeGroupTags(opts.Tags),
		Visibility:       opts.Visibility,
		JoinMode:         opts.JoinMode,
		PolicyConfigured: opts.Policy != nil,
		PolicySource:     opts.PolicySource,
		PolicySummary:    state.PolicySummary,
		OpenInvite:       openInvite,
	}
	if opts.Visibility == groupVisibilityPublic {
		out.NextPublicDescriptorCommand = fmt.Sprintf("entmootd group public publish -group %s -esp-url <ESP_URL> --json", state.GroupID.String())
	}
	data, err := json.Marshal(out)
	if err != nil {
		slog.Error("group create: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	committed = true
	return exitOK
}

func groupCreatePilotNodeID(ctx context.Context, socketPath string) (entmoot.NodeID, error) {
	drv, err := ipcclient.Connect(socketPath)
	if err != nil {
		return 0, fmt.Errorf("pilot: connect %q: %w", socketPath, err)
	}
	defer drv.Close()
	info, err := drv.InfoStruct(ctx)
	if err != nil {
		return 0, fmt.Errorf("pilot: info: %w", err)
	}
	if info.NodeID == 0 {
		return 0, fmt.Errorf("pilot: info: missing node_id")
	}
	return entmoot.NodeID(info.NodeID), nil
}

func parseGroupCreateOptions(args []string) (groupCreateOptions, int) {
	fs := flag.NewFlagSet("group create", flag.ContinueOnError)
	name := fs.String("name", "", "informational group name (required)")
	description := fs.String("description", "", "group description")
	visibility := fs.String("visibility", groupVisibilityPrivate, "visibility: private, unlisted, public")
	joinMode := fs.String("join-mode", groupJoinModeInviteOnly, "join mode: invite_only, open_invite")
	policySource := fs.String("policy", "preset:standard", "policy: preset:standard, preset:relaxed, none, file:policy.json")
	jsonOut := fs.Bool("json", false, "print JSON")
	var tags stringListFlag
	fs.Var(&tags, "tag", "group tag; repeatable")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return groupCreateOptions{Help: true}, exitOK
		}
		return groupCreateOptions{}, exitInvalidArgument
	}
	opts := groupCreateOptions{
		Name:        strings.TrimSpace(*name),
		Description: strings.TrimSpace(*description),
		Tags:        normalizeGroupTags(tags),
		Visibility:  normalizeGroupVisibility(*visibility),
		JoinMode:    normalizeGroupJoinMode(*joinMode),
		JSONOutput:  *jsonOut,
	}
	if opts.Name == "" {
		fmt.Fprintln(os.Stderr, "group create: -name is required")
		return groupCreateOptions{}, exitInvalidArgument
	}
	if opts.Visibility == "" {
		fmt.Fprintf(os.Stderr, "group create: invalid -visibility %q (want: private, unlisted, public)\n", *visibility)
		return groupCreateOptions{}, exitInvalidArgument
	}
	if opts.JoinMode == "" {
		fmt.Fprintf(os.Stderr, "group create: invalid -join-mode %q (want: invite_only, open_invite)\n", *joinMode)
		return groupCreateOptions{}, exitInvalidArgument
	}
	resolved, err := resolveGroupCreatePolicy(*policySource)
	if err != nil {
		fmt.Fprintf(os.Stderr, "group create: -policy: %v\n", err)
		return groupCreateOptions{}, exitInvalidArgument
	}
	opts.Policy = resolved.Policy
	opts.PolicySource = resolved.Source
	return opts, exitOK
}

func normalizeGroupVisibility(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case groupVisibilityPrivate, "":
		return groupVisibilityPrivate
	case groupVisibilityUnlisted:
		return groupVisibilityUnlisted
	case groupVisibilityPublic:
		return groupVisibilityPublic
	default:
		return ""
	}
}

func normalizeGroupJoinMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case groupJoinModeInviteOnly, "":
		return groupJoinModeInviteOnly
	case groupJoinModeOpenInvite:
		return groupJoinModeOpenInvite
	default:
		return ""
	}
}

func resolveGroupCreatePolicy(raw string) (policy.SourceResolution, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = "preset:standard"
	}
	switch {
	case strings.HasPrefix(raw, "preset:"):
		return policy.ResolveSource(strings.TrimPrefix(raw, "preset:"), "")
	case raw == policy.PresetNone:
		return policy.ResolveSource(policy.PresetNone, "")
	case strings.HasPrefix(raw, "file:"):
		return policy.ResolveSource("", strings.TrimPrefix(raw, "file:"))
	default:
		return policy.SourceResolution{}, fmt.Errorf("unsupported policy source %q (want: preset:standard, preset:relaxed, none, file:policy.json)", raw)
	}
}

type groupCreateLocalStateInput struct {
	DataDir      string
	Identity     *keystore.Identity
	FounderNode  entmoot.NodeID
	GroupID      entmoot.GroupID
	Name         string
	Description  string
	Tags         []string
	Visibility   string
	JoinMode     string
	Policy       *policy.Policy
	PolicySource string
	NowMS        int64
}

func createGroupLocalState(ctx context.Context, in groupCreateLocalStateInput) (groupCreateState, func(), error) {
	if in.Identity == nil {
		return groupCreateState{}, nil, errors.New("identity is required")
	}
	now := in.NowMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	metadata, err := normalizeGroupMetadata(groupCreatePayload{
		Name:        in.Name,
		Description: in.Description,
		Tags:        in.Tags,
		Visibility:  in.Visibility,
		JoinMode:    in.JoinMode,
	})
	if err != nil {
		return groupCreateState{}, nil, err
	}
	groupPath := groupDirPath(in.DataDir, in.GroupID)
	groupPreexisted := pathExists(groupPath)
	metadataStore, err := esphttp.OpenSQLiteStateStore(in.DataDir)
	if err != nil {
		return groupCreateState{}, nil, err
	}
	defer metadataStore.Close()
	policyStore, err := policy.OpenFileStore(in.DataDir)
	if err != nil {
		return groupCreateState{}, nil, err
	}
	previousPolicy, hadPreviousPolicy, err := policyStore.Get(ctx, in.GroupID)
	if err != nil {
		return groupCreateState{}, nil, err
	}
	previousMetadata, hadPreviousMetadata, err := metadataStore.GetGroupMetadata(ctx, in.GroupID)
	if err != nil {
		return groupCreateState{}, nil, err
	}
	rollback := func() {
		if hadPreviousPolicy {
			_ = policyStore.Put(context.Background(), in.GroupID, previousPolicy)
		} else {
			_ = policyStore.Delete(context.Background(), in.GroupID)
		}
		if store, err := esphttp.OpenSQLiteStateStore(in.DataDir); err == nil {
			defer store.Close()
			if hadPreviousMetadata {
				_ = store.SetGroupMetadata(context.Background(), in.GroupID, previousMetadata)
			} else {
				_ = store.DeleteGroupMetadata(context.Background(), in.GroupID)
			}
		}
		if !groupPreexisted {
			_ = os.RemoveAll(groupPath)
		}
	}
	st, err := store.OpenSQLite(in.DataDir)
	if err != nil {
		return groupCreateState{}, rollback, err
	}
	defer st.Close()
	r, err := roster.OpenJSONL(in.DataDir, in.GroupID)
	if err != nil {
		return groupCreateState{}, rollback, err
	}
	defer r.Close()
	founder := entmoot.NodeInfo{
		PilotNodeID:   in.FounderNode,
		EntmootPubKey: append([]byte(nil), in.Identity.PublicKey...),
	}
	if err := r.Genesis(in.Identity, founder, now); err != nil {
		return groupCreateState{}, rollback, err
	}
	if err := metadataStore.SetGroupMetadata(ctx, in.GroupID, metadata); err != nil {
		return groupCreateState{}, rollback, err
	}
	if in.Policy != nil {
		if err := policyStore.Put(ctx, in.GroupID, *in.Policy); err != nil {
			return groupCreateState{}, rollback, err
		}
	} else if err := policyStore.Delete(ctx, in.GroupID); err != nil {
		return groupCreateState{}, rollback, err
	}
	root, err := st.MerkleRoot(ctx, in.GroupID)
	if err != nil {
		return groupCreateState{}, rollback, err
	}
	invite := entmoot.Invite{
		GroupID:    in.GroupID,
		Founder:    founder,
		RosterHead: r.Head(),
		MerkleRoot: root,
		IssuedAt:   now,
		ValidUntil: now + int64((24*time.Hour)/time.Millisecond),
		Issuer:     founder,
	}
	if err := signInvite(in.Identity, &invite); err != nil {
		return groupCreateState{}, rollback, err
	}
	state := groupCreateState{
		GroupID:      in.GroupID,
		Founder:      founder,
		Metadata:     metadata,
		Policy:       clonePolicyPtr(in.Policy),
		PolicySource: in.PolicySource,
		Invite:       invite,
	}
	if in.Policy != nil {
		state.PolicySummary = policy.Summary(*in.Policy)
	}
	return state, rollback, nil
}

func maybeCreateGroupOpenInvite(ctx context.Context, gf *globalFlags, state groupCreateState) (*groupCreateOpenInviteOutput, error) {
	var meta map[string]any
	if err := json.Unmarshal(state.Metadata, &meta); err != nil {
		return nil, err
	}
	joinMode, _ := meta["join_mode"].(string)
	if joinMode != groupJoinModeOpenInvite {
		return nil, nil
	}
	issuerURL, err := groupCreateOpenInviteIssuerURL()
	if err != nil {
		return nil, err
	}
	sockPath := controlSocketPath(gf.data)
	if !controlSocketAlive(sockPath, 200*time.Millisecond) {
		return nil, errors.New("open_invite join mode requires a running entmootd daemon; start `entmootd serve` and rerun group create")
	}
	exec := espOperationExecutor{socketPath: sockPath, timeout: 30 * time.Second}
	if _, err := exec.checkInviteAuthorityOverIPC(ctx, &ipc.InviteAuthorityCheckReq{GroupID: state.GroupID, CandidateInvite: &state.Invite}); err != nil {
		return nil, fmt.Errorf("open invite authority unavailable: %w", err)
	}
	cleanupActivated := func(err error) error {
		if _, cleanupErr := exec.deactivateGroupOverIPC(context.Background(), &ipc.GroupDeactivateReq{GroupID: state.GroupID}); cleanupErr != nil {
			var opErr *esphttp.OperationError
			if errors.As(cleanupErr, &opErr) && opErr.Code == "group_not_found" {
				return err
			}
			return fmt.Errorf("%w; daemon cleanup failed: %v", err, cleanupErr)
		}
		return err
	}
	if resp, frame, err := joinGroupReqOverIPC(ctx, sockPath, &ipc.JoinGroupReq{
		Invite:        state.Invite,
		GroupMetadata: state.Metadata,
		GroupPolicy:   clonePolicyPtr(state.Policy),
	}, defaultJoinTimeout); err != nil {
		return nil, cleanupActivated(fmt.Errorf("activate group through daemon: %w", err))
	} else if frame != nil {
		return nil, cleanupActivated(fmt.Errorf("activate group through daemon: %s: %s", frame.Code, frame.Message))
	} else if resp == nil {
		return nil, cleanupActivated(errors.New("activate group through daemon: empty response"))
	}
	espState, err := esphttp.OpenSQLiteStateStore(gf.data)
	if err != nil {
		return nil, cleanupActivated(err)
	}
	defer espState.Close()
	token, tokenHash, err := esphttp.NewOpenInviteToken()
	if err != nil {
		return nil, cleanupActivated(err)
	}
	rec, err := espState.CreateOpenInvite(ctx, esphttp.OpenInviteRecord{
		TokenHash:   tokenHash,
		GroupID:     state.GroupID,
		MaxUses:     esphttp.OpenInviteUnlimitedMaxUses,
		CreatedAtMS: time.Now().UnixMilli(),
	})
	if err != nil {
		return nil, cleanupActivated(err)
	}
	out := &groupCreateOpenInviteOutput{
		Token:       token,
		TokenHash:   rec.TokenHash,
		Link:        fmt.Sprintf("entmoot://open-invite?issuer=%s&token=%s", url.QueryEscape(issuerURL), url.QueryEscape(token)),
		IssuerURL:   issuerURL,
		MaxUses:     rec.MaxUses,
		ExpiresAtMS: rec.ExpiresAtMS,
	}
	if groupCreateStateVisibility(state) == groupVisibilityPublic {
		if err := persistGroupCreatePublicOpenInvite(ctx, espState, state, out); err != nil {
			_, _, _ = espState.RevokeOpenInvite(context.Background(), rec.TokenHash, time.Now().UnixMilli())
			return nil, cleanupActivated(fmt.Errorf("persist public open invite descriptor metadata: %w", err))
		}
	}
	return out, nil
}

func groupCreateStateVisibility(state groupCreateState) string {
	var meta map[string]any
	if err := json.Unmarshal(state.Metadata, &meta); err != nil {
		return ""
	}
	visibility, _ := meta["visibility"].(string)
	return visibility
}

func persistGroupCreatePublicOpenInvite(ctx context.Context, espState *esphttp.SQLiteStateStore, state groupCreateState, invite *groupCreateOpenInviteOutput) error {
	if espState == nil || invite == nil {
		return errors.New("open invite metadata store is not configured")
	}
	meta, err := decodeGroupMetadataObject(state.Metadata)
	if err != nil {
		return err
	}
	meta["open_invite"] = map[string]any{
		"issuer_url": invite.IssuerURL,
		"token":      invite.Token,
		"link":       invite.Link,
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	normalized, err := esphttp.NormalizeGroupMetadata(raw)
	if err != nil {
		return err
	}
	return espState.SetGroupMetadata(ctx, state.GroupID, normalized)
}

func groupCreateOpenInviteIssuerURL() (string, error) {
	raw := strings.TrimSpace(os.Getenv("ENTMOOT_ESP_URL"))
	if raw == "" {
		return "", errors.New("open_invite join mode requires ENTMOOT_ESP_URL so the CLI can emit a redeemable open-invite link")
	}
	issuer, _, err := parseOpenInviteAcceptPayload(openInviteAcceptPayload{
		IssuerURL: raw,
		Token:     "validation-token",
	})
	if err != nil {
		return "", fmt.Errorf("open_invite join mode requires ENTMOOT_ESP_URL to be redeemable by entmoot join: %w", err)
	}
	return strings.TrimRight(issuer.String(), "/"), nil
}
