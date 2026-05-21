package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
	"entmoot/pkg/entmoot/roster"
)

type groupPublicOptions struct {
	GroupID    entmoot.GroupID
	ESPURL     string
	JSONOutput bool
	Help       bool
}

type groupPublicPublishResult struct {
	Status     string                `json:"status"`
	GroupID    entmoot.GroupID       `json:"group_id"`
	ESPURL     string                `json:"esp_url"`
	Descriptor publicmoot.Descriptor `json:"descriptor"`
	Response   json.RawMessage       `json:"response,omitempty"`
}

func cmdGroupPublic(gf *globalFlags, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "group public: missing op (want: descriptor, publish)")
		return exitInvalidArgument
	}
	op := args[0]
	switch op {
	case "descriptor", "publish":
	default:
		fmt.Fprintf(os.Stderr, "group public: unknown op %q\n", op)
		return exitInvalidArgument
	}
	opts, code := parseGroupPublicOptions(op, args[1:])
	if code != exitOK {
		return code
	}
	if opts.Help {
		return exitOK
	}
	ctx, cancel := withTimeout(10 * time.Second)
	defer cancel()
	desc, err := buildPublicMootDescriptor(ctx, gf, opts.GroupID, time.Now().UnixMilli())
	if err != nil {
		fmt.Fprintf(os.Stderr, "group public %s: %v\n", op, err)
		return groupPublicErrorExit(err)
	}
	switch op {
	case "descriptor":
		return printPublicMootDescriptor(desc, opts.JSONOutput)
	case "publish":
		resp, err := publishPublicMootDescriptor(ctx, opts.ESPURL, desc)
		if err != nil {
			fmt.Fprintf(os.Stderr, "group public publish: %v\n", err)
			return groupPublicErrorExit(err)
		}
		return printPublicMootPublishResult(groupPublicPublishResult{
			Status:     "published",
			GroupID:    desc.GroupID,
			ESPURL:     strings.TrimSpace(opts.ESPURL),
			Descriptor: desc,
			Response:   resp,
		}, opts.JSONOutput)
	default:
		return exitInvalidArgument
	}
}

func parseGroupPublicOptions(op string, args []string) (groupPublicOptions, int) {
	fs := flag.NewFlagSet("group public "+op, flag.ContinueOnError)
	groupRaw := fs.String("group", "", "group id")
	espURL := fs.String("esp-url", "", "ESP URL")
	jsonOut := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return groupPublicOptions{Help: true}, exitOK
		}
		return groupPublicOptions{}, exitInvalidArgument
	}
	gid, err := decodeGroupID(*groupRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "group public %s: -group: %v\n", op, err)
		return groupPublicOptions{}, exitInvalidArgument
	}
	opts := groupPublicOptions{
		GroupID:    gid,
		ESPURL:     strings.TrimSpace(*espURL),
		JSONOutput: *jsonOut,
	}
	if op == "publish" && opts.ESPURL == "" {
		fmt.Fprintln(os.Stderr, "group public publish: -esp-url is required")
		return groupPublicOptions{}, exitInvalidArgument
	}
	return opts, exitOK
}

func buildPublicMootDescriptor(ctx context.Context, gf *globalFlags, gid entmoot.GroupID, nowMS int64) (publicmoot.Descriptor, error) {
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	s, err := setup(gf)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	if !pathExists(groupDirPath(s.dataDir, gid)) {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: group directory is missing", errGroupPublicNotFound)
	}
	r, err := roster.OpenJSONL(s.dataDir, gid)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	defer r.Close()
	founder, ok := r.Founder()
	if !ok {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: roster founder is missing", errGroupPublicNotFound)
	}
	if !bytes.Equal(founder.EntmootPubKey, s.identity.PublicKey) {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: local identity is not the group founder", errGroupPublicForbidden)
	}
	meta, err := loadGroupPublicMetadata(ctx, s.dataDir, gid)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	visibility := metadataString(meta, "visibility")
	if visibility != groupVisibilityPublic {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: visibility is %q, want public", errGroupPublicInvalid, visibility)
	}
	name := metadataString(meta, "name")
	if name == "" {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: metadata name is required", errGroupPublicInvalid)
	}
	joinMode := metadataString(meta, "join_mode")
	if joinMode == "" {
		joinMode = groupJoinModeInviteOnly
	}
	if normalizeGroupJoinMode(joinMode) == "" {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: invalid join_mode %q", errGroupPublicInvalid, joinMode)
	}
	policyStore, err := policy.OpenFileStore(s.dataDir)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	p, ok, err := policyStore.Get(ctx, gid)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	if !ok {
		return publicmoot.Descriptor{}, fmt.Errorf("%w: group policy is not configured", errGroupPublicInvalid)
	}
	openInvite, err := metadataOpenInvite(meta, "open_invite")
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	desc := publicmoot.Descriptor{
		Type:        publicmoot.DescriptorType,
		GroupID:     gid,
		Name:        name,
		Description: metadataString(meta, "description"),
		Tags:        metadataStringSlice(meta, "tags"),
		Visibility:  visibility,
		JoinMode:    joinMode,
		OpenInvite:  openInvite,
		Policy:      p,
		Founder:     founder,
		Indexing: publicmoot.Indexing{
			Directory: true,
			Messages:  false,
		},
		UpdatedAtMS: nowMS,
	}
	signed, err := publicmoot.Sign(desc, s.identity)
	if err != nil {
		return publicmoot.Descriptor{}, err
	}
	if err := publicmoot.Verify(signed); err != nil {
		return publicmoot.Descriptor{}, err
	}
	return signed, nil
}

var (
	errGroupPublicNotFound  = errors.New("group public: group not found")
	errGroupPublicForbidden = errors.New("group public: forbidden")
	errGroupPublicInvalid   = errors.New("group public: invalid public moot")
)

func loadGroupPublicMetadata(ctx context.Context, dataDir string, gid entmoot.GroupID) (map[string]any, error) {
	state, err := esphttp.OpenSQLiteStateStore(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()
	raw, ok, err := state.GetGroupMetadata(ctx, gid)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: group metadata is missing", errGroupPublicNotFound)
	}
	meta, err := decodeGroupMetadataObject(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: metadata: %v", errGroupPublicInvalid, err)
	}
	return meta, nil
}

func metadataString(meta map[string]any, key string) string {
	value, _ := meta[key].(string)
	return strings.TrimSpace(value)
}

func metadataStringSlice(meta map[string]any, key string) []string {
	values, ok := meta[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			continue
		}
		out = append(out, text)
	}
	return normalizeGroupTags(out)
}

func metadataOpenInvite(meta map[string]any, key string) (*publicmoot.OpenInviteDescriptor, error) {
	value, ok := meta[key]
	if !ok {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: open_invite: %v", errGroupPublicInvalid, err)
	}
	var out publicmoot.OpenInviteDescriptor
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%w: open_invite: %v", errGroupPublicInvalid, err)
	}
	if strings.TrimSpace(out.IssuerURL) == "" && strings.TrimSpace(out.Token) == "" && strings.TrimSpace(out.Link) == "" {
		return nil, nil
	}
	if err := publicmoot.ValidateOpenInvite(out); err != nil {
		return nil, fmt.Errorf("%w: open_invite: %v", errGroupPublicInvalid, err)
	}
	return &out, nil
}

func publishPublicMootDescriptor(ctx context.Context, espURL string, desc publicmoot.Descriptor) (json.RawMessage, error) {
	if err := publicmoot.Verify(desc); err != nil {
		return nil, err
	}
	endpoint, err := publicMootPublishEndpoint(espURL)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		message := strings.TrimSpace(string(raw))
		if message == "" {
			message = resp.Status
		}
		return nil, fmt.Errorf("ESP %s: %s", resp.Status, message)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("ESP response is not JSON")
	}
	return append(json.RawMessage(nil), raw...), nil
}

func publicMootPublishEndpoint(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", errors.New("esp-url must be an absolute http(s) URL")
	}
	if u.User != nil {
		return "", errors.New("esp-url must not contain credentials")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && publicMootHostAllowsCleartext(u.Hostname())) {
		return "", errors.New("esp-url must use https except for localhost or .local development hosts")
	}
	u.RawQuery = ""
	u.Fragment = ""
	prefix := strings.TrimRight(u.Path, "/")
	if prefix == "" {
		u.Path = "/v1/public-moots"
	} else {
		u.Path = prefix + "/v1/public-moots"
	}
	return u.String(), nil
}

func publicMootHostAllowsCleartext(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".local")
}

func printPublicMootDescriptor(desc publicmoot.Descriptor, jsonOut bool) int {
	var (
		data []byte
		err  error
	)
	if jsonOut {
		data, err = json.Marshal(desc)
	} else {
		data, err = json.MarshalIndent(desc, "", "  ")
	}
	if err != nil {
		slog.Error("group public descriptor: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

func printPublicMootPublishResult(out groupPublicPublishResult, jsonOut bool) int {
	if jsonOut {
		data, err := json.Marshal(out)
		if err != nil {
			slog.Error("group public publish: marshal", slog.String("err", err.Error()))
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	}
	fmt.Printf("published public moot descriptor for %s to %s\n", out.GroupID.String(), out.ESPURL)
	return exitOK
}

func groupPublicErrorExit(err error) int {
	switch {
	case errors.Is(err, errGroupPublicNotFound):
		return exitGroupNotFound
	case errors.Is(err, errGroupPublicForbidden):
		return exitNotMember
	case errors.Is(err, errGroupPublicInvalid), errors.Is(err, publicmoot.ErrInvalidDescriptor), errors.Is(err, publicmoot.ErrDescriptorSignature):
		return exitInvalidArgument
	default:
		return exitTransport
	}
}
