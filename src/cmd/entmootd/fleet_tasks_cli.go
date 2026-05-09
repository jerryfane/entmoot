package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/keystore"
)

type fleetTasksFlags struct {
	espURL string
	fleet  string
	group  string
	status string
	task   string
	title  string
	desc   string
	mode   string
	nodeID uint64
	text   string
}

type fleetTasksClient struct {
	baseURL  *url.URL
	nodeID   entmoot.NodeID
	identity *keystore.Identity
	client   *http.Client
}

func cmdFleetTasks(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd fleet tasks <list|show|create|approve|assign|claim|submit|complete|reject|cancel> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "fleet tasks: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	if args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd fleet tasks <list|show|create|approve|assign|claim|submit|complete|reject|cancel> [flags]")
		return exitOK
	}
	switch args[0] {
	case "list":
		return cmdFleetTasksList(gf, args[1:])
	case "show":
		return cmdFleetTasksShow(gf, args[1:])
	case "create":
		return cmdFleetTasksCreate(gf, args[1:])
	case "approve", "assign", "claim", "submit", "complete", "reject", "cancel":
		return cmdFleetTasksMutate(gf, args[0], args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fleet tasks: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdFleetTasksList(gf *globalFlags, args []string) int {
	fs, cfg := newFleetTasksFlagSet("fleet tasks list")
	fs.StringVar(&cfg.status, "status", "", "optional task status filter")
	if code, ok := parseFleetTasksFlags(fs, args); !ok {
		return code
	}
	client, fleetID, code, ok := prepareFleetTasksClient(gf, cfg)
	if !ok {
		return code
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/tasks"
	query := url.Values{}
	if strings.TrimSpace(cfg.status) != "" {
		query.Set("status", strings.TrimSpace(cfg.status))
	}
	return client.doAndPrint(context.Background(), http.MethodGet, path, query, nil, "")
}

func cmdFleetTasksShow(gf *globalFlags, args []string) int {
	fs, cfg := newFleetTasksFlagSet("fleet tasks show")
	fs.StringVar(&cfg.task, "task", "", "task id")
	if code, ok := parseFleetTasksFlags(fs, args); !ok {
		return code
	}
	if strings.TrimSpace(cfg.task) == "" {
		fmt.Fprintln(os.Stderr, "fleet tasks show: -task is required")
		return exitInvalidArgument
	}
	client, fleetID, code, ok := prepareFleetTasksClient(gf, cfg)
	if !ok {
		return code
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/tasks/" + url.PathEscape(cfg.task)
	return client.doAndPrint(context.Background(), http.MethodGet, path, nil, nil, "")
}

func cmdFleetTasksCreate(gf *globalFlags, args []string) int {
	fs, cfg := newFleetTasksFlagSet("fleet tasks create")
	fs.StringVar(&cfg.title, "title", "", "task title")
	fs.StringVar(&cfg.desc, "description", "", "task description")
	fs.StringVar(&cfg.mode, "mode", esphttp.FleetTaskModeOpenSubmission, "task mode: open_submission, first_claim, or direct_assignee")
	fs.Uint64Var(&cfg.nodeID, "assignee-node-id", 0, "active member node id for direct tasks")
	if code, ok := parseFleetTasksFlags(fs, args); !ok {
		return code
	}
	if strings.TrimSpace(cfg.title) == "" {
		fmt.Fprintln(os.Stderr, "fleet tasks create: -title is required")
		return exitInvalidArgument
	}
	client, fleetID, code, ok := prepareFleetTasksClient(gf, cfg)
	if !ok {
		return code
	}
	body := map[string]any{
		"title":       cfg.title,
		"description": cfg.desc,
		"mode":        cfg.mode,
	}
	if cfg.nodeID != 0 {
		body["assignee_node_id"] = cfg.nodeID
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/tasks"
	return client.doAndPrint(context.Background(), http.MethodPost, path, nil, body, randomIdempotencyKey("fleet-task-create"))
}

func cmdFleetTasksMutate(gf *globalFlags, action string, args []string) int {
	fs, cfg := newFleetTasksFlagSet("fleet tasks " + action)
	fs.StringVar(&cfg.task, "task", "", "task id")
	if action == esphttp.FleetTaskActionAssign {
		fs.Uint64Var(&cfg.nodeID, "assignee-node-id", 0, "active member node id")
	}
	if action == esphttp.FleetTaskActionSubmit {
		fs.StringVar(&cfg.text, "content", "", "submission content")
	}
	if code, ok := parseFleetTasksFlags(fs, args); !ok {
		return code
	}
	if strings.TrimSpace(cfg.task) == "" {
		fmt.Fprintf(os.Stderr, "fleet tasks %s: -task is required\n", action)
		return exitInvalidArgument
	}
	body := map[string]any{}
	switch action {
	case esphttp.FleetTaskActionAssign:
		if cfg.nodeID == 0 {
			fmt.Fprintln(os.Stderr, "fleet tasks assign: -assignee-node-id is required")
			return exitInvalidArgument
		}
		body["assignee_node_id"] = cfg.nodeID
	case esphttp.FleetTaskActionSubmit:
		if strings.TrimSpace(cfg.text) == "" {
			fmt.Fprintln(os.Stderr, "fleet tasks submit: -content is required")
			return exitInvalidArgument
		}
		body["content"] = cfg.text
	default:
		body = nil
	}
	client, fleetID, code, ok := prepareFleetTasksClient(gf, cfg)
	if !ok {
		return code
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/tasks/" + url.PathEscape(cfg.task) + "/" + url.PathEscape(action)
	return client.doAndPrint(context.Background(), http.MethodPost, path, nil, body, randomIdempotencyKey("fleet-task-"+action))
}

func newFleetTasksFlagSet(name string) (*flag.FlagSet, *fleetTasksFlags) {
	cfg := &fleetTasksFlags{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.StringVar(&cfg.espURL, "esp-url", os.Getenv("ENTMOOT_ESP_URL"), "ESP base URL (defaults to ENTMOOT_ESP_URL)")
	fs.StringVar(&cfg.fleet, "fleet", "", "fleet id")
	fs.StringVar(&cfg.group, "group", "", "fleet control group id")
	return fs, cfg
}

func parseFleetTasksFlags(fs *flag.FlagSet, args []string) (int, bool) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK, false
		}
		return exitInvalidArgument, false
	}
	return exitOK, true
}

func prepareFleetTasksClient(gf *globalFlags, cfg *fleetTasksFlags) (*fleetTasksClient, string, int, bool) {
	if cfg == nil {
		return nil, "", exitInvalidArgument, false
	}
	if strings.TrimSpace(cfg.espURL) == "" {
		fmt.Fprintln(os.Stderr, "fleet tasks: -esp-url or ENTMOOT_ESP_URL is required")
		return nil, "", exitInvalidArgument, false
	}
	if strings.TrimSpace(cfg.fleet) == "" && strings.TrimSpace(cfg.group) == "" {
		fmt.Fprintln(os.Stderr, "fleet tasks: either -fleet or -group is required")
		return nil, "", exitInvalidArgument, false
	}
	client, code, ok := newFleetTasksClient(gf, cfg.espURL)
	if !ok {
		return nil, "", code, false
	}
	fleetID := strings.TrimSpace(cfg.fleet)
	if fleetID == "" {
		resolved, code, ok := client.resolveFleetID(context.Background(), cfg.group)
		if !ok {
			return nil, "", code, false
		}
		fleetID = resolved
	}
	return client, fleetID, exitOK, true
}

func newFleetTasksClient(gf *globalFlags, rawBase string) (*fleetTasksClient, int, bool) {
	base, err := url.Parse(strings.TrimSpace(rawBase))
	if err != nil || base.Scheme == "" || base.Host == "" {
		fmt.Fprintf(os.Stderr, "fleet tasks: invalid -esp-url %q\n", rawBase)
		return nil, exitInvalidArgument, false
	}
	setupRes, err := setup(gf)
	if err != nil {
		slog.Error("fleet tasks: setup", slog.String("err", err.Error()))
		return nil, exitTransport, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := infoOverIPCContext(ctx, controlSocketPath(gf.data))
	if err != nil || info.PilotNodeID == 0 {
		if err == nil {
			err = errors.New("running daemon did not report a Pilot node id")
		}
		fmt.Fprintf(os.Stderr, "fleet tasks: running Entmoot daemon with Pilot identity is required: %v\n", err)
		return nil, exitTransport, false
	}
	return &fleetTasksClient{
		baseURL:  base,
		nodeID:   info.PilotNodeID,
		identity: setupRes.identity,
		client:   &http.Client{Timeout: 30 * time.Second},
	}, exitOK, true
}

func (c *fleetTasksClient) resolveFleetID(ctx context.Context, rawGroup string) (string, int, bool) {
	gid, err := decodeGroupID(rawGroup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet tasks: -group: %v\n", err)
		return "", exitInvalidArgument, false
	}
	query := url.Values{}
	query.Set("control_group_id", gid.String())
	var resp struct {
		Fleets []esphttp.FleetRecord `json:"fleets"`
	}
	if code, ok := c.doJSON(ctx, http.MethodGet, "/v1/fleets", query, nil, "", &resp); !ok {
		return "", code, false
	}
	if len(resp.Fleets) != 1 {
		fmt.Fprintf(os.Stderr, "fleet tasks: expected one fleet for group %s, got %d\n", gid.String(), len(resp.Fleets))
		return "", exitGroupNotFound, false
	}
	return resp.Fleets[0].FleetID, exitOK, true
}

func (c *fleetTasksClient) doAndPrint(ctx context.Context, method, path string, query url.Values, body any, idempotencyKey string) int {
	var raw json.RawMessage
	code, ok := c.doJSON(ctx, method, path, query, body, idempotencyKey, &raw)
	if !ok {
		return code
	}
	data, err := json.Marshal(raw)
	if err != nil {
		slog.Error("fleet tasks: marshal response", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

func (c *fleetTasksClient) doJSON(ctx context.Context, method, path string, query url.Values, body any, idempotencyKey string, out any) (int, bool) {
	req, err := c.newRequest(ctx, method, path, query, body, idempotencyKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet tasks: request: %v\n", err)
		return exitInvalidArgument, false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet tasks: %s %s: %v\n", method, req.URL.String(), err)
		return exitTransport, false
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet tasks: read response: %v\n", err)
		return exitTransport, false
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if len(data) > 0 {
			fmt.Fprintln(os.Stderr, string(data))
		} else {
			fmt.Fprintf(os.Stderr, "fleet tasks: HTTP %d\n", resp.StatusCode)
		}
		return exitTransport, false
	}
	if out != nil {
		if raw, ok := out.(*json.RawMessage); ok {
			*raw = append((*raw)[:0], data...)
			return exitOK, true
		}
		if err := json.Unmarshal(data, out); err != nil {
			fmt.Fprintf(os.Stderr, "fleet tasks: decode response: %v\n", err)
			return exitTransport, false
		}
	}
	return exitOK, true
}

func (c *fleetTasksClient) newRequest(ctx context.Context, method, path string, query url.Values, body any, idempotencyKey string) (*http.Request, error) {
	u := *c.baseURL
	u.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	u.RawQuery = query.Encode()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	timestampMS := time.Now().UnixMilli()
	nonce := randomNonce()
	input := esphttp.MemberSigningInput(method, req.URL.RequestURI(), c.nodeID, c.identity.PublicKey, timestampMS, nonce, data)
	req.Header.Set("X-Entmoot-Member-Node-ID", strconv.FormatUint(uint64(c.nodeID), 10))
	req.Header.Set("X-Entmoot-Member-Pubkey", base64.StdEncoding.EncodeToString(c.identity.PublicKey))
	req.Header.Set("X-Entmoot-Timestamp-Ms", strconv.FormatInt(timestampMS, 10))
	req.Header.Set("X-Entmoot-Nonce", nonce)
	req.Header.Set("X-Entmoot-Member-Signature", base64.StdEncoding.EncodeToString(c.identity.Sign([]byte(input))))
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return req, nil
}

func randomNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func randomIdempotencyKey(prefix string) string {
	return prefix + "-" + randomNonce()
}
