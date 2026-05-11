package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
)

type fleetCommandsFlags struct {
	espURL                         string
	fleet                          string
	group                          string
	target                         string
	targetNodeID                   uint64
	action                         string
	argsJSON                       string
	instruction                    string
	timeoutMS                      int64
	externalActionID               string
	externalActionKind             string
	externalActionChannel          string
	externalActionTarget           string
	externalActionRequired         bool
	externalActionDeliveryRequired bool
	expiresAtMS                    int64
	autoAccept                     bool
	commandID                      string
	status                         string
	summary                        string
	output                         string
	startedAtMS                    int64
	completedAtMS                  int64
}

func cmdFleetCommands(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd fleet commands <catalog|send|result> [flags]")
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "fleet commands: missing op")
			return exitInvalidArgument
		}
		return exitOK
	}
	switch args[0] {
	case "catalog":
		return printJSON(map[string]any{"commands": esphttp.FleetCommandCatalog()})
	case "send":
		return cmdFleetCommandsSend(gf, args[1:])
	case "result":
		return cmdFleetCommandsResult(gf, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fleet commands: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdFleetCommandsSend(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet commands send", flag.ContinueOnError)
	cfg := &fleetCommandsFlags{autoAccept: true, target: esphttp.FleetCommandTargetAll, externalActionRequired: true, externalActionDeliveryRequired: true}
	fs.StringVar(&cfg.espURL, "esp-url", os.Getenv("ENTMOOT_ESP_URL"), "ESP base URL (defaults to ENTMOOT_ESP_URL)")
	fs.StringVar(&cfg.fleet, "fleet", "", "fleet id")
	fs.StringVar(&cfg.group, "group", "", "fleet control group id")
	fs.StringVar(&cfg.target, "target", esphttp.FleetCommandTargetAll, "target: all or node")
	fs.Uint64Var(&cfg.targetNodeID, "target-node-id", 0, "target node id when -target node")
	fs.StringVar(&cfg.action, "action", "", "safe command action")
	fs.StringVar(&cfg.argsJSON, "args-json", "", "optional command args JSON object")
	fs.StringVar(&cfg.instruction, "instruction", "", "natural-language instruction for agent.instruction")
	fs.Int64Var(&cfg.timeoutMS, "timeout-ms", 0, "optional instruction timeout in milliseconds")
	fs.StringVar(&cfg.externalActionID, "external-action-id", "", "optional external action id for agent.instruction")
	fs.StringVar(&cfg.externalActionKind, "external-action-kind", "", "external action kind for agent.instruction, for example message.send")
	fs.StringVar(&cfg.externalActionChannel, "external-action-channel", "", "external action channel, for example telegram")
	fs.StringVar(&cfg.externalActionTarget, "external-action-target", "", "external action target")
	fs.BoolVar(&cfg.externalActionRequired, "external-action-required", cfg.externalActionRequired, "require external action support")
	fs.BoolVar(&cfg.externalActionDeliveryRequired, "external-delivery-required", cfg.externalActionDeliveryRequired, "require external delivery evidence")
	fs.Int64Var(&cfg.expiresAtMS, "expires-at-ms", 0, "optional command expiration timestamp")
	fs.BoolVar(&cfg.autoAccept, "auto-accept", true, "request agent auto-accept for safe commands")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if strings.TrimSpace(cfg.action) == "" && strings.TrimSpace(cfg.instruction) != "" {
		cfg.action = esphttp.FleetCommandActionAgentInstruction
	}
	cfg.action = esphttp.NormalizeFleetCommandAction(cfg.action)
	if cfg.action == esphttp.FleetCommandActionAgentInstruction {
		cfg.autoAccept = false
	}
	if strings.TrimSpace(cfg.action) == "" {
		fmt.Fprintln(os.Stderr, "fleet commands send: -action is required")
		return exitInvalidArgument
	}
	commandArgs, code, ok := fleetCommandSendArgs(cfg)
	if !ok {
		return code
	}
	taskFlags := &fleetTasksFlags{espURL: cfg.espURL, fleet: cfg.fleet, group: cfg.group}
	client, fleetID, code, ok := prepareFleetTasksClient(gf, taskFlags)
	if !ok {
		return code
	}
	body := map[string]any{
		"target":      cfg.target,
		"action":      cfg.action,
		"auto_accept": cfg.autoAccept,
	}
	if cfg.targetNodeID != 0 {
		body["target_node_id"] = cfg.targetNodeID
	}
	if cfg.expiresAtMS != 0 {
		body["expires_at_ms"] = cfg.expiresAtMS
	}
	if len(commandArgs) > 0 {
		body["args"] = commandArgs
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/commands"
	return client.doAndPrint(context.Background(), http.MethodPost, path, nil, body, randomIdempotencyKey("fleet-command-send"))
}

func fleetCommandSendArgs(cfg *fleetCommandsFlags) (map[string]any, int, bool) {
	commandArgs := map[string]any{}
	if strings.TrimSpace(cfg.argsJSON) != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(cfg.argsJSON), &parsed); err != nil {
			fmt.Fprintf(os.Stderr, "fleet commands send: -args-json: %v\n", err)
			return nil, exitInvalidArgument, false
		}
		if parsed == nil {
			fmt.Fprintln(os.Stderr, "fleet commands send: -args-json must be a JSON object")
			return nil, exitInvalidArgument, false
		}
		commandArgs = parsed
	}
	if strings.TrimSpace(cfg.instruction) != "" {
		if cfg.action != esphttp.FleetCommandActionAgentInstruction {
			fmt.Fprintln(os.Stderr, "fleet commands send: -instruction can only be used with agent.instruction")
			return nil, exitInvalidArgument, false
		}
		commandArgs["instruction"] = cfg.instruction
	}
	if cfg.timeoutMS != 0 {
		if cfg.action != esphttp.FleetCommandActionAgentInstruction {
			fmt.Fprintln(os.Stderr, "fleet commands send: -timeout-ms can only be used with agent.instruction")
			return nil, exitInvalidArgument, false
		}
		commandArgs["timeout_ms"] = cfg.timeoutMS
	}
	if fleetCommandExternalActionFlagSet(cfg) {
		if cfg.action != esphttp.FleetCommandActionAgentInstruction {
			fmt.Fprintln(os.Stderr, "fleet commands send: external action flags can only be used with agent.instruction")
			return nil, exitInvalidArgument, false
		}
		if strings.TrimSpace(cfg.externalActionKind) == "" {
			fmt.Fprintln(os.Stderr, "fleet commands send: -external-action-kind is required when using external action flags")
			return nil, exitInvalidArgument, false
		}
		action := map[string]any{
			"kind":              strings.TrimSpace(cfg.externalActionKind),
			"required":          cfg.externalActionRequired,
			"delivery_required": cfg.externalActionDeliveryRequired,
		}
		if strings.TrimSpace(cfg.externalActionID) != "" {
			action["id"] = strings.TrimSpace(cfg.externalActionID)
		}
		if strings.TrimSpace(cfg.externalActionChannel) != "" {
			action["channel"] = strings.TrimSpace(cfg.externalActionChannel)
		}
		if strings.TrimSpace(cfg.externalActionTarget) != "" {
			action["target"] = strings.TrimSpace(cfg.externalActionTarget)
		}
		actions, ok := appendFleetCommandExternalAction(commandArgs["actions"], action)
		if !ok {
			fmt.Fprintln(os.Stderr, "fleet commands send: -args-json actions must be an array when using external action flags")
			return nil, exitInvalidArgument, false
		}
		commandArgs["actions"] = actions
	}
	if cfg.action == esphttp.FleetCommandActionAgentInstruction || len(commandArgs) > 0 {
		if err := esphttp.ValidateFleetCommandArgs(cfg.action, commandArgs); err != nil {
			fmt.Fprintf(os.Stderr, "fleet commands send: %v\n", err)
			return nil, exitInvalidArgument, false
		}
	}
	return commandArgs, exitOK, true
}

func fleetCommandExternalActionFlagSet(cfg *fleetCommandsFlags) bool {
	return strings.TrimSpace(cfg.externalActionID) != "" ||
		strings.TrimSpace(cfg.externalActionKind) != "" ||
		strings.TrimSpace(cfg.externalActionChannel) != "" ||
		strings.TrimSpace(cfg.externalActionTarget) != ""
}

func appendFleetCommandExternalAction(existing interface{}, action map[string]any) ([]any, bool) {
	actions := []any{}
	if existing == nil {
		return append(actions, action), true
	}
	switch v := existing.(type) {
	case []any:
		actions = append(actions, v...)
	default:
		return nil, false
	}
	return append(actions, action), true
}

func cmdFleetCommandsResult(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet commands result", flag.ContinueOnError)
	cfg := &fleetCommandsFlags{action: esphttp.FleetCommandActionAgentInstruction}
	fs.StringVar(&cfg.group, "group", "", "fleet control group id")
	fs.StringVar(&cfg.fleet, "fleet", "", "fleet id")
	fs.StringVar(&cfg.commandID, "command-id", "", "command id")
	fs.StringVar(&cfg.action, "action", esphttp.FleetCommandActionAgentInstruction, "command action")
	fs.StringVar(&cfg.status, "status", "", "result status: completed, failed, rejected, running")
	fs.StringVar(&cfg.summary, "summary", "", "short result summary")
	fs.StringVar(&cfg.output, "output", "", "optional result output")
	fs.Int64Var(&cfg.startedAtMS, "started-at-ms", 0, "optional start timestamp in milliseconds")
	fs.Int64Var(&cfg.completedAtMS, "completed-at-ms", 0, "optional completion timestamp in milliseconds")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if strings.TrimSpace(cfg.group) == "" {
		fmt.Fprintln(os.Stderr, "fleet commands result: -group is required")
		return exitInvalidArgument
	}
	if strings.TrimSpace(cfg.fleet) == "" {
		fmt.Fprintln(os.Stderr, "fleet commands result: -fleet is required")
		return exitInvalidArgument
	}
	if strings.TrimSpace(cfg.commandID) == "" {
		fmt.Fprintln(os.Stderr, "fleet commands result: -command-id is required")
		return exitInvalidArgument
	}
	status := esphttp.NormalizeFleetCommandResultStatus(cfg.status)
	if status == "" {
		fmt.Fprintln(os.Stderr, "fleet commands result: -status must be completed, failed, rejected, running, accepted, expired, or duplicate")
		return exitInvalidArgument
	}
	groupID, err := decodeGroupID(cfg.group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: -group: %v\n", err)
		return exitInvalidArgument
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, err := infoOverIPCContext(ctx, controlSocketPath(gf.data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: running Entmoot daemon is required: %v\n", err)
		return exitTransport
	}
	result := esphttp.FleetCommandResultEnvelope{
		Type:          esphttp.FleetCommandResultType,
		Version:       1,
		CommandID:     strings.TrimSpace(cfg.commandID),
		FleetID:       strings.TrimSpace(cfg.fleet),
		AgentNodeID:   info.PilotNodeID,
		Action:        strings.TrimSpace(cfg.action),
		Status:        status,
		Summary:       strings.TrimSpace(cfg.summary),
		Output:        cfg.output,
		StartedAtMS:   cfg.startedAtMS,
		CompletedAtMS: cfg.completedAtMS,
	}
	if result.CompletedAtMS == 0 && fleetCommandStatusIsTerminal(status) {
		result.CompletedAtMS = time.Now().UnixMilli()
	}
	if result.Summary == "" {
		result.Summary = "Agent reported command " + status
	}
	return publishFleetCommandResultOverIPC(ctx, controlSocketPath(gf.data), groupID, result)
}

func publishFleetCommandResultOverIPC(ctx context.Context, sockPath string, groupID entmoot.GroupID, result esphttp.FleetCommandResultEnvelope) int {
	data, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: encode result: %v\n", err)
		return exitTransport
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", sockPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: dial control socket: %v\n", err)
		return exitTransport
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: set deadline: %v\n", err)
		return exitTransport
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.PublishReq{
		GroupID: &groupID,
		Topics:  []string{"fleet/commands/results"},
		Content: data,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: write publish request: %v\n", err)
		return exitTransport
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fleet commands result: read publish response: %v\n", err)
		return exitTransport
	}
	switch v := payload.(type) {
	case *ipc.PublishResp:
		return printJSON(map[string]any{
			"status":       "published",
			"message_id":   v.MessageID,
			"group_id":     v.GroupID,
			"timestamp_ms": v.TimestampMS,
		})
	case *ipc.ErrorFrame:
		fmt.Fprintf(os.Stderr, "fleet commands result: ipc error %s: %s\n", v.Code, v.Message)
		return exitTransport
	default:
		fmt.Fprintf(os.Stderr, "fleet commands result: unexpected ipc response %T\n", payload)
		return exitTransport
	}
}
