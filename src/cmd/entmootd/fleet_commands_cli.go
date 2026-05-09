package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"entmoot/pkg/entmoot/esphttp"
)

type fleetCommandsFlags struct {
	espURL       string
	fleet        string
	group        string
	target       string
	targetNodeID uint64
	action       string
	argsJSON     string
	expiresAtMS  int64
	autoAccept   bool
}

func cmdFleetCommands(gf *globalFlags, args []string) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(os.Stderr, "usage: entmootd fleet commands <catalog|send> [flags]")
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
	default:
		fmt.Fprintf(os.Stderr, "fleet commands: unknown op %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdFleetCommandsSend(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("fleet commands send", flag.ContinueOnError)
	cfg := &fleetCommandsFlags{autoAccept: true, target: esphttp.FleetCommandTargetAll}
	fs.StringVar(&cfg.espURL, "esp-url", os.Getenv("ENTMOOT_ESP_URL"), "ESP base URL (defaults to ENTMOOT_ESP_URL)")
	fs.StringVar(&cfg.fleet, "fleet", "", "fleet id")
	fs.StringVar(&cfg.group, "group", "", "fleet control group id")
	fs.StringVar(&cfg.target, "target", esphttp.FleetCommandTargetAll, "target: all or node")
	fs.Uint64Var(&cfg.targetNodeID, "target-node-id", 0, "target node id when -target node")
	fs.StringVar(&cfg.action, "action", "", "safe command action")
	fs.StringVar(&cfg.argsJSON, "args-json", "", "optional command args JSON object")
	fs.Int64Var(&cfg.expiresAtMS, "expires-at-ms", 0, "optional command expiration timestamp")
	fs.BoolVar(&cfg.autoAccept, "auto-accept", true, "request agent auto-accept for safe commands")
	if err := fs.Parse(args); err != nil {
		return exitInvalidArgument
	}
	if strings.TrimSpace(cfg.action) == "" {
		fmt.Fprintln(os.Stderr, "fleet commands send: -action is required")
		return exitInvalidArgument
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
	if strings.TrimSpace(cfg.argsJSON) != "" {
		var args map[string]any
		if err := json.Unmarshal([]byte(cfg.argsJSON), &args); err != nil {
			fmt.Fprintf(os.Stderr, "fleet commands send: -args-json: %v\n", err)
			return exitInvalidArgument
		}
		body["args"] = args
	}
	path := "/v1/fleets/" + url.PathEscape(fleetID) + "/commands"
	return client.doAndPrint(context.Background(), http.MethodPost, path, nil, body, randomIdempotencyKey("fleet-command-send"))
}
