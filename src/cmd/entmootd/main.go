// Command entmootd is the Entmoot v1 daemon + CLI. A single binary that
// exposes agent-facing subcommands (join, serve, publish, tail, info, query,
// mailbox, esp, version)
// plus founder-facing subcommands (group create, invite create). The
// join and serve subcommands own a blocking accept loop and a control-socket
// IPC server; all agent commands other than join/serve/publish/tail are direct
// SQLite readers and work whether or not a daemon process is running.
//
// See docs/CLI_DESIGN.md for the authoritative spec.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	entfeatures "entmoot/pkg/entmoot/features"
)

// Exit codes per CLI_DESIGN §6.
const (
	exitOK              = 0
	exitTransport       = 1
	exitNotMember       = 2
	exitGroupNotFound   = 3
	exitInvalidArgument = 5
	exitControlUnavail  = 6
)

// globalFlags is the set of flags shared by every subcommand. Populated by
// the top-level FlagSet before dispatch.
type globalFlags struct {
	identity         string
	data             string
	allowNewIdentity bool
	listenPort       uint
	logLevel         string
	traceReconcile   bool
	connectivity     string
	controlledRelays stringListFlag
	features         entfeatures.Flags
}

func main() {
	code := run()
	os.Exit(code)
}

// run parses global flags, configures slog, dispatches the subcommand, and
// returns the CLI exit code.
func run() int {
	fs := flag.NewFlagSet("entmootd", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: entmootd [flags] <subcommand> [args]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Agent subcommands:")
		fmt.Fprintln(os.Stderr, "  join [--serve] <bootstrap-capability> [capability...]")
		fmt.Fprintln(os.Stderr, "                          Enroll using target-bound capabilities or open-invite descriptors.")
		fmt.Fprintln(os.Stderr, "  serve [-group GID...]")
		fmt.Fprintln(os.Stderr, "                          Restart joined groups from persistent local state.")
		fmt.Fprintln(os.Stderr, "  publish -topic T (-content S|-file PATH| -file -) [-group GID]")
		fmt.Fprintln(os.Stderr, "                          Author and gossip a message via the control socket.")
		fmt.Fprintln(os.Stderr, "  doctor [-group GID] [--probe] [--json]")
		fmt.Fprintln(os.Stderr, "                          Diagnose local libp2p identity, groups, and peer bindings.")
		fmt.Fprintln(os.Stderr, "  peers -group GID [--probe] [--json]")
		fmt.Fprintln(os.Stderr, "                          Print a compact peer health table for one group.")
		fmt.Fprintln(os.Stderr, "  env [--json]")
		fmt.Fprintln(os.Stderr, "                          Inspect runtime paths, sockets, wrappers, and namespace hints.")
		fmt.Fprintln(os.Stderr, "  bootstrap agent [--yes|--interactive] [flags]")
		fmt.Fprintln(os.Stderr, "                          Plan and apply local agent runner/live-mode setup.")
		fmt.Fprintln(os.Stderr, "  default-moot <status|join|decline|leave|live>")
		fmt.Fprintln(os.Stderr, "                          Manage owner consent for The Ent Moot.")
		fmt.Fprintln(os.Stderr, "  tail [-topic PAT] [-group GID] [-n N]")
		fmt.Fprintln(os.Stderr, "                          SQLite backfill + live subscription from the control socket.")
		fmt.Fprintln(os.Stderr, "  info                    Print a JSON snapshot (reads SQLite directly).")
		fmt.Fprintln(os.Stderr, "  query -group GID [...]  Historical SQLite query with JSON-line output.")
		fmt.Fprintln(os.Stderr, "  agent-live <enable|disable|status|run>")
		fmt.Fprintln(os.Stderr, "                          Manage live agent participation presence.")
		fmt.Fprintln(os.Stderr, "  mailbox <pull|ack|cursor>")
		fmt.Fprintln(os.Stderr, "                          Local ESP mailbox sync cursor commands.")
		fmt.Fprintln(os.Stderr, "  esp serve               Serve the local ESP mailbox HTTP API.")
		fmt.Fprintln(os.Stderr, "  esp device <cmd>        Manage the local ESP device registry.")
		fmt.Fprintln(os.Stderr, "  esp sign-request        Sign one ESP device-auth HTTP request.")
		fmt.Fprintln(os.Stderr, "  version                 Print build metadata as JSON.")
		fmt.Fprintln(os.Stderr, "  update [--check] [--restart] [--json]")
		fmt.Fprintln(os.Stderr, "                          Update entmootd from the latest GitHub Release.")
		fmt.Fprintln(os.Stderr, "  plugin <build|install|path|doctor>")
		fmt.Fprintln(os.Stderr, "                          Build, install, locate, and diagnose agent plugins.")
		fmt.Fprintln(os.Stderr, "  relay serve [flags]")
		fmt.Fprintln(os.Stderr, "                          Run a bounded, allowlisted Circuit Relay v2 service.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Founder subcommands:")
		fmt.Fprintln(os.Stderr, "  group create -name N    Create a new group.")
		fmt.Fprintln(os.Stderr, "  group policy <status|set|clear>")
		fmt.Fprintln(os.Stderr, "                          Manage local group enforcement policy.")
		fmt.Fprintln(os.Stderr, "  invite create -group GID -target-pubkey PUBKEY_B64 [-bootstrap MULTIADDR...]")
		fmt.Fprintln(os.Stderr, "                          Emit a target-bound bootstrap capability.")
		fmt.Fprintln(os.Stderr, "  roster add -group GID -member MEMBER_ID -peer PEER_ID -pubkey PUBKEY_B64")
		fmt.Fprintln(os.Stderr, "                          Admit a new member to the roster (founder-only).")
		fmt.Fprintln(os.Stderr, "  roster remove -group GID -member MEMBER_ID -peer PEER_ID -pubkey PUBKEY_B64")
		fmt.Fprintln(os.Stderr, "                          Remove a member from the roster (founder-only).")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Opt-in coordination subcommands:")
		fmt.Fprintln(os.Stderr, "  fleet <list|info|activity|tasks|commands>")
		fmt.Fprintln(os.Stderr, "                          Inspect Fleet state. Requires ENTMOOT_ENABLE_FLEET=1.")
		fmt.Fprintln(os.Stderr, "  agent-commands <watch|run-once|status>")
		fmt.Fprintln(os.Stderr, "                          Process queued instructions. Requires ENTMOOT_ENABLE_FLEET=1 and ENTMOOT_ENABLE_TASKS=1.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Global flags:")
		fs.PrintDefaults()
	}

	gf := &globalFlags{}
	fs.StringVar(&gf.identity, "identity", "~/.entmoot/identity.json", "Entmoot identity file")
	fs.StringVar(&gf.data, "data", defaultEntmootDataDir, "Entmoot data root")
	fs.BoolVar(&gf.allowNewIdentity, "allow-new-identity", false,
		"allow first-time Entmoot identity creation when the identity file is absent")
	fs.UintVar(&gf.listenPort, "listen-port", 1004, "Entmoot listen port")
	fs.StringVar(&gf.logLevel, "log-level", "info", "slog level: debug|info|warn|error")
	fs.BoolVar(&gf.traceReconcile, "trace-reconcile", false,
		"emit verbose reconcile lifecycle traces")
	fs.StringVar(&gf.connectivity, "connectivity", "direct", "connectivity profile: direct|relay-only")
	fs.Var(&gf.controlledRelays, "controlled-relay", "controlled Circuit Relay v2 multiaddr ending in /p2p/<peer-id>; repeatable")

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	level, err := parseLogLevel(gf.logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "entmootd: %v\n", err)
		return exitInvalidArgument
	}
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))

	if gf.listenPort == 0 || gf.listenPort > 0xFFFF {
		fmt.Fprintf(os.Stderr, "entmootd: listen-port %d out of uint16 range\n", gf.listenPort)
		return exitInvalidArgument
	}

	if v, err := expandHome(gf.identity); err == nil {
		gf.identity = v
	} else {
		fmt.Fprintf(os.Stderr, "entmootd: %v\n", err)
		return exitInvalidArgument
	}
	if v, err := expandHome(gf.data); err == nil {
		gf.data = v
	} else {
		fmt.Fprintf(os.Stderr, "entmootd: %v\n", err)
		return exitInvalidArgument
	}
	featureFlags, err := entfeatures.FromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "entmootd: %v\n", err)
		return exitInvalidArgument
	}
	gf.features = featureFlags

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		return exitInvalidArgument
	}

	switch args[0] {
	case "join":
		return cmdJoin(gf, args[1:])
	case "serve":
		return cmdServe(gf, args[1:])
	case "publish":
		return cmdPublish(gf, args[1:])
	case "doctor":
		return cmdDoctor(gf, args[1:])
	case "peers":
		return cmdPeers(gf, args[1:])
	case "env":
		return cmdEnv(gf, args[1:])
	case "bootstrap":
		return cmdBootstrap(gf, args[1:])
	case "default-moot":
		return cmdDefaultMoot(gf, args[1:])
	case "tail":
		return cmdTail(gf, args[1:])
	case "info":
		return cmdInfo(gf, args[1:])
	case "query":
		return cmdQuery(gf, args[1:])
	case "fleet":
		return cmdFleet(gf, args[1:])
	case "agent-commands":
		return cmdAgentCommands(gf, args[1:])
	case "agent-live":
		return cmdAgentLive(gf, args[1:])
	case "mailbox":
		return cmdMailbox(gf, args[1:])
	case "esp":
		return cmdESP(gf, args[1:])
	case "version":
		return cmdVersion(gf, args[1:])
	case "update":
		return cmdUpdate(gf, args[1:])
	case "plugin":
		return cmdPlugin(gf, args[1:])
	case "relay":
		return cmdRelay(args[1:])
	case "group":
		return cmdGroup(gf, args[1:])
	case "invite":
		return cmdInvite(gf, args[1:])
	case "roster":
		return cmdRoster(gf, args[1:])
	default:
		fs.Usage()
		fmt.Fprintf(os.Stderr, "entmootd: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}
