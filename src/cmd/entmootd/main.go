// Command entmootd is the Entmoot v1 daemon + CLI. A single binary that
// exposes five agent-facing subcommands (join, publish, tail, info, query)
// plus two founder-facing subcommands (group create, invite create). The
// join subcommand owns a blocking accept loop and a control-socket IPC
// server; all agent commands other than join/publish/tail are direct SQLite
// readers and work whether or not a join process is running.
//
// See docs/CLI_DESIGN.md for the authoritative spec.
package main

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
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
	socket     string
	identity   string
	data       string
	listenPort uint
	logLevel   string
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
		fmt.Fprintln(os.Stderr, "  join <invite>           Block, run the accept loop, and serve the control socket.")
		fmt.Fprintln(os.Stderr, "  publish -topic T -content S [-group GID]")
		fmt.Fprintln(os.Stderr, "                          Author and gossip a message via the control socket.")
		fmt.Fprintln(os.Stderr, "  tail [-topic PAT] [-group GID] [-n N]")
		fmt.Fprintln(os.Stderr, "                          SQLite backfill + live subscription from the control socket.")
		fmt.Fprintln(os.Stderr, "  info                    Print a JSON snapshot (reads SQLite directly).")
		fmt.Fprintln(os.Stderr, "  query -group GID [...]  Historical SQLite query with JSON-line output.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Founder subcommands:")
		fmt.Fprintln(os.Stderr, "  group create -name N    Create a new group.")
		fmt.Fprintln(os.Stderr, "  invite create -group GID [-peers ...] [-valid-for DUR]")
		fmt.Fprintln(os.Stderr, "                          Emit a signed invite JSON bundle.")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Global flags:")
		fs.PrintDefaults()
	}

	gf := &globalFlags{}
	fs.StringVar(&gf.socket, "socket", "/tmp/pilot.sock", "Pilot daemon IPC socket path")
	fs.StringVar(&gf.identity, "identity", "~/.entmoot/identity.json", "Entmoot identity file")
	fs.StringVar(&gf.data, "data", "~/.entmoot", "Entmoot data root")
	fs.UintVar(&gf.listenPort, "listen-port", 1004, "Entmoot listen port")
	fs.StringVar(&gf.logLevel, "log-level", "info", "slog level: debug|info|warn|error")

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

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		return exitInvalidArgument
	}

	switch args[0] {
	case "join":
		return cmdJoin(gf, args[1:])
	case "publish":
		return cmdPublish(gf, args[1:])
	case "tail":
		return cmdTail(gf, args[1:])
	case "info":
		return cmdInfo(gf, args[1:])
	case "query":
		return cmdQuery(gf, args[1:])
	case "group":
		return cmdGroup(gf, args[1:])
	case "invite":
		return cmdInvite(gf, args[1:])
	default:
		fs.Usage()
		fmt.Fprintf(os.Stderr, "entmootd: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}
