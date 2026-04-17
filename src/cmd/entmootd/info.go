package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// cmdInfo prints a one-shot snapshot of node + group state as a single
// JSON object on stdout. Reads SQLite directly and probes the control
// socket to set `running`; does not require a Pilot daemon.
func cmdInfo(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}

	s, err := setup(gf)
	if err != nil {
		slog.Error("info: setup", slog.String("err", err.Error()))
		return exitTransport
	}

	running := controlSocketAlive(controlSocketPath(s.dataDir), 500*time.Millisecond)

	resp := ipc.InfoResp{
		// PilotNodeID is not known without dialing Pilot; leave zero
		// when info is run standalone. CLI_DESIGN §3.4 says info reads
		// SQLite directly and works without a running join; we therefore
		// do not open Pilot here.
		PilotNodeID:   0,
		EntmootPubKey: s.identity.PublicKey,
		ListenPort:    uint16(gf.listenPort),
		DataDir:       s.dataDir,
		Running:       running,
		Groups:        []ipc.GroupInfo{},
	}

	// If a join is running, ask it for the snapshot over IPC — that's
	// the only path with a live pilot_node_id and merkle root.
	if running {
		if live, err := infoOverIPC(controlSocketPath(s.dataDir)); err == nil {
			resp = *live
			resp.Running = true
			resp.DataDir = s.dataDir
		} else {
			slog.Warn("info: ipc query failed, falling back to SQLite",
				slog.String("err", err.Error()))
			resp.Running = false
		}
	}

	if !resp.Running {
		// Direct-SQLite path: enumerate <data>/groups/* and count
		// members + messages per group. merkle_root stays nil.
		gids, err := listGroupIDs(s.dataDir, slog.Default())
		if err != nil {
			fmt.Fprintf(os.Stderr, "info: %v\n", err)
			return exitTransport
		}
		if len(gids) > 0 {
			st, err := store.OpenSQLite(s.dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "info: open store: %v\n", err)
				return exitTransport
			}
			defer func() { _ = st.Close() }()

			ctx, cancel := withTimeout(10 * time.Second)
			defer cancel()

			groups := make([]ipc.GroupInfo, 0, len(gids))
			for _, gid := range gids {
				r, err := roster.OpenJSONL(s.dataDir, gid)
				if err != nil {
					slog.Warn("info: open roster",
						slog.String("group", gid.String()),
						slog.String("err", err.Error()))
					continue
				}
				members := len(r.Members())
				_ = r.Close()

				msgs, err := st.Range(ctx, gid, 0, 0)
				msgCount := 0
				if err == nil {
					msgCount = len(msgs)
				} else {
					slog.Warn("info: range",
						slog.String("group", gid.String()),
						slog.String("err", err.Error()))
				}
				groups = append(groups, ipc.GroupInfo{
					GroupID:    gid,
					Members:    members,
					Messages:   msgCount,
					MerkleRoot: nil, // nil per CLI_DESIGN §3.4 when not running
				})
			}
			resp.Groups = groups
		}
	}

	data, err := json.Marshal(&resp)
	if err != nil {
		slog.Error("info: marshal", slog.String("err", err.Error()))
		return exitTransport
	}
	fmt.Println(string(data))
	return exitOK
}

// infoOverIPC dials the running join's control socket, sends info_req,
// and decodes the response.
func infoOverIPC(sockPath string) (*ipc.InfoResp, error) {
	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("dial control socket: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.InfoReq{}); err != nil {
		return nil, fmt.Errorf("write info_req: %w", err)
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, fmt.Errorf("read info_resp: %w", err)
	}
	switch v := payload.(type) {
	case *ipc.InfoResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, fmt.Errorf("ipc error %s: %s", v.Code, v.Message)
	default:
		return nil, fmt.Errorf("unexpected ipc response: %T", payload)
	}
}
