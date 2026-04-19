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

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
)

// cmdPublish authors a message by asking the running join process to
// sign and gossip it on our behalf. Connects to the control socket via
// IPC; exits 6 if the socket is absent or unresponsive.
func cmdPublish(gf *globalFlags, args []string) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	topicFlag := fs.String("topic", "", "comma-separated topics (required)")
	content := fs.String("content", "", "message content (required)")
	groupStr := fs.String("group", "", "base64 group id (optional when exactly one group is joined)")
	timeoutFlag := fs.Duration("timeout", 30*time.Second, "IPC response deadline; raise on slow/flaky networks")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitInvalidArgument
	}
	if *topicFlag == "" {
		fmt.Fprintln(os.Stderr, "publish: -topic is required")
		return exitInvalidArgument
	}
	if *content == "" {
		fmt.Fprintln(os.Stderr, "publish: -content is required")
		return exitInvalidArgument
	}
	topics := parseTopicList(*topicFlag)
	if len(topics) == 0 {
		fmt.Fprintln(os.Stderr, "publish: -topic had no valid entries")
		return exitInvalidArgument
	}

	var gidPtr *entmoot.GroupID
	if *groupStr != "" {
		gid, err := decodeGroupID(*groupStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "publish: %v\n", err)
			return exitInvalidArgument
		}
		gidPtr = &gid
	}

	sockPath := controlSocketPath(gf.data)
	if !controlSocketAlive(sockPath, 500*time.Millisecond) {
		fmt.Fprintln(os.Stderr, noJoinHelp)
		return exitControlUnavail
	}

	conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
	if err != nil {
		fmt.Fprintln(os.Stderr, noJoinHelp)
		return exitControlUnavail
	}
	defer conn.Close()
	// Deadline for the entire publish RPC round-trip. Default 30s — the old
	// 10s default was tight enough to fire on perfectly healthy publishes
	// when the daemon was busy gossiping to multiple peers, surfacing a
	// misleading "publish: read response ... i/o timeout" error while the
	// message had actually persisted. --timeout lets ops bump it further on
	// slow networks.
	if err := conn.SetDeadline(time.Now().Add(*timeoutFlag)); err != nil {
		slog.Error("publish: set deadline", slog.String("err", err.Error()))
		return exitTransport
	}

	req := &ipc.PublishReq{
		GroupID: gidPtr,
		Topics:  topics,
		Content: []byte(*content),
	}
	if err := ipc.EncodeAndWrite(conn, req); err != nil {
		slog.Error("publish: write publish_req", slog.String("err", err.Error()))
		return exitTransport
	}

	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		slog.Error("publish: read response", slog.String("err", err.Error()))
		return exitTransport
	}
	switch v := payload.(type) {
	case *ipc.PublishResp:
		// The v1 ipc.PublishResp struct does not carry the author
		// NodeID; the daemon is always the author, so the publisher can
		// derive it by running `entmootd info` if needed. We include it
		// on stdout if the daemon's info_resp happens to be cheaply
		// available — otherwise we publish {message_id, group_id,
		// topic, timestamp_ms} which is what every downstream consumer
		// actually keys on. See deviation note in V1-B report.
		out := map[string]any{
			"message_id":   v.MessageID,
			"group_id":     v.GroupID,
			"topic":        topics,
			"timestamp_ms": v.TimestampMS,
		}
		data, err := json.Marshal(out)
		if err != nil {
			slog.Error("publish: marshal", slog.String("err", err.Error()))
			return exitTransport
		}
		fmt.Println(string(data))
		return exitOK
	case *ipc.ErrorFrame:
		fmt.Fprintf(os.Stderr, "publish: %s: %s\n", v.Code, v.Message)
		return ipc.ExitCode(v.Code)
	default:
		slog.Error("publish: unexpected response type", slog.String("type", fmt.Sprintf("%T", payload)))
		return exitTransport
	}
}

// noJoinHelp is the canonical CLI_DESIGN §5.5 help string emitted when
// the control socket is absent or unresponsive.
const noJoinHelp = `entmootd: no running join process found; start one with "entmootd join <invite>"`
