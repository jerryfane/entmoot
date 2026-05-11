package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
)

func publishIPCMessage(ctx context.Context, gf *globalFlags, groupID entmoot.GroupID, topics []string, content []byte) error {
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", controlSocketPath(gf.data))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.PublishReq{
		GroupID: &groupID,
		Topics:  topics,
		Content: content,
	}); err != nil {
		return err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return err
	}
	switch v := payload.(type) {
	case *ipc.PublishResp:
		return nil
	case *ipc.ErrorFrame:
		return fmt.Errorf("ipc error %s: %s", v.Code, v.Message)
	default:
		return fmt.Errorf("unexpected ipc response %T", payload)
	}
}
