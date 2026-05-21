package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/ipc"
)

type publishIPCError struct {
	code    ipc.ErrorCode
	message string
}

func (e *publishIPCError) Error() string {
	return fmt.Sprintf("ipc error %s: %s", e.code, e.message)
}

func publishIPCExitCode(err error) int {
	if code := publishIPCErrorCode(err); code != "" {
		return ipc.ExitCode(code)
	}
	return exitTransport
}

func publishIPCErrorCode(err error) ipc.ErrorCode {
	var ipcErr *publishIPCError
	if errors.As(err, &ipcErr) {
		return ipcErr.code
	}
	return ""
}

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
		return &publishIPCError{code: v.Code, message: v.Message}
	default:
		return fmt.Errorf("unexpected ipc response %T", payload)
	}
}
