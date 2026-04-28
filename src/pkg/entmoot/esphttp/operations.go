package esphttp

import (
	"context"
	"encoding/json"
	"net/http"
)

// OperationExecutor runs phone-approved ESP operations after the sign request
// signature has been verified by the HTTP layer.
type OperationExecutor interface {
	ExecuteSignRequest(context.Context, SignRequest, []byte) (json.RawMessage, error)
}

// OperationError maps executor failures to stable ESP HTTP errors.
type OperationError struct {
	HTTPStatus int
	Code       string
	Message    string
}

func (e *OperationError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func operationHTTPError(status int, code, message string) *OperationError {
	if status == 0 {
		status = http.StatusInternalServerError
	}
	if code == "" {
		code = "internal_error"
	}
	if message == "" {
		message = "operation failed"
	}
	return &OperationError{HTTPStatus: status, Code: code, Message: message}
}

func executableOperationKind(kind string) bool {
	switch kind {
	case signRequestKindGroupCreate,
		signRequestKindGroupUpdate,
		signRequestKindInviteCreate,
		signRequestKindInviteAccept:
		return true
	default:
		return false
	}
}
