package ipc

import (
	"errors"

	entmoot "entmoot/pkg/entmoot"
)

// ErrorCode is the machine-readable code carried by a MsgError frame.
// Values are short uppercase strings (CLI_DESIGN §5.4) so humans can
// read them in logs and a thin client can map them to exit codes
// without a lookup table beyond what this package provides.
type ErrorCode string

// Registered v1 error codes. See CLI_DESIGN §5.4 + §6.
const (
	// CodeOK is the nominal "no error" code. It is not emitted on the
	// wire (success uses the type-specific response frame) but is
	// defined so ExitCode(CodeOK) == 0.
	CodeOK ErrorCode = "OK"
	// CodeInternal is a generic server-side / transport failure.
	CodeInternal ErrorCode = "INTERNAL"
	// CodeNotMember is returned when the local node is not a current
	// member of the group named in the request.
	CodeNotMember ErrorCode = "NOT_MEMBER"
	// CodeGroupNotFound is returned when the named group is not joined
	// on this host.
	CodeGroupNotFound ErrorCode = "GROUP_NOT_FOUND"
	// CodeInvalidArgument is returned on flag or payload validation
	// failure (e.g. PublishReq with no topics, or a nil GroupID when
	// more than one group is joined).
	CodeInvalidArgument ErrorCode = "INVALID_ARGUMENT"
)

// ErrorFrame is the JSON body of a MsgError frame.
//
// Type is always the literal string "error" (CLI_DESIGN §5.4). Clients
// can switch on Code for machine handling and fall back to Message for
// human display. GroupID is populated when the error is group-scoped
// (NOT_MEMBER, GROUP_NOT_FOUND) and omitted otherwise.
type ErrorFrame struct {
	// Type is always "error". Encode enforces this — callers that
	// leave it empty get it filled in automatically.
	Type string `json:"type"`
	// Code is the machine-readable code; see the Code* constants.
	Code ErrorCode `json:"code"`
	// GroupID, when set, scopes the error to a group.
	GroupID *entmoot.GroupID `json:"group_id,omitempty"`
	// Message is a human-readable description. Clients should show it
	// but must not try to parse it.
	Message string `json:"message"`
}

// ExitCode maps an ErrorCode to the CLI process exit code per
// CLI_DESIGN §6. Unknown codes (anything not listed below) return 1 so
// callers get a sane default instead of zero.
//
//	CodeOK              -> 0
//	CodeInternal        -> 1
//	CodeNotMember       -> 2
//	CodeGroupNotFound   -> 3
//	CodeInvalidArgument -> 5
//	(unknown)           -> 1
func ExitCode(c ErrorCode) int {
	switch c {
	case CodeOK:
		return 0
	case CodeInternal:
		return 1
	case CodeNotMember:
		return 2
	case CodeGroupNotFound:
		return 3
	case CodeInvalidArgument:
		return 5
	default:
		return 1
	}
}

// ErrUnknownMessage is returned by Encode for an unsupported payload
// type and by Decode for an unregistered 1-byte MsgType.
var ErrUnknownMessage = errors.New("ipc: unknown message type")

// ErrMalformedFrame is returned by ReadFrame on a zero-length frame and
// by Decode when json.Unmarshal fails.
var ErrMalformedFrame = errors.New("ipc: malformed frame")

// ErrOversized is returned by WriteFrame/ReadFrame when a frame's
// payload exceeds MaxFrameSize.
var ErrOversized = errors.New("ipc: frame exceeds 16 MiB cap")
