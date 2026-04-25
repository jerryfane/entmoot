package ipcclient

import (
	"errors"
	"fmt"
)

// ErrFrameTooLarge is returned when a frame's declared or actual size
// exceeds the 1 MiB maximum defined by the spec.
var ErrFrameTooLarge = errors.New("ipcclient: frame exceeds 1 MiB limit")

// ErrClosed is returned by operations issued on a Driver, Listener, or
// Conn after it has been Close()d, or when the underlying IPC socket
// has been closed by the daemon.
var ErrClosed = errors.New("ipcclient: closed")

// ErrShortResponse is returned when a daemon response frame is shorter
// than the opcode-specific minimum (e.g. DialOK without the 4-byte
// conn_id). The daemon should never emit such a frame; if we see one
// the only safe response is to surface a typed error and let the
// caller tear down the session.
var ErrShortResponse = errors.New("ipcclient: truncated daemon response")

// ErrSubscribeUnsupported is returned by Driver.Subscribe when the
// connected pilot daemon doesn't recognise the opSubscribe opcode
// (i.e. predates v1.9.0-jf.11b). Callers should branch on this with
// errors.Is and fall back to legacy polling for the desired state.
// (v1.5.0)
var ErrSubscribeUnsupported = errors.New("ipcclient: subscribe: pilot daemon does not support pub/sub (pre-jf.11b)")

// IPCError wraps an Error (0x0A) response from the daemon. The Code
// field is the 2-byte error code from the wire frame; Message is the
// UTF-8 tail. The textual Error() output is the message alone for
// compatibility with Go's convention of lowercase error strings, but
// callers can type-assert to inspect Code if they need to branch on
// specific conditions.
type IPCError struct {
	Code    uint16
	Message string
}

func (e *IPCError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("ipcclient: daemon error (code %d)", e.Code)
	}
	return fmt.Sprintf("ipcclient: daemon: %s", e.Message)
}
