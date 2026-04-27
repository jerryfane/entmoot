package pilot

import (
	"errors"

	"github.com/hashicorp/yamux"

	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// ClassifyStreamError implements gossip.StreamErrorClassifier. Gossip decides
// retry/drop/backoff policy; this adapter only translates Pilot/yamux-specific
// errors into generic stream failure facts.
func (t *Transport) ClassifyStreamError(err error) gossip.StreamErrorClassification {
	return ClassifyStreamError(err)
}

func ClassifyStreamError(err error) gossip.StreamErrorClassification {
	if err == nil {
		return gossip.StreamErrorClassification{}
	}
	switch {
	case errors.Is(err, ipcclient.ErrConnectionNotFound),
		errors.Is(err, ipcclient.ErrConnectionNotEstablished),
		errors.Is(err, ipcclient.ErrConnectionClosing),
		errors.Is(err, errSessionNotActive),
		errors.Is(err, yamux.ErrSessionShutdown),
		errors.Is(err, yamux.ErrStreamClosed),
		errors.Is(err, yamux.ErrRemoteGoAway),
		errors.Is(err, yamux.ErrConnectionReset),
		errors.Is(err, yamux.ErrKeepAliveTimeout):
		return gossip.StreamErrorClassification{
			Retryable:    true,
			StaleSession: true,
		}
	case errors.Is(err, yamux.ErrTimeout),
		errors.Is(err, yamux.ErrConnectionWriteTimeout):
		return gossip.StreamErrorClassification{
			Retryable: true,
			Timeout:   true,
		}
	default:
		return gossip.StreamErrorClassification{}
	}
}
