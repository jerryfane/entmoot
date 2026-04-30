package pilot

import (
	"errors"

	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/transport/pilot/ipcclient"
)

// ClassifyStreamError implements gossip.StreamErrorClassifier. Gossip decides
// retry/drop/backoff policy; this adapter only translates Pilot-specific
// errors into generic stream failure facts.
func (t *Transport) ClassifyStreamError(err error) gossip.StreamErrorClassification {
	return ClassifyStreamError(err)
}

func ClassifyStreamError(err error) gossip.StreamErrorClassification {
	if err == nil {
		return gossip.StreamErrorClassification{}
	}
	switch {
	case errors.Is(err, ErrDialLimiterTimeout),
		errors.Is(err, ErrDialCallerTimeout):
		return gossip.StreamErrorClassification{
			Retryable:    true,
			Timeout:      true,
			LocalContext: true,
		}
	case errors.Is(err, ErrDaemonDialTimeout):
		return gossip.StreamErrorClassification{
			Retryable: true,
			Timeout:   true,
		}
	case errors.Is(err, ipcclient.ErrClosed),
		errors.Is(err, ipcclient.ErrConnectionNotFound),
		errors.Is(err, ipcclient.ErrConnectionNotEstablished),
		errors.Is(err, ipcclient.ErrConnectionClosing),
		errors.Is(err, ipcclient.ErrSendFailed):
		return gossip.StreamErrorClassification{
			Retryable:    true,
			StaleSession: true,
		}
	default:
		return gossip.StreamErrorClassification{}
	}
}
