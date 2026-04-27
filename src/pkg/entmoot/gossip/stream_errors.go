package gossip

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"
)

func (g *Gossiper) classifyStreamError(ctx context.Context, err error) StreamErrorClassification {
	c := classifyGenericStreamError(ctx, err)
	c = mergeStreamErrorClassification(c, g.classifyTransportStreamError(err))
	if c.StaleSession {
		c.Retryable = true
	}
	return c
}

func (g *Gossiper) classifyTransportStreamError(err error) StreamErrorClassification {
	classifier, ok := g.cfg.Transport.(StreamErrorClassifier)
	if !ok {
		return StreamErrorClassification{}
	}
	c := classifier.ClassifyStreamError(err)
	if c.StaleSession {
		c.Retryable = true
	}
	return c
}

func classifyGenericStreamError(ctx context.Context, err error) StreamErrorClassification {
	var c StreamErrorClassification
	if err == nil {
		return c
	}
	if errors.Is(err, context.Canceled) {
		c.LocalContext = true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		c.Retryable = true
		c.Timeout = true
		if ctx.Err() != nil {
			c.LocalContext = true
		}
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) {
		c.Retryable = true
		c.StaleSession = true
	}
	errText := strings.ToLower(err.Error())
	if strings.Contains(errText, "closed pipe") ||
		strings.Contains(errText, "use of closed network connection") {
		c.Retryable = true
		c.StaleSession = true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		c.Retryable = true
		c.Timeout = true
	}
	return c
}

func mergeStreamErrorClassification(a, b StreamErrorClassification) StreamErrorClassification {
	return StreamErrorClassification{
		Retryable:    a.Retryable || b.Retryable,
		StaleSession: a.StaleSession || b.StaleSession,
		Timeout:      a.Timeout || b.Timeout,
		LocalContext: a.LocalContext || b.LocalContext,
	}
}

func shouldRetryStreamFailure(c StreamErrorClassification, attempt int, parentCtx context.Context, attemptTimeout time.Duration) bool {
	if attempt != 0 {
		return false
	}
	if !(c.Retryable || c.LocalContext) {
		return false
	}
	return attemptTimeout > 0 || parentCtx.Err() == nil
}

func shouldDropPeerSessionAfterStreamFailure(c StreamErrorClassification, attempt int) bool {
	if c.StaleSession {
		return true
	}
	return attempt > 0 && c.Retryable
}

func shouldDropPeerSessionAfterResponseFailure(c StreamErrorClassification) bool {
	return c.Retryable
}

func shouldRecordDialFailure(c StreamErrorClassification) bool {
	return !c.LocalContext && !c.StaleSession
}

func shouldRecordDialFailureAfterDialError(classification, transportClassification StreamErrorClassification) bool {
	return !classification.LocalContext && !transportClassification.StaleSession
}
