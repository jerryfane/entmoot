package policy

import (
	"fmt"
	"math"

	"entmoot/pkg/entmoot/ratelimit"
)

// ContentLimits returns token-bucket limits for content messages from p.
func ContentLimits(p Policy) (ratelimit.Limits, error) {
	if err := p.Validate(); err != nil {
		return ratelimit.Limits{}, err
	}
	if p.ByteBurstPerAuthor > int64(math.MaxInt) {
		return ratelimit.Limits{}, fmt.Errorf("byte_burst_per_author %d overflows int", p.ByteBurstPerAuthor)
	}
	msgRate, err := ParseMessageRate(p.MessageRatePerAuthor)
	if err != nil {
		return ratelimit.Limits{}, fmt.Errorf("message_rate_per_author: %w", err)
	}
	byteRate, err := ParseByteRate(p.ByteRatePerAuthor)
	if err != nil {
		return ratelimit.Limits{}, fmt.Errorf("byte_rate_per_author: %w", err)
	}
	return ratelimit.Limits{
		MsgRate:    msgRate.Limit(),
		MsgBurst:   p.MessageBurstPerAuthor,
		BytesRate:  byteRate.Limit(),
		BytesBurst: int(p.ByteBurstPerAuthor),
	}, nil
}

// SystemLimits returns the existing Entmoot wire/system-topic limiter defaults.
func SystemLimits(_ *Policy) ratelimit.Limits {
	return ratelimit.DefaultLimits()
}
