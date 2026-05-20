// Package policy defines local, group-scoped Entmoot enforcement policies.
package policy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const (
	DefaultMessageRatePerAuthor  = "6/min"
	DefaultMessageBurstPerAuthor = 12
	DefaultByteRatePerAuthor     = "64KiB/min"
	DefaultByteBurstPerAuthor    = 128 * 1024
	DefaultMaxMessageBytes       = 8192
	DefaultLiveTriggerRate       = "6/min"
	DefaultLiveTriggerBurst      = 6
	DefaultLiveMaxActionsPerScan = 1
	DefaultLiveMaxActionBytes    = 4096
	DefaultRetentionDays         = 30
)

// Policy is a local enforcement policy for one Entmoot group.
type Policy struct {
	MessageRatePerAuthor  string `json:"message_rate_per_author"`
	MessageBurstPerAuthor int    `json:"message_burst_per_author"`
	ByteRatePerAuthor     string `json:"byte_rate_per_author"`
	ByteBurstPerAuthor    int64  `json:"byte_burst_per_author"`
	MaxMessageBytes       int64  `json:"max_message_bytes"`
	LiveTriggerRate       string `json:"live_trigger_rate"`
	LiveTriggerBurst      int    `json:"live_trigger_burst"`
	LiveMaxActionsPerScan int    `json:"live_max_actions_per_scan"`
	LiveMaxActionBytes    int64  `json:"live_max_action_bytes"`
	RetentionDays         int    `json:"retention_days"`
}

// TheEntMootDefault returns the default local policy for The Ent Moot.
func TheEntMootDefault() Policy {
	return Policy{
		MessageRatePerAuthor:  DefaultMessageRatePerAuthor,
		MessageBurstPerAuthor: DefaultMessageBurstPerAuthor,
		ByteRatePerAuthor:     DefaultByteRatePerAuthor,
		ByteBurstPerAuthor:    DefaultByteBurstPerAuthor,
		MaxMessageBytes:       DefaultMaxMessageBytes,
		LiveTriggerRate:       DefaultLiveTriggerRate,
		LiveTriggerBurst:      DefaultLiveTriggerBurst,
		LiveMaxActionsPerScan: DefaultLiveMaxActionsPerScan,
		LiveMaxActionBytes:    DefaultLiveMaxActionBytes,
		RetentionDays:         DefaultRetentionDays,
	}
}

// Validate checks that p can be converted into concrete runtime limits.
func (p Policy) Validate() error {
	if _, err := ParseMessageRate(p.MessageRatePerAuthor); err != nil {
		return fmt.Errorf("message_rate_per_author: %w", err)
	}
	if p.MessageBurstPerAuthor <= 0 {
		return errors.New("message_burst_per_author must be positive")
	}
	if _, err := ParseByteRate(p.ByteRatePerAuthor); err != nil {
		return fmt.Errorf("byte_rate_per_author: %w", err)
	}
	if p.ByteBurstPerAuthor <= 0 {
		return errors.New("byte_burst_per_author must be positive")
	}
	if p.MaxMessageBytes <= 0 {
		return errors.New("max_message_bytes must be positive")
	}
	if p.ByteBurstPerAuthor < p.MaxMessageBytes {
		return errors.New("byte_burst_per_author must be greater than or equal to max_message_bytes")
	}
	if _, err := ParseMessageRate(p.LiveTriggerRate); err != nil {
		return fmt.Errorf("live_trigger_rate: %w", err)
	}
	if p.LiveTriggerBurst <= 0 {
		return errors.New("live_trigger_burst must be positive")
	}
	if p.LiveMaxActionsPerScan < 0 {
		return errors.New("live_max_actions_per_scan must be non-negative")
	}
	if p.LiveMaxActionBytes < 0 {
		return errors.New("live_max_action_bytes must be non-negative")
	}
	if p.RetentionDays <= 0 {
		return errors.New("retention_days must be positive")
	}
	return nil
}

// RateSpec is a parsed token-bucket refill rate.
type RateSpec struct {
	Units  int64
	Period time.Duration
}

// Limit converts the rate to the events-per-second unit expected by x/time/rate.
func (r RateSpec) Limit() rate.Limit {
	return rate.Limit(float64(r.Units) / r.Period.Seconds())
}

// ParseMessageRate parses rates such as "6/min".
func ParseMessageRate(raw string) (RateSpec, error) {
	return parseRate(raw, quantityMessages)
}

// ParseByteRate parses byte rates such as "64KiB/min".
func ParseByteRate(raw string) (RateSpec, error) {
	return parseRate(raw, quantityBytes)
}

type quantityKind int

const (
	quantityMessages quantityKind = iota
	quantityBytes
)

func parseRate(raw string, kind quantityKind) (RateSpec, error) {
	left, right, ok := strings.Cut(strings.TrimSpace(raw), "/")
	if !ok {
		return RateSpec{}, errors.New("rate must use <quantity>/<period>")
	}
	if strings.Contains(right, "/") {
		return RateSpec{}, errors.New("rate must contain one period separator")
	}
	units, err := parseQuantity(strings.TrimSpace(left), kind)
	if err != nil {
		return RateSpec{}, err
	}
	period, err := parsePeriod(strings.TrimSpace(right))
	if err != nil {
		return RateSpec{}, err
	}
	return RateSpec{Units: units, Period: period}, nil
}

func parseQuantity(raw string, kind quantityKind) (int64, error) {
	if raw == "" {
		return 0, errors.New("quantity is required")
	}
	if kind == quantityMessages {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n <= 0 {
			return 0, errors.New("message quantity must be a positive integer")
		}
		return n, nil
	}

	number, suffix := splitNumberSuffix(raw)
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("byte quantity must be a positive integer")
	}
	mult, ok := byteUnitMultiplier(suffix)
	if !ok {
		return 0, fmt.Errorf("unsupported byte unit %q", suffix)
	}
	if n > (1<<63-1)/mult {
		return 0, errors.New("byte quantity overflows int64")
	}
	return n * mult, nil
}

func splitNumberSuffix(raw string) (string, string) {
	for i, r := range raw {
		if r < '0' || r > '9' {
			return raw[:i], raw[i:]
		}
	}
	return raw, ""
}

func byteUnitMultiplier(raw string) (int64, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "b":
		return 1, true
	case "kib":
		return 1024, true
	case "mib":
		return 1024 * 1024, true
	case "gib":
		return 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func parsePeriod(raw string) (time.Duration, error) {
	switch strings.ToLower(raw) {
	case "s", "sec", "second", "seconds":
		return time.Second, nil
	case "m", "min", "minute", "minutes":
		return time.Minute, nil
	case "h", "hr", "hour", "hours":
		return time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported period %q", raw)
	}
}
