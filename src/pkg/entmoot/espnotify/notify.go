// Package espnotify contains ESP-only wakeup notification providers.
package espnotify

import (
	"context"
	"errors"
)

// EventType names one mobile wakeup reason.
type EventType string

const (
	EventNotificationTest EventType = "notification_test"
	EventSignRequest      EventType = "sign_request"
)

// DeviceTarget identifies one mobile device push endpoint.
type DeviceTarget struct {
	DeviceID string
	Platform string
	Token    string
}

// WakeupEvent is the provider-neutral notification payload.
type WakeupEvent struct {
	Type    EventType
	GroupID string
	Reason  string
}

// DeliveryResult describes one provider delivery attempt.
type DeliveryResult struct {
	Status     string `json:"status"`
	ProviderID string `json:"provider_id,omitempty"`
	Retryable  bool   `json:"retryable,omitempty"`
}

// DeliveryError classifies provider failures so ESP state can react.
type DeliveryError struct {
	Code         string
	Message      string
	Retryable    bool
	InvalidToken bool
}

func (e *DeliveryError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	return e.Code
}

// Notifier sends a wakeup notification to a mobile device.
type Notifier interface {
	SendWakeup(context.Context, DeviceTarget, WakeupEvent) (DeliveryResult, error)
}

// NoopNotifier is the development/test provider.
type NoopNotifier struct{}

func (NoopNotifier) SendWakeup(_ context.Context, target DeviceTarget, _ WakeupEvent) (DeliveryResult, error) {
	if target.Token == "" {
		return DeliveryResult{}, &DeliveryError{Code: "missing_token", Message: "push token is not registered"}
	}
	return DeliveryResult{Status: "queued"}, nil
}

// IsInvalidToken reports whether err means the stored device token should be
// cleared.
func IsInvalidToken(err error) bool {
	var delivery *DeliveryError
	return errors.As(err, &delivery) && delivery.InvalidToken
}

// IsRetryable reports whether err may succeed later without client changes.
func IsRetryable(err error) bool {
	var delivery *DeliveryError
	return errors.As(err, &delivery) && delivery.Retryable
}
