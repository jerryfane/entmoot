package espnotify

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	APNsProductionEndpoint = "https://api.push.apple.com"
	APNsSandboxEndpoint    = "https://api.sandbox.push.apple.com"
)

// APNsConfig configures token-authenticated APNs delivery.
type APNsConfig struct {
	TeamID     string
	KeyID      string
	Topic      string
	KeyPath    string
	Endpoint   string
	HTTPClient *http.Client
	Now        func() time.Time
}

// APNsNotifier sends background wakeups through Apple's provider API.
type APNsNotifier struct {
	teamID string
	keyID  string
	topic  string
	key    *ecdsa.PrivateKey
	client *http.Client
	url    string
	now    func() time.Time
}

// NewAPNsNotifier loads cfg.KeyPath and returns an APNs notifier.
func NewAPNsNotifier(cfg APNsConfig) (*APNsNotifier, error) {
	if strings.TrimSpace(cfg.TeamID) == "" {
		return nil, errors.New("espnotify: APNs team id is required")
	}
	if strings.TrimSpace(cfg.KeyID) == "" {
		return nil, errors.New("espnotify: APNs key id is required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("espnotify: APNs topic is required")
	}
	if strings.TrimSpace(cfg.KeyPath) == "" {
		return nil, errors.New("espnotify: APNs key path is required")
	}
	keyData, err := os.ReadFile(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("espnotify: read APNs key: %w", err)
	}
	key, err := parseAPNsPrivateKey(keyData)
	if err != nil {
		return nil, err
	}
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	if endpoint == "" {
		endpoint = APNsProductionEndpoint
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &APNsNotifier{
		teamID: strings.TrimSpace(cfg.TeamID),
		keyID:  strings.TrimSpace(cfg.KeyID),
		topic:  strings.TrimSpace(cfg.Topic),
		key:    key,
		client: client,
		url:    endpoint,
		now:    now,
	}, nil
}

func parseAPNsPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("espnotify: APNs key must be PEM encoded")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("espnotify: parse APNs key: %w", err)
	}
	ecdsaKey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("espnotify: APNs key must be ECDSA")
	}
	return ecdsaKey, nil
}

func (n *APNsNotifier) SendWakeup(ctx context.Context, target DeviceTarget, event WakeupEvent) (DeliveryResult, error) {
	token := strings.TrimSpace(target.Token)
	if token == "" {
		return DeliveryResult{}, &DeliveryError{Code: "missing_token", Message: "push token is not registered"}
	}
	auth, err := n.jwt()
	if err != nil {
		return DeliveryResult{}, err
	}
	payload := map[string]any{
		"aps": map[string]any{
			"content-available": 1,
		},
		"type": event.Type,
	}
	if event.GroupID != "" {
		payload["group_id"] = event.GroupID
	}
	if event.Reason != "" {
		payload["reason"] = event.Reason
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return DeliveryResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url+"/3/device/"+token, bytes.NewReader(body))
	if err != nil {
		return DeliveryResult{}, err
	}
	req.Header.Set("authorization", "bearer "+auth)
	req.Header.Set("apns-topic", n.topic)
	req.Header.Set("apns-push-type", "background")
	req.Header.Set("apns-priority", "5")
	req.Header.Set("content-type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		return DeliveryResult{}, &DeliveryError{Code: "provider_unavailable", Message: err.Error(), Retryable: true}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return DeliveryResult{Status: "sent", ProviderID: resp.Header.Get("apns-id")}, nil
	}
	reason := apnsReason(data)
	return DeliveryResult{}, classifyAPNSError(resp.StatusCode, reason)
}

func (n *APNsNotifier) jwt() (string, error) {
	header, err := json.Marshal(map[string]string{
		"alg": "ES256",
		"kid": n.keyID,
	})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss": n.teamID,
		"iat": n.now().Unix(),
	})
	if err != nil {
		return "", err
	}
	unsigned := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	sum := sha256.Sum256([]byte(unsigned))
	r, s, err := ecdsa.Sign(rand.Reader, n.key, sum[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func apnsReason(data []byte) string {
	var body struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return ""
	}
	return body.Reason
}

func classifyAPNSError(status int, reason string) *DeliveryError {
	code := strings.TrimSpace(reason)
	if code == "" {
		code = fmt.Sprintf("apns_status_%d", status)
	}
	err := &DeliveryError{Code: code, Message: code}
	switch status {
	case http.StatusGone:
		err.InvalidToken = true
	case http.StatusBadRequest:
		switch reason {
		case "BadDeviceToken", "DeviceTokenNotForTopic":
			err.InvalidToken = true
		}
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable:
		err.Retryable = true
	}
	return err
}
