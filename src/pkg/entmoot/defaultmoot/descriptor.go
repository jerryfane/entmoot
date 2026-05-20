// Package defaultmoot owns the signed descriptor contract for The Ent Moot.
package defaultmoot

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
)

const (
	DescriptorType = "entmoot.default_moot.v1"
	Name           = "The Ent Moot"

	DefaultDescriptorURL = "https://entmoot.xyz/.well-known/the-ent-moot.json"
	// DefaultDescriptorPubKeyBase64 is the pinned Ed25519 public key for the
	// official descriptor signer. The matching private key is intentionally
	// not tracked in Git.
	DefaultDescriptorPubKeyBase64 = "UsIV+iaJEljYZSdiW+h9NoA+qkBNhsZTAAEJPweJrz8="

	EnvDescriptorURL    = "ENTMOOT_DEFAULT_MOOT_DESCRIPTOR_URL"
	EnvDescriptorPubKey = "ENTMOOT_DEFAULT_MOOT_DESCRIPTOR_PUBKEY"

	defaultDescriptorMaxBytes = 1 << 20
)

var (
	ErrInvalidDescriptor     = errors.New("defaultmoot: invalid descriptor")
	ErrDescriptorSignature   = errors.New("defaultmoot: invalid descriptor signature")
	ErrDescriptorUnavailable = errors.New("defaultmoot: descriptor unavailable")
)

// Descriptor is the signed, well-known bootstrap document for The Ent Moot.
type Descriptor struct {
	Type                   string                `json:"type"`
	Name                   string                `json:"name"`
	GroupID                entmoot.GroupID       `json:"group_id"`
	OpenInvite             OpenInviteDescriptor  `json:"open_invite,omitempty"`
	OpenInviteLink         string                `json:"open_invite_link,omitempty"`
	DescriptorSignerPubKey []byte                `json:"descriptor_signer_pubkey"`
	Issuer                 entmoot.NodeInfo      `json:"issuer"`
	DefaultTopics          []string              `json:"default_topics"`
	RecommendedLiveConfig  RecommendedLiveConfig `json:"recommended_live_config"`
	Policy                 policy.Policy         `json:"policy"`
	IssuedAtMS             int64                 `json:"issued_at_ms"`
	Signature              []byte                `json:"signature,omitempty"`
}

// OpenInviteDescriptor is the existing open-invite descriptor/link payload
// embedded inside the signed default-moot descriptor.
type OpenInviteDescriptor struct {
	IssuerURL string `json:"issuer_url,omitempty"`
	Token     string `json:"token,omitempty"`
	Link      string `json:"link,omitempty"`
}

// RecommendedLiveConfig describes the safe default live-agent settings for the
// public moot. Later CLI tasks consume this; this package only validates and
// transports it.
type RecommendedLiveConfig struct {
	Mode           string   `json:"mode"`
	AllowedActions []string `json:"allowed_actions"`
	MaxActions     int      `json:"max_actions"`
	MaxActionBytes int64    `json:"max_action_bytes"`
}

// Config is the resolved fetch-and-verify configuration.
type Config struct {
	URL             string
	PinnedPublicKey ed25519.PublicKey
}

// LoadConfigFromEnv resolves the descriptor URL and pinned key from defaults
// plus documented environment overrides.
func LoadConfigFromEnv() (Config, error) {
	return ConfigFromEnv(os.Getenv)
}

// ConfigFromEnv resolves descriptor configuration using lookup, making tests
// independent of process-global environment state.
func ConfigFromEnv(lookup func(string) string) (Config, error) {
	if lookup == nil {
		lookup = os.Getenv
	}
	descriptorURL := strings.TrimSpace(lookup(EnvDescriptorURL))
	if descriptorURL == "" {
		descriptorURL = DefaultDescriptorURL
	}
	if err := validateHTTPURL(descriptorURL); err != nil {
		return Config{}, fmt.Errorf("%w: descriptor URL: %v", ErrInvalidDescriptor, err)
	}
	pubRaw := strings.TrimSpace(lookup(EnvDescriptorPubKey))
	if pubRaw == "" {
		pubRaw = DefaultDescriptorPubKeyBase64
	}
	pub, err := DecodePublicKey(pubRaw)
	if err != nil {
		return Config{}, err
	}
	return Config{URL: descriptorURL, PinnedPublicKey: pub}, nil
}

// DecodePublicKey parses a base64-encoded Ed25519 public key.
func DecodePublicKey(raw string) (ed25519.PublicKey, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("%w: descriptor public key is required", ErrInvalidDescriptor)
	}
	decoders := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	var lastErr error
	for _, enc := range decoders {
		key, err := enc.DecodeString(raw)
		if err == nil {
			if len(key) != ed25519.PublicKeySize {
				return nil, fmt.Errorf("%w: descriptor public key length %d", ErrInvalidDescriptor, len(key))
			}
			return ed25519.PublicKey(key), nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("%w: descriptor public key is not base64: %v", ErrInvalidDescriptor, lastErr)
}

// FetchAndVerify fetches the configured descriptor and verifies it with the
// pinned public key before returning it to any join/live caller.
func FetchAndVerify(ctx context.Context, client *http.Client, cfg Config) (Descriptor, error) {
	desc, err := Fetch(ctx, client, cfg.URL)
	if err != nil {
		return Descriptor{}, err
	}
	if err := Verify(desc, cfg.PinnedPublicKey); err != nil {
		return Descriptor{}, err
	}
	return desc, nil
}

// Fetch retrieves and decodes a descriptor from descriptorURL.
func Fetch(ctx context.Context, client *http.Client, descriptorURL string) (Descriptor, error) {
	if err := validateHTTPURL(descriptorURL); err != nil {
		return Descriptor{}, fmt.Errorf("%w: descriptor URL: %v", ErrInvalidDescriptor, err)
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, descriptorURL, nil)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: build request: %v", ErrDescriptorUnavailable, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: fetch: %v", ErrDescriptorUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return Descriptor{}, fmt.Errorf("%w: %s", ErrDescriptorUnavailable, resp.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, defaultDescriptorMaxBytes+1))
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: read: %v", ErrDescriptorUnavailable, err)
	}
	if len(raw) > defaultDescriptorMaxBytes {
		return Descriptor{}, fmt.Errorf("%w: descriptor exceeds %d bytes", ErrInvalidDescriptor, defaultDescriptorMaxBytes)
	}
	return Parse(raw)
}

// Parse decodes a descriptor without verifying its signature.
func Parse(raw []byte) (Descriptor, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var desc Descriptor
	if err := dec.Decode(&desc); err != nil {
		return Descriptor{}, fmt.Errorf("%w: decode: %v", ErrInvalidDescriptor, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Descriptor{}, fmt.Errorf("%w: multiple JSON values", ErrInvalidDescriptor)
	}
	return desc, nil
}

// Verify validates desc and verifies its Ed25519 signature against pinnedKey.
func Verify(desc Descriptor, pinnedKey ed25519.PublicKey) error {
	if err := Validate(desc); err != nil {
		return err
	}
	if len(pinnedKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: pinned public key length %d", ErrDescriptorSignature, len(pinnedKey))
	}
	if !bytes.Equal(desc.DescriptorSignerPubKey, pinnedKey) {
		return fmt.Errorf("%w: descriptor signer does not match pinned public key", ErrDescriptorSignature)
	}
	signingBytes, err := SigningBytes(desc)
	if err != nil {
		return err
	}
	if !keystore.Verify(pinnedKey, signingBytes, desc.Signature) {
		return fmt.Errorf("%w: Ed25519 verification failed", ErrDescriptorSignature)
	}
	return nil
}

// Sign signs desc with priv and returns a copy with DescriptorSignerPubKey and
// Signature populated. It is useful for tests and release tooling that creates
// the well-known JSON document outside this package.
func Sign(desc Descriptor, priv ed25519.PrivateKey) (Descriptor, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return Descriptor{}, fmt.Errorf("%w: descriptor private key length %d", ErrDescriptorSignature, len(priv))
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return Descriptor{}, fmt.Errorf("%w: descriptor private key has no Ed25519 public key", ErrDescriptorSignature)
	}
	desc.DescriptorSignerPubKey = append([]byte(nil), pub...)
	signingBytes, err := SigningBytes(desc)
	if err != nil {
		return Descriptor{}, err
	}
	desc.Signature = ed25519.Sign(priv, signingBytes)
	return desc, nil
}

// SigningBytes returns the canonical descriptor bytes covered by Signature.
func SigningBytes(desc Descriptor) ([]byte, error) {
	signing := desc
	signing.Signature = nil
	return canonical.Encode(signing)
}

// Validate checks required descriptor fields before signature verification.
func Validate(desc Descriptor) error {
	if desc.Type != DescriptorType {
		return fmt.Errorf("%w: type must be %q", ErrInvalidDescriptor, DescriptorType)
	}
	if strings.TrimSpace(desc.Name) != Name {
		return fmt.Errorf("%w: name must be %q", ErrInvalidDescriptor, Name)
	}
	if desc.GroupID == (entmoot.GroupID{}) {
		return fmt.Errorf("%w: group_id is required", ErrInvalidDescriptor)
	}
	if err := desc.OpenInvite.validate(desc.OpenInviteLink); err != nil {
		return err
	}
	if len(desc.DescriptorSignerPubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: descriptor_signer_pubkey length %d", ErrInvalidDescriptor, len(desc.DescriptorSignerPubKey))
	}
	if desc.Issuer.PilotNodeID == 0 {
		return fmt.Errorf("%w: issuer pilot_node_id is required", ErrInvalidDescriptor)
	}
	if len(desc.Issuer.EntmootPubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: issuer entmoot_pubkey length %d", ErrInvalidDescriptor, len(desc.Issuer.EntmootPubKey))
	}
	if !defaultTopicsOK(desc.DefaultTopics) {
		return fmt.Errorf("%w: default_topics must be [chat/general introductions]", ErrInvalidDescriptor)
	}
	if err := validateRecommendedLiveConfig(desc.RecommendedLiveConfig); err != nil {
		return err
	}
	if err := desc.Policy.Validate(); err != nil {
		return fmt.Errorf("%w: policy: %v", ErrInvalidDescriptor, err)
	}
	if desc.IssuedAtMS <= 0 {
		return fmt.Errorf("%w: issued_at_ms must be positive", ErrInvalidDescriptor)
	}
	if len(desc.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: signature length %d", ErrInvalidDescriptor, len(desc.Signature))
	}
	return nil
}

func (o OpenInviteDescriptor) validate(topLevelLink string) error {
	hasIssuerToken := strings.TrimSpace(o.IssuerURL) != "" || strings.TrimSpace(o.Token) != ""
	var expected *openInviteTarget
	if hasIssuerToken {
		if strings.TrimSpace(o.IssuerURL) == "" || strings.TrimSpace(o.Token) == "" {
			return fmt.Errorf("%w: open_invite requires issuer_url and token together", ErrInvalidDescriptor)
		}
		target, err := normalizeOpenInviteTarget(o.IssuerURL, o.Token)
		if err != nil {
			return fmt.Errorf("%w: open_invite issuer_url: %v", ErrInvalidDescriptor, err)
		}
		expected = &target
	}
	hasLink := false
	for _, rawLink := range []string{o.Link, topLevelLink} {
		if strings.TrimSpace(rawLink) == "" {
			continue
		}
		hasLink = true
		target, err := parseOpenInviteLinkTarget(rawLink)
		if err != nil {
			return fmt.Errorf("%w: open invite link: %v", ErrInvalidDescriptor, err)
		}
		if expected != nil && *expected != target {
			return fmt.Errorf("%w: open invite link does not match issuer_url/token fields", ErrInvalidDescriptor)
		}
		expected = &target
	}
	if !hasIssuerToken && !hasLink {
		return fmt.Errorf("%w: open invite descriptor or link is required", ErrInvalidDescriptor)
	}
	return nil
}

func validateHTTPURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("host is required")
	}
	return nil
}

type openInviteTarget struct {
	issuer string
	token  string
}

func normalizeOpenInviteTarget(issuerRaw, tokenRaw string) (openInviteTarget, error) {
	issuer, err := url.Parse(strings.TrimSpace(issuerRaw))
	if err != nil {
		return openInviteTarget{}, err
	}
	if err := validateOpenInviteIssuerURL(issuer.String()); err != nil {
		return openInviteTarget{}, err
	}
	token := strings.TrimSpace(tokenRaw)
	if token == "" {
		return openInviteTarget{}, errors.New("token is required")
	}
	return openInviteTarget{issuer: issuer.String(), token: token}, nil
}

func parseOpenInviteLinkTarget(raw string) (openInviteTarget, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return openInviteTarget{}, err
	}
	if u.Scheme != "entmoot" {
		return openInviteTarget{}, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	linkKind := strings.Trim(strings.TrimSpace(u.Host+u.Path), "/")
	if linkKind != "open-invite" {
		return openInviteTarget{}, fmt.Errorf("unsupported entmoot link %q", linkKind)
	}
	q := u.Query()
	return normalizeOpenInviteTarget(q.Get("issuer"), q.Get("token"))
}

func validateOpenInviteIssuerURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return errors.New("issuer_url must be an absolute http(s) URL")
	}
	if u.User != nil {
		return errors.New("issuer_url must not contain credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("issuer_url must not contain query or fragment")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && issuerHostAllowsCleartext(u.Hostname())) {
		return errors.New("issuer_url must use https except for localhost or .local development hosts")
	}
	return nil
}

func issuerHostAllowsCleartext(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".local")
}

func defaultTopicsOK(topics []string) bool {
	return len(topics) == 2 && topics[0] == "chat/general" && topics[1] == "introductions"
}

func validateRecommendedLiveConfig(cfg RecommendedLiveConfig) error {
	if cfg.Mode != "converse" {
		return fmt.Errorf("%w: recommended_live_config mode must be converse", ErrInvalidDescriptor)
	}
	if len(cfg.AllowedActions) != 1 || cfg.AllowedActions[0] != "reply" {
		return fmt.Errorf("%w: recommended_live_config allowed_actions must be [reply]", ErrInvalidDescriptor)
	}
	if cfg.MaxActions != policy.DefaultLiveMaxActionsPerScan {
		return fmt.Errorf("%w: recommended_live_config max_actions must be %d", ErrInvalidDescriptor, policy.DefaultLiveMaxActionsPerScan)
	}
	if cfg.MaxActionBytes != policy.DefaultLiveMaxActionBytes {
		return fmt.Errorf("%w: recommended_live_config max_action_bytes must be %d", ErrInvalidDescriptor, policy.DefaultLiveMaxActionBytes)
	}
	return nil
}
