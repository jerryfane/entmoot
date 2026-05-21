// Package publicmoot owns the founder-signed descriptor contract for public
// moot directory listing.
package publicmoot

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/policy"
)

const (
	DescriptorType = "entmoot.public_moot.v1"

	VisibilityPublic        = "public"
	JoinModeInviteOnly      = "invite_only"
	JoinModeOpenInvite      = "open_invite"
	defaultDescriptorMaxLen = 1 << 20
)

var (
	ErrInvalidDescriptor   = errors.New("publicmoot: invalid descriptor")
	ErrDescriptorSignature = errors.New("publicmoot: invalid descriptor signature")
)

type Descriptor struct {
	Type        string                `json:"type"`
	GroupID     entmoot.GroupID       `json:"group_id"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Tags        []string              `json:"tags,omitempty"`
	Visibility  string                `json:"visibility"`
	JoinMode    string                `json:"join_mode"`
	OpenInvite  *OpenInviteDescriptor `json:"open_invite,omitempty"`
	Policy      policy.Policy         `json:"policy"`
	Founder     entmoot.NodeInfo      `json:"founder"`
	Indexing    Indexing              `json:"indexing"`
	UpdatedAtMS int64                 `json:"updated_at_ms"`
	ExpiresAtMS int64                 `json:"expires_at_ms,omitempty"`
	Signature   []byte                `json:"signature,omitempty"`
}

type OpenInviteDescriptor struct {
	IssuerURL string `json:"issuer_url,omitempty"`
	Token     string `json:"token,omitempty"`
	Link      string `json:"link,omitempty"`
}

type Indexing struct {
	Directory bool `json:"directory"`
	Messages  bool `json:"messages"`
}

func Parse(raw []byte) (Descriptor, error) {
	if len(raw) > defaultDescriptorMaxLen {
		return Descriptor{}, fmt.Errorf("%w: descriptor exceeds %d bytes", ErrInvalidDescriptor, defaultDescriptorMaxLen)
	}
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

func Sign(desc Descriptor, identity *keystore.Identity) (Descriptor, error) {
	if identity == nil {
		return Descriptor{}, fmt.Errorf("%w: signer identity is required", ErrDescriptorSignature)
	}
	if len(identity.PublicKey) != ed25519.PublicKeySize {
		return Descriptor{}, fmt.Errorf("%w: signer public key length %d", ErrDescriptorSignature, len(identity.PublicKey))
	}
	if desc.Founder.PilotNodeID == 0 {
		return Descriptor{}, fmt.Errorf("%w: founder pilot_node_id is required", ErrInvalidDescriptor)
	}
	desc.Founder.EntmootPubKey = append([]byte(nil), identity.PublicKey...)
	signingBytes, err := SigningBytes(desc)
	if err != nil {
		return Descriptor{}, err
	}
	desc.Signature = identity.Sign(signingBytes)
	return desc, nil
}

func Verify(desc Descriptor) error {
	if err := Validate(desc); err != nil {
		return err
	}
	signingBytes, err := SigningBytes(desc)
	if err != nil {
		return err
	}
	if !keystore.Verify(desc.Founder.EntmootPubKey, signingBytes, desc.Signature) {
		return fmt.Errorf("%w: Ed25519 verification failed", ErrDescriptorSignature)
	}
	return nil
}

func SigningBytes(desc Descriptor) ([]byte, error) {
	signing := desc
	signing.Signature = nil
	return canonical.Encode(signing)
}

func Validate(desc Descriptor) error {
	if desc.Type != DescriptorType {
		return fmt.Errorf("%w: type must be %q", ErrInvalidDescriptor, DescriptorType)
	}
	if strings.TrimSpace(desc.Name) == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidDescriptor)
	}
	if desc.GroupID == (entmoot.GroupID{}) {
		return fmt.Errorf("%w: group_id is required", ErrInvalidDescriptor)
	}
	if desc.Visibility != VisibilityPublic {
		return fmt.Errorf("%w: visibility must be public", ErrInvalidDescriptor)
	}
	switch desc.JoinMode {
	case JoinModeInviteOnly, JoinModeOpenInvite:
	default:
		return fmt.Errorf("%w: join_mode must be invite_only or open_invite", ErrInvalidDescriptor)
	}
	if desc.JoinMode == JoinModeInviteOnly && desc.OpenInvite != nil {
		return fmt.Errorf("%w: open_invite is not allowed for invite_only join mode", ErrInvalidDescriptor)
	}
	if desc.JoinMode == JoinModeOpenInvite && desc.OpenInvite == nil {
		return fmt.Errorf("%w: open_invite is required for open_invite join mode", ErrInvalidDescriptor)
	}
	if desc.OpenInvite != nil {
		if err := ValidateOpenInvite(*desc.OpenInvite); err != nil {
			return err
		}
	}
	if err := desc.Policy.Validate(); err != nil {
		return fmt.Errorf("%w: policy: %v", ErrInvalidDescriptor, err)
	}
	if desc.Founder.PilotNodeID == 0 {
		return fmt.Errorf("%w: founder pilot_node_id is required", ErrInvalidDescriptor)
	}
	if len(desc.Founder.EntmootPubKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: founder entmoot_pubkey length %d", ErrInvalidDescriptor, len(desc.Founder.EntmootPubKey))
	}
	if !desc.Indexing.Directory {
		return fmt.Errorf("%w: indexing.directory must be true", ErrInvalidDescriptor)
	}
	if desc.Indexing.Messages {
		return fmt.Errorf("%w: indexing.messages is not supported in v1", ErrInvalidDescriptor)
	}
	if desc.UpdatedAtMS <= 0 {
		return fmt.Errorf("%w: updated_at_ms must be positive", ErrInvalidDescriptor)
	}
	if desc.ExpiresAtMS > 0 && desc.ExpiresAtMS <= desc.UpdatedAtMS {
		return fmt.Errorf("%w: expires_at_ms must be after updated_at_ms", ErrInvalidDescriptor)
	}
	if len(desc.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: signature length %d", ErrInvalidDescriptor, len(desc.Signature))
	}
	return nil
}

func ValidateOpenInvite(o OpenInviteDescriptor) error {
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
	if strings.TrimSpace(o.Link) != "" {
		target, err := parseOpenInviteLinkTarget(o.Link)
		if err != nil {
			return fmt.Errorf("%w: open_invite link: %v", ErrInvalidDescriptor, err)
		}
		if expected != nil && *expected != target {
			return fmt.Errorf("%w: open_invite link does not match issuer_url/token fields", ErrInvalidDescriptor)
		}
		expected = &target
	}
	if !hasIssuerToken && strings.TrimSpace(o.Link) == "" {
		return fmt.Errorf("%w: open_invite descriptor or link is required", ErrInvalidDescriptor)
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
