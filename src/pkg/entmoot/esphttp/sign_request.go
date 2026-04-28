package esphttp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/signing"
)

const (
	signRequestKindMessagePublish = "message_publish"
	signRequestKindGroupCreate    = "group_create"
	signRequestKindGroupUpdate    = "group_update"
	signRequestKindInviteCreate   = "invite_create"
	signRequestKindInviteAccept   = "invite_accept"

	canonicalTypeMessageV1      = "entmoot.message.v1"
	canonicalTypeESPOperationV1 = "entmoot.esp.operation.v1"
	signatureAlgorithmEd25519   = "ed25519"
)

type messagePublishDraft struct {
	Author     entmoot.NodeInfo    `json:"author"`
	Topics     []string            `json:"topics,omitempty"`
	Content    []byte              `json:"content,omitempty"`
	Parents    []entmoot.MessageID `json:"parents,omitempty"`
	References []entmoot.MessageID `json:"references,omitempty"`
}

type messagePublishPayload struct {
	Message entmoot.Message `json:"message"`
}

type genericSignRequestEnvelope struct {
	Version     int             `json:"version"`
	RequestID   string          `json:"request_id"`
	Kind        string          `json:"kind"`
	GroupID     entmoot.GroupID `json:"group_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAtMS int64           `json:"created_at_ms"`
	ExpiresAtMS int64           `json:"expires_at_ms,omitempty"`
}

func buildMessagePublishSignRequest(deviceID string, groupID entmoot.GroupID, draft messagePublishDraft, timestampMS int64) (SignRequest, error) {
	if draft.Author.PilotNodeID == 0 {
		return SignRequest{}, fmt.Errorf("author.pilot_node_id is required")
	}
	if len(draft.Author.EntmootPubKey) != 32 {
		return SignRequest{}, fmt.Errorf("author.entmoot_pubkey must be 32 bytes")
	}
	msg := entmoot.Message{
		GroupID:    groupID,
		Author:     cloneNodeInfo(draft.Author),
		Timestamp:  timestampMS,
		Topics:     append([]string(nil), draft.Topics...),
		Parents:    append([]entmoot.MessageID(nil), draft.Parents...),
		Content:    append([]byte(nil), draft.Content...),
		References: append([]entmoot.MessageID(nil), draft.References...),
	}
	payload, err := json.Marshal(messagePublishPayload{Message: msg})
	if err != nil {
		return SignRequest{}, fmt.Errorf("marshal message publish payload: %w", err)
	}
	signingBytes, err := signing.MessageSigningBytes(msg)
	if err != nil {
		return SignRequest{}, fmt.Errorf("message signing bytes: %w", err)
	}
	req := SignRequest{
		DeviceID: deviceID,
		Kind:     signRequestKindMessagePublish,
		GroupID:  groupID,
		Payload:  append(json.RawMessage(nil), payload...),
	}
	setSignRequestSigningPayload(&req, canonicalTypeMessageV1, signingBytes)
	return req, nil
}

func ensureSignRequestSigningFields(req *SignRequest) error {
	if req == nil {
		return nil
	}
	if req.CanonicalType == "" {
		req.CanonicalType = canonicalTypeESPOperationV1
	}
	if req.SignatureAlgorithm == "" {
		req.SignatureAlgorithm = signatureAlgorithmEd25519
	}
	if req.SigningPayload != "" && req.SigningPayloadSHA256 != "" {
		return nil
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	signingBytes, err := canonical.Encode(genericSignRequestEnvelope{
		Version:     1,
		RequestID:   req.ID,
		Kind:        req.Kind,
		GroupID:     req.GroupID,
		Payload:     payload,
		CreatedAtMS: req.CreatedAtMS,
		ExpiresAtMS: req.ExpiresAtMS,
	})
	if err != nil {
		return fmt.Errorf("canonical sign request: %w", err)
	}
	setSignRequestSigningPayload(req, req.CanonicalType, signingBytes)
	return nil
}

func setSignRequestSigningPayload(req *SignRequest, canonicalType string, signingBytes []byte) {
	req.CanonicalType = canonicalType
	req.SignatureAlgorithm = signatureAlgorithmEd25519
	req.SigningPayload = base64.StdEncoding.EncodeToString(signingBytes)
	sum := sha256.Sum256(signingBytes)
	req.SigningPayloadSHA256 = base64.StdEncoding.EncodeToString(sum[:])
}

func cloneNodeInfo(in entmoot.NodeInfo) entmoot.NodeInfo {
	out := in
	out.EntmootPubKey = append([]byte(nil), in.EntmootPubKey...)
	return out
}
