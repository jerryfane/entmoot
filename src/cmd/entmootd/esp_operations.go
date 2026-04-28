package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/ipc"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

type espOperationExecutor struct {
	dataDir       string
	identity      *keystore.Identity
	socketPath    string
	timeout       time.Duration
	metadataStore esphttp.GroupMetadataStore
	deviceGroups  deviceGroupAuthorizer
}

type groupCreatePayload struct {
	Name     string          `json:"name,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type inviteCreatePayload struct {
	ValidFor       string           `json:"valid_for,omitempty"`
	ValidUntilMS   int64            `json:"valid_until_ms,omitempty"`
	Peers          string           `json:"peers,omitempty"`
	BootstrapPeers []entmoot.NodeID `json:"bootstrap_peers,omitempty"`
}

type inviteAcceptPayload struct {
	Invite *entmoot.Invite `json:"invite,omitempty"`
}

func (e espOperationExecutor) ExecuteSignRequest(ctx context.Context, req esphttp.SignRequest, _ []byte) (json.RawMessage, error) {
	switch req.Kind {
	case "invite_accept":
		return e.acceptInvite(ctx, req)
	case "invite_create":
		return e.createInvite(ctx, req)
	case "group_create":
		return e.createGroup(ctx, req)
	case "group_update":
		return e.updateGroup(ctx, req)
	default:
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "unsupported_operation", Message: "unsupported sign request kind"}
	}
}

func (e espOperationExecutor) acceptInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	invite, err := parseInviteAccept(req.Payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	resp, err := e.joinGroup(ctx, invite)
	if err != nil {
		return nil, err
	}
	groupID := resp.GroupID
	if groupID == (entmoot.GroupID{}) {
		groupID = invite.GroupID
	}
	if err := e.grantDeviceGroupIfNeeded(ctx, req.DeviceID, groupID); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   resp.Status,
		"group_id": groupID,
		"members":  resp.Members,
	})
}

func (e espOperationExecutor) createInvite(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload inviteCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid invite_create payload"}
	}
	info, err := e.daemonInfo()
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	invite, err := e.buildInvite(ctx, req.GroupID, info.PilotNodeID, payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   "created",
		"group_id": invite.GroupID,
		"invite":   invite,
	})
}

func (e espOperationExecutor) createGroup(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	var payload groupCreatePayload
	if err := json.Unmarshal(req.Payload, &payload); err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid group_create payload"}
	}
	metadata, err := normalizeGroupMetadata(payload)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	info, err := e.daemonInfo()
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	var gid entmoot.GroupID
	gid, err = groupIDForCreateRequest(req)
	if err != nil {
		return nil, err
	}
	groupPath := groupDirPath(e.dataDir, gid)
	groupPreexisted := pathExists(groupPath)
	committed := false
	metadataWritten := false
	var previousMetadata json.RawMessage
	metadataHadPrevious := false
	deviceGroupGranted := false
	var st *store.SQLite
	var rlog *roster.RosterLog
	defer func() {
		if rlog != nil {
			_ = rlog.Close()
		}
		if st != nil {
			_ = st.Close()
		}
		if committed {
			return
		}
		if metadataWritten && e.metadataStore != nil {
			var err error
			if metadataHadPrevious {
				err = e.metadataStore.SetGroupMetadata(context.Background(), gid, previousMetadata)
			} else {
				err = e.metadataStore.DeleteGroupMetadata(context.Background(), gid)
			}
			if err != nil {
				slog.Warn("esp group_create rollback: restore metadata failed", slog.String("group_id", gid.String()), slog.String("err", err.Error()))
			}
		}
		if deviceGroupGranted && e.deviceGroups != nil {
			if err := e.deviceGroups.RevokeDeviceGroup(context.Background(), req.DeviceID, gid); err != nil {
				slog.Warn("esp group_create rollback: revoke device group failed", slog.String("group_id", gid.String()), slog.String("device_id", req.DeviceID), slog.String("err", err.Error()))
			}
		}
		if !groupPreexisted {
			if err := os.RemoveAll(groupPath); err != nil {
				slog.Warn("esp group_create rollback: remove group dir failed", slog.String("group_id", gid.String()), slog.String("path", groupPath), slog.String("err", err.Error()))
			}
		}
	}()
	if req.DeviceID != "" {
		changed, err := e.grantDeviceGroup(ctx, req.DeviceID, gid)
		if err != nil {
			return nil, err
		}
		deviceGroupGranted = changed
	}
	st, err = store.OpenSQLite(e.dataDir)
	if err != nil {
		return nil, err
	}
	rlog, err = roster.OpenJSONL(e.dataDir, gid)
	if err != nil {
		return nil, err
	}
	founder := entmoot.NodeInfo{
		PilotNodeID:   info.PilotNodeID,
		EntmootPubKey: append([]byte(nil), e.identity.PublicKey...),
	}
	now := req.CreatedAtMS
	if now == 0 {
		now = time.Now().UnixMilli()
	}
	if existing, ok := rlog.Founder(); ok {
		if existing.PilotNodeID != founder.PilotNodeID || !bytes.Equal(existing.EntmootPubKey, founder.EntmootPubKey) {
			return nil, &esphttp.OperationError{HTTPStatus: http.StatusConflict, Code: "group_create_conflict", Message: "deterministic group id already belongs to another founder"}
		}
	} else {
		if err := rlog.Genesis(e.identity, founder, now); err != nil {
			return nil, err
		}
	}
	if e.metadataStore != nil {
		previousMetadata, metadataHadPrevious, err = e.metadataStore.GetGroupMetadata(ctx, gid)
		if err != nil {
			return nil, err
		}
		if err := e.metadataStore.SetGroupMetadata(ctx, gid, metadata); err != nil {
			return nil, err
		}
		metadataWritten = true
	}
	root, err := st.MerkleRoot(ctx, gid)
	if err != nil {
		return nil, err
	}
	invite := entmoot.Invite{
		GroupID:    gid,
		Founder:    founder,
		RosterHead: rlog.Head(),
		MerkleRoot: root,
		IssuedAt:   now,
		ValidUntil: now + int64((24*time.Hour)/time.Millisecond),
		Issuer:     founder,
	}
	if err := signInvite(e.identity, &invite); err != nil {
		return nil, err
	}
	if err := rlog.Close(); err != nil {
		return nil, err
	}
	rlog = nil
	if err := st.Close(); err != nil {
		return nil, err
	}
	st = nil
	resp, err := e.joinGroup(ctx, invite)
	if err != nil {
		return nil, err
	}
	result, err := json.Marshal(map[string]any{
		"status":   resp.Status,
		"group_id": gid,
		"members":  resp.Members,
		"founder":  founder,
		"metadata": json.RawMessage(metadata),
	})
	if err != nil {
		return nil, err
	}
	committed = true
	return result, nil
}

func (e espOperationExecutor) updateGroup(ctx context.Context, req esphttp.SignRequest) (json.RawMessage, error) {
	if req.GroupID == (entmoot.GroupID{}) {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_update requires group_id"}
	}
	if e.metadataStore == nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "metadata_unavailable", Message: "group metadata store is not configured"}
	}
	metadata := req.Payload
	metadata, err := esphttp.NormalizeGroupMetadata(metadata)
	if err != nil {
		return nil, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
	}
	if err := e.metadataStore.SetGroupMetadata(ctx, req.GroupID, metadata); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"status":   "updated",
		"group_id": req.GroupID,
		"metadata": json.RawMessage(metadata),
	})
}

func (e espOperationExecutor) buildInvite(ctx context.Context, gid entmoot.GroupID, issuerNode entmoot.NodeID, payload inviteCreatePayload) (entmoot.Invite, error) {
	if gid == (entmoot.GroupID{}) {
		return entmoot.Invite{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invite_create requires group_id"}
	}
	rlog, err := roster.OpenJSONL(e.dataDir, gid)
	if err != nil {
		return entmoot.Invite{}, err
	}
	defer rlog.Close()
	founder, ok := rlog.Founder()
	if !ok {
		return entmoot.Invite{}, &esphttp.OperationError{HTTPStatus: http.StatusNotFound, Code: "group_not_found", Message: "group has no founder"}
	}
	st, err := store.OpenSQLite(e.dataDir)
	if err != nil {
		return entmoot.Invite{}, err
	}
	defer st.Close()
	root, err := st.MerkleRoot(ctx, gid)
	if err != nil {
		return entmoot.Invite{}, err
	}
	ttl := 24 * time.Hour
	if payload.ValidFor != "" {
		parsed, err := parseDurationDays(payload.ValidFor)
		if err != nil {
			return entmoot.Invite{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "invalid valid_for"}
		}
		if parsed <= 0 {
			return entmoot.Invite{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "valid_for must be positive"}
		}
		ttl = parsed
	}
	peers := append([]entmoot.NodeID(nil), payload.BootstrapPeers...)
	if len(peers) == 0 && payload.Peers != "" {
		parsed, err := parsePeerList(payload.Peers)
		if err != nil {
			return entmoot.Invite{}, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: err.Error()}
		}
		peers = parsed
	}
	if len(peers) == 0 {
		peers = defaultBootstrapPeers(rlog, founder.PilotNodeID, 5)
	}
	bootstrap := make([]entmoot.BootstrapPeer, 0, len(peers))
	for _, p := range peers {
		if p == issuerNode {
			continue
		}
		bootstrap = append(bootstrap, entmoot.BootstrapPeer{NodeID: p})
	}
	issuedAt := time.Now().UnixMilli()
	validUntil := issuedAt + ttl.Milliseconds()
	if payload.ValidUntilMS > 0 {
		validUntil = payload.ValidUntilMS
	}
	invite := entmoot.Invite{
		GroupID:        gid,
		Founder:        founder,
		RosterHead:     rlog.Head(),
		MerkleRoot:     root,
		BootstrapPeers: bootstrap,
		IssuedAt:       issuedAt,
		ValidUntil:     validUntil,
		Issuer: entmoot.NodeInfo{
			PilotNodeID:   issuerNode,
			EntmootPubKey: append([]byte(nil), e.identity.PublicKey...),
		},
	}
	if err := signInvite(e.identity, &invite); err != nil {
		return entmoot.Invite{}, err
	}
	return invite, nil
}

func (e espOperationExecutor) daemonInfo() (*ipc.InfoResp, error) {
	return infoOverIPC(e.socketPath)
}

func (e espOperationExecutor) joinGroup(ctx context.Context, invite entmoot.Invite) (*ipc.JoinGroupResp, error) {
	timeout := e.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "unix", e.socketPath)
	if err != nil {
		return nil, joinUnavailableError(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := ipc.EncodeAndWrite(conn, &ipc.JoinGroupReq{Invite: invite}); err != nil {
		return nil, err
	}
	_, payload, err := ipc.ReadAndDecode(conn)
	if err != nil {
		return nil, err
	}
	switch v := payload.(type) {
	case *ipc.JoinGroupResp:
		return v, nil
	case *ipc.ErrorFrame:
		return nil, operationIPCError(v)
	default:
		return nil, fmt.Errorf("unexpected join group response %T", payload)
	}
}

func parseInviteAccept(payload json.RawMessage) (entmoot.Invite, error) {
	var wrapped inviteAcceptPayload
	if err := json.Unmarshal(payload, &wrapped); err == nil && wrapped.Invite != nil {
		return *wrapped.Invite, nil
	}
	var invite entmoot.Invite
	if err := json.Unmarshal(payload, &invite); err != nil {
		return entmoot.Invite{}, fmt.Errorf("invalid invite_accept payload")
	}
	return invite, nil
}

func signInvite(identity *keystore.Identity, invite *entmoot.Invite) error {
	if identity == nil || invite == nil {
		return fmt.Errorf("invite signer is not configured")
	}
	signing := *invite
	signing.Signature = nil
	sigInput, err := canonical.Encode(signing)
	if err != nil {
		return err
	}
	invite.Signature = identity.Sign(sigInput)
	return nil
}

func normalizeGroupMetadata(payload groupCreatePayload) (json.RawMessage, error) {
	if len(payload.Metadata) > 0 && json.Valid(payload.Metadata) {
		return esphttp.NormalizeGroupMetadata(payload.Metadata)
	}
	if len(payload.Metadata) > 0 {
		return nil, fmt.Errorf("esphttp: group metadata must be a JSON object")
	}
	if payload.Name == "" {
		return esphttp.NormalizeGroupMetadata(nil)
	}
	data, err := json.Marshal(map[string]string{"name": payload.Name})
	if err != nil {
		return nil, err
	}
	return esphttp.NormalizeGroupMetadata(data)
}

func groupIDForCreateRequest(req esphttp.SignRequest) (entmoot.GroupID, error) {
	var gid entmoot.GroupID
	if req.ID == "" {
		return gid, &esphttp.OperationError{HTTPStatus: http.StatusBadRequest, Code: "bad_request", Message: "group_create requires sign request id"}
	}
	sum := sha256.Sum256([]byte("entmoot.esp.group_create.v1\x00" + req.ID + "\x00" + req.SigningPayloadSHA256))
	copy(gid[:], sum[:])
	return gid, nil
}

func groupDirPath(dataDir string, gid entmoot.GroupID) string {
	return filepath.Join(groupsDir(dataDir), base64.RawURLEncoding.EncodeToString(gid[:]))
}

func (e espOperationExecutor) grantDeviceGroupIfNeeded(ctx context.Context, deviceID string, gid entmoot.GroupID) error {
	if deviceID == "" {
		return nil
	}
	_, err := e.grantDeviceGroup(ctx, deviceID, gid)
	return err
}

func (e espOperationExecutor) grantDeviceGroup(ctx context.Context, deviceID string, gid entmoot.GroupID) (bool, error) {
	if e.deviceGroups == nil {
		return false, &esphttp.OperationError{HTTPStatus: http.StatusServiceUnavailable, Code: "device_registry_unavailable", Message: "device group authorizer is not configured"}
	}
	return e.deviceGroups.GrantDeviceGroup(ctx, deviceID, gid)
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func joinUnavailableError(err error) error {
	return &esphttp.OperationError{
		HTTPStatus: http.StatusServiceUnavailable,
		Code:       "join_unavailable",
		Message:    noJoinHelp + ": " + err.Error(),
	}
}

func operationIPCError(frame *ipc.ErrorFrame) error {
	if frame == nil {
		return &esphttp.OperationError{HTTPStatus: http.StatusInternalServerError, Code: "internal_error", Message: "operation failed"}
	}
	status := http.StatusInternalServerError
	code := "internal_error"
	switch frame.Code {
	case ipc.CodeInvalidArgument:
		status = http.StatusBadRequest
		code = "bad_request"
	case ipc.CodeNotMember:
		status = http.StatusForbidden
		code = "not_member"
	case ipc.CodeGroupNotFound:
		status = http.StatusNotFound
		code = "group_not_found"
	case ipc.CodeInternal:
		status = http.StatusInternalServerError
	}
	return &esphttp.OperationError{HTTPStatus: status, Code: code, Message: frame.Message}
}
