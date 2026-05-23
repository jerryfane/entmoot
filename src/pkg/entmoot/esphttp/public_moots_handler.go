package esphttp

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/publicmoot"
)

const (
	PublicMootMirrorNone   = "none"
	PublicMootMirrorMember = "member"
	PublicMootMirrorHosted = "hosted"
)

type PublicMootDirectoryEntry struct {
	Type                    string                           `json:"type"`
	GroupID                 entmoot.GroupID                  `json:"group_id"`
	Name                    string                           `json:"name"`
	Description             string                           `json:"description,omitempty"`
	Tags                    []string                         `json:"tags,omitempty"`
	Visibility              string                           `json:"visibility"`
	JoinMode                string                           `json:"join_mode"`
	OpenInvite              *publicmoot.OpenInviteDescriptor `json:"open_invite,omitempty"`
	Policy                  policy.Policy                    `json:"policy"`
	PolicySummary           string                           `json:"policy_summary"`
	Founder                 entmoot.NodeInfo                 `json:"founder"`
	Indexing                publicmoot.Indexing              `json:"indexing"`
	UpdatedAtMS             int64                            `json:"updated_at_ms"`
	ExpiresAtMS             int64                            `json:"expires_at_ms,omitempty"`
	Signature               []byte                           `json:"signature,omitempty"`
	IndexStatus             string                           `json:"index_status"`
	MirrorState             string                           `json:"mirror_state"`
	MessageHistoryAvailable bool                             `json:"message_history_available"`
}

type publicMootPostResponse struct {
	Status string                   `json:"status"`
	Moot   PublicMootDirectoryEntry `json:"moot"`
}

type publicMootIndexStatusPatch struct {
	Status string `json:"status"`
}

func (h *Handler) handlePublicMootRoute(w http.ResponseWriter, r *http.Request, authed bool) bool {
	const root = "/v1/public-moots"
	if r.URL.Path == root {
		if authed {
			return false
		}
		switch r.Method {
		case http.MethodGet:
			h.handleListPublicMoots(w, r)
		case http.MethodPost:
			h.handlePostPublicMoot(w, r)
		default:
			methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
		}
		return true
	}
	const prefix = root + "/"
	escapedPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escapedPath, prefix) {
		return false
	}
	rest := strings.TrimPrefix(escapedPath, prefix)
	escapedGroup, suffix, _ := strings.Cut(rest, "/")
	rawGroup, err := url.PathUnescape(escapedGroup)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	groupID, err := decodeGroupID(rawGroup)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return true
	}
	if suffix == "" && !authed {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGetPublicMoot(w, r, groupID)
		return true
	}
	if suffix == "index-status" && authed {
		if r.Method != http.MethodPatch {
			methodNotAllowed(w, http.MethodPatch)
			return true
		}
		h.handlePatchPublicMootIndexStatus(w, r, groupID)
		return true
	}
	return false
}

func (h *Handler) handleListPublicMoots(w http.ResponseWriter, r *http.Request) {
	if h.state == nil {
		writeJSON(w, http.StatusOK, map[string]any{"moots": []PublicMootDirectoryEntry{}})
		return
	}
	records, err := h.state.ListPublicMoots(r.Context(), PublicMootStatusListed)
	if err != nil {
		h.logger.Error("esphttp: list public moots", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot listing failed")
		return
	}
	entries := make([]PublicMootDirectoryEntry, 0, len(records))
	nowMS := h.clock().UnixMilli()
	for _, rec := range records {
		if !PublicMootRecordHasDescriptor(rec) {
			continue
		}
		if publicMootDescriptorExpired(rec.Descriptor, nowMS) {
			continue
		}
		entry, err := h.publicMootDirectoryEntry(r, rec)
		if err != nil {
			h.logger.Error("esphttp: public moot entry", slog.String("err", err.Error()))
			writeError(w, http.StatusInternalServerError, "internal_error", "public moot listing failed")
			return
		}
		entries = append(entries, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"moots": entries})
}

func (h *Handler) handleGetPublicMoot(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	if h.state == nil {
		writeError(w, http.StatusNotFound, "public_moot_not_found", "public moot not found")
		return
	}
	rec, ok, err := h.state.GetPublicMoot(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: get public moot", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot lookup failed")
		return
	}
	if !ok ||
		NormalizePublicMootStatus(rec.Status) != PublicMootStatusListed ||
		!PublicMootRecordHasDescriptor(rec) ||
		publicMootDescriptorExpired(rec.Descriptor, h.clock().UnixMilli()) {
		writeError(w, http.StatusNotFound, "public_moot_not_found", "public moot not found")
		return
	}
	entry, err := h.publicMootDirectoryEntry(r, rec)
	if err != nil {
		h.logger.Error("esphttp: public moot entry", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (h *Handler) handlePostPublicMoot(w http.ResponseWriter, r *http.Request) {
	if h.state == nil {
		writeError(w, http.StatusServiceUnavailable, "public_directory_unavailable", "public directory store is not configured")
		return
	}
	body, ok := decodeRawBody(w, r, 1<<20, nil)
	if !ok {
		return
	}
	desc, err := publicmoot.Parse(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_descriptor", err.Error())
		return
	}
	if err := publicmoot.Verify(desc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_descriptor", err.Error())
		return
	}
	if desc.ExpiresAtMS > 0 && desc.ExpiresAtMS <= h.clock().UnixMilli() {
		writeError(w, http.StatusBadRequest, "invalid_descriptor", "descriptor is expired")
		return
	}
	blocked, err := h.publicMootBlocked(r, desc)
	if err != nil {
		h.logger.Error("esphttp: public moot block check", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot block check failed")
		return
	}
	if blocked {
		writeError(w, http.StatusForbidden, "public_moot_blocked", "public moot is blocked")
		return
	}
	rec, stored, err := h.state.UpsertPublicMootDescriptor(r.Context(), PublicMootRecord{Descriptor: desc}, h.clock().UnixMilli())
	if err != nil {
		if errors.Is(err, ErrPublicMootFounderMismatch) {
			writeError(w, http.StatusConflict, "public_moot_founder_mismatch", "public moot founder does not match existing descriptor")
			return
		}
		h.logger.Error("esphttp: upsert public moot", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot indexing failed")
		return
	}
	entry, err := h.publicMootDirectoryEntry(r, rec)
	if err != nil {
		h.logger.Error("esphttp: public moot entry", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot indexing failed")
		return
	}
	status := "indexed"
	code := http.StatusCreated
	if !stored {
		status = "stale_ignored"
		code = http.StatusOK
	}
	if NormalizePublicMootStatus(rec.Status) != PublicMootStatusListed {
		status = NormalizePublicMootStatus(rec.Status)
		code = http.StatusAccepted
	}
	writeJSON(w, code, publicMootPostResponse{Status: status, Moot: entry})
}

func (h *Handler) handlePatchPublicMootIndexStatus(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	auth := authFromContext(r)
	if !auth.bearer {
		writeError(w, http.StatusForbidden, "forbidden", "public directory moderation requires bearer admin auth")
		return
	}
	var patch publicMootIndexStatusPatch
	if _, ok := decodeRawBody(w, r, 64<<10, &patch); !ok {
		return
	}
	if strings.TrimSpace(patch.Status) == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "public moot status is required")
		return
	}
	status := NormalizePublicMootStatus(patch.Status)
	if status == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid public moot status")
		return
	}
	rec, ok, err := h.state.SetPublicMootIndexStatus(r.Context(), groupID, status, h.clock().UnixMilli())
	if err != nil {
		h.logger.Error("esphttp: set public moot status", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot status update failed")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "public_moot_not_found", "public moot not found")
		return
	}
	if !PublicMootRecordHasDescriptor(rec) {
		writeJSON(w, http.StatusOK, map[string]any{
			"group_id":      groupID,
			"index_status":  NormalizePublicMootStatus(rec.Status),
			"updated_at_ms": rec.UpdatedAtMS,
		})
		return
	}
	entry, err := h.publicMootDirectoryEntry(r, rec)
	if err != nil {
		h.logger.Error("esphttp: public moot entry", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot status update failed")
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (h *Handler) publicMootBlocked(r *http.Request, desc publicmoot.Descriptor) (bool, error) {
	current, ok, err := h.state.GetPublicMoot(r.Context(), desc.GroupID)
	if err != nil {
		return false, err
	}
	if ok && NormalizePublicMootStatus(current.Status) == PublicMootStatusBlocked {
		return true, nil
	}
	blocked, err := h.state.ListPublicMoots(r.Context(), PublicMootStatusBlocked)
	if err != nil {
		return false, err
	}
	for _, rec := range blocked {
		if !PublicMootRecordHasDescriptor(rec) {
			continue
		}
		if bytes.Equal(rec.Descriptor.Founder.EntmootPubKey, desc.Founder.EntmootPubKey) {
			return true, nil
		}
	}
	return false, nil
}

func (h *Handler) publicMootDirectoryEntry(r *http.Request, rec PublicMootRecord) (PublicMootDirectoryEntry, error) {
	if !PublicMootRecordHasDescriptor(rec) {
		return PublicMootDirectoryEntry{}, fmt.Errorf("public moot descriptor is missing")
	}
	desc := rec.Descriptor
	mirrorState := PublicMootMirrorNone
	messageHistory := false
	if h.groups != nil {
		if ok, err := h.groupExists(r.Context(), desc.GroupID); err != nil {
			return PublicMootDirectoryEntry{}, err
		} else if ok {
			mirrorState = PublicMootMirrorMember
			messageHistory = true
		}
	}
	return PublicMootDirectoryEntry{
		Type:                    desc.Type,
		GroupID:                 desc.GroupID,
		Name:                    desc.Name,
		Description:             desc.Description,
		Tags:                    append([]string(nil), desc.Tags...),
		Visibility:              desc.Visibility,
		JoinMode:                desc.JoinMode,
		OpenInvite:              cloneOpenInviteDescriptor(desc.OpenInvite),
		Policy:                  desc.Policy,
		PolicySummary:           policy.Summary(desc.Policy),
		Founder:                 entmoot.NodeInfo{PilotNodeID: desc.Founder.PilotNodeID, EntmootPubKey: append([]byte(nil), desc.Founder.EntmootPubKey...)},
		Indexing:                desc.Indexing,
		UpdatedAtMS:             desc.UpdatedAtMS,
		ExpiresAtMS:             desc.ExpiresAtMS,
		Signature:               append([]byte(nil), desc.Signature...),
		IndexStatus:             NormalizePublicMootStatus(rec.Status),
		MirrorState:             mirrorState,
		MessageHistoryAvailable: messageHistory,
	}, nil
}

func cloneOpenInviteDescriptor(in *publicmoot.OpenInviteDescriptor) *publicmoot.OpenInviteDescriptor {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func publicMootDescriptorExpired(desc publicmoot.Descriptor, nowMS int64) bool {
	return desc.ExpiresAtMS > 0 && desc.ExpiresAtMS <= nowMS
}
