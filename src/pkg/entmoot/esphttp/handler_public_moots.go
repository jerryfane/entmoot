package esphttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/publicmoot"
)

func (h *Handler) handlePublicMootRoute(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/v1/public-moots" {
		h.handlePublicMoots(w, r)
		return true
	}
	const prefix = "/v1/public-moots/"
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
	switch suffix {
	case "":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return true
		}
		h.handleGetPublicMoot(w, r, groupID)
	case "index-status":
		if r.Method != http.MethodPatch {
			methodNotAllowed(w, http.MethodPatch)
			return true
		}
		h.handlePatchPublicMootIndexStatus(w, r, groupID)
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
	return true
}

func (h *Handler) handlePublicMoots(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleListPublicMoots(w, r)
	case http.MethodPost:
		h.handlePostPublicMoot(w, r)
	default:
		methodNotAllowed(w, http.MethodGet+", "+http.MethodPost)
	}
}

func (h *Handler) handleListPublicMoots(w http.ResponseWriter, r *http.Request) {
	records, err := h.state.ListPublicMoots(r.Context(), PublicMootListFilter{Statuses: []string{PublicMootStatusListed}})
	if err != nil {
		h.logger.Error("esphttp: list public moots", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot listing failed")
		return
	}
	entries := make([]PublicMootDirectoryEntry, 0, len(records))
	nowMS := h.clock().UnixMilli()
	for _, rec := range records {
		if publicMootDescriptorExpired(rec.Descriptor, nowMS) {
			continue
		}
		entry, err := h.publicMootEntry(r.Context(), rec)
		if err != nil {
			h.logger.Error("esphttp: public moot mirror state", slog.String("err", err.Error()))
			writeError(w, http.StatusInternalServerError, "internal_error", "public moot mirror lookup failed")
			return
		}
		entries = append(entries, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"public_moots": entries})
}

func (h *Handler) handleGetPublicMoot(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	rec, ok, err := h.state.GetPublicMoot(r.Context(), groupID)
	if err != nil {
		h.logger.Error("esphttp: get public moot", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot lookup failed")
		return
	}
	if !ok || normalizePublicMootStatus(rec.Status) != PublicMootStatusListed || publicMootDescriptorExpired(rec.Descriptor, h.clock().UnixMilli()) {
		writeError(w, http.StatusNotFound, "public_moot_not_found", "public moot not found")
		return
	}
	entry, err := h.publicMootEntry(r.Context(), rec)
	if err != nil {
		h.logger.Error("esphttp: public moot mirror state", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot mirror lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"public_moot": entry})
}

func (h *Handler) handlePostPublicMoot(w http.ResponseWriter, r *http.Request) {
	raw, err := readPublicMootRequestBody(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	desc, err := publicmoot.Parse(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_public_moot", err.Error())
		return
	}
	if err := publicmoot.Verify(desc); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_public_moot", err.Error())
		return
	}
	if publicMootDescriptorExpired(desc, h.clock().UnixMilli()) {
		writeError(w, http.StatusBadRequest, "invalid_public_moot", "public moot descriptor is expired")
		return
	}
	rec, changed, err := h.state.UpsertPublicMoot(r.Context(), PublicMootRecord{Descriptor: desc}, h.clock().UnixMilli())
	if errors.Is(err, ErrPublicMootBlocked) {
		writeError(w, http.StatusForbidden, "public_moot_blocked", "public moot or founder is blocked")
		return
	}
	if errors.Is(err, ErrPublicMootFounderMismatch) {
		writeError(w, http.StatusForbidden, "public_moot_founder_mismatch", "public moot founder does not match existing record")
		return
	}
	if err != nil {
		h.logger.Error("esphttp: upsert public moot", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot indexing failed")
		return
	}
	status := "indexed"
	code := http.StatusAccepted
	if !changed {
		status = "stale_ignored"
		code = http.StatusOK
	}
	entry, err := h.publicMootEntry(r.Context(), rec)
	if err != nil {
		h.logger.Error("esphttp: public moot mirror state", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot mirror lookup failed")
		return
	}
	writeJSON(w, code, map[string]any{
		"status":      status,
		"public_moot": entry,
	})
}

func (h *Handler) handlePatchPublicMootIndexStatus(w http.ResponseWriter, r *http.Request, groupID entmoot.GroupID) {
	auth, ok := h.authorize(w, r)
	if !ok {
		return
	}
	if !auth.bearer {
		writeError(w, http.StatusForbidden, "operator_required", "public moot index status requires operator bearer auth")
		return
	}
	var req struct {
		Status string `json:"status"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	status := normalizePublicMootStatus(req.Status)
	if !validPublicMootStatus(status) {
		writeError(w, http.StatusBadRequest, "bad_request", "status must be listed, pending, delisted, or blocked")
		return
	}
	rec, ok, err := h.state.UpdatePublicMootIndexStatus(r.Context(), groupID, status, h.clock().UnixMilli())
	if err != nil {
		h.logger.Error("esphttp: update public moot index status", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot status update failed")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "public_moot_not_found", "public moot not found")
		return
	}
	entry, err := h.publicMootEntry(r.Context(), rec)
	if err != nil {
		h.logger.Error("esphttp: public moot mirror state", slog.String("err", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal_error", "public moot mirror lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"public_moot": entry})
}

func readPublicMootRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("invalid JSON body: %v", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, errors.New("descriptor is required")
	}
	return append([]byte(nil), raw...), nil
}

func publicMootDescriptorExpired(desc publicmoot.Descriptor, nowMS int64) bool {
	return desc.ExpiresAtMS > 0 && desc.ExpiresAtMS <= nowMS
}

func (h *Handler) publicMootEntry(ctx context.Context, rec PublicMootRecord) (PublicMootDirectoryEntry, error) {
	entry := PublicMootEntryFromRecord(rec)
	if rec.Descriptor.GroupID == (entmoot.GroupID{}) || !h.groupExistsConfigured {
		return entry, nil
	}
	exists, err := h.groupExists(ctx, rec.Descriptor.GroupID)
	if err != nil {
		return entry, err
	}
	if exists {
		entry.MirrorState = PublicMootMirrorMember
		entry.MessageHistoryAvailable = true
	}
	return entry, nil
}
