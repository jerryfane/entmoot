package libp2ptransport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

const (
	maxSyncRequestBytes  = 8 << 10
	maxRosterResponse    = 512 << 10
	maxHistoryListBytes  = 128 << 10
	maxHistoryBodyBytes  = 384 << 10
	maxSyncPageItems     = 1024
	maxHistoryBodyItems  = 64
	syncSnapshotLifetime = 30 * time.Second
	maxPeerSnapshots     = 4
	maxGlobalSnapshots   = 32
)

type SyncErrorCode string

const (
	SyncMalformed           SyncErrorCode = "malformed"
	SyncUnauthorized        SyncErrorCode = "unauthorized"
	SyncBootstrapDenied     SyncErrorCode = "bootstrap_denied"
	SyncSnapshotExpired     SyncErrorCode = "snapshot_expired"
	SyncCoverageUnavailable SyncErrorCode = "coverage_unavailable"
	SyncResourceExhausted   SyncErrorCode = "resource_exhausted"
	SyncInternal            SyncErrorCode = "internal"
)

type RosterSyncRequest struct {
	Version       uint8                `json:"version"`
	RequestID     string               `json:"request_id"`
	GroupID       entmoot.GroupID      `json:"group_id"`
	Capability    *BootstrapCapability `json:"capability,omitempty"`
	SnapshotToken string               `json:"snapshot_token,omitempty"`
	AfterSequence uint64               `json:"after_sequence,omitempty"`
	Limit         int                  `json:"limit,omitempty"`
}

type RosterSyncResponse struct {
	Version       uint8                 `json:"version"`
	RequestID     string                `json:"request_id"`
	GroupID       entmoot.GroupID       `json:"group_id"`
	Entries       []entmoot.RosterEntry `json:"entries,omitempty"`
	NextSequence  uint64                `json:"next_sequence,omitempty"`
	SnapshotToken string                `json:"snapshot_token,omitempty"`
	Complete      bool                  `json:"complete"`
	CommittedHead entmoot.RosterEntryID `json:"committed_head"`
	Error         SyncErrorCode         `json:"error,omitempty"`
}

type HistorySyncRequest struct {
	Version             uint8                `json:"version"`
	RequestID           string               `json:"request_id"`
	GroupID             entmoot.GroupID      `json:"group_id"`
	Capability          *BootstrapCapability `json:"capability,omitempty"`
	Mode                string               `json:"mode"`
	SnapshotToken       string               `json:"snapshot_token,omitempty"`
	Generation          uint64               `json:"generation,omitempty"`
	CoverageFloorMS     int64                `json:"coverage_floor_ms,omitempty"`
	CoverageCeilMS      int64                `json:"coverage_ceiling_ms,omitempty"`
	AfterTimestampMS    int64                `json:"after_timestamp_ms,omitempty"`
	AfterAuthorMemberID entmoot.MemberID     `json:"after_author_member_id,omitempty"`
	AfterID             *entmoot.MessageID   `json:"after_message_id,omitempty"`
	IDs                 []entmoot.MessageID  `json:"ids,omitempty"`
	Limit               int                  `json:"limit,omitempty"`
}
type LegacyHistoryProof struct {
	MessageID entmoot.MessageID `json:"message_id"`
	Proof     merkle.Proof      `json:"proof"`
}

type HistorySyncResponse struct {
	Version            uint8                `json:"version"`
	RequestID          string               `json:"request_id"`
	GroupID            entmoot.GroupID      `json:"group_id"`
	IDs                []entmoot.MessageID  `json:"ids,omitempty"`
	Messages           []entmoot.Message    `json:"messages,omitempty"`
	Missing            []entmoot.MessageID  `json:"missing,omitempty"`
	LegacyProofs       []LegacyHistoryProof `json:"legacy_proofs,omitempty"`
	SnapshotToken      string               `json:"snapshot_token,omitempty"`
	Generation         uint64               `json:"generation,omitempty"`
	NextTimestampMS    int64                `json:"next_timestamp_ms,omitempty"`
	NextAuthorMemberID entmoot.MemberID     `json:"next_author_member_id,omitempty"`
	NextID             *entmoot.MessageID   `json:"next_message_id,omitempty"`
	HasMore            bool                 `json:"has_more,omitempty"`
	SnapshotChanged    bool                 `json:"snapshot_changed,omitempty"`
	CoverageFloorMS    int64                `json:"coverage_floor_ms,omitempty"`
	CoverageCeilMS     int64                `json:"coverage_ceiling_ms,omitempty"`
	Error              SyncErrorCode        `json:"error,omitempty"`
}

type syncSnapshot struct {
	peerID     peer.ID
	groupID    entmoot.GroupID
	kind       protocolKind
	expires    time.Time
	rosterSize int
	generation uint64
}

type protocolKind uint8

const (
	rosterSnapshot protocolKind = iota + 1
	historySnapshot
)

// SyncServer serves bounded authenticated roster and history pages.
type SyncServer struct {
	Host          host.Host
	Admission     *BootstrapAdmission
	Roster        func(entmoot.GroupID) (*roster.RosterLog, bool)
	Store         store.MessageStore
	LegacyHistory func(entmoot.GroupID) (*merkle.Tree, bool)
	Now           func() time.Time

	snapshotMu sync.Mutex
	snapshots  map[string]syncSnapshot
}

func (s *SyncServer) Install() error {
	if s == nil || s.Host == nil || s.Roster == nil || s.Store == nil || s.Admission == nil {
		return errors.New("libp2p: complete sync server is required")
	}
	s.snapshots = make(map[string]syncSnapshot)
	s.Host.SetStreamHandler(RosterProtocol, s.handleRoster)
	s.Host.SetStreamHandler(HistoryProtocol, s.handleHistory)
	return nil
}

func (s *SyncServer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *SyncServer) authorize(stream network.Stream, groupID entmoot.GroupID, capability *BootstrapCapability, requested protocol.ID) error {
	r, ok := s.Roster(groupID)
	if !ok {
		return errors.New("unknown group")
	}
	remote := stream.Conn().RemotePeer()
	publicKey, err := remote.ExtractPublicKey()
	if err != nil || publicKey == nil {
		publicKey = s.Host.Peerstore().PubKey(remote)
	}
	if publicKey != nil {
		raw, marshalErr := publicKey.Raw()
		if marshalErr == nil {
			binding, bindingErr := BindingFromPublicKey(raw)
			if bindingErr == nil && binding.PeerID == remote && r.IsMemberID(binding.MemberID) {
				return nil
			}
		}
	}
	if capability == nil {
		return ErrBootstrapDenied
	}
	if capability.GroupID != groupID || capability.RosterHead != r.Head() {
		return ErrBootstrapDenied
	}
	founder, ok := r.Founder()
	if !ok || !equalMemberID(founder.MemberID, capability.Founder.MemberID) ||
		!bytes.Equal(founder.EntmootPubKey, capability.Founder.EntmootPubKey) {
		return ErrBootstrapDenied
	}
	allowedServer := false
	for _, allowed := range capability.AllowedPeerIDs {
		if allowed == s.Host.ID().String() {
			allowedServer = true
			break
		}
	}
	if !allowedServer {
		return ErrBootstrapDenied
	}
	return s.Admission.Verify(*capability, remote, requested, s.now())
}

func (s *SyncServer) handleRoster(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var request RosterSyncRequest
	if err := decodeJSONLimit(stream, maxSyncRequestBytes, &request); err != nil {
		s.writeRoster(stream, RosterSyncResponse{Version: 2, Error: SyncMalformed})
		return
	}
	response := RosterSyncResponse{Version: 2, RequestID: request.RequestID, GroupID: request.GroupID}
	if request.Version != 2 || request.RequestID == "" {
		response.Error = SyncMalformed
		s.writeRoster(stream, response)
		return
	}
	if err := s.authorize(stream, request.GroupID, request.Capability, RosterProtocol); err != nil {
		response.Error = SyncUnauthorized
		s.writeRoster(stream, response)
		return
	}
	r, ok := s.Roster(request.GroupID)
	if !ok {
		response.Error = SyncUnauthorized
		s.writeRoster(stream, response)
		return
	}
	entries := r.Entries()
	snapshot, token, err := s.rosterSnapshot(stream.Conn().RemotePeer(), request, len(entries))
	if err != nil {
		response.Error = SyncSnapshotExpired
		s.writeRoster(stream, response)
		return
	}
	if snapshot.rosterSize < len(entries) {
		entries = entries[:snapshot.rosterSize]
	}
	limit := boundedLimit(request.Limit, 256, maxSyncPageItems)
	start := int(request.AfterSequence)
	if start > len(entries) {
		response.Error = SyncMalformed
		s.writeRoster(stream, response)
		return
	}
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	response.Entries = entries[start:end]
	response.NextSequence = uint64(end)
	response.Complete = end == len(entries)
	response.SnapshotToken = token
	response.CommittedHead = entries[len(entries)-1].ID
	s.writeRoster(stream, response)
}

func (s *SyncServer) handleHistory(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(30 * time.Second))
	var request HistorySyncRequest
	if err := decodeJSONLimit(stream, maxSyncRequestBytes, &request); err != nil {
		s.writeHistory(stream, HistorySyncResponse{Version: 2, Error: SyncMalformed}, maxHistoryListBytes)
		return
	}
	response := HistorySyncResponse{Version: 2, RequestID: request.RequestID, GroupID: request.GroupID, CoverageCeilMS: request.CoverageCeilMS}
	if request.Version != 2 || request.RequestID == "" {
		response.Error = SyncMalformed
		s.writeHistory(stream, response, maxHistoryListBytes)
		return
	}
	if err := s.authorize(stream, request.GroupID, request.Capability, HistoryProtocol); err != nil {
		response.Error = SyncUnauthorized
		s.writeHistory(stream, response, maxHistoryListBytes)
		return
	}
	switch request.Mode {
	case "list":
		s.handleHistoryList(stream, request, response)
	case "bodies":
		s.handleHistoryBodies(stream, request, response)
	default:
		response.Error = SyncMalformed
		s.writeHistory(stream, response, maxHistoryListBytes)
	}
}

func (s *SyncServer) handleHistoryList(stream network.Stream, request HistorySyncRequest, response HistorySyncResponse) {
	var cursor *store.RangeCursor
	if request.AfterID != nil {
		cursor = &store.RangeCursor{TimestampMS: request.AfterTimestampMS, AuthorMemberID: request.AfterAuthorMemberID, ID: *request.AfterID}
	}
	limit := boundedLimit(request.Limit, 256, maxSyncPageItems)
	var page store.MessageIDPage
	var err error
	if request.CoverageCeilMS != 0 {
		if request.CoverageCeilMS <= request.CoverageFloorMS {
			response.Error = SyncMalformed
			s.writeHistory(stream, response, maxHistoryListBytes)
			return
		}
		windowed, ok := s.Store.(store.WindowedPagedMessageIDStore)
		if !ok {
			response.Error = SyncCoverageUnavailable
			s.writeHistory(stream, response, maxHistoryListBytes)
			return
		}
		page, err = windowed.MessageIDsPageWindow(context.Background(), request.GroupID, request.CoverageFloorMS, request.CoverageCeilMS, cursor, request.Generation, limit)
	} else {
		paged, ok := s.Store.(store.PagedMessageIDStore)
		if !ok {
			response.Error = SyncInternal
			s.writeHistory(stream, response, maxHistoryListBytes)
			return
		}
		page, err = paged.MessageIDsPage(context.Background(), request.GroupID, request.CoverageFloorMS, cursor, request.Generation, limit)
	}
	if err != nil {
		response.Error = SyncInternal
		s.writeHistory(stream, response, maxHistoryListBytes)
		return
	}
	_, token, snapshotErr := s.historySnapshot(stream.Conn().RemotePeer(), request, page.Generation)
	if snapshotErr != nil {
		response.Error = SyncSnapshotExpired
		s.writeHistory(stream, response, maxHistoryListBytes)
		return
	}
	response.IDs = page.IDs
	response.Generation = page.Generation
	response.SnapshotToken = token
	response.HasMore = page.HasMore
	response.SnapshotChanged = page.SnapshotChanged
	response.CoverageFloorMS = page.CoverageFloorMS
	if page.Next != nil {
		response.NextTimestampMS = page.Next.TimestampMS
		response.NextAuthorMemberID = page.Next.AuthorMemberID
		next := page.Next.ID
		response.NextID = &next
	}
	if page.SnapshotChanged {
		response.Error = SyncSnapshotExpired
	}
	s.writeHistory(stream, response, maxHistoryListBytes)
}

func (s *SyncServer) handleHistoryBodies(stream network.Stream, request HistorySyncRequest, response HistorySyncResponse) {
	if len(request.IDs) == 0 || len(request.IDs) > maxHistoryBodyItems {
		response.Error = SyncResourceExhausted
		s.writeHistory(stream, response, maxHistoryBodyBytes)
		return
	}
	for _, id := range request.IDs {
		message, err := s.Store.Get(context.Background(), request.GroupID, id)
		if errors.Is(err, store.ErrNotFound) {
			response.Missing = append(response.Missing, id)
			continue
		}
		if err != nil {
			response.Error = SyncInternal
			s.writeHistory(stream, response, maxHistoryBodyBytes)
			return
		}
		if message.Version == 0 {
			if s.LegacyHistory == nil {
				response.Error = SyncInternal
				s.writeHistory(stream, response, maxHistoryBodyBytes)
				return
			}
			tree, ok := s.LegacyHistory(request.GroupID)
			if !ok || tree == nil {
				response.Error = SyncInternal
				s.writeHistory(stream, response, maxHistoryBodyBytes)
				return
			}
			proof, proofErr := tree.Proof(message.ID)
			if proofErr != nil {
				response.Error = SyncInternal
				s.writeHistory(stream, response, maxHistoryBodyBytes)
				return
			}
			response.LegacyProofs = append(response.LegacyProofs, LegacyHistoryProof{MessageID: message.ID, Proof: proof})
		}
		response.Messages = append(response.Messages, message)
	}
	s.writeHistory(stream, response, maxHistoryBodyBytes)
}

func boundedLimit(value, fallback, maximum int) int {
	if value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

func (s *SyncServer) rosterSnapshot(peerID peer.ID, request RosterSyncRequest, size int) (syncSnapshot, string, error) {
	return s.snapshot(peerID, request.GroupID, rosterSnapshot, request.SnapshotToken, size, 0)
}

func (s *SyncServer) historySnapshot(peerID peer.ID, request HistorySyncRequest, generation uint64) (syncSnapshot, string, error) {
	return s.snapshot(peerID, request.GroupID, historySnapshot, request.SnapshotToken, 0, generation)
}

func (s *SyncServer) snapshot(peerID peer.ID, groupID entmoot.GroupID, kind protocolKind, token string, rosterSize int, generation uint64) (syncSnapshot, string, error) {
	now := s.now()
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	for key, value := range s.snapshots {
		if !value.expires.After(now) {
			delete(s.snapshots, key)
		}
	}
	if token != "" {
		value, ok := s.snapshots[token]
		if !ok || value.peerID != peerID || value.groupID != groupID || value.kind != kind || !value.expires.After(now) {
			return syncSnapshot{}, "", ErrBootstrapDenied
		}
		if kind == historySnapshot && value.generation != generation {
			return syncSnapshot{}, "", ErrBootstrapDenied
		}
		return value, token, nil
	}
	peerCount := 0
	for _, value := range s.snapshots {
		if value.peerID == peerID {
			peerCount++
		}
	}
	if peerCount >= maxPeerSnapshots || len(s.snapshots) >= maxGlobalSnapshots {
		return syncSnapshot{}, "", errors.New("snapshot capacity exhausted")
	}
	var random [24]byte
	if _, err := rand.Read(random[:]); err != nil {
		return syncSnapshot{}, "", err
	}
	token = base64.RawURLEncoding.EncodeToString(random[:])
	value := syncSnapshot{peerID: peerID, groupID: groupID, kind: kind, expires: now.Add(syncSnapshotLifetime), rosterSize: rosterSize, generation: generation}
	s.snapshots[token] = value
	return value, token, nil
}

func (s *SyncServer) writeRoster(stream network.Stream, response RosterSyncResponse) {
	if err := encodeJSONLimit(stream, response, maxRosterResponse); err != nil {
		_ = stream.Reset()
	}
}

func (s *SyncServer) writeHistory(stream network.Stream, response HistorySyncResponse, limit int) {
	if err := encodeJSONLimit(stream, response, limit); err != nil {
		fallback := HistorySyncResponse{Version: 2, RequestID: response.RequestID, GroupID: response.GroupID, Error: SyncResourceExhausted}
		if fallbackErr := encodeJSONLimit(stream, fallback, maxHistoryListBytes); fallbackErr != nil {
			_ = stream.Reset()
		}
	}
}

func decodeJSONLimit(reader io.Reader, limit int64, value any) error {
	limited := &io.LimitedReader{R: reader, N: limit + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	if limited.N <= 0 {
		return errors.New("frame exceeds limit")
	}
	return nil
}

func encodeJSONLimit(writer io.Writer, value any, limit int) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > limit {
		return fmt.Errorf("encoded response is %d bytes, limit %d", len(payload), limit)
	}
	payload = append(payload, '\n')
	_, err = writer.Write(payload)
	return err
}
