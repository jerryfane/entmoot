package libp2ptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
)

const maxEnrollmentFrameBytes = 64 << 10

// Enrollment rejection codes are safe, typed protocol text. Detail carries the
// issuer's own explanation so a joiner can tell a stale invite from a revoked
// one instead of seeing one opaque failure.
const (
	EnrollRejectMalformed          = "malformed"
	EnrollRejectUnauthorizedServer = "unauthorized_server"
	EnrollRejectCapability         = "capability_denied"
	EnrollRejectApplicant          = "applicant_mismatch"
	EnrollRejectUnknownGroup       = "unknown_group"
	EnrollRejectNotIssuer          = "not_issuer"
	EnrollRejectIssuerMismatch     = "issuer_mismatch"
	EnrollRejectUnknownCheckpoint  = "unknown_checkpoint"
	EnrollRejectIdentityConflict   = "identity_conflict"
	EnrollRejectCommitFailed       = "admission_commit_failed"
	EnrollRejectInternal           = "internal"
)

// EnrollmentRequest carries the invite plus the applicant's own identity key.
// An open invite names no target, so the applicant states who it is and the
// server binds that key to the authenticated stream peer.
type EnrollmentRequest struct {
	Version            uint8               `json:"version"`
	Capability         BootstrapCapability `json:"capability"`
	ApplicantPublicKey []byte              `json:"applicant_public_key"`
}

// EnrollmentResponse acknowledges that the applicant identity is installed in
// the group roster at RosterHead. Error is a typed code; Detail is a
// human-readable reason for a rejection.
type EnrollmentResponse struct {
	RosterHead entmoot.RosterEntryID `json:"roster_head"`
	Entries    []entmoot.RosterEntry `json:"entries,omitempty"`
	Error      string                `json:"error,omitempty"`
	Detail     string                `json:"detail,omitempty"`
}

// EnrollmentRejection lets the roster-owning callback state exactly why it
// refused, so the server reports a real reason rather than one generic code.
type EnrollmentRejection struct {
	Code   string
	Detail string
}

func (e *EnrollmentRejection) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// RejectEnrollment builds a typed enrollment rejection.
func RejectEnrollment(code, format string, args ...any) error {
	return &EnrollmentRejection{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// EnrollmentServer validates and consumes a bootstrap capability before
// invoking the roster mutation owned by the issuing node.
type EnrollmentServer struct {
	Admission *BootstrapAdmission
	Enroll    func(ctx context.Context, capability BootstrapCapability, applicant entmoot.NodeInfo) (EnrollmentResponse, error)
	Now       func() time.Time
	host      host.Host
}

func (s *EnrollmentServer) Install(h host.Host) error {
	if h == nil || s == nil || s.Admission == nil || s.Enroll == nil {
		return errors.New("libp2p: complete enrollment server is required")
	}
	s.host = h
	h.SetStreamHandler(EnrollmentProtocol, s.handle)
	return nil
}

func (s *EnrollmentServer) handle(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	reject := func(code, format string, args ...any) {
		_ = json.NewEncoder(stream).Encode(EnrollmentResponse{Error: code, Detail: fmt.Sprintf(format, args...)})
	}
	var request EnrollmentRequest
	if err := decodeBoundedJSON(stream, &request); err != nil {
		reject(EnrollRejectMalformed, "decode enrollment request: %v", err)
		return
	}
	if request.Version != 1 {
		reject(EnrollRejectMalformed, "unsupported enrollment request version %d", request.Version)
		return
	}
	capability := request.Capability
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	applicant, err := applicantIdentity(request, stream.Conn().RemotePeer())
	if err != nil {
		reject(EnrollRejectApplicant, "%v", err)
		return
	}
	allowedServer := false
	for _, allowed := range capability.AllowedPeerIDs {
		if allowed == s.host.ID().String() {
			allowedServer = true
			break
		}
	}
	if !allowedServer {
		reject(EnrollRejectUnauthorizedServer, "this node is not an enrollment server for the invite")
		return
	}
	remote := stream.Conn().RemotePeer()
	if err := s.Admission.Reserve(capability, remote, EnrollmentProtocol, now); err != nil {
		if errors.Is(err, ErrBootstrapUnavailable) {
			// A store failure says nothing about the invite, and its driver
			// text is not the joiner's business.
			reject(EnrollRejectInternal, "admission state is unavailable; retry")
			return
		}
		reject(EnrollRejectCapability, "%v", err)
		return
	}
	response, err := s.Enroll(context.Background(), capability, applicant)
	if err != nil {
		// Releasing returns the use to the invite, so a failure the applicant
		// can fix does not burn its remaining uses.
		_ = s.Admission.Release(capability, remote)
		var rejection *EnrollmentRejection
		if errors.As(err, &rejection) {
			reject(rejection.Code, "%s", rejection.Detail)
			return
		}
		reject(EnrollRejectInternal, "enrollment failed")
		return
	}
	if err := s.Admission.Commit(capability, remote); err != nil {
		_ = s.Admission.Release(capability, remote)
		reject(EnrollRejectCommitFailed, "%v", err)
		return
	}
	_ = json.NewEncoder(stream).Encode(response)
}

// applicantIdentity binds the applicant's stated key to the authenticated
// stream peer, and to the invite's target when the invite names one.
func applicantIdentity(request EnrollmentRequest, remote peer.ID) (entmoot.NodeInfo, error) {
	binding, err := BindingFromPublicKey(request.ApplicantPublicKey)
	if err != nil {
		return entmoot.NodeInfo{}, fmt.Errorf("derive applicant identity: %w", err)
	}
	if binding.PeerID != remote {
		return entmoot.NodeInfo{}, errors.New("applicant key does not match the connected peer")
	}
	capability := request.Capability
	if !capability.IsOpenInvite() && (binding.MemberID != capability.TargetMemberID || binding.PeerID.String() != capability.TargetPeerID) {
		return entmoot.NodeInfo{}, errors.New("applicant is not the invite target")
	}
	memberID := binding.MemberID
	return entmoot.NodeInfo{
		EntmootPubKey: append([]byte(nil), request.ApplicantPublicKey...),
		MemberID:      &memberID,
		PeerID:        binding.PeerID.String(),
	}, nil
}

// Enroll opens the pre-membership protocol. applicantPublicKey is the joining
// identity; the server binds it to this stream's authenticated PeerID.
func Enroll(ctx context.Context, h host.Host, remote peer.AddrInfo, capability BootstrapCapability, applicantPublicKey []byte) (EnrollmentResponse, error) {
	if h == nil || remote.ID == "" {
		return EnrollmentResponse{}, errors.New("libp2p: local host and remote peer are required")
	}
	if err := h.Connect(ctx, remote); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("libp2p: connect enrollment peer: %w", err)
	}
	stream, err := h.NewStream(network.WithAllowLimitedConn(ctx, "Entmoot enrollment"), remote.ID, EnrollmentProtocol)
	if err != nil {
		return EnrollmentResponse{}, fmt.Errorf("libp2p: open enrollment stream: %w", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	request := EnrollmentRequest{Version: 1, Capability: capability, ApplicantPublicKey: applicantPublicKey}
	if err := json.NewEncoder(stream).Encode(request); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("libp2p: write enrollment: %w", err)
	}
	if err := stream.CloseWrite(); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("libp2p: finish enrollment request: %w", err)
	}
	var response EnrollmentResponse
	if err := decodeBoundedJSON(stream, &response); err != nil {
		return EnrollmentResponse{}, fmt.Errorf("libp2p: read enrollment: %w", err)
	}
	if response.Error != "" {
		if response.Detail != "" {
			return response, fmt.Errorf("libp2p: enrollment rejected: %s: %s", response.Error, response.Detail)
		}
		return response, fmt.Errorf("libp2p: enrollment rejected: %s", response.Error)
	}
	return response, nil
}

func decodeBoundedJSON(reader io.Reader, value any) error {
	limited := &io.LimitedReader{R: reader, N: maxEnrollmentFrameBytes + 1}
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
		return errors.New("enrollment frame exceeds limit")
	}
	return nil
}
