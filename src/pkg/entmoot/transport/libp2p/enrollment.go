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
)

const maxEnrollmentFrameBytes = 64 << 10

// EnrollmentResponse acknowledges that the target identity is installed in
// the group roster at RosterHead. Error is safe, typed protocol text.
type EnrollmentResponse struct {
	RosterHead string `json:"roster_head,omitempty"`
	Error      string `json:"error,omitempty"`
}

// EnrollmentServer validates and consumes a bootstrap capability before
// invoking the founder-owned roster mutation.
type EnrollmentServer struct {
	Admission *BootstrapAdmission
	Enroll    func(context.Context, BootstrapCapability) (string, error)
	Now       func() time.Time
}

func (s *EnrollmentServer) Install(h host.Host) error {
	if h == nil || s == nil || s.Admission == nil || s.Enroll == nil {
		return errors.New("libp2p: complete enrollment server is required")
	}
	h.SetStreamHandler(EnrollmentProtocol, s.handle)
	return nil
}

func (s *EnrollmentServer) handle(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var capability BootstrapCapability
	if err := decodeBoundedJSON(stream, &capability); err != nil {
		_ = json.NewEncoder(stream).Encode(EnrollmentResponse{Error: "malformed"})
		return
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	if err := s.Admission.Authorize(capability, stream.Conn().RemotePeer(), EnrollmentProtocol, now); err != nil {
		_ = json.NewEncoder(stream).Encode(EnrollmentResponse{Error: "unauthorized"})
		return
	}
	head, err := s.Enroll(context.Background(), capability)
	if err != nil {
		_ = json.NewEncoder(stream).Encode(EnrollmentResponse{Error: "enrollment_failed"})
		return
	}
	_ = json.NewEncoder(stream).Encode(EnrollmentResponse{RosterHead: head})
}

// Enroll opens the pre-membership protocol. The server authenticates the
// stream's PeerID against the target key in capability.
func Enroll(ctx context.Context, h host.Host, remote peer.AddrInfo, capability BootstrapCapability) (EnrollmentResponse, error) {
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
	if err := json.NewEncoder(stream).Encode(capability); err != nil {
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
