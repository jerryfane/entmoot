package libp2ptransport

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
)

const AcceptanceProtocol protocol.ID = "/entmoot/acceptance/2"

type AcceptanceRequest struct {
	Version uint8           `json:"version"`
	Message entmoot.Message `json:"message"`
}

type AcceptanceResponse struct {
	Version    uint8                      `json:"version"`
	Acceptance *entmoot.MessageAcceptance `json:"acceptance,omitempty"`
	Error      string                     `json:"error,omitempty"`
}

// AcceptanceServer is the founder authority for current member-authored
// messages. It signs only a message already bound to the current roster head.
type AcceptanceServer struct {
	Host     host.Host
	Identity *keystore.Identity
	Roster   func(entmoot.GroupID) (*roster.RosterLog, bool)
	Now      func() time.Time
}

func (s *AcceptanceServer) Install() error {
	if s == nil || s.Host == nil || s.Identity == nil || s.Roster == nil {
		return errors.New("libp2p: complete acceptance server is required")
	}
	s.Host.SetStreamHandler(AcceptanceProtocol, s.handle)
	return nil
}

func (s *AcceptanceServer) handle(stream network.Stream) {
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(10 * time.Second))
	var request AcceptanceRequest
	if err := decodeJSONLimit(stream, maxHistoryBodyBytes, &request); err != nil || request.Version != 2 {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "malformed"}, maxSyncRequestBytes)
		return
	}
	groupRoster, ok := s.Roster(request.Message.GroupID)
	if !ok {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "unknown_group"}, maxSyncRequestBytes)
		return
	}
	founder, ok := groupRoster.Founder()
	if !ok || founder.MemberID == nil {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "founder_unavailable"}, maxSyncRequestBytes)
		return
	}
	local, err := BindingFromPublicKey(s.Identity.PublicKey)
	if err != nil || local.MemberID != *founder.MemberID || !equalBytes(founder.EntmootPubKey, s.Identity.PublicKey) {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "not_founder"}, maxSyncRequestBytes)
		return
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	if err := VerifyLiveAuthor(groupRoster, request.Message, now); err != nil {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "unauthorized"}, maxSyncRequestBytes)
		return
	}
	if remote := stream.Conn().RemotePeer(); request.Message.Author.PeerID != remote.String() {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "peer_mismatch"}, maxSyncRequestBytes)
		return
	}
	acceptance, err := SignMessageAcceptance(s.Identity, founder, request.Message)
	if err != nil {
		_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Error: "internal"}, maxSyncRequestBytes)
		return
	}
	_ = encodeJSONLimit(stream, AcceptanceResponse{Version: 2, Acceptance: &acceptance}, maxSyncRequestBytes)
}

func SignMessageAcceptance(founderIdentity *keystore.Identity, founder entmoot.NodeInfo, message entmoot.Message) (entmoot.MessageAcceptance, error) {
	if founderIdentity == nil || message.RosterHead == nil {
		return entmoot.MessageAcceptance{}, errors.New("libp2p: founder identity and roster head are required")
	}
	acceptance := entmoot.MessageAcceptance{
		Version:    1,
		GroupID:    message.GroupID,
		MessageID:  message.ID,
		RosterHead: *message.RosterHead,
		Authority:  founder,
	}
	bytes, err := canonical.MessageAcceptanceSigningBytes(acceptance)
	if err != nil {
		return entmoot.MessageAcceptance{}, err
	}
	acceptance.Signature = founderIdentity.Sign(bytes)
	return acceptance, nil
}

func RequestMessageAcceptance(ctx context.Context, h host.Host, remote peer.AddrInfo, message entmoot.Message) (entmoot.MessageAcceptance, error) {
	request := AcceptanceRequest{Version: 2, Message: message}
	var response AcceptanceResponse
	if err := requestResponse(ctx, h, remote, AcceptanceProtocol, request, maxHistoryBodyBytes, &response, maxSyncRequestBytes); err != nil {
		return entmoot.MessageAcceptance{}, fmt.Errorf("libp2p: request acceptance: %w", err)
	}
	if response.Version != 2 || response.Error != "" || response.Acceptance == nil {
		return entmoot.MessageAcceptance{}, fmt.Errorf("libp2p: acceptance rejected: %s", response.Error)
	}
	return *response.Acceptance, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
