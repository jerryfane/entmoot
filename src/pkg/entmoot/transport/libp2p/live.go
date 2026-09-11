package libp2ptransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/gossip"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

const (
	groupTopicDomain       = "entmoot/topic/v2\x00"
	pubsubEnvelopeDomain   = "entmoot/pubsub-envelope/v2\x00"
	maxPubSubEnvelopeBytes = 384 << 10
)

type DeliveryState string

const (
	DeliveryPublished      DeliveryState = "published"
	DeliveryPendingHistory DeliveryState = "pending_history"
	DeliveryAlreadyStored  DeliveryState = "already_stored"
)

type LiveConfig struct {
	Host     host.Host
	GroupID  entmoot.GroupID
	Roster   *roster.RosterLog
	Store    store.MessageStore
	OnIngest func(entmoot.Message)
	Now      func() time.Time
}

type validatedLiveMessage struct {
	message  entmoot.Message
	inserted bool
}

// LiveGroup owns one authorized GossipSub topic. It has no Plumtree fallback.
type LiveGroup struct {
	cfg          LiveConfig
	pubsub       *pubsub.PubSub
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	cancel       context.CancelFunc
	done         chan struct{}
	closeOnce    sync.Once
}

func GroupTopic(groupID entmoot.GroupID) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(groupTopicDomain))
	_, _ = hash.Write(groupID[:])
	return "/entmoot/group/2/" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func NewLiveGroup(ctx context.Context, cfg LiveConfig) (*LiveGroup, error) {
	if cfg.Host == nil || cfg.Roster == nil || cfg.Store == nil {
		return nil, errors.New("libp2p: host, roster and store are required for live delivery")
	}
	localBinding, err := hostBinding(cfg.Host)
	if err != nil || !cfg.Roster.IsMemberID(localBinding.MemberID) {
		return nil, fmt.Errorf("libp2p: local host is not a current group member")
	}
	topicName := GroupTopic(cfg.GroupID)
	params := pubsub.DefaultGossipSubParams()
	params.D = 4
	params.Dlo = 2
	params.Dhi = 6
	params.Dout = 1
	params.HeartbeatInterval = time.Second
	filter := &memberSubscriptionFilter{host: cfg.Host, roster: cfg.Roster, topic: topicName}
	ps, err := pubsub.NewGossipSub(ctx, cfg.Host,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
		pubsub.WithMessageIdFn(envelopeMessageID),
		pubsub.WithGossipSubParams(params),
		pubsub.WithValidateThrottle(64),
		pubsub.WithValidateQueueSize(128),
		pubsub.WithPeerOutboundQueueSize(128),
		pubsub.WithSubscriptionFilter(filter),
		pubsub.WithPeerFilter(func(remote peer.ID, topic string) bool {
			return topic == topicName && peerIsMember(cfg.Host, cfg.Roster, remote)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("libp2p: create GossipSub: %w", err)
	}
	liveCtx, cancel := context.WithCancel(ctx)
	group := &LiveGroup{cfg: cfg, pubsub: ps, cancel: cancel, done: make(chan struct{})}
	if err := ps.RegisterTopicValidator(topicName, group.validate,
		pubsub.WithValidatorConcurrency(8)); err != nil {
		cancel()
		return nil, fmt.Errorf("libp2p: register topic validator: %w", err)
	}
	topic, err := ps.Join(topicName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("libp2p: join group topic: %w", err)
	}
	subscription, err := topic.Subscribe()
	if err != nil {
		_ = topic.Close()
		cancel()
		return nil, fmt.Errorf("libp2p: subscribe group topic: %w", err)
	}
	group.topic = topic
	group.subscription = subscription
	go group.consume(liveCtx)
	return group, nil
}

func envelopeMessageID(message *pubsubpb.Message) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(pubsubEnvelopeDomain))
	_, _ = hash.Write(message.GetData())
	return string(hash.Sum(nil))
}

func (g *LiveGroup) now() time.Time {
	if g.cfg.Now != nil {
		return g.cfg.Now()
	}
	return time.Now()
}

func (g *LiveGroup) validate(_ context.Context, _ peer.ID, envelope *pubsub.Message) pubsub.ValidationResult {
	if envelope == nil || len(envelope.Data) == 0 || len(envelope.Data) > maxPubSubEnvelopeBytes {
		return pubsub.ValidationReject
	}
	local, err := hostBinding(g.cfg.Host)
	if err != nil || !g.cfg.Roster.IsMemberID(local.MemberID) {
		return pubsub.ValidationReject
	}
	var message entmoot.Message
	decoder := json.NewDecoder(bytes.NewReader(envelope.Data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&message); err != nil {
		return pubsub.ValidationReject
	}
	if message.GroupID != g.cfg.GroupID || message.Author.MemberID == nil {
		return pubsub.ValidationReject
	}
	binding, err := BindingFromPublicKey(message.Author.EntmootPubKey)
	if err != nil || binding.MemberID != *message.Author.MemberID || binding.PeerID != envelope.GetFrom() {
		return pubsub.ValidationReject
	}
	if err := gossip.VerifyLiveMessage(g.cfg.Roster, message, g.now()); err != nil {
		return pubsub.ValidationReject
	}
	inserted, err := g.cfg.Store.Put(context.Background(), g.cfg.GroupID, message)
	if err != nil {
		return pubsub.ValidationReject
	}
	envelope.ValidatorData = validatedLiveMessage{message: message, inserted: inserted}
	return pubsub.ValidationAccept
}

func (g *LiveGroup) consume(ctx context.Context) {
	defer close(g.done)
	for {
		envelope, err := g.subscription.Next(ctx)
		if err != nil {
			return
		}
		validated, ok := envelope.ValidatorData.(validatedLiveMessage)
		if ok && validated.inserted && g.cfg.OnIngest != nil {
			g.cfg.OnIngest(validated.message)
		}
	}
}

func (g *LiveGroup) Publish(ctx context.Context, message entmoot.Message) (DeliveryState, error) {
	if message.GroupID != g.cfg.GroupID {
		return "", errors.New("libp2p: publish group mismatch")
	}
	binding, err := BindingFromPublicKey(message.Author.EntmootPubKey)
	if err != nil || binding.PeerID != g.cfg.Host.ID() || message.Author.MemberID == nil || binding.MemberID != *message.Author.MemberID {
		return "", errors.New("libp2p: publisher identity does not match local host")
	}
	if err := gossip.VerifyLiveMessage(g.cfg.Roster, message, g.now()); err != nil {
		return "", err
	}
	inserted, err := g.cfg.Store.Put(ctx, g.cfg.GroupID, message)
	if err != nil {
		return "", err
	}
	if !inserted {
		return DeliveryAlreadyStored, nil
	}
	if g.cfg.OnIngest != nil {
		g.cfg.OnIngest(message)
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return DeliveryPendingHistory, err
	}
	if len(payload) > maxPubSubEnvelopeBytes {
		return DeliveryPendingHistory, errors.New("libp2p: live envelope exceeds limit")
	}
	if err := g.topic.Publish(ctx, payload); err != nil {
		return DeliveryPendingHistory, err
	}
	return DeliveryPublished, nil
}

func (g *LiveGroup) Close() error {
	var closeErr error
	g.closeOnce.Do(func() {
		g.cancel()
		g.subscription.Cancel()
		closeErr = g.topic.Close()
		<-g.done
	})
	return closeErr
}

type memberSubscriptionFilter struct {
	host   host.Host
	roster *roster.RosterLog
	topic  string
}

func (f *memberSubscriptionFilter) CanSubscribe(topic string) bool {
	return topic == f.topic
}

func (f *memberSubscriptionFilter) FilterIncomingSubscriptions(remote peer.ID, subscriptions []*pubsubpb.RPC_SubOpts) ([]*pubsubpb.RPC_SubOpts, error) {
	if !peerIsMember(f.host, f.roster, remote) {
		return nil, nil
	}
	filtered := make([]*pubsubpb.RPC_SubOpts, 0, 1)
	for _, subscription := range subscriptions {
		if subscription.GetTopicid() == f.topic {
			filtered = append(filtered, subscription)
		}
	}
	return filtered, nil
}

func peerIsMember(h host.Host, r *roster.RosterLog, peerID peer.ID) bool {
	publicKey, err := peerID.ExtractPublicKey()
	if err != nil || publicKey == nil {
		publicKey = h.Peerstore().PubKey(peerID)
	}
	if publicKey == nil {
		return false
	}
	raw, err := publicKey.Raw()
	if err != nil {
		return false
	}
	binding, err := BindingFromPublicKey(raw)
	return err == nil && binding.PeerID == peerID && r.IsMemberID(binding.MemberID)
}

func hostBinding(h host.Host) (Binding, error) {
	publicKey := h.Peerstore().PubKey(h.ID())
	if publicKey == nil {
		var err error
		publicKey, err = h.ID().ExtractPublicKey()
		if err != nil {
			return Binding{}, err
		}
	}
	raw, err := publicKey.Raw()
	if err != nil {
		return Binding{}, err
	}
	return BindingFromPublicKey(raw)
}
