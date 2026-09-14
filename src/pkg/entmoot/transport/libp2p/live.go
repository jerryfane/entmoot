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
	"entmoot/pkg/entmoot/membership"
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
	Host      host.Host
	GroupID   entmoot.GroupID
	Group     *membership.Group
	Store     store.MessageStore
	OnIngest  func(entmoot.Message)
	Authorize func(entmoot.Message) error
	Now       func() time.Time
}

type validatedLiveMessage struct {
	message  entmoot.Message
	inserted bool
}

// LiveRouter owns the single GossipSub instance installed on a libp2p host.
// Group topics share this router so their protocol handlers never overwrite
// each other.
type LiveRouter struct {
	host      host.Host
	pubsub    *pubsub.PubSub
	filter    *memberSubscriptionFilter
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// LiveGroup owns one authorized topic on its host's shared GossipSub router.
type LiveGroup struct {
	cfg          LiveConfig
	router       *LiveRouter
	ownedRouter  *LiveRouter
	topicName    string
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	cancel       context.CancelFunc
	done         chan struct{}
	quarantine   *rosterAheadQuarantine
	closeOnce    sync.Once
}

func GroupTopic(groupID entmoot.GroupID) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(groupTopicDomain))
	_, _ = hash.Write(groupID[:])
	return "/entmoot/group/2/" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
}

func NewLiveRouter(ctx context.Context, h host.Host) (*LiveRouter, error) {
	if h == nil {
		return nil, errors.New("libp2p: host is required for live delivery")
	}
	routerCtx, cancel := context.WithCancel(ctx)
	filter := &memberSubscriptionFilter{host: h, groups: make(map[string]*membership.Group)}
	params := pubsub.DefaultGossipSubParams()
	params.D = 4
	params.Dlo = 2
	params.Dhi = 6
	params.Dout = 1
	params.HeartbeatInterval = time.Second
	ps, err := pubsub.NewGossipSub(routerCtx, h,
		pubsub.WithMessageSignaturePolicy(pubsub.StrictSign),
		pubsub.WithMessageIdFn(envelopeMessageID),
		pubsub.WithGossipSubParams(params),
		pubsub.WithValidateThrottle(64),
		pubsub.WithValidateQueueSize(128),
		pubsub.WithPeerOutboundQueueSize(128),
		pubsub.WithSubscriptionFilter(filter),
		pubsub.WithPeerFilter(filter.peerAllowed),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("libp2p: create GossipSub: %w", err)
	}
	return &LiveRouter{host: h, pubsub: ps, filter: filter, cancel: cancel}, nil
}

func (r *LiveRouter) AddGroup(ctx context.Context, cfg LiveConfig) (*LiveGroup, error) {
	if r == nil || r.pubsub == nil || cfg.Host != r.host || cfg.Group == nil || cfg.Store == nil {
		return nil, errors.New("libp2p: router host, roster and store are required for live delivery")
	}
	localBinding, err := hostBinding(cfg.Host)
	if err != nil || !cfg.Group.IsMemberID(localBinding.MemberID) {
		return nil, fmt.Errorf("libp2p: local host is not a current group member")
	}
	topicName := GroupTopic(cfg.GroupID)
	if !r.filter.add(topicName, cfg.Group) {
		return nil, fmt.Errorf("libp2p: group topic is already active")
	}
	liveCtx, cancel := context.WithCancel(ctx)
	group := &LiveGroup{cfg: cfg, router: r, topicName: topicName, cancel: cancel, done: make(chan struct{}), quarantine: newRosterAheadQuarantine(cfg.Now)}
	if err := r.pubsub.RegisterTopicValidator(topicName, group.validate,
		pubsub.WithValidatorConcurrency(8)); err != nil {
		r.filter.remove(topicName)
		cancel()
		return nil, fmt.Errorf("libp2p: register topic validator: %w", err)
	}
	topic, err := r.pubsub.Join(topicName)
	if err != nil {
		_ = r.pubsub.UnregisterTopicValidator(topicName)
		r.filter.remove(topicName)
		cancel()
		return nil, fmt.Errorf("libp2p: join group topic: %w", err)
	}
	subscription, err := topic.Subscribe()
	if err != nil {
		_ = topic.Close()
		_ = r.pubsub.UnregisterTopicValidator(topicName)
		r.filter.remove(topicName)
		cancel()
		return nil, fmt.Errorf("libp2p: subscribe group topic: %w", err)
	}
	group.topic = topic
	group.subscription = subscription
	go group.consume(liveCtx)
	return group, nil
}

// NewLiveGroup is a single-group convenience used by focused callers and
// tests. Multi-group runtimes must create one LiveRouter and call AddGroup.
func NewLiveGroup(ctx context.Context, cfg LiveConfig) (*LiveGroup, error) {
	router, err := NewLiveRouter(ctx, cfg.Host)
	if err != nil {
		return nil, err
	}
	group, err := router.AddGroup(ctx, cfg)
	if err != nil {
		_ = router.Close()
		return nil, err
	}
	group.ownedRouter = router
	return group, nil
}

func (r *LiveRouter) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(r.cancel)
	return nil
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
	if err != nil || !g.cfg.Group.IsMemberID(local.MemberID) {
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
	if err := VerifyLiveMessage(g.cfg.Group, message, g.now()); err != nil {
		// A message naming a roster head we have not synchronized yet is a
		// race with a membership change, not a bad message. Hold it, ignore
		// the envelope so it is neither propagated nor counted against the
		// sender, and retry after the next roster sync.
		if errors.Is(err, entmoot.ErrRosterHeadUnknown) && g.quarantine != nil && g.quarantine.hold(message) {
			return pubsub.ValidationIgnore
		}
		return pubsub.ValidationReject
	}
	if g.cfg.Authorize != nil && envelope.GetFrom() != g.cfg.Host.ID() {
		if err := g.cfg.Authorize(message); err != nil {
			return pubsub.ValidationReject
		}
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
	if err := VerifyLiveMessage(g.cfg.Group, message, g.now()); err != nil {
		return "", err
	}
	if g.cfg.Authorize != nil {
		if err := g.cfg.Authorize(message); err != nil {
			return "", err
		}
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
		_ = g.router.pubsub.UnregisterTopicValidator(g.topicName)
		g.router.filter.remove(g.topicName)
		<-g.done
		if g.ownedRouter != nil {
			_ = g.ownedRouter.Close()
		}
	})
	return closeErr
}

type memberSubscriptionFilter struct {
	mu     sync.RWMutex
	host   host.Host
	groups map[string]*membership.Group
}

func (f *memberSubscriptionFilter) add(topic string, group *membership.Group) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.groups[topic]; exists {
		return false
	}
	f.groups[topic] = group
	return true
}

func (f *memberSubscriptionFilter) remove(topic string) {
	f.mu.Lock()
	delete(f.groups, topic)
	f.mu.Unlock()
}

func (f *memberSubscriptionFilter) group(topic string) (*membership.Group, bool) {
	f.mu.RLock()
	group, ok := f.groups[topic]
	f.mu.RUnlock()
	return group, ok
}

func (f *memberSubscriptionFilter) CanSubscribe(topic string) bool {
	_, ok := f.group(topic)
	return ok
}

func (f *memberSubscriptionFilter) peerAllowed(remote peer.ID, topic string) bool {
	group, ok := f.group(topic)
	return ok && peerIsMember(f.host, group, remote)
}

func (f *memberSubscriptionFilter) FilterIncomingSubscriptions(remote peer.ID, subscriptions []*pubsubpb.RPC_SubOpts) ([]*pubsubpb.RPC_SubOpts, error) {
	filtered := make([]*pubsubpb.RPC_SubOpts, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		if f.peerAllowed(remote, subscription.GetTopicid()) {
			filtered = append(filtered, subscription)
		}
	}
	return filtered, nil
}

func peerIsMember(h host.Host, group *membership.Group, peerID peer.ID) bool {
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
	return err == nil && binding.PeerID == peerID && group.IsMemberID(binding.MemberID)
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
