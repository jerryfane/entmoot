package gossip

import (
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/topic"
)

// Message shape limits apply to local publish, live ingress, and history
// backfill. Existing records loaded directly from local storage remain readable
// but cannot be reintroduced through a network admission path if they exceed
// these limits.
const (
	MaxMessageParents        = 3
	MaxMessageTopics         = 16
	MaxMessageTopicBytes     = 256
	MaxMessageReferences     = 64
	MaxCanonicalMessageBytes = 256 * 1024
	MaxMessageFutureSkew     = 2 * time.Minute
)

// ValidateMessageShape performs cheap count and topic checks before canonical
// encoding. It intentionally does not verify membership or signatures.
func ValidateMessageShape(msg entmoot.Message, now time.Time) error {
	switch msg.Version {
	case 0:
		if msg.RosterHead != nil {
			return fmt.Errorf("gossip: legacy message must not carry roster_head")
		}
	case 2:
		if msg.RosterHead == nil {
			return fmt.Errorf("gossip: version-2 message requires roster_head")
		}
	default:
		return fmt.Errorf("gossip: unsupported message version %d", msg.Version)
	}
	if len(msg.Parents) > MaxMessageParents {
		return fmt.Errorf("gossip: message has %d parents, cap is %d", len(msg.Parents), MaxMessageParents)
	}
	if len(msg.Topics) > MaxMessageTopics {
		return fmt.Errorf("gossip: message has %d topics, cap is %d", len(msg.Topics), MaxMessageTopics)
	}
	for i, name := range msg.Topics {
		if len(name) > MaxMessageTopicBytes {
			return fmt.Errorf("gossip: topic %d is %d bytes, cap is %d", i, len(name), MaxMessageTopicBytes)
		}
		if err := topic.ValidTopic(name); err != nil {
			return fmt.Errorf("gossip: topic %d: %w", i, err)
		}
	}
	if len(msg.References) > MaxMessageReferences {
		return fmt.Errorf("gossip: message has %d references, cap is %d", len(msg.References), MaxMessageReferences)
	}
	if msg.Timestamp > now.Add(MaxMessageFutureSkew).UnixMilli() {
		return fmt.Errorf("gossip: message timestamp %d exceeds future skew %s", msg.Timestamp, MaxMessageFutureSkew)
	}
	encoded, err := canonical.Encode(msg)
	if err != nil {
		return fmt.Errorf("gossip: canonical message encoding: %w", err)
	}
	if len(encoded) > MaxCanonicalMessageBytes {
		return fmt.Errorf("gossip: canonical message is %d bytes, cap is %d", len(encoded), MaxCanonicalMessageBytes)
	}
	return nil
}
