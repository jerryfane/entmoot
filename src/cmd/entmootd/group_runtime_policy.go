package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/membership"
	entpolicy "entmoot/pkg/entmoot/policy"
	"entmoot/pkg/entmoot/store"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

// policyMessageStore is shared by live publication, live receipt and history
// catch-up. Their transport validators authenticate the message before Put.
// Persist the signed message first: it is the recovery record if applying the
// policy fails or the process exits between the two writes.
type policyMessageStore struct {
	store.MessageStore
	group    *membership.Group
	policies *entpolicy.FileStore
}

func isPolicyUpdate(message entmoot.Message) bool {
	return stringMember(message.Topics, entpolicy.UpdateTopic)
}

func founderPolicyUpdate(group *membership.Group, groupID entmoot.GroupID, message entmoot.Message) (entpolicy.Update, error) {
	if message.GroupID != groupID {
		return entpolicy.Update{}, fmt.Errorf("policy update: message group mismatch")
	}
	if !bytes.Equal(message.Author.EntmootPubKey, group.Founder().EntmootPubKey) {
		return entpolicy.Update{}, fmt.Errorf("policy update: only the founder may change policy: %w", entmoot.ErrNotMember)
	}
	update, err := entpolicy.ParseUpdate(message.Content)
	if err != nil {
		return entpolicy.Update{}, err
	}
	if update.GroupID != groupID {
		return entpolicy.Update{}, fmt.Errorf("policy update: payload group mismatch")
	}
	return update, nil
}

func (s *policyMessageStore) Put(ctx context.Context, groupID entmoot.GroupID, message entmoot.Message) (bool, error) {
	if !isPolicyUpdate(message) {
		return s.MessageStore.Put(ctx, groupID, message)
	}
	update, err := founderPolicyUpdate(s.group, groupID, message)
	if err != nil {
		return false, err
	}
	inserted, err := s.MessageStore.Put(ctx, groupID, message)
	if err != nil {
		return false, err
	}
	// ApplyUpdate arbitrates concurrent writers by sequence. Stale history is
	// still valid history, but never rolls back a newer policy (including clear).
	if _, err := s.policies.ApplyUpdate(ctx, update); err != nil {
		return inserted, fmt.Errorf("apply stored founder policy: %w", err)
	}
	return inserted, nil
}

// Replay before live traffic or pruning starts. This also upgrades nodes that
// already stored policy messages while the runtime lacked a consumer. Select by
// sequence, not message time or traversal order; only one store write is needed.
func (r *groupRuntime) reconcilePolicyFromHistory(ctx context.Context, groupID entmoot.GroupID, group *membership.Group) error {
	var boundary *store.PageBoundary
	var newest entpolicy.Update
	for {
		messages, err := r.store.LatestByTopicBefore(ctx, groupID, entpolicy.UpdateTopic, 128, boundary)
		if err != nil {
			return err
		}
		for _, message := range messages {
			if err := libp2ptransport.VerifyHistoricalMessage(group, message, time.Now()); err != nil {
				continue
			}
			update, err := founderPolicyUpdate(group, groupID, message)
			if err == nil && update.Sequence > newest.Sequence {
				newest = update
			}
		}
		if len(messages) < 128 {
			break
		}
		boundary = nextProfileBoundary(messages)
	}
	if newest.Sequence == 0 {
		return nil
	}
	_, err := r.policyStore.ApplyUpdate(ctx, newest)
	return err
}
