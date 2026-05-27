package store

import (
	"bytes"
	"context"
	"sort"

	"entmoot/pkg/entmoot"
)

const (
	// DefaultMessageContextBefore is the suggested number of messages to load
	// before a search target when callers do not specify a context size.
	DefaultMessageContextBefore = 25
	// DefaultMessageContextAfter is the suggested number of messages to load
	// after a search target when callers do not specify a context size.
	DefaultMessageContextAfter = 25
	// MaxMessageContextSide bounds each side of a context request.
	MaxMessageContextSide = 100
)

// MessageContextOptions controls a bounded conversation window around one
// target message.
type MessageContextOptions struct {
	Before int
	After  int
	Topic  string
}

// NormalizeMessageContextOptions clamps context sizes to the store contract.
func NormalizeMessageContextOptions(opts MessageContextOptions) MessageContextOptions {
	if opts.Before < 0 {
		opts.Before = 0
	}
	if opts.After < 0 {
		opts.After = 0
	}
	if opts.Before > MaxMessageContextSide {
		opts.Before = MaxMessageContextSide
	}
	if opts.After > MaxMessageContextSide {
		opts.After = MaxMessageContextSide
	}
	return opts
}

// DefaultMessageContextOptions returns the recommended default context window.
func DefaultMessageContextOptions() MessageContextOptions {
	return MessageContextOptions{
		Before: DefaultMessageContextBefore,
		After:  DefaultMessageContextAfter,
	}
}

// MessageContextResult is the storage-level conversation window around a
// target message. Messages are ordered oldest-to-newest by the same stable
// recency tuple used by history pagination.
type MessageContextResult struct {
	Target              entmoot.Message
	Messages            []entmoot.Message
	HasMoreOlder        bool
	OlderCursorBoundary *PageBoundary
}

// MessageContexter is implemented by stores with a native context lookup.
type MessageContexter interface {
	MessageContext(ctx context.Context, groupID entmoot.GroupID, messageID entmoot.MessageID, opts MessageContextOptions) (MessageContextResult, error)
}

// MessageContext returns a bounded conversation window around messageID using
// a native store implementation when available, otherwise a deterministic scan.
func MessageContext(ctx context.Context, st MessageStore, groupID entmoot.GroupID, messageID entmoot.MessageID, opts MessageContextOptions) (MessageContextResult, error) {
	opts = NormalizeMessageContextOptions(opts)
	if contexter, ok := st.(MessageContexter); ok {
		return contexter.MessageContext(ctx, groupID, messageID, opts)
	}
	return scanMessageContext(ctx, st, groupID, messageID, opts)
}

func scanMessageContext(ctx context.Context, st MessageStore, groupID entmoot.GroupID, messageID entmoot.MessageID, opts MessageContextOptions) (MessageContextResult, error) {
	target, err := st.Get(ctx, groupID, messageID)
	if err != nil {
		return MessageContextResult{}, err
	}
	if opts.Topic != "" && !messageHasTopic(target, opts.Topic) {
		return MessageContextResult{}, ErrNotFound
	}
	msgs, err := st.Range(ctx, groupID, 0, 0)
	if err != nil {
		return MessageContextResult{}, err
	}

	var older, newer []entmoot.Message
	for _, msg := range msgs {
		if msg.ID == target.ID {
			continue
		}
		if opts.Topic != "" && !messageHasTopic(msg, opts.Topic) {
			continue
		}
		cmp := compareMessageRecency(msg, target)
		switch {
		case cmp < 0:
			older = append(older, msg)
		case cmp > 0:
			newer = append(newer, msg)
		}
	}
	sortMessagesNewestFirst(older)
	sortMessagesOldestFirst(newer)

	hasMoreOlder := len(older) > opts.Before
	if hasMoreOlder {
		older = older[:opts.Before]
	}
	sortMessagesOldestFirst(older)
	if len(newer) > opts.After {
		newer = newer[:opts.After]
	}

	messages := make([]entmoot.Message, 0, len(older)+1+len(newer))
	messages = append(messages, older...)
	messages = append(messages, target)
	messages = append(messages, newer...)

	return messageContextResult(target, messages, hasMoreOlder), nil
}

func messageContextResult(target entmoot.Message, messages []entmoot.Message, hasMoreOlder bool) MessageContextResult {
	result := MessageContextResult{
		Target:       target,
		Messages:     messages,
		HasMoreOlder: hasMoreOlder,
	}
	if hasMoreOlder {
		boundary := pageBoundaryFromMessage(target)
		if len(messages) > 0 {
			boundary = pageBoundaryFromMessage(messages[0])
		}
		result.OlderCursorBoundary = &boundary
	}
	if result.Messages == nil {
		result.Messages = []entmoot.Message{}
	}
	return result
}

func pageBoundaryFromMessage(m entmoot.Message) PageBoundary {
	return PageBoundary{
		TimestampMS:  m.Timestamp,
		AuthorNodeID: m.Author.PilotNodeID,
		MessageID:    m.ID,
	}
}

func compareMessageRecency(a, b entmoot.Message) int {
	if a.Timestamp != b.Timestamp {
		if a.Timestamp < b.Timestamp {
			return -1
		}
		return 1
	}
	if a.Author.PilotNodeID != b.Author.PilotNodeID {
		if a.Author.PilotNodeID < b.Author.PilotNodeID {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.ID[:], b.ID[:])
}

func sortMessagesNewestFirst(msgs []entmoot.Message) {
	sortMessagesByRecency(msgs, true)
}

func sortMessagesOldestFirst(msgs []entmoot.Message) {
	sortMessagesByRecency(msgs, false)
}

func sortMessagesByRecency(msgs []entmoot.Message, newestFirst bool) {
	if len(msgs) < 2 {
		return
	}
	sort.Slice(msgs, func(i, j int) bool {
		cmp := compareMessageRecency(msgs[i], msgs[j])
		if newestFirst {
			return cmp > 0
		}
		return cmp < 0
	})
}
