package store

import (
	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/order"
)

// topoOrder sorts messages so every parent precedes its children. Messages
// whose ids are not in the resolved order are dropped, which is what callers
// paging a window want: a child whose parent fell outside the window keeps its
// position relative to the messages that are present.
func topoOrder(msgs []entmoot.Message) ([]entmoot.Message, error) {
	if len(msgs) == 0 {
		return []entmoot.Message{}, nil
	}
	ids, err := order.Topological(msgs)
	if err != nil {
		return nil, err
	}
	index := make(map[entmoot.MessageID]entmoot.Message, len(msgs))
	for _, m := range msgs {
		index[m.ID] = m
	}
	out := make([]entmoot.Message, 0, len(ids))
	for _, id := range ids {
		if m, ok := index[id]; ok {
			out = append(out, m)
		}
	}
	return out, nil
}
