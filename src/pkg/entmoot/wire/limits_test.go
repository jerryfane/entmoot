package wire

import (
	"errors"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestPayloadCollectionLimitBoundary(t *testing.T) {
	allowed := &Gossip{IDs: make([]entmoot.MessageID, MaxGossipIDs)}
	if _, _, err := Encode(allowed); err != nil {
		t.Fatalf("Encode at gossip id cap: %v", err)
	}
	rejected := &Gossip{IDs: make([]entmoot.MessageID, MaxGossipIDs+1)}
	if _, _, err := Encode(rejected); !errors.Is(err, entmoot.ErrOversized) {
		t.Fatalf("Encode over gossip id cap = %v, want ErrOversized", err)
	}
}

func TestDecodeRejectsCollectionOverLimit(t *testing.T) {
	body := []byte(`{"group_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","ids":[`)
	for i := 0; i < MaxGossipIDs+1; i++ {
		if i > 0 {
			body = append(body, ',')
		}
		body = append(body, `"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="`...)
	}
	body = append(body, ']', '}')
	if _, err := Decode(MsgGossip, body); !errors.Is(err, entmoot.ErrOversized) {
		t.Fatalf("Decode over gossip id cap = %v, want ErrOversized", err)
	}
}

func TestRangeRequestLimitBoundary(t *testing.T) {
	allowed := &RangeReq{Limit: MaxRangeIDs}
	if _, _, err := Encode(allowed); err != nil {
		t.Fatalf("Encode at range limit: %v", err)
	}
	oversized := &RangeReq{Limit: MaxRangeIDs + 1}
	if _, _, err := Encode(oversized); !errors.Is(err, entmoot.ErrOversized) {
		t.Fatalf("Encode over range limit = %v, want ErrOversized", err)
	}
	negative := &RangeReq{Limit: -1}
	if _, _, err := Encode(negative); !errors.Is(err, entmoot.ErrMalformedFrame) {
		t.Fatalf("Encode negative range limit = %v, want ErrMalformedFrame", err)
	}
}
