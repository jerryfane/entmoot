package policy

import (
	"testing"

	"entmoot/pkg/entmoot/ratelimit"
)

func TestContentLimitsFromPolicy(t *testing.T) {
	p := TheEntMootDefault()
	limits, err := ContentLimits(p)
	if err != nil {
		t.Fatalf("ContentLimits: %v", err)
	}
	if got := float64(limits.MsgRate); got != 0.1 {
		t.Fatalf("MsgRate = %v, want 0.1", got)
	}
	if limits.MsgBurst != 12 {
		t.Fatalf("MsgBurst = %d, want 12", limits.MsgBurst)
	}
	if got := float64(limits.BytesRate); got != float64(64*1024)/60 {
		t.Fatalf("BytesRate = %v, want %v", got, float64(64*1024)/60)
	}
	if limits.BytesBurst != 128*1024 {
		t.Fatalf("BytesBurst = %d, want 128KiB", limits.BytesBurst)
	}
	if len(limits.TopicLimits) != 0 {
		t.Fatalf("TopicLimits = %+v, want none for content limiter", limits.TopicLimits)
	}
}

func TestSystemLimitsPreservesDefaultLimiter(t *testing.T) {
	got := SystemLimits(nil)
	want := ratelimit.DefaultLimits()
	if got.MsgRate != want.MsgRate || got.MsgBurst != want.MsgBurst ||
		got.BytesRate != want.BytesRate || got.BytesBurst != want.BytesBurst {
		t.Fatalf("SystemLimits = %+v, want %+v", got, want)
	}
	if len(got.TopicLimits) != len(want.TopicLimits) {
		t.Fatalf("SystemLimits topic count = %d, want %d", len(got.TopicLimits), len(want.TopicLimits))
	}
}
