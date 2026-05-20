package policy

import (
	"testing"
	"time"
)

func TestTheEntMootDefaultPolicy(t *testing.T) {
	p := TheEntMootDefault()
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if p.MessageRatePerAuthor != "6/min" {
		t.Fatalf("MessageRatePerAuthor = %q, want 6/min", p.MessageRatePerAuthor)
	}
	if p.MessageBurstPerAuthor != 12 {
		t.Fatalf("MessageBurstPerAuthor = %d, want 12", p.MessageBurstPerAuthor)
	}
	if p.ByteRatePerAuthor != "64KiB/min" {
		t.Fatalf("ByteRatePerAuthor = %q, want 64KiB/min", p.ByteRatePerAuthor)
	}
	if p.ByteBurstPerAuthor != 128*1024 {
		t.Fatalf("ByteBurstPerAuthor = %d, want 128KiB", p.ByteBurstPerAuthor)
	}
	if p.MaxMessageBytes != 8192 {
		t.Fatalf("MaxMessageBytes = %d, want 8192", p.MaxMessageBytes)
	}
	if p.LiveTriggerRate != "6/min" {
		t.Fatalf("LiveTriggerRate = %q, want 6/min", p.LiveTriggerRate)
	}
	if p.LiveTriggerBurst != 6 {
		t.Fatalf("LiveTriggerBurst = %d, want 6", p.LiveTriggerBurst)
	}
	if p.LiveMaxActionsPerScan != 1 {
		t.Fatalf("LiveMaxActionsPerScan = %d, want 1", p.LiveMaxActionsPerScan)
	}
	if p.LiveMaxActionBytes != 4096 {
		t.Fatalf("LiveMaxActionBytes = %d, want 4096", p.LiveMaxActionBytes)
	}
	if p.RetentionDays != 30 {
		t.Fatalf("RetentionDays = %d, want 30", p.RetentionDays)
	}
}

func TestParseRates(t *testing.T) {
	msg, err := ParseMessageRate("6/min")
	if err != nil {
		t.Fatalf("ParseMessageRate: %v", err)
	}
	if msg.Units != 6 || msg.Period != time.Minute {
		t.Fatalf("message rate = %+v, want 6/min", msg)
	}
	if got := float64(msg.Limit()); got != 0.1 {
		t.Fatalf("message rate limit = %v, want 0.1", got)
	}

	bytes, err := ParseByteRate("64KiB/min")
	if err != nil {
		t.Fatalf("ParseByteRate: %v", err)
	}
	if bytes.Units != 64*1024 || bytes.Period != time.Minute {
		t.Fatalf("byte rate = %+v, want 65536/min", bytes)
	}
}

func TestPolicyValidateRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Policy)
	}{
		{
			name:   "bad message rate",
			mutate: func(p *Policy) { p.MessageRatePerAuthor = "1.5/min" },
		},
		{
			name:   "zero message burst",
			mutate: func(p *Policy) { p.MessageBurstPerAuthor = 0 },
		},
		{
			name:   "bad byte unit",
			mutate: func(p *Policy) { p.ByteRatePerAuthor = "64KB/min" },
		},
		{
			name:   "zero byte burst",
			mutate: func(p *Policy) { p.ByteBurstPerAuthor = 0 },
		},
		{
			name:   "zero max message bytes",
			mutate: func(p *Policy) { p.MaxMessageBytes = 0 },
		},
		{
			name: "byte burst below max message bytes",
			mutate: func(p *Policy) {
				p.ByteBurstPerAuthor = 1024
				p.MaxMessageBytes = 2048
			},
		},
		{
			name:   "bad live trigger rate",
			mutate: func(p *Policy) { p.LiveTriggerRate = "6/day" },
		},
		{
			name:   "zero live trigger burst",
			mutate: func(p *Policy) { p.LiveTriggerBurst = 0 },
		},
		{
			name:   "negative live max actions",
			mutate: func(p *Policy) { p.LiveMaxActionsPerScan = -1 },
		},
		{
			name:   "negative live max action bytes",
			mutate: func(p *Policy) { p.LiveMaxActionBytes = -1 },
		},
		{
			name:   "zero retention",
			mutate: func(p *Policy) { p.RetentionDays = 0 },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := TheEntMootDefault()
			tc.mutate(&p)
			if err := p.Validate(); err == nil {
				t.Fatal("Validate returned nil, want error")
			}
		})
	}
}
