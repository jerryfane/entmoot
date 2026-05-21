package policy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
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

func TestPolicyPresets(t *testing.T) {
	standard, err := PresetPolicy(PresetStandard)
	if err != nil {
		t.Fatalf("standard preset: %v", err)
	}
	if standard == nil {
		t.Fatal("standard preset returned nil policy")
	}
	if *standard != TheEntMootDefault() {
		t.Fatalf("standard = %+v, want default %+v", *standard, TheEntMootDefault())
	}
	if err := standard.Validate(); err != nil {
		t.Fatalf("standard Validate: %v", err)
	}

	relaxed, err := PresetPolicy(PresetRelaxed)
	if err != nil {
		t.Fatalf("relaxed preset: %v", err)
	}
	if relaxed == nil {
		t.Fatal("relaxed preset returned nil policy")
	}
	if err := relaxed.Validate(); err != nil {
		t.Fatalf("relaxed Validate: %v", err)
	}
	if relaxed.RetentionDays <= standard.RetentionDays {
		t.Fatalf("relaxed retention = %d, want above standard %d", relaxed.RetentionDays, standard.RetentionDays)
	}

	none, err := PresetPolicy(PresetNone)
	if err != nil {
		t.Fatalf("none preset: %v", err)
	}
	if none != nil {
		t.Fatalf("none preset = %+v, want nil", none)
	}

	if _, err := PresetPolicy("unknown"); err == nil {
		t.Fatal("unknown preset returned nil error")
	}
}

func TestResolveSource(t *testing.T) {
	if _, err := ResolveSource("", ""); err == nil {
		t.Fatal("ResolveSource without source returned nil error")
	}
	if _, err := ResolveSource(PresetStandard, "policy.json"); err == nil {
		t.Fatal("ResolveSource with preset and file returned nil error")
	}

	none, err := ResolveSource(PresetNone, "")
	if err != nil {
		t.Fatalf("ResolveSource none: %v", err)
	}
	if none.Policy != nil || none.Source != "preset:none" {
		t.Fatalf("none source = %+v, want nil preset:none", none)
	}

	path := filepath.Join(t.TempDir(), "policy.json")
	raw, err := json.Marshal(Relaxed())
	if err != nil {
		t.Fatalf("Marshal relaxed: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	fromFile, err := ResolveSource("", path)
	if err != nil {
		t.Fatalf("ResolveSource file: %v", err)
	}
	if fromFile.Policy == nil || *fromFile.Policy != Relaxed() {
		t.Fatalf("file source policy = %+v, want relaxed", fromFile.Policy)
	}
	if !strings.HasPrefix(fromFile.Source, "file:") {
		t.Fatalf("file source = %q, want file prefix", fromFile.Source)
	}
}

func TestLoadJSONFileRejectsUnknownFieldsAndTrailingData(t *testing.T) {
	dir := t.TempDir()
	unknown := filepath.Join(dir, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"message_rate_per_author":"6/min","message_burst_per_author":12,"byte_rate_per_author":"64KiB/min","byte_burst_per_author":131072,"max_message_bytes":8192,"live_trigger_rate":"6/min","live_trigger_burst":6,"live_max_actions_per_scan":1,"live_max_action_bytes":4096,"retention_days":30,"extra":true}`), 0o600); err != nil {
		t.Fatalf("WriteFile unknown: %v", err)
	}
	if _, err := LoadJSONFile(unknown); err == nil {
		t.Fatal("LoadJSONFile unknown field returned nil error")
	}

	trailing := filepath.Join(dir, "trailing.json")
	raw, err := json.Marshal(Standard())
	if err != nil {
		t.Fatalf("Marshal standard: %v", err)
	}
	raw = append(raw, []byte(` {}`)...)
	if err := os.WriteFile(trailing, raw, 0o600); err != nil {
		t.Fatalf("WriteFile trailing: %v", err)
	}
	if _, err := LoadJSONFile(trailing); err == nil {
		t.Fatal("LoadJSONFile trailing data returned nil error")
	}
}

func TestPolicySummary(t *testing.T) {
	got := Summary(TheEntMootDefault())
	for _, want := range []string{"message_rate=6/min", "burst=12", "max_message_bytes=8192", "live_rate=6/min", "retention_days=30"} {
		if !strings.Contains(got, want) {
			t.Fatalf("Summary = %q, missing %q", got, want)
		}
	}
}
