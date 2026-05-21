package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	PresetStandard = "standard"
	PresetRelaxed  = "relaxed"
	PresetNone     = "none"
)

// SourceResolution is the result of resolving CLI/user policy input.
type SourceResolution struct {
	Policy *Policy
	Source string
}

// Standard returns the default policy used for newly managed moots.
func Standard() Policy {
	return Policy{
		MessageRatePerAuthor:  DefaultMessageRatePerAuthor,
		MessageBurstPerAuthor: DefaultMessageBurstPerAuthor,
		ByteRatePerAuthor:     DefaultByteRatePerAuthor,
		ByteBurstPerAuthor:    DefaultByteBurstPerAuthor,
		MaxMessageBytes:       DefaultMaxMessageBytes,
		LiveTriggerRate:       DefaultLiveTriggerRate,
		LiveTriggerBurst:      DefaultLiveTriggerBurst,
		LiveMaxActionsPerScan: DefaultLiveMaxActionsPerScan,
		LiveMaxActionBytes:    DefaultLiveMaxActionBytes,
		RetentionDays:         DefaultRetentionDays,
	}
}

// Relaxed returns a higher-throughput preset for private or high-trust groups.
func Relaxed() Policy {
	return Policy{
		MessageRatePerAuthor:  "60/min",
		MessageBurstPerAuthor: 120,
		ByteRatePerAuthor:     "1MiB/min",
		ByteBurstPerAuthor:    4 * 1024 * 1024,
		MaxMessageBytes:       64 * 1024,
		LiveTriggerRate:       "30/min",
		LiveTriggerBurst:      30,
		LiveMaxActionsPerScan: 3,
		LiveMaxActionBytes:    16 * 1024,
		RetentionDays:         90,
	}
}

// PresetPolicy resolves a named preset. The "none" preset is valid but returns
// no policy, allowing callers to clear local enforcement state explicitly.
func PresetPolicy(name string) (*Policy, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case PresetStandard, "":
		p := Standard()
		return &p, nil
	case PresetRelaxed:
		p := Relaxed()
		return &p, nil
	case PresetNone:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown policy preset %q (want: standard, relaxed, none)", name)
	}
}

// ResolveSource resolves exactly one preset or JSON file into an optional policy.
func ResolveSource(presetName, filePath string) (SourceResolution, error) {
	presetName = strings.TrimSpace(presetName)
	filePath = strings.TrimSpace(filePath)
	if presetName != "" && filePath != "" {
		return SourceResolution{}, errors.New("choose either -preset or -file, not both")
	}
	if presetName == "" && filePath == "" {
		return SourceResolution{}, errors.New("choose -preset or -file")
	}
	if presetName != "" {
		p, err := PresetPolicy(presetName)
		if err != nil {
			return SourceResolution{}, err
		}
		return SourceResolution{Policy: p, Source: "preset:" + strings.ToLower(presetName)}, nil
	}
	p, err := LoadJSONFile(filePath)
	if err != nil {
		return SourceResolution{}, err
	}
	return SourceResolution{Policy: &p, Source: "file:" + filePath}, nil
}

// LoadJSONFile decodes and validates one policy JSON document from disk.
func LoadJSONFile(path string) (Policy, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Policy{}, fmt.Errorf("read policy file %q: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var p Policy
	if err := dec.Decode(&p); err != nil {
		return Policy{}, fmt.Errorf("decode policy file %q: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return Policy{}, fmt.Errorf("decode policy file %q: trailing JSON data", path)
	} else if !errors.Is(err, io.EOF) {
		return Policy{}, fmt.Errorf("decode policy file %q: trailing JSON data: %w", path, err)
	}
	if err := p.Validate(); err != nil {
		return Policy{}, fmt.Errorf("validate policy file %q: %w", path, err)
	}
	return p, nil
}

// Summary returns the stable one-line policy summary used by status commands.
func Summary(p Policy) string {
	return fmt.Sprintf("message_rate=%s burst=%d max_message_bytes=%d live_rate=%s live_burst=%d retention_days=%d",
		p.MessageRatePerAuthor, p.MessageBurstPerAuthor, p.MaxMessageBytes, p.LiveTriggerRate, p.LiveTriggerBurst, p.RetentionDays)
}
