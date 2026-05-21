package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"entmoot/pkg/entmoot"
)

const (
	UpdateTopic = "_entmoot/policy/v1"
	UpdateType  = "entmoot.policy_update.v1"
)

// Update is the founder-authored policy update payload carried on UpdateTopic.
// A nil Policy clears the stored per-moot policy and restores legacy behavior.
type Update struct {
	Type        string          `json:"type"`
	GroupID     entmoot.GroupID `json:"group_id"`
	Policy      *Policy         `json:"policy"`
	UpdatedAtMS int64           `json:"updated_at_ms"`
	Sequence    uint64          `json:"sequence"`
}

func NewUpdate(groupID entmoot.GroupID, p *Policy, updatedAtMS int64, sequence uint64) Update {
	var policyCopy *Policy
	if p != nil {
		cp := *p
		policyCopy = &cp
	}
	return Update{
		Type:        UpdateType,
		GroupID:     groupID,
		Policy:      policyCopy,
		UpdatedAtMS: updatedAtMS,
		Sequence:    sequence,
	}
}

func ParseUpdate(raw []byte) (Update, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var update Update
	if err := dec.Decode(&update); err != nil {
		return Update{}, fmt.Errorf("decode policy update: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return Update{}, errors.New("decode policy update: trailing JSON data")
	} else if !errors.Is(err, io.EOF) {
		return Update{}, fmt.Errorf("decode policy update: trailing JSON data: %w", err)
	}
	if err := update.Validate(); err != nil {
		return Update{}, err
	}
	return update, nil
}

func (u Update) Validate() error {
	if u.Type != UpdateType {
		return fmt.Errorf("policy update type = %q, want %q", u.Type, UpdateType)
	}
	if u.GroupID == (entmoot.GroupID{}) {
		return errors.New("policy update group_id is required")
	}
	if u.UpdatedAtMS <= 0 {
		return errors.New("policy update updated_at_ms must be positive")
	}
	if u.Sequence == 0 {
		return errors.New("policy update sequence must be positive")
	}
	if u.Policy != nil {
		if err := u.Policy.Validate(); err != nil {
			return fmt.Errorf("policy update policy: %w", err)
		}
	}
	return nil
}
