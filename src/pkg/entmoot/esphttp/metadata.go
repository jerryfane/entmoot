package esphttp

import (
	"encoding/json"
	"errors"
)

// NormalizeGroupMetadata enforces the ESP group metadata contract: metadata is
// app-facing object JSON. Empty input becomes an empty object.
func NormalizeGroupMetadata(metadata json.RawMessage) (json.RawMessage, error) {
	if len(metadata) == 0 {
		metadata = json.RawMessage(`{}`)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(metadata, &obj); err != nil {
		return nil, errors.New("esphttp: group metadata must be a JSON object")
	}
	if obj == nil {
		return nil, errors.New("esphttp: group metadata must be a JSON object")
	}
	return append(json.RawMessage(nil), metadata...), nil
}
