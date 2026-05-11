package esphttp

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"entmoot/pkg/entmoot"
)

const (
	FleetCommandMessageType = "fleet.command"
	FleetCommandResultType  = "fleet.command.result"

	FleetCommandTargetAll  = "all"
	FleetCommandTargetNode = "node"

	FleetCommandRiskSafe   = "safe"
	FleetCommandRiskManual = "manual"

	FleetCommandStatusReceived  = "received"
	FleetCommandStatusAccepted  = "accepted"
	FleetCommandStatusRunning   = "running"
	FleetCommandStatusCompleted = "completed"
	FleetCommandStatusFailed    = "failed"
	FleetCommandStatusRejected  = "rejected"
	FleetCommandStatusExpired   = "expired"
	FleetCommandStatusDuplicate = "duplicate"

	FleetCommandActionEcho             = "echo"
	FleetCommandActionEntmootInfo      = "entmoot.info"
	FleetCommandActionEntmootVersion   = "entmoot.version"
	FleetCommandActionEntmootDoctor    = "entmoot.doctor_probe"
	FleetCommandActionPilotInfo        = "pilot.info"
	FleetCommandActionFleetLocalState  = "fleet.local_state"
	FleetCommandActionAgentInstruction = "agent.instruction"

	FleetCommandExternalActionMessageSend = "message.send"
)

const DefaultFleetCommandTTL = 5 * time.Minute
const (
	MaxFleetInstructionBytes         = 16 << 10
	MaxFleetInstructionContextBytes  = 16 << 10
	DefaultFleetInstructionTimeoutMS = 10 * 60 * 1000
	MinFleetInstructionTimeoutMS     = 5 * 1000
	MaxFleetInstructionTimeoutMS     = 30 * 60 * 1000
)

type FleetCommandTarget struct {
	Kind        string         `json:"kind"`
	PilotNodeID entmoot.NodeID `json:"pilot_node_id,omitempty"`
}

type FleetCommandEnvelope struct {
	Type           string                   `json:"type"`
	Version        int                      `json:"version"`
	CommandID      string                   `json:"command_id"`
	FleetID        string                   `json:"fleet_id"`
	ControlGroupID entmoot.GroupID          `json:"control_group_id"`
	IssuerNodeID   entmoot.NodeID           `json:"issuer_node_id"`
	Target         FleetCommandTarget       `json:"target"`
	Action         string                   `json:"action"`
	Args           map[string]interface{}   `json:"args,omitempty"`
	AutoAccept     bool                     `json:"auto_accept"`
	CreatedAtMS    int64                    `json:"created_at_ms"`
	ExpiresAtMS    int64                    `json:"expires_at_ms,omitempty"`
	IssuerProof    *FleetCommandIssuerProof `json:"issuer_proof,omitempty"`
}

type FleetCommandIssuerProof struct {
	Scheme        string          `json:"scheme"`
	NodeID        entmoot.NodeID  `json:"node_id"`
	EntmootPubKey string          `json:"entmoot_pubkey"`
	Method        string          `json:"method"`
	Path          string          `json:"path"`
	TimestampMS   int64           `json:"timestamp_ms"`
	Nonce         string          `json:"nonce"`
	Body          json.RawMessage `json:"body"`
	Signature     string          `json:"signature"`
}

const FleetCommandIssuerProofMemberV1 = "member_v1"

type FleetCommandResultEnvelope struct {
	Type          string         `json:"type"`
	Version       int            `json:"version"`
	CommandID     string         `json:"command_id"`
	FleetID       string         `json:"fleet_id"`
	AgentNodeID   entmoot.NodeID `json:"agent_node_id"`
	Action        string         `json:"action,omitempty"`
	Status        string         `json:"status"`
	Summary       string         `json:"summary,omitempty"`
	Output        string         `json:"output,omitempty"`
	StartedAtMS   int64          `json:"started_at_ms,omitempty"`
	CompletedAtMS int64          `json:"completed_at_ms,omitempty"`
}

type FleetCommandInstructionSpec struct {
	Instruction string                       `json:"instruction"`
	TimeoutMS   int64                        `json:"timeout_ms"`
	Context     map[string]interface{}       `json:"context,omitempty"`
	Actions     []FleetCommandExternalAction `json:"actions,omitempty"`
}

type FleetCommandExternalAction struct {
	ID               string `json:"id,omitempty"`
	Kind             string `json:"kind"`
	Channel          string `json:"channel,omitempty"`
	Target           string `json:"target,omitempty"`
	Required         bool   `json:"required,omitempty"`
	DeliveryRequired bool   `json:"delivery_required,omitempty"`
}

type FleetCommandCatalogEntry struct {
	Name           string `json:"name"`
	Risk           string `json:"risk"`
	ReadOnly       bool   `json:"read_only"`
	Destructive    bool   `json:"destructive"`
	Idempotent     bool   `json:"idempotent"`
	AutoAcceptSafe bool   `json:"auto_accept_safe"`
	TimeoutMS      int64  `json:"timeout_ms"`
	MaxOutputBytes int    `json:"max_output_bytes"`
	Description    string `json:"description"`
}

var fleetCommandCatalog = []FleetCommandCatalogEntry{
	{Name: FleetCommandActionEcho, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 2_000, MaxOutputBytes: 2_048, Description: "Echo a short coordinator-provided message."},
	{Name: FleetCommandActionEntmootVersion, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 2_000, MaxOutputBytes: 2_048, Description: "Report the local Entmoot build version."},
	{Name: FleetCommandActionEntmootInfo, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 5_000, MaxOutputBytes: 8_192, Description: "Report local Entmoot runtime group state."},
	{Name: FleetCommandActionEntmootDoctor, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 15_000, MaxOutputBytes: 16_384, Description: "Run a bounded local Fleet diagnostic snapshot."},
	{Name: FleetCommandActionPilotInfo, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 5_000, MaxOutputBytes: 8_192, Description: "Report local Pilot daemon info."},
	{Name: FleetCommandActionFleetLocalState, Risk: FleetCommandRiskSafe, ReadOnly: true, Idempotent: true, AutoAcceptSafe: true, TimeoutMS: 5_000, MaxOutputBytes: 8_192, Description: "Report local Fleet membership state from ESP storage."},
	{Name: FleetCommandActionAgentInstruction, Risk: FleetCommandRiskManual, ReadOnly: false, Idempotent: false, AutoAcceptSafe: false, TimeoutMS: MaxFleetInstructionTimeoutMS, MaxOutputBytes: 32_768, Description: "Queue a natural-language instruction for the local agent runtime."},
}

func FleetCommandCatalog() []FleetCommandCatalogEntry {
	out := make([]FleetCommandCatalogEntry, len(fleetCommandCatalog))
	copy(out, fleetCommandCatalog)
	return out
}

func FleetCommandCatalogLookup(action string) (FleetCommandCatalogEntry, bool) {
	action = NormalizeFleetCommandAction(action)
	for _, entry := range fleetCommandCatalog {
		if entry.Name == action {
			return entry, true
		}
	}
	return FleetCommandCatalogEntry{}, false
}

func NormalizeFleetCommandAction(action string) string {
	return strings.TrimSpace(strings.ToLower(action))
}

func NormalizeFleetCommandTarget(kind string) string {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case "", FleetCommandTargetAll:
		return FleetCommandTargetAll
	case FleetCommandTargetNode:
		return FleetCommandTargetNode
	default:
		return strings.TrimSpace(strings.ToLower(kind))
	}
}

func NormalizeFleetCommandResultStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case FleetCommandStatusAccepted,
		FleetCommandStatusRunning,
		FleetCommandStatusCompleted,
		FleetCommandStatusFailed,
		FleetCommandStatusRejected,
		FleetCommandStatusExpired,
		FleetCommandStatusDuplicate:
		return strings.TrimSpace(strings.ToLower(status))
	default:
		return ""
	}
}

func NewFleetCommandID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("fleet command id: %w", err)
	}
	return "cmd_" + hex.EncodeToString(b[:]), nil
}

func FleetCommandIDFromIssuerProofMaterial(proof FleetCommandIssuerProof) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		proof.Scheme,
		strconv.FormatUint(uint64(proof.NodeID), 10),
		proof.EntmootPubKey,
		strings.ToUpper(strings.TrimSpace(proof.Method)),
		proof.Path,
		strconv.FormatInt(proof.TimestampMS, 10),
		proof.Nonce,
		string(proof.Body),
		proof.Signature,
	}, "\n")))
	return "cmd_" + hex.EncodeToString(sum[:16])
}

func DecodeFleetCommandArgs(raw json.RawMessage) (map[string]interface{}, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var args map[string]interface{}
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid command args: %w", err)
	}
	return args, nil
}

func ValidateFleetCommandArgs(action string, args map[string]interface{}) error {
	switch NormalizeFleetCommandAction(action) {
	case FleetCommandActionAgentInstruction:
		_, err := FleetCommandInstructionSpecFromArgs(args)
		return err
	default:
		return nil
	}
}

func FleetCommandInstructionArgs(args map[string]interface{}) (string, int64, map[string]interface{}, error) {
	spec, err := FleetCommandInstructionSpecFromArgs(args)
	if err != nil {
		return "", 0, nil, err
	}
	return spec.Instruction, spec.TimeoutMS, spec.Context, nil
}

func FleetCommandInstructionSpecFromArgs(args map[string]interface{}) (FleetCommandInstructionSpec, error) {
	if args == nil {
		return FleetCommandInstructionSpec{}, fmt.Errorf("instruction args are required")
	}
	instruction, ok := args["instruction"].(string)
	instruction = strings.TrimSpace(instruction)
	if !ok || instruction == "" {
		return FleetCommandInstructionSpec{}, fmt.Errorf("instruction is required")
	}
	if len([]byte(instruction)) > MaxFleetInstructionBytes {
		return FleetCommandInstructionSpec{}, fmt.Errorf("instruction is too large")
	}
	timeoutMS := int64(DefaultFleetInstructionTimeoutMS)
	if raw, ok := args["timeout_ms"]; ok {
		parsed, err := fleetCommandInt64Arg(raw)
		if err != nil {
			return FleetCommandInstructionSpec{}, fmt.Errorf("timeout_ms must be an integer")
		}
		if parsed < MinFleetInstructionTimeoutMS {
			parsed = MinFleetInstructionTimeoutMS
		}
		if parsed > MaxFleetInstructionTimeoutMS {
			parsed = MaxFleetInstructionTimeoutMS
		}
		timeoutMS = parsed
	}
	contextArgs := map[string]interface{}{}
	if raw, ok := args["context"]; ok {
		obj, ok := raw.(map[string]interface{})
		if !ok {
			return FleetCommandInstructionSpec{}, fmt.Errorf("context must be an object")
		}
		encoded, err := json.Marshal(obj)
		if err != nil {
			return FleetCommandInstructionSpec{}, fmt.Errorf("context: %w", err)
		}
		if len(encoded) > MaxFleetInstructionContextBytes {
			return FleetCommandInstructionSpec{}, fmt.Errorf("context is too large")
		}
		contextArgs = obj
	}
	actions, err := fleetCommandExternalActionsArg(args["actions"])
	if err != nil {
		return FleetCommandInstructionSpec{}, err
	}
	return FleetCommandInstructionSpec{
		Instruction: instruction,
		TimeoutMS:   timeoutMS,
		Context:     contextArgs,
		Actions:     actions,
	}, nil
}

func fleetCommandInt64Arg(v interface{}) (int64, error) {
	switch n := v.(type) {
	case float64:
		if n != float64(int64(n)) {
			return 0, fmt.Errorf("not an integer")
		}
		return int64(n), nil
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case json.Number:
		return n.Int64()
	default:
		return 0, fmt.Errorf("not an integer")
	}
}

func fleetCommandExternalActionsArg(raw interface{}) ([]FleetCommandExternalAction, error) {
	if raw == nil {
		return nil, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("actions: %w", err)
	}
	var actions []FleetCommandExternalAction
	if err := json.Unmarshal(data, &actions); err != nil {
		return nil, fmt.Errorf("actions must be an array of objects")
	}
	requiredMessageSendActions := 0
	for i := range actions {
		action := &actions[i]
		action.ID = strings.TrimSpace(action.ID)
		action.Kind = strings.TrimSpace(strings.ToLower(action.Kind))
		action.Channel = strings.TrimSpace(strings.ToLower(action.Channel))
		action.Target = strings.TrimSpace(action.Target)
		if action.Kind == "" {
			return nil, fmt.Errorf("actions[%d].kind is required", i)
		}
		switch action.Kind {
		case FleetCommandExternalActionMessageSend:
			if action.Required || action.DeliveryRequired {
				requiredMessageSendActions++
				if requiredMessageSendActions > 1 {
					return nil, fmt.Errorf("only one required message.send action is supported")
				}
				if action.Channel == "" {
					return nil, fmt.Errorf("actions[%d].channel is required for message.send", i)
				}
				if action.Target == "" {
					return nil, fmt.Errorf("actions[%d].target is required for message.send", i)
				}
			}
		default:
			if action.Required || action.DeliveryRequired {
				return nil, fmt.Errorf("actions[%d].kind %q is not supported for required external actions", i, action.Kind)
			}
		}
	}
	return actions, nil
}

func VerifyFleetCommandIssuerProof(cmd FleetCommandEnvelope, coordinatorPubKey []byte) bool {
	proof := cmd.IssuerProof
	if proof == nil || proof.Scheme != FleetCommandIssuerProofMemberV1 {
		return false
	}
	if proof.NodeID == 0 || proof.NodeID != cmd.IssuerNodeID {
		return false
	}
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(proof.EntmootPubKey))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return false
	}
	if !bytes.Equal(pub, coordinatorPubKey) {
		return false
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimSpace(proof.Signature))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return false
	}
	method := strings.ToUpper(strings.TrimSpace(proof.Method))
	if method != http.MethodPost {
		return false
	}
	if !fleetCommandProofPathMatches(proof.Path, cmd.FleetID) {
		return false
	}
	body := proof.Body
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("{}")
	}
	if !fleetCommandProofBodyMatches(body, cmd) {
		return false
	}
	if cmd.CommandID != FleetCommandIDFromIssuerProofMaterial(*proof) {
		return false
	}
	input := MemberSigningInput(method, proof.Path, proof.NodeID, pub, proof.TimestampMS, proof.Nonce, body)
	return ed25519.Verify(pub, []byte(input), sig)
}

func fleetCommandProofPathMatches(rawPath, fleetID string) bool {
	u, err := url.Parse(rawPath)
	if err != nil {
		return false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "v1" || parts[1] != "fleets" || parts[3] != "commands" {
		return false
	}
	got, err := url.PathUnescape(parts[2])
	return err == nil && got == fleetID
}

func fleetCommandProofBodyMatches(body []byte, cmd FleetCommandEnvelope) bool {
	var req struct {
		Target       string          `json:"target"`
		TargetNodeID uint64          `json:"target_node_id"`
		Action       string          `json:"action"`
		Args         json.RawMessage `json:"args"`
		AutoAccept   *bool           `json:"auto_accept"`
		ExpiresAtMS  int64           `json:"expires_at_ms"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	if NormalizeFleetCommandTarget(req.Target) != NormalizeFleetCommandTarget(cmd.Target.Kind) {
		return false
	}
	if cmd.Target.Kind == FleetCommandTargetNode && entmoot.NodeID(req.TargetNodeID) != cmd.Target.PilotNodeID {
		return false
	}
	if NormalizeFleetCommandAction(req.Action) != NormalizeFleetCommandAction(cmd.Action) {
		return false
	}
	autoAccept := true
	if req.AutoAccept != nil {
		autoAccept = *req.AutoAccept
	}
	if autoAccept != cmd.AutoAccept {
		return false
	}
	if req.ExpiresAtMS != 0 {
		if req.ExpiresAtMS != cmd.ExpiresAtMS {
			return false
		}
	} else if cmd.ExpiresAtMS != cmd.IssuerProof.TimestampMS+DefaultFleetCommandTTL.Milliseconds() {
		return false
	}
	args, err := DecodeFleetCommandArgs(req.Args)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(args, cmd.Args)
}
