package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/canonical"
	"entmoot/pkg/entmoot/esphttp"
	"entmoot/pkg/entmoot/features"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/mailbox/mailboxtest"
	"entmoot/pkg/entmoot/store"
	"entmoot/pkg/entmoot/store/storetest"
)

func TestFleetCLIMemberIDAssignmentsAndCommands(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	identities := make([]*keystore.Identity, 3)
	members := make([]entmoot.NodeInfo, 3)
	for i := range identities {
		identity, err := keystore.Generate()
		if err != nil {
			t.Fatal(err)
		}
		identities[i] = identity
		memberID, err := entmoot.MemberIDFromPublicKey(identity.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		peerID, err := entmoot.PeerIDFromPublicKey(identity.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		members[i] = entmoot.NodeInfo{MemberID: &memberID, PeerID: peerID, EntmootPubKey: identity.PublicKey}
	}
	gf := &globalFlags{data: root, identity: filepath.Join(root, "identity.json")}
	if err := identities[0].Save(gf.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := setup(gf); err != nil {
		t.Fatal(err)
	}
	state, err := esphttp.OpenSQLiteStateStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	gid := entmoot.GroupID{14, 3, 1}
	const fleetID = "cli-member-contract"
	if _, err := state.CreateFleet(ctx, esphttp.FleetRecord{FleetID: fleetID, Name: "CLI", Coordinator: members[0], ControlGroupID: gid, Status: esphttp.FleetStatusActive}); err != nil {
		t.Fatal(err)
	}
	for i, member := range members {
		role := esphttp.FleetRoleAgent
		if i == 0 {
			role = esphttp.FleetRoleCoordinator
		}
		if _, err := state.UpsertFleetMember(ctx, esphttp.FleetMemberRecord{FleetID: fleetID, MemberID: *member.MemberID, PeerID: member.PeerID, EntmootPubKey: base64.StdEncoding.EncodeToString(member.EntmootPubKey), Role: role, Status: esphttp.FleetMemberActive}); err != nil {
			t.Fatal(err)
		}
	}
	messages := storetest.New(t)
	service := mailboxtest.New(t, messages, nil)
	handler, err := esphttp.NewHandler(esphttp.Config{Token: "unused-bearer-token", Service: service, State: state, Features: features.Flags{FleetEnabled: true, TasksEnabled: true}, TaskEvents: fleetCLIEventPublisher{identities[0], members[0], messages}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	common := []string{"-esp-url", server.URL, "-fleet", fleetID}
	tasks := func(args ...string) string {
		t.Helper()
		all := append([]string{args[0]}, common...)
		all = append(all, args[1:]...)
		code, stdout, stderr := captureCommandOutput(t, func() int { return cmdFleetTasks(gf, all) })
		if code != exitOK {
			t.Fatalf("tasks %v: exit=%d stderr=%s", args, code, stderr)
		}
		return stdout
	}
	var created struct {
		Task esphttp.FleetTaskRecord `json:"task"`
	}
	if err := json.Unmarshal([]byte(tasks("create", "-title", "direct task", "-mode", "direct_assignee", "-assignee-member-id", members[1].MemberID.String())), &created); err != nil {
		t.Fatal(err)
	}
	if created.Task.Assignee == nil || *created.Task.Assignee.MemberID != *members[1].MemberID || created.Task.Status != esphttp.FleetTaskStatusAssigned {
		t.Fatalf("direct assignment: %+v", created.Task)
	}
	tasks("assign", "-task", created.Task.TaskID, "-assignee-member-id", members[2].MemberID.String())
	var shown struct {
		Task esphttp.FleetTaskRecord `json:"task"`
	}
	if err := json.Unmarshal([]byte(tasks("show", "-task", created.Task.TaskID)), &shown); err != nil {
		t.Fatal(err)
	}
	if shown.Task.Assignee == nil || *shown.Task.Assignee.MemberID != *members[2].MemberID {
		t.Fatalf("persisted reassignment: %+v", shown.Task)
	}
	commandArgs := append(append([]string{}, common...), "-target", "node", "-target-member-id", members[1].MemberID.String(), "-action", esphttp.FleetCommandActionEcho, "-args-json", `{"message":"member contract"}`)
	code, stdout, stderr := captureCommandOutput(t, func() int { return cmdFleetCommandsSend(gf, commandArgs) })
	if code != exitOK {
		t.Fatalf("send: exit=%d stderr=%s", code, stderr)
	}
	var sent struct {
		Command esphttp.FleetCommandEnvelope `json:"command"`
	}
	if err := json.Unmarshal([]byte(stdout), &sent); err != nil {
		t.Fatal(err)
	}
	detail, found, err := state.GetFleetCommandDetail(ctx, fleetID, sent.Command.CommandID)
	if err != nil || !found || detail.Command.Target.MemberID != *members[1].MemberID || detail.Command.IssuerMemberID != *members[0].MemberID {
		t.Fatalf("stored command: %+v found=%v err=%v", detail, found, err)
	}
	// Full-width but unknown identities must reach the real handler and fail
	// membership checks, rather than being truncated into an existing member.
	unknown := *members[1].MemberID
	unknown[31] ^= 1
	badArgs := append(append([]string{}, common...), "-target", "node", "-target-member-id", unknown.String(), "-action", esphttp.FleetCommandActionEcho, "-args-json", `{"message":"unknown"}`)
	code, _, _ = captureCommandOutput(t, func() int { return cmdFleetCommandsSend(gf, badArgs) })
	if code != exitTransport {
		t.Fatalf("unknown member accepted: exit=%d", code)
	}
	for _, args := range [][]string{
		{"assign", "-task", created.Task.TaskID, "-assignee-node-id", "7"},
		{"assign", "-task", created.Task.TaskID, "-assignee-member-id", "7"},
		{"assign", "-task", created.Task.TaskID, "-assignee-member-id", (entmoot.MemberID{}).String()},
		{"assign", "-task", created.Task.TaskID},
	} {
		code, _, _ := captureCommandOutput(t, func() int { return cmdFleetTasks(gf, append(args, common...)) })
		if code != exitInvalidArgument {
			t.Fatalf("invalid task args %v: exit=%d", args, code)
		}
	}
	for _, args := range [][]string{
		{"-target", "node", "-target-node-id", "7"},
		{"-target", "node", "-target-member-id", "7"},
		{"-target", "node"},
	} {
		code, _, _ := captureCommandOutput(t, func() int { return cmdFleetCommandsSend(gf, append(args, common...)) })
		if code != exitInvalidArgument {
			t.Fatalf("invalid command args %v: exit=%d", args, code)
		}
	}
}

// Keep the HTTP handler and durable state real; replace only its external
// publication boundary with a signed, queryable in-process message store.
type fleetCLIEventPublisher struct {
	identity *keystore.Identity
	author   entmoot.NodeInfo
	messages store.MessageStore
}

func (p fleetCLIEventPublisher) PublishTaskEvent(ctx context.Context, gid entmoot.GroupID, topics []string, content []byte) (esphttp.PublishResult, error) {
	message := entmoot.Message{Version: 2, GroupID: gid, Author: p.author, Topics: topics, Content: content, Timestamp: time.Now().UnixMilli()}
	message.ID = canonical.MessageID(message)
	payload, err := canonical.MessageSigningBytes(message)
	if err != nil {
		return esphttp.PublishResult{}, err
	}
	message.Signature = p.identity.Sign(payload)
	_, err = p.messages.Put(ctx, gid, message)
	return esphttp.PublishResult{Status: "published", GroupID: gid, MessageID: message.ID, AuthorMemberID: *p.author.MemberID, TimestampMS: message.Timestamp}, err
}
