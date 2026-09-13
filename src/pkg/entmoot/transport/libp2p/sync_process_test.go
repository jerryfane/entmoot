package libp2ptransport

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/merkle"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/signing"
	"entmoot/pkg/entmoot/store"
)

type processSyncAddress struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

func TestHistorySyncAcrossSeparateProcessWithoutPilot(t *testing.T) {
	clientIdentity := mustIdentity(t)
	command := exec.Command(os.Args[0], "-test.run=^TestHistorySyncSeparateProcessServer$", "-test.v=false")
	command.Env = append(os.Environ(),
		"ENTMOOT_SYNC_HELPER=1",
		"ENTMOOT_SYNC_CLIENT_KEY="+base64.StdEncoding.EncodeToString(clientIdentity.PublicKey),
		"ENTMOOT_SYNC_DATA="+t.TempDir(),
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdin.Close()
		_ = command.Wait()
	}()
	var address processSyncAddress
	if err := json.NewDecoder(bufio.NewReader(stdout)).Decode(&address); err != nil {
		body, _ := io.ReadAll(stderr)
		t.Fatalf("read helper address: %v; stderr=%s", err, body)
	}
	remoteID, err := peer.Decode(address.PeerID)
	if err != nil {
		t.Fatal(err)
	}
	remote := peer.AddrInfo{ID: remoteID}
	for _, value := range address.Addrs {
		parsed, err := multiaddr.NewMultiaddr(value)
		if err != nil {
			t.Fatal(err)
		}
		remote.Addrs = append(remote.Addrs, parsed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	clientHost, _, err := NewHost(ctx, clientIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer clientHost.Close()
	destination, err := store.OpenSQLite(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	groupID := processSyncGroupID()
	progress := SyncFromKeepers(ctx, clientHost, groupID, []peer.AddrInfo{remote}, destination, func(message entmoot.Message, _ *merkle.Proof) error {
		return signing.VerifyMessage(message, message.Author)
	}, &HistorySyncState{})
	if len(progress) != 1 || progress[0].Err != nil || progress[0].Inserted != 12 {
		body, _ := io.ReadAll(stderr)
		t.Fatalf("separate-process progress=%+v stderr=%s", progress, body)
	}
}

func TestHistorySyncSeparateProcessServer(t *testing.T) {
	if os.Getenv("ENTMOOT_SYNC_HELPER") != "1" {
		t.Skip("helper process")
	}
	clientPublicKey, err := base64.StdEncoding.DecodeString(os.Getenv("ENTMOOT_SYNC_CLIENT_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	serverIdentity := mustIdentity(t)
	ctx := context.Background()
	serverHost, _, err := NewHost(ctx, serverIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer serverHost.Close()
	groupID := processSyncGroupID()
	rosterLog := roster.New(groupID)
	founder := mustNodeInfo(t, serverIdentity.PublicKey)
	if err := rosterLog.Genesis(serverIdentity, founder, 1_000); err != nil {
		t.Fatal(err)
	}
	entry, err := rosterLog.SignEntry(serverIdentity, "add", mustNodeInfo(t, clientPublicKey), nil, 2_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(entry); err != nil {
		t.Fatal(err)
	}
	source, err := store.OpenSQLite(os.Getenv("ENTMOOT_SYNC_DATA"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	signer, err := signing.NewLocalSigner(founder, serverIdentity)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		head := rosterLog.Head()
		message, err := signer.SignMessage(ctx, entmoot.Message{Version: 2, GroupID: groupID, Timestamp: int64(3_000 + i), Topics: []string{"process"}, Content: []byte(fmt.Sprintf("process-%02d", i)), RosterHead: &head})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Put(ctx, groupID, message); err != nil {
			t.Fatal(err)
		}
	}
	server := SyncServer{
		Host:      serverHost,
		Admission: NewBootstrapAdmission(),
		Roster: func(want entmoot.GroupID) (*roster.RosterLog, bool) {
			return rosterLog, want == groupID
		},
		Store: source,
	}
	if err := server.Install(); err != nil {
		t.Fatal(err)
	}
	address := processSyncAddress{PeerID: serverHost.ID().String()}
	for _, value := range serverHost.Addrs() {
		address.Addrs = append(address.Addrs, value.String())
	}
	if err := json.NewEncoder(os.Stdout).Encode(address); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func processSyncGroupID() entmoot.GroupID {
	var groupID entmoot.GroupID
	groupID[0] = 0x51
	return groupID
}
