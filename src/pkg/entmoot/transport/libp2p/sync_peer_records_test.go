package libp2ptransport

import (
	"context"
	"strings"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/record"
	multiaddr "github.com/multiformats/go-multiaddr"

	"entmoot/pkg/entmoot"
	"entmoot/pkg/entmoot/keystore"
	"entmoot/pkg/entmoot/roster"
	"entmoot/pkg/entmoot/store"
)

// A member reaches a peer it cannot dial only if some other member forwards
// that peer's signed record. This is the whole point of the exchange: without
// forwarding, two members behind NAT never learn each other's circuit address.
func TestPeerRecordsForwardVerifiedMemberAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	thirdIdentity := mustIdentity(t)
	strangerIdentity := mustIdentity(t)
	founderBinding, err := BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	groupID, rosterLog := syncRoster(t, founderIdentity, founderBinding.MemberID, memberIdentity, memberBinding.MemberID)
	entry, err := rosterLog.SignEntry(founderIdentity, "add", mustNodeInfo(t, thirdIdentity.PublicKey), nil, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(entry); err != nil {
		t.Fatal(err)
	}
	rosterFor := func(want entmoot.GroupID) (*roster.RosterLog, bool) {
		return rosterLog, want == groupID
	}

	founderHost, _, err := NewHost(ctx, founderIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer founderHost.Close()
	founderCache := NewPeerRecordCache()
	founderServer := SyncServer{
		Host: founderHost, Admission: NewBootstrapAdmission(), Roster: rosterFor, Store: store.NewMemory(),
		PeerRecords: func(want entmoot.GroupID) (*PeerRecordCache, bool) {
			return founderCache, want == groupID
		},
	}
	if err := founderServer.Install(); err != nil {
		t.Fatal(err)
	}
	founderInfo := peer.AddrInfo{ID: founderHost.ID(), Addrs: founderHost.Addrs()}

	// The member is reachable by the founder but never by the third member,
	// which stands in for two peers behind different NATs.
	memberHost, _, err := NewHost(ctx, memberIdentity, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer memberHost.Close()
	memberServer := SyncServer{
		Host: memberHost, Admission: NewBootstrapAdmission(), Roster: rosterFor, Store: store.NewMemory(),
	}
	if err := memberServer.Install(); err != nil {
		t.Fatal(err)
	}
	memberInfo := peer.AddrInfo{ID: memberHost.ID(), Addrs: memberHost.Addrs()}
	waitForCertifiedRecord(ctx, t, memberHost, memberHost.ID())

	strangerHost, _, err := NewHost(ctx, strangerIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer strangerHost.Close()
	if _, err := RequestPeerRecords(ctx, strangerHost, founderInfo, groupID); err == nil ||
		!strings.Contains(err.Error(), string(SyncUnauthorized)) {
		t.Fatalf("non-member peer record request error = %v", err)
	}

	thirdHost, _, err := NewHost(ctx, thirdIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer thirdHost.Close()
	beforeForwarding, err := RequestPeerRecords(ctx, thirdHost, founderInfo, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if recordsCoverPeer(t, beforeForwarding, memberHost.ID()) {
		t.Fatal("founder forwarded a member record it had never verified")
	}

	fromMember, err := RequestPeerRecords(ctx, founderHost, memberInfo, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if installed := InstallPeerRecords(founderHost, rosterLog, fromMember, DirectConnectivity, nil, founderCache); installed != 1 {
		t.Fatalf("founder installed %d member records, want 1", installed)
	}

	forwarded, err := RequestPeerRecords(ctx, thirdHost, founderInfo, groupID)
	if err != nil {
		t.Fatal(err)
	}
	if !recordsCoverPeer(t, forwarded, memberHost.ID()) {
		t.Fatalf("member record was not forwarded among %d records", len(forwarded))
	}
	thirdCache := NewPeerRecordCache()
	if installed := InstallPeerRecords(thirdHost, rosterLog, forwarded, DirectConnectivity, nil, thirdCache); installed == 0 {
		t.Fatal("no forwarded record installed")
	}
	if len(thirdHost.Peerstore().Addrs(memberHost.ID())) == 0 {
		t.Fatal("forwarded member addresses did not reach the peerstore")
	}

	tampered := append([]byte(nil), forwarded[len(forwarded)-1]...)
	tampered[len(tampered)-1] ^= 0xff
	if installed := InstallPeerRecords(thirdHost, rosterLog, [][]byte{tampered}, DirectConnectivity, nil, thirdCache); installed != 0 {
		t.Fatalf("tampered record installed %d addresses", installed)
	}

	strangerRecord := sealPeerRecord(t, strangerIdentity, multiaddr.StringCast("/ip4/203.0.113.7/tcp/4001"), 9)
	if installed := InstallPeerRecords(thirdHost, rosterLog, [][]byte{strangerRecord}, DirectConnectivity, nil, thirdCache); installed != 0 {
		t.Fatalf("non-member record installed %d addresses", installed)
	}
}

// A record from an abandoned network must not survive a newer one, or a member
// keeps dialling an address its peer no longer holds.
func TestPeerRecordInstallDropsStaleAddresses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	founderBinding, err := BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, rosterLog := syncRoster(t, founderIdentity, founderBinding.MemberID, memberIdentity, memberBinding.MemberID)
	localHost, _, err := NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()

	current := multiaddr.StringCast("/ip4/198.51.100.11/tcp/4001")
	abandoned := multiaddr.StringCast("/ip4/198.51.100.12/tcp/4001")
	cache := NewPeerRecordCache()
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{
		sealPeerRecord(t, memberIdentity, current, 5),
		sealPeerRecord(t, memberIdentity, abandoned, 4),
	}, DirectConnectivity, nil, cache); installed != 1 {
		t.Fatalf("installed=%d, want only the newest record", installed)
	}
	addresses := localHost.Peerstore().Addrs(memberBinding.PeerID)
	if len(addresses) != 1 || !addresses[0].Equal(current) {
		t.Fatalf("peerstore kept stale addresses: %v", addresses)
	}
	if existing, tracked := cache.current(memberBinding.PeerID); !tracked || existing.seq != 5 {
		t.Fatalf("cached sequence = %d tracked=%t, want 5", existing.seq, tracked)
	}
	if len(cache.Snapshot(memberBinding.PeerID)) != 0 {
		t.Fatal("snapshot returned the requester's own record")
	}
	if len(cache.Snapshot(localHost.ID())) != 1 {
		t.Fatalf("snapshot = %d records, want the member record", len(cache.Snapshot(localHost.ID())))
	}
}

// A relay-only member must never accept or forward a direct address.
func TestPeerRecordInstallKeepsRelayOnlyProfilePrivate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	founderBinding, err := BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, rosterLog := syncRoster(t, founderIdentity, founderBinding.MemberID, memberIdentity, memberBinding.MemberID)
	relayIdentity := mustIdentity(t)
	relayBinding, err := BindingFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	relays := []peer.AddrInfo{{ID: relayBinding.PeerID, Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/203.0.113.9/tcp/4001")}}}
	localHost, _, err := NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()

	direct := sealPeerRecord(t, memberIdentity, multiaddr.StringCast("/ip4/198.51.100.30/tcp/4001"), 3)
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{direct},
		RelayOnlyConnectivity, relays, NewPeerRecordCache()); installed != 0 {
		t.Fatal("relay-only member installed a direct address")
	}
	circuit := sealPeerRecord(t, memberIdentity,
		multiaddr.StringCast("/ip4/203.0.113.9/tcp/4001/p2p/"+relayBinding.PeerID.String()+"/p2p-circuit"), 4)
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{circuit},
		RelayOnlyConnectivity, relays, NewPeerRecordCache()); installed != 1 {
		t.Fatal("relay-only member rejected an approved circuit address")
	}
}

// An update this node cannot use must not cost it a working address: losing the
// only dialable address would make a reachable member unreachable until the
// subject happens to publish again.
func TestPeerRecordInstallKeepsWorkingAddressWhenUpdateIsUnusable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	founderBinding, err := BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, rosterLog := syncRoster(t, founderIdentity, founderBinding.MemberID, memberIdentity, memberBinding.MemberID)
	relayIdentity := mustIdentity(t)
	relayBinding, err := BindingFromPublicKey(relayIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	relays := []peer.AddrInfo{{ID: relayBinding.PeerID, Addrs: []multiaddr.Multiaddr{multiaddr.StringCast("/ip4/203.0.113.9/tcp/4001")}}}
	localHost, _, err := NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()

	circuit := multiaddr.StringCast("/ip4/203.0.113.9/tcp/4001/p2p/" + relayBinding.PeerID.String() + "/p2p-circuit")
	cache := NewPeerRecordCache()
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{sealPeerRecord(t, memberIdentity, circuit, 5)},
		RelayOnlyConnectivity, relays, cache); installed != 1 {
		t.Fatal("approved circuit address was not installed")
	}
	// Newer, authentic, and useless to a relay-only member.
	direct := sealPeerRecord(t, memberIdentity, multiaddr.StringCast("/ip4/198.51.100.30/tcp/4001"), 6)
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{direct},
		RelayOnlyConnectivity, relays, cache); installed != 0 {
		t.Fatal("relay-only member installed a direct address")
	}
	addresses := localHost.Peerstore().Addrs(memberBinding.PeerID)
	if len(addresses) != 1 || !addresses[0].Equal(circuit) {
		t.Fatalf("working circuit address lost to an unusable update: %v", addresses)
	}
	existing, tracked := cache.current(memberBinding.PeerID)
	if !tracked || existing.seq != 6 {
		t.Fatalf("freshness floor = %d tracked=%t, want 6", existing.seq, tracked)
	}
	// The floor must now reject the superseded record instead of reinstalling it.
	if installed := InstallPeerRecords(localHost, rosterLog, [][]byte{sealPeerRecord(t, memberIdentity, circuit, 5)},
		RelayOnlyConnectivity, relays, cache); installed != 0 {
		t.Fatal("superseded record was installed again")
	}
}

// A member removed from the roster must stop being forwarded.
func TestPeerRecordCacheDropsFormerMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	founderIdentity := mustIdentity(t)
	memberIdentity := mustIdentity(t)
	founderBinding, err := BindingFromPublicKey(founderIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	memberBinding, err := BindingFromPublicKey(memberIdentity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	_, rosterLog := syncRoster(t, founderIdentity, founderBinding.MemberID, memberIdentity, memberBinding.MemberID)
	localHost, _, err := NewHost(ctx, founderIdentity, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatal(err)
	}
	defer localHost.Close()
	cache := NewPeerRecordCache()
	if installed := InstallPeerRecords(localHost, rosterLog,
		[][]byte{sealPeerRecord(t, memberIdentity, multiaddr.StringCast("/ip4/198.51.100.40/tcp/4001"), 2)},
		DirectConnectivity, nil, cache); installed != 1 {
		t.Fatal("member record was not installed")
	}
	removal, err := rosterLog.SignEntry(founderIdentity, "remove", mustNodeInfo(t, memberIdentity.PublicKey), nil, 4_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := rosterLog.Apply(removal); err != nil {
		t.Fatal(err)
	}
	InstallPeerRecords(localHost, rosterLog, nil, DirectConnectivity, nil, cache)
	if cache.Len() != 0 {
		t.Fatalf("cache still forwards %d removed member records", cache.Len())
	}
}

func waitForCertifiedRecord(ctx context.Context, t *testing.T, h host.Host, subject peer.ID) {
	t.Helper()
	certified, ok := peerstore.GetCertifiedAddrBook(h.Peerstore())
	if !ok {
		t.Fatal("host peerstore does not keep certified records")
	}
	for {
		if certified.GetPeerRecord(subject) != nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no signed peer record for %s", subject)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func recordsCoverPeer(t *testing.T, records [][]byte, subject peer.ID) bool {
	t.Helper()
	for _, payload := range records {
		_, value, err := record.ConsumeEnvelope(payload, peer.PeerRecordEnvelopeDomain)
		if err != nil {
			t.Fatalf("served record failed verification: %v", err)
		}
		if peerRecord, ok := value.(*peer.PeerRecord); ok && peerRecord.PeerID == subject {
			return true
		}
	}
	return false
}

func sealPeerRecord(t *testing.T, identity *keystore.Identity, address multiaddr.Multiaddr, seq uint64) []byte {
	t.Helper()
	privateKey, err := libp2pcrypto.UnmarshalEd25519PrivateKey(identity.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := BindingFromPublicKey(identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := record.Seal(&peer.PeerRecord{
		PeerID: binding.PeerID,
		Addrs:  []multiaddr.Multiaddr{address},
		Seq:    seq,
	}, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := envelope.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
