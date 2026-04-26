package pilot

import (
	"context"
	"testing"

	"entmoot/pkg/entmoot"
)

func TestTransportSetPeerEndpointsSelfNoop(t *testing.T) {
	t.Parallel()

	const local entmoot.NodeID = 45491
	tr := &Transport{
		nodeID: local,
		closed: make(chan struct{}),
	}

	err := tr.SetPeerEndpoints(context.Background(), local, []entmoot.NodeEndpoint{
		{Network: "turn", Addr: "104.30.150.206:23549"},
	})
	if err != nil {
		t.Fatalf("SetPeerEndpoints(self): %v", err)
	}
}
