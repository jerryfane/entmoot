package libp2ptransport

import (
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

// closeStaleWebRTCConns closes an older inbound webrtc-direct connection from
// a peer as soon as that peer dials in again over webrtc-direct.
//
// A browser that reloads comes back with the same key, so the same peer ID,
// while this node still holds the dead page's connection until WebRTC times
// it out (about 35 s). GossipSub keeps one outbound stream per peer: it keeps
// writing to the dead connection, and when that connection finally dies it
// clears every subscription the peer holds, including the SUBSCRIBE that came
// in on the new connection. The new page then gets no live messages. Closing
// the old connection at once moves that reset to the moment the page
// reconnects, before it has subscribed.
//
// Only webrtc-direct is touched: browsers reach the node only that way and a
// page holds one connection, while native peers may keep TCP and WebSocket
// connections side by side on purpose.
func closeStaleWebRTCConns(h host.Host) network.Notifiee {
	notifiee := &network.NotifyBundle{ConnectedF: func(n network.Network, fresh network.Conn) {
		if fresh.Stat().Direction != network.DirInbound || !isWebRTCDirect(fresh) {
			return
		}
		for _, old := range n.ConnsToPeer(fresh.RemotePeer()) {
			if old == fresh || old.Stat().Direction != network.DirInbound || !isWebRTCDirect(old) {
				continue
			}
			if old.Stat().Opened.After(fresh.Stat().Opened) {
				continue
			}
			// Close blocks on the transport; notifications must not.
			go func(conn network.Conn) { _ = conn.Close() }(old)
		}
	}}
	h.Network().Notify(notifiee)
	return notifiee
}

// isWebRTCDirect asks the transport: a webrtc-direct connection's remote
// address is plain /udp/<port>, without the webrtc-direct component.
func isWebRTCDirect(conn network.Conn) bool {
	return conn.ConnState().Transport == "webrtc-direct"
}
