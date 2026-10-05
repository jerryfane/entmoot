package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"syscall"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
)

// defaultListenPort is the -listen-port default. It is below 1024, so only
// root (or CAP_NET_BIND_SERVICE) can bind it.
const defaultListenPort = 1004

// directListenAddr is the TCP listener a direct-mode host gets when no
// -p2p-listen replaces it. An explicit -listen-port is used as given, so a
// failure to bind it stays a hard error. The default port is privileged, so a
// non-root agent cannot bind it, and a second daemon on the host finds it
// taken; then the default falls back to a port the OS assigns. That serves an
// outbound-only agent, and join/serve/info report the port actually bound.
//
// libp2p flattens listen errors into text, so the default port is probed
// with a plain listener first. That also refuses to share the port with a
// daemon that bound it with SO_REUSEPORT, which libp2p would otherwise do.
func directListenAddr(gf *globalFlags) string {
	configured := fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", gf.listenPort)
	if gf.listenPortSet {
		return configured
	}
	probe, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", gf.listenPort))
	if err == nil {
		_ = probe.Close()
		return configured
	}
	if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EPERM) && !errors.Is(err, syscall.EADDRINUSE) {
		// Not a condition a different port fixes; let the host report it.
		return configured
	}
	slog.Info("daemon: default listen port unavailable; using an OS-assigned port (pass -listen-port to pin one)",
		slog.Uint64("port", uint64(gf.listenPort)), slog.String("err", err.Error()))
	return "/ip4/0.0.0.0/tcp/0"
}

// reportedListenPort is the listen_port join, serve and info report. For the
// default TCP listener it is the port the host bound, which differs from
// -listen-port after a fallback. A -p2p-listen or relay-only host reports the
// configured port, as before.
func reportedListenPort(gf *globalFlags, h host.Host) uint16 {
	if len(gf.p2pListen) == 0 && (gf.connectivity == "" || gf.connectivity == "direct") {
		for _, address := range h.Network().ListenAddresses() {
			value, err := address.ValueForProtocol(multiaddr.P_TCP)
			if err != nil {
				continue
			}
			if port, err := strconv.ParseUint(value, 10, 16); err == nil && port != 0 {
				return uint16(port)
			}
		}
	}
	return uint16(gf.listenPort)
}
