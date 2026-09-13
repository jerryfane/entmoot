package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"entmoot/pkg/entmoot/keystore"
	libp2ptransport "entmoot/pkg/entmoot/transport/libp2p"
)

const (
	defaultRelayIdentityPath = "~/.entmoot/relay-identity.json"
	defaultRelayListenAddr   = "/ip4/0.0.0.0/tcp/4001"
)

type relayServeConfig struct {
	identity              string
	allowNewIdentity      bool
	listenAddrs           stringListFlag
	announceAddrs         stringListFlag
	allowedPeers          stringListFlag
	reservationTTL        time.Duration
	circuitDuration       time.Duration
	circuitBytes          uint64
	maxReservations       uint
	maxCircuitsPerPeer    uint
	maxReservationsPerIP  uint
	maxReservationsPerASN uint
}

func cmdRelay(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: entmootd relay serve [flags]")
		return exitInvalidArgument
	}
	switch args[0] {
	case "serve":
		return cmdRelayServe(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "entmootd relay: unknown subcommand %q\n", args[0])
		return exitInvalidArgument
	}
}

func cmdRelayServe(args []string) int {
	cfg, code, ok := parseRelayServeConfig(args)
	if !ok {
		return code
	}

	allowedPeers := make([]peer.ID, 0, len(cfg.allowedPeers))
	for _, raw := range cfg.allowedPeers {
		id, err := peer.Decode(raw)
		if err != nil {
			slog.Error("relay serve: allowed peer", slog.String("peer_id", raw), slog.String("err", err.Error()))
			return exitInvalidArgument
		}
		allowedPeers = append(allowedPeers, id)
	}
	identity, err := loadRelayIdentity(cfg.identity, cfg.allowNewIdentity)
	if err != nil {
		slog.Error("relay serve: identity", slog.String("err", err.Error()))
		return exitInvalidArgument
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h, _, err := libp2ptransport.NewRelayServer(ctx, identity, libp2ptransport.RelayServerConfig{
		ListenAddrs:           cfg.listenAddrs,
		AnnounceAddrs:         cfg.announceAddrs,
		AllowedPeers:          allowedPeers,
		ReservationTTL:        cfg.reservationTTL,
		CircuitDuration:       cfg.circuitDuration,
		CircuitBytes:          int64(cfg.circuitBytes),
		MaxReservations:       int(cfg.maxReservations),
		MaxCircuitsPerPeer:    int(cfg.maxCircuitsPerPeer),
		MaxReservationsPerIP:  int(cfg.maxReservationsPerIP),
		MaxReservationsPerASN: int(cfg.maxReservationsPerASN),
	})
	if err != nil {
		slog.Error("relay serve: start", slog.String("err", err.Error()))
		return exitTransport
	}
	defer h.Close()

	addresses, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()})
	if err != nil {
		slog.Error("relay serve: addresses", slog.String("err", err.Error()))
		return exitTransport
	}
	addressStrings := make([]string, 0, len(addresses))
	for _, address := range addresses {
		addressStrings = append(addressStrings, address.String())
	}
	if code := printJSON(map[string]any{
		"event":                    "relay_ready",
		"peer_id":                  h.ID(),
		"addresses":                addressStrings,
		"allowed_peers":            len(allowedPeers),
		"reservation_ttl":          cfg.reservationTTL.String(),
		"circuit_duration":         cfg.circuitDuration.String(),
		"circuit_bytes":            cfg.circuitBytes,
		"max_reservations":         cfg.maxReservations,
		"max_circuits_per_peer":    cfg.maxCircuitsPerPeer,
		"max_reservations_per_ip":  cfg.maxReservationsPerIP,
		"max_reservations_per_asn": cfg.maxReservationsPerASN,
	}); code != exitOK {
		return code
	}
	<-ctx.Done()
	return exitOK
}

func parseRelayServeConfig(args []string) (relayServeConfig, int, bool) {
	fs := flag.NewFlagSet("relay serve", flag.ContinueOnError)
	cfg := relayServeConfig{}
	fs.StringVar(&cfg.identity, "identity", defaultRelayIdentityPath, "dedicated relay identity file")
	fs.BoolVar(&cfg.allowNewIdentity, "allow-new-identity", false, "create the relay identity when it is absent")
	fs.Var(&cfg.listenAddrs, "listen", "libp2p listen multiaddr; repeatable")
	fs.Var(&cfg.announceAddrs, "announce", "public libp2p multiaddr without /p2p; repeatable")
	fs.Var(&cfg.allowedPeers, "allow-peer", "libp2p PeerID allowed to reserve and use circuits; repeatable")
	fs.DurationVar(&cfg.reservationTTL, "reservation-ttl", time.Hour, "relay reservation lifetime")
	fs.DurationVar(&cfg.circuitDuration, "circuit-duration", 15*time.Minute, "maximum circuit lifetime")
	fs.Uint64Var(&cfg.circuitBytes, "circuit-bytes", 64<<20, "maximum bytes relayed in each direction per circuit")
	fs.UintVar(&cfg.maxReservations, "max-reservations", 128, "maximum active reservations")
	fs.UintVar(&cfg.maxCircuitsPerPeer, "max-circuits-per-peer", 16, "maximum active circuits per peer")
	fs.UintVar(&cfg.maxReservationsPerIP, "max-reservations-per-ip", 8, "maximum reservations per source IP")
	fs.UintVar(&cfg.maxReservationsPerASN, "max-reservations-per-asn", 32, "maximum reservations per source ASN")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return relayServeConfig{}, exitOK, false
		}
		return relayServeConfig{}, exitInvalidArgument, false
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "relay serve: positional arguments are not supported")
		return relayServeConfig{}, exitInvalidArgument, false
	}
	if len(cfg.listenAddrs) == 0 {
		cfg.listenAddrs = stringListFlag{defaultRelayListenAddr}
	}
	if len(cfg.allowedPeers) == 0 {
		fmt.Fprintln(os.Stderr, "relay serve: at least one -allow-peer is required")
		return relayServeConfig{}, exitInvalidArgument, false
	}
	if cfg.reservationTTL <= 0 || cfg.circuitDuration <= 0 || cfg.circuitBytes == 0 || cfg.circuitBytes > math.MaxInt64 ||
		cfg.maxReservations == 0 || cfg.maxReservations > math.MaxInt ||
		cfg.maxCircuitsPerPeer == 0 || cfg.maxCircuitsPerPeer > math.MaxInt ||
		cfg.maxReservationsPerIP == 0 || cfg.maxReservationsPerIP > math.MaxInt ||
		cfg.maxReservationsPerASN == 0 || cfg.maxReservationsPerASN > math.MaxInt {
		fmt.Fprintln(os.Stderr, "relay serve: every resource limit must be positive and fit the platform")
		return relayServeConfig{}, exitInvalidArgument, false
	}
	expanded, err := expandHome(cfg.identity)
	if err != nil {
		fmt.Fprintf(os.Stderr, "relay serve: %v\n", err)
		return relayServeConfig{}, exitInvalidArgument, false
	}
	cfg.identity = expanded
	return cfg, exitOK, true
}

func loadRelayIdentity(path string, allowNew bool) (*keystore.Identity, error) {
	identity, err := keystore.Load(path)
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) || !allowNew {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w; pass -allow-new-identity for first launch", err)
		}
		return nil, err
	}
	return keystore.LoadOrGenerate(path)
}
