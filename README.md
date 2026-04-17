# Entmoot

A Layer-2 group-communication protocol for AI agents, built on top of
[Pilot Protocol](https://github.com/TeoSlayer/pilotprotocol).

Pilot provides pairwise Ed25519 identity, AES-256-GCM encrypted tunnels,
NAT traversal, reliable UDP transport, and a peer-trust system — but no
group primitives. Entmoot adds what's missing: multi-party gossip,
topic-based pub/sub, Merkle-verified message completeness, and signed
group membership.

Named after the Ents' council in *The Lord of the Rings* — distributed
consensus, no central authority, "don't be hasty" about cryptographic
verification.

## Status

**v0, experimental.** The core protocol works end-to-end. The design is
pinned in [`ARCHITECTURE.md`](./ARCHITECTURE.md); v0 deliberately ships a
subset (see [Deferred from v0](#deferred-from-v0)).

What works today:

- Signed append-only membership roster (founder-only admin in v0).
- Push-only gossip over Pilot unicast streams, with random peer sampling
  at configurable fan-out.
- Content-addressable message store with per-group Merkle trees.
- Selective subscriber filters using MQTT-style topic patterns
  (`foo/bar`, `foo/+/baz`, `foo/#`).
- Three-tier bootstrap from a signed invite bundle
  (invite peers → Pilot-trusted peers ∩ roster → founder fallback).
- Per-peer token-bucket rate limiting (message rate + byte rate) with
  replay protection (5 min / 30 s window + sha256 dedupe).
- End-to-end integration proven by a canary test: three peers join,
  exchange three messages, the third peer Merkle-verifies completeness.

## Quick start

### Prerequisites

- Go ≥ 1.25.3 (tested on 1.26.2).
- macOS or Linux. NAT traversal needs an outbound internet connection
  because Pilot relies on a public rendezvous.

### Build

All Go sources live under `src/`. Build from there.

```sh
# Entmoot itself
cd src && go build ./...

# Pilot binaries (for running daemons locally, referenced by the canary)
cd repos/pilotprotocol && make build && cd -
```

### Run the fast canary (no network)

```sh
cd src && go test -race -short ./...
```

This runs the in-memory canary: three gossipers join a group over a mock
transport, publish three messages, and verify Merkle convergence.
Finishes in a couple of seconds.

### Run the full canary against real Pilot daemons

```sh
cd src && go test -run TestCanaryPilot -timeout 120s ./test/canary/...
```

Spawns three sandboxed Pilot daemons as subprocesses, establishes trust,
runs the same canary over real encrypted tunnels. Takes around 12 seconds.
Skipped under `-short` or when `ENTMOOT_SKIP_PILOT` is set.

### Use the binary

`entmootd` is the single binary that talks to a local Pilot daemon.

```sh
entmootd run                                   # long-running node
entmootd group create -name demo               # print a new group id
entmootd invite create -group <GID>            # signed invite JSON on stdout
entmootd join -invite invite.json              # join a group from an invite
entmootd publish -group <GID> -topic t -content "hi"
entmootd info                                  # node id, pubkey, groups
```

Global flags: `-socket`, `-identity`, `-data`, `-listen-port`, `-log-level`.
Defaults write to `~/.entmoot/`.

## Repository layout

```
entmoot/
├── ARCHITECTURE.md            # authoritative design document
├── docs/                      # design docs (CLI spec, etc.)
├── paper/                     # LaTeX paper sources
├── repos/                     # (gitignored) vendored reference repos
├── notes/                     # (gitignored) scratch notes
└── src/                       # Go module
    ├── go.mod                 # requires Pilot via local ../repos/pilotprotocol
    ├── cmd/entmootd/          # CLI binary
    ├── pkg/entmoot/           # library packages
    │   ├── canonical/         # deterministic JSON encoding for hashing/signing
    │   ├── clock/             # injectable clock (System + Fake)
    │   ├── gossip/            # push-only epidemic + bootstrap + transport iface
    │   ├── keystore/          # Ed25519 identity persistence
    │   ├── merkle/            # domain-separated Merkle tree + inclusion proofs
    │   ├── order/             # topological order over message DAG
    │   ├── ratelimit/         # per-peer token buckets
    │   ├── roster/            # signed membership log
    │   ├── store/             # message store (Memory + JSONL)
    │   ├── topic/             # MQTT-style pattern matcher
    │   ├── transport/pilot/   # Pilot `pkg/driver` adapter for gossip
    │   └── wire/              # framing, codec, replay check, rate check
    └── test/canary/           # end-to-end tests
```

## How it works (one paragraph)

A group is 32 random bytes of identity plus a signed append-only roster.
Messages are author-signed, reference up to three parents to form a DAG,
and carry a list of MQTT-style topics. `entmootd` on each member opens a
listener on port 1004 through its local Pilot daemon. When you publish, we
sign the message, store it, and push its id (just the hash) to a random
sample of roster peers. Peers that don't have the body fetch it via a
separate connection, verifying the signature against the roster's entry
for the author. Every peer maintains a Merkle tree over the messages it
holds in topological order; two peers with the same set produce the same
root, so convergence is checkable in constant space.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full spec — data model,
wire format, bootstrap flow, security posture, and the resolved-for-v0
decisions table.

## Deferred from v0

Tracked explicitly; each has a documented upgrade path in `ARCHITECTURE.md`.

- Push-pull gossip (v0 is push-only).
- Near-peer sampling (v0 is random-only).
- Cooldown / backoff on rate-limit breach (v0 hard-disconnects).
- `announce_group` wire message (invites cover group discovery in v0).
- Merkle absence proofs for filtered views (positive inclusion only in v0).
- DHT-assigned keepers.
- Bridge to Pilot's EventStream for legacy consumers.
- Group encryption (messages are plaintext + author-signed; transport is
  still encrypted by Pilot pairwise).
- Multi-admin / quorum rosters (v0 is founder-only).
- Key rotation.
- SQLite / Bolt persistence (v0 uses JSONL).
- Python SDK (v0 is Go-only).

## Running Entmoot against the live Pilot network

The `entmootd` binary needs a running Pilot daemon to talk to. The Pilot
daemon, in turn, needs to reach Pilot's public registry at `34.71.57.205`
to obtain its 48-bit address — there is no fully-offline mode unless you
also run your own registry and rendezvous.

For local experimentation, run a sandboxed Pilot daemon that does not
collide with any system install:

```sh
mkdir -p ~/.pilot-sandbox
./repos/pilotprotocol/bin/daemon \
  -socket /tmp/pilot-sandbox.sock \
  -identity ~/.pilot-sandbox/identity.json \
  -email you@example.com \
  -listen :0 \
  > ~/.pilot-sandbox/daemon.log 2>&1 &

entmootd -socket /tmp/pilot-sandbox.sock info
```

Two daemons want to handshake? At least one must be launched with
`-public`; otherwise the private-endpoint rules in Pilot's registry
prevent contact.

## Contributing

Early days. The architecture doc is the starting point. Tests must pass
under `-race` before a change can land; `go vet` must be clean. New
packages ship with at least one happy-path and one failure-path test.

## License

Not yet chosen.
