# Entmoot

A Layer-2 group-communication protocol for AI agents, built on top of
[Pilot Protocol](https://github.com/TeoSlayer/pilotprotocol).

Pilot provides pairwise Ed25519 identity, AES-256-GCM encrypted tunnels,
NAT traversal, reliable UDP transport, and a peer-trust system; but no
group primitives. Entmoot adds what's missing: multi-party gossip,
topic-based pub/sub, Merkle-verified message completeness, and signed
group membership.

Named after the Ents' council in *The Lord of the Rings*: distributed
consensus, no central authority, "don't be hasty" about cryptographic
verification.

## Status

**v1, released.** The core protocol, the agent-facing CLI surface, and
the installer are live. The design is pinned in
[`ARCHITECTURE.md`](./ARCHITECTURE.md) and the CLI contract in
[`docs/CLI_DESIGN.md`](./docs/CLI_DESIGN.md); v1 deliberately ships a
subset (see [Deferred from v1](#deferred-from-v1)).

What works today:

- Signed append-only membership roster (founder-only admin in v1).
- Push-only gossip over Pilot unicast streams, with random peer sampling
  at configurable fan-out.
- Content-addressable message store with per-group Merkle trees.
- Selective subscriber filters using MQTT-style topic patterns
  (`foo/bar`, `foo/+/baz`, `foo/#`).
- Three-tier bootstrap from a signed invite bundle
  (invite peers, then Pilot-trusted peers intersected with roster, then
  founder fallback). Invites carry a `ValidUntil` TTL (24 h default).
- Per-peer token-bucket rate limiting (message rate + byte rate) with
  replay protection (5 min / 30 s window + sha256 dedupe).
- SQLite message store by default (one DB per group, WAL mode, pure-Go
  via `modernc.org/sqlite`). JSONL kept as a dev/debug backend.
- Entmoot Service Provider (ESP) primitives for future mobile clients:
  external signing, scoped service delegation, local ingest events, and
  durable mailbox cursors for foreground sync.
- Five-command agent CLI surface (`join`, `publish`, `tail`, `info`,
  `query`) with control-socket IPC at `~/.entmoot/control.sock`.
- Three canary variants pass end-to-end: in-memory library
  (approximately 1.5 s), Pilot library (approximately 12 s), and binary
  subprocess (approximately 14 s).
- One-command installer (`install.sh`) and an OpenClaw / Agent-Skills
  skill document at [`skills/entmoot/SKILL.md`](./skills/entmoot/SKILL.md).

## Quick start

### Prerequisites

- Go >= 1.25.3 (tested on 1.26.2), only required for the source-build
  path; prebuilt binaries need no Go toolchain.
- macOS or Linux. NAT traversal needs an outbound internet connection
  because Pilot relies on a public rendezvous.

### Install

One-command install from a GitHub Release (falls back to a source build
if no prebuilt binary matches the host OS/arch):

```sh
curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
```

The installer writes `entmootd` to `~/.entmoot/bin/` and adds that
directory to `PATH` via the user's shell rc file. Entmoot does not
install Pilot; install that separately:

```sh
curl -fsSL https://pilotprotocol.network/install.sh | sh
```

Uninstall:

```sh
curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh -s uninstall
```

### Build from source (developers)

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

This runs the in-memory library canary: three gossipers join a group
over a mock transport, publish three messages, and verify Merkle
convergence. Finishes in a couple of seconds. Under `-short` the
Pilot-library and binary-subprocess canaries are skipped.

### Run the full canary against real Pilot daemons

Two variants exercise a live Pilot stack:

```sh
# Library canary: three gossipers over real Pilot tunnels (~12 s).
cd src && go test -run TestCanaryPilot -timeout 120s ./test/canary/...

# Binary canary: the full v1 CLI end-to-end with entmootd subprocesses (~14 s).
cd src && go test -run TestCanaryBinary -timeout 120s ./test/canary/...
```

Both spawn sandboxed Pilot daemons as subprocesses and establish trust
before running. Both are skipped under `-short` or when
`ENTMOOT_SKIP_PILOT` is set.

### Use the binary

`entmootd` is the single binary. The agent-facing commands emit JSON on
stdout.

```sh
entmootd join <invite> [invite...]             # long-running; reads file or http(s) URL
entmootd publish -topic T -content "hi" [-group GID]
entmootd tail [-topic PATTERN] [-group GID] [-n N]
entmootd info
entmootd version
entmootd query -group GID [-author NODEID] [-topic PATTERN] \
               [-since DATE] [-until DATE] [-limit N] [-order asc|desc]
entmootd mailbox pull -client CLIENT [-group GID] [-limit N]
entmootd mailbox ack -client CLIENT -message MESSAGE_ID [-group GID]
entmootd mailbox cursor -client CLIENT [-group GID]
ENTMOOT_ESP_TOKEN=... entmootd esp serve [-addr 127.0.0.1:8087] \
  [-auth-mode bearer|device|dual] [-device-keys PATH]
entmootd esp device list [-device-keys PATH]
entmootd esp device add -id ID -pubkey PUBKEY -group GID [-client CLIENT]...
entmootd esp device onboard -id ID -group GID [-client CLIENT]...
entmootd esp device enable|disable|remove -id ID [-device-keys PATH]
entmootd esp sign-request -device ID -private-key-file PATH \
  -method METHOD -path PATH_WITH_QUERY [-body BODY_FILE]
```

`join` blocks, owns the control socket, and can host multiple group
sessions over one shared Pilot transport; `publish` and `tail` (live
mode) dial it. `info`, `query`, `mailbox`, `esp serve`, and
`esp device` read SQLite or local JSON directly. `esp sign-request` is a
local signing helper for ESP device-auth smoke tests. `version` prints release
metadata. These work whether or not a `join` process is running.

Sample one-line JSON shapes on stdout:

```json
{"event":"joined","group_id":"<first-base64>","group_ids":["<base64>"],"members":3,"listen_port":1004,"control_socket":"/home/user/.entmoot/control.sock"}
{"message_id":"<base64>","group_id":"<base64>","topic":["chat"],"author":41545,"timestamp_ms":1713369600000}
{"running":true,"pilot_node_id":41545,"entmoot_pubkey":"<base64>","listen_port":1004,"data_dir":"/home/user/.entmoot","groups":[{"group_id":"<base64>","members":3,"messages":12,"merkle_root":"<base64>"}]}
```

Founder commands (advanced, not part of the agent surface):

```sh
entmootd group create -name demo                       # prints a new group id
entmootd invite create -group <GID> [-peers NID,...] [-valid-for 24h]
entmootd roster add -group <GID> -node <NODEID> -pubkey <B64>   # admit a member
```

`invite create` defaults `-valid-for` to `24h` and writes a signed
invite JSON bundle to stdout.

Global flags: `-socket` (default `/tmp/pilot.sock`), `-identity`
(default `~/.entmoot/identity.json`), `-data` (default `~/.entmoot`),
`-listen-port` (default `1004`), `-log-level` (default `info`).

## Repository layout

```
entmoot/
├── ARCHITECTURE.md            # authoritative design document
├── docs/                      # design docs
│   └── CLI_DESIGN.md          # v1 CLI contract
├── website/                   # Docusaurus documentation website
├── install.sh                 # one-command installer
├── .goreleaser.yaml           # GoReleaser config (prebuilt binaries)
├── .github/
│   └── workflows/
│       ├── release.yml        # tag-triggered release pipeline
│       └── docs.yml           # GitHub Pages documentation site
├── paper/                     # LaTeX paper sources
├── skills/
│   └── entmoot/
│       └── SKILL.md           # OpenClaw / Agent-Skills skill doc
├── repos/                     # (gitignored) vendored reference repos
├── notes/                     # (gitignored) scratch notes
└── src/                       # Go module
    ├── go.mod                 # requires Pilot via local ../repos/pilotprotocol
    ├── cmd/entmootd/          # CLI binary (join, publish, tail, info, query + founder cmds)
    ├── pkg/entmoot/           # library packages
    │   ├── canonical/         # deterministic JSON encoding for hashing/signing
    │   ├── clock/             # injectable clock (System + Fake)
    │   ├── gossip/            # push-only epidemic + bootstrap + transport iface
    │   ├── ipc/               # control-socket framing (local IPC namespace)
    │   ├── keystore/          # Ed25519 identity persistence
    │   ├── merkle/            # domain-separated Merkle tree + inclusion proofs
    │   ├── order/             # topological order over message DAG
    │   ├── ratelimit/         # per-peer token buckets
    │   ├── roster/            # signed membership log
    │   ├── store/             # message store (Memory + JSONL + SQLite)
    │   ├── topic/             # MQTT-style pattern matcher
    │   ├── transport/pilot/   # Pilot `pkg/driver` adapter for gossip
    │   └── wire/              # framing, codec, replay check, rate check
    └── test/canary/           # end-to-end tests (in-memory, Pilot, binary)
```

## How it works (one paragraph)

A group is 32 random bytes of identity plus a signed append-only roster.
Messages are author-signed, reference up to three parents to form a DAG,
and carry a list of MQTT-style topics. `entmootd join` on each member
opens a listener on port 1004 through its local Pilot daemon and holds
a per-host control socket at `~/.entmoot/control.sock` through which
`publish` and `tail` clients route their requests. When you publish, we
sign the message, store it in SQLite, and push its id (just the hash) to
a random sample of roster peers. Peers that don't have the body fetch
it via a separate connection, verifying the signature against the
roster's entry for the author. Every peer maintains a Merkle tree over
the messages it holds in topological order; two peers with the same set
produce the same root, so convergence is checkable in constant space.

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full spec (data
model, wire format, bootstrap flow, security posture, resolved-for-v1
decisions) and [`docs/CLI_DESIGN.md`](./docs/CLI_DESIGN.md) for the
full command and IPC contract.

## Entmoot Service Providers

An Entmoot Service Provider (ESP) is an always-on service peer that can
support intermittent mobile clients without holding their signing keys.
The phone can keep the author identity and sign messages externally,
while the ESP runs normal Entmoot/Pilot infrastructure, syncs already
signed messages, emits local notification hooks, and tracks per-client
mailbox cursors.

ESP mailbox cursors are local service state, not consensus state. The
`mailbox` package exposes a pluggable `CursorStore`: tests and ephemeral
service peers can use the default in-memory store, while durable ESP
deployments can use `OpenSQLiteCursorStore(<data-dir>)`, which stores
cursors in `<data-dir>/mailbox.sqlite` and survives process restarts.
The same durable cursor path is exposed locally through
`entmootd mailbox pull|ack|cursor`, giving ESP operators a production
smoke-test surface. `entmootd esp serve` exposes that mailbox sync surface,
device/session state, group/member read APIs, sign-request queues, push-token
registration, and phone-signed publish forwarding over a small authenticated
HTTP API for local reverse-proxy/mobile integration. ESP-local mobile state is
stored in `<data-dir>/esp.sqlite`; mailbox cursors remain in
`<data-dir>/mailbox.sqlite`. The ESP never signs on the phone's behalf.

```sh
ENTMOOT_ESP_TOKEN='replace-me' entmootd esp serve
curl -H "Authorization: Bearer replace-me" \
  "http://127.0.0.1:8087/v1/mailbox/pull?client_id=ios-1&group_id=<base64>&limit=50"
curl -H "Authorization: Bearer replace-me" \
  -H "Content-Type: application/json" \
  -d '{"message":{...full signed Entmoot message...}}' \
  "http://127.0.0.1:8087/v1/messages"
curl -H "Authorization: Bearer replace-me" \
  "http://127.0.0.1:8087/v1/groups"
curl -H "Authorization: Bearer replace-me" \
  -H "Content-Type: application/json" \
  -d '{"name":"mobile-created group"}' \
  "http://127.0.0.1:8087/v1/groups" # returns a sign_request
```

The server binds to `127.0.0.1:8087` by default and refuses non-loopback
binds unless `-allow-non-loopback` is set. Production deployments should
keep it behind TLS/auth infrastructure. `/healthz` is unauthenticated;
all `/v1/*` routes require ESP auth. The default is bearer-token auth for
compatibility. Production mobile deployments should use `-auth-mode=device`
or `-auth-mode=dual` with a local device registry:

```sh
# Production/import path: the phone generates the private key and gives the
# operator only the public key.
entmootd esp device add \
  -id ios-1-device \
  -pubkey '<base64 ed25519 public key>' \
  -group '<base64 group id>' \
  -client ios-1

# Development/onboarding path: generate a test device keypair and print the
# private key once on stdout. The registry still stores only the public key.
entmootd esp device onboard \
  -id ios-1-device \
  -group '<base64 group id>' \
  -client ios-1

# Rotate only the ESP API device-auth key. This does not recover or replace
# the phone-held Entmoot author key.
entmootd esp device rotate-key \
  -id ios-1-device \
  -pubkey '<new base64 ed25519 public key>'
```

```json
{"devices":[{"id":"ios-1","public_key":"<base64 ed25519 pubkey>","groups":["<base64 group id>"],"client_ids":["ios-1"]}]}
```

The registry defaults to `<data>/esp-devices.json`; pass `-device-keys PATH`
to manage a different file. `disable` is the preferred temporary revocation
tool because it preserves the device entry for audit/rollback; `remove`
hard-deletes the local entry. `onboard` prints a generated private key once
for development/operator handoff, but never stores it in the ESP registry.
For production iOS custody, the phone/client side should generate and retain
its own private key and use `esp device add` to import only the public key.

Device-authenticated requests sign
`ENTMOOT-ESP-AUTH-V1\nMETHOD\nPATH?QUERY\nTIMESTAMP_MS\nNONCE\nBASE64_SHA256_BODY`
with the device Ed25519 key and send the signature in
`X-Entmoot-Signature` alongside `X-Entmoot-Device-ID`,
`X-Entmoot-Timestamp-Ms`, and `X-Entmoot-Nonce`. Mailbox read/cursor routes
work without a running `join` process; signed publish requires `join`
because gossip fanout and roster verification are owned by the daemon. Group
creation, invite acceptance, invite creation, and unsigned message drafts
return ESP sign requests so a phone-held key can authorize the operation
before the ESP relays it.

Mobile clients should send `Idempotency-Key` on mutating ESP requests such as
sign-request creation, sign-request completion, and push-token update. The ESP
stores the request body hash and original JSON response in `esp.sqlite`: a
repeat with the same key and body replays the first response; the same key
with a different body returns `idempotency_conflict`.

APNs delivery is optional and isolated to ESP. Without APNs config,
`esp serve` uses a no-op notifier for development. To send real background
wakeups, provide all APNs settings through flags or environment variables:

```sh
ENTMOOT_APNS_TEAM_ID='TEAMID' \
ENTMOOT_APNS_KEY_ID='KEYID' \
ENTMOOT_APNS_TOPIC='com.example.app' \
ENTMOOT_APNS_KEY='~/AuthKey_KEYID.p8' \
entmootd esp serve -auth-mode=device
```

Use `-apns-sandbox` or `ENTMOOT_APNS_SANDBOX=true` for development builds.
APNs pushes are background wakeups only: no message content is placed in the
push payload; the app wakes and syncs through the ESP mailbox API.

Executable ESP sign requests expose canonical signing metadata. For
`message_publish`, the returned sign request includes `signing_payload`
(base64 canonical Entmoot message signing bytes) and
`signing_payload_sha256`; the phone base64-decodes `signing_payload`, signs
those bytes with its author key, then completes the request with both the
`signature` and matching `signing_payload_sha256`. The ESP verifies the digest
guard and signature, then forwards the completed message through signed
publish. The older `payload` field remains useful for draft/debug display, but
it is not signing material.

```sh
curl -H "Authorization: Bearer replace-me" \
  -H "Content-Type: application/json" \
  -d '{"author":{"pilot_node_id":45491,"entmoot_pubkey":"<base64-ed25519-pubkey>"},"topics":["chat"],"content":"aGVsbG8="}' \
  "http://127.0.0.1:8087/v1/groups/<base64>/messages"
curl -H "Authorization: Bearer replace-me" \
  -H "Content-Type: application/json" \
  -d '{"signature":"<base64-ed25519-signature>","signing_payload_sha256":"<sha256>"}' \
  "http://127.0.0.1:8087/v1/sign-requests/<id>/complete"
```

For manual smoke tests, write the onboarded private key to a local `0600`
file and ask the CLI to produce request headers:

```sh
entmootd esp sign-request \
  -device ios-1-device \
  -private-key-file ./ios-1-device.key \
  -method GET \
  -path '/v1/mailbox/pull?client_id=ios-1&group_id=<base64 group id>'
```

## Deferred from v1

Tracked explicitly; each has a documented upgrade path in
`ARCHITECTURE.md` or `docs/CLI_DESIGN.md`.

- Push-pull gossip (v1 is push-only).
- Near-peer sampling (v1 is random-only).
- Cooldown / backoff on rate-limit breach (v1 hard-disconnects).
- `announce_group` wire message (invites cover group discovery in v1).
- Merkle absence proofs for filtered views (positive inclusion only in v1).
- DHT-assigned keepers.
- Bridge to Pilot's EventStream for legacy consumers.
- Group encryption (messages are plaintext + author-signed; transport is
  still encrypted by Pilot pairwise).
- Multi-admin / quorum rosters (v1 is founder-only).
- Key rotation.
- `entmootd search` (FTS5 over the SQLite store).
- `entmootd stats` (aggregate counters for debugging).
- `entmootd prune -older-than` (retention policy).
- Encryption at rest for the SQLite store.
- Python SDK (v1 is Go-only).

## Pilot runtime notes relevant to Entmoot

The `entmootd` binary needs a running Pilot daemon to talk to. The Pilot
daemon, in turn, needs to reach Pilot's public registry at `34.71.57.205`
to obtain its 48-bit address; there is no fully-offline mode unless you
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

Entmoot is licensed under the [Apache License 2.0](./LICENSE).

Entmoot is an independent project. It interacts with the
[Pilot Protocol](https://github.com/TeoSlayer/pilotprotocol) daemon
over an IPC wire protocol but does not incorporate Pilot Protocol
source code. The IPC client (`pkg/entmoot/transport/pilot/ipcclient`)
is an original implementation written from Pilot's public specification.

Contributions follow a Developer Certificate of Origin — see
[CONTRIBUTING.md](./CONTRIBUTING.md).
