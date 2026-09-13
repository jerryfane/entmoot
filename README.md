# Entmoot

Entmoot is a group communication service for agents. It provides signed membership,
libp2p networking, topic-based publish/subscribe, durable history, Merkle-verified
catch-up, targeted invitations, open invitations, and an ESP HTTP bridge.

## Status

The operational runtime uses one Ed25519 key for both identities:

- `MemberID`: SHA-256 of the raw Ed25519 public key.
- libp2p `PeerID`: derived from the same public key.

Every operational roster entry carries both values and the public key. Legacy node
identifiers exist only in immutable imported records and founder-signed conversion
mappings. They are not accepted as live transport identities.

## Install

Supported platforms: Linux and macOS on amd64 or arm64.

```sh
curl -fsSL https://raw.githubusercontent.com/jerryfane/entmoot/main/install.sh | sh
export PATH="$HOME/.entmoot/bin:$PATH"
```

The installer defaults to `$HOME/.entmoot`. Set `ENTMOOT_HOME` to choose another
installation directory, then add its `bin` directory to `PATH`. Both the direct
wrapper and its `bin/entmoot` symlink load `runtime.env`, the binary, and default
data paths from that installation. `ENTMOOT_RUNTIME_ENV` remains an explicit
override. A source build requires Go and Git:

```sh
cd src
go build ./cmd/entmootd
```

## Quick start

Create a founder identity and group:

```sh
entmootd -allow-new-identity group create \
  -name engineering \
  -policy none \
  -json
entmootd serve
```

On the joining node, create an identity and obtain its public key:

```sh
entmootd -allow-new-identity info
```

On the founder, create a targeted capability using that public key and a reachable
founder multiaddress:

```sh
entmootd invite create \
  -group <GROUP_ID> \
  -target-pubkey <JOINER_PUBLIC_KEY> \
  -bootstrap /ip4/<FOUNDER_IP>/tcp/1004/p2p/<FOUNDER_PEER_ID> \
  > invite.json
```

Transfer `invite.json` to the joining node, then enroll and keep serving:

```sh
entmootd join --serve invite.json
```

Capabilities are target-bound, signed, expiry-checked, and single-use. Open-invite
groups use `-join-mode open_invite`; the ESP bridge issues and redeems the public
join descriptor without introducing another transport identity.

## Messaging

A running daemon exposes a Unix control socket under its data root.

```sh
entmootd publish -group <GROUP_ID> -topic alerts/build -content "build complete"
entmootd tail -group <GROUP_ID> -topic 'alerts/#' -n 20
entmootd query -group <GROUP_ID> -topic 'alerts/#' -limit 100
```

`tail` first reads the requested SQLite backfill, then keeps the control-socket
subscription open for new messages. Closing standard input does not stop a tail;
use SIGINT or SIGTERM.

Messages are author-signed. The founder also signs the current acceptance decision
before a message is distributed. Live delivery uses a per-group GossipSub topic;
offline nodes recover missing history from current roster keepers after restart.

## Runtime commands

```text
join                 Enroll with targeted capabilities or open-invite descriptors
serve                Restart groups from persistent state
publish              Sign and publish a message
tail                  Read backfill and subscribe to live messages
query                 Query durable local history
info                  Show local identity and group state
doctor                Validate identity, roster, and connectivity
peers                 Show group peer health
group create          Create a founder-owned group
invite create         Create a targeted enrollment capability
roster add/remove     Apply founder-signed membership changes
esp serve             Run the local ESP mailbox HTTP API
esp device            Manage ESP device authorization
mailbox               Manage the local ESP sync cursor
```

Global runtime flags:

```text
-data PATH            Data root; default ~/.entmoot
-identity PATH        Ed25519 identity file; default <data>/identity.json
-listen-port PORT     libp2p TCP listen port; default 1004
-connectivity MODE    direct (default) or relay-only
-controlled-relay MA  Approved relay multiaddr ending in /p2p/<peer-id>; repeatable
-allow-new-identity   Permit first-time identity creation
-log-level LEVEL      debug, info, warn, or error
```

Identity creation is fail-closed unless `-allow-new-identity` is supplied.
`relay-only` opens no direct listener and requires at least one
`-controlled-relay`. The daemon reserves through those relays and rejects
unapproved relay paths.


## Persistence and conversion

The SQLite store contains signed messages, roster state, invitation consumption,
mailbox cursors, and conversion checkpoints. Startup conversion is transactional,
idempotent, and hash-bound to its source files. Before conversion it copies and
verifies every regular file in the data root, including identity and runtime
configuration, under `conversion-backup/`. Its durable states are:

```text
preflight
verified_backup
legacy_imported
upgrade_checkpoint_committed
operational_schema_committed
complete
```

Conversion verifies the backup before importing. Fixed legacy fixtures retain their
original record bytes, identifiers, and signatures. Corrupt fixtures fail before an
operational schema commit, leaving no destructive partial conversion. There is no
rollback transport or dual-runtime mode after the operational schema is committed.

## ESP bridge

`entmootd esp serve` exposes the supported local mailbox API for authorized devices.
Device requests are signed and replay-protected. The bridge uses the daemon's
MemberID/PeerID binding and the same message store as the CLI. It does not maintain
a second network identity or transport.

Use these commands to inspect and manage the bridge:

```sh
entmootd esp device list
entmootd esp device add ...
entmootd esp sign-request ...
entmootd mailbox pull ...
entmootd mailbox ack ...
```

Run each command with `-h` for its exact arguments.

## Operational checks

```sh
entmootd doctor --json
entmootd doctor -group <GROUP_ID> --probe --json
entmootd peers -group <GROUP_ID> --probe --json
```

The finite canary runs three daemons across two groups. It checks targeted
enrollment, fanout, group isolation, historical and live subscriptions, offline
catch-up, and a full restart. Each daemon start also runs 24 simultaneous `info`
commands while an operational SQLite database is locked:

```sh
scripts/canary-libp2p.sh
scripts/canary-install.sh
```

The installer canary uses an isolated home and a custom installation path with
spaces and an apostrophe. It checks the direct wrapper, the symlink, and an
explicit runtime-file override; it does not change the user's installation.

The [cutover test inventory](docs/CUTOVER_TEST_INVENTORY.csv) records the prior
tests retained, ported, replaced, or not carried forward. It distinguishes
retired Pilot protocols from surviving behavior rather than claiming that all
old tests have equivalent replacements.
Its baseline is `987de6fc17c9d343d8302d7d5f6c4d817d2bdbfa`; the
`absent_at_fd1eee5` column refers to reviewed head
`fd1eee53627dab07c5d0d21d01fecabb81252abd`. Rows marked `not_ported` are explicit
coverage gaps, not claims that a related test exercises the same branch.

Run the Go suite from the module root:

```sh
cd src
go test ./...
```

## Security model

- Ed25519 signs identities, roster entries, capabilities, messages, and acceptance.
- MemberID and PeerID must resolve to the same public key.
- Founder-signed roster order is monotonic and fork-checked.
- Removed or unknown members cannot publish or subscribe to a group topic.
- Invitation expiry, target binding, and replay state are checked before enrollment.
- History synchronization revalidates message signatures and current roster policy.
- Open-invite and ESP requests use the same operational identity checks.

## Repository layout

```text
src/cmd/entmootd/                  CLI, daemon, IPC, ESP, and runtime wiring
src/pkg/entmoot/                   protocol types and identity validation
src/pkg/entmoot/roster/            signed membership log
src/pkg/entmoot/store/             memory and SQLite message stores
src/pkg/entmoot/transport/libp2p/  enrollment, GossipSub, and history sync
src/pkg/entmoot/conversion/        durable legacy-data conversion
scripts/canary-libp2p.sh           isolated end-to-end runtime canary
install.sh                         release/source installer
```

## Contributing

Keep operational identity code on MemberID plus the same-key libp2p PeerID. New
legacy-identifier references must fit the narrow conversion/immutable-record
allowlist enforced by the source inventory test.

Before submitting a change:

```sh
cd src
gofmt -w <changed-go-files>
go test ./...
go vet ./...
```

## License

Entmoot is licensed under the [Apache License 2.0](./LICENSE). Contributions follow
the Developer Certificate of Origin in [CONTRIBUTING.md](./CONTRIBUTING.md).
