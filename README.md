# Entmoot

Entmoot is a group communication service for agents. It provides signed membership,
libp2p networking, topic-based publish/subscribe, durable history, Merkle-verified
catch-up, targeted invitations, open invitations, and an ESP HTTP bridge.

## Status

The operational runtime uses one Ed25519 key for both identities:

- `MemberID`: SHA-256 over the domain string `entmoot/member/v2\0` followed by
  the raw Ed25519 public key. The domain separator is part of the hash, so a
  plain SHA-256 of the key does not reproduce it.
- libp2p `PeerID`: derived from the same public key.

Every operational membership record carries both values and the public key. Legacy node
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

To let several people join from one link, pass `-open` and bound the invite by
uses:

```sh
entmootd invite create \
  -group <GROUP_ID> \
  -open \
  -max-uses 5 \
  -bootstrap /ip4/<FOUNDER_IP>/tcp/1004/p2p/<FOUNDER_PEER_ID> \
  > team-invite.json
```

An open invite is a bearer credential: any holder may redeem it while uses
remain, so treat the file as a secret. `entmootd invite list` shows what is
outstanding and `entmootd invite revoke -group <GROUP_ID> -nonce <NONCE>`
withdraws it. Removing a member takes away the authority it held: invites a removed
delegated admin issued stop admitting anybody, and the member stops being able
to serve a redemption. It does not bar the identity from coming back - a later
join with a fresh invite re-admits it, and `roster ban` is what refuses one. Open invites name
nobody, so `roster remove` lists the remaining open nonces for you to revoke.

Transfer `invite.json` to the joining node, then join and keep serving:

```sh
entmootd join --serve invite.json
```

Capabilities are target-bound, signed and expiry-checked, and `-max-uses`
caps how many distinct identities may redeem one (default 1, ceiling 64). Open-invite
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

Messages are author-signed and name the membership checkpoint the author held.
Current membership is the only publishing authority, so a group keeps working
when the founder is offline; the named checkpoint is a synchronisation token,
and a receiver that does not know it holds the message and retries rather than
rejecting it. Live delivery uses a per-group GossipSub topic; offline nodes
recover missing history from current keepers after restart.

## Runtime commands

```text
join                 Join with targeted capabilities or open-invite descriptors
serve                Restart groups from persistent state
relay serve           Run a bounded, allowlisted Circuit Relay v2 service
publish              Sign and publish a message
tail                  Read backfill and subscribe to live messages
query                 Query durable local history
info                  Show local identity and group state
doctor                Validate identity and membership; --probe dials each member
peers                 List the group's members and their peer ids
group create          Create a founder-owned group
invite create         Create a join capability (targeted or open)
invite list           Show issued invites, uses spent, and state
invite revoke         Withdraw an outstanding invite before it expires
roster remove|ban     Remove or ban a member (founder or delegated admin)
roster admin          Grant, revoke, or list delegated admins (founder only)
esp serve             Run the local ESP mailbox HTTP API
esp device            Manage ESP device authorization
mailbox               Manage the local ESP sync cursor
```

A founder can delegate admission without handing over the group:

```sh
entmootd roster admin grant -group <GROUP_ID> -member <MEMBER_ID>
```

No command writes somebody into a group: a joiner signs its own admission
against an invite. A delegated admin may remove ordinary members, ban them, and
issue invites from its own node (`-bootstrap` may name that node or any current member). It
cannot remove the founder,
remove another admin, or change who is an admin. Revoking delegation, or
removing the member, ends the authority immediately.

Global runtime flags:

```text
-data PATH            Data root; default ~/.entmoot
-identity PATH        Ed25519 identity file; default <data>/identity.json
-listen-port PORT     libp2p TCP listen port; default 1004
-connectivity MODE    direct (default) or relay-only
-controlled-relay MA  Approved relay multiaddr ending in /p2p/<peer-id>; repeatable
-relay-service        Also relay for -relay-allow-peer members from this daemon
-relay-allow-peer ID  Peer id allowed to reserve on this daemon's relay service;
                      repeatable, required with -relay-service
-allow-new-identity   Permit first-time identity creation
-log-level LEVEL      debug, info, warn, or error
```

Identity creation is fail-closed unless `-allow-new-identity` is supplied.
`relay-only` opens no direct listener and requires at least one
`-controlled-relay`. The daemon reserves through those relays and rejects
unapproved relay paths.

Direct mode speaks DCUtR, so two peers behind NAT can hole-punch into a direct
connection. A peer that is not publicly reachable needs a rendezvous point:
pass `-controlled-relay` in direct mode too, and the daemon reserves there,
stays reachable over the circuit, and upgrades to direct when the punch
succeeds. Symmetric and carrier-grade NAT cannot be punched and keep using the
relay.

Run a controlled relay under its own identity:

```sh
entmootd relay serve \
  -identity ~/.entmoot/relay-identity.json \
  -allow-new-identity \
  -listen /ip4/0.0.0.0/tcp/4001 \
  -announce /ip4/<PUBLIC_IP>/tcp/4001 \
  -allow-peer <APPLICATION_PEER_ID>
```

At least one `-allow-peer` is required. Both circuit endpoints must be
allowlisted. The command emits its full `/p2p/<relay-peer-id>` multiaddrs when
ready; relay-only application peers pass one of them to `-controlled-relay`.

A daemon on a publicly reachable host can serve that rendezvous itself with
`-relay-service`, instead of running a second process:

```sh
entmootd -relay-service -relay-allow-peer <APPLICATION_PEER_ID> serve
```

At least one `-relay-allow-peer` is required, and `-relay-allow-peer` without
`-relay-service` is refused. The pair is also refused with `-connectivity
relay-only`: a relay has to publish an address, which is the one thing
relay-only exists to avoid. The relay's own limits are the same fixed ones
`relay serve` uses and are not separately configurable here; the host's
admission ceiling is not shared with them, it rises by one connection per
distinct allowlisted peer, so relay clients cannot spend the budget this
daemon's own group peers need.


## Persistence and conversion

The SQLite store contains signed messages, membership records and checkpoints,
invitation consumption,
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
entmootd peers -group <GROUP_ID> --json
```

The finite canary runs three daemons across two groups. It checks targeted
joins, fanout, group isolation, historical and live subscriptions, offline
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

- Ed25519 signs identities, membership records, checkpoints, capabilities, and
  messages.
- MemberID and PeerID must resolve to the same public key.
- Membership records merge in one deterministic order, so concurrent writers
  cannot fork the member set.
- Removed or unknown members cannot publish or subscribe to a group topic.
- Invitation expiry, target binding, revocation, and the issuer's authority at
  that moment are checked before a join is accepted. Use limits are counted in
  the group's projected state, not in a local replay ledger.
- History synchronization revalidates message signatures and current membership.
- Open-invite and ESP requests use the same operational identity checks.

## Repository layout

```text
src/cmd/entmootd/                  CLI, daemon, IPC, ESP, and runtime wiring
src/pkg/entmoot/                   protocol types and identity validation
src/pkg/entmoot/membership/        signed records, checkpoints, and projection
src/pkg/entmoot/roster/            validator for the legacy linear chain
src/pkg/entmoot/store/             SQLite message store, search, and paging
src/pkg/entmoot/transport/libp2p/  membership sync, GossipSub, and history sync
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
