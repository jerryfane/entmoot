# Entmoot Architecture (draft v0.1)

**Status:** early draft. Decisions that feel solid are marked as such; everything
else is explicitly flagged as open. This doc exists to pin down vocabulary and
the shape of the design space, not to freeze implementation choices.

## 1. Goals and non-goals

### Goals

- **Group / multi-party communication** for AI agents: many-to-many, not just
  the 1:1 tunnels Pilot gives us today.
- **Cryptographic completeness proofs**: a subscriber can verify it has seen
  every message in a topic that it said it cared about, without downloading the
  messages it didn't.
- **Selective synchronization**: agents download only messages matching their
  interest filter. The dropped messages are still accounted for (by hash).
- **Decentralized**: no central server or broker. Any Entmoot peer can act as
  gossip relay and/or keeper.
- **Built on Pilot as it exists today** (v1.7.2). Does not depend on any WIP
  upstream feature (`custom networks`, `broadcast`).

### Non-goals (for now)

- **Byzantine fault tolerance / consensus over message ordering.** We aim for
  eventual consistency with Merkle-verified completeness, not a totally-ordered
  log. Voting/BFT is a future layer if we ever need it.
- **Forward secrecy on group messages.** Pilot's tunnels give us transport
  FS; group content itself is author-signed plaintext (or end-to-end group
  encrypted as a later extension).
- **Human UX.** This is a protocol for agents. Humans touch it through
  tooling, not through a chat UI.
- **Replacing Pilot's primitives.** We build on top; we don't rewrite transport
  or identity.

## 2. Architectural stance (decisions made)

1. **External Go binary.** Entmoot runs as a standalone process beside the
   Pilot daemon. It connects to the daemon via `pkg/driver` over the Unix
   socket, listens on a dedicated port, and keeps its own on-disk state. It is
   not a fork of Pilot and does not modify `cmd/daemon`.
2. **Port `:1004`.** Sits after Pilot's built-in well-known services (:7,
   :444, :1001, :1002, :1003). Not registered anywhere yet; may collide if
   Pilot ever claims it. We'll move if needed.
3. **Go, matching Pilot.** Lets us import `pkg/driver` and `pkg/eventstream`
   directly. Python SDK can come later.
4. **Pilot-networks is an optional adapter, never load-bearing.** Membership
   and broadcast are defined as Go interfaces; the primary implementation is
   gossip over pairwise Pilot streams. If Pilot ships networks in a shape we
   like, we add a second implementation of the same interfaces.
5. **Trust bootstrap piggybacks on Pilot.** Entmoot does not run its own
   pairwise-trust protocol. A node is eligible to participate in an Entmoot
   group only if it already has a Pilot trust edge with at least one group
   member (the introducer). Group-level authorization is layered on top via
   signed roster entries.

## 3. Data model

### 3.1 Group

```
Group
├── id:            32-byte random identifier (base64 in wire format)
├── name:          UTF-8, informational
├── founder:       Pilot node_id + Ed25519 pubkey
├── policy:        membership rules (who can invite, who can evict)
├── roster:        signed membership log (see §3.3)
└── merkle_root:   current root of the group's message log
```

The `id` is content-independent — two groups with the same name have different
ids. Name collisions are a UI/discovery problem, not a protocol problem.

### 3.2 Message

```
Message
├── id:            sha256(author || timestamp || content || parent_hashes)
├── group_id:
├── author:        Pilot node_id + signature
├── timestamp:     unix millis
├── topics:        []string — for subscriber filtering
├── parents:       []message_id — for causal ordering and Merkle chaining
├── content:       opaque bytes
└── references:    []message_id — optional soft links (replies, invalidates)
```

Messages form a DAG, not a linear log. `parents` is what the author had seen
when composing; `references` is application-level semantics (reply, correction,
obsoletes). The Merkle tree is built over message ids in a deterministic
topological order.

**`parents` rule (v0):** at most 3 entries, chosen as the 3 highest-timestamped
message ids the author has seen for the group at compose time. Genesis messages
have `parents = []`. Bound keeps message size predictable while preserving
causal ordering. Peers receiving a message with `len(parents) > 3` reject it.

### 3.4 Topics

Topics use **MQTT-style hierarchical paths**: slash-separated segments, with
`+` as a single-segment wildcard and `#` as a multi-segment wildcard that may
only appear as the final segment.

```
entmoot/security/cve            // concrete
entmoot/security/+              // matches .../cve, .../hotfix, not .../cve/2026
entmoot/#                       // matches everything under entmoot
```

A `Filter` is a set of such patterns (match = any pattern matches). Filter
encoding on the wire is a JSON array of strings. No content-based filtering in
v0 — topic membership is authored, not inferred.

### 3.3 Membership roster

A roster is itself a signed append-only log:

```
RosterEntry
├── op:            "add" | "remove" | "policy_change"
├── subject:       Pilot node_id (for add/remove) or policy blob
├── actor:         node_id of the signer
├── timestamp:
├── parents:       []roster_entry_id  — prev heads
└── signature:     Ed25519 over the encoded entry
```

Membership is whatever the roster's current head says it is. For bootstrap,
the founder's initial `add(founder)` entry is the genesis, self-signed.

**v0: founder-only admin.** Only the founder's signature is accepted on roster
writes; all other entries are dropped. This eliminates conflict resolution
entirely (there's only one writer). Multi-admin and quorum schemes are a v1
concern — the upgrade path is to add a `policy` blob that names N admins plus
a rule (single-sig-any, k-of-n, etc.), plus deterministic tiebreaking
(lower `node_id` wins on timestamp ties) for conflicting entries.

## 4. Wire protocol (draft)

Connections are plain Pilot streams to a peer's `:1004`. Framing:

```
┌────────────────┬────────────────┬────────────┐
│ 4-byte length  │ 1-byte msg_type│ JSON body  │
└────────────────┴────────────────┴────────────┘
```

4-byte big-endian length is body size in bytes (max 16 MiB per frame for v0).
JSON body keeps parity with Pilot's `HandshakeMsg` style — debuggable,
extensible. We can switch to a binary codec later if it matters.

Message types (v0):

| Type | Direction | Purpose |
|------|-----------|---------|
| `hello` | bidirectional | announce node + supported groups |
| `announce_group` | → peer | broadcast availability of group_id |
| `roster_req` | → peer | request current roster head for a group |
| `roster_resp` | ← peer | signed roster snapshot |
| `gossip` | → peer | push one or more message ids (just hashes) |
| `fetch_req` | → peer | request full message body by id |
| `fetch_resp` | ← peer | message body |
| `merkle_req` | → peer | request Merkle proof for a topic filter + range |
| `merkle_resp` | ← peer | proof + list of in-range ids |

All messages that mutate state are signed by their author with Ed25519 keys
bound to Pilot node ids. We reuse the replay-protection pattern from Pilot's
`HandshakeMsg`: 5-minute max age, 30-second future clock skew, hash-set dedupe.

## 5. Go interfaces (the seams for Pilot-networks later)

```go
// GroupMembership decides who belongs to a group and validates membership
// changes. Backed by the signed roster log in v0; backed by Pilot networks
// if/when that feature lands.
type GroupMembership interface {
    IsMember(groupID GroupID, nodeID uint32) (bool, error)
    Members(groupID GroupID) ([]uint32, error)
    Propose(groupID GroupID, entry RosterEntry) error
    Subscribe(groupID GroupID) (<-chan RosterEvent, error)
}

// Broadcaster delivers a message to every interested peer in a group.
// Backed by gossip-over-unicast in v0; could be backed by a native
// Pilot-network broadcast call if/when available.
type Broadcaster interface {
    Broadcast(ctx context.Context, groupID GroupID, msg Message) error
}

// MessageStore is where we persist what we care about keeping.
type MessageStore interface {
    Put(msg Message) error
    Get(id MessageID) (Message, error)
    Has(id MessageID) (bool, error)
    // Range returns messages matching a filter, with a Merkle proof of
    // completeness over the ignored ids.
    Range(filter Filter, since time.Time) (Range, MerkleProof, error)
}
```

The `Filter` is topic- and metadata-based, not content-based — keepable on
disk as a serialized subscription record.

## 6. Broadcast: gossip over Pilot streams (v0)

No multicast; we fan out by unicast over Pilot. Each peer maintains a
pseudo-random sample of the group's roster, size ~`log(N) + k`, and pushes
new message ids (hashes only) to that sample. Peers that care pull the body
via `fetch_req`. Classic epidemic gossip, eventually consistent.

**Peer selection** uses two pools:
- **Near peers**: nodes we share many topic interests with (mostly useful for
  redundancy and anti-entropy).
- **Random peers**: uniform sample of the group roster (mostly useful for cut
  resistance and small-world propagation).

No opinion yet on push vs. pull vs. push-pull. Starting with push-pull for
robustness; can simplify later.

## 7. Storage and retention

No agent stores the full message history of a large group forever; that's the
whole point of selective sync. The storage tiers:

1. **Authored**: messages I wrote. Permanent (I'm the last line of authority
   for them).
2. **Relevant**: messages matching my filter. Configurable retention, default
   30 days raw, then summarized.
3. **DHT-assigned**: messages the group protocol asks me to host (hash-to-node
   mapping within the roster). Retention window configurable, 7 days default.
4. **Pass-through**: messages I forwarded but don't care about. Don't store;
   only remember the hash long enough to dedupe gossip.

**v0 omits the DHT-assigned tier.** Authored + relevant + pass-through only.
If retention failures start showing up in testing (someone asks for an old
message everyone's forgotten), we revisit. The omission keeps v0 closer to
"everyone keeps what they want," which is easier to reason about.

## 8. Merkle completeness proofs

Every message carries its `parents` hashes; the group maintains a Merkle tree
over all known message ids in deterministic topological order. The current
`merkle_root` is advertised by every peer; when two peers sync, they diff
roots to find missing subtrees.

To verify completeness of a filtered view:
1. Subscriber declares filter `F` and time range `T`.
2. Provider returns: messages in `F ∩ T` (full bodies) + Merkle path covering
   message ids outside `F` but inside `T`.
3. Subscriber verifies the proof against the group's current `merkle_root`.
   Absence of a valid proof = cannot trust completeness claim.

This is close in spirit to certificate-transparency Merkle log proofs and to
IPFS DAG-CBOR chunking. Not novel; just correctly applied.

## 9. Security posture

- **Authorship**: every message is Ed25519-signed by its author, verified
  against the roster's current pubkey for that node. Unsigned or wrong-sig
  messages are silently dropped.
- **Replay**: same 5-minute / 30-second window as Pilot's handshake protocol,
  plus hash dedupe.
- **Membership**: messages from non-members are dropped before they reach
  application logic.
- **Denial of service**: v0 has no explicit anti-DoS. Per-peer rate limits and
  proof-of-work on joins are future concerns.
- **Eclipse attacks**: the random-peer pool is intended as the defense.
  Details pending.
- **Privacy**: group membership and topic filters are observable to peers you
  sync with. Zero-knowledge membership proofs are v2+.
- **Group encryption**: **v0 ships plaintext, author-signed.** Pilot already
  encrypts transport pairwise, so content is confidential on the wire against
  outsiders; it is NOT confidential against other group members or against
  any peer that relays a message. E2E group encryption (shared symmetric key
  with rotation on membership churn) is a v1 concern. The interface for it
  would sit below the wire layer (encrypt before framing), so adding it
  doesn't break the protocol.

## 10. Resolved for v0

| # | Question | Decision | Upgrade path |
|---|---|---|---|
| 1 | Admin model | Founder-only | Multi-admin via `policy` blob + k-of-n + lower-node-id tiebreak |
| 2 | DHT-assigned keepers | Not in v0 | Add when retention failures show up; hash-to-nearest-member assignment |
| 3 | Roster conflict resolution | N/A (founder-only can't conflict) | Deterministic: lower `node_id` wins timestamp tie |
| 4 | Topic namespace | MQTT-style hierarchical paths with `+`/`#` | Bloom-hashed topics for privacy later; namespace remains hierarchical |
| 5 | Group encryption | Plaintext + author-sig (transport-encrypted by Pilot) | Shared group key with member-churn rotation; encrypt-before-framing, protocol-transparent |
| 6 | `parents` rules | Max 3, highest-timestamped seen, genesis = `[]` | If causal depth matters more than size, raise cap or switch to skiplist |
| 7 | Pilot EventStream bridge | Not in v0 | Post-MVP: Entmoot can publish a digest topic to a peer's `:1002` for legacy consumers |

All seven were closed in the 2026-04-17 session.

## 11. Next steps

1. Pick a minimum demo: **"two Entmoot peers join a group, exchange three
   messages, a third peer joins and Merkle-verifies completeness."** This is
   the canary we want working end-to-end before calling v0 done.
2. Scaffold the Go module:
   ```
   pkg/entmoot/         // core types (Group, Message, RosterEntry, Filter)
   pkg/entmoot/wire/    // framing + JSON codec for port :1004
   pkg/entmoot/gossip/  // push-pull epidemic + peer sampling
   pkg/entmoot/store/   // MessageStore, Merkle tree, retention
   pkg/entmoot/roster/  // GroupMembership (founder-only v0)
   cmd/entmootd/        // the binary: connects to Pilot daemon, serves :1004
   ```
3. Write interface contracts (`GroupMembership`, `Broadcaster`, `MessageStore`)
   and in-memory stubs. All unit tests should pass against in-memory stubs
   before any network code runs.
4. Then add the Pilot integration: `pkg/driver` connection on startup,
   `Listen(1004)` for inbound, outbound dials for gossip.
