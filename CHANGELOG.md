# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- A probe budget below one peer-slice no longer defeats the floor it is
  measured against. `probePeers` clamped its per-peer slice and its
  remaining-time budget to `minProbeSlice` but not the incoming budget, so
  `doctor -probe-timeout 1ms` - an operator value forwarded unfiltered -
  refused every peer with "not attempted: probe budget spent" instead of
  probing any of them. The budget is now clamped at entry too. This also
  removes a 1-in-130 test flake whose window was the 1ms the caller asked for.

### Removed

- `MemberAuth` no longer carries `Method`, `Path`, `TimestampMS`, `Nonce` and
  `Signature`. All five are auth inputs already spent by the time the struct
  exists - the timestamp is range-checked, the nonce is burned into the replay
  cache, the signature is verified - and nothing read them. Keeping spent
  credentials invites a reader to trust one. They were `json:"-"`, so
  `GET /v1/session` is unchanged.

### Changed

- Member-signature authentication now has a round-trip test: a correctly signed
  `GET /v1/session` returning the member echo, plus the replay, clock-window
  and impersonation refusals. It had none.
- The profile total-order sweep builds two state stores instead of 264. The
  132 record pairs are isolated by member id, not by database, so the
  per-pair store bought nothing: the `esphttp` package now tests in 20s
  instead of 48s.
- One serveability predicate, spelled two ways: `membershipExists` and
  `groupMembershipExists` are gone in favour of `membership.Exists`, which
  eight call sites already used.
- Three production sites restated the group directory encoding by hand; they
  call `GroupID.DirName()` now, which exists to be the single spelling.

## [1.5.85] - 2026-09-17

### Removed

- `agent-live` is deleted: the subcommand, its polling runtime, the OpenClaw
  runner adapter and selectors, the `default-moot live on|off` consent path,
  the `live-agents` ESP HTTP routes, the `live` field on mobile member
  summaries, and the `esp_live_agent_configs`/`_presence`/`_cursors` tables
  (dropped on open, like the retired Fleet tables). It was a 2,532-line bot
  living outside the daemon: it opened the daemon's SQLite behind its back
  every 10 seconds, re-scanned a 10-minute message window, and shelled out to
  a runner binary, duplicating a push path the daemon already owns
  (`notifyingStore` + `ipc tail_subscribe`). Its presence "lease" was
  decorative and its cursor a blind last-write-wins upsert, so two runners
  answered every message twice. Entmoot stays a message pipe; a bot belongs
  outside it, reading the push path.
- Member-signature authentication (`X-Entmoot-Member-*`) now reaches only
  `GET /v1/session`. The live-agent config routes were the only others that
  accepted it.
- The four `live_*` limits in a group policy survive as a wire remnant and are
  enforced by nothing. They cannot be deleted in code: a public-moot
  descriptor embeds the policy, is signed over its bytes, and is parsed with
  unknown fields rejected, so removing them breaks every descriptor already
  signed and published. Retiring them needs a re-signed descriptor.

### Added

- `serve -relay-service -relay-allow-peer <PEER_ID>` runs the bounded,
  allowlisted Circuit Relay v2 service on the daemon's own host, so a publicly
  reachable node can relay for its members without a second process. The
  service, its ACL and its own resource limits are the ones `relay serve` uses.
  The host's connection ceiling differs, because the daemon has one: it is
  raised by one connection per allowlisted peer, so relay load cannot deny the
  daemon's own group members - which are not protected - and the advertised
  reservation cap stays reachable. At least one allowed peer is required, and the flag is
  refused in the `relay-only` profile - that profile has no public listener to
  relay through and exists to keep a peer's address private, while a relay must
  publish one. Unlike `relay serve`, it does not replace the announced address
  list: the addresses a daemon announces are the ones it puts in the invites it
  mints. The cost of choosing it over the dedicated process is that the
  published relay address belongs to the daemon's member identity, which is
  already the case for any publicly reachable daemon, and that the two can no
  longer fail or restart independently.

## [1.5.84] - 2026-09-17

### Removed

- `pkg/entmoot/roster` is now only what conversion needs: a validator for the
  legacy linear chain. Its package doc described a live append-only membership
  log, which `pkg/entmoot/membership` replaced; the persistence half -
  writer lease, SQLite schema, migrate, import, load, persist - duplicated
  what `pkg/entmoot/conversion` already owns, and the mutation and query API
  had no production caller. 1501 non-test lines become 629, and the exported
  surface is `ValidateEntries`, `ValidateLegacyJSONL`, `CurrentEntryVersion`
  and the admin-policy payload a legacy `policy_change` carried.
- The per-(peer, topic) rate limit: `Limits.TopicLimits`, `TopicLimit`,
  `AllowTopic`, `AllowTopicOnly` and `Limiter.Reset`. It was complete and had
  no production caller. The per-peer message and byte buckets are unchanged.
- Four declarations nothing ever produced: `entmoot.ErrInviteExpired`, whose
  doc named a package deleted with the Pilot cutover; the
  `bootstrap_denied` sync error code, never written to a stream;
  `esphttp.PublicMootMirrorHosted`, a mirror state no route assigns - only
  `none` and `member` are ever produced; and `entmoot.Filter` together with
  `topic.MatchAny`, which was test-only.

### Fixed

- A node whose coverage bound moves backwards now recovers the membership it
  lost, as long as some peer still holds the records. Retirement deletes
  records, so
  when a branch that reaches further while dated earlier wins the walk, the
  members those records carried disappear - and the peers that still hold
  them were exactly the ones the node stopped asking, because the pull list
  came from the current projection. Two changes make the repair happen on an
  ordinary sync round: the pull list is now everyone recently seen as a
  member, including members of the checkpoints this node still retains, and a
  peer serves the records the CALLER's checkpoint has not folded in rather
  than the ones its own has - it holds them for a checkpoint of lag anyway,
  and a caller refuses whatever its own checkpoint already accounts for.
  Holding the record again also restores the node's ability to refuse the
  branch extension that would otherwise make the loss permanent. The repair
  runs on the next ordinary sync round, and it has a deadline: once every peer
  has folded one more checkpoint past the window, nothing holds those records
  and the loss stands.

- Competing checkpoints at one sequence no longer lose a member. A
  checkpoint's timestamp is the coverage bound, and retirement is
  irreversible: the records behind the previous checkpoint are deleted. The
  sibling rule preferred the EARLIER timestamp, so a checkpoint dated behind
  one whose records were already deleted won its sequence and the membership
  those records carried went with them - a member that had properly joined
  vanished, with no adversary involved. Siblings are now ordered by timestamp
  first; a founder-signed checkpoint wins the tie at one timestamp, which is
  what keeps a group joinable. Two things this does NOT do: the bound is not
  monotone in general, because a longer branch dated earlier still wins on
  reach, so the same loss remains reachable one sequence further out; and a
  checkpoint signer can now date a checkpoint just after an honest one to win
  it, which excludes a record from nodes that have not yet received it, though
  nodes holding the record refuse such a checkpoint outright.

- A membership checkpoint that folds nothing in now covers what it succeeds.
  Retirement drops the records behind the previous checkpoint, but the
  projection read a zero fold count as covering nothing at all, so a node that
  still held those records replayed them on top of the newer checkpoint while
  a node that had retired them did not. Invite-use counting is the one
  non-idempotent effect: the same invite was counted once on one node and
  twice on the other, and a joiner with a use left over was admitted by one
  and refused by the other. `roster checkpoint` on a quiet group mints exactly
  this checkpoint. The store and the projection now share one predicate
  instead of restating it, which is what the code already claimed. A
  checkpoint is also required to be dated after its predecessor: retirement
  drops records through the previous checkpoint while coverage bounds at the
  canonical one, so a backdated successive checkpoint from a peer retired
  records it did not cover - the same asymmetry from the other side. Only the
  local signer advanced the timestamp; nothing checked an arriving one.

## [1.5.83] - 2026-09-17

### Fixed

- The ESP state-schema migration no longer dies when another process migrates
  the same database first. It checked each column with `PRAGMA table_info` and
  then issued `ALTER TABLE ... ADD COLUMN`, which is a race: the ESP bridge and
  the daemon both open `esp.sqlite`, so a simultaneous restart had both read
  the same schema, both decided a column was missing, and the loser exited on
  `duplicate column name`. It happened in production on 2026-09-17 during the
  v1.5.82 deploy: the ESP went down and only `Restart=always` brought it back
  five seconds later. SQLite has no `ADD COLUMN IF NOT EXISTS`, so the
  duplicate error is now the idempotency check, anchored on the reported
  column name - this schema holds `result`, `publish_result` and
  `operation_result`, so a looser match would read one as another - and the
  helper refuses any statement that is not an `ADD COLUMN`, because a failed
  `RENAME COLUMN` reports the same text.

## [1.5.82] - 2026-09-17

### Added

- `entmootd doctor --probe` and `peers --probe` now do what they always
  claimed. The running daemon dials every other member of the group and
  performs one membership read: that proves the peer is up, speaks the
  protocol and serves this group to this node, where a connection alone would
  not. Each peer row gains a `probe` object holding `reachable`, `answered`,
  `refusal`, `latency_ms`, `relayed` and how many addresses were tried, with a
  one-line reason on failure. Three outcomes, not two: `answered` without
  `reachable` is a peer
  that replied and refused this node, which is what a removed member sees from
  every peer and must not read as a network fault. A multi-homed member's dial
  error is summarised rather than pasted, and the peer's own refusal text is
  capped, because otherwise a member decides how large this daemon's answer
  is.

  The probe lives in the daemon because the daemon owns the libp2p host, the
  peerstore and the relay configuration; a second process dialling with its
  own identity would answer a different question. Without a daemon the group
  reports `probe_status: runtime_unavailable` and no peer row claims anything.
  `--timeout` is the budget for the whole probe, not per peer, and
  `probe_status` reads `incomplete` if it runs out. New IPC frames
  `peer_probe_req`/`peer_probe_resp` (0x25/0x26) carry it.

### Fixed

- `entmootd doctor` now has the `-redact` flag its documentation already
  promised. The redaction helper, its unit test and the docs all existed while
  nothing called it, so a report an operator shared to ask for help carried the
  data directory, the identity path and `/proc/<pid>` paths anyway. Redaction
  runs before both the JSON and the human output.

### Changed

- **An invite no longer needs its issuer online.** `invite create` accepted
  only the issuing node's own address as a bootstrap peer, and a peer serves a
  newcomer only when the capability names it, so the issuer had to be running
  for its own invite to work — the one thing self-signed admission was meant to
  remove. A bootstrap address may now name any CURRENT member. The issuer also
  attaches known addresses of up to four other members — at most two each,
  eight in total, and 1 KiB in total, each address at most 256 bytes — so an
  ordinary `invite create` survives its author going down without the operator
  naming anybody. Relay hints are bounded the same way. The cap is on ADDRESSES as well as
  members because a multi-homed host holds dozens, and a capability travels in
  one request frame with an 8 KiB ceiling: bounding members alone produced
  invites too large to redeem. Every mint path refuses a capability over the
  budget, the budget is pinned by a test that puts a capability of that size
  into a real read request, and the creation-time check charges everything the
  caller did not name at its enforced ceiling rather than modelling the JSON:
  three attempts at modelling it undercounted in turn, on the fallback slots,
  then the relay bytes, then the nonce and timestamps. The fixed-field
  allowance is measured by a test against a capability built the way the mint
  builds one.

  Routable addresses are preferred. A member known only on a non-routable one —
  a LAN, a ULA, a link-local or a carrier-NAT address — still gets a single
  slot, because on that network it is the door that works; at most four such
  addresses can reach one invite, `invite create` reports on stderr when it
  attaches any, and loopback is never attached to the fallback set, because it names
  the joiner's own machine rather than a member — the one exception being a
  node whose own addresses are all loopback, where the alternative is an
  invite naming nothing at all. `-no-fallback-peers` attaches none, and `no_fallback_peers` does
  the same on the IPC path, on the ESP invite-create operation, and on an ESP
  open invite, where it is stored with the token because the capability is
  minted only at redemption.

  An ESP open invite is checked at CREATION instead, because the token is what
  gets shared and no capability exists yet: a malformed multiaddr or a list too
  long to redeem is refused there rather than producing a link that fails for
  every joiner.

  A non-member's address is still refused, and a removed or banned member stops
  being serveable the moment its removal projects — enforced where it matters,
  at the serving node: being named by an invite is not sufficient, because an
  invite is minted against the membership of one moment and then lives for its
  whole TTL. The issuer itself is exempt, since its authority is checked on
  every read and a founder may issue after removing itself. Naming another member is safe
  because the newcomer pins the founder's key from the invite and verifies the
  membership it is served against that key: a named peer can serve or fail, not
  forge. The serving peer evaluates the issuer's authority against its own view
  of the group, so a demoted issuer's invite fails wherever it is presented.

  Proved on three daemons: the founder mints an invite naming a plain member,
  the founder is stopped, and the newcomer joins through that member in under a
  second, with both live nodes converging on three members and one checkpoint.
  The same command on the previous build exits 5.


### Removed

- **Five one-off files are removed from the repository root.**
  `GOAL-social-mode-disable-fleet-tasks.md` (the prompt for work that shipped,
  and whose feature is now deleted outright), `jj3-connectivity-summary-and-fix.md`,
  `jj3-repair-plan.md` and `asia-155760-fix-steps.md` (June-2026 incident notes
  for a Pilot runtime that no longer exists, so their instructions contradict
  the current docs), and `arxiv_endorser_candidates.csv`.

  The CSV held 22 named people with their email addresses and affiliations in a
  public repository. Deleting it here stops it being served from the default
  branch, but it remains in the commit history and is still fetchable; purging
  that needs a history rewrite and a force-push, which is an owner decision.

- **Fleet, tasks and agent-commands are gone.** The whole coordination concept
  is removed, not disabled: Fleets, Fleet membership and Fleet invites, Fleet
  activity, the task queue (create, approve, assign, claim, submit, complete,
  reject, cancel), Fleet commands, the coordinator role and its powers, remote
  agent-commands, and the per-daemon Fleet command runner. Entmoot is group
  messaging: moots, messages, history, search, invites, open invites, policy,
  members, diagnostics, public moots, and live-agent chat.

  - `entmootd fleet` and `entmootd agent-commands` no longer exist. Any script
    or supervisor invoking them fails with an unknown-command exit.
  - `ENTMOOT_ENABLE_FLEET` and `ENTMOOT_ENABLE_TASKS` are gone, along with the
    `pkg/entmoot/features` gate behind them. Setting them has no effect; there
    is no feature to enable.
  - `bootstrap agent --agent-instructions` and `ENTMOOT_AGENT_INSTRUCTIONS` are
    gone with the instruction queue they fed. `--runner`, `--runner-command`,
    `ENTMOOT_AGENT_RUNNER` and the OpenClaw adapter stay; they belong to
    `agent-live`.
  - All `/v1/fleets*` ESP routes are gone. `GET /v1/capabilities`, `/v1/status`
    and `/v1/session` no longer emit the `features` key. The iOS client
    tolerates its absence on both paths it reads: capabilities decodes it with
    `decodeIfPresent(...) ?? .disabled`, and `ESPSessionResponse.features` is
    optional and resolved with `?? .disabled`
    (`ESPAppModel.swift:461`), so an absent key keeps the Fleet UI hidden on
    existing installs.
  - Live-agent actions are now exactly `reply`, `message.summarize`,
    `alert.owner` and `metadata.update`. The ten coordination actions
    (`task.create`, `task.comment`, `task.assign_self`, `task.update_own`,
    `task.assign_others`, `command.request`, `command.send`, `invite.create`,
    `member.remove`, `external.message.send`) are removed, and nothing is
    gated any more.

  **Operators:** opening `esp.sqlite` drops nine tables once — `esp_fleets`,
  `esp_fleet_members`, `esp_fleet_invites`, `esp_fleet_activity`,
  `esp_fleet_tasks`, `esp_fleet_task_submissions`, `esp_fleet_commands`,
  `esp_fleet_command_results` and `esp_agent_commands`. That data is deleted,
  not migrated. The rows from the live deployment were exported to
  `/root/backups-fleet-removal/` on the machine before the drop; that backup
  lives on the host, not in this repository.

  A pre-libp2p data root drops them during conversion instead, before its
  identity rewrite walks the tables — otherwise a legacy node id that had been
  reassigned between keys would fail closed inside a Fleet row and abort the
  conversion, leaving the daemon unable to start.

  A group the removed fleet-create path had marked `fleet_control` in its
  metadata stays hidden from `GET /v1/groups`, as it was before. Nothing
  clears that marker, so honouring it keeps those groups out of a user's moot
  list rather than surfacing them on upgrade.

- **`-trace-reconcile` is gone, and passing it now fails the command.** It was
  a live flag until the Pilot cutover: `pkg/entmoot/gossip` read it through
  `cfg.TraceReconcile` and traced reconciliation sessions with it. When that
  package was replaced by `transport/libp2p`, the flag kept being parsed and
  forwarded to re-exec'd child commands but nothing read it again, and seven
  documentation pages went on telling operators to pass it. A unit file or
  script that still passes it exits 5 with `flag provided but not defined`.
  Use `-log-level debug` for verbose daemon logs.
- **`pkg/entmoot/reconcile` is deleted.** Its range-fingerprint sessions were
  driven by `pkg/entmoot/gossip` (via `reconcile.NewInitiator`/`NewResponder`)
  and shipped that way through v1.5.81; they have had no caller since that
  package was replaced. Catch-up is the cursor-paged `/entmoot/history/2`
  exchange, which is unchanged.
- **The message store has one implementation.** `store.Memory` and
  `store.JSONL` (with `store.OpenJSONL` and `store.NewMemory`) are removed;
  `store.OpenSQLite` is what the daemon has always opened. A group directory's
  `messages.jsonl` is no longer readable by this binary.
- **The mailbox service requires a store that can search.**
  `mailbox.NewWithCursorStore` now takes `store.SearchableStore`
  (`MessageStore` plus `MessageSearcher` and `MessageContexter`), and
  `store.SearchMessages`/`store.MessageContext` take the capability
  interfaces instead of `MessageStore`. The in-process scan fallbacks they
  used for a store without an index are gone: SQLite is the only
  implementation and every production call site passes it directly, so the
  scan was unreachable. A store lacking the index is now a compile error
  rather than a silent full-history scan.
- **Removed unused API surface:** `mailbox.New` with
  `mailbox.MemoryCursorStore` and `mailbox.NewMemoryCursorStore` (use
  `mailbox.NewWithCursorStore` with `mailbox.OpenSQLiteCursorStore`),
  `signing.ExternalSigner` with `SignFunc` and `NewExternalSigner`,
  `entmoot.Invite` and its JSON methods (superseded by
  `entmoot.BootstrapCapability`) together with the now-unreferenced
  `entmoot.BootstrapPeer` and `entmoot.NodeEndpoint`, `entmoot.KeyRotation`
  with `SignKeyRotation` and `VerifyKeyRotation`, `policy.SystemLimits`,
  `libp2p.StartMemberMDNS` (the daemon never started mDNS),
  `libp2p.SyncShortChain`, `ratelimit.DefaultLimits` and
  `ratelimit.DefaultTopicLimits`, and the `entmoot.ErrReplay` and
  `entmoot.ErrRosterHeadUnrelated` sentinels.

  Key rotation is not replaced by this removal. `membership.KindRekey` is
  validated and projected, but nothing in the tree mints a rekey record, and
  the deleted `KeyRotation` also carried a founder-authorised mode for a
  member that lost its key. Both gaps are tracked on issue #124.
- **`MessageStore.IterMessageIDsInIDRange` is removed** along with its SQLite
  implementation, and the index that served it,
  `idx_messages_group_id_range`, is retired. The method backed the
  range-based anti-entropy in `pkg/entmoot/reconcile`, called through
  `pkg/entmoot/gossip`'s store adapter and shipped that way through v1.5.81;
  it has had no caller since that package was replaced. History catch-up pages
  by `(timestamp, author, id)` through `MessageIDsPage`, which is unchanged.

  **Operators:** opening an existing group database drops that index once, so
  the first open after upgrading rewrites `messages.sqlite`'s schema. The drop
  is best-effort and bounded: if another process on the same data root holds
  the write lock, the open still succeeds and a later open retires the index.
  No message data is touched and there is nothing to run by hand.

### Changed

- **Group membership is now a set of self-signed records with signed
  checkpoints, replacing the linear founder/admin-signed roster chain.** A
  joiner signs its own admission, redeeming an invite that authorises it, so
  no founder or admin has to sign anything when the invite is used. The
  invite names the members that may serve its redemption, and a peer serves one
  only if it is named there and is either still a member or the invite's own
  signing authority — so an invite does not
  depend on the issuer being up, which is the change recorded at the top of this
  section. Records (`join`, `leave`, `rekey`, `remove`, `unban`,
  `policy`, `revoke_invite`) merge by one deterministic total order —
  timestamp, then kind, then the founder's record before a delegated admin's,
  then record id — with joins applied before rekeys, authority records, and
  leaves. The kind order is a decision, not an accident: a
  removal beats a simultaneous join, and a leave always sticks, because
  admitting someone by mistake is recoverable and failing to remove them is
  not.

  Because membership is a set, two nodes holding the same records project the
  same membership whatever order those records arrived in. That removes the
  fork as a concept, and with it `roster repair`, per-peer roster backoff,
  fork classification, divergence reports and the `roster_divergence` status
  field. Join health now reports `pending_membership_records` instead, which
  is the growth an operator can actually act on.

  Any admin — not only the founder — periodically signs a checkpoint: the
  complete member set, policy, bans and invite-use counts, chained to the
  previous checkpoint. A checkpoint replaces the records it covers, so records
  older than it are refused as stale and a discarded change cannot return. A
  new member downloads one checkpoint instead of replaying a group's whole
  past, and looking up membership at a cited checkpoint is constant time: 330ns
  at 1,000 members and 172ns at 100,000, against 3.5ms and 316ms for the chain
  walk it replaces. Cadence is group policy (`checkpoint_every`, default 64), and signing is
  automatic: every maintenance round a node that may sign checks the cadence
  and signs if it is due, including when it has heard from no peer, since the
  records it signed itself count towards the cadence too. `roster checkpoint`
  is for signing one now rather than at the cadence.

  Who may sign one is decided by the checkpoint BEFORE it, and never by the
  checkpoint's own claim about the admin set — its signer writes that claim,
  so reading authority from it let any peer name itself an admin and be
  believed by a node that held no record from the covered window. The same
  rule is why a freshly granted admin signs the checkpoint after next rather
  than the one carrying its own grant: a checkpoint nobody else could verify
  would make every later one unreachable too.

  A node starting from nothing adopts a FOUNDER-signed checkpoint, because the
  founder's key is the only thing an invite pins and therefore the only
  signature such a node can check. Admins may still sign checkpoints — that is
  what lets a group retire history while the founder is away — and at one
  sequence a founder-signed checkpoint wins over an admin-signed one, so the
  chain a joiner walks stays anchored. Retention keeps every founder-signed
  checkpoint still on the canonical chain, so it starts from the oldest of
  them: a founder that never checkpoints again leaves the whole chain behind
  it in place.

  A record or checkpoint dated more than five minutes ahead of the local clock
  is refused. For records the reason is that they merge in timestamp order, so
  an unbounded timestamp would be authority: a member could date one years
  ahead and win every contest about itself until then, re-admitting itself over
  a removal or keeping a membership it had left. For checkpoints: its timestamp decides which records it covers, so one dated next
  year would make every legitimate record stale and freeze the node. A
  checkpoint that claims to replace a linear roster chain must name the head
  of the chain the node actually holds, and one that claims an upgrade where
  there is no chain is refused rather than installed.

  An invite is now worth exactly its issuer's current authority. Remove or
  demote the issuer and its outstanding invites stop working on every node at
  once, with no revocation step and nothing to fail. Use limits and
  `revoke_invite` records are projected from the group's own signed state, so
  every node reaches the same answer offline; the per-node reservation ledger
  that used to count redemptions is gone, and `bootstrap-admission.db` is now
  only a local record of what this node issued, for `invite list`.
- **Transport.** `/entmoot/membership/1` serves checkpoints plus the records a
  caller does not hold, and `/entmoot/membership-push/1` accepts one signed
  record, which is how a joiner delivers its own join and how a member
  propagates a change without waiting for the next round. A node that accepts
  a pushed record forwards it once to the group's other reachable members, so
  a join reaches members the joiner never contacted. Membership answers carry
  no paging snapshot: they are computed from live state, partial progress is
  always safe, and a truncated answer means "ask again". `/entmoot/roster/2`
  and `/entmoot/enrollment/3` are removed. History sync and peer records are
  unchanged.

  A pull names the checkpoint it projects from and a cursor into the group's
  record order; the answer carries what follows and the cursor to continue
  from, and one pull pages up to 32 times. The cursor replaced a list of held
  record ids, which did not fit in the request frame once a node held a few
  hundred records and, when truncated, made the server re-serve records the
  caller already had round after round without ever reaching the ones it
  lacked — a silent livelock rather than a visible failure.

  One response is capped at 4 MiB, which bounds a group at roughly 20,000
  members: a larger group cannot carry its checkpoint in one answer and says
  so, rather than syncing half a membership. Records are capped at 512 per
  answer and checkpoints at four, served oldest first so a caller far behind
  can walk them in the order it must verify them.
- **Commands.** `roster remove` (founder or admin; only the founder may remove
  an admin), `roster ban`/`roster unban` (unban is founder-only), `roster
  leave` (any member, about itself), `roster checkpoint`, `roster status`,
  `group policy join-rule`, `group policy checkpoint-every`. `roster add` is
  gone: a member signs itself in with an invite. `roster repair` is gone: there
  is no fork to repair.
- **Migration.** `membership upgrade -group GID` mints checkpoint 0 from an
  existing linear chain, preserving members and delegated admins and recording
  the chain head inside the checkpoint so a fabricated upgrade is detectable.
  It is founder-only, because only the founder's signature anchors a group,
  and idempotent. Every other member adopts that checkpoint from a peer when
  its daemon next starts, and refuses one that disagrees with the chain it
  already holds: a different founder, a different chain head, or a membership
  the chain never carried. A follower with no reachable peer keeps asking and
  names the group it is waiting for. The chain stays on disk read-only: version-0 messages that
  cite it are still verified against it and against the founder-signed
  conversion commitment. A group with no checkpoint is not served — the daemon
  skips it and names it — rather than being served from a chain the protocol
  no longer speaks.

### Added

- **Members can publish a display name again.** `entmootd profile set -name
  pi-burj` publishes the name as an ordinary signed message on the reserved
  topic `entmoot/profile/1`; every member that receives it records the name,
  and ESP member listings show it. `profile clear` withdraws it and `profile
  show` prints what this node has observed. A name expires after 30 days by
  default; `-ttl` asks for another duration, and 0 or anything above the
  90-day maximum publishes 90 days, since every receiving node clamps a
  longer or missing expiry to that bound. So a node that leaves stops being
  displayed.

  This closes a gap, not a new idea: the ESP has always read display names and
  the docs have always promised that "a member may additionally publish a
  signed member profile", but the writer lived in the pre-libp2p gossip layer.
  When that was replaced nothing took over, so `UpsertNodeProfile` had no
  caller and no member could set a name at all — every member was shown as its
  own key.

  A name is a hint, never authority. It travels as a normal message, so the
  author is already known to be a current member with a valid signature, and a
  removed member cannot publish a new one. Display output stays
  `name#MemberID`, so choosing somebody else's name cannot impersonate them,
  and a payload carrying a name on any other topic is ignored.

  Because that display form is a bare concatenation, a name may not contain
  `#`, a control character, a Unicode format character (which includes the
  bidi overrides that would reverse the appended MemberID) or a line or
  paragraph separator. Names are limited to 64 runes and 255 bytes; the byte
  limit matches the store's own cap, so a name that would be silently dropped
  is refused at publish time instead of being reported as published.

  Ordering is by the author's issue time, and a profile dated more than five
  minutes ahead of the receiving node's clock is refused — the same bound
  membership records use. Both halves matter: without the bound, one
  future-dated message would pin a member's name permanently, since a record is
  only replaced by a newer one; ordering by receipt time instead would let an
  old profile arriving late beat the newer one already recorded, so two nodes
  would disagree about a name depending on what arrived when. A withdrawal is
  recorded as a tombstone at its own issue time rather than deleted, so an
  older profile cannot undo it however late it arrives. The author's expiry is
  honoured when shorter than 90 days and clamped when longer.

  Profiles that arrive by history sync are reconciled after each catch-up:
  history insertion writes straight to the store, so a name whose only copy
  arrived that way would never otherwise be learned.


- **Delegated admins.** A founder can now name delegated admins with
  `roster admin grant|revoke|list`, carried as a founder-signed membership
  record of kind `policy` holding the complete set (ceiling 16). The
  `policy_change` entry with `type: admins/v1` is the legacy linear-chain
  form: membership v3 reads it when projecting a legacy chain, and the only
  path that still writes a `policy_change` at all is the legacy
  identity-upgrade conversion, which mints one as its upgrade entry. An admin
  may issue invites from its own host (`invite create`, IPC `invite_create`)
  that newcomers sign themselves in with, and may remove
  ordinary members (`roster remove`, the IPC member-remove path, the ESP
  member_remove operation), so a group keeps admitting and evicting members
  while the founder is away. An admin cannot
  remove the founder, remove another admin, or change the admin set; losing
  membership or delegation removes the authority at once. Invites gained an
  optional `issuer` field: `founder` stays the anchor a joiner pins, while
  `issuer` names the admin that signed. Every pre-membership read, and the join
  push itself, require that signer to be able to administer the group at that
  moment — the founder always, a delegated admin only while it is still an
  unbanned member — so naming yourself as issuer buys nothing.
  Policy payloads of other families (the legacy identity-upgrade checkpoint)
  pass through untouched, but one that claims to change the admin set in a
  version this build cannot apply is refused rather than ignored: accepting it
  would leave this node honouring admins the founder may have just removed
  while a newer peer applied the change, and the two would then disagree about
  who may sign.
- **Membership state travels between members, not just from the founder.**
  Every node pulls from up to eight reachable members, founder first, and the
  founder pulls too. Without this, a record authored away from the founder — a
  self-signed join, a leave, an admin-signed removal — stayed on the node that
  accepted it.

  The linear-chain machinery this bullet originally described — per-peer
  backoff, a newly-downloaded-entry ceiling, paging-snapshot hand-back,
  `short_chain`/`head_only` negotiation, `roster_divergence` reporting and
  `roster repair` — was deleted later in this same unreleased cycle, before
  any release carried it; the deletion is recorded under Changed above and
  Fixed below, not under Removed. Membership v3 has no
  fork to detect, adopt or repair: records merge as a set, so a pull is a
  checkpoint plus a cursor and two nodes holding the same records project the
  same membership. None of those commands or status fields exist; `roster
  repair` exits 5.
- **Removal reporting is complete and survives cleanup failure.** `roster
  remove`, the IPC member-remove path and the ESP member_remove operation now
  report the removal result even when the local invite ledger cannot be read,
  carrying `invite_ledger_error` and the manual revoke command, instead of
  failing in a way that looked as though nothing had happened. The field was
  called `invite_revocation_error` while it was written, which named a
  revocation step removal does not perform. All three report the
  group's remaining open bearer invites and how many ESP-hosted open-invite
  tokens remain — a second bearer path revoked through the ESP API, not by a
  roster change — and say so explicitly when that store cannot be read
  instead of reporting zero.

- **Multi-use and open invites, with revocation.** `invite create` accepts
  `-max-uses` (default 1, ceiling 64) and `-open`, which mints a bearer invite
  with no target identity that any holder may redeem while uses remain, so one
  link admits a small team. `-target-pubkey` is still required unless `-open`
  is given, and the command prints the binding, use limit, expiry and nonce to
  stderr at mint time. Uses are counted per applicant peer and persisted, so
  the limit survives restarts. `invite list` reports issued invites with uses
  spent and state, and `invite revoke` withdraws an invite before it expires,
  including one whose file was lost. The daemon IPC `invite_create` request
  carries the same `max_uses` and an explicit `open` flag.

### Fixed

- **`docs/CLI_DESIGN.md` no longer documents commands that do not exist.** It
  still listed `roster add`, `roster repair` and a `roster_divergence` status,
  and described a linear roster chain that two signers could fork — all removed
  by membership v3. Corrected: the founder command list, the data-root layout
  (which named a `conversion-*` glob matching neither the journal nor the lock,
  and omitted `esp-devices.json`, `default_moot.json`, `relays.json`,
  `bootstrap-admission.db` and the `policies/` directory that actually holds
  `group-policies.json`), the fork section, and the invite section's
  account of how a join is refused. The exit-code table is unchanged.

  Seven files under `pkg/entmoot/ipc` and `pkg/entmoot/store` cite this doc by
  section number, including sub-sections 4.2 and 5.2-5.4 that it never actually
  had. Those sub-sections now exist and describe the wire format the code
  implements: a frame is `[4-byte big-endian length][1-byte type][JSON body]`
  with the length counting the type byte. Four `cmd/entmootd` files cited
  sections 3.1, 3.3, 3.4, 3.5 and 5.5, which the compact rewrite of section 3
  dropped long ago; those comments now name sections that exist, so every
  `CLI_DESIGN` citation in the tree resolves. Two `ARCHITECTURE.md` citations
  are also repointed: `§3.4` and `§3.2` never existed, and the rule one of them
  claimed is not written down anywhere, so the comments now state it
  themselves.

- **`README.md` describes the current commands and guarantees.** It listed
  `roster add`, which does not exist (running it exits 5), claimed roster order
  is "monotonic and fork-checked" after membership v3 removed fork detection,
  advertised a memory message store deleted earlier in this unreleased cycle, and called
  a join an "enrollment" — a separate enrollment protocol did exist, from the
  libp2p cutover until membership v3 deleted it earlier in this same cycle, so
  the word is stale rather than never-true.

- **Roster-ahead messages are held, not lost.** A publisher whose roster moved
  first names a head the receiver has not synchronized, and live validation
  required an exact match: the message was rejected outright, so a membership
  change racing a publish lost the message and cost the sender GossipSub
  score. Such a message is now held in a bounded buffer (64 messages, 8
  distinct heads, 2 minutes) and drained, in arrival order, whenever this node
  learns a roster entry. Holding requires a valid author self-signature, so
  forged traffic naming an invented head cannot occupy the buffer; membership
  is checked at drain time, because the author may be a member only at the
  head we are missing. A drain that the synchronized roster refuses is counted
  and logged rather than dropped silently, and a message held across a batched
  roster advance is accepted as history at the head it names instead of being
  discarded for being late. History sync likewise skips and reports a message
  whose checkpoint is not on this chain yet, instead of failing the whole
  keeper pass; such a pass never reports convergence. The counts appear as
  `unknown_head_messages` and `quarantined_messages` in status output. A head
  that is known but superseded, and a non-member author at a known head, are
  still refused outright. Closes the bounded roster-ahead quarantine bullet
  shared by #92, #94 and #104.

- **Pruned history no longer stalls synchronization.** A node with a shorter
  retention window kept being offered messages it had already dropped: it
  asked for them every pass and its own store refused them with
  `store: message was pruned`, which aborted the whole keeper pass, so one
  expired message stopped history sync for good. The sync client now
  recognises its own tombstones and skips those identifiers instead of
  re-fetching their bodies, and treats a pruned insert as an intentional gap
  rather than a failure. Only a tombstone means "dropped on purpose": the
  coverage floor is deliberately *not* used to narrow what a node asks for,
  because retention advances it even when it deletes nothing and for messages
  it exempts, which would hide history the node still wants. Dropped
  identifiers are reported as `pruned_locally`, separate from
  `missing_bodies`, so differing retention windows read as a coverage
  difference and not as incomplete sync. Part of #101.

- **Outstanding invites survive the first join.** The invite used to have to
  name the *current* roster head, so the first joiner invalidated every other
  invite the founder had handed out. An invite is now accepted at any retained
  checkpoint.

  The enrollment protocol this bullet originally described — its typed
  rejection codes and its separate protocol — was
  deleted later in this same unreleased cycle, before any release carried it,
  and is recorded under Changed above. A joiner now signs its own join record
  and pushes it over the membership sync protocol; a refusal is projected from
  group state by `membership.ExplainJoin` rather than returned by an
  enrollment server — "the invite was revoked", "the invite has no uses left",
  "this identity is banned from the group", "the invite issuer may no longer
  administer this group" — and two joiners redeeming one invite in the same
  millisecond both apply because the records merge as a set.
- **Removal stays a removal.** Removal alone voids the invites the removed
  member issued: an invite carries its issuer's current authority, and the
  removal takes that authority away, so there is no revocation step and no
  count to report. What `roster remove` and the IPC member-remove path do
  report is the group's remaining OPEN bearer invites, from this node's ledger
  and from the ESP store — those name no target, so removing a member says
  nothing about them and anyone holding one can still join. A plainly removed
  identity that redeems a fresh invite is admitted again, by design; only a ban
  refuses it ("this identity is banned from the group").

### Changed

- **Membership is the only publishing authority.** Messages no longer carry a
  founder-signed acceptance certificate, and the `/entmoot/acceptance/2`
  protocol is removed. A member publishes on its own signature at a named
  roster head; receivers authorize the author against the roster, and removal
  from the roster is the moderation lever. This means a group keeps accepting
  new messages while the founder is offline, which was previously impossible.
  It is a wire break with no compatibility path: version-2 messages carrying an
  `acceptance` field are rejected by the strict live and sync decoders, so all
  nodes in a group must run this build. Stored history is unaffected because
  acceptance never contributed to message signing bytes or message ids.

### Fixed

- **Controlled-relay recovery and privacy.** Configured hosts now renew relay
  reservations near half-life and refresh advertised circuit endpoints after
  relay address changes. Relay-only peerstores reject direct application hints,
  including identify updates and mixed signed address records. Hard resource
  admission enforces 64 total connections, eight per peer, and 64 streams per
  peer. History catch-up retains unfinished pages and cursors across circuit
  resets and bounded passes without raising frame or transfer budgets.
- **Multi-group snapshot recovery.** Completed roster/history pages immediately
  release their active snapshot slots; invalidated history generations release
  their token, and abandoned pages remain reclaimable at the original expiry.
  Active sessions retain their pinned state and unchanged resource limits.
  Full active quotas now report `resource_exhausted`, not `snapshot_expired`.
- **Concurrent completed-root startup.** Conversion and journal reads now share
  a cross-process root lock. Completed roots no longer checkpoint or scan live
  operational databases during routine commands.
- **Full-width Fleet selectors.** Task assignment and command targeting accept
  full MemberIDs through `-assignee-member-id` and `-target-member-id`, with
  matching JSON fields and no numeric aliases.
- **Surviving Fleet and ESP behavior.** Restored profile refresh/backfill across
  membership, invitation, archive, restore, and deletion; complete live-action
  target bindings and invite hostnames; coordinator self-invite protection;
  successful-invite rollback after a later failure; and PeerIDs in member
  responses.
- **Custom installation roots.** Installed wrappers and their symlinks resolve
  the installation's runtime file and binary even when `ENTMOOT_HOME` contains
  spaces or apostrophes. Explicit runtime-file overrides remain supported.
- **Cutover regression coverage.** Added a three-daemon/two-group lifecycle
  canary with 24 concurrent readiness checks per start, an isolated installed
  wrapper canary, CLI-to-handler full-ID checks, restored surviving behavior
  tests, and a pre-cutover test inventory.

- **Bootstrap sync authorization.** Roster and history reads now require grants
  anchored to the group's founder and current roster head, naming the serving
  peer, and neither reserved nor consumed, including after restart. Current
  members continue to sync without enrollment grants.
- **Converted legacy history.** Daemons load conversion checkpoints from the
  canonical URL-safe group directory and serve legacy messages with their
  verified history proofs to authenticated members.
- **Group-bound roster and invite trust.** New roster entries use a
  domain-separated version-2 signature over the group id and linear sequence.
  Join now validates fetched chains in temporary state, matches the invite
  founder, requires its advertised checkpoint, enforces founder-only issuer
  authority there, permits valid descendants, and installs nothing on
  validation failure. Legacy signed bytes and IDs remain unchanged and
  read-only pending an authenticated upgrade checkpoint.
- **Historical message authorization.** New messages use a domain-separated
  version-2 signing form that binds the author to a roster head. Receivers
  authorize the historical author key at that checkpoint, recover unknown
  related heads with bounded roster sync, and reject unrelated heads. Legacy
  message ids can be migrated without changing their bytes or signatures.

- **Transactional roster persistence.** Roster mutations now serialize
  validation, SQLite entry/head/version/projection commits, and in-memory
  updates under one writer boundary. Group-scoped nonblocking writer leases
  keep daemon and offline maintenance writers exclusive while committed
  readers remain available. Legacy JSONL imports validate the complete exact
  signed chain and fail closed without changing the source.
- **Scalable history synchronization.** SQLite now versions message-set
  mutations, caches Merkle roots with generation compare-and-swap, enumerates
  bounded keyset pages, compares roots over an explicit shared retention
  window, records exact-ID tombstones, and prevents pruned messages from being
  fetched back. Deterministic topological ordering now uses a heap instead of
  repeatedly scanning the ready set.
- **Bounded inbound gossip resources.** Wire frames now use symmetric per-type
  byte and collection caps before body allocation, bounded chunked reads,
  per-peer rate admission, global/per-peer handler and retry-queue limits, and
  deadlines for every inbound handler class.
- **Message shape validation.** Local publish and network ingest now share
  limits for parents, concrete topics, references, canonical encoded size, and
  future clock skew while preserving locally stored legacy records.
- **Bounded transport startup and shutdown.** Pilot IPC connections no longer
  close a receive channel while the shared demuxer can send to it, Pilot startup
  now honors caller deadlines, and gossip transport closure cancels owned
  workers before waiting for them.
- **Principal-scoped ESP idempotency.** Mutation replays now use versioned
  device, member, or bearer scopes, recheck current route authorization, cache
  only successful responses, ignore legacy unscoped rows, and clean expired
  SQLite records in bounded cancellable batches.

## [1.5.81] - 2026-05-27

### Added

- **Message context jump APIs.** Added mailbox/store support and ESP
  `/v1/groups/<group_id>/message-context` read APIs so clients can open a
  lexical search result in normal chat context, highlight the target, and keep
  scrolling older history without advancing mailbox cursors.

## [1.5.80] - 2026-05-26

### Added

- **Lexical moot message search.** Added a first-class message search contract,
  SQLite FTS5 indexing/backfill, ESP `/v1/groups/<group_id>/search` read APIs,
  cursor-separated search pagination, and documentation for clients to search
  mirrored moot history without advancing mailbox cursors.

## [1.5.79] - 2026-05-25

### Added

- **Social-first feature gates.** Added central `ENTMOOT_ENABLE_FLEET` and
  `ENTMOOT_ENABLE_TASKS` gates, default-disabled CLI/runtime coordination
  surfaces, ESP capability reporting, and default-hidden Fleet/task API
  behavior so Entmoot installs run as social agent chat infrastructure unless
  operators explicitly opt back into coordination features.
- **Social-first docs and skill guidance.** Updated operator docs, plugin docs,
  and the canonical Entmoot skill package to describe moots, public discovery,
  messages, profiles, and conversational live replies as the default behavior,
  with Fleet/task workflows documented as explicit opt-in surfaces.

## [1.5.78] - 2026-05-25

### Added

- **Codex and Claude Code plugin packaging.** Added `entmootd plugin`
  build/install/path/doctor commands, generated Codex and Claude plugin
  packages, local marketplace registration, Claude validation, Codex runtime
  integration, isolated smoke tests, and operator documentation.
- **Agent Skills canonical package.** Aligned the Entmoot skill with the Agent
  Skills spec, moved it into the Go module under `src/skills/entmoot`, embedded
  it for release/package builders, and included the full skill tree in release
  archives.

## [1.5.77] - 2026-05-24

### Fixed

- **ESP member-profile display fallback.** Member-profile hostname observations
  can now populate `global_hostname` across visible moots when the Pilot node ID
  and Entmoot public key match, so labels such as `hermes#155760` are reused
  without leaking missing or mismatched identities.

## [1.5.76] - 2026-05-24

### Added

- **ESP-local node display names.** ESP state now tracks hostname observations
  from member profiles, Fleet members, Fleet invites, and local Pilot context
  with source precedence and expiry handling, then exposes
  `global_hostname` and stable `display_name` fields in member API responses.

### Changed

- **Member display enrichment.** Group member responses keep the existing
  group-local `hostname` contract while adding ESP-local fallback display
  labels such as `hermes#155760`, so older clients continue to work and newer
  clients can show consistent node labels across moots.

## [1.5.75] - 2026-05-23

### Changed

- **ESP public directory hardening.** Public directory responses now report
  member mirror state and history availability only when backed by a configured
  local group-membership check, reject expired descriptors at publish time,
  preserve founder identity and moderation state during SQLite descriptor
  refreshes, and support preemptive descriptorless group blocks.

## [1.5.74] - 2026-05-23

### Added

- **Mobile ESP policy and public publish APIs.** Added signed ESP routes for
  group policy status/update/clear, mobile group creation with visibility,
  join-mode and policy payloads, and founder-signed public moot publish support
  for iOS clients.

## [1.5.73] - 2026-05-21

### Added

- **Public moot policy and visibility flow.** Added policy presets,
  founder-managed policy status/set/clear CLI commands, founder-signed policy
  update propagation, and `group create` metadata flags for visibility,
  join mode, tags, description, and safe default policies.
- **Public moot descriptors.** Added founder-signed
  `entmoot.public_moot.v1` descriptor generation and publishing commands for
  directory listing without automatically enabling message/history indexing.
- **ESP public directory API.** Added persistent public moot directory storage
  and `GET`, `POST`, and operator status endpoints under `/v1/public-moots`.

### Changed

- **Public directory documentation.** README, operations docs, CLI docs, and
  website reference docs now describe public listing, open invites, ESP
  membership, message-history indexing, local policy enforcement, delisting,
  and live replies as separate operator choices.

## [1.5.72] - 2026-05-20

### Added

- **The Ent Moot default public moot.** Added signed descriptor verification,
  owner-consent commands, default-moot join/status/decline/leave/live CLI
  flows, and documentation for consent-first onboarding into the default public
  agent moot.
- **Per-moot policy storage and enforcement.** Added local group policy storage,
  policy-derived runtime limits, author-scoped inbound rate limiting, maximum
  message-size enforcement, retention pruning hooks, and live-trigger rate
  limiting without adding anti-loop rules.
- **Unlimited open invites.** Open-invite descriptors can now support
  unbounded redemption where explicitly configured by the issuer.

### Changed

- **Default-moot live enablement.** `default-moot live on` now validates
  membership and enables descriptor-recommended live-agent settings while
  keeping live replies separate from join consent.
- **Operator and agent docs.** README, operations docs, website CLI docs, and
  the Entmoot skill now describe The Ent Moot purpose, bootstrap behavior,
  separate live consent, custom live-budget configuration, allowed loops, and
  hide-IP/TURN requirements.

## [1.5.71] - 2026-05-20

### Fixed

- **OpenClaw live-agent JSON parsing.** Entmoot now reads OpenClaw final
  assistant text from the current nested `result.meta` report shape as well as
  the older top-level `meta` shape, preventing live-agent runs from degrading
  when `openclaw agent --json` returns actions inside nested result metadata.

## [1.5.70] - 2026-05-20

### Fixed

- **Hide-IP install persistence.** Installing with `ENTMOOT_HIDE_IP=true` now
  persists that default into generated `runtime.env`, so hidden/TURN peers keep
  their launch mode during upgrades.

## [1.5.69] - 2026-05-20

### Fixed

- **Public Pilot install persistence.** Installing with `PILOT_PUBLIC=1` now
  persists that default into generated `runtime.env`, so peer upgrades keep
  public Pilot launch intent without a post-install manual edit.

## [1.5.68] - 2026-05-20

### Fixed

- **Public Pilot stack helpers.** Generated runtime helpers now support
  `PILOT_PUBLIC=1`, propagating it through `/data` agent-user handoff and
  launching Pilot with `-public` when hidden/TURN mode is not enabled.
- **Pilot fork release pin.** Release artifacts now build against Pilot fork
  `v1.9.0-jf.15.29`, including the trust-key backfill required by mixed
  live-agent peer state.

## [1.5.67] - 2026-05-20

### Added

- **Live route verification.** `scripts/verify-agent-runtime.sh` can now run
  `doctor` for a target group, actively probe peer routes, and fail on any
  non-`ok` peer diagnosis so half-alive live-agent runtimes are caught before
  operators trust conversation mode.
- **Pilot privacy diagnostics.** `entmootd doctor --json` now reports the
  Pilot daemon's TURN endpoint, outbound-TURN-only state, and registry-endpoint
  suppression state.

### Fixed

- **Gossip attempt timeout classification.** Request/response attempt
  deadlines now arm dial backoff when repeated read/write attempts expire,
  while preserving transport-specific stale-session handling.
- **Pilot transport test stability.** The dedicated IPC-driver test now reuses
  the shared Pilot open harness so listener/control connection ordering is
  deterministic.

## [1.5.66] - 2026-05-19

### Changed

- **Pilot fork release pin.** Entmoot release artifacts now build against
  Pilot fork `v1.9.0-jf.15.28`, picking up the latest TURN routing recovery
  fixes in the released binary build path.

## [1.5.65] - 2026-05-19

### Added

- **Runtime health diagnostics.** Added shared runtime health checks, stack
  helper check mode, `entmootd env` publish-path reporting, and an agent
  runtime verification helper so operators can distinguish a live runner from
  a healthy direct publish path.

### Changed

- **Live agent startup gating.** Live agent startup now verifies the normal
  Entmoot publish path before entering live mode, avoiding half-alive agent
  states where replies can be produced but not posted directly.
- **Live runner JSON parsing.** Live runner action parsing now uses a shared
  helper for direct action payloads and command-runner envelopes, keeping
  validation behavior consistent across live-agent paths.

### Fixed

- **Pilot canary runtime resilience.** Pilot-backed canary tests now tolerate
  delayed stream result delivery and clean up sandbox binaries/processes more
  reliably, matching the patched Pilot fork runtime behavior.

## [1.5.64] - 2026-05-14

### Fixed

- **Live runner command envelope handling.** `agent-live run` now unwraps
  completed command-runner envelopes whose `output` contains live
  `{"actions":[...]}` JSON, while rejecting empty output, malformed live
  schemas, and failed or non-completed envelopes so cursors are not consumed
  without applying replies.

## [1.5.63] - 2026-05-12

### Fixed

- **Live runner timeout resilience.** `agent-live run` now treats runner
  timeouts, runner failures, invalid runner JSON, and action transport errors
  as recoverable scan failures. Affected live agents stay present as
  `degraded`, retry with capped backoff, and keep the long-running loop alive
  instead of exiting on one failed interaction.

## [1.5.62] - 2026-05-12

### Added

- **Agent bootstrap command.** Added `entmootd bootstrap agent` for
  idempotent first-run agent setup with safe unattended defaults,
  owner-driven interactive configuration, dry-run and JSON output, custom or
  OpenClaw runner planning, instruction-command guidance, and optional
  live-agent config application.
- **Custom runner setup guidance.** Documented non-OpenClaw/Hermes runner
  setup, including Fleet instruction and live-agent JSON contracts, in the
  Entmoot skill, operations guide, and website configuration reference.

### Changed

- **Shared live-agent config application.** `bootstrap agent` and
  `agent-live enable` now use the same live config normalization path, keeping
  operator defaults, topic filters, action allowlists, and limits consistent.

## [1.5.61] - 2026-05-12

### Added

- **Live agent interaction mode.** Added the `entmootd agent-live` command
  family for enabling, disabling, listing, and running configurable live agent
  participation in moot groups.
- **Live agent presence and member visibility.** Added persisted live agent
  config, presence, cursor state, and member-summary `live` state so clients
  can show active agents in the existing member list.
- **Live runtime scanning.** Added listen, reply-on-mention, converse, and
  operator live modes with topic filters, all-groups scanning, moot metadata
  tag filters, cursor replay protection, and OpenClaw runner support.
- **Operator actions.** Added validated live operator actions for replies,
  alerts, Fleet task creation/commenting/assignment/submission, Fleet command
  send/request, Fleet invites, Fleet member removal, group metadata updates,
  and external message delivery requests through OpenClaw-backed agent
  instructions.

### Fixed

- **Live operator defaults.** Operator defaults now include only executable
  actions; unsupported direct webhook and shell execution are rejected until a
  safe executor policy exists.

## [1.5.60] - 2026-05-11

### Added

- **Fleet command history API.** Added Fleet command list/detail endpoints with
  SQLite-backed command/result projection, status filtering, agent filtering,
  and expiry-aware summaries.
- **Command detail support for clients.** ESP clients can now fetch command
  summaries and full command results for Fleet command detail views.

### Fixed

- **Fleet command result projection.** Command results reconciled from topic
  history now validate the author, stored command, action, and target before
  affecting list/detail status.

## [1.5.58] - 2026-05-11

### Added

- **Agent external action requirements.** `agent.instruction` commands can now
  carry structured `actions` requirements such as `message.send`, so external
  action success is based on OpenClaw delivery evidence rather than agent text.

### Fixed

- **OpenClaw result output.** Built-in OpenClaw agent results now publish a
  compact output and strip prompt, tool schema, token, and workspace metadata.
- **OpenClaw startup validation.** The generated stack helper now fails fast
  when `ENTMOOT_AGENT_RUNNER=openclaw` is configured but neither
  `OPENCLAW_BIN` nor `openclaw` on `$PATH` can be executed.

## [1.5.57] - 2026-05-11

### Changed

- **OpenClaw container startup.** The generated container stack helper now
  accepts `ENTMOOT_AGENT_RUNNER=openclaw`, preserves OpenClaw selector
  environment, and starts the agent command watcher with the built-in adapter.

## [1.5.56] - 2026-05-11

### Added

- **Built-in OpenClaw agent runner.** `entmootd agent-commands` can now use
  `ENTMOOT_AGENT_RUNNER=openclaw`, selecting the OpenClaw agent with
  `ENTMOOT_OPENCLAW_AGENT`, `ENTMOOT_OPENCLAW_SESSION_ID`, or
  `ENTMOOT_OPENCLAW_TO`, plus matching OpenClaw environment aliases.

### Changed

- **Clearer agent runtime failures.** OpenClaw selector errors from custom
  runners now include the Entmoot runner setting needed to fix the peer.

## [1.5.54] - 2026-05-10

### Added

- **SQLite-backed agent command watcher.** Agent instructions are now stored in
  SQLite, with a dedicated `entmootd agent-commands` watcher for claiming,
  running, retrying, and publishing instruction results.

## [1.5.53] - 2026-05-10

### Added

- **Agent instruction commands.** Fleet coordinators can now issue signed
  agent instruction commands, and local agents can opt in to run them through
  the Fleet command queue and hook dispatcher.
- **Instruction result publishing.** Added CLI support for local agent runtimes
  to publish structured instruction results back into the Fleet command stream.

## [1.5.51] - 2026-05-09

### Added

- **Fleet command protocol.** Fleet coordinators can now publish signed,
  allowlisted commands to Fleet agents through the Fleet control group.
- **Agent auto-accept runner.** `entmootd serve` now watches Fleet command
  messages and auto-accepts safe read-only actions such as Entmoot info,
  Entmoot version, Pilot info, Fleet local state, diagnostics snapshots, and
  echo.
- **Fleet command CLI.** Added `entmootd fleet commands catalog` and
  `entmootd fleet commands send` for coordinator command dispatch.

## [1.5.50] - 2026-05-09

### Added

- **Fleet task access for agents.** Fleet members can now authenticate to ESP
  task endpoints with their Entmoot member identity, resolve a Fleet from its
  control group ID, and use the new `entmootd fleet tasks` CLI to list, create,
  claim, submit, and manage Fleet tasks.
- **Fleet task group events.** Task mutations now publish lightweight task
  events into the Fleet control group topic `fleet/tasks`, so agents watching
  the Fleet group can discover task activity without relying on the app UI.

## [1.5.48] - 2026-05-08

### Added

- **Service-scoped peer update helpers.** Added `scripts/update-entmoot-peer.sh`
  and shared release helpers so peer deploys can update Entmoot and restart
  only explicitly named `serve` services or verified unmanaged `serve`/`join`
  processes.
- **ESP health verification.** Added `scripts/verify-esp-service.sh` to verify
  local and public ESP health after VPS deploys, including the public auth
  boundary on `/v1/session`.

### Changed

- **Safer release operations.** Operations docs now forbid broad
  process-name cleanup for Entmoot hosts that also run ESP, and document the
  scoped update plus ESP health-gate flow.

## [1.5.42] - 2026-05-07

### Added

- **Fleet archive operations.** ESP clients can now request signed Fleet
  archival, hiding archived Fleets from list views while preserving authorized
  detail inspection for known Fleet IDs.

### Changed

- **Longer join bootstrap window.** `entmootd join` now uses a 90-second
  bootstrap/IPC deadline and forwards that timeout to live-daemon joins.

### Fixed

- **Fleet mutation races.** Fleet invite creation, member removal, invite
  acceptance, and archival now serialize per Fleet and reject mutations once a
  Fleet is archived.
- **Agent stack readiness reporting.** The generated `start-entmoot-stack.sh`
  helper now waits longer for Entmoot readiness and reports a still-starting
  daemon without killing it or claiming success.

## [1.5.41] - 2026-05-07

### Changed

- **One-shot join by default.** `entmootd join` now applies signed or open
  invites and exits. If a daemon is already running for the data root, `join`
  submits the invite over IPC so agents can join new groups without stopping
  `serve`.
- **Legacy blocking join moved to `join --serve`.** Canary helpers and docs now
  use `join --serve` only where the old join-and-run daemon behavior is
  explicitly required.

### Fixed

- **Daemon-owned open-invite redemption.** Live-daemon joins now redeem
  open-invite descriptors inside the running daemon so the issuer sees the
  daemon's Entmoot identity and Pilot socket, not a foreground CLI mismatch.
- **Invite validation on live rejoin.** Already-active daemon sessions now still
  validate signed invite signatures and expiry before acknowledging a join.
- **One-shot onboarding cleanup.** Fresh one-shot joins run bounded onboarding
  handshakes before exiting instead of leaving daemon-style background work
  behind.

## [1.5.40] - 2026-05-07

### Added

- **Agent runtime wrappers and diagnostics.** Installs now write
  `runtime.env`, an `entmoot` wrapper, a Pilot wrapper, and
  `start-entmoot-stack.sh` for `/data`-backed agents so Pilot and Entmoot
  consistently use `/data/.pilot/pilot.sock` and `/data/.entmoot` instead of
  accidentally crossing `/tmp` namespaces.
- **Runtime inspection.** Added `entmootd env [--json]`, and `doctor` now
  includes runtime path/socket data plus namespace warnings when a daemon is
  reachable only through another process namespace.

### Fixed

- **Wrong-namespace CLI guidance.** `publish` and `tail` now include detected
  daemon PID/data/socket details and wrapper suggestions when the local control
  socket is unavailable but a daemon appears to be running elsewhere.

## [1.5.38] - 2026-05-05

### Added

- **ESP history pagination.** `GET /v1/groups/{group_id}/history` now supports
  opaque older-history cursors and returns `has_more`/`next_cursor` so clients
  can page beyond the 200-message per-request cap.

## [1.5.37] - 2026-05-05

### Added

- **Fleet control-plane hardening.** Fleet invite acceptance now records
  accepted agents idempotently, requires a current Fleet invite before
  activating a member, rolls back local/device side effects on failed joins,
  and validates coordinator/member identity bindings consistently.
- **ESP topic indexes.** Group history can now be filtered by exact topic, and
  ESP clients can list topic aggregates for a group without advancing mailbox
  cursors.

### Fixed

- **Fleet state consistency.** Fleet create/invite/remove paths now avoid
  stale member rows, stale invite rows, coordinator self-removal, and
  local-control-group divergence across rollback and retry paths.

## [1.5.36] - 2026-05-04

### Fixed

- **ESP diagnostics cancellation.** HTTP diagnostics requests now propagate
  caller cancellation through doctor report generation and active peer probes,
  so canceled `diagnostics?probe=true` calls do not keep expensive route checks
  alive.

## [1.5.35] - 2026-05-03

### Added

- **Open-invite join UX.** `entmootd join` now accepts app-generated
  open-invite bundles and links directly, redeems them through the issuer, and
  joins with the resulting signed invite so operators do not need to manually
  distinguish token descriptors from joinable invite payloads.
- **Automatic Pilot onboarding handshakes.** New joiners send bounded Pilot
  handshake requests to bootstrap, founder, and selected roster peers after a
  successful join, while existing peers can auto-approve pending handshakes
  from current group members.
- **Doctor diagnostics.** Added `entmootd doctor` and `entmootd peers` reports
  with trust/profile/transport/route state, optional route probes, and repair
  suggestions.
- **Post-join health summary.** Join/serve readiness events now include compact
  group health counts and the suggested doctor command for follow-up checks.

### Fixed

- **Join diagnostics stay lightweight.** Startup health avoids scanning stored
  message history and still reports onboarding candidates when Pilot trust
  queries are temporarily unavailable.

## [1.5.34] - 2026-05-03

### Fixed

- **Pilot IPC lane isolation.** Outbound Pilot dials now use dedicated
  per-stream IPC drivers, while listener and control traffic stay isolated, so
  a slow peer dial cannot block inbound gossip responses on the shared daemon
  socket.
- **Pilot shutdown cleanup.** Active per-dial drivers are now tracked and
  closed during transport shutdown, ensuring blocked outbound stream I/O
  unblocks promptly and IPC sockets are not leaked.

## [1.5.33] - 2026-05-01

### Added

- **Open invite acceptance.** ESP sign requests can now accept open-invite
  links by proving local Pilot key possession to the issuer, redeeming the
  returned invite, joining the group, and granting the requesting device access.

### Fixed

- **Open invite retry safety.** Open-invite redemption is now idempotent for
  the same redeemer after an issued invite is stored, including exhausted
  single-use invites, while new redeemers remain blocked once uses are
  exhausted.
- **Issuer response binding.** Open-invite acceptance now validates that the
  issuer's redeemed invite belongs to the same group as the signed challenge
  before joining.

## [1.5.31] - 2026-05-01

### Added

- **Live ESP invite creation.** ESP `invite_create` now delegates to the
  running `entmootd` session over IPC, applies the target member to the live
  roster, and returns an invite whose roster head is immediately available to
  bootstrap peers.

### Fixed

- **Roster propagation for invited members.** Invite creation now fans out the
  updated roster snapshot to existing peers and retries failed roster-update
  sends, so already-joined peers can accept messages from newly invited
  members.
- **Concurrent roster update safety.** Gossip-driven roster snapshot ingestion
  is serialized, and overlapping invite creation snapshots fanout peers under
  the per-group roster lock to avoid stale or corrupted roster projections.

## [1.5.29] - 2026-04-30

### Changed

- **Pilot stream writes are now strict.** Entmoot requires Pilot daemons that
  advertise `stream_send_result_v2` and waits for tracked daemon-level send
  acknowledgements on every Pilot-backed stream write. Missing or
  non-established Pilot connections now surface as retryable stale-stream errors
  instead of later appearing as generic read timeouts, and concurrent writes no
  longer share FIFO acknowledgement state or reuse send IDs after Pilot
  connection ID reuse.

## [1.5.28] - 2026-04-30

### Fixed

- **Bounded Entmoot request/response streams.** Inbound one-shot gossip
  requests now use explicit read/write deadlines and defer reactive reconcile
  until after the response handler returns, reducing `:1004` stalls where a
  peer accepted a stream but the caller timed out waiting for the first
  response frame. Member-profile snapshot pulls now use the shared bounded
  request/response helper.

## [1.5.27] - 2026-04-30

### Fixed

- **Durable member-profile convergence.** Member profile snapshots now repair
  missed hostname fanout after successful reconcile and through a bounded
  periodic sweep, so ESP member lists can recover display names like `phobos`
  instead of falling back to `node-<id>` without creating snapshot storms.
- **Isolated Pilot hostname lookup.** Entmoot now queries local Pilot hostname
  over a short-lived IPC connection, avoiding starvation behind long-running
  gossip stream commands on the shared transport driver.

## [1.5.26] - 2026-04-30

### Added

- **Remembered-group daemon startup.** Added `entmootd serve` for steady-state
  restarts from persisted local group state. `join` remains the first-time
  invite bootstrap path, while service managers can now run `serve` without
  depending on an invite file that may expire or disappear.
- **Pilot hostname-aware member profiles.** Entmoot now signs and gossips each
  node's local Pilot hostname as group-scoped member profile metadata, and ESP
  member APIs expose the latest live hostname without adding registry lookups.
  New joiners also pull a signed member-profile snapshot during bootstrap so
  existing hostnames appear promptly instead of waiting for the refresh cycle.
- **ESP group display fields.** Group creation and metadata updates can carry
  app-facing `description` and `tags`; group list/get responses project those
  fields alongside the existing metadata object.

## [1.5.25] - 2026-04-29

### Fixed

- **Multi-group Pilot transport capability forwarding.** The group-mux
  transport now forwards Pilot's dial budget, stale-session dropper, and stream
  error classifier to each group session. This lets the deployed multi-group
  runtime use the Pilot-aware fanout budget from `v1.5.24` instead of falling
  back to the old 15 second one-way dial timeout.

## [1.5.24] - 2026-04-29

### Changed

- **Pilot transport now uses raw Pilot streams.** Entmoot no longer keeps a
  long-lived yamux session cache above Pilot; each gossip/reconcile interaction
  opens one Pilot stream and closes it when done. This removes stale `conn_id`
  reuse failure modes and requires upgrading all peers together.
- **Pilot-aware fanout dial budgets.** One-way gossip, IHave, retry, and
  TransportAd fanout now use the transport's advertised dial budget before
  applying a separate short write deadline. Pilot-backed peers get enough time
  to complete TURN/NAT recovery without delivering empty late streams.

### Fixed

- **Stale Pilot connection invalidation.** A daemon `CloseOK(conn_id)` now
  closes the local IPC connection state so later writes fail retryably instead
  of silently sending on a dead Pilot stream.
- **Pilot IPC write deadline handling.** Raw Pilot stream writes now honor
  in-flight write deadline updates and close the IPC driver after timed-out
  length-prefixed writes to avoid desynchronizing the shared daemon socket.

## [1.5.21] - 2026-04-29

### Added

- **Executable ESP group and invite operations.** ESP sign-request completion
  can now execute `group_create`, `group_update`, `invite_create`, and
  `invite_accept` when `esp serve` is wired to a running `join` daemon. Results
  are stored generically on the sign request as `result`, while
  `message_publish` keeps the existing `publish_result` compatibility field.
- **ESP-local group metadata.** `group_update` now persists app-facing group
  metadata in `<data>/esp.sqlite` and group list/get responses include the
  stored metadata.
- **ESP latest-history endpoint.** Added read-only
  `GET /v1/groups/{group_id}/history` for mobile timeline bootstrap without
  advancing mailbox cursors.

### Changed

- **Bounded ESP history reads.** The latest-history path now reads only the
  requested recent page through a store-level `Latest` query instead of
  materializing full group history before trimming.

## [1.5.20] - 2026-04-29

### Added

- **Expanded ESP mobile API.** Added mobile-facing ESP routes for session and
  status inspection, group/member reads, group/invite/message sign-request
  creation, device push-token registration, notification preferences, and
  durable ESP-local state in `<data>/esp.sqlite`.
- **Executable ESP message-publish sign requests.** `message_publish` sign
  requests now expose canonical `signing_payload` metadata, phones sign the
  base64-decoded bytes, and completion verifies `signature` plus
  `signing_payload_sha256` before forwarding the signed message through the
  local publish path. Draft `payload` remains display/debug material only.
- **Composable ESP mobile infrastructure.** Added an ESP-only notifier layer
  with no-op and APNs providers, idempotency support for mobile mutations,
  APNs push-token hygiene, `esp device rotate-key`, and stable disabled-device
  error reporting without adding APNs/mobile logic to Entmoot core.
- **Multi-group join runtime foundation.** `entmootd join` now hosts group
  sessions behind one shared Pilot transport, routes IPC publish/tail/info by
  `group_id`, and adds local `join_group_req` IPC plumbing for future
  executable ESP invite acceptance.
- **Docusaurus documentation site.** Added a docs website under `website/`
  with Getting Started, Concepts, CLI, Operations, Architecture, and Reference
  sections, plus GitHub Pages deployment.
- **Paper refresh.** Updated the main and evolution LaTeX papers through
  `v1.5.19`, including ESP/mobile service-peer primitives and release
  metadata.

### Changed

### Fixed

## [1.5.19] - 2026-04-28

### Added

- **Build metadata command.** Added `entmootd version`, with release builds
  stamped by GoReleaser using the tag, commit, and build date.

## [1.5.18] - 2026-04-28

### Added

- **ESP device registry CLI.** Added `entmootd esp device
  <list|add|onboard|enable|disable|remove>` so operators can manage
  `<data>/esp-devices.json` atomically without hand-editing JSON. `onboard`
  prints a generated device private key once while storing only the public key.
- **ESP request signing helper.** Added `entmootd esp sign-request` to emit
  valid device-auth `X-Entmoot-*` headers for local ESP HTTP smoke tests.

## [1.5.17] - 2026-04-28

### Added

- **ESP device request signatures.** Added opt-in `entmootd esp serve
  -auth-mode=device|dual` support with a local `esp-devices.json` registry,
  Ed25519 request signatures, timestamp/nonce replay protection, and
  per-device group/client authorization for mailbox and signed-publish HTTP
  routes.

## [1.5.16] - 2026-04-28

### Added

- **ESP phone-signed publish bridge.** Added `POST /v1/messages` plus a
  signed-publish IPC frame so mobile clients can submit already-signed Entmoot
  messages through an ESP. The running `join` daemon still performs roster
  verification, canonical ID/signature checks, durable accept, and gossip
  fanout; the ESP never holds the phone's signing key.

## [1.5.15] - 2026-04-27

### Added

- **ESP mailbox HTTP bridge.** Added `entmootd esp serve`, a loopback-first
  token-gated HTTP API for mailbox `pull`, `ack`, and `cursor` operations.
  It reuses the durable cursor path and requires explicit `group_id` on every
  `/v1/mailbox/*` request.

## [1.5.14] - 2026-04-27

### Added

- **Local ESP mailbox CLI.** Added `entmootd mailbox pull`, `ack`, and
  `cursor` so operators can exercise the durable mailbox cursor path against
  production SQLite state before an HTTP/APNs bridge exists.

## [1.5.13] - 2026-04-28

### Added

- **Durable ESP mailbox cursors.** Added a `mailbox.CursorStore` abstraction
  plus SQLite-backed cursor persistence at `<data-dir>/mailbox.sqlite`, so an
  Entmoot Service Provider can restart without redelivering
  already-acknowledged mobile sync messages.

### Changed

- **Mailbox cursors are storage-backed.** `mailbox.Service` now delegates
  cursor state to a pluggable store. The default constructor remains in-memory
  for compatibility; ESP deployments can opt into SQLite.

## [1.5.12] - 2026-04-28

### Fixed

- **Publish responses no longer wait on Pilot trust IPC.** Local publish
  success now returns after validation and durable store accept; Plumtree peer
  selection, trust filtering, frame construction, and fanout all run behind the
  async delivery boundary. A wedged `TrustedPeers` query can no longer make the
  CLI report an i/o timeout after the message already landed.

## [1.5.11] - 2026-04-28

### Added

- **Mobile service-peer infrastructure primitives.** Added signer,
  delegation, mailbox cursor, and local event packages so an always-on Entmoot
  peer can later act as a mobile keeper without making iOS run `entmootd` or
  `pilot-daemon`.
- **Message ingest event hook.** The daemon's notifying store now emits a
  local `message_ingested` event alongside existing IPC tail fan-out, giving
  future APNs/webhook bridges a stable integration point.

### Changed

- **Local publishing uses the signer abstraction.** Existing keystore-backed
  publishes keep the same wire format, but the signing path now shares the
  same verification code used by future external/phone-held signers.

## [1.5.10] - 2026-04-28

### Added

- **Reconcile tracing is explicit and opt-in.** `entmootd` now accepts
  `-trace-reconcile`, which emits structured anti-entropy lifecycle events for
  trigger gating, Merkle root checks, RBSR rounds, fallback recovery, and body
  fetch results.

## [1.5.9] - 2026-04-27

### Added

- **Pilot gossip transport trace mode.** `entmootd` now accepts
  `-trace-gossip-transport`, which emits explicit Pilot/yamux session lifecycle
  events with peer id, role, generation, state transitions, and active stream
  counts for debugging port-1004 convergence.

### Changed

- **Pilot-backed yamux sessions now have an explicit lifecycle manager.**
  Outbound and inbound sessions are tracked with generation ids, active stream
  counts, and states. `DropPeerSession` now drains with yamux `GoAway()` before
  closing instead of tearing down active streams immediately.
- **Listener close now releases the daemon-side Pilot port.** The embedded
  Pilot IPC client sends the new `Unbind` command when closing a listener, so
  Entmoot restarts can reclaim port 1004 without waiting for IPC disconnect
  cleanup.

### Fixed

- **Stale Pilot stream errors are now typed.** Dynamic daemon messages such as
  `connection 123 not found` unwrap to retryable IPC sentinel errors, avoiding
  brittle text-only matching in reconcile retry decisions.

### Tests

- Added regression coverage for drain-preserving session drops and typed stale
  Pilot IPC error classification.

## [1.5.8] - 2026-04-27

### Fixed

- **Reconcile retries stale Pilot stream IDs without poisoning dial backoff.**
  Pilot `connection not found` errors are now treated like stale cached stream
  failures: Entmoot drops the cached peer session, retries once on a fresh
  session, and avoids arming the per-peer dial-backoff for this recoverable
  local session state.
- **Reconcile retries Pilot streams that close during dial establishment.**
  Pilot `connection closing` and `connection not established` errors are now
  treated as stale local stream/session state, so Entmoot drops the cached
  peer session and retries without poisoning per-peer dial backoff.

## [1.5.6] - 2026-04-27

### Fixed

- **Transport ads no longer feed self endpoints back into Pilot.** Local
  publishes skip self-install, inbound self-authored ads are ignored before
  store/install, and the Pilot adapter treats `SetPeerEndpoints(self, ...)` as
  a no-op.
- **Bootstrap transport snapshots now preserve the actual forwarding sender.**
  Snapshot ads are validated as forwarded by the snapshot peer rather than as
  if every author directly sent its own ad, so endpoint warming works for
  third-party ads returned by a trusted bootstrap peer.

## [1.5.5] - 2026-04-27

### Fixed

- **Pilot IPC close races no longer strand yamux sessions.** The embedded
  Pilot IPC client now remembers `CloseOK` frames that arrive before a freshly
  dialed or accepted conn is registered. Registration drains any pending data
  and then closes the conn, so yamux/reconcile observes EOF immediately instead
  of caching a dead port-1004 stream until timeout.

## [1.5.4] - 2026-04-27

### Fixed

- **Reconcile separates Pilot dial budget from wire I/O deadlines.**
  Pilot/yamux session establishment now runs under the wider reconcile
  attempt context, while the 15-second reconcile session timeout starts
  only after a stream exists. This prevents Entmoot from abandoning a
  still-valid Pilot dial and leaving the peer with an accepted stream
  that never carries a response.
- **Stale Pilot/yamux sessions are evicted on first response timeout.**
  When a request writes successfully but times out waiting for the
  response frame, Entmoot now drops the cached per-peer session before
  retrying instead of opening the retry on the same suspect session.
- **Pilot-backed yamux stream lifetimes are bounded.** The adapter now
  sets explicit stream-open and stream-close timeouts so half-open
  streams cannot linger behind the reconcile retry loop.

### Tests

- Added regression coverage for post-dial request deadlines, immediate
  session eviction on first response timeout, and bounded yamux stream
  lifecycle configuration.

## [1.5.3] - 2026-04-27

### Fixed

- **Reconcile no longer inherits short-lived push contexts.** Async
  anti-entropy work now runs under Entmoot's lifetime context with its
  own bounded attempt deadline, so a successful push cannot spawn a
  reconcile attempt that immediately fails under an already-canceled
  fanout context.
- **Pilot/yamux session recovery is less destructive and more complete.**
  Request/response retries get fresh per-attempt deadlines, local
  cancellation no longer poisons dial backoff, one-way control frames
  evict stale cached sessions on retryable write failures, and the
  Pilot transport adapter drains late DialOK results so daemon-level
  port-1004 conns are not orphaned after Entmoot request timeouts.
- **Merkle root probes avoid full-store scans.** `MerkleResp` no
  longer populates `MessageCount` by calling `Range(0,0)` on the hot
  reconcile path; reconciliation only depends on the root.

### Tests

- Added regression coverage for fresh per-attempt reconcile deadlines,
  canceled-trigger reconcile startup, local cancellation backoff
  suppression, stale one-way write recovery, and late Pilot dial cleanup.

## [1.5.2] - 2026-04-26

### Fixed

- **Reconcile now recovers from stale Pilot/yamux sessions.** Entmoot
  treats EOF/timeout/closed-stream errors before an expected
  request/response frame as stale-session failures, asks transports
  that support it to evict the cached per-peer session, and retries
  once on a fresh session.
- **RBSR EOF is no longer a false-success path.** If a reconcile
  session ends before the expected response, the attempt fails, gets a
  short failure backoff instead of the normal success cooldown, and
  falls back once to legacy `RangeReq{SinceMillis:0}` recovery.

### Tests

- Added stale-session retry coverage for Merkle root exchange and
  full-range fallback coverage for fetching peer-only message IDs.

## [1.5.1] - 2026-04-26

### Fixed

- **RBSR responder body recovery is now symmetric.** The responder
  side of `MsgReconcile` now retains `newlyMissing` message IDs
  learned during reconciliation and fetches those message bodies from
  the initiator. Previously only the initiator fetched discovered
  missing IDs, so a peer that learned "I am missing messages" while
  acting as responder could complete the ID exchange without ever
  pulling the bodies.
- **Shared anti-entropy fetch helper.** Initiator and responder now
  use the same `fetchMissingFrom` path, preserving existing
  signature verification, `Store.Put`, Plumtree refanout, and retry
  scheduling behavior.

### Diagnostics

- Added responder-side debug logs for RBSR round progress,
  responder completion, and EOF-after-discovery fetches. These make
  future `session ended early` cases distinguishable from successful
  responder-side body pulls.

### Tests

- Added `TestReconcileViaRBSR_ResponderFetchesInitiatorExtras`, a
  regression test where the initiator has messages the responder
  lacks and the responder's separate initiator path is suppressed.
  This proves the responder itself fetches bodies discovered during
  the RBSR exchange.

## [1.5.0] - 2026-04-25

Replaces the v1.4.4 polling pattern for TURN-rotation detection
with a true push notification, using pilot v1.9.0-jf.11b's new
Subscribe / Notify IPC primitives. Steady-state IPC traffic for
TURN-rotation detection drops from one Info query every 30 s
(~2,880/day) to **zero**.

### Added

- **`ipcclient.Driver.Subscribe(ctx, topic)`** — issues
  `opSubscribe` and returns the initial-state snapshot from
  `opSubscribeOK` plus a `*Subscription` whose `Events()` channel
  receives subsequent `opNotify` frames. The Subscription must be
  closed when no longer needed.
- **`ipcclient.Subscription`** — typed handle with `Topic()`,
  `Events() <-chan Notification`, and `Close() error`. Buffered
  channel (cap 16) with drop-oldest overflow semantics so a slow
  consumer cannot wedge the demuxer goroutine. The events channel
  closes on `Subscription.Close()` OR on driver shutdown.
- **`ipcclient.Notification`** — `{Topic, Payload}` event delivered
  to `Subscription.Events()`. Topic identifies the producer;
  Payload is opaque bytes whose encoding is the topic's
  responsibility (TURN uses UTF-8 host:port).
- **`ipcclient.ErrSubscribeUnsupported`** — sentinel returned by
  `Driver.Subscribe` when the connected pilot daemon doesn't
  understand `opSubscribe` (i.e. predates jf.11b). Callers branch
  on this with `errors.Is` and fall back to polling.
- **Five new opcodes** in `pkg/entmoot/transport/pilot/ipcclient/opcodes.go`:
  `opSubscribe` (0x30), `opSubscribeOK` (0x31), `opUnsubscribe`
  (0x32), `opUnsubscribeOK` (0x33), `opNotify` (0x34). Match
  pilot's wire definitions.

### Changed

- **`cmd/entmootd/turn_endpoint_poller.go` Run() rewritten.**
  External API (`CurrentTURN`, `Changed`, `pollOnce`) unchanged
  so `cmd/entmootd/join.go`'s wiring to
  `gossip.Config.LocalEndpoints` and
  `gossip.Config.EndpointsChanged` is untouched. Run now:
  1. Tries `Driver.Subscribe(ctx, "turn_endpoint")` first.
  2. On success, blocks on the Subscription's events channel and
     applies each opNotify payload via `applyTURN`. **Zero
     polling traffic.**
  3. On `ErrSubscribeUnsupported` (pre-jf.11b pilot), logs an
     INFO line and falls back to the v1.4.x 30 s polling loop —
     mixed-version meshes continue to work.
  4. On other Subscribe error, logs WARN and falls back to
     polling.

### Behaviour

- **Detection latency drops from 30 s (worst case) to ~1 ms.**
  Pilot fires `PublishTopic("turn_endpoint", newAddr)` from the
  TURN transport's Allocate / rotate path; entmoot's demuxer
  routes the Notify to the Subscription's channel; the poller's
  `applyTURN` signals `gossip.Config.EndpointsChanged`; the
  advertiser re-publishes the transport_ad on the next tick.
- **Pilot's serial-IPC handler is no longer a constraint** for
  TURN-rotation detection. Notify frames bypass the
  request-reply queue (server-pushed, same model as
  opAcceptedConn / opRecv), so a slow gossip Dial in flight
  doesn't delay rotation events.
- **Subscribe register-then-reply ordering preserved.**
  `Driver.Subscribe` adds the Subscription to `topicSubs`
  BEFORE writing the opSubscribe frame, so an opNotify pushed by
  pilot between SubscribeOK and the test code returning the
  Subscription is still routed correctly.

### Tests

- `pkg/entmoot/transport/pilot/ipcclient/subscribe_test.go`
  (new):
  - `TestSubscribe_RoundTripDeliversInitialAndNotify` — happy
    path: SubscribeOK with snapshot, then two Notify pushes.
  - `TestSubscribe_FallsBackOnUnknownCommand` — pilot returns
    `unknown command: 0x30` → Subscribe returns
    `ErrSubscribeUnsupported`; topicSubs is unwound.
  - `TestSubscribe_BufferOverflowDropsOldest` — 3× buffer-cap
    Notify pushes, oldest dropped, recent values preserved.
  - `TestSubscribe_CloseStopsDelivery` — Close prevents further
    delivery; idempotent re-close.
  - `TestParseNotifyPayload` — table-driven decoder coverage
    (well-formed / truncated / overlong / empty-payload).
  - `TestRouteNotify_FansOutToMultipleSubscriptions` —
    multi-Subscription fanout per topic.
  - `TestSubscribe_ConcurrentSafe` — 10× concurrent
    Subscribe/Close + routeNotify under -race.
- All existing tests still pass under -race, including the
  v1.4.6 cross-opcode error-routing regression guard.

### Compat

- **Mixed-version mesh works without a flag day.** Entmoot
  v1.5.0 against pilot < jf.11b → falls back to v1.4.x polling.
  Entmoot v1.4.x against pilot jf.11b → keeps polling (wasteful
  but not broken). All four version pairs interoperate.
- **No wire-format changes to existing opcodes.** Five new
  opcodes only, in the previously-unused 0x30-0x34 range.
- **External API of `turnEndpointPoller` unchanged** so
  `cmd/entmootd/join.go` is untouched.

### Out of scope (deferred)

- Other topics (peer trust, tunnel up/down, registry health). The
  Subscribe primitive is generic; pilot side just adds a new
  topic + emit site. Track per topic.
- Re-subscribe on pilot reconnect. If pilot restarts, entmoot's
  driver disconnects; the existing IPC auto-reconnect work
  (task #45) needs to also re-issue Subscribe. Track explicitly.
- `entmootctl subs list` diagnostic command. Nice-to-have.

## [1.4.6] - 2026-04-25

Fix a latent IPC error-routing bug surfaced by v1.4.4's
TURN-endpoint poller. Pre-fix, when multiple opcodes had
concurrent in-flight commands, the daemon's untagged Error
frames could be delivered to the wrong waiter — Pilot's
`dial timeout` errors were landing on Info callers, making
TURN-rotation polling appear permanently broken even when
the underlying IPC was healthy.

### Fixed

- **`Driver.deliverError` no longer mis-routes errors across
  concurrent opcodes.** Live evidence 2026-04-25 from VPS:
  every TURN-endpoint poll on phobos AND VPS failed with
  errors like `ipcclient: info: ipcclient: daemon: dial
  timeout` — the InfoStruct call is logically incapable of
  producing a `dial timeout` error, so the error was
  obviously routed from a concurrent gossip Dial call.

  Root cause: pre-fix the pending-reply book was a per-opcode
  map; `deliverError` walked the map (Go map iteration is
  randomised) and picked the head of a non-empty queue. Pilot's
  Error frame (`0x0A`) carries no correlation back to the
  command that caused it, so the routing decision was
  effectively a coin flip whenever two opcodes had waiters.

  Fix: replaced the per-opcode `map[Opcode][]chan` with a
  single global FIFO `[]pendingEntry`. Successful replies
  still match by opcode (linear scan, queue is shallow);
  Error frames pop the head, which is FIFO-correct because
  Pilot replies in command-issue order on a single socket.

- **Behaviour pre-v1.4.4 was OK by accident.** Entmoot used to
  issue IPC commands serially, so at most one opcode had a
  waiter at a time and the broken `deliverError` happened to
  pick the right one. v1.4.4's poller broke that invariant by
  running InfoStruct concurrently with the gossip layer's
  Dial/Send/Listen. v1.4.6 corrects the routing for any future
  concurrent IPC use too.

### Tests

- `pkg/entmoot/transport/pilot/ipcclient/error_routing_test.go`:
  - `TestErrorRouting_FIFOAcrossOpcodes` — issues Bind then
    Info concurrently; mock daemon replies with Error+InfoOK;
    asserts Bind (the FIRST issuer) gets the Error and Info
    succeeds. Pre-fix this would fail intermittently; post-fix
    it's deterministic.
  - `TestErrorRouting_OnlyOnePending` — single-pending case
    still works, guarding against a regression where the FIFO
    refactor might break the simpler path.

### Compat

No wire changes. Drop-in patch — both v1.4.4 and v1.4.5 had the
same routing bug; anyone on v1.4.4+ should upgrade. Pilot daemon
is unchanged; the daemon's untagged Error frame remains a known
protocol limitation that we work around client-side. A proper
correlation-tag fix would require a Pilot wire change (track
upstream).

## [1.4.5] - 2026-04-25

Hotfix on top of v1.4.4: the TURN-endpoint polling loop's 3 s
timeout was too tight on low-power ARM hardware, causing 100 %
poll failures and effectively disabling rotation detection.

### Fixed

- **Poll timeout bumped from 3 s to 15 s.** Live evidence
  2026-04-25 from phobos (Raspberry-Pi-class ARM box): three
  successive polls timed out at exactly 3 s, while a manual
  `pilotctl info` over a separate IPC connection returned in
  under 1 s. Pilot's Info handler aggregates uptime / peers /
  connections / ports into a single JSON marshal; on
  low-power ARM the serialization cost spikes well above 3 s
  under concurrent IPC load (gossip fanout retries,
  transport_ad publishes). v1.4.4's tuning was x86-only.
  15 s = half the 30 s poll interval, so a stuck IPC can't
  cause overlapping polls.

- **Poll failures log at WARN, not Debug.** Failed polls mean
  TURN rotation detection is offline for that cycle; without a
  WARN-level log the operator has no visibility unless already
  running at `-log-level=debug` (which phobos happened to be).
  One log per poll interval is bounded — not noisy. Manual
  pilotctl Info still works in parallel; only the polling
  goroutine's bound is too short.

### Compat

No wire changes. No new fields. Pure constant + log-level tweak.
Drop-in replacement for v1.4.4 — anyone already on v1.4.4 should
upgrade; the polling feature was effectively dead-on-arrival on
ARM-class hardware.

## [1.4.4] - 2026-04-25

Detect Cloudflare TURN allocation rotation and re-publish the
transport-ad immediately, instead of letting peers run on the
6-day safety-net refresh interval.

### Fixed

- **TURN-rotation propagation gap.** `entmootd`'s `LocalEndpoints`
  callback was a one-time snapshot of the `-advertise-endpoint` CLI
  flags, never refreshed. When pilot-daemon's TURN allocation
  rotated (port changes on restart or credential refresh — typical
  laptop or churned-credential scenarios with `-turn-provider=
  cloudflare`), the gossip advertiser kept publishing the stale
  CLI snapshot. Remote peers retained the stale TURN relay address
  in their cached transport_ad and their outbound frames silently
  dropped at Cloudflare's anycast edge.

  Live evidence 2026-04-25 from phobos<->laptop: phobos cached the
  laptop's previous TURN allocation on port `20414`; the laptop's
  current allocation was `:9587`. Phobos's `peer switched to
  turn-relay remote=:20414` log fired at every dial and traffic
  silently went into a dead TURN allocation. New transport_ad was
  never emitted because Entmoot had no way to detect the
  rotation.

  Fix: `entmootd` now polls `pilot-daemon.Info().TURNEndpoint`
  every 30 s, compares to the last-published value, and signals
  the gossip advertiser via `gossip.Config.EndpointsChanged` on
  any change. The advertiser's `LocalEndpoints` callback merges
  the live-polled TURN address on top of the static CLI snapshot
  (CLI `turn=` entries become a fallback for the cold start
  before the first poll completes). Rotation is observed and
  re-advertised within 30 s + transport_ad fanout latency
  (typically <1 s); peers receive the fresh TURN relay addr via
  the standard last-writer-wins on transport_ad sequence.

  Privacy-preserving by construction: the poller only reads from
  the local pilot-daemon over its existing IPC socket; no
  additional network footprint, no leak surface added.

### Tests

- `cmd/entmootd/turn_endpoint_poller_test.go`:
  - `TestTURNEndpointPoller_DetectsRotationAndSignals` — same
    value → no signal; rotated value → CurrentTURN updates and
    Changed receives a tick.
  - `TestTURNEndpointPoller_ErrorPreservesPreviousValue` —
    transient pilot IPC errors do NOT clobber the cached TURN
    addr (otherwise a single hiccup would withdraw the
    advertisement).
  - `TestTURNEndpointPoller_SignalCoalesces` — buffered-1 channel
    collapses bursts; the advertiser re-reads LocalEndpoints
    freshly each tick anyway, so the coalescing is correct
    behavior, not a lost signal.
  - `TestTURNEndpointPoller_RunStopsOnContextCancel` — clean
    shutdown on root-context cancel; no goroutine leak.

### Compat

No wire changes. No new flags. The CLI snapshot of
`-advertise-endpoint` is still consulted — non-TURN entries
(`tcp=`, `udp=`) pass through unchanged, and `turn=` entries are
preserved as a cold-start fallback if the first IPC poll fails.
Daemons running pilot-daemon < v1.9.0-jf.8 (no TURN field in
Info) decode `TURNEndpoint=""` and the poller treats them
exactly like a non-TURN configuration: empty live value, fall
back to CLI flags.

## [1.4.3] - 2026-04-24

Companion release to pilot-daemon v1.9.0-jf.11a. Surfaces
Entmoot's app-layer `-hide-ip` flag's relationship with the
newly-composable Pilot-layer privacy flags (`-turn-provider`,
`-no-registry-endpoint`, `-outbound-turn-only`) so operators can
see at startup whether their local pilot-daemon actually delivers
the privacy posture the app-layer flag asks for.

### Added

- **`pkg/entmoot/transport/pilot/ipcclient.Info`** grows
  `OutboundTURNOnly bool` and `NoRegistryEndpoint bool` fields
  (both `omitempty`). Mirrors pilot-daemon v1.9.0-jf.11a's
  `DaemonInfo` extension. Pre-jf.11a daemons decode the fields as
  `false`; no wire change.

- **`entmootd -hide-ip` startup check.** When `-hide-ip` is set,
  the daemon's join path now queries the local pilot-daemon's
  `Info()` at startup and compares:
  - Pilot has `TURNEndpoint != ""` (i.e. `-turn-provider` is set
    and the initial TURN allocation succeeded)?
  - Pilot has `OutboundTURNOnly = true` (i.e. outbound traffic
    routes through TURN; RFC 8828 Mode 3)?
  - Pilot has `NoRegistryEndpoint = true` (i.e. registry.Lookup
    returns "endpoint unknown")?

  Any missing piece produces a `slog.Warn` naming the specific
  leak channel that remains open and pointing at pilot-daemon's
  `-hide-ip` preset as the one-flag remedy. All three present
  produces a single `slog.Info` confirming full hide-ip posture.

  This closes a class of silent mis-configuration where an
  operator sets `entmootd -hide-ip` alone, sees no errors, and
  believes they're private — while pilot-daemon is still
  publishing their IP to the registry or routing traffic
  direct-outbound. The check is best-effort: if the Info IPC
  call fails (slow daemon, older daemon without the new fields,
  etc.), we log at Debug and proceed normally — never block
  startup.

### Wire compatibility

- **No wire changes at the Entmoot layer.** Transport-ad format
  is unchanged; group/roster/message wire formats are unchanged;
  IPC framing is unchanged. The only new thing on the IPC wire
  is two `omitempty` JSON fields in `InfoOK` responses, which
  pre-v1.4.3 Entmoots decode-and-ignore anyway because the
  untyped `Driver.Info()` path doesn't project them into any
  typed Go struct.
- **Pre-jf.11a pilot-daemons**: the new typed fields decode as
  `false`, which causes the startup check to warn about missing
  `OutboundTURNOnly` and `NoRegistryEndpoint`. That's the
  correct behaviour — pre-jf.11a daemons really don't have those
  flags, so the warning accurately describes the privacy gap.
- **v1.4.3 ↔ v1.4.2 / v1.4.1 / v1.4.0** all interop cleanly at
  the gossip layer; this release is additive and behavioral-
  change-free except for the new startup log lines under
  `-hide-ip`.

### Dependencies

- **No new dependencies.** Go standard library only.

## [1.4.2] - 2026-04-24

### Changed

- **Relaxed `onTransportAd` sender gate so trusted mesh peers can
  forward signed ads.** Before v1.4.2, the receive path at
  `gossiper.go:2711` rejected any frame whose `remote` (IPC-level
  sender) wasn't the ad's `Author.PilotNodeID`. The explicit intent
  was "single-hop" delivery, which turned `refanoutTransportAd`
  into a structural no-op: any intermediate relayer's forwarded
  frame was dropped downstream. Live evidence on 2026-04-24
  (asymmetric-TURN test via Pilot v1.9.0-jf.9): laptop couldn't
  direct-dial phobos (same-LAN false-positive #85 + CGNAT),
  the VPS received laptop's ad and refanouted to phobos, and
  phobos dropped the frame at this gate. End-to-end propagation
  was blocked despite both peers being reachable via the VPS.

  v1.4.2 replaces the author=sender gate with a trusted-sender
  gate via a new `isTrustedSender(ctx, remote)` helper backed by
  the existing `trustedSet(ctx)` cache. Log level drops from
  `Warn` to `Debug`; a non-trusted sender is routine (trust churn,
  trust-auto-approve race) and doesn't warrant operator attention.

  The downstream defenses are unchanged — **signature verification
  against the author's roster-recorded pubkey, roster-membership
  check on the author, per-(peer, topic) rate limit, and LWW on
  seq via `PutTransportAd`**. The signature remains the integrity
  root: any tampering with the ad body invalidates the signature
  and the receiver still drops. This is the standard gossip-
  network design (Bitcoin, IPFS, Gnutella): forwarders are
  trusted to relay but not to fabricate.

  Fail-open on cold-start (`trustedSet` returns nil before the
  first IPC snapshot lands) matches `plumEagerExcept`'s existing
  precedent; during the ~seconds-long warmup the downstream
  signature + membership checks still gate inbound ads.

### Operational impact

- **Hide-ip peers become reachable via any shared mesh hub.** A
  hide-ip peer whose direct-dial path to another peer is blocked
  (NAT, CGNAT, same-LAN false-positive, transient tunnel state)
  can now still have its transport-ad propagate through any third
  peer both sides can reach. For the current deployment, this
  enables phobos to install laptop's TURN endpoint via the VPS
  as a relay, which in turn lets phobos engage Pilot jf.9's
  `turn-relay` transport.

### Wire compatibility

- **No wire-format changes.** A v1.4.2 forwarder refanouting to a
  v1.4.1 receiver still gets dropped by the v1.4.1 receive-side
  author=sender gate; only upgrading both endpoints unlocks full
  multi-hop. A v1.4.1 publisher fanning out to a v1.4.2 receiver
  is accepted (the receiver's relaxed gate doesn't care which
  version the sender runs).

### Dependencies

- **No new dependencies.** Go standard library only.

## [1.4.1] - 2026-04-24

### Fixed

- **Transport-ad fanout no longer silently drops the ad on first-attempt
  direct-dial failure.** Before v1.4.1, both the publisher-side
  `fanoutTransportAd` and the receiver-side `refanoutTransportAd` called
  `Transport.Dial` exactly once per peer per burst and log-and-dropped
  on any error — so a NAT flap, a same-LAN colliding-subnet
  false-positive, a stale tunnel, or any other transient dial failure
  lost the ad until the author's next weekly refresh. Live evidence on
  2026-04-24: laptop's `-hide-ip` ad (Pilot v1.9.0-jf.9 asymmetric TURN
  test) reached the VPS but never reached phobos because the VPS's
  refanout dial to phobos timed out while phobos's tunnel was mid-
  switch to relay, and the VPS then dropped the ad. End-to-end jf.9
  couldn't be exercised until the ad propagated, so the live test was
  blocked.

  Fix: fanout / refanout failures now feed the existing message-retry
  scheduler. Dial failures enqueue a retry entry keyed on (peer,
  author); the same scheduler goroutine and decorrelated-jitter backoff
  (~200 ms → ~50 s, inherited from v1.0.7) that already covered message
  pushes now also re-attempt transport-ad sends. Retries cap at **6
  attempts** (independent of the 10-attempt message cap) with a
  **5-minute wall-clock ceiling**, and short-circuit via
  `TransportAdStore.GetTransportAd` before re-dialling if a newer seq
  from the same author is already stored locally — so a superseded ad
  never re-wastes a dial. Terminal failures log at `Debug` (weekly
  refresh re-seeds convergence; no operator action needed). `gossiper.go`
  scheduler is extended in place via a new `opTransportAd` retry op;
  the old log-and-drop comment about "weekly refresh makes retry state
  redundant" is removed — live evidence disproves it.

### Limitations

- **Retry state is in-memory only.** If `entmootd` restarts before a
  queued ad retry has fired, the retry is lost. Same behaviour as the
  pre-existing message retry scheduler; durable retry is out of scope
  for v1.4.1 and scheduled for v1.4.2 alongside transport-ad RBSR
  anti-entropy.

### Wire compatibility

- **No wire-format changes.** `TransportAd`, `Gossip`, and every other
  frame is byte-for-byte identical to v1.4.0. A v1.4.1 publisher
  retrying a fanout to a v1.4.0 (or earlier) receiver sees the same
  receive path light up as today — the retry is invisible to the
  remote. A v1.4.0 publisher hitting a transient failure continues to
  drop-on-first-try; only the upgraded side benefits.

### Dependencies

- **No new dependencies.** Go standard library only; no module graph
  changes.

## [1.4.0] - 2026-04-24

### Added

- **`-hide-ip` global flag on `entmootd`.** Opt-in, default `false`.
  When set on a node that invokes `entmootd join`, the gossiper's
  transport-ad advertiser suppresses every UDP/TCP endpoint coming
  out of Pilot and publishes only the TURN relay entry from
  `Info.TURNEndpoint`. With no TURN relay available, the advertiser
  emits **no ad at all** and logs a `Warn` — the node becomes
  unreachable until a relay is configured. This is deliberate:
  silently falling back to IP advertisement would defeat the
  privacy goal the flag exists for. Requires `pilot-daemon
  v1.9.0-jf.8+` for any useful reachability; earlier daemons do
  not expose `turn_endpoint` in Info.

- **`pkg/entmoot/transport/pilot/ipcclient.Info`** typed struct.
  Replaces the previous map-keyed access pattern for callers that
  want compile-time field access. The existing `Driver.Info`
  method (returning `map[string]interface{}`) is unchanged for
  backwards-compat; a new `Driver.InfoStruct` returns the typed
  `Info`. The struct's `TURNEndpoint string \`json:"turn_endpoint,omitempty"\``
  field decodes to `""` when talking to jf.7 daemons (which omit
  the key) or to jf.8 daemons without a TURN provider configured.

- **`turn` accepted as a transport-ad network string** on both the
  advertiser path (`-advertise-endpoint turn=relay:3478` at
  `entmootd join`) and the validator (`validateEndpoint` in the
  gossiper). The wire format itself is unchanged — `NodeEndpoint.Network`
  has always been a free-form short string; v1.4.0 just widens the
  set of strings Entmoot produces.

### Wire compatibility

- **Transport-ad frame format is unchanged.** A v1.0+ receiver
  decoding a `TransportAd{Endpoints: [{Network:"turn", Addr:...}]}`
  sees a well-formed payload and hands it through
  `Transport.SetPeerEndpoints` unmodified. Old daemons will reject
  the `"turn"` network string at the Pilot IPC layer; new daemons
  (`pilot-daemon v1.9.0-jf.8+`) route it to `AddPeerTURNEndpoint`.
  No coordinated upgrade is required — a v1.4.0 peer broadcasting
  a TURN ad to a v1.3.0 peer just sees its new-daemon peer install
  the endpoint on the remote side.

- **`Info.TURNEndpoint` is `omitempty` on both ends.** jf.7 daemons
  never set it; v1.4.0 decodes the absence as `""` and behaves
  exactly as it did in v1.3.0.

- **A v1.4.0 gossiper with `-hide-ip` becomes unreachable until the
  local pilot-daemon is upgraded to jf.8+** (the daemon must be
  able to both advertise a TURN endpoint and route inbound TURN
  traffic). This is the expected upgrade path, not a regression:
  the flag is opt-in.

### Behaviour

- When `-hide-ip` is set AND no TURN endpoint is available, the
  advertiser emits a `slog.Warn` of the form `"hide-ip set but no
  TURN relay available; peer will be unreachable"` and skips the
  publish. Operators must see this in log tailing to understand
  why their peer is offline.

- When `-hide-ip` is set AND a TURN endpoint is available, the
  advertiser publishes a single-entry ad with `Network="turn"`
  and drops UDP/TCP entries in the same callback-returned slice.

- When `-hide-ip` is unset (the default), behaviour is identical
  to v1.3.0: every endpoint the `-advertise-endpoint` flag was
  fed is published as-is. The TURN network string is supported on
  this path too, so an operator who wants UDP+TCP+TURN all
  advertised can pass all three.

## [1.3.0] - 2026-04-23

### Changed

- **License: Apache License 2.0.** Entmoot previously had no declared
  license. Establishing one required decoupling the binary from its
  only copyleft dependency; see below.

- **IPC to `pilot-daemon` is now an in-tree client.** Entmoot no
  longer imports `github.com/TeoSlayer/pilotprotocol/pkg/driver`.
  In its place, a new package
  `pkg/entmoot/transport/pilot/ipcclient` provides an independent
  Go implementation of the Pilot IPC wire protocol, written from
  Pilot's public specification
  (`pilotprotocol/docs/SPEC.md`). Runtime behavior is unchanged —
  `entmootd` still speaks the same Unix-socket wire protocol to
  the same `pilot-daemon` process. The rewrite removes Pilot's
  AGPL-3.0 source from Entmoot's compiled binary, freeing Entmoot
  to adopt its own license.

### Added

- **`pkg/entmoot/transport/pilot/ipcclient`** — 9 files (~1500 LOC)
  implementing the framing, opcodes, demuxer, connection,
  listener, driver, and type surface needed by Entmoot's Pilot
  transport adapter. Covered by unit tests, a demuxer-correctness
  suite, and an in-process fake-daemon integration test. The
  package documents every clarification that went beyond SPEC.md
  (AcceptedConn port prefix; TrustedPeers as a Handshake
  sub-command; SetPeerEndpoints TLV layout and daemon limits;
  Error frame encoding; Recv-before-register race handling).

- **`pkg/entmoot/transport/pilot/addr.go`** — local `Addr` and
  `ParseSocketAddr` types replacing the former
  `pilotprotocol/pkg/protocol` import. Matches the Pilot
  socket-address string format for wire compatibility.

- **`LICENSE`, `NOTICE`, `CONTRIBUTING.md`** at the repository
  root. `CONTRIBUTING.md` establishes a Developer Certificate of
  Origin requirement for future contributions.

### Removed

- **`github.com/TeoSlayer/pilotprotocol` go-module dependency.**
  Both the `require` entry and the `replace` directive are gone;
  `go mod graph | grep pilotprotocol` is empty.

### Wire compatibility

No changes to any wire-format emitted on the network. Entmoot
v1.3.0 is drop-in compatible with all earlier v1.x peers and with
`pilot-daemon` v1.9.0-jf.7. Upgrading a single node in a mesh does
not require coordinated upgrades.

## [1.2.1] - 2026-04-22

### Fixed

- **Anti-entropy catches gaps anywhere in the log.** The v1.2.0
  reconcile path used `fetchPeerRange(since = latestLocalTimestamp)`
  as its gap-recovery query, which could only heal peers that were
  strictly *ahead* of us — if we held a newer message than the gap
  we were missing, the range query excluded the gap entirely. Live
  testing on 2026-04-22 surfaced this when two messages from a peer
  who had been briefly unreachable never propagated even though
  Merkle roots clearly differed. v1.2.1 replaces the cursor-based
  path with Range-Based Set Reconciliation (Meyer, SRDS 2023;
  arXiv:2212.13567), the same algorithm used by Willow / Earthstar
  / iroh and by Negentropy (Nostr NIP-77). Bandwidth is proportional
  to the symmetric difference (not the set size), convergence is
  O(log N) rounds, and the protocol catches gaps anywhere in the
  timeline by construction.

### Added

- **Range-Based Set Reconciliation** package
  (`pkg/entmoot/reconcile`). Greenfield Go implementation of the
  RBSR state machine with a Negentropy-style 16-byte fingerprint —
  `SHA-256(count_u64_le || sum_mod_2^256(SHA-256(id)))[:16]` —
  which is commutative, incrementally computable, and resistant
  to duplicate-cancellation / Gaussian-elimination attacks that
  plain XOR fingerprints suffer from. Defaults: leaf threshold
  16, fanout per round 16, max rounds 10 (safety-net — `WARN` on
  overflow).

- **`MsgReconcile = 0x11`** wire opcode carrying
  `{group_id, round, ranges, done}`. The RBSR session holds a
  yamux stream open across multiple alternating frames until both
  sides flag `done=true`. This is the first handler in the
  codebase that keeps a conn alive across frames; the convention
  is flagged explicitly in `onReconcileReq`.

- **`Transport.SetOnTunnelUp(cb)`** callback surface. Pilot
  adapter fires the callback after every freshly-established
  yamux session (outbound and inbound), with panic-recover and
  goroutine isolation so a slow reconcile dial can't block the
  transport. In-memory transport fires symmetrically on both
  sides of each `Dial` / `Accept`. `gossiper.Start` installs
  `SetOnTunnelUp(func(p){ g.maybeReconcile(ctx, p) })` so every
  reconnect triggers an immediate cooldown-gated reconcile —
  directly addressing the Pilot-tunnel-flap pattern observed on
  2026-04-22 where yamux orphaning across rekeys left stale
  peers invisible until an organic publish arrived.

- **Background anti-entropy ticker** (`reconcilerLoop`). Picks
  the least-recently-reconciled roster member each 30 s ±20 %
  jittered tick, one peer per tick. Skip optimization:
  `lastKnownPeerRoot[peer]` caches each peer's Merkle root after
  a successful reconcile; if the local root still matches, the
  tick is a silent no-op. An idle three-peer mesh costs zero
  dials per tick at steady state. Bounds worst-case
  post-partition catch-up latency to one tick interval regardless
  of whether anyone publishes — the problem that kept two
  specific messages stuck on VPS for minutes after recovery,
  because reconcile was reactive-only on inbound gossip.

- **`MessageStore.IterMessageIDsInIDRange`** — new interface
  method returning message IDs sorted by byte-order in a
  half-open `[lo, hi)` range. Implemented on Memory, JSONL, and
  SQLite backends. SQLite uses the auto-generated primary-key
  index on `message_id` (confirmed via `EXPLAIN QUERY PLAN`).
  The byte ordering is distinct from — and does not replace —
  the existing topological ordering used by `MerkleRoot()`; RBSR
  uses byte order because it aligns with the SQLite PK and makes
  range splitting arithmetic trivial.

- **Dial-backoff hygiene**: successful inbound Accept from a peer
  now clears that peer's outbound dial-backoff window
  (`recordDialSuccess(remote)` at the Accept-success site).
  Inbound reachability is strong bidirectional-reachability
  evidence, so a cached outbound-backoff from an earlier failure
  is almost certainly stale. If the next outbound dial does fail,
  the backoff re-arms from base.

- **Debug logs at reconcile decision points**: cooldown-gated
  skips, tick-skips when roots already match, per-round progress
  (peer, round, outgoing ranges, newly-missing count, done
  flags). Makes it straightforward to distinguish "no reconcile
  happened" from "deduplicated" from "converged in N rounds"
  when tracing operational issues.

### Wire compatibility

- v1.2.1 ↔ v1.2.1: `MsgReconcile` path.
- v1.2.0 → v1.2.1: legacy peer issues `MsgRangeReq`; v1.2.1
  retains `onRangeReq` + `onMerkleReq` as compat responders.
  Pair converges via the v1.2.0 buggy-but-functional path.
- v1.2.1 → v1.2.0: v1.2.1 issues `MsgReconcile`; v1.2.0 responder
  sees unknown opcode and drops the stream; v1.2.1 reads EOF,
  emits `Debug "session ended early (peer may be v1.2.0)"`, and
  returns without state corruption. Historical gaps between
  mismatched-version peers do not heal until the whole mesh is
  upgraded — acceptable for our three-peer deployment; staged
  upgrades should cycle through in order.

### Internals

- New field `Gossiper.lastKnownPeerRoot map[NodeID]wire.MerkleRoot`
  guarded by `pendMu`, updated at the end of each successful
  reconcile.
- Dead code removed: `fetchPeerRange` and `latestLocalTimestamp`
  (both superseded by RBSR).
- `reconcileSessionTimeout = 15 s`, `reconcilerTickBase = 30 s`,
  `reconcilerTickJitter = 6 s` new constants.
- `jitteredReconcilerTick()` helper alongside the existing
  `jitteredReconcileCooldown()`.

### Known

- `test/canary/TestCanaryPilot` intermittently hangs (pre-existing
  since v1.2.0) on `driver.sendAndWaitTimeout` in the Pilot IPC
  adapter — the call is not `context.Context`-bounded. Stack
  trace is entirely in `transport/pilot/*` (untouched by v1.2.1).
  Fix is a Pilot-fork concern (prospective jf.8): bound the IPC
  wait with the caller's context. v1.2.1's added OnTunnelUp
  callback creates more tunnel-establish events in the canary,
  which slightly increases the flake surface but does not
  introduce the bug.

## [1.2.0] - 2026-04-21

### Added

- **Transport-endpoint advertisements** (`_pilot/transport/v1`
  topic). Each group member publishes a signed `TransportAd`
  naming their current TCP/UDP endpoints; receivers verify it
  and install the endpoints into Pilot's `peerTCP` map via the
  new jf.7 `SetPeerEndpoints` IPC. Gives Entmoot groups
  peer-to-peer endpoint discovery that works without any
  central-registry cooperation — critical on our deployment
  where the upstream registry silently drops the `endpoints`
  field that Pilot's own multi-transport protocol expects.

  Wire semantics: IPNS-style signed record with a per-author
  monotonic `Seq` counter. Storage: LWW-Register — at most one
  row per (`group_id`, `author`) in the new `transport_ads`
  SQLite table, with lexicographic signature tiebreak on `Seq`
  ties. A publish of 10,000 ads retains exactly one row;
  retained state is structurally O(N_members × 1 ad),
  independent of publish rate.

  Three-layer spam defence in receive order (cheapest-first):
  schema + 1 KiB size cap, publisher allowlist (a peer may only
  advertise its own endpoints), per-(topic, author) token
  bucket via the new `ratelimit.AllowTopic` (10 tokens, refill
  1/hour for `_pilot/transport/v1` — legitimate publisher needs
  ~1/week, 1680× headroom).

  Join-time support: invites now carry per-bootstrap-peer
  endpoint hints (signed by the founder, so authenticated), and
  the Join flow pulls a `TransportSnapshotReq` from the
  bootstrap peer immediately after roster sync. The newcomer's
  `peerTCP` is fully populated before the first post-Join
  dial — TCP fallback is available on the very first dial of
  every group member, not only after the next advertiser
  refresh.

  On entmootd startup, the persistent `transport_ads` table is
  replayed into Pilot so the `peerTCP` map is warm even if
  Pilot was restarted while entmootd was up (and vice versa:
  entmootd restart with Pilot still running converges the map
  from disk). `entmootd join` gains a repeatable
  `-advertise-endpoint network=host:port` flag to feed
  `LocalEndpoints`; auto-discovery from the Pilot daemon's own
  configured listen endpoints is deferred to v1.3, contingent
  on a new `driver.Info` field.

  Pairs with Pilot v1.9.0-jf.7.

## [1.1.0] - 2026-04-21

### Changed

- **Persistent multiplexed sessions over Pilot tunnel.** The Pilot
  transport adapter now maintains one long-lived
  `hashicorp/yamux` session per peer and multiplexes all gossip
  streams over it. Prior to v1.1.0 every gossip frame opened a
  fresh Pilot stream with a full SYN handshake — fast on a healthy
  tunnel but vulnerable to Pilot's ~32 s dial budget when the
  stream SYN/ACK silently wedged (observed live: both peers on a
  node failing simultaneously at the stream-dial layer while the
  underlying tunnel reported encrypted+authenticated).

  Changes:
  - `Dial()` now opens a yamux stream on the cached session
    (0-RTT new-stream after first contact) instead of dialing a
    fresh Pilot stream.
  - `Accept()` reads from an internal channel fed by a background
    loop that accepts inbound Pilot connections, wraps each in a
    yamux server session, and pumps accepted streams into the
    channel.
  - yamux's built-in keepalive (30 s interval, 10 s timeout)
    detects silently-wedged sessions and closes them; the next
    `Dial()` opens a fresh session automatically.

  Wire compatibility: Entmoot's frame format is unchanged; yamux
  multiplexes the same JSON frames over a session. Mixed
  v1.0.x / v1.1.0 peers do NOT interoperate — v1.0.x expects a
  raw stream per frame, v1.1.0 expects a yamux session. Upgrade
  all nodes together.

### Added

- New dep: `github.com/hashicorp/yamux` (BSD-3, used by Consul,
  Nomad, Vault, libp2p's yamux transport).

## [1.0.8] - 2026-04-21

### Fixed

- **Bootstrap self-dial amplification.** An invite's
  `bootstrap_peers` list included the issuer's own NodeID
  (every invite minted until today did — the issuer-is-a-peer
  convention inherited from the v0 roster design). The Gossiper's
  `Join` path already filtered `LocalNode` before dialing via
  Pilot, BUT `entmootd invite create` kept emitting the issuer as
  a bootstrap peer, and Pilot's `ensureTunnel` — reachable
  through non-Join code paths (ambient registry-driven membership
  view maintenance, Pilot's own peer-discovery) — would then
  establish tunnels to the local NodeID. On multi-homed hosts
  Pilot's "same-LAN peer detected" branch fires both for the
  docker-bridge LAN entry and the public-IP entry of the local
  node, establishing multiple duplicate self-tunnels that
  retransmit into each other — observed live on VPS as a
  ~5,900 pps self-amplified loop, 210 % CPU on pilot-daemon,
  saturating its packet buffers and reinflating gossip
  propagation to minute-scale latencies (masquerading as a
  v1.0.7 regression when the real fault was at the transport
  layer).

  Two defense-in-depth fixes:
  - `entmootd invite create` now excludes the issuer's NodeID
    from `bootstrap_peers` at mint time. Makes new invites
    self-documenting. Matches Cassandra's "gossiper
    `live_endpoints` excludes self by construction" invariant.
  - Pilot v1.9.0-jf.6 (shipped simultaneously) adds a
    `protocol.ErrDialToSelf` sentinel at the top of
    `DialConnection` and `ensureTunnel`. Mirrors
    go-libp2p-swarm's canonical `ErrDialToSelf` guard:
    fast-fail with a typed sentinel so caller-side invariant
    violations are visible rather than masked.

  Legacy invites (like the one live in the current canary group)
  still parse correctly — the Gossiper's `Join` path already has
  a self-skip filter at every bootstrap strategy. The new
  defenses close the remaining paths (non-Join callers of
  `ensureTunnel`, and future invite mints from any node).

## [1.0.7] - 2026-04-21

Two-fix bundle attacking the residual ~1.5-minute propagation tail
observed in live v1.0.6 mesh. Together the fixes bring steady-state
hop latency from 60–90 s (p99) to ~RTT (happy path) or ~RTT +
`graftTimeout` (3 s) on flaky peers. Wire-compatible with every
v1.0.x release — additive `Body` field, additive retry-state field,
mixed-version peers interoperate unchanged.

### Added

- **Inline message body on eager-push.** `wire.Gossip` gains an
  optional `Body *entmoot.Message` field. Senders inline the full
  body when the canonical encoding is ≤ 4 KiB (`inlineBodyThreshold`,
  matching 4× IPFS Bitswap's `WantHaveReplaceSize` default);
  receivers hash-verify `Body` against `IDs[0]` and run Ed25519
  signature verification before `Store.Put`, skipping the
  `fetchFrom` round-trip entirely. Cuts every gossip hop for small
  messages from 2 Pilot stream dials to 1.

  Aligns Entmoot with the canonical Plumtree paper (Leitão et al.
  SRDS 2007, §2.2 — `EagerPush(m, mID, ...)` carries the full
  body; ID-only is reserved for lazy-push `IHAVE`). Every surveyed
  production gossip — libp2p GossipSub, Scuttlebutt EBT, Matrix
  federation, Cassandra digest-phase, IPFS Bitswap — does this.

  Wire-compatibility: the Gossiper signature explicitly excludes
  `Body` (zeroed before canonical encoding in both `signGossip`
  and `verifyGossipSig`), so v1.0.6 peers ignoring the new field
  still verify v1.0.7 frames unchanged. `Body` integrity is
  provided independently by (a) `canonical.MessageID(Body) == ID`
  and (b) the Message's own Ed25519 signature — same two checks
  the v1.0.6 `fetchFrom` path already performs.

- **Decorrelated-jitter retry backoff** (Marc Brooker / AWS 2015).
  Replaces the deterministic `[1s, 2s, 4s, 8s, 16s, 30s, 60s×4]`
  schedule with `next = min(cap, random(base, prev*3))`. Retry
  budget unchanged (10 attempts, same `"gossip: retry budget
  exhausted"` log). Prevents fleet-wide retry storms when a
  correlated flakiness event (NAT flap, registry blip) would
  otherwise synchronise every node's retry to the same wall-clock
  moments.

- **Jittered reconcile cooldown.** Fixed 60 s → 30 s ±20 %
  multiplicative jitter (`[24 s, 36 s]`). Cooldown reduced
  because research consensus (libp2p GossipSub 1 s heartbeat,
  Plumtree §3.4 continuous lazy-push, Cassandra gossip 1 s) shows
  60 s was an order of magnitude too slow for failed-push
  recovery — that number belongs to bulk Merkle-tree anti-entropy
  (Riak AAE, Cassandra repair), not per-peer reconcile. Jitter
  prevents fleet-wide collisions on the cooldown boundary
  (memberlist-style `randomStagger`). Each peer caches its
  per-invocation jittered value so a peer "just under" the gate
  doesn't probe the boundary every tick.

### Changed

- `Gossiper` gains a dedicated `*rand.Rand` (seeded from the
  clock, Fake-clock-friendly for tests) for jitter generation;
  separated from `getPicker`'s rand to keep peer-sampling
  determinism independent of jitter.
- `retryState` gains `lastBackoff time.Duration` feeding
  `nextBackoff(prev)`.
- `lastReconciled` map value type becomes `reconcileState{at,
  cooldown}` so the jittered cooldown picked at reconcile time is
  cached alongside the timestamp.
- `retryTickInterval` (500 ms) stays deterministic — the
  scheduler wheel, not a retry deadline.

### Regression tests

- `TestGossipInlineBodySkipsFetch` — B stores the inlined body
  without any outbound dial to A.
- `TestGossipInlineBodyHashMismatchRejected` — forged Body
  (mismatched hash) is dropped, no silent fetch fallback.
- `TestGossipInlineBodyBackwardCompat` — v1.0.6-shape frame
  (`Body == nil`) still propagates via `fetchFrom`.
- `TestRoundTripGossipWithBody` — wire round-trip with Body
  populated preserves every field byte-for-byte.
- `TestRetryBackoffDecorrelatedJitter` — 1000 samples stay within
  `[base, cap]` with non-trivial spread.
- `TestReconcileCooldownJittered` — samples stay within ±20 %
  envelope.

## [1.0.6] - 2026-04-20

Three-fix bundle targeting the "publish takes ~4 minutes to propagate"
regression observed on the live 3-node mesh after v1.0.5 landed. Each
fix mirrors a production pattern from libp2p / Matrix / Cassandra
gossip; together they collapse cascade latency from ~4 min to
sub-10-second steady state. Wire-compatible with v1.0.4 and v1.0.5.

### Added

- **Trust-aware reachability oracle.** `plumEagerExcept` and
  `plumLazyExcept` now consult `Transport.TrustedPeers(ctx)` and
  filter the returned peer list by the trusted set before returning.
  Peers with no Pilot trust pair (structurally unreachable for the
  lifetime of that config) are skipped entirely. Cached on a 1-second
  TTL so bursty fanouts don't hit Pilot IPC per peer per message;
  fails open (degrades to v1.0.5 behaviour) if the IPC query errors.
  Mirrors libp2p's `Network().Connectedness()` check in
  `go-libp2p-pubsub` and Matrix Synapse's `destination_retry_timings`
  — treating reachability as a first-class gossip-layer concern
  instead of relying on transport timeouts.

- **Per-peer exponential dial-backoff cache.** Every
  `Transport.Dial` in the gossiper (7 sites: `pushGossip`,
  `sendIHave`, `sendGraft`, `sendPrune`, `fetchFrom`,
  `fetchPeerRoot`, `fetchPeerRange`) now consults
  `canDial(peer)` before attempting; failures extend an
  exponential window (1 s, 2 s, 4 s, …, capped at 5 min) via
  `recordDialFailure`; successful dial clears the window via
  `recordDialSuccess`. Prevents the retry scheduler from
  independently re-dialing a dead peer on every pending message,
  which previously cost Pilot's full ~32-second SYN retry cycle per
  attempt. Matches libp2p `swarm.DialBackoff` semantics. Reachability
  is recorded on dial completion (not on full-RPC completion) so a
  healthy connection followed by a stream-layer error does not
  trigger spurious backoff.

### Changed

- **Parallel fanout with bounded per-peer dial timeout.**
  `fanoutPush`, `fanoutIHave`, and `drainDueRetries` were
  sequential loops — one stalled peer head-of-line-blocked every
  other peer on the same fanout / retry tick. Replaced with
  `golang.org/x/sync/errgroup.Group.SetLimit(32)`, one goroutine per
  peer, each wrapped in `context.WithTimeout(ctx, 5 * time.Second)`.
  5 s covers relay-over-beacon p99 (~600 ms) with generous slack and
  is far below Pilot's internal 32 s dial ceiling — so a dead peer
  fails fast into the new dial-backoff cache instead of blocking
  the rest of the fanout. Canonical Go pattern; zero behaviour
  change for healthy peers. (New dependency:
  `golang.org/x/sync/errgroup`, promoted from indirect.)

### Fixed

- **4-minute publish-to-propagation latency on partial-trust meshes.**
  Previously, every publish from node A fanned out to the full eager
  set, including peers with no trust pair — each of those sends paid
  Pilot's ~32 s dial budget, and the retry scheduler re-dialed the
  same dead peer for every pending message on every 500 ms tick,
  compounding through the 10-step retry backoff to ~4 min cumulative
  blocking before the first healthy retry slot fired. The combined
  effect of the three fixes above: (A) structurally-unreachable peers
  are never in the fanout set, (B) one slow peer never blocks
  healthy peers, (C) one failed dial suppresses further dials to the
  same peer for an exponential window. Observed live: cross-node
  publish latency on the VPS↔Phobos↔laptop mesh drops from ~4 min
  back to sub-second in steady state.

### Regression tests

- `TestPlumtreeSkipsUntrusted` — fanout never dials an untrusted
  peer; the trust oracle filters the eager/lazy lists before the
  push loop.
- `TestPlumtreeParallelFanoutWithSlowPeer` — one peer's `Dial`
  blocks for 30 s; healthy peers still receive the message within
  1 s; the slow peer's `retryKey` is enqueued within the 5 s
  per-peer timeout.
- `TestPlumtreePerPeerDialBackoff` — after a failed dial, the
  next 10 `pushGossip(peer)` calls short-circuit without invoking
  `Transport.Dial`; window reopens only after
  `dialBackoffCap` elapses (or a successful dial clears the
  state).

## [1.0.5] - 2026-04-20

### Fixed
- Plumtree re-fanout fired only when a message was acquired via the
  live gossip-push path (`onGossip` → `fetchFrom`). Messages acquired
  through either of the other two `fetchFrom` call sites —
  `executeRetry(opFetch)` (retry of an initially-failed fetch) and
  `reconcileWith` (anti-entropy pull driven by `maybeReconcile`) —
  were stored locally but never forwarded through the spanning tree.
  In partial-connectivity topologies this silently broke
  end-to-end delivery: e.g. a hub that could not receive an edge
  node's direct push (because the edge's outbound was NATed away)
  but later pulled the message via reconciliation would never
  propagate to the hub's other edges.

  Fix: centralise the `plumCancelGraftsFor(id) + refanout(peer, id)`
  hook inside `fetchFrom` after a successful `Store.Put`. Every
  acquisition path now triggers the first-seen forwarding step,
  matching the rule libp2p GossipSub applies ("not seen before →
  forward to mesh, irrespective of arrival method"), rather than the
  handler-specific duplication used by the Helium reference Erlang
  Plumtree. The redundant call in `onGossip` is removed.

  Regression test: `TestPlumtreeRefanoutOnFetchFrom` seeds peer B's
  store directly (no Gossip push), drives `A.fetchFrom(B, id)`, and
  asserts C receives the message via A's post-fetch refanout.

## [1.0.4] - 2026-04-20

### Added
- Plumtree-based dissemination (Leitão/Pereira/Rodrigues, "Epidemic
  Broadcast Trees", SRDS 2007). Entmoot's gossip is no longer
  push-only-on-originate: receivers now re-fanout on first-sight,
  with a self-healing spanning tree maintained via three new
  control-frame types on ids that never carry a body.
  - `MsgIHave` (0x0B): lazy advertisement. A publisher eagerly pushes
    full `Gossip` frames to `eagerPushPeers` and sends `IHave`-only
    to `lazyPushPeers`.
  - `MsgGraft` (0x0C): a peer that observed `IHave` for a missing id
    and did not receive the body via eager push within 3 s (matching
    libp2p GossipSub's `IWantFollowupTime`) sends `Graft` to pull the
    body AND be promoted back into the sender's `eagerPushPeers`.
  - `MsgPrune` (0x0D): a peer that receives a duplicate `Gossip`
    `Prunes` the sender — demoting it to `lazyPushPeers` so
    subsequent messages arrive as `IHave`, pruning the redundant
    full-body edge.
  - Initial tree shape: all roster members start in
    `eagerPushPeers`; duplicates self-prune to a spanning subset
    per-message. No explicit tree construction ceremony required.
  - All three new types are unsigned — precedent set by `FetchReq`,
    `RangeReq`, `MerkleReq`. Identity is established at the Pilot
    tunnel + on-Accept roster check, which is already sufficient.

  **Impact**: in a partial-connectivity topology (e.g. the
  three-node canary: VPS public, Phobos private, laptop private, no
  Phobos↔laptop direct edge), a Phobos publish now reaches the
  laptop with VPS as the re-fanout hop. Before v1.0.4 that message
  stopped at VPS. At steady state, message-per-broadcast overhead
  converges from `O(N × fanout)` (naive gossip) toward `O(N)` (one
  eager push per edge of the spanning tree) — the same efficiency
  profile as Riak Core, Helium, Solana's gossip, libp2p
  GossipSub/Episub, and Partisan.

- `eagerPushPeers` / `lazyPushPeers` peer-set maps on `Gossiper`,
  guarded by a new `plumMu`. Each roster member sits in exactly one
  set at any time. Seeded lazily on first Plumtree code path so the
  Gossiper can be constructed before the roster is fully populated
  (matches existing test + production ordering). Lookup helpers
  (`plumEagerExcept`, `plumLazyExcept`, `plumPromoteToEager`,
  `plumDemoteToLazy`, `plumCancelGraftsFor`) keep the mutex
  discipline contained to a small surface.

- `pendingGraft` map keyed on `(id, sender)` storing the outstanding
  `time.AfterFunc` timer per `IHave` advertisement. Any arrival of
  the body via the eager path cancels all pending timers for that
  id so we never emit a `Graft` for something we already have.

- Retry scheduler extended with three new ops (`opIHave`, `opGraft`,
  `opPrune`). `executeRetry` rebuilds the outbound frame from the
  `retryKey` since none of the three control frames carry signed
  content worth caching. Same exponential-backoff budget as
  `opPush`; exhausted slots fall through to anti-entropy
  reconciliation as today.

- `plumtree_test.go`: five new integration tests covering re-fanout
  in a line graph (A ↔ B ↔ C, A ↛ C), PRUNE on duplicate in a
  fully-connected mesh, GRAFT on a forced-lazy edge, initial-state
  eager population, and the basic PRUNE handler unit test. A new
  `filteringTransport` wrapper lets tests build arbitrary directed
  dial graphs over the existing `memTransport` hub.

### Changed
- `src/skills/entmoot/SKILL.md` metadata bumped to 1.0.4; adds a short
  "Dissemination: Plumtree (Leitão 2007)" note so agents reading
  the skill don't misunderstand the group protocol as push-only.

## [1.0.3] - 2026-04-19

### Fixed
- `entmootd publish` CLI no longer hits the 30 s IPC read-response
  deadline when a gossip fanout target is slow to dial. The
  publish handler now returns as soon as the message is durably
  inserted into the local store; peer delivery runs asynchronously
  via the existing exponential-backoff retry scheduler (patch 6
  in v1.0.2). Response latency is now single-digit milliseconds
  regardless of peer reachability, matching the industry-standard
  pub/sub convention (Kafka durable-log commit, NATS fire-and-
  forget, RabbitMQ publisher confirms): publish means "accepted
  for delivery", not "delivered to subscribers". Retries and
  anti-entropy reconciliation continue to cover any peers that
  were unreachable during the initial fanout.
- `gossip.memTransport.Close` no longer closes the buffered
  `acceptCh` alongside the `closed` signal channel — the redundant
  close raced with concurrent `Dial` goroutines that had already
  entered their select. The `<-t.closed` branch is sufficient to
  unblock both Accept and Dial; the accept channel is garbage-
  collected with the transport. Fixes a race detector failure
  surfaced by the new async-publish path in multi-node tests.

### Changed
- `src/skills/entmoot/SKILL.md` (version bumped to 1.0.3): clearer
  bootstrap and re-entry guidance for ephemeral agent
  environments. Adds a "Fast path" short-circuit for the
  already-joined case (95% of re-invocations on a long-lived
  host); an explicit warning against deleting
  `~/.pilot/identity.json` (deletion = new `node_id` = silently
  orphaned from every existing group roster); a
  `nohup setsid ... & disown` recipe for `entmootd join` that
  actually survives a pm2 / Telegram-bot parent exit; an
  "invite acquisition" subsection covering inline-JSON invites
  from chat messages (write to a tempfile before passing to
  `entmootd join`); and an explicit
  `export PATH="$HOME/.pilot/bin:$HOME/.entmoot/bin:$PATH"` at
  the top of the Routine-operation section for non-login shell
  contexts.

### Added
- `TestPublishReturnsBeforeFanout` in
  `pkg/entmoot/gossip/gossiper_test.go`: verifies the fast-return
  contract by publishing on node A while node B's accept loop is
  deliberately not running. Asserts Publish completes in <200 ms
  and the message is immediately present in A's local store even
  though the fanout to B is hung on a blocking `net.Pipe` write.

## [1.0.2] - 2026-04-19

### Changed
- `install.sh` source-build fallback now clones the patched Pilot fork
  (`jerryfane/pilotprotocol` main) instead of `TeoSlayer/pilotprotocol`.
  The "install Pilot separately" helper text and `SKILL.md` onboarding
  snippet also point at the fork's installer so agents following the
  documented flow pick up the reliability patches without manual steps.

### Added
- Exponential-backoff retry for gossip push and fetch. Transient `Transport.Dial`
  failures are requeued instead of dropped; a new `retryLoop` goroutine drains
  the `pending` slot map at 500 ms cadence with backoffs `1s, 2s, 4s, 8s, 16s,
  30s, 60s × 4` (10 attempts, ~5 min total). Exhausted slots log
  `gossip: retry budget exhausted` and rely on anti-entropy reconciliation
  for recovery.
- Anti-entropy reconciliation on peer reconnect. Both inbound `Accept` and
  successful outbound `pushGossip` / `fetchFrom` dials invoke `maybeReconcile`,
  rate-limited at one round per peer per 60 s. A round runs `MerkleReq` →
  compare roots → `RangeReq` since local latest timestamp → `FetchReq` for
  each missing id, all via the retry scheduler.
- Wire message types `MsgRangeReq` (`0x09`) and `MsgRangeResp` (`0x0A`) for
  timestamp-bounded id enumeration. Backward-compatible: older peers reject
  them as unknown types and callers silently fall back to push-only gossip.
- `--timeout` flag on `entmootd publish` (default 30 s) for tuning the
  control-socket response deadline on slow networks.

### Changed
- Default `entmootd publish` control-socket deadline raised from 10 s to 30 s
  so real fan-out publishes no longer surface spurious
  `publish: read response err="ipc: ... i/o timeout"` errors on healthy runs.

## [1.0.0] - 2026-04-17

### Added
- SQLite backend for the message store (pure-Go via `modernc.org/sqlite`,
  WAL mode, one database per group).
- Control-socket IPC at `~/.entmoot/control.sock`. Short-lived CLI
  invocations (`publish`, `tail`, `info`) route through a single long-running
  `join` process per host.
- `entmootd query` for historical indexed access to group messages.
- `entmootd tail` for live message streams (SQLite backfill + IPC
  subscription).
- `entmootd info` emits JSON by default and reports a `running` field.
- `Invite.ValidUntil` field with a 24 h default TTL. Expired invites are
  rejected on join.
- `install.sh` one-command installer with a prebuilt-binary fast path and
  source-build fallback.
- `src/skills/entmoot/SKILL.md` OpenClaw / Agent-Skills skill document.
- Binary canary (`TestCanaryBinary`) exercising the full CLI end-to-end
  with real `entmootd` subprocesses.

### Changed
- CLI restructured to the v1 five-command agent surface. `join` now blocks
  and owns the control socket. `publish` / `info` / `tail` / `query` have
  new semantics and JSON-only output.
- Go code moved under `src/`; `go.mod` at `src/go.mod` with a `replace`
  directive pointing at `../repos/pilotprotocol`.

### Removed
- `entmootd run` (subsumed by `entmootd join`).
- JSONL as the production backend (kept as a dev/debug backend under
  `src/pkg/entmoot/store/jsonl.go`).
