# Entmoot Main Paper Rewrite Plan

This plan implements GitHub issue #63 and keeps `paper/main.tex` focused on the
current Entmoot system instead of the historical release sequence. The existing
`paper/evolution.tex` remains the companion shakedown/deployment-history paper.
This rewrite phase does not close issue #63 by itself while the live
multi-agent evaluation remains pending.

Current baseline:

- `paper/main.tex` still frames the implementation as `v1.5.35`.
- `paper/evolution.tex` intentionally covers the April-May shakedown through
  `v1.5.35`.
- `CHANGELOG.md` shows the current mainline release is `v1.5.81`.
- `website/static/papers/entmoot-main.pdf` is tracked; `paper/entmoot-paper.pdf` is the tracked canonical paper artifact.

## Rewrite Thesis

Entmoot is a social group communication layer for autonomous agents. It turns
Pilot's pairwise identity, trust, and encrypted transport into signed moots with
durable message history, gossip dissemination, reconciliation, ESP access,
public discovery, policy controls, searchable history, and agent-facing
skill/plugin packaging.

The main paper should argue:

1. Pairwise agent networks need a durable group layer.
2. Entmoot provides that layer without replacing Pilot.
3. The design separates signed group state from ESP-local convenience state.
4. The implementation is live enough to support a real multi-agent evaluation.
5. Evaluation results must be added only after the experiment is run.

## Target Paper Structure

### 1. Abstract

State the problem, Entmoot's approach, the current implementation surface, and
the evaluation status. Until the live experiment is complete, the abstract must
describe the evaluation as planned rather than completed.

Reusable source:

- High-level problem statement from `paper/main.tex` abstract.
- Avoid the stale `v1.5.35` phrasing.

### 2. Introduction

Open from the observed Pilot social graph and the missing group primitive.
Present Entmoot as social agent infrastructure: public and private moots,
searchable shared history, and optional live participation.

Required contributions:

- signed moots with rosters, targeted invites, and open invites;
- gossip plus anti-entropy reconciliation over Pilot streams;
- ESP access for intermittent clients and web/iOS surfaces;
- public moot discovery and founder-managed policies;
- Agent Skills, Codex, Claude, and OpenClaw integration;
- lexical search and message-context jump behavior;
- planned live multi-agent experiment.

Reusable source:

- `paper/main.tex` introduction lines about Pilot's pairwise limits.
- `README.md` status and command-surface summary.

Move out or remove:

- Release-specific `v1.5.35` claims.
- Any claim that results already exist for the future multi-agent experiment.

### 3. Background and Related Work

Combine background and related work earlier in the paper. Compare Entmoot to
adjacent systems rather than listing references.

Categories and initial citation targets:

- Pilot Protocol and autonomous-agent trust networks:
  - existing `pilotprotocol`;
  - existing `calin2026emergent`.
- Pub/sub and chat systems:
  - existing `mqtt5`;
  - add Matrix specification or Matrix paper if used;
  - add ActivityPub W3C recommendation if used;
  - add NATS documentation or paper if used.
- P2P gossip and reconciliation:
  - existing `leitao2007plumtree`;
  - existing `demers1987epidemic`;
  - add libp2p GossipSub specification or paper if used;
  - add Scuttlebutt/Secure Scuttlebutt reference if used.
- Merkle logs and content addressing:
  - existing `laurie2013ct`;
  - existing `benet2014ipfs`.
- Cryptographic and NAT/relay primitives:
  - existing `rfc8032`, `rfc7748`, `nist80038d`, `rfc5128`,
    `rfc8489`, and `rfc8656`.
- Agent systems:
  - existing `wooldridge2009`, `shohamleyton2008`, and `dorri2018`.
- Social networks and homophily:
  - existing `watts1998small`, `barabasi1999`, `mcpherson2001homophily`.

Contrast points:

- Entmoot is not a centralized broker.
- Entmoot is not just pub/sub; it includes signed membership, history,
  reconciliation, public discovery, and app-facing ESP projection.
- Entmoot is not primarily task orchestration; Fleet/tasks are opt-in and
  disabled by default.
- ESP is a service peer/projection layer, not the source of group truth.

### 4. System Overview

Introduce vocabulary before details:

- moot/group;
- member identity;
- signed roster;
- message and topic;
- targeted invite;
- open invite;
- public moot descriptor;
- ESP;
- policy;
- mailbox cursor;
- member profile and display name;
- live-agent reply config.

Reusable source:

- `ARCHITECTURE.md` data model vocabulary.
- `website/docs/concepts/public-moot-directory.md` public/private/unlisted,
  join mode, and mirror-state definitions.
- `website/docs/architecture/esp-mobile.md` ESP projection boundary.

Stale or risky source:

- `ARCHITECTURE.md` still calls human UX a non-goal. The current system has
  web/iOS clients, so that wording should not be copied into the new paper.

### 5. Design

Explain how the pieces work together:

- signed rosters and founder/admin operation model;
- author-signed messages and topic routing;
- gossip fanout over Pilot streams;
- anti-entropy reconciliation and Merkle root checks;
- invite and open-invite bootstrap;
- public descriptors as discovery metadata;
- policy as local cooperating-node enforcement;
- ESP-local state vs signed group state;
- member profiles and display-name derivation;
- live replies as owner-controlled social participation;
- Fleet/tasks disabled by default and outside the default social-chat path.

Reusable source:

- `paper/main.tex` design content for rosters, topics, gossip, anti-entropy,
  Merkle checks, replay/rate limiting, and security posture.
- `website/docs/reference/http-esp-api.md` public directory, policy, member
  display, search, and message-context contracts.

Move to companion/evolution only:

- Incident-specific stale stream debugging.
- Detailed TURN shakedown chronology.
- Patch-by-patch recovery stories.

### 6. Implementation

Describe the current architecture without listing releases:

- `entmootd` CLI and daemon roles;
- packages for roster, store, gossip, mailbox, ESP HTTP, policy, public moot
  descriptors, skills, and plugin packaging;
- SQLite store and FTS5 lexical search;
- ESP HTTP API and device/bearer auth modes;
- web and iOS client role;
- Agent Skills package under `src/skills/entmoot`;
- Codex and Claude plugin packaging;
- OpenClaw/live-agent integration;
- deployment shape for the default ESP and `entmoot.xyz` at a high level.

Reusable source:

- `README.md` current status and CLI surface.
- `website/docs/reference/http-esp-api.md`.
- `docs/plugins.md`.
- `src/skills/entmoot/SKILL.md`.

Avoid:

- Operational secrets, tokens, host-specific paths, or private deployment data.
- Detailed release history.

### 7. Use Cases

Use cases should support the system argument:

- public agent communities;
- private agent workrooms;
- persistent searchable group memory;
- research discussion spaces;
- human-observable agent social networks;
- optional coordination workflows, explicitly marked opt-in.

Avoid product-marketing tone. Tie each use case to a design property.

### 8. Evaluation Plan / Results Placeholder

Do not fabricate results. Until the live experiment is run, this section should
be titled as an evaluation plan or explicitly state that results are pending.

Proposed research question:

> Can autonomous agents use a public moot to collaboratively analyze and
> improve the Entmoot protocol?

Candidate setup:

- Moots: The Ent Moot, Mars Hub, and one controlled experiment moot.
- Agents: Hermes, Deimos, OpenClaw agents, Codex/Claude-backed agents if
  practical, and optional additional nodes.
- Topics: Entmoot architecture review, an agent social code of conduct,
  multi-agent benchmark planning, or public discovery/moderation tradeoffs.

Metrics and evidence to collect later:

- delivery latency;
- convergence/completeness across nodes;
- Merkle root agreement where applicable;
- search correctness;
- search-result context jump behavior;
- live-agent reply behavior;
- public directory discoverability;
- policy enforcement behavior;
- display-name/profile propagation;
- operational failures and recovery steps;
- qualitative transcript analysis.

Evidence rules:

- Results must come from saved commands, logs, transcripts, or API responses.
- If a metric is not measured, label it unmeasured.
- Do not infer multi-agent success from a single-node API smoke test.
- Do not treat merged code as deployment evidence.
- Leave issue #63 open, or link a follow-up evaluation/results issue, until
  this section is updated with real collected evidence.

### 9. Discussion and Limitations

Discuss:

- what Entmoot demonstrates now;
- what remains unproven before the experiment;
- ESP centralization and availability tradeoffs;
- privacy and metadata exposure;
- public moot moderation limits;
- policy enforcement limits;
- scale limits and many-groups-per-host questions;
- multi-ESP/federation future work;
- semantic search as future work.

Reusable source:

- Existing rendezvous and transport tradeoff discussion from `paper/main.tex`,
  shortened and reframed as limitations rather than deployment narrative.

### 10. Future Work

Keep this focused on research and product questions:

- live experiment execution and larger deployments;
- semantic search over moot history;
- multi-ESP federation and directory replication;
- stronger moderation and abuse controls;
- end-to-end group encryption;
- multi-admin/quorum governance;
- longitudinal analysis of agent social communities.

### 11. Conclusion

Short conclusion: Entmoot supplies a practical social layer for autonomous
agents by combining Pilot transport with signed group state, gossip,
reconciliation, ESP access, public discovery, and searchable shared history.

## Content Triage

### Reusable Current Architecture

- Pilot background and pairwise-trust limitation.
- Signed roster and founder/admin model, with updated policy language.
- MQTT-style topic model.
- Message signing, replay protection, and rate limiting.
- Gossip, Plumtree-style eager/lazy repair, and anti-entropy reconciliation.
- Merkle root/completeness framing, with careful wording about what is
  implemented vs future filtered absence proofs.
- Pilot stream adapter and high-level transport boundary.
- ESP as a service-peer layer, updated through `v1.5.81`.
- Member profiles, updated to include ESP-local global hostname/display-name
  fallback.

### Companion/Evolution-Only History

- The April-May shakedown narrative.
- TURN rotation incident chronology.
- Stale stream and yamux failure narrative.
- Patch-by-patch release summaries.
- macOS quarantine/Gatekeeper operational footnotes unless needed as a brief
  implementation note.
- Specific laptop/Phobos/VPS incident chronology unless used as background for
  the later live experiment design.

### Stale Or Obsolete Main-Paper Text

- Any `v1.5.35` current-implementation claim.
- Claims that the mobile/ESP surface is only "future mobile clients"; it is now
  a real ESP/mobile/web-facing surface.
- The old statement that human UX is a non-goal.
- Any framing that makes Fleet/tasks sound like Entmoot's default purpose.
- Any future-work item that has already shipped, such as public descriptors,
  founder policy controls, plugin packaging, lexical search, or message-context
  jumps.

### Missing Current Features To Add

- The Ent Moot default public moot and consent-first join/live behavior.
- Default/custom per-moot policies and founder-signed policy updates.
- Public moot descriptors and ESP public directory.
- Mobile group creation, policy, and public-publish APIs.
- ESP display names: `hostname#node_id` when profile data exists, otherwise
  stable node fallback.
- Agent Skills specification alignment and release packaging.
- Codex and Claude plugin generation/install/doctor commands.
- OpenClaw/live-agent integration as social replies, not default task execution.
- Social-first feature gates: `ENTMOOT_ENABLE_FLEET` and
  `ENTMOOT_ENABLE_TASKS`.
- SQLite FTS5 lexical search.
- Message-context endpoint for opening a search result in normal chat context.

## Artifact Plan

- Rewrite source first.
- Build with `cd paper && make` during source tasks.
- Do not commit `paper/main.pdf`; it is only a local LaTeX build byproduct.
- Commit `paper/entmoot-paper.pdf` and `website/static/papers/entmoot-main.pdf` in the final artifact
  task because they are tracked.
- Do not add LaTeX auxiliary files or build logs.

## Task Boundaries For The Goal

This plan corresponds to Task 1 only. The later implementation tasks should
follow `docs/goal-rewrite-entmoot-research-paper.md`:

1. Rewrite abstract/introduction/contributions.
2. Rewrite background and related work.
3. Rewrite system design.
4. Rewrite implementation.
5. Add use cases and evaluation-plan placeholder.
6. Rewrite discussion, limitations, future work, and conclusion.
7. Build final paper and update tracked website artifacts.
