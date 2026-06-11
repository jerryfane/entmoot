# Goal Prompt: Rewrite The Entmoot Research Paper

Use this prompt with `/goal` to rewrite the main Entmoot research paper as a
clean systems / operational-systems paper.

This goal is based on GitHub issue #63:

https://github.com/jerryfane/entmoot/issues/63

The current paper is useful as an engineering record, but it still describes
the implementation around `v1.5.35`. Entmoot is now at `v1.5.81` and includes
public moots, founder policies, the ESP public directory, social-first defaults,
Agent Skills and Codex/Claude plugin packaging, lexical message search, and
search-result jumps into message context.

Primary objective:

- Rewrite `paper/main.tex` around the current Entmoot system design.
- Keep the companion shakedown/evolution paper as deployment history.
- Do not invent evaluation results. The Evaluation/Results section should be a
  clearly marked future/placeholder section or an experiment-design section
  until the multi-agent experiment is actually run.
- Preserve or reuse accurate architectural material from the existing paper
  where it still describes the current system.
- Build the paper and update the website paper artifact only when the source is
  ready.
- Treat this as the paper rewrite/draft phase for issue #63, not the final
  closure of issue #63. The issue must remain open, or a follow-up issue must
  be opened, until the live multi-agent experiment is run and the Results
  section contains collected evidence.

## Ground Rules

- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Keep changes scoped to paper/docs unless a task explicitly says otherwise.
- Do not fabricate experimental results, measurements, transcript examples, or
  claims of live validation.
- Use citations for protocol, systems, and related-work claims. Prefer primary
  sources and official specs when adding new references.
- Keep release-history detail out of the new main paper. Put historical detail
  in `paper/evolution.tex` or the changelog, not in `paper/main.tex`.
- Preserve accurate existing content where it is still useful, but prioritize a
  coherent new paper over patching stale paragraphs.
- Avoid code/docs duplication. If a concept appears in multiple places, reuse
  terminology and definitions consistently.
- Do not commit generated LaTeX intermediates, temporary logs, local build
  caches, secrets, credentials, or runtime data.
- Commit generated PDFs only if the repo already tracks them and the task
  explicitly requires publishing updated paper artifacts.

## Fresh-Session Preflight

1. Confirm repo and branch state:
   - `pwd`
   - `git status --short --branch`
   - `git remote -v`
   - `git log --oneline -8`
2. Read these files before editing:
   - `docs/goal-pr-task-workflow.md`
   - `paper/main.tex`
   - `paper/evolution.tex`
   - `paper/refs.bib`
   - `paper/Makefile`
   - `CHANGELOG.md`
   - `README.md`
   - `ARCHITECTURE.md`
   - `website/docs/reference/papers.md`
   - `website/docs/reference/http-esp-api.md`
   - `website/docs/concepts/public-moot-directory.md`
   - `website/docs/architecture/esp-mobile.md`
   - `website/docs/operations/search-ui-qa.md`
3. Review GitHub issue #63 for the agreed paper direction.
4. Check whether `paper/entmoot-paper.pdf` and `website/static/papers/entmoot-main.pdf`
   are tracked before deciding whether to update generated PDFs.
5. If the worktree has unrelated changes, do not overwrite them. Stop and ask
   only if the dirty state makes task commits ambiguous.

## Target Paper Structure

The rewritten `paper/main.tex` should use this structure unless inspection
shows a better local fit:

1. Abstract
2. Introduction
3. Background and Related Work
4. System Overview
5. Design
6. Implementation
7. Use Cases
8. Evaluation Plan / Results Placeholder
9. Discussion and Limitations
10. Future Work
11. Conclusion

The eventual final paper may rename sections, but it must preserve the narrative:

problem -> design -> implementation -> use cases -> experiment/results -> limits.

## Task 1: Paper Outline And Content Triage

Branch: `task/paper-outline-triage`

Scope:

- Create a concise plan document for the rewrite before changing the main
  paper body.
- Triage existing paper content into:
  - reusable current architecture;
  - companion/evolution-only history;
  - stale or obsolete text to remove;
  - missing current features to add.

Details:

- Add `paper/REWRITE_PLAN.md` or an equivalent docs file.
- Map the new paper structure to source sections and existing reusable material.
- List the current features that must appear in the rewrite:
  - public moots and signed descriptors;
  - founder-managed policies;
  - ESP public directory;
  - ESP/mobile/web architecture;
  - member profiles and display names;
  - social-first defaults with Fleet/tasks disabled by default;
  - Agent Skills and Codex/Claude plugin packaging;
  - lexical search and message-context jumps;
  - live-agent replies as owner-controlled social participation.
- Identify related-work categories and initial citation targets.
- Define the Evaluation/Results placeholder contract: no fake results, only the
  experiment design until live data exists.

Checks:

- `git diff --check`
- Manual review of the plan against issue #63 and current `CHANGELOG.md`.
- `codex exec review --uncommitted`

Acceptance:

- The rewrite plan is committed and merged.
- The plan gives enough structure for the main rewrite without guessing.

Suggested commit message:

`docs: plan Entmoot research paper rewrite`

## Task 2: Rewrite Abstract, Introduction, And Contributions

Branch: `task/paper-intro-contributions`

Scope:

- Rewrite the front matter, abstract, introduction, and contribution list.
- Update stale version references from `v1.5.35` to the current system framing.

Details:

- Position Entmoot as social group communication infrastructure for autonomous
  agents over Pilot Protocol.
- Explain the gap: Pilot provides pairwise trust/transport, not durable group
  chat, shared searchable history, public discovery, or group policy.
- State contributions crisply:
  - signed moots, rosters, and invites;
  - gossip and anti-entropy reconciliation;
  - ESP/mobile/web access;
  - public discovery and founder policies;
  - agent-facing skill/plugin packaging;
  - planned real multi-agent evaluation.
- Avoid release-by-release narration.
- Keep the abstract honest: mention evaluation as planned if live results have
  not yet been collected.

Checks:

- `git diff --check`
- Build syntax check if practical: `cd paper && make`
- `codex exec review --uncommitted`

Acceptance:

- The paper opens with the current Entmoot thesis and does not claim missing
  evaluation results.

Suggested commit message:

`docs: rewrite Entmoot paper introduction`

## Task 3: Rewrite Background And Related Work

Branch: `task/paper-related-work`

Scope:

- Replace or rewrite the related-work/background section so it compares Entmoot
  to the right systems literature.

Details:

- Cover these categories:
  - Pilot Protocol and pairwise agent trust networks;
  - pub/sub and chat systems such as MQTT, Matrix, ActivityPub, and NATS;
  - P2P gossip and reconciliation such as Plumtree, GossipSub, Scuttlebutt, and
    Merkle logs / Certificate Transparency;
  - agent coordination and social-agent systems.
- Add or update BibTeX entries in `paper/refs.bib`.
- Compare and contrast. Do not write a list of unrelated summaries.
- Clarify that Entmoot is not primarily a task orchestration system; it is a
  social chat/shared-memory layer, with coordination features opt-in.

Checks:

- `cd paper && make`
- BibTeX/LaTeX warnings reviewed for missing citations.
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Related work explains why Entmoot is distinct and where it borrows proven
  techniques.

Suggested commit message:

`docs: rewrite Entmoot related work`

## Task 4: Rewrite System Design

Branch: `task/paper-system-design`

Scope:

- Rewrite the system overview and design sections around current Entmoot
  concepts.

Details:

- Define:
  - moot/group;
  - member identity;
  - signed roster;
  - targeted invite;
  - open invite;
  - public moot descriptor;
  - ESP;
  - policy;
  - topic;
  - mailbox cursor;
  - member profile/display name.
- Explain:
  - signed messages and topic routing;
  - gossip fanout;
  - anti-entropy reconciliation;
  - Merkle completeness model;
  - ESP-local state vs signed group/consensus state;
  - public visibility vs group membership;
  - descriptor indexing vs message/history indexing;
  - founder policy update semantics;
  - owner-controlled live replies;
  - social-first defaults and disabled Fleet/tasks.
- Add or update diagrams only if they make the design clearer. Prefer simple
  TikZ diagrams already compatible with the paper build.

Checks:

- `cd paper && make`
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- A reader can understand how Entmoot works without reading the changelog.
- Current features are included without turning the paper into release notes.

Suggested commit message:

`docs: rewrite Entmoot system design`

## Task 5: Rewrite Implementation Section

Branch: `task/paper-implementation`

Scope:

- Rewrite implementation details around the current architecture.

Details:

- Describe:
  - `entmootd`;
  - core packages for roster, store, gossip, mailbox, ESP HTTP, policy, public
    moot descriptors, plugin packaging, and skills;
  - SQLite storage and FTS5 lexical search;
  - message-context API for search-result jumps;
  - ESP HTTP API and mobile/web client role;
  - iOS and web client architecture at a high level;
  - Agent Skills package and Codex/Claude/OpenClaw integration;
  - deployment shape for the default ESP and entmoot.xyz, without exposing
    operational secrets.
- Move release history or debugging narrative to the companion paper if it is
  still valuable.

Checks:

- `cd paper && make`
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Implementation section reflects `v1.5.81` behavior and current production
  surfaces.

Suggested commit message:

`docs: rewrite Entmoot implementation section`

## Task 6: Add Use Cases And Evaluation Plan Placeholder

Branch: `task/paper-use-cases-eval-plan`

Scope:

- Add use cases and a non-fabricated Evaluation Plan / Results Placeholder
  section.

Details:

- Use cases should support the paper's technical claim:
  - public agent communities;
  - private agent workrooms;
  - persistent searchable group memory;
  - research discussion spaces;
  - human-observable agent social networks;
  - optional coordination workflows, clearly marked opt-in.
- Add an Evaluation Plan section for the later experiment:
  - question: can autonomous agents use a public moot to collaboratively analyze
    and improve Entmoot?
  - participants: Hermes, Deimos, OpenClaw agents, Codex/Claude-backed agents
    if practical, and optional additional nodes;
  - moots: The Ent Moot, Mars Hub, and one controlled experiment moot;
  - task/topic candidates: Entmoot architecture review, agent social
    code-of-conduct, multi-agent benchmark planning, public discovery and
    moderation tradeoffs;
  - metrics: delivery latency, convergence/completeness, Merkle root agreement
    where applicable, search correctness, search-result context jump behavior,
    live-agent replies, public directory discovery, policy enforcement,
    profile/display propagation, operational failures, and qualitative
    transcript analysis.
- Make it explicit that final Results will be filled after the live experiment.

Checks:

- `cd paper && make`
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Paper has a clear path for the later Results section without making
  unsupported claims.

Suggested commit message:

`docs: add Entmoot paper use cases and evaluation plan`

## Task 7: Rewrite Discussion, Limitations, Future Work, And Conclusion

Branch: `task/paper-discussion-conclusion`

Scope:

- Rewrite the ending sections so they match the new paper and current system.

Details:

- Discuss:
  - what Entmoot demonstrates now;
  - what remains unproven until the evaluation is run;
  - ESP centralization and availability tradeoffs;
  - privacy and metadata exposure;
  - public moot moderation limits;
  - policy enforcement limits;
  - scale limits and multi-ESP/federation future work;
  - semantic search as future work;
  - larger live deployments and longitudinal social-agent studies.
- Keep the conclusion short and thesis-driven.

Checks:

- `cd paper && make`
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- The paper closes honestly, with clear limitations and a crisp conclusion.

Suggested commit message:

`docs: rewrite Entmoot paper discussion`

## Task 8: Final Paper Build, Artifact Update, And Website Link Check

Branch: `task/paper-build-artifacts`

Scope:

- Build the final paper and update tracked website artifacts if appropriate.

Details:

- Run the LaTeX build:
  - `cd paper && make`
- Review build warnings and fix missing references/citations.
- Update `paper/entmoot-paper.pdf`.
- If `website/static/papers/entmoot-main.pdf` is tracked, copy/update the final
  built PDF there.
- Verify `website/docs/reference/papers.md` still points to the right artifact.
- Verify issue #63 is not closed by this task if the Results section still
  contains only the evaluation plan. If a separate tracking issue is preferred,
  create or link one for the live experiment and final Results update.
- Do not commit auxiliary LaTeX files unless they are already tracked and the
  repository intentionally tracks them.
- If generated auxiliaries are already tracked but should not be, do not clean
  that up in this task unless the user explicitly asks. Keep the task focused.

Checks:

- `cd paper && make`
- `git diff --check`
- inspect `git status --short` for accidental generated clutter
- `codex exec review --uncommitted`

Acceptance:

- The rewritten paper builds.
- The website paper artifact is updated if tracked.
- Issue #63 remains open, or a follow-up evaluation/results issue is linked,
  when live experiment results are still pending.
- No accidental logs/caches/secrets are committed.

Suggested commit message:

`docs: build rewritten Entmoot paper`

## Final Completion Criteria

- All task PRs are merged.
- `paper/main.tex` is a clean current research paper, not a changelog.
- `paper/evolution.tex` remains available as the shakedown/deployment-history
  companion.
- The Evaluation/Results section contains an explicit future experiment plan
  unless real collected evidence is already available. It must not contain
  fabricated results.
- The LaTeX paper builds successfully.
- The website paper artifact is refreshed if tracked.
- This goal is not used as evidence that issue #63 is fully complete unless the
  live multi-agent experiment has been run and the Results section has been
  updated with collected evidence.
- Final response lists:
  - completed tasks;
  - branch and PR for each task;
  - merged commit hashes;
  - checks run;
  - skipped checks or residual risks;
  - exact final `codex exec review --uncommitted` output for the final task.
