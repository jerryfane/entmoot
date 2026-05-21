# Goal Prompt: Public Moot Directory and Founder Policies

Use this prompt with `/goal` to implement public moot visibility/indexing and
default/custom founder-managed policies.

This workflow builds on `docs/goal-pr-task-workflow.md` and the completed
default-moot work in `docs/goal-the-ent-moot-default-public-moot.md`. Keep the
same task-by-task discipline, review-fix loop, PR gates, release gates, and real
peer validation discipline.

```text
Implement public moot visibility/indexing and default/custom group policies
task by task. Each task must be developed, reviewed, opened as its own pull
request, merged, released when needed, deployed when needed, and verified before
moving on, unless tasks are explicitly safe to run in parallel.

Primary objective:
- Give every newly-created moot a clean policy story:
  - existing groups with no policy keep current behavior;
  - new groups default to a safe standard policy;
  - founders can inspect, set, clear, and propagate policies;
  - policy enforcement remains local and protects the receiving node.
- Add public moot visibility:
  - `visibility` and `join_mode` are separate;
  - public listing does not require the default ESP to join the group;
  - the default ESP indexes signed public descriptors;
  - entmoot.xyz lists public moots from the default ESP directory;
  - message/history indexing remains separate from descriptor indexing.

Product rules:
- `open invite` means anyone can join through a reusable invite.
- `public` means eligible for discovery/listing on the public directory.
- Do not make `public` imply `open invite`.
- Do not make `open invite` imply public directory listing.
- The default ESP must not automatically join every public moot.
- The default ESP may delist, block, or hide public directory entries from
  Entmoot-operated surfaces to protect users and infrastructure.
- Founder/admin policy updates are for cooperating nodes. A malicious peer can
  ignore its own local policy, but each receiving node must still enforce the
  policy it accepts locally.
- Do not silently enable live replies in any public moot. Live participation
  remains a separate owner decision.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- If tasks are independent, have disjoint write sets, and do not depend on each
  other's results, they may run in parallel on separate branches.
- Do not start dependent work until the prerequisite task has passed checks,
  passed `codex exec review --uncommitted`, been pushed, opened as a PR,
  merged, and verified on the target branch.
- Each task must get its own PR and merge before the next dependent task begins.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. Extract shared helpers for repeated policy parsing,
  policy summaries, descriptor canonicalization/signing, metadata normalization,
  visibility/join-mode validation, ESP directory DTO mapping, API path encoding,
  CLI rendering, release validation, SSH/container discovery, and test fixtures
  when that matches existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, or large runtime dumps unless the task
  explicitly says they are intended tracked fixtures.
- Every time a plan, external integration, HTTP API, public descriptor format,
  signing format, ESP behavior, website build/deploy behavior, GitHub CLI
  action, release process, service launcher, Docker/container behavior,
  OpenClaw/Hermes behavior, rate-limit behavior, TURN behavior, or third-party
  tool behavior matters, perform a current web search and prefer official or
  primary sources before editing. Record the relevant source in the PR body.

Before starting:
1. Inspect current repo state for every repo that may be touched:
   - `/Users/jerryfane/Desktop/repo/entmoot`
   - `/Users/jerryfane/Desktop/repo/entmoot-web`
   - `git status --short`
   - current branch
   - current remote
   - latest tags/releases if a release may be needed
2. If the target branch is unclear, the remote looks wrong, or any worktree has
   unrelated existing changes that make task commits ambiguous, stop and ask.
3. Inspect and follow:
   - `docs/goal-pr-task-workflow.md`
   - `docs/goal-the-ent-moot-default-public-moot.md`
   - `docs/OPERATIONS.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - existing `group`, `default-moot`, `join`, `invite`, `esp`, `policy`,
     `ratelimit`, `gossip`, `store`, `agent-live`, and release patterns.
4. Inspect entmoot-web before touching it:
   - Nest server proxy under `server/src/entmoot`;
   - Vue explorer under `frontend/src/api`, `frontend/src/stores`,
     `frontend/src/pages`, and `frontend/src/components/entmoot`;
   - production process config in `ecosystem.config.js`.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - repo remotes resolve to the expected GitHub repositories.
6. Verify external contracts before editing:
   - RFC 8615 well-known URI guidance when adding public descriptor locations.
   - RFC 7033 WebFinger only if adding handle-style discovery.
   - W3C ActivityPub public/discovery concepts only as design reference; do not
     implement ActivityPub compatibility unless explicitly scoped.
   - Schema.org structured data if adding public directory JSON-LD.
   - Current Entmoot descriptor, open-invite, ESP auth, and policy behavior
     from repo code and live CLI help.

Definitions:
- `visibility=private`: not listed, invite-only by default.
- `visibility=unlisted`: not listed, but joinable if a descriptor/link is
  shared.
- `visibility=public`: eligible for default ESP/entmoot.xyz directory listing.
- `join_mode=invite_only`: public page can exist, but joining still requires an
  invite.
- `join_mode=open_invite`: public descriptor may include an open-invite
  descriptor/link.
- `mirror_state=none`: ESP indexes descriptor only; it is not a group member.
- `mirror_state=member`: ESP is a member and can show member-scoped data.
- `mirror_state=hosted`: ESP is intentionally hosting/mirroring messages.

Task 1: Policy presets, custom policy parsing, and local CLI
1. Add reusable policy presets in `pkg/entmoot/policy`:
   - `standard`: the current The Ent Moot policy values;
   - `relaxed`: higher limits suitable for private/high-volume groups;
   - `none`: no stored per-moot policy, preserving legacy behavior.
2. Centralize policy parsing/validation so CLI, default-moot, ESP group create,
   and descriptor code do not duplicate field parsing, file parsing, summaries,
   or preset handling.
3. Add:
   - `entmootd group policy status -group <GROUP_ID> [--json]`;
   - `entmootd group policy set -group <GROUP_ID> -preset standard|relaxed|none [--json]`;
   - `entmootd group policy set -group <GROUP_ID> -file policy.json [--json]`;
   - `entmootd group policy clear -group <GROUP_ID> [--json]`.
4. `status` must show:
   - stored policy if present;
   - effective runtime behavior;
   - whether the running daemon has applied the policy if discoverable;
   - a concise policy summary.
5. `set` must validate before storing. `clear` must remove local policy and
   restore legacy no-policy behavior.
6. Add focused tests for:
   - preset values and validation;
   - malformed custom policy files;
   - text and JSON CLI output;
   - `clear` restoring no-policy behavior;
   - default-moot policy behavior remaining unchanged.

Task 2: Founder-signed policy update propagation
1. Add founder-signed policy update messages on reserved topic
   `_entmoot/policy/v1`.
2. Payload type:
   - `type`: `entmoot.policy_update.v1`;
   - `group_id`;
   - `policy`;
   - `updated_at_ms`;
   - `sequence`.
3. Only the group founder can publish accepted policy updates in v1.
4. Receiving nodes must verify:
   - message author is the group founder;
   - payload group matches the message group;
   - policy validates;
   - sequence is newer than the last accepted policy update.
5. Accepted updates must be persisted in the existing policy store and applied
   to a running gossiper through the existing runtime policy path.
6. Invalid, stale, or non-founder update messages must not alter local policy.
7. `group policy set` must publish the update through the daemon by default
   when the group is active. Add `--local-only` for operators who intentionally
   want local-only policy changes.
8. Add tests for:
   - founder update applies locally and on another node;
   - non-founder update ignored;
   - stale sequence ignored;
   - invalid policy rejected without crashing;
   - daemon restart loads the latest stored policy.

Task 3: Group creation defaults, metadata, visibility, and join mode
1. Extend `entmootd group create`:
   - `-name`;
   - `-description`;
   - repeated `-tag`;
   - `-visibility private|unlisted|public`;
   - `-join-mode invite_only|open_invite`;
   - `-policy preset:standard|preset:relaxed|none|file:policy.json`;
   - `--json`.
2. Defaults:
   - `visibility=private`;
   - `join_mode=invite_only`;
   - `policy=preset:standard`.
3. Existing groups with no policy must remain unchanged.
4. Store group display metadata through the existing ESP metadata format/helper
   where appropriate; do not create a second incompatible metadata format.
5. If `join_mode=open_invite`, create an open invite only when the local runtime
   can issue one safely. If not available, return a clear next-step error and
   leave the group and policy state consistent.
6. Output must include:
   - group id;
   - founder identity;
   - visibility;
   - join mode;
   - effective policy summary;
   - open-invite descriptor/link when created;
   - next command to publish a public descriptor when `visibility=public`.
7. Add tests for:
   - default create stores `standard` policy;
   - `-policy none` preserves no-policy behavior;
   - custom policy file;
   - metadata normalization;
   - public metadata;
   - open-invite join mode success and clear failure modes.

Task 4: Public moot descriptor package and publish CLI
1. Add a general signed public moot descriptor package by reusing/refactoring
   the default-moot canonical signing/verification helpers.
2. Descriptor type: `entmoot.public_moot.v1`.
3. Descriptor fields:
   - `group_id`;
   - `name`;
   - `description`;
   - `tags`;
   - `visibility`;
   - `join_mode`;
   - optional `open_invite`;
   - `policy`;
   - `founder`;
   - `indexing.directory`;
   - `indexing.messages`;
   - `updated_at_ms`;
   - optional `expires_at_ms`;
   - `signature`.
4. `indexing.directory=true` means descriptor listing only.
5. `indexing.messages=true` must be rejected in v1 unless an explicit hosted or
   member mirror path already exists and is verified by the ESP.
6. Add:
   - `entmootd group public descriptor -group <GROUP_ID> [--json]`;
   - `entmootd group public publish -group <GROUP_ID> -esp-url <URL> [--json]`.
7. Descriptor generation must fail unless the local identity is the founder and
   local metadata says `visibility=public`.
8. Add tests for:
   - canonical signing and verification;
   - tamper detection;
   - wrong founder key rejection;
   - private/unlisted groups rejected for public descriptor generation;
   - embedded open-invite descriptor preserved;
   - invalid indexing flags rejected.

Task 5: ESP public directory store and API
1. Add ESP public directory persistence to the existing ESP state store instead
   of a separate ad hoc database.
2. Add public directory endpoints:
   - `GET /v1/public-moots`;
   - `GET /v1/public-moots/{group_id}`;
   - `POST /v1/public-moots`.
3. Add operator/admin endpoint:
   - `PATCH /v1/public-moots/{group_id}/index-status`.
4. Directory statuses:
   - `listed`;
   - `pending`;
   - `delisted`;
   - `blocked`.
5. `POST /v1/public-moots` may be unauthenticated, but it must accept only
   valid founder-signed public descriptors.
6. Store only the newest descriptor per group by `updated_at_ms`.
7. Do not join the group as part of listing.
8. Public list output must include:
   - descriptor fields safe for public display;
   - policy summary;
   - `mirror_state=none|member|hosted`;
   - whether message history is available from this ESP.
9. Existing `/v1/groups` behavior must remain member/local-group listing and
   must not become the public directory endpoint.
10. Add tests for:
    - valid descriptor listed;
    - stale descriptor ignored;
    - invalid signature rejected;
    - blocked group/founder rejected;
    - listed descriptor does not require local group membership;
    - existing authenticated group APIs unchanged.

Task 6: entmoot-web public directory integration
1. In `/Users/jerryfane/Desktop/repo/entmoot-web`, update the Nest server proxy
   to read `GET /v1/public-moots` from the ESP directory first.
2. Keep existing hosted/mirrored group member/topic/message proxy paths.
3. Extend shared DTOs with:
   - `visibility`;
   - `joinMode`;
   - `policySummary`;
   - `mirrorState`;
   - `openInviteLink`;
   - `messageHistoryAvailable`.
4. Update `/explore` to show directory-only public moots and mirrored moots in
   one list.
5. Update group detail so directory-only groups show metadata, policy, join
   mode, and open-invite action without trying to render missing history as an
   error.
6. Show message feed only when `messageHistoryAvailable=true`.
7. Add Schema.org `DataCatalog` / `Dataset` JSON-LD for listed public moots.
8. Preserve the current visual direction and explorer ergonomics. Do not build a
   marketing-only page; keep `/explore` useful as the first screen for public
   discovery.
9. Add tests/build checks for:
   - server mapping of directory-only entries;
   - frontend render of directory-only entries;
   - group detail with no mirrored history;
   - existing mirrored group flow.

Task 7: Documentation, operator guidance, and examples
1. Update Entmoot docs:
   - README;
   - `docs/OPERATIONS.md`;
   - CLI/help docs;
   - website reference docs;
   - agent-facing docs/skills if present.
2. Document:
   - public is separate from open invite;
   - listed is separate from ESP membership;
   - descriptor indexing is separate from message/history indexing;
   - policy is local enforcement;
   - founder-signed policy updates apply only to cooperating nodes;
   - default ESP can delist/block entries on Entmoot-operated surfaces;
   - live replies remain opt-in per node.
3. Add copy-paste examples with placeholder values only. Do not use real
   credentials, tokens, private keys, or live production secrets.
4. Keep docs aligned with real `entmootd --help`, ESP API behavior, and
   entmoot-web behavior.

Task 8: Release, deploy, and live validation
1. If any merged PR changes runtime behavior, helper behavior, installer
   behavior, CLI behavior, release workflow, open-invite behavior, live-agent
   behavior, policy behavior, descriptor behavior, ESP API behavior, website
   behavior, or deployed peer operation, cut a new Entmoot release.
2. Before tagging, run the release checklist in `docs/OPERATIONS.md`.
3. Verify GitHub release workflow succeeds and expected assets exist.
4. Deploy entmoot-web only after the ESP/API release it depends on is live.
5. Validate using real running agents:
   - Use SSH multiplexing with credentials from
     `/Users/jerryfane/Desktop/repo/entmoot/temp/vps-ssh.txt`.
   - Never print, paste, commit, or summarize credentials.
   - Check VPS, Deimos, and Hermes when runtime behavior changes.
   - Discover actual runtime namespaces, containers, binary paths, data roots,
     Pilot sockets, control sockets, process trees, and running versions from
     live state. Do not assume host paths.
   - Do not run a second Entmoot install beside the existing one.
6. Real-peer validation must prove:
   - current Entmoot version on each updated peer;
   - `entmootd env --json` is healthy in the correct namespace;
   - direct publish/query smoke still works;
   - policy update propagation works on a test moot;
   - public descriptor publish appears in the default ESP public directory;
   - the default ESP does not join a directory-only public moot;
   - entmoot.xyz renders the public directory entry;
   - existing The Ent Moot default-moot descriptor/status still works;
   - live agents remain opt-in and are not silently enabled;
   - relevant logs show no new invalid JSON, empty output, socket unreachable,
     missing child process, restart loop, or OOM evidence.
7. Close SSH multiplex masters before final response.

Review-fix loop:
1. If `codex exec review --uncommitted` finds issues, do not only patch the
   literal line.
2. Identify the underlying invariant/class of bug.
3. Audit nearby and sibling paths for the same issue.
4. Write a concise fix plan using:
   "Review found these issues: <<PASTE RAW REVIEW RESULTS BY REPO>>.
   For each issue, identify the underlying invariant/class of bug, audit sibling
   paths for the same issue, and plan the smallest safe fix. Verify external
   assumptions with local commands and/or official sources. Preserve repo
   patterns, avoid unnecessary refactors, and list tests/checks per repo."
5. Execute the fix plan.
6. Re-run focused tests/checks and `codex exec review --uncommitted` in every
   repo with uncommitted changes.
7. Repeat until the final raw review output contains no findings, or stop if
   blocked or if a finding is incorrect after verification.

Commit, PR, and merge gate:
1. Before committing, run `git diff --check` and inspect the final diff.
2. Commit only the current task's intended tracked changes.
3. Use a concise conventional commit message that describes only the current
   task unless this prompt specifies a different message.
4. Push the task branch.
5. Create one PR for the current task.
6. PR title must describe only the current task.
7. PR body must include:
   - WHY: why the task was needed and what evidence supports the design;
   - WHAT: what changed at a behavioral level;
   - CHANGES: concrete implementation changes;
   - RESULTS: tests, checks, operational smoke, and review results;
   - NEXT STEPS: release, deploy, follow-up monitoring, or residual work.
8. Include the exact raw final `codex exec review --uncommitted` output for each
   changed repo in the PR body.
9. If CI or required checks exist, wait for them and fix failures before merge.
10. Merge using the repository's configured/preferred merge method. If no
    preference is discoverable, use squash merge for clean task-level history.
11. After merge, update the local target base branch and verify every touched
    worktree is clean.
12. Record PR number, PR URL, branch name, and merged commit hash.

Parallel task rules:
- Parallelize only when tasks are independent, have disjoint write sets, and can
  be reviewed and merged without order-dependent assumptions.
- Use one branch per task.
- Clearly assign each branch a task number and file ownership.
- Do not duplicate work across branches.
- If parallel branches conflict after one PR merges, rebase or update the
  remaining branch on the latest target base and re-run checks and review.
- If a task becomes dependent on another task, stop treating it as parallel and
  merge the dependency first.

Final response after all tasks:
- List completed tasks.
- For each task, list branch, PR URL, merge status, and merged commit hash.
- List releases cut, website deploys, and peers updated.
- List tests/checks run.
- Include exact final raw `codex exec review --uncommitted` output for the last
  task/repo.
- Include public-directory validation evidence:
  - descriptor group id;
  - directory API response;
  - website URL;
  - mirror state proving the ESP did not auto-join directory-only moots.
- Include real-agent validation evidence from VPS, Deimos, and Hermes when
  peer validation was required:
  - discovered namespace/container;
  - running Entmoot version;
  - publish/query smoke;
  - policy propagation status;
  - default-moot status;
  - live-agent status when relevant.
- Mention skipped checks, blockers, or residual risk.
- Do not claim interactive `/review` is clean. Say:
  "codex exec review is clean; ready for manual /review."
```
