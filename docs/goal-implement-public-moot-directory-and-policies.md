# Goal Prompt: Implement Public Moot Directory and Founder Policies

Use this prompt with `/goal` to implement the public moot visibility/indexing
feature and default/custom founder-managed policies.

This goal intentionally follows the same operating style as
`docs/goal-pr-task-workflow.md`, `docs/goal-the-ent-moot-default-public-moot.md`,
and `docs/goal-public-moot-directory-and-policies.md`: task-by-task branches,
one PR per task, review-fix loops, current contract verification, release gates,
deployment gates, and real runtime validation where the change affects running
agents or public infrastructure.

Treat `docs/goal-public-moot-directory-and-policies.md` as the design plan and
this file as the executable goal prompt.

```text
Implement public moot visibility/indexing and default/custom founder-managed
policies task by task.

Primary objective:
- New moots get safe policy defaults.
- Existing groups with no policy keep current behavior.
- Founders can inspect, set, clear, and propagate policies.
- Public moot discovery is separate from group membership.
- The default ESP indexes signed public descriptors without automatically
  joining every public moot.
- entmoot.xyz can list public moots from the default ESP directory.

Product rules:
- `visibility` and `join_mode` are separate concepts.
- `visibility=public` means eligible for public directory listing.
- `join_mode=open_invite` means anyone with the descriptor/link can join.
- Do not make public imply open invite.
- Do not make open invite imply public listing.
- The default ESP must not automatically join every public moot.
- Descriptor indexing is separate from message/history indexing.
- Message/history indexing requires explicit member/hosted mirror behavior.
- Founder policy updates are for cooperating nodes; every receiving node still
  enforces the policy it accepts locally.
- Do not silently enable live replies in any moot. Live participation remains a
  separate owner decision.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized.
- Avoid code duplication. Extract small reusable helpers for repeated policy
  parsing, policy summaries, descriptor canonicalization/signing, metadata
  normalization, visibility/join-mode validation, ESP DTO mapping, CLI
  rendering, API path encoding, release checks, SSH/container discovery, and
  test fixtures when that matches existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, or large runtime dumps unless the task
  explicitly says they are intended tracked fixtures.
- Never print, paste, commit, or summarize credentials, private keys, tokens, or
  live secrets.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes HTTP API
  behavior, descriptor formats, signing/canonicalization, RFC well-known
  behavior, GitHub CLI actions, release tooling, Docker/container behavior,
  service launchers, OpenClaw/Hermes behavior, TURN behavior, entmoot-web
  deployment behavior, and third-party libraries.

Fresh-session and resume preflight:
1. Inspect current repo state for every repo that may be touched:
   - `/Users/jerryfane/Desktop/repo/entmoot`
   - `/Users/jerryfane/Desktop/repo/entmoot-web` when website work begins
   - `git status --short --branch`
   - current branch
   - current remotes
   - latest relevant local and remote commits
   - latest tags/releases if a release may be needed
2. If there are uncommitted changes, identify whether they belong to the current
   task. Do not overwrite or revert unrelated user changes. Stop and ask only if
   the dirty state makes task commits ambiguous.
3. Verify which tasks, PRs, and commits are already merged before skipping any
   task. Do not rely only on memory.
4. If resuming from an interrupted branch, finish the active task first unless
   the branch is clearly abandoned or the user explicitly redirects.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - the remote resolves to the expected GitHub repository.
6. Inspect and follow current repo patterns before editing:
   - `docs/goal-pr-task-workflow.md`
   - `docs/goal-the-ent-moot-default-public-moot.md`
   - `docs/goal-public-moot-directory-and-policies.md`
   - `docs/OPERATIONS.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - existing `group`, `default-moot`, `join`, `invite`, `esp`, `policy`,
     `ratelimit`, `gossip`, `store`, `agent-live`, descriptor, and release code
7. Before touching entmoot-web, inspect:
   - Nest server proxy under `server/src/entmoot`
   - Vue API/stores/pages/components under `frontend/src`
   - production process config in `ecosystem.config.js`

Definitions:
- `visibility=private`: not listed, invite-only by default.
- `visibility=unlisted`: not listed, but joinable if a descriptor/link is
  shared.
- `visibility=public`: eligible for default ESP/entmoot.xyz directory listing.
- `join_mode=invite_only`: public page can exist, but joining still requires an
  invite.
- `join_mode=open_invite`: descriptor may include an open-invite descriptor or
  link.
- `mirror_state=none`: ESP indexes descriptor only; it is not a group member.
- `mirror_state=member`: ESP is a member and can expose member-scoped data.
- `mirror_state=hosted`: ESP intentionally hosts or mirrors messages.

Task 1: Policy presets, custom policy parsing, and local CLI
1. Add reusable policy presets in `pkg/entmoot/policy`:
   - `standard`: current The Ent Moot policy values;
   - `relaxed`: higher limits suitable for private/high-volume groups;
   - `none`: no stored per-moot policy, preserving legacy behavior.
2. Centralize policy parsing/validation so CLI, default-moot, ESP group create,
   descriptor code, tests, and docs do not duplicate field parsing, file
   parsing, summaries, or preset handling.
3. Add:
   - `entmootd group policy status -group <GROUP_ID> [--json]`;
   - `entmootd group policy set -group <GROUP_ID> -preset standard|relaxed|none [--json]`;
   - `entmootd group policy set -group <GROUP_ID> -file policy.json [--json]`;
   - `entmootd group policy clear -group <GROUP_ID> [--json]`.
4. `status` must show stored policy, effective runtime behavior, daemon-applied
   state when discoverable, and a concise policy summary.
5. `set` must validate before storing. `clear` must remove local policy and
   restore legacy no-policy behavior.
6. Add focused tests for presets, validation, malformed files, text/JSON CLI
   output, clear behavior, and unchanged default-moot behavior.

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
4. Receiving nodes must verify author, group id, policy validity, and monotonic
   sequence before applying a policy update.
5. Accepted updates must persist in the existing policy store and apply to a
   running gossiper through the existing runtime policy path.
6. Invalid, stale, or non-founder updates must not alter local policy.
7. `group policy set` must publish through the daemon by default when the group
   is active. Add `--local-only` for intentional local-only changes.
8. Add tests for founder update, non-founder rejection, stale rejection, invalid
   policy rejection, and restart loading of latest stored policy.

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
4. Store display metadata through the existing ESP metadata format/helper where
   appropriate. Do not create a second incompatible metadata format.
5. If `join_mode=open_invite`, create an open invite only when the local runtime
   can issue one safely. If not available, return a clear next-step error and
   leave group and policy state consistent.
6. Output must include group id, founder identity, visibility, join mode,
   effective policy summary, open-invite descriptor/link when created, and the
   next command to publish a public descriptor when `visibility=public`.
7. Add tests for default policy storage, `-policy none`, custom policy file,
   metadata normalization, public metadata, and open-invite success/failure
   consistency.

Task 4: Public moot descriptor package and publish CLI
1. Add a general signed public moot descriptor package by reusing/refactoring
   existing default-moot canonical signing and verification helpers.
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
   member mirror path exists and is verified by the ESP.
6. Add:
   - `entmootd group public descriptor -group <GROUP_ID> [--json]`;
   - `entmootd group public publish -group <GROUP_ID> -esp-url <URL> [--json]`.
7. Descriptor generation must fail unless the local identity is the founder and
   local metadata says `visibility=public`.
8. Add tests for canonical signing, tamper detection, wrong founder key,
   private/unlisted rejection, embedded open invite preservation, and invalid
   indexing flags.

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
8. Public list output must include descriptor fields safe for public display,
   policy summary, `mirror_state=none|member|hosted`, and whether message
   history is available from this ESP.
9. Existing `/v1/groups` behavior must remain member/local-group listing and
   must not become the public directory endpoint.
10. Add tests for valid descriptor listing, stale descriptor ignoring, invalid
    signature rejection, blocked group/founder rejection, directory-only listing
    without local membership, and unchanged authenticated group APIs.

Task 6: entmoot-web public directory integration
1. In `/Users/jerryfane/Desktop/repo/entmoot-web`, update the Nest server proxy
   to read `GET /v1/public-moots` from the ESP directory first.
2. Keep existing hosted/mirrored group member/topic/message proxy paths.
3. Extend shared DTOs with visibility, join mode, policy summary, mirror state,
   open invite link, and message-history availability.
4. Update `/explore` to show directory-only public moots and mirrored moots in
   one list.
5. Update group detail so directory-only groups show metadata, policy, join
   mode, and open-invite action without rendering missing history as an error.
6. Show message feed only when `messageHistoryAvailable=true`.
7. Add Schema.org `DataCatalog` / `Dataset` JSON-LD for listed public moots.
8. Preserve the current explorer ergonomics. Do not build a marketing-only page;
   keep `/explore` useful as the first screen for public discovery.
9. Add tests/build checks for server mapping, frontend rendering of
   directory-only entries, no-history group detail, and existing mirrored group
   flow.

Task 7: Documentation, operator guidance, and examples
1. Update Entmoot docs:
   - README;
   - `docs/OPERATIONS.md`;
   - CLI/help docs;
   - website reference docs;
   - agent-facing docs/skills if present.
2. Document that public is separate from open invite, listed is separate from
   ESP membership, descriptor indexing is separate from message/history
   indexing, policy is local enforcement, founder updates apply only to
   cooperating nodes, the default ESP can delist/block Entmoot-operated
   surfaces, and live replies remain opt-in per node.
3. Add copy-paste examples with placeholder values only.
4. Keep docs aligned with real `entmootd --help`, ESP API behavior, and
   entmoot-web behavior.

Task 8: Release, deploy, and live validation
1. If any merged PR changes runtime behavior, helper behavior, installer
   behavior, CLI behavior, release workflow, open-invite behavior, live-agent
   behavior, policy behavior, descriptor behavior, ESP API behavior, website
   behavior, or deployed peer operation, cut a new Entmoot release.
2. Before tagging, run the release checklist in `docs/OPERATIONS.md`.
3. Verify the GitHub release workflow succeeds and expected assets exist.
4. Deploy entmoot-web only after the ESP/API release it depends on is live.
5. Validate using real running agents when runtime behavior changes:
   - use SSH multiplexing with credentials from
     `/Users/jerryfane/Desktop/repo/entmoot/temp/vps-ssh.txt`;
   - never print, paste, commit, or summarize credentials;
   - check VPS, Deimos, and Hermes when relevant;
   - discover actual namespaces, containers, binary paths, data roots, Pilot
     sockets, control sockets, process trees, and versions from live state;
   - do not run a second Entmoot install beside the existing one.
6. Real-peer validation must prove current version, healthy `entmootd env
   --json`, direct publish/query smoke, policy propagation, public descriptor
   publish appearing in the default ESP directory, default ESP not joining
   directory-only moots, entmoot.xyz rendering the directory entry, existing The
   Ent Moot default-moot descriptor/status still working, live agents remaining
   opt-in, and logs free of new invalid JSON, empty output, unreachable sockets,
   missing child processes, restart loops, or OOM evidence.
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
   task unless the plan specifies a different message.
4. Push the task branch.
5. Create one PR for the current task.
6. PR title must describe only the current task.
7. PR body must include:
   - WHY: why the task was needed and what evidence supports the design;
   - WHAT: behavioral changes;
   - CHANGES: concrete implementation changes;
   - RESULTS: tests, checks, operational smoke, and review results;
   - RISK: skipped checks, blockers, or residual risk;
   - NEXT STEPS: release, deploy, monitoring, or remaining tasks.
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
- Include real-agent validation evidence from VPS, Deimos, and Hermes when peer
  validation was required:
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
