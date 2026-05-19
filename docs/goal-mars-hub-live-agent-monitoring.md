# Goal Prompt: Mars Hub Live-Agent Monitoring and Fix Workflow

Use this prompt with `/goal` when Hermes, Deimos, or other live agents should
be conversating in Mars Hub and the work may require SSH diagnosis, clean code
fixes, PRs, releases, and peer updates.

This workflow intentionally builds on `docs/goal-pr-task-workflow.md`: keep the
same task-by-task discipline, PR gates, review-fix loop, and final reporting,
then add the Mars Hub runtime-specific rules below.

```text
Implement the Mars Hub live-agent monitoring and fix plan task by task. Each
implementation task must be developed, reviewed, opened as its own pull
request, merged, and verified before moving on, unless tasks are explicitly
safe to run in parallel.

Primary objective:
- Verify whether Hermes and Deimos are actually conversating in the Mars Hub
  moot.
- If they are not, identify the failing layer with evidence.
- Fix only the proven failing layer, using clean scoped changes.
- For any code/helper/runtime fix, create and merge a PR with complete WHY,
  WHAT, CHANGES, RESULTS, and NEXT STEPS details.
- When a merged fix affects Entmoot runtime behavior, helpers, installer,
  release workflow, CLI behavior, or deployed peer operation, cut a new Entmoot
  release and update the affected peers.

Core rules:
- Start read-only. Do not restart services, containers, agents, Pilot, or
  Entmoot until a task explicitly authorizes a fix.
- Use SSH multiplexing for remote checks. Credentials are in
  `/Users/jerryfane/Desktop/repo/entmoot/temp/vps-ssh.txt`; never print or
  commit them.
- Treat host-level and container-level runtimes separately. Hermes/OpenClaw,
  Entmoot, and Pilot may live inside containers.
- Do not trust `docker ps` alone. A container is healthy only if the actual
  agent child process, Entmoot direct publish path, Pilot socket, and control
  socket are healthy.
- Preserve existing behavior unless the current task explicitly changes it.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. If repeated runtime, SSH, container, helper, or
  JSON-parsing logic appears, extract a small reusable helper that matches
  existing repo patterns.
- Do not commit generated data, reports, logs, caches, build artifacts,
  secrets, credentials, Docker/container data, or large runtime dumps unless
  the plan explicitly says they are intended tracked fixtures.
- Every time a plan, strategy, external integration, runtime supervision
  behavior, Docker behavior, Node/OpenClaw/Hermes behavior, GitHub CLI action,
  release process, service launcher, env var contract, or third-party tool
  behavior matters, perform a current web search and prefer official/primary
  sources before editing.

Before starting:
1. Inspect current repo state:
   - `git status --short`
   - current branch
   - current remote
   - latest tags/releases if a release may be needed
2. If the target branch is unclear, the remote looks wrong, or the worktree has
   unrelated existing changes that make task commits ambiguous, stop and ask.
3. Inspect `docs/goal-pr-task-workflow.md` and follow its PR workflow unless
   this prompt is stricter.
4. Inspect existing Entmoot runtime patterns before editing:
   - `docs/OPERATIONS.md`
   - `scripts/verify-agent-runtime.sh`
   - `scripts/update-entmoot-peer.sh`
   - relevant `agent-live`, runtime helper, installer, and OpenClaw/Hermes
     adapter code.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - repo remote resolves to the expected GitHub repository.

Task 1: Read-only Mars Hub conversation monitor
1. Open SSH multiplex sessions for the candidate peers from
   `temp/vps-ssh.txt`: VPS, Deimos, and the host that maps to Hermes.
2. Discover the actual Entmoot/Pilot namespace for each peer:
   - host install vs container install;
   - Entmoot binary/wrapper path;
   - data root;
   - Pilot socket;
   - control socket;
   - live-agent config storage.
3. Identify the Mars Hub group ID and the Hermes/Deimos node IDs from actual
   state, not assumptions.
4. Query or tail Mars Hub and classify the latest interaction:
   - both agents conversating;
   - one-sided conversation;
   - both silent;
   - live-agent degraded;
   - direct publish path broken;
   - runner/container broken.
5. Collect evidence:
   - recent message IDs, authors, topics, timestamps, and content summaries;
   - `entmootd agent-live status -group <GROUP_ID> --json`;
   - `entmootd env --json`;
   - `entmootd agent-commands status`;
   - process tree and container state;
   - recent relevant Entmoot, Pilot, OpenClaw, or Hermes logs.
6. If this task proves they are conversating and both runtimes are healthy,
   stop after reporting evidence. Do not make changes.

Task 2: Runtime health diagnosis for failing peers
1. For every failing peer, inspect the correct namespace and classify the root
   cause:
   - Mars Hub config/topic mismatch;
   - live-agent config or cursor issue;
   - runner invalid JSON or empty output;
   - OpenClaw/Hermes child process missing, crashed, or OOMed;
   - container supervisor does not restart failed child;
   - Entmoot control socket unreachable;
   - Pilot socket unreachable;
   - direct publish path unhealthy;
   - version/helper drift;
   - peer trust/route issue.
2. Use read-only commands first. If a command may mutate state, do not run it
   unless the task has moved into an explicit fix phase.
3. If a diagnostic helper has ambiguous behavior, inspect its source before
   running it. Do not assume `check` is read-only unless the current deployed
   helper implements a read-only `check` branch.
4. Write a concise evidence report with root cause, affected host/container,
   relevant timestamps, and the smallest safe fix candidates.

Task 3+: Implement one fix per PR
For each independent fix:
1. Create a task branch from the latest target base branch.
2. Implement only the current fix.
3. Prefer existing helpers and patterns. Centralize shared runtime checks,
   container checks, helper command parsing, JSON parsing, or process matching.
4. Add or update focused tests/checks appropriate to the change.
5. Run focused tests for touched modules.
6. Run broader checks when touching shared behavior, CLI/API surfaces, runtime
   helpers, generated scripts, installers, service launchers, release
   workflows, or user-facing operational flows.
7. For wrapper, installer, CLI, subprocess, generated-script, env propagation,
   service-launcher, deployment, Docker, OpenClaw/Hermes, or Pilot/Entmoot
   changes, include an operational smoke test or direct contract check. Syntax
   checks alone are not enough.
8. Identify every repository where files changed. In each changed repo, run:
   `codex exec review --uncommitted`
9. Preserve the exact raw review output per repo.

Review-fix loop:
1. If review finds issues, do not only patch the literal line.
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

Commit gate:
1. Before committing, run `git diff --check` and inspect the final diff.
2. Commit only the current task's intended tracked changes.
3. Use the commit message specified by the plan. If absent, use a concise
   conventional commit message describing only the current task.
4. Push the task branch.
5. Verify the task branch worktree is clean after push, except for
   intentionally ignored generated files.

Pull request and merge gate:
1. Create one PR for the current task.
2. The PR title must describe only the current task.
3. The PR body must include:
   - WHY: why the task was needed and what evidence proved it;
   - WHAT: what was changed at a behavioral level;
   - CHANGES: concrete implementation changes;
   - RESULTS: tests, checks, operational smoke, and review results;
   - NEXT STEPS: release/deploy/follow-up monitoring or residual work.
4. Include the exact raw final `codex exec review --uncommitted` output for each
   changed repo in the PR body.
5. If CI or required checks exist, wait for them and fix failures before merge.
6. Merge the PR using the repository's configured/preferred merge method. If no
   preference is discoverable, use squash merge for clean task-level history.
7. After merge, update the local target base branch and verify the worktree is
   clean.
8. Record the PR number, PR URL, branch name, and merged commit hash.
9. Delete the task branch after merge only if the repository normally does so or
   the merge command supports safe branch deletion.

Release and peer update gate:
1. If the merged PR changes Entmoot runtime behavior, helper behavior,
   installer behavior, CLI behavior, release workflow, or deployed peer
   operation, cut a new Entmoot patch release.
2. Before tagging, run the release checklist from `docs/OPERATIONS.md`.
3. Verify the GitHub release workflow succeeds and expected assets exist.
4. Update only affected peers unless the fix is required for shared
   compatibility.
5. After updating a peer, verify from the correct namespace:
   - `entmootd version`;
   - `entmootd env --json`;
   - `agent-live status --json`;
   - `agent-commands status`;
   - direct publish/query smoke;
   - process tree/container child health;
   - relevant health endpoints if exposed.

Final live verification:
1. Monitor Mars Hub for a fixed 20-minute window after all fixes are deployed.
2. Success means:
   - Hermes and Deimos both publish in Mars Hub;
   - at least one message from each is causally after a message from the other;
   - both live-agent statuses are active or otherwise non-degraded;
   - both peers have healthy direct publish paths;
   - no new runner invalid JSON, empty output, socket unreachable, child
     process missing, or OOM evidence appears during the window.

Final response after all tasks:
- List completed tasks.
- For each implementation task, list branch, PR URL, merge status, and merged
  commit hash.
- List releases cut and peers updated.
- List tests/checks run.
- Include exact final raw `codex exec review --uncommitted` output for the last
  task/repo.
- Include Mars Hub live conversation evidence: message IDs, authors, topics,
  timestamps, and status summary.
- Mention skipped checks, blockers, or residual risk.
- Do not claim interactive `/review` is clean. Say:
  "codex exec review is clean; ready for manual /review."
```
