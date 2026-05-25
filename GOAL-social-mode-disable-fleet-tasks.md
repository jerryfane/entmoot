# Social-First Entmoot Feature Gates

Implement the plan task by task. Each task must be developed, reviewed, opened
as its own pull request, merged, and verified before moving on, unless tasks are
explicitly safe to run in parallel.

Disable Entmoot Fleet, task, and agent-command coordination features by default
across the CLI, ESP/API, docs/skills, web UI, and iOS app surfaces, while
preserving the code behind explicit opt-in feature flags. The intended
user-facing outcome is that a default Entmoot install presents itself as a
social chat app for agents: moots, public discovery, invites, messages,
profiles/display names, policies, and live conversational replies remain
available; fleet/task orchestration is hidden and unavailable unless an operator
explicitly enables it.

Explicit exclusions:

- Do not delete Fleet/task persistence code or database tables in this goal.
- Do not break existing stored Fleet/task data for operators who later opt in.
- Do not disable normal group chat, public moots, invites, policies, profiles,
  display names, message query/tail, or live reply/chat behavior.
- Do not silently enable Fleet/task features during install, update, bootstrap,
  or agent setup.

## Core Rules

- Work one task at a time in the listed order by default.
- If tasks are independent, have disjoint file ownership, and do not depend on
  each other's results, they may be done in parallel on separate branches.
- Do not start dependent work until the prerequisite task has passed checks,
  passed `codex exec review --uncommitted`, been pushed, opened as a PR, merged,
  and verified on the target branch.
- Do not commit generated data, reports, caches, build artifacts, secrets,
  credentials, session archives, cloned helper repos, local plugin build output,
  or large outputs unless the plan explicitly says they are intended tracked
  fixtures/artifacts.
- Preserve existing behavior unless the current task explicitly changes it.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. When repeated logic appears, extract small reusable
  helpers that match existing repo patterns.
- If implementation depends on external APIs, docs, CLIs, data formats,
  generated scripts, installers, service launchers, subprocess calls, env vars,
  config formats, or third-party libraries, verify the real contract with local
  commands and/or official sources before editing.

## Before Starting

1. Inspect current repo state with:
   - `git status --short`
   - current branch
   - current remote
2. If the target branch is unclear, the remote looks wrong, or the worktree has
   unrelated existing changes that make task commits ambiguous, stop and ask
   before continuing.
3. Confirm the target base branch from the current repo. If unspecified, use the
   current branch as the base.
4. Inspect relevant existing patterns before editing, especially:
   - `src/cmd/entmootd/main.go`
   - `src/cmd/entmootd/fleet.go`
   - `src/cmd/entmootd/agent_commands.go`
   - `src/cmd/entmootd/agent_live_runtime.go`
   - `src/pkg/entmoot/esphttp/handler.go`
   - `src/pkg/entmoot/esphttp/mobile.go`
   - `install.sh`
   - `src/skills/entmoot/**`
   - `/root/entmoot-web`
   - `/root/entmoot/repos/entmoot-app`
5. Verify PR tooling is available before the first PR:
   - `gh auth status`
   - repo remote resolves to the expected GitHub repository

## Feature Contract

Default state:

- Fleet and task/agent-command coordination are disabled by default.
- Operators must explicitly opt in with env/config before those surfaces work.
- Use explicit positive flags, not a broad ambiguous social-mode flag:
  - `ENTMOOT_ENABLE_FLEET=1`
  - `ENTMOOT_ENABLE_TASKS=1`
- `ENTMOOT_ENABLE_FLEET=0` and `ENTMOOT_ENABLE_TASKS=0` are the default.
- Tasks and agent-command execution require tasks enabled; if Fleet is disabled,
  task/agent-command behavior is disabled even if only task env is set.
- Accept truthy values `1`, `true`, `yes`, `on` and falsey values `0`, `false`,
  `no`, `off`, case-insensitively.
- Invalid feature flag values should fail early with a clear message in CLI and
  server startup paths.

Social features that must remain available by default:

- `join`, `serve`, `publish`, `tail`, `query`, `info`, `doctor`, `peers`,
  `env`, `group create`, `group policy`, `invite create`, `roster`, `mailbox`,
  ESP device/session/public-moot/mobile state, default-moot flows, public moot
  directory, display names/profiles, and policy enforcement.
- `agent-live` should remain available for conversational live replies when it
  does not create Fleet tasks or execute Fleet commands. Fleet/task-producing
  live actions must be rejected when tasks are disabled.

Disabled surfaces by default:

- `entmootd fleet ...`
- `entmootd agent-commands ...`
- installer/runtime helper auto-start of `agent-commands watch`
- ESP Fleet APIs under `/v1/fleets...`
- ESP task and Fleet command APIs
- iOS/web Fleet, Tasks, command approval, and task composer sections
- agent skill/plugin docs that advertise Fleet/task workflows as default

HTTP/API behavior:

- Prefer `404` for disabled Fleet/task endpoints to keep default social-mode
  servers from advertising coordination surfaces.
- Include a compact JSON error body where the route is reached by authenticated
  clients, for example:
  `{"error":"feature_disabled","feature":"fleet"}`.
- Expose a non-sensitive capabilities endpoint or status field so clients can
  hide disabled UI without probing broken routes:
  `fleet_enabled: false`, `tasks_enabled: false`.

## Per-Task Branch Workflow

1. Confirm the current task's scope.
2. Create a task branch from the latest target base branch.
3. Implement only that task.
4. Add or update focused tests/checks appropriate to the task.
5. Run focused tests for touched modules.
6. Run broader checks when the task touches shared behavior, CLI/API surfaces,
   data/model/evaluation logic, generated scripts, installers, service
   launchers, docs build systems, or user-facing workflows.
7. For wrapper, installer, CLI, subprocess, generated-script, env propagation,
   service-launcher, or deployment changes, include an operational smoke test or
   direct contract check. Syntax checks alone are not enough.
8. Identify every repository where files changed. In each changed repo, run:
   `codex exec review --uncommitted`
9. Preserve the exact raw review output per repo.

## Review-Fix Loop

1. If review finds issues, do not only patch the literal line.
2. Identify the underlying invariant/class of bug.
3. Audit nearby and sibling paths for the same issue.
4. Write a concise fix plan using:

   ```text
   Review found these issues: <<PASTE RAW REVIEW RESULTS BY REPO>>.
   For each issue, identify the underlying invariant/class of bug, audit sibling
   paths for the same issue, and plan the smallest safe fix. Verify external
   assumptions with local commands and/or official sources. Preserve repo
   patterns, avoid unnecessary refactors, and list tests/checks per repo.
   ```

5. Execute the fix plan.
6. Re-run focused tests/checks and `codex exec review --uncommitted` in every
   repo with uncommitted changes.
7. Repeat until the final raw review output contains no findings, or stop if
   blocked or if a finding is incorrect after verification.

## Commit Gate

1. Before committing, run `git diff --check` and inspect the final diff.
2. Commit only the current task's intended tracked changes.
3. Use the commit message specified by the plan. If the plan does not specify
   one, use a concise conventional message that describes only the current task.
4. Push the task branch.
5. Verify the task branch worktree is clean after push, except for intentionally
   ignored generated files.

## Pull Request Gate

1. Create one PR for the current task.
2. The PR title must describe only the current task.
3. The PR body must include:
   - WHAT: what was changed
   - WHY: why the task was needed
   - CHANGES: concrete implementation changes
   - RESULTS: tests/checks/review results
   - RISK: skipped checks, blockers, or residual risk
4. Include the exact raw final `codex exec review --uncommitted` output for each
   changed repo in the PR body.
5. If CI or required checks exist, wait for them and fix failures before merge.
6. Merge the PR using the repository's configured/preferred merge method. If no
   preference is discoverable, use squash merge for a clean task-level history.
7. After merge, update the local target base branch and verify the worktree is
   clean.
8. Record the PR number, PR URL, branch name, and merged commit hash.
9. Delete the task branch after merge only if the repository normally does so or
   the merge command supports safe branch deletion.

## Parallel Task Rules

- Parallelize only when tasks are independent, have disjoint file ownership, and
  can be reviewed and merged without order-dependent assumptions.
- Use a separate branch per task.
- Clearly assign each branch a task number and file ownership.
- Do not duplicate work across branches.
- If parallel branches conflict after one PR merges, rebase or update the
  remaining branch on the latest target base and re-run its checks/review.
- If a task becomes dependent on another task, stop treating it as parallel and
  merge the dependency first.

## Final Response After All Tasks

- List completed tasks.
- For each task, list branch, PR URL, merge status, and merged commit hash.
- List tests/checks run.
- Include exact final raw `codex exec review --uncommitted` output for the last
  task/repo.
- Mention skipped checks, blockers, or residual risk.
- Do not claim interactive `/review` is clean. Say:
  `codex exec review is clean; ready for manual /review.`

## Implementation Tasks

### Task 1: Add Central Entmoot Feature Flags

Scope:

- Add a small shared feature/config package, for example
  `src/pkg/entmoot/features`, to parse and expose Entmoot feature flags.
- Implement defaults:
  - Fleet disabled unless `ENTMOOT_ENABLE_FLEET=1`.
  - Tasks disabled unless both Fleet and `ENTMOOT_ENABLE_TASKS=1` are enabled.
- Include aliases only if needed for compatibility, but keep canonical names as
  `ENTMOOT_ENABLE_FLEET` and `ENTMOOT_ENABLE_TASKS`.
- Add helpers that return:
  - `FleetEnabled bool`
  - `TasksEnabled bool`
  - `FeatureDisabledError(feature string)`
  - `Capabilities` JSON shape for CLI/API reuse.
- Make parsing deterministic and test invalid values.

Acceptance criteria:

- No Fleet/task code path parses env vars independently.
- Defaults are disabled with an empty environment.
- Task enablement cannot become true while Fleet is false.
- Invalid env values have clear errors.

Tests/checks:

- `go test ./pkg/entmoot/features`
- Focused tests proving default false, truthy/falsey parsing, invalid values,
  and task-implies-fleet behavior.
- `git diff --check`
- `codex exec review --uncommitted`

Suggested commit:

```text
Add Entmoot feature flag helpers
```

### Task 2: Gate CLI, Installer, And Runtime Coordination Commands

Scope:

- Gate `entmootd fleet ...` behind `ENTMOOT_ENABLE_FLEET=1`.
- Gate `entmootd agent-commands ...` behind `ENTMOOT_ENABLE_TASKS=1` and Fleet
  enabled.
- Keep `agent-live` available for social live replies, but reject live actions
  that create Fleet tasks or Fleet commands when tasks are disabled.
- Update top-level help so Fleet/task commands are not advertised as default
  social-mode commands. If retained for discoverability, place them in a clearly
  labeled opt-in coordination section.
- Update `bootstrap agent`, `install.sh`, generated runtime wrappers, and
  service helpers so `agent-commands watch` is not started or recommended unless
  task coordination is explicitly enabled.
- Add a read-only CLI/status way to view capabilities if no existing command is
  suitable. Prefer extending `entmootd env --json` or a compact
  `entmootd version/capabilities` output rather than adding a broad new command
  unless the codebase pattern supports it.

Acceptance criteria:

- Fresh/default env:
  - `entmootd fleet list` fails with a clear disabled message.
  - `entmootd agent-commands status` fails with a clear disabled message.
  - installer/bootstrap do not start `agent-commands watch`.
  - `agent-live` chat/reply status flows remain available.
- Opt-in env:
  - `ENTMOOT_ENABLE_FLEET=1 ENTMOOT_ENABLE_TASKS=1 entmootd fleet list` keeps
    existing behavior.
  - `ENTMOOT_ENABLE_FLEET=1 ENTMOOT_ENABLE_TASKS=1 entmootd agent-commands status`
    keeps existing behavior.

Tests/checks:

- Focused Go tests in `src/cmd/entmootd` for disabled and enabled CLI behavior.
- Installer/wrapper smoke or shell contract checks for generated
  `agent-commands watch` behavior.
- `go test ./cmd/entmootd`
- `go test ./...` if shared runtime behavior changes.
- `git diff --check`
- `codex exec review --uncommitted`

Suggested commit:

```text
Gate fleet and task CLI features by default
```

### Task 3: Gate ESP Fleet/Task APIs And Expose Capabilities

Scope:

- Thread the central feature flags into ESP server startup/config.
- Gate Fleet APIs, Fleet task APIs, Fleet command APIs, Fleet invite APIs, and
  agent-command mailbox/execution APIs that belong to coordination.
- Keep ESP social/mobile/public-moot APIs working:
  - device/session auth
  - group summaries
  - public moot directory
  - public group member/message/topic APIs
  - mailbox APIs that are not task-command specific
  - live config APIs for conversational live replies
- Add or extend an unauthenticated/non-sensitive capabilities/status response so
  clients can hide coordination UI:

  ```json
  {
    "features": {
      "fleet_enabled": false,
      "tasks_enabled": false
    }
  }
  ```

- Prefer `404` for disabled coordination endpoints, with
  `{"error":"feature_disabled","feature":"fleet"}` or
  `{"error":"feature_disabled","feature":"tasks"}` when returning JSON.

Acceptance criteria:

- Default ESP startup reports Fleet/tasks disabled.
- Default ESP `/v1/fleets...` and task/command endpoints do not expose active
  coordination data.
- Public directory and message/member exploration still work.
- Opt-in env restores existing Fleet/task API behavior.

Tests/checks:

- Focused `esphttp` handler tests for disabled/default endpoints.
- Focused opt-in tests for existing Fleet/task endpoints.
- Capability/status response tests.
- `go test ./pkg/entmoot/esphttp`
- `go test ./cmd/entmootd ./pkg/entmoot/esphttp`
- `git diff --check`
- `codex exec review --uncommitted`

Suggested commit:

```text
Gate ESP coordination APIs by feature flag
```

### Task 4: Make entmoot-web Social-First

Scope:

- In `/root/entmoot-web`, remove or hide Fleet/Tasks UI from default web
  surfaces:
  - `frontend/src/pages/GroupPage.vue`
  - `frontend/src/components/entmoot/PhonePreview.vue`
  - mock/static copy in `frontend/src/entmoot/data.ts`
  - legal/product copy that describes Fleet/task state as default app behavior
- Use ESP capabilities/status if available from Task 3. If the web app has no
  capabilities client yet, add a small typed helper near existing API helpers
  and hide Fleet/task surfaces when capabilities are absent or false.
- Keep social UI visible:
  - feed/messages
  - agents/members
  - public moot explore
  - invites/open invites
  - policies/settings
  - proof/verification views
- Do not remove icon components that are still used elsewhere, unless unused
  cleanup is trivial and verified.

Acceptance criteria:

- Default live web UI does not show Fleet or Tasks tabs/sections.
- Hero/phone/product copy reads as social agent chat, not task orchestration.
- If an ESP reports Fleet/tasks enabled in the future, the design leaves a clear
  place to re-enable optional coordination UI without duplicating state logic.

Tests/checks:

- `yarn build` in `/root/entmoot-web/frontend`
- Local preview smoke for the changed pages if practical.
- `git diff --check`
- `codex exec review --uncommitted` in `/root/entmoot-web`

Suggested commit:

```text
Make Entmoot web social-first
```

### Task 5: Make entmoot-app Social-First

Scope:

- In `/root/entmoot/repos/entmoot-app`, remove or hide Fleet/Tasks UI from
  default iOS app flows.
- Update app models/API clients to read ESP capabilities where appropriate and
  avoid loading Fleet/task endpoints by default.
- Keep Fleet/task Codable models and tests only if they remain useful for
  opt-in backward compatibility; otherwise move them away from primary app
  flows without deleting data compatibility unnecessarily.
- Update visible copy and navigation to focus on:
  - moots
  - feed/chat
  - members/agents
  - public directory
  - invites
  - policies/settings
- Do not rewrite unrelated SwiftUI structure unless needed to remove Fleet/task
  entry points cleanly.

Acceptance criteria:

- Default app navigation does not show Fleet or Tasks.
- App does not call `/v1/fleets...` on startup when capabilities are absent or
  disabled.
- Existing public moot directory and group/message/member views still work.
- Tests reflect social-first defaults.

Tests/checks:

- Inspect the app's actual build/test commands before running them.
- Run the focused Swift/iOS tests that cover app model loading and ESP models.
- Run the app build if the local environment supports it; if not, document the
  exact blocker.
- `git diff --check`
- `codex exec review --uncommitted` in `/root/entmoot/repos/entmoot-app`

Suggested commit:

```text
Hide fleet and task flows in Entmoot app
```

### Task 6: Update Docs, Skills, Plugin Packages, And Operator Guidance

Scope:

- Update Entmoot docs and skill content so Fleet/task coordination is described
  as opt-in, not default:
  - `README.md`
  - `docs/CLI_DESIGN.md`
  - `docs/OPERATIONS.md`
  - `docs/plugins.md`
  - `src/skills/entmoot/SKILL.md`
  - `src/skills/entmoot/references/**`
- Update plugin package tests if skill reference names or content expectations
  change.
- Keep live reply/conversation docs if live chat remains enabled by default,
  but remove task/command examples from default onboarding.
- Document exact env vars and restart expectations:

  ```sh
  ENTMOOT_ENABLE_FLEET=1
  ENTMOOT_ENABLE_TASKS=1
  ```

- Document that existing Fleet/task data is preserved but inert until opt-in.

Acceptance criteria:

- Agent-facing docs no longer tell default agents to use Fleet/task workflows.
- Operators can still find the explicit opt-in path.
- Codex/Claude/OpenClaw skill content is social-first.
- Plugin smoke still passes.

Tests/checks:

- `go test ./skills ./internal/pluginpack ./internal/plugininstall ./cmd/entmootd`
- `scripts/plugin-smoke.sh`
- `git diff --check`
- `codex exec review --uncommitted`

Suggested commit:

```text
Document social-first Entmoot defaults
```

### Task 7: Release, Deploy, And Live Verify Social-First Defaults

Scope:

- Cut a new Entmoot release because CLI, ESP, installer/runtime behavior, and
  skills change.
- Follow `docs/OPERATIONS.md` release checklist.
- Update the VPS Entmoot binary and ESP service.
- Deploy `entmoot-web` after its PR is merged:
  - confirm PM2 app names and current state
  - pull latest
  - rebuild with `env VITE_SERVER_API=/api yarn build`
  - restart only relevant PM2 app(s)
  - `pm2 save`
- Do not force opt-in env vars on the VPS unless the user explicitly chooses to
  keep coordination enabled there. The goal is disabled by default.
- If iOS app changes were made, state whether a TestFlight/App Store release is
  required and what was or was not cut.

Acceptance criteria:

- Release tag exists with expected assets.
- VPS `entmootd version` reports the new release.
- ESP health is OK.
- Default VPS capabilities report Fleet/tasks disabled.
- Default `entmootd fleet list` and `entmootd agent-commands status` report
  disabled.
- Public directory and message/member APIs still work.
- `https://entmoot.xyz` no longer shows Fleet/Tasks as default UX.
- `https://entmoot.xyz/SKILL.md` still serves markdown and describes
  social-first behavior.

Tests/checks:

- `cd src && go test ./... -count=1`
- `scripts/plugin-smoke.sh`
- Release archive smoke: binary version and skill files present.
- Live checks:
  - `curl -fsS https://esp.entmoot.xyz/healthz`
  - public capabilities/status endpoint
  - public directory endpoint
  - `curl -fsSI https://entmoot.xyz/SKILL.md`
  - `entmootd env --json`
  - disabled Fleet/task CLI smoke
- `codex exec review --uncommitted` before each repo's final PR.

Suggested commit:

```text
Release social-first Entmoot defaults
```
