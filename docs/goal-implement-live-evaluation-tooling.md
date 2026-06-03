# Goal Prompt: Implement Live Evaluation Tooling For The Entmoot Paper

Implement the plan task by task. Each task must be developed, reviewed, opened
as its own pull request, merged, and verified before moving on, unless tasks are
explicitly safe to run in parallel.

Build the reproducible tooling needed to run the live multi-agent evaluation
tracked in GitHub issue #72:

https://github.com/jerryfane/entmoot/issues/72

The intended outcome is a clean repo-level runbook and small Bash tooling suite
that can collect baseline health, join nodes to the controlled study moot,
publish deterministic smoke messages, verify lexical search and message-context
retrieval, export transcripts, and package sanitized evidence for the research
paper. This goal must not run the final live multi-agent study, enable live
agent replies, add Fleet/task behavior, or add temporary production
`entmootd eval ...` subcommands.

Controlled study moot:

- Name: `Entmoot Evaluation Study`
- Group ID: `pEX049VvIhcNFiBqEcIqr8FPn+54KNkM7uA4ZhxYpfI=`
- Visibility: `private`
- Join mode: `invite_only`
- Policy: standard preset
- Existing local invite path on the current evaluation host:
  `/tmp/entmoot-evaluation-study/invite.json`

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
- Keep evaluation tooling as repo scripts and documentation. Do not add core
  daemon subcommands unless a later product goal explicitly requests them.
- Do not commit raw evaluation artifacts by default. Raw outputs may contain
  hostnames, node IDs, logs, private messages, or other sensitive metadata.
- Fleet and task coordination must stay disabled and out of scope. This study
  evaluates social-chat behavior.

## Before Starting

1. Inspect current repo state with:
   - `pwd`
   - `git status --short --branch`
   - `git remote -v`
   - `git log --oneline -8`
2. Confirm the target base branch from the current repo. If unspecified, use
   `main`.
3. Verify PR tooling is available before the first PR:
   - `gh auth status`
   - repo remote resolves to `jerryfane/entmoot`
4. Read these files before editing:
   - `docs/goal-pr-task-workflow.md`
   - `docs/goal-rewrite-entmoot-research-paper.md`
   - `paper/main.tex`, especially the Evaluation Plan section
   - `docs/OPERATIONS.md`
   - `README.md`
   - `scripts/verify-agent-runtime.sh`
   - `scripts/verify-mesh-node.sh`
   - `scripts/verify-esp-service.sh`
   - `website/docs/reference/http-esp-api.md`
5. Review GitHub issue #72 for the latest evaluation scope.
6. Check current command contracts before coding scripts:
   - `entmootd`
   - `entmootd env --json`
   - `entmootd info`
   - `entmootd doctor -group <gid> --probe --json`
   - `entmootd peers -group <gid> --probe --json`
   - `entmootd join <invite>`
   - `entmootd publish -group <gid> -topic <topic> -file -`
   - `entmootd query -group <gid> ...`
7. Search for the current search/message-context CLI or HTTP surfaces before
   implementing `search-context-check.sh`. Prefer existing CLI surfaces if
   present; otherwise use the documented ESP HTTP endpoint only when all
   required URL/client/auth inputs are provided by flags or environment.

## Planned File Layout

Tracked files to add:

```text
docs/evaluation-live-agent-study-runbook.md

scripts/eval/
  lib.sh
  baseline.sh
  join-controlled-moot.sh
  publish-smoke.sh
  search-context-check.sh
  export-transcript.sh
  package-artifacts.sh

artifacts/evaluation/
  .gitignore
  README.md
```

Ignored runtime output:

```text
artifacts/evaluation/<timestamp>/
```

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
5. Verify the task branch worktree is clean after push, except for
   intentionally ignored generated files.

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

## Implementation Tasks

### Task 1: Add Evaluation Runbook And Artifact Policy

Branch: `task/eval-runbook-artifact-policy`

Scope:

- Add `docs/evaluation-live-agent-study-runbook.md`.
- Add `artifacts/evaluation/README.md` and
  `artifacts/evaluation/.gitignore`.
- Do not add executable scripts yet.

Details:

- Document the study objective, moots, participants, consent rules, and the
  controlled moot group ID.
- Make explicit that joining is separate from live replies, and live replies
  are separate from Fleet/task coordination.
- Define the artifact directory shape:
  `artifacts/evaluation/<run-id>/...`
- Define the sanitization policy: raw transcripts/logs are ignored by default
  and must be reviewed before publication.
- Document the exact planned command sequence:
  baseline, join, baseline again, smoke publish, search/context check, pilot
  conversation, main study, optional restart/anti-entropy check, transcript
  export, artifact packaging, paper update.
- Include a small operator checklist for Hermes and Deimos/OpenClaw container
  runtimes, but do not include SSH credentials, host IPs, passwords, tokens, or
  private runtime paths beyond generic examples.

Checks:

- `git diff --check`
- Markdown readability inspection
- `codex exec review --uncommitted`

Acceptance:

- A new operator can understand the evaluation plan and artifact policy without
  reading issue #72.
- Raw artifact paths are ignored by default, while README and `.gitignore` are
  tracked.

Suggested commit message:

`docs: add live evaluation runbook`

### Task 2: Add Shared Evaluation Script Library And Baseline Capture

Branch: `task/eval-baseline-scripts`

Scope:

- Add `scripts/eval/lib.sh`.
- Add `scripts/eval/baseline.sh`.
- Keep scripts POSIX-friendly Bash matching existing script style.

Details:

- `lib.sh` should centralize repeated logic:
  - strict shell setup expectations;
  - usage helpers;
  - timestamp/run-id generation;
  - output directory creation;
  - command existence checks;
  - Entmoot binary/wrapper resolution via `ENTMOOTD`, `/data/.entmoot/entmoot`,
    or `entmootd`;
  - optional `PILOT_SOCKET`, `ENTMOOT_DATA`, and `ENTMOOT_IDENTITY` flag
    assembly;
  - command runner that writes stdout, stderr, exit status, and a small metadata
    file per command.
- `baseline.sh` should accept at least:
  - `--group <gid>`
  - `--node-label <label>`
  - `--out-dir <dir>`
  - `--probe`
  - `--timeout <duration>`
  - `-h|--help`
- `baseline.sh` should run:
  - `entmootd env --json`
  - `entmootd info`
  - `entmootd doctor -group <gid> --json`, with `--probe --timeout` when
    requested
  - `entmootd peers -group <gid> --json`, with `--probe --timeout` when
    requested
- It should save outputs under:
  `artifacts/evaluation/<run-id>/<node-label>/baseline/` by default, unless
  `--out-dir` is supplied.
- Avoid parsing secrets or printing credentials.

Checks:

- `bash -n scripts/eval/lib.sh scripts/eval/baseline.sh`
- `scripts/eval/baseline.sh --help`
- Local smoke with the controlled group if local Entmoot is healthy:
  `scripts/eval/baseline.sh --group pEX049VvIhcNFiBqEcIqr8FPn+54KNkM7uA4ZhxYpfI= --node-label local --probe`
- Verify generated output remains ignored:
  `git status --ignored --short artifacts/evaluation`
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Baseline capture creates a structured ignored run directory.
- Command failures are captured clearly without losing stdout/stderr.
- Shared helper code avoids duplicated command/output handling in later scripts.

Suggested commit message:

`feat: add evaluation baseline capture scripts`

### Task 3: Add Join And Publish Smoke Scripts

Branch: `task/eval-join-publish-scripts`

Scope:

- Add `scripts/eval/join-controlled-moot.sh`.
- Add `scripts/eval/publish-smoke.sh`.
- Reuse `scripts/eval/lib.sh`; extend it only when needed for shared helpers.

Details:

`join-controlled-moot.sh`:

- Accept:
  - `--invite <path>`
  - `--group <gid>` optional but recommended
  - `--node-label <label>`
  - `--out-dir <dir>`
  - `-h|--help`
- Verify the invite file exists and is non-empty.
- Run the local Entmoot join command through the resolved wrapper/binary.
- Record stdout, stderr, exit status, and command metadata.
- Run post-join `env`, `info`, and `doctor -group <gid> --json`.
- Fail clearly when the invite, Pilot socket, Entmoot binary, or control socket
  is unavailable.

`publish-smoke.sh`:

- Accept:
  - `--group <gid>`
  - `--node-label <label>`
  - `--topic <topic>` defaulting to `eval/smoke`
  - `--anchor <anchor>` optional
  - `--out-dir <dir>`
  - `-h|--help`
- Generate a deterministic anchor if none is provided:
  `EVAL-CONTEXT-ANCHOR-<timestamp>-<node-label>`.
- Publish at least three messages:
  - pre-anchor neighbor;
  - anchor message;
  - post-anchor neighbor.
- Use `entmootd publish ... -file -` so shell quoting does not corrupt content.
- Record send timestamps, topics, content hashes or anchors, stdout/stderr, and
  exit status.

Checks:

- `bash -n scripts/eval/lib.sh scripts/eval/join-controlled-moot.sh scripts/eval/publish-smoke.sh`
- `scripts/eval/join-controlled-moot.sh --help`
- `scripts/eval/publish-smoke.sh --help`
- Local smoke publish against the controlled moot if safe.
- Verify generated output remains ignored.
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Joins and smoke publishes are repeatable from local, Hermes, or
  Deimos/OpenClaw container runtimes.
- Smoke messages provide known anchors for search/context checks.

Suggested commit message:

`feat: add evaluation join and smoke publish scripts`

### Task 4: Add Search Context Check And Transcript Export

Branch: `task/eval-search-transcript-scripts`

Scope:

- Add `scripts/eval/search-context-check.sh`.
- Add `scripts/eval/export-transcript.sh`.
- Reuse and extend `scripts/eval/lib.sh` only for shared command/output helpers.

Details:

`search-context-check.sh`:

- Accept:
  - `--group <gid>`
  - `--anchor <anchor>`
  - `--node-label <label>`
  - `--topic <topic>` optional
  - `--out-dir <dir>`
  - optional ESP HTTP inputs if required by the available message-context
    contract, such as `--esp-url`, `--client-id`, and auth-related env vars.
- Before implementation, inspect current CLI/API support for lexical search and
  message-context retrieval.
- Prefer existing CLI commands when they expose the needed behavior.
- If the message-context check must use ESP HTTP, make the dependency explicit
  and fail with a clear message when required flags/env vars are absent.
- Save raw search results, raw context response, timing metadata, and a small
  pass/fail summary.
- Verify:
  - search returns the anchor message;
  - context includes the target message;
  - neighboring smoke messages are present when expected;
  - mailbox/read cursor is not mutated when this can be checked from available
    APIs.

`export-transcript.sh`:

- Accept:
  - `--group <gid>`
  - `--topic <topic>` repeatable or comma-separated
  - `--node-label <label>`
  - `--limit <n>`
  - `--out-dir <dir>`
  - `-h|--help`
- Run `entmootd query` for selected topics.
- Save raw JSONL exactly as returned.
- Produce a readable Markdown transcript with timestamps, author/node labels
  where available, topics, message IDs, and content.
- Preserve message IDs for paper citations and context checks.

Checks:

- `bash -n scripts/eval/lib.sh scripts/eval/search-context-check.sh scripts/eval/export-transcript.sh`
- `scripts/eval/search-context-check.sh --help`
- `scripts/eval/export-transcript.sh --help`
- Local smoke after Task 3 generated an anchor, if available.
- Verify generated output remains ignored.
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- Search/context evidence can be collected without hand-copying commands.
- Transcript export produces both raw JSONL and human-readable Markdown.
- Missing optional ESP/client/auth inputs fail clearly rather than silently
  skipping checks.

Suggested commit message:

`feat: add evaluation search and transcript scripts`

### Task 5: Add Artifact Packaging And End-To-End Dry Run Documentation

Branch: `task/eval-package-artifacts`

Scope:

- Add `scripts/eval/package-artifacts.sh`.
- Update `docs/evaluation-live-agent-study-runbook.md` with the exact final
  script sequence and expected outputs.
- Do not run the main live study.

Details:

`package-artifacts.sh`:

- Accept:
  - `--run-dir <dir>`
  - `--output <path>` optional
  - `--include-screenshots` optional
  - `-h|--help`
- Validate that the run directory exists and is under
  `artifacts/evaluation/` unless the caller explicitly confirms another path.
- Collect baseline outputs, join outputs, smoke outputs, search/context outputs,
  transcript exports, logs/notes if present, and screenshots if explicitly
  included.
- Write checksums, including SHA-256 for every packaged file.
- Record:
  - repo commit;
  - Entmoot version output when available;
  - run timestamp;
  - group IDs;
  - node labels;
  - script versions or git commit.
- Create a `.tar.gz` bundle or deterministic directory package.
- Avoid bundling files with obvious secret names such as `*password*`,
  `*token*`, `*secret*`, SSH keys, or raw credential files.

Runbook update:

- Document a dry-run command sequence using only local or explicitly provided
  safe inputs.
- Document how to copy the invite to remote containers without committing it.
- Document how to invoke scripts inside Hermes and Deimos/OpenClaw containers.
- Document what to inspect before enabling live replies.
- Document how to keep issue #72 open until the real Results section lands.

Checks:

- `bash -n scripts/eval/*.sh`
- `scripts/eval/package-artifacts.sh --help`
- End-to-end local dry run of baseline, smoke publish, transcript export, and
  packaging when safe.
- Verify generated output remains ignored.
- `git diff --check`
- `codex exec review --uncommitted`

Acceptance:

- A complete dry-run artifact bundle can be generated without committing raw
  outputs.
- The runbook is decision-complete for the later live study.
- Issue #72 remains open because this goal only adds tooling.

Suggested commit message:

`feat: package live evaluation artifacts`

## Final Completion Criteria

- All task PRs are merged.
- `docs/evaluation-live-agent-study-runbook.md` exists and matches issue #72.
- `scripts/eval/` contains reusable, shellcheck-style Bash scripts for
  baseline, join, smoke publish, search/context checks, transcript export, and
  artifact packaging.
- Raw evaluation outputs are ignored by default under `artifacts/evaluation/`.
- The tooling has passed syntax checks, help output checks, and at least a local
  operational dry run where safe.
- No secrets, credentials, raw transcripts, runtime logs, or generated bundles
  are committed.
- The goal does not claim the live evaluation has been run. It only prepares the
  reproducible tooling needed to run it.
- Final response lists:
  - completed tasks;
  - branch and PR for each task;
  - merged commit hashes;
  - checks run;
  - skipped checks or residual risks;
  - exact final `codex exec review --uncommitted` output for the final task.
