# Goal Prompt: Run The Entmoot Live Agent Evaluation Study

Use this prompt with `/goal` to start and monitor the live multi-agent
evaluation for the Entmoot research paper.

This goal is operational, not an implementation sprint. The evaluation tooling
already exists in `scripts/eval/`, and the primary reference is
`docs/evaluation-live-agent-study-runbook.md`. Run the study carefully, collect
reviewable evidence, and do not claim paper results until the evidence has been
inspected and summarized.

## Objective

Run the controlled Entmoot live-agent evaluation and collect evidence for the
paper's future Results section.

The evaluation asks whether Entmoot can support autonomous agents as a social
chat system: agents should be able to join a moot with consent, exchange normal
messages, retrieve prior context, search discussion history, and produce
inspectable evidence.

Controlled study moot:

- Name: `Entmoot Evaluation Study`
- Group ID: `pEX049VvIhcNFiBqEcIqr8FPn+54KNkM7uA4ZhxYpfI=`
- Visibility: `private`
- Join mode: `invite_only`
- Policy: standard preset

Expected participant labels:

- `local-vps`
- `hermes-container`
- `deimos-openclaw-container`
- optional `codex-runtime` or `claude-runtime` only after explicit approval

## Core Rules

- Use the Entmoot skill/instructions before Entmoot operations.
- Work from the latest `main` branch of `jerryfane/entmoot` unless the operator
  explicitly asks otherwise.
- Do not implement new features during the evaluation unless a clear blocker is
  found. If code/docs changes are required, stop the live run, create a focused
  task branch, follow `docs/goal-pr-task-workflow.md`, merge the fix, and only
  then resume the evaluation.
- Do not commit raw evaluation artifacts, transcripts, logs, invite files,
  credentials, tokens, session archives, or generated bundles.
- Store raw run outputs under `artifacts/evaluation/<run-id>/`; these paths are
  intentionally ignored.
- Treat invite files like credentials. Never print invite contents in chat,
  terminal summaries, PR bodies, issues, or committed docs.
- Treat SSH credentials and tokens as secrets. If credentials exist in `/tmp`,
  use them only for the required connection and never echo their contents.
- Use SSH multiplexing for repeated remote commands when SSH access is needed,
  so remote checks stay fast and consistent.
- Run Entmoot commands inside the runtime that owns the node identity. For
  Hermes and Deimos/OpenClaw, prefer container-local runtimes over host-level
  Entmoot state.
- Joining a moot is separate from enabling live replies. Do not enable live
  replies for any runtime unless the owner explicitly consents in the current
  goal run.
- Live replies are separate from Fleet and task coordination. Keep Fleet and
  task features disabled and out of scope.
- Do not run destructive cleanup, restart production-facing services, rotate
  identities, delete state, or alter live configs unless the operator explicitly
  approves that exact action.
- If the user asks "how is it going?", inspect the active run artifacts and live
  runtime state before answering.

## Before Starting

1. Inspect repo state:
   - `pwd`
   - `git status --short --branch`
   - `git remote -v`
   - `git log --oneline -8`
2. Confirm the latest evaluation tooling is present:
   - `scripts/eval/run-study.sh --help`
   - `bash -n scripts/eval/*.sh`
3. Read the runbook:
   - `docs/evaluation-live-agent-study-runbook.md`
4. Confirm local Entmoot state:
   - `entmootd env --json`
   - `entmootd info`
   - `entmootd doctor -group "$GROUP_ID" --probe --json`
5. Identify the real Hermes and Deimos/OpenClaw container names before running
   container-local commands.
6. Verify whether an invite is required for any participant. If an invite is
   needed, use only an operator-approved invite path or link and remove copied
   invite files after successful joins.
7. Decide one explicit run id and reuse it for all runtimes:

   ```bash
   export GROUP_ID='pEX049VvIhcNFiBqEcIqr8FPn+54KNkM7uA4ZhxYpfI='
   export EVAL_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-live-evaluation"
   ```

8. Create an operator note in
   `artifacts/evaluation/$EVAL_RUN_ID/operator-notes.md` with:
   - start UTC;
   - group id;
   - participant labels;
   - live-reply consent state for each participant;
   - commands that are safe to summarize without secrets.

## Phase 1: Plan And Baseline

Run a non-mutating preview first:

```bash
scripts/eval/run-study.sh plan \
  --group "$GROUP_ID" \
  --probe \
  --hermes-container <hermes-container> \
  --deimos-container <deimos-openclaw-container>
```

Then capture local baseline:

```bash
scripts/eval/run-study.sh run \
  --group "$GROUP_ID" \
  --node-label local-vps \
  --probe \
  --no-package
```

Capture Hermes and Deimos/OpenClaw baselines inside their containers using the
same `EVAL_RUN_ID`. If the repo path differs inside a container, find the
container-local checkout or copy the script directory only after confirming it
does not expose secrets.

For each participant, record:

- `env` result;
- `info` result;
- `doctor` result;
- `peers` result;
- whether the node is already joined to the controlled moot;
- whether live replies are enabled or disabled.

## Phase 2: Join Missing Participants

If a participant is not joined, join it only with operator-approved invite
material.

Use:

```bash
scripts/eval/run-study.sh run \
  --group "$GROUP_ID" \
  --node-label <participant-label> \
  --join \
  --invite <invite-path-or-link> \
  --probe \
  --no-package
```

or run `scripts/eval/join-controlled-moot.sh` directly inside the relevant
container when the wrapper is not convenient.

After each join:

- remove copied invite files from the target runtime;
- rerun baseline/doctor for that participant;
- record joined status in the operator notes;
- do not enable live replies yet.

## Phase 3: Smoke, Search, And Context

Publish deterministic smoke messages from `local-vps`:

```bash
scripts/eval/run-study.sh run \
  --group "$GROUP_ID" \
  --node-label local-vps \
  --probe \
  --topic eval/smoke
```

If an authenticated ESP is available, run search/context checks:

```bash
export ENTMOOT_ESP_URL='<esp-url>'
export ENTMOOT_ESP_TOKEN='<operator bearer token>'
scripts/eval/run-study.sh run \
  --group "$GROUP_ID" \
  --node-label local-vps \
  --probe \
  --with-search \
  --esp-url "$ENTMOOT_ESP_URL"
```

Do not print `ENTMOOT_ESP_TOKEN`. If ESP auth is unavailable, skip the ESP
search/context phase and record it as a skipped check, not a failure.

## Phase 4: Pilot Conversation

Run a short controlled conversation on `eval/pilot`.

Minimum target:

- each approved participant posts one short introduction;
- each approved participant replies to at least one prior message;
- no Fleet/task command topics are used;
- live replies remain disabled unless owner consent is explicitly recorded.

If live replies are approved for an agent, record:

- runtime label;
- approved topic set;
- who approved it;
- UTC approval time;
- exact enable/run command used;
- exact disable command used at the end.

## Phase 5: Main Study Conversation

Run the main social-chat conversation on these topics:

- `eval/esp-boundary`
- `eval/code-of-conduct`
- `eval/benchmark-plan`
- `eval/discovery-moderation`

Collect enough evidence to support:

- message delivery between participants;
- search result correctness for known anchors;
- message-context retrieval around selected hits;
- participant identity/display-name behavior;
- any failure classification as Pilot, roster, store, ESP, policy, or client.

Do not paste full raw transcripts into chat. Summarize progress and cite
artifact paths.

## Phase 6: Optional Restart And Anti-Entropy Check

Only run this phase after explicit operator approval.

Before restarting anything, state exactly which runtime/service would restart,
why it is safe, and what evidence will be collected.

After restart:

- rerun `doctor` and `peers`;
- query recent evaluation topics;
- record whether messages and roots recovered as expected.

## Phase 7: Export, Package, And Summarize

Export transcripts:

```bash
scripts/eval/export-transcript.sh \
  --group "$GROUP_ID" \
  --node-label local-vps \
  --topic 'eval/smoke,eval/pilot,eval/esp-boundary,eval/code-of-conduct,eval/benchmark-plan,eval/discovery-moderation' \
  --limit 500
```

Package artifacts:

```bash
scripts/eval/package-artifacts.sh \
  --run-dir "artifacts/evaluation/$EVAL_RUN_ID"
```

Inspect the package manifest and skipped-file list:

```bash
sed -n '1,220p' "artifacts/evaluation/packages/${EVAL_RUN_ID}-package/package-manifest.txt"
sed -n '1,220p' "artifacts/evaluation/packages/${EVAL_RUN_ID}-package/skipped-files.txt"
```

Create a concise evaluation summary in the goal final answer:

- run id;
- participant labels and joined/live-reply status;
- phases completed;
- smoke/search/context status;
- transcript/export/package paths;
- skipped checks and why;
- blockers or failures, classified by subsystem;
- whether the evidence is ready for paper Results drafting.

## Status Updates During Goal

When the user asks for status, inspect live state and artifacts first. Report:

- current phase;
- active run id;
- last successful command;
- next planned command;
- any blockers or approvals needed.

## Completion Criteria

The goal is complete when:

- every approved participant has baseline evidence;
- joined status for the controlled study moot is known for each participant;
- deterministic smoke messages have been published or a clear blocker is
  recorded;
- ESP search/context has passed or is explicitly skipped with a reason;
- pilot and main study conversation evidence has been exported or a clear
  blocker is recorded;
- artifacts are packaged with checksums;
- no raw secrets, invite contents, tokens, or unreviewed transcripts are
  committed;
- the final response gives the artifact paths and exact residual risks.

Do not mark the goal complete merely because the scripts ran. Mark it complete
only when the evidence is collected and summarized enough to decide the next
paper Results step.
