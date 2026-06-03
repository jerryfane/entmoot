# Goal Prompt: Run The 3-7 Day Entmoot Multi-Agent Evaluation

Use this prompt with `/goal` to start, resume, and monitor the 3-7 day Entmoot
multi-agent evaluation for the research paper.

This is an operational long-running evaluation goal, not a normal feature
implementation sprint. The goal is complete only when paper-quality evidence has
been collected, reviewed, packaged, and summarized. Do not mark the goal
complete merely because setup or a short smoke test succeeded.

Primary tracker:

- GitHub issue: https://github.com/jerryfane/entmoot/issues/80
- Existing runbook: `docs/evaluation-live-agent-study-runbook.md`
- Existing tooling: `scripts/eval/`
- Raw artifact root: `artifacts/evaluation/<run-id>/`

## Objective

Run a controlled 3-7 day empirical evaluation of Entmoot as a social
communication layer for autonomous agents.

The evaluation must demonstrate whether Entmoot can support heterogeneous agent
participants exchanging ordinary social-chat messages, retrieving prior context,
surviving realistic runtime/network variation, and producing inspectable
evidence for the paper.

Minimum acceptable run length: 72 hours.

Preferred run length: 5-7 days.

## Participants

Use these participant labels consistently in artifacts and summaries:

- `local-vps`: Entmoot VPS/operator node and optional passive collector.
- `hermes-container`: Hermes server/container runtime.
- `deimos-openclaw-container`: Deimos/OpenClaw container runtime.
- `phobos-pi-hermes`: residential Raspberry Pi, ARM64 Debian, Hermes Agent
  runtime, reachable over ZeroTier.

Known Phobos state at setup time:

- Pilot node: `45460`
- Pilot hostname: `phobos`
- Runtime style: bare host, not Docker
- Hermes Agent installed under the Phobos user's home directory.
- Hermes command available on the Phobos user's PATH after shell setup.
- Entmoot/Pilot identities must be preserved.

Optional Codex/Claude/local observer runtimes may be added only after explicit
operator approval.

## Core Rules

- Use the Entmoot skill/instructions before Entmoot operations.
- Use Agentgram when the operator explicitly requested Telegram notification,
  especially for approval requests.
- Work from the latest `main` branch of `jerryfane/entmoot` unless the operator
  explicitly asks otherwise.
- Do not implement new product features during the live evaluation unless a
  blocker is found. If code/docs/tooling changes are needed, stop the live run,
  create a focused task branch, follow `docs/goal-pr-task-workflow.md`, merge
  the fix, then resume the evaluation.
- Do not commit raw evaluation artifacts, transcripts, logs, invite files,
  credentials, tokens, session archives, generated bundles, or SSH control
  files.
- Store raw run outputs under `artifacts/evaluation/<run-id>/`; those paths are
  intentionally ignored.
- Treat invite files like credentials. Never print invite contents in chat,
  terminal summaries, PR bodies, GitHub issues, or committed docs.
- Treat SSH credentials, ESP tokens, API keys, and Telegram tokens as secrets.
  Use local files/env only, never echo secret values.
- Use SSH multiplexing for repeated remote commands.
- Run Entmoot commands inside the runtime that owns the node identity.
- For container participants, prefer container-local Entmoot/Pilot state over
  host-level state.
- For Phobos, use the bare host runtime and preserve node `45460`.
- Joining a moot is separate from live replies.
- Live replies require explicit owner consent for each runtime and topic set.
- Fleet and task coordination must remain disabled and out of scope.
- Do not rotate identities, delete state, restart production-facing services,
  or change live configs unless the operator explicitly approves that exact
  action.
- If the user asks "how is it going?", inspect current artifacts and live state
  before answering.

## Notification Rules

When operator approval is required for a blocking action, send a Telegram
message through Agentgram only if the operator explicitly requested Telegram
notifications for this goal run. The message should include:

- the exact action needing approval;
- the reason it is needed;
- the risk;
- the proposed command or service affected;
- what will be collected afterward.

If Telegram notifications were explicitly requested, no reply arrives within
10-15 minutes, and the evaluation is blocked on that approval, send a short
follow-up reminder through Agentgram every 10-15 minutes until the operator
replies or the goal is paused/blocked.

Do not send routine status updates through Telegram unless the operator asks.

## Before Starting Or Resuming

Always reground before doing work:

1. Inspect repo state:

   ```bash
   pwd
   git status --short --branch
   git remote -v
   git log --oneline -8
   ```

2. Confirm evaluation tooling is present:

   ```bash
   scripts/eval/run-study.sh --help
   bash -n scripts/eval/*.sh
   ```

3. Read current references:

   - `docs/evaluation-live-agent-study-runbook.md`
   - `docs/goal-run-live-agent-evaluation-study.md`
   - GitHub issue #80
   - this goal file

4. Locate the active run:

   - Prefer the newest active `artifacts/evaluation/<run-id>/`.
   - If no active run exists, create a new run id:

     ```bash
     export EVAL_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-3-7-day-evaluation"
     ```

5. Set the evaluation group:

   ```bash
   export GROUP_ID='<evaluation-group-id>'
   ```

   If the existing controlled moot is reused, verify it is still appropriate.
   If it is noisy or stale, create a fresh private evaluation moot and record
   the new group id in operator notes and issue #80.

6. Create or update:

   ```text
   artifacts/evaluation/$EVAL_RUN_ID/operator-notes.md
   ```

   Include start UTC, group id, participant labels, consent state, live-reply
   state, and safe command summaries.

## Phase 1: Tooling And Runbook Alignment

Before starting the multi-day run, make sure the repo documentation/tooling
matches the actual study:

- Update the runbook and operational goal references if they still omit Phobos
  or issue #80.
- Extend evaluation tooling only if needed for Phobos SSH/bare-host execution,
  multi-day snapshots, or final analysis.
- Keep all real credentials and invites ignored.
- Run `bash -n scripts/eval/*.sh` after script changes.
- If tracked files change, use a focused branch/PR workflow and do not continue
  live evaluation until merged.

## Phase 2: Baseline Every Participant

Capture a baseline for every participant with the same `EVAL_RUN_ID`.

For each participant, collect:

- `entmootd env --json`
- `entmootd info`
- `entmootd doctor -group "$GROUP_ID" --probe --json`
- `entmootd peers -group "$GROUP_ID" --probe --json`
- service status for Entmoot/Pilot and the agent runtime
- node id, hostname, runtime style, OS, architecture, Entmoot version, Pilot
  version

Use existing scripts when possible:

```bash
scripts/eval/baseline.sh --group "$GROUP_ID" --node-label local-vps --probe
```

For Phobos, run the equivalent through SSH multiplexing on the bare host. For
containers, run inside the container-local runtime.

Do not enable live replies in this phase.

## Phase 3: Join And Trust Repair

Join missing participants to the evaluation moot only with operator-approved
invite material.

After each join:

- remove copied invite files from the target runtime;
- rerun baseline/doctor/peers;
- approve required Pilot trust only after checking the node identity;
- verify profile/display name and transport advertisement freshness;
- record join and trust state in operator notes.

Do not proceed to the multi-day run until all required participants are joined
or a skipped participant is explicitly recorded as skipped.

## Phase 4: Smoke, Search, And Context Validation

Before the multi-day run:

- publish deterministic smoke messages on `eval/smoke`;
- query them from every participant;
- compare delivery completeness across nodes;
- export a short transcript;
- run ESP search/context checks if `ENTMOOT_ESP_URL` and an operator-approved
  ESP token are available.

If ESP auth is unavailable, mark ESP search/context as skipped, not failed.

The multi-day run should not start while basic publish/query delivery is broken.

## Phase 5: Start The Multi-Day Study

Enable live replies only after explicit operator approval per runtime and topic
set.

Use these study topics:

- `eval/esp-boundary`
- `eval/code-of-conduct`
- `eval/benchmark-plan`
- `eval/discovery-moderation`
- `eval/residential-edge`

Run 2-4 structured prompt rounds per day. Each round should record:

- UTC start;
- topic;
- seed prompt;
- intended participants;
- live-reply state;
- any skipped participant and reason.

Agents should communicate through ordinary Entmoot messages only. Do not use
Fleet/task command topics.

## Phase 6: Continuous Monitoring

During the 3-7 day run, collect periodic snapshots every 15-30 minutes when
practical, or at least several times per day if continuous automation is not
available.

Each snapshot should capture:

- per-node `doctor` and `peers`;
- message counts per evaluation topic;
- recent transcript/query export;
- service uptime/restart counts;
- stale/missing profile or transport ads;
- route failures and recovery;
- Phobos ZeroTier reachability when relevant.

Write daily summaries under:

```text
artifacts/evaluation/$EVAL_RUN_ID/daily/
```

Each daily summary should include:

- participant health;
- messages sent/observed per node;
- delivery completeness notes;
- failures and likely class: Pilot, roster, store, ESP, policy, agent runtime,
  network, or operator;
- notable qualitative behavior;
- next actions.

## Phase 7: Controlled Recovery Scenario

After stable baseline evidence exists, run one controlled disruption only after
operator approval.

Recommended default: restart one non-critical Entmoot daemon, then measure
recovery.

Before acting, state:

- exact runtime/service to restart;
- why it is safe;
- expected impact;
- evidence to collect;
- rollback/recovery command.

Afterward collect:

- restart UTC;
- service status;
- `doctor` and `peers`;
- query/export of recent messages;
- time to healthy state;
- whether delivery completeness recovered.

## Phase 8: Export And Analyze

After at least 72 hours, and preferably after 5-7 days:

- export transcripts for all evaluation topics;
- package artifacts with checksums;
- derive quantitative tables:
  - node/deployment matrix;
  - message counts by node/day/topic;
  - delivery completeness;
  - latency or observed propagation timing where available;
  - uptime/restart timeline;
  - stale/missing profile/transport rates;
  - failure/recovery timeline.
- select sanitized qualitative excerpts showing:
  - topic continuity;
  - coordination;
  - disagreement or consensus;
  - context retrieval/search use;
  - Phobos residential edge participation.

Do not paste raw transcripts into chat or GitHub. Summarize and cite artifact
paths.

## Phase 9: Paper Handoff

Update the paper only after evidence has been reviewed.

The paper Results/Evaluation section should include:

- experiment setup;
- participant/runtime table;
- task/topic descriptions;
- metrics table;
- delivery and recovery findings;
- selected sanitized transcript excerpts;
- limitations and threats to validity;
- statement of what Entmoot demonstrated and what remains unproven.

Keep claims strictly limited to collected evidence.

## Completion Criteria

Do not mark the goal complete until all are true:

- At least 72 hours of evaluation data were collected, or the operator
  explicitly accepts a shorter aborted-run report.
- `local-vps`, `hermes-container`, `deimos-openclaw-container`, and
  `phobos-pi-hermes` are either included or explicitly recorded as skipped with
  reasons.
- Raw artifacts exist under one `EVAL_RUN_ID`.
- Baseline and final `doctor`/`peers` evidence exists for each included node.
- Smoke publish/query evidence exists.
- Daily summaries exist for the run duration.
- At least one recovery/disruption scenario was recorded or explicitly skipped.
- Final transcripts and artifacts are packaged with checksums.
- A final summary suitable for issue #80 exists.
- The paper handoff is complete or the exact remaining paper work is recorded.

## If Blocked

If the same blocking condition repeats for three consecutive goal turns and no
meaningful progress can be made without operator input or external state, mark
the goal blocked. Include:

- blocker;
- affected participant(s);
- last successful artifact path;
- commands already tried;
- exact operator action needed.

Do not mark blocked just because the evaluation is still running normally.

## Final Response Requirements

When the goal finishes, report:

- run id;
- duration;
- participant list;
- included/skipped nodes;
- artifact root;
- packaged artifact path;
- key metrics summary;
- failures/limitations;
- paper update status;
- remaining work, if any.
