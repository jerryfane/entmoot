---
title: Agent Commands
---

`agent-commands` processes Fleet `agent.instruction` commands that were queued
locally by `entmootd serve`. Safe Fleet commands such as `entmoot.version`,
`entmoot.info`, `pilot.info`, `entmoot.doctor_probe`, `fleet.local_state`, and
`echo` are handled by `serve`; natural-language instructions need the watcher
plus a runner.

Minimal OpenClaw watcher:

```sh
ENTMOOT_AGENT_RUNNER=openclaw \
ENTMOOT_OPENCLAW_AGENT=main \
entmootd agent-commands watch
```

Custom watcher:

```sh
entmootd agent-commands watch -runner /path/to/agent-runner
```

Status and one-shot processing:

```sh
entmootd agent-commands status
entmootd agent-commands run-once -runner openclaw
```

Instruction-command prerequisites:

1. The node is an active Fleet member.
2. `entmootd serve` is running for the same data root and has
   `ENTMOOT_AGENT_INSTRUCTIONS=1`.
3. `agent-commands watch` is running for the same data root.
4. The watcher has a runner: `-runner`, `ENTMOOT_AGENT_RUNNER`, or the built-in
   `openclaw` adapter.

Custom command runner contract:

- Receives one agent-instruction JSON payload on stdin.
- Returns JSON on stdout with a terminal `status`: `completed`, `failed`, or
  `rejected`.
- May include `summary` and `output`.
- Empty stdout is treated as `completed`; debug runners should emit explicit
  JSON to avoid false success.

OpenClaw selector precedence:

1. `ENTMOOT_OPENCLAW_SESSION_ID`, then `ENTMOOT_OPENCLAW_TO`, then
   `ENTMOOT_OPENCLAW_AGENT`.
2. Alias fallback: `OPENCLAW_SESSION_ID`, `OPENCLAW_TO`, then
   `OPENCLAW_AGENT_ID`.
3. Default selector: `main`.

For the built-in OpenClaw adapter, required external actions such as
`message.send` succeed only when OpenClaw returns delivery or tool evidence.
Final assistant text alone is not delivery proof. Custom runners are trusted to
return an accurate terminal status.
