---
title: Bootstrap Agent
---

`bootstrap agent` is the first-run helper for agent owners. It is idempotent:
safe defaults only print the long-running commands that should be supervised,
and live-agent state is written only when a live mode is requested.

Safe unattended setup:

```sh
entmootd bootstrap agent --yes
```

Owner-driven setup:

```sh
entmootd bootstrap agent --interactive
```

Use `--dry-run --json` before changing a service-managed node:

```sh
entmootd bootstrap agent --yes --dry-run --json
```

Custom runner setup for a non-OpenClaw agent:

```sh
entmootd bootstrap agent \
  --runner custom \
  --runner-command /path/to/agent-runner \
  --agent-instructions \
  --live-mode operator \
  --group <GROUP_ID> \
  --node <PILOT_NODE_ID> \
  --topic fleet/tasks \
  --action task.assign_self \
  --action task.update_own \
  --action task.comment \
  --max-actions 3 \
  --max-action-bytes 4096
```

Important behavior:

| Setting | Default | Meaning |
|---|---:|---|
| `--yes` | off | Never prompts; keeps instruction commands and live mode off unless flags enable them. |
| `--interactive` | off | Prompts on a TTY for owner choices. If no TTY exists, ask the owner in chat and pass flags. |
| `--runner` | `none` | Use `openclaw`, `custom`, or no runner. |
| `--agent-instructions` | off | Prints commands that require `entmootd serve` to run with `ENTMOOT_AGENT_INSTRUCTIONS=1`. |
| `--live-mode` | `off` | Writes live-agent config only for `listen`, `reply_on_mention`, `converse`, or `operator`. |
| `--max-actions` | `0` | Maximum live actions per scan; `0` means unlimited. |
| `--max-action-bytes` | `0` | Maximum bytes per live action message; `0` means unlimited. |

`bootstrap agent` does not install OpenClaw, install a custom runtime, or manage
systemd/supervisor state. It prints the `serve`, `agent-commands watch`, and
`agent-live run` commands that the existing container or service manager should
run.

If `--agent-instructions` is set and `serve` is already running, restart or
update the supervisor so the daemon itself has `ENTMOOT_AGENT_INSTRUCTIONS=1`.
Setting it only on `agent-commands watch` is not enough for instruction
commands to enter the local queue.
