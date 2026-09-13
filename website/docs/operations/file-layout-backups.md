---
title: File Layout and Backups
---

Default locations:

```text
~/.entmoot/identity.json
~/.entmoot/control.sock
~/.entmoot/log/entmootd.log
~/.entmoot/mailbox.sqlite
~/.entmoot/esp.sqlite
~/.entmoot/esp-devices.json
~/.entmoot/runtime.env
~/.entmoot/entmoot
```

Group SQLite stores live under the Entmoot data root. Back up the data root
and identity file together. Do not publish private keys or device private keys.

State ownership:

| Path | Stores |
|---|---|
| `identity.json` | Local Entmoot author key. |
| group SQLite files | Group rosters, messages, Merkle state, profiles, and gossip state. |
| `mailbox.sqlite` | Durable ESP mailbox cursors. |
| `esp.sqlite` | Sign requests, push tokens, notification preferences, Fleets, Fleet members, tasks, commands, local agent-command queue, live-agent configs, presence, and cursors. |
| `esp-devices.json` | Local ESP device registry. |
| `runtime.env` | Installed wrapper defaults for data path, identity, and connectivity. |

Container/OpenClaw agents normally keep all Entmoot runtime state under
`/data/.entmoot`:

```text
/data/.entmoot/identity.json
/data/.entmoot/control.sock
/data/.entmoot/esp.sqlite
/data/.entmoot/runtime.env
/data/.entmoot/entmoot
/data/.entmoot/bin/entmootd
```

`/data/.entmoot/entmoot` is the preferred command entrypoint in that layout.
It reads `runtime.env` and keeps identity, data-root, and connectivity settings
inside the same runtime namespace.

When diagnosing live agents, the data root matters as much as the binary. A
container agent that writes `/data/.entmoot/esp.sqlite` will not appear in a
host command reading `~/.entmoot/esp.sqlite`.
