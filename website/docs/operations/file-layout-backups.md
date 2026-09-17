---
title: File Layout and Backups
---

Default locations:

```text
~/.entmoot/identity.json
~/.entmoot/control.sock
~/.entmoot/mailbox.sqlite
~/.entmoot/esp.sqlite
~/.entmoot/esp-devices.json
~/.entmoot/runtime.env
~/.entmoot/entmoot
```

The daemon writes its log to stdout, which systemd captures - there is no log
file under the data root unless an operator redirects output to one.
`scripts/verify-mesh-node.sh` looks for `~/.entmoot/log/entmootd.log` by that
convention and takes `ENTMOOT_LOG` to override it.

Group SQLite stores live under the Entmoot data root, one directory per group
beneath `groups/`. Back up the data root and identity file together. Do not
publish private keys or device private keys.

State ownership:

| Path | Stores |
|---|---|
| `identity.json` | Local Entmoot author key. |
| `groups/<group>/membership.sqlite` | The group's signed membership: checkpoints and the records not yet folded into one. |
| `groups/<group>/membership.writer.lock` | Single-writer lease over that group's membership. |
| other group SQLite files | Messages, Merkle state, profiles, and gossip state. |
| `bootstrap-admission.db` | Local record of the invites this node issued, used by `invite list`. Not an authority: invite use limits and revocations are projected from the group's signed state. |
| `mailbox.sqlite` | Durable ESP mailbox cursors. |
| `esp.sqlite` | Sign requests, push tokens, notification preferences, and public moot directory records. |
| `esp-devices.json` | Local ESP device registry. |
| `runtime.env` | Wrapper defaults: binary, data root, identity, port. |

Legacy state, present only in groups created before signed checkpoints:

| Path | Stores |
|---|---|
| `groups/<group>/roster.sqlite` | The pre-checkpoint linear roster chain. Read-only. `entmootd membership upgrade` mints checkpoint 0 from it, and it stays on disk afterwards so version-0 messages that cite one of its entries can still be verified. |
| `groups/<group>/roster.jsonl` | Older import form of the same chain. |

A group directory that holds only the legacy chain and no
`membership.sqlite` is not serveable until the founder runs
`entmootd membership upgrade`.

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
It reads `runtime.env` and keeps the binary, identity and data root inside the
same runtime namespace. Connectivity is not among them - the wrapper execs
only `-identity`, `-data` and `-listen-port`.

When diagnosing ESP state, the data root matters as much as the binary. A
container agent that writes `/data/.entmoot/esp.sqlite` will not appear in a
host command reading `~/.entmoot/esp.sqlite`.
