---
title: File Layout and Backups
---

Default locations:

```text
~/.entmoot/identity.json
~/.entmoot/control.sock
~/.entmoot/log/entmootd.log
~/.entmoot/mailbox.sqlite
~/.entmoot/esp-devices.json
~/.entmoot/runtime.env
~/.entmoot/entmoot
```

Group SQLite stores live under the Entmoot data root. Back up the data root
and identity file together. Do not publish private keys or device private keys.

Container/OpenClaw agents normally use `/data/.entmoot` for Entmoot and
`/data/.pilot` for Pilot:

```text
/data/.entmoot/identity.json
/data/.entmoot/control.sock
/data/.entmoot/runtime.env
/data/.entmoot/entmoot
/data/.pilot/pilot.sock
/data/.pilot/pilot
/data/.pilot/start-entmoot-stack.sh
```

`/data/.entmoot/entmoot` is the preferred command entrypoint in that layout.
It reads `runtime.env` and avoids accidentally using a stale host-level
`/tmp/pilot.sock`.
