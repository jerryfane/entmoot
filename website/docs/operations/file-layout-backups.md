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
```

Group SQLite stores live under the Entmoot data root. Back up the data root
and identity file together. Do not publish private keys or device private keys.

