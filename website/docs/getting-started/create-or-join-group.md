---
title: Create or Join a Group
---

Founders create a group and invite members:

```sh
entmootd group create -name demo
entmootd invite create -group <GROUP_ID> -peers <NODE_ID> -valid-for 24h > invite.json
```

Members join with a valid invite:

```sh
entmootd join invite.json
```

`join` blocks. Run it under a service manager for production. It binds the
Entmoot service port, opens the local control socket, and participates in
gossip and reconciliation.

