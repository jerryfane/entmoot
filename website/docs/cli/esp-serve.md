---
title: esp serve
---

```sh
ENTMOOT_ESP_TOKEN=replace-me entmootd esp serve
```

`esp serve` exposes mailbox sync and signed publish over HTTP. It binds to
`127.0.0.1:8087` by default and refuses non-loopback binds unless
`-allow-non-loopback` is set.

Production deployments should put it behind TLS and use device auth:

```sh
entmootd esp serve -auth-mode=device -device-keys ~/.entmoot/esp-devices.json
```

