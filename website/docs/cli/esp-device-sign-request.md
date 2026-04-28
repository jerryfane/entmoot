---
title: esp device and sign-request
---

Manage ESP device keys:

```sh
entmootd esp device list
entmootd esp device add -id ios-1 -pubkey <BASE64_PUBKEY> -group <GROUP_ID> -client ios-1
entmootd esp device onboard -id ios-1 -group <GROUP_ID> -client ios-1
entmootd esp device disable -id ios-1
entmootd esp device remove -id ios-1
```

Sign one request for manual smoke tests:

```sh
entmootd esp sign-request \
  -device ios-1 \
  -private-key-file ./ios-1.key \
  -method GET \
  -path '/v1/mailbox/pull?client_id=ios-1&group_id=<GROUP_ID>'
```

Production phones should generate and retain the private key. The ESP registry
stores only public keys.

