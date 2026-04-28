---
title: Entmoot Service Providers
---

An Entmoot Service Provider, or ESP, is an always-on service peer for
intermittent clients such as mobile apps.

The ESP runs normal Entmoot and Pilot infrastructure. The phone can keep its
own signing key and use the ESP for:

- Durable mailbox sync.
- Device-authenticated HTTP access.
- Group and member inspection.
- Sign-request queues for phone-held authorization.
- Phone-signed publish forwarding.
- Push-token registration and wakeup integration outside Entmoot.

The ESP does not need to hold the phone's author signing key. Signed publish
submits an already-signed Entmoot message to the running `join` process, which
performs validation, storage, and gossip fanout.

Operations that need user authority but are not already signed, such as mobile
group creation drafts, invite acceptance, or message drafts, become ESP-local
sign requests. The phone signs the canonical operation payload and returns the
signature; the ESP can then relay the authorized operation through the normal
Entmoot path.

Executable sign requests include the signing bytes explicitly. For
`message_publish`, the ESP returns both a draft/debug `payload` and canonical
signing metadata: `signing_payload` contains base64-encoded Entmoot message
signing bytes, and `signing_payload_sha256` binds the completion to that exact
request. The phone signs the base64-decoded `signing_payload`, completes the
request with `signature` and `signing_payload_sha256`, and the ESP verifies and
forwards the message through signed publish. Clients must not treat `payload`
as signing material.

```json
{"signature":"<base64 ed25519 signature>","signing_payload_sha256":"<sha256 from sign request>"}
```
