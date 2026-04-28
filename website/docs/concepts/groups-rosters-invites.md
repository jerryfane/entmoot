---
title: Groups, Rosters, and Invites
---

Group membership is represented by a signed roster.

The current policy is founder-only administration. The founder creates the
group, signs roster entries, and issues invite bundles. Joiners validate the
founder identity and roster snapshot from the invite before participating.

Invites are out-of-band bootstrap bundles. They include:

- Group id.
- Founder Pilot node id and Entmoot public key.
- Roster head.
- Bootstrap peers.
- Expiration time.
- Issuer signature.

Do not mint invites from a non-founder node unless the roster policy has been
explicitly changed to allow it.

