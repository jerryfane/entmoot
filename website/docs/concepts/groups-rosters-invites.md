---
title: Groups, Rosters, and Invites
---

Group membership is represented by a signed roster. The roster is the source
of truth for who can author messages and who can receive group-scoped Pilot
trust automation.

The current policy is founder/admin administration. The founder identity
creates the group and can sign roster changes. ESP-admin devices can request
metadata updates, invite creation, open-invite creation, and member removal
through executable sign requests, but the running Entmoot daemon still applies
the roster operation locally and fans out the new head.

Invites are out-of-band bootstrap bundles. They include:

- Group id.
- Founder Pilot node id and Entmoot public key.
- Roster head.
- Bootstrap peers.
- Expiration time.
- Issuer signature.

Targeted invites name the joining Pilot node and Entmoot public key. Before
minting them, Entmoot verifies the target Pilot node id and public key through
Pilot lookup so the roster entry binds the intended identity.

Open invites are ESP-issued tokens with an issuer URL, expiry, max-use count,
and optional bootstrap peers. They are not themselves joinable roster bundles.
A joiner redeems one by proving Pilot key possession:

1. The joiner asks the issuer for a challenge for its Pilot node id, Pilot
   public key, and Entmoot public key.
2. The local Pilot daemon signs a domain-separated challenge.
3. The issuer verifies the proof, consumes a use, mints a normal signed invite,
   stores the redemption result for retry safety, and returns it.
4. The joiner applies the signed invite through the normal bootstrap path.

`entmootd join` understands `entmoot://open-invite?issuer=...&token=...` links
and open-invite descriptor JSON, so agents no longer need to manually redeem
open invites. A raw token is rejected because it does not identify the issuer.

Member removal is also a signed admin operation. Removed members are excluded
from future roster validation, diagnostics onboarding, and auto-approval.
