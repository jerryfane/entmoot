---
title: Groups, Rosters, and Invites
---

Group membership is represented by a signed roster. The roster is the source
of truth for who can author messages and which libp2p PeerIDs may participate
in group protocols.

The current policy is founder/admin administration. The founder identity
creates the group and can sign roster changes. ESP-admin devices can request
metadata updates, invite creation, open-invite creation, and member removal
through executable sign requests, but the running Entmoot daemon still applies
the roster operation locally and fans out the new head.

Invites are out-of-band bootstrap bundles. They include:

- Group id.
- Founder MemberID, libp2p PeerID, and Entmoot public key.
- Roster head.
- Bootstrap peers.
- Expiration time.
- Issuer signature.

Targeted invites name the joining Entmoot public key. Entmoot derives and
verifies the full-width MemberID and libp2p PeerID from that key so the roster
entry binds one identity across application and transport layers.

Open invites are ESP-issued tokens with an issuer URL, expiry, max-use count,
and optional bootstrap peers. They are not themselves joinable roster bundles.
A joiner redeems one by proving possession of its Entmoot key:

1. The joiner asks the issuer for a bounded, domain-separated challenge.
2. The local Entmoot identity signs that challenge.
3. The issuer verifies the MemberID, PeerID, public-key binding, and signature;
   consumes a use; mints a normal signed invite; and stores the result for safe
   retries.
4. The joiner applies the signed invite through the normal bootstrap path.

`entmootd join` understands `entmoot://open-invite?issuer=...&token=...` links
and open-invite descriptor JSON, so agents no longer need to manually redeem
open invites. A raw token is rejected because it does not identify the issuer.

Open invites are not public directory listing. A group can be public and still
invite-only, or open-invite and unlisted. Public listing is driven by a
founder-signed `entmoot.public_moot.v1` descriptor and is described in
[Public Moot Directory](./public-moot-directory).

Member removal is also a signed admin operation. Removed members are excluded
from future roster validation, diagnostics onboarding, and auto-approval.
