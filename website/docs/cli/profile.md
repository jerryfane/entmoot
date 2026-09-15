---
title: profile
---

A display name is a hint an app may show instead of a truncated MemberID. It is
not identity and grants nothing.

```sh
entmootd profile set -name pi-burj [-group GID] [-ttl 720h]
entmootd profile clear [-group GID]
entmootd profile show [-group GID]
```

`set` publishes the name into the group as an ordinary signed message on the
reserved topic `entmoot/profile/1`. It needs the daemon running, for the same
reason `publish` does: the daemon holds the identity, the store and the
GossipSub topic. Every member that receives the message records the name, and
ESP member listings show it.

`clear` publishes an empty name, withdrawing it. `show` prints the names this
node has observed for the group's members and works with the daemon stopped.

A name expires after 30 days unless `-ttl` says otherwise; `-ttl 0` means no
expiry. Republishing before then extends it. Expiry is why a node that leaves
for good eventually stops being displayed.

## What a name cannot do

- It cannot impersonate another member. Display output is `name#MemberID`, so
  the full identity travels with the name.
- It cannot be set for somebody else. The name is taken from the message
  author, and a message is only accepted from a current member.
- It cannot be set by a removed member. Publishing requires membership.
- It cannot be smuggled in on another topic. A profile payload published on
  any topic other than the reserved one is ignored.
- It cannot break or forge a client's layout. Because the display form is a
  bare `name#MemberID`, a name may not contain `#`, a control character, a
  Unicode format character (the bidi overrides such as U+202E would otherwise
  reverse the appended MemberID, and zero-width characters would hide text) or
  a line or paragraph separator (U+2028, U+2029).
- It cannot be pinned. A node orders profiles by when it received them, not by
  the timestamp in the payload, so a message dated in the future cannot freeze
  a member's name against later updates.

Names are limited to 64 runes and 255 bytes. The rune limit is what a user
notices, so a name in a non-Latin script is not cut shorter than a Latin one;
the byte limit matches the store's own cap, so a name that could not be stored
is refused at publish time rather than reported as published and dropped.

An expiry longer than 90 days is clamped to 90 days.
