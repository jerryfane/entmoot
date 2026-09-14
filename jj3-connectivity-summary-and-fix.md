# JJ3 Connectivity Summary And Fix

Last updated: 2026-06-16

## Short Version

JJ3 sync broke in two separate layers:

1. Asia `155760` had a Pilot identity mismatch.
2. After the identity was repaired, Entmoot transport advertisements were still
   missing or unstable.

The identity problem is now fixed. VPS and Italy are now healthy from the VPS
view. Asia now publishes a transport ad, but its UDP endpoint has changed after
restart and streams still appear unreliable from outside Asia.

The durable fix for Asia is either:

- pin Asia Pilot to a fixed reachable UDP port, or
- use TURN for Asia.

TURN is the safer recommendation if Asia is behind NAT that changes mappings or
causes stalled streams.

## What Happened

Asia node `155760` was running with a Pilot key that did not match the Pilot
registry. Peers saw `155760` as the expected node id, but the cryptographic key
did not match the registry record. That made Pilot authentication unreliable and
caused Entmoot gossip to fail or enter dial-backoff.

After Asia repaired or rotated the registry identity, the key mismatch was
resolved. The current Asia Pilot key observed from VPS is:

```text
YfL9GxB0+3OPOFVpZKPBjmXcpDcnT0kx4h5DbxPH0hM=
```

Then the remaining problem became Entmoot transport advertisement. Entmoot peers
need signed transport ads so other group members know which Pilot endpoint to
use for group streams.

## Why It Happened

There were two causes:

1. The active Asia Pilot private identity changed or was replaced, while the
   registry still expected the old key.
2. Asia is behind a network path where the public UDP mapping can change after
   Pilot restarts.

The Asia endpoint changed from:

```text
124.79.12.31:8992
```

to:

```text
124.79.12.31:5678
```

That strongly suggests Asia Pilot was using a random local listen port, such as:

```sh
-listen :0
```

With `-listen :0`, the OS picks a random local UDP port. Behind NAT, that can
also produce a different public mapped port each restart. It may not change on
literally every restart, but it is uncontrolled and should not be treated as a
stable Entmoot endpoint.

## What `-advertise-endpoint` Means

`-advertise-endpoint` tells other Entmoot peers how to reach this node's Pilot
daemon from the outside.

Example:

```sh
-advertise-endpoint udp=37.27.59.89:37463
```

means:

```text
Peers should reach this node's Pilot daemon over UDP at 37.27.59.89:37463.
```

Important distinction:

```text
Pilot endpoint: public UDP/TCP/TURN address used to reach Pilot
Entmoot service port: 1004 inside the Pilot virtual network
```

Port `1004` is the Entmoot service port inside Pilot. It is not automatically
the public network port to advertise.

## Current Known State

From VPS `45981`:

```text
45981 vps:
  Pilot endpoint: 37.27.59.89:37463
  Entmoot transport: ok
  Diagnosis: ok

45460 phobos / Italy:
  Pilot route: ok
  Entmoot transport: ok
  Diagnosis: ok

155760 smithyx-china / Asia:
  Pilot lookup: public
  Current endpoint: 124.79.12.31:5678
  Entmoot advertised endpoint: udp=124.79.12.31:5678
  Transport ad: present
  Remaining issue from VPS: stream probe can still time out
```

Asia's previous endpoint `124.79.12.31:8992` is stale. Do not use it anymore.

## Why VPS And Italy Are Less Affected

VPS has a real public server endpoint:

```text
37.27.59.89:37463
```

That is stable and not being rewritten by a residential or restrictive NAT.

Italy currently has a stable enough endpoint/route:

```text
87.18.57.196:4000
```

Asia is different because its public UDP mapping changed after restart. That
means a signed Entmoot transport ad can become stale if it points at the old
NAT mapping.

## Recommended Fix

### Best Reliable Fix: TURN For Asia

Use TURN for Asia so peers do not depend on a changing direct UDP NAT mapping.

Target shape:

```text
Asia Pilot -> TURN relay -> VPS/Italy
```

Then Asia should advertise the TURN endpoint instead of the changing public UDP
mapping:

```sh
-advertise-endpoint turn=<asia-turn-endpoint>
```

This is the preferred fix if Asia's NAT keeps changing ports or streams keep
stalling even when the current UDP endpoint is advertised.

### Direct UDP Fix: Only If Asia Controls The Network

If Asia can control its firewall/router/NAT, use a fixed Pilot UDP port.

Steps:

1. Pick a fixed Pilot UDP port, for example `5678`.
2. Change Asia `pilot-daemon.service` from:

```sh
-listen :0
```

to:

```sh
-listen :5678
```

3. Allow/forward UDP `5678` to the Asia machine.
4. Restart Pilot.
5. Confirm the registry stays stable:

```sh
pilotctl lookup 155760
```

Expected stable shape:

```text
124.79.12.31:5678
```

6. Restart Asia Entmoot with:

```sh
-advertise-endpoint udp=124.79.12.31:5678
```

7. VPS and Italy reprobe:

```sh
entmootd peers -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' --probe --json -timeout 30s
```

## Action Plan By Node

### VPS `45981`

Already fixed.

Current persistent Entmoot advertisement:

```sh
-advertise-endpoint udp=37.27.59.89:37463
```

### Italy `45460`

Currently looks healthy from VPS.

Keep advertising the real current Pilot endpoint and continue to approve or
refresh Pilot trust with `155760` and `45981` if prompted.

### Asia `155760`

Do not use the stale endpoint:

```text
124.79.12.31:8992
```

Current observed endpoint:

```text
124.79.12.31:5678
```

Short-term:

```sh
-advertise-endpoint udp=124.79.12.31:5678
```

Durable:

- pin Pilot to a fixed UDP listen port and make sure NAT/firewall preserves it,
  or
- configure TURN and advertise the TURN endpoint.

## Final Recommendation

Use TURN for Asia if this is meant to stay reliable. Pinning a UDP port may
work if Asia controls the router/firewall, but the observed behavior already
shows changing NAT mappings and stalled virtual streams. TURN avoids that class
of failure.
