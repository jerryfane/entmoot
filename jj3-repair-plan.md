# JJ3 Entmoot Repair Plan

## What Happened

JJ3 did not suddenly lose all network reachability. It lost the conditions Entmoot needs for reliable gossip.

There are two failures layered together:

1. **Entmoot transport ads disappeared or became missing for JJ3.**
   On the VPS node `45981`, Entmoot restarted on **June 12, 2026 around 15:42-15:53 CEST** and logged:

   ```text
   gossip: transport_ad skip publish (no local endpoints)
   ```

   That means the daemon was serving groups, but it did not publish a current signed Entmoot endpoint for peers to dial. Current JJ3 diagnostics show `transport_missing` for all three JJ3 members: `45981`, `45460`, and `155760`.

2. **Asia/Hermes `155760` has a Pilot identity mismatch.**
   The VPS Pilot log has repeatedly shown, since at least **June 12, 2026 15:22 CEST**:

   ```text
   auth key exchange: Ed25519 pubkey mismatch with registry
   ```

   for peer `155760`.

   Registry currently says `155760` should have Pilot public key:

   ```text
   Vhqghgj95WH0xHI+y7dEBNtrL6ZVnSkKzKNAJQgFO5c=
   ```

   If the running Asia daemon presents a different key, Pilot rejects some tunnels even though direct Pilot text can sometimes still ACK through an existing or alternate path.

## Why It Worked Before

It likely worked because some state was already warm:

- Existing Pilot tunnels were open.
- Cached peer routes or previous transport ads were still useful.
- Entmoot streams on port `1004` had not yet needed a clean cold reconnect.
- Direct Pilot messaging is simpler than Entmoot gossip sync, so "Pilot text ACK" does not prove JJ3 gossip health.

Then after restarts, retries, stream closure, NAT/relay churn, or dial-backoff, Entmoot needed fresh transport ads and clean Pilot identity verification. At that point:

- JJ3 had missing transport ads.
- Asia had key mismatch/backoff.
- Trust was not fully mutual.
- Entmoot sync stopped even though low-level Pilot lookup/ping could still work.

## Fix Order

Fix `155760` first. Then restart JJ3 Entmoot with valid advertised endpoints. Then verify from all sides.

## Asia / Hermes `155760`

1. Check the running Pilot identity:

```sh
export PATH="$HOME/.pilot/bin:$HOME/.entmoot/bin:$PATH"

pilotctl info --json
pilotctl lookup 155760
```

2. Compare `pilotctl info --json` public key to this registry key:

```text
Vhqghgj95WH0xHI+y7dEBNtrL6ZVnSkKzKNAJQgFO5c=
```

3. If it differs, the wrong Pilot identity/daemon is running. Do not delete identities. Check for duplicate daemons:

```sh
ps aux | rg 'pilot-daemon|pilotctl'
pilotctl daemon status
```

Start the intended Pilot daemon using the correct local service/wrapper and identity.

4. After Pilot identity is correct:

```sh
pilotctl ping 45981 --count 3 --timeout 10s
pilotctl ping 45460 --count 3 --timeout 10s
pilotctl trust | rg '45981|45460'
```

5. If trust is not mutual, exchange handshakes:

```sh
pilotctl handshake 45981 "JJ3 repair"
pilotctl handshake 45460 "JJ3 repair"
pilotctl pending
```

6. Restart Entmoot for JJ3 with a real endpoint or TURN:

```sh
entmootd -socket /tmp/pilot.sock \
  -identity ~/.entmoot/identity.json \
  -data ~/.entmoot \
  serve -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' \
  -advertise-endpoint tcp=<reachable-host-or-ip>:1004
```

## VPS / Hub `45981`

1. Confirm Pilot state:

```sh
pilotctl info --json
pilotctl lookup 45981
```

Registry currently shows VPS endpoint `37.27.59.89:37463`, but Entmoot still needs an app-level endpoint for `1004`.

2. Restart Entmoot with an advertised endpoint or working TURN:

```sh
entmootd -socket /tmp/pilot.sock \
  -identity /root/.entmoot/identity.json \
  -data /root/.entmoot \
  serve -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' \
  -advertise-endpoint tcp=37.27.59.89:1004
```

3. Check trust:

```sh
pilotctl trust | rg '155760|45460'
```

If needed:

```sh
pilotctl handshake 155760 "JJ3 repair"
pilotctl handshake 45460 "JJ3 repair"
```

## Italy / Phobos `45460`

1. Confirm Pilot identity:

```sh
pilotctl info --json
pilotctl lookup 45460
```

Registry expects Italy Pilot public key:

```text
ic0HN3QsaH4A1XOL9xxk5xwrQLtT9jVNjsNZgDSSWEM=
```

2. Check routes and trust:

```sh
pilotctl ping 45981 --count 3 --timeout 10s
pilotctl ping 155760 --count 3 --timeout 10s
pilotctl trust | rg '45981|155760'
```

3. Restart Entmoot with an endpoint or TURN:

```sh
entmootd -socket /tmp/pilot.sock \
  -identity ~/.entmoot/identity.json \
  -data ~/.entmoot \
  serve -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' \
  -advertise-endpoint tcp=<italy-public-host-or-ip>:1004
```

## Final Verification

Run this on all three:

```sh
entmootd peers -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' --probe --json -timeout 15s
```

Success means each peer shows:

```text
transport: ok
trust: trusted/self
route: ok
diagnosis: ok
```

Then publish one short JJ3 test message from each node and confirm all three see the same latest messages.
