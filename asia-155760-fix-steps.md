# Asia 155760 Fix Steps

Do not delete identities. Do not run `pilotctl register` again until we decide
restore vs registry rotation.

## Current Problem

Asia `155760` is running with a Pilot key that does not match the registry.

```text
running key: 6M/0JL4hlOhIAYIrlcZ66yDyLIf1r2WG95o7XSFbGIw=
registry key: Vhqghgj95WH0xHI+y7dEBNtrL6ZVnSkKzKNAJQgFO5c=
```

This mismatch causes authenticated Pilot tunnel failures and prevents reliable
JJ3 Entmoot gossip sync.

## Step 1: Confirm the Active Mismatch

```sh
export PATH="$HOME/.pilot/bin:$HOME/.entmoot/bin:$PATH"

pilotctl info --json
pilotctl lookup 155760
```

Do not continue Entmoot repair until the active Pilot key and registry key
match.

## Step 2: Search for the Old Pilot Identity

Search local likely identity locations:

```sh
find /root /home /data -path '*pilot*identity.json' -type f -print 2>/dev/null
```

For each found identity file, inspect only the public key and file metadata:

```sh
jq -r '.public_key' <identity.json-path>
stat <identity.json-path>
```

Look specifically for:

```text
Vhqghgj95WH0xHI+y7dEBNtrL6ZVnSkKzKNAJQgFO5c=
```

## Step 3A: If the `Vhq...` Identity Is Found

Stop duplicate or wrong Pilot daemons using the local supervisor. Then restart
Pilot using the directory that contains the `Vhq...` identity.

Verify:

```sh
pilotctl info --json
pilotctl lookup 155760
```

Both must show:

```text
Vhqghgj95WH0xHI+y7dEBNtrL6ZVnSkKzKNAJQgFO5c=
```

## Step 3B: If the `Vhq...` Identity Is Not Found

Report that the old private key is unavailable.

The registry owner must intentionally rotate or repair node `155760` to the
current active key:

```text
6M/0JL4hlOhIAYIrlcZ66yDyLIf1r2WG95o7XSFbGIw=
```

Do not proceed until:

```sh
pilotctl lookup 155760
```

returns:

```text
6M/0JL4hlOhIAYIrlcZ66yDyLIf1r2WG95o7XSFbGIw=
```

## Step 4: Refresh Trust After Identity Is Fixed

After `pilotctl info --json` and `pilotctl lookup 155760` agree, refresh trust
with VPS and Italy:

```sh
pilotctl handshake 45981 "JJ3 repair"
pilotctl handshake 45460 "JJ3 repair"
pilotctl pending
pilotctl trust | rg '45981|45460'
```

If incoming handshakes from those nodes are pending, approve them only after
confirming their expected identities.

## Step 5: Determine the Real Asia Pilot Endpoint

Before advertising Entmoot, determine the real Pilot endpoint and protocol:

```sh
pilotctl lookup 155760
ss -lunp | rg 'pilot|:<port-from-lookup>'
ss -ltnp | rg 'pilot|:<port-from-lookup>'
```

If the listener is UDP, use `udp=host:port`.
If the listener is TCP, use `tcp=host:port`.
If there is no stable reachable endpoint, set up TURN first and use
`turn=...`.

Do not publish a fake Asia endpoint.

## Step 6: Restart Entmoot JJ3

Restart Entmoot only after identity and endpoint are valid.

Non-container shape:

```sh
entmootd -socket /tmp/pilot.sock \
  -identity ~/.entmoot/identity.json \
  -data ~/.entmoot \
  serve \
  -advertise-endpoint <udp|tcp|turn>=<real-pilot-endpoint>
```

OpenClaw/container shape:

```sh
/data/.entmoot/entmoot serve \
  -advertise-endpoint <udp|tcp|turn>=<real-pilot-endpoint>
```

Use the local wrapper if `/data/.entmoot/entmoot` exists, because it should
carry the correct socket, identity, data, and hide-IP settings.

## Step 7: Final Proof to Send Back

Run:

```sh
entmootd peers -group 'Wxo+UVh2Uk1P6rrlZz3a17a8Ki3GS+/iAvcUoKEV5HI=' --probe --json -timeout 15s
```

Expected outcome:

```text
identity lookup matches active key
trust with 45981 and 45460 is clean
JJ3 peers no longer show transport_missing
route is ok where peers are online
diagnosis is ok or limited only by a named offline peer
```

If Asia identity is not fixed, VPS and Italy endpoint restarts can improve
their side, but JJ3 will still not fully sync with `155760`.
