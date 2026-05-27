---
title: Search UI QA
---

Use this checklist after changing moot message search in Entmoot core, the iOS
app, or entmoot-web.

## Public Web Proxy

Mars Hub is the standard public smoke moot for web search:

```sh
GROUP_ID_ENC='MPuqSRv6ImBjx%2B0KZvTJv%2F5%2BrtQYOg6XzWetn2dBpIY%3D'
curl -fsS "https://entmoot.xyz/api/entmoot/explore/groups/search?groupId=${GROUP_ID_ENC}&q=mars&limit=5"
```

Expected signal: the response is JSON with a `results` array. Each hit contains
`message.content`; `snippet` may be `null`, and clients must fall back to the
message content when it is missing.

Verify cursor pagination without using a visible load-more control:

```sh
GROUP_ID_ENC='MPuqSRv6ImBjx%2B0KZvTJv%2F5%2BrtQYOg6XzWetn2dBpIY%3D'
FIRST_PAGE="$(curl -fsS "https://entmoot.xyz/api/entmoot/explore/groups/search?groupId=${GROUP_ID_ENC}&q=mars&limit=2")"
CURSOR="$(printf '%s' "$FIRST_PAGE" | node -e "let s='';process.stdin.on('data',d=>s+=d);process.stdin.on('end',()=>process.stdout.write(JSON.parse(s).nextCursor || ''))")"
CURSOR_ENC="$(node -e "process.stdout.write(encodeURIComponent(process.argv[1] || ''))" "$CURSOR")"
test -n "$CURSOR_ENC" && curl -fsS "https://entmoot.xyz/api/entmoot/explore/groups/search?groupId=${GROUP_ID_ENC}&q=mars&limit=2&cursor=${CURSOR_ENC}"
```

Expected signal: if the first page has `hasMoreOlder: true`, the cursor request
returns another JSON page. Search cursors are independent from normal history
cursors.

## Direct ESP

Run the same query against the ESP when validating whether a web issue is in the
proxy or the ESP itself:

```sh
GROUP_ID_ENC='MPuqSRv6ImBjx%2B0KZvTJv%2F5%2BrtQYOg6XzWetn2dBpIY%3D'
curl -fsS -H "Authorization: Bearer $ENTMOOT_ESP_TOKEN" \
  "https://esp.entmoot.xyz/v1/groups/${GROUP_ID_ENC}/search?q=mars&limit=5&client_id=web-qa"
```

Expected signal: the direct ESP shape is `results[].message` plus an optional
`results[].snippet`. The web proxy maps that response to the Explore DTO shape.

## entmoot-web UI

1. Open `https://entmoot.xyz/explore/MPuqSRv6ImBjx%2B0KZvTJv%2F5%2BrtQYOg6XzWetn2dBpIY%3D`.
2. Enter `mars` in the header search field.
3. Confirm the feed switches to matching messages and the label says matching
   messages.
4. Scroll down to load older search-result pages.
5. Clear the search field and confirm the normal feed returns with its normal
   history pagination.
6. Open `Task8 Directory-Only Smoke` from `/explore` and confirm it shows the
   descriptor/info view rather than message search.

## iOS App

On macOS, run the app test workflow from the app checkout:

```sh
cd <entmoot-app>/ios
xcodegen generate
xcodebuild -project Entmoot.xcodeproj -scheme Entmoot -destination 'platform=iOS Simulator,name=iPhone 16 Pro' CODE_SIGNING_ALLOWED=NO test
```

Manual signals:

- Open a joined moot with message history.
- Search for a term that is not currently visible in the loaded feed.
- Confirm results load from ESP search and not local-only filtering.
- Scroll to the search pagination edge to fetch older matches.
- Change or retry the same query after a transient error and confirm stale
  errors clear.
- Clear the query and confirm normal feed history is still available.

## Runtime Health

Before and after a production web deploy:

```sh
entmootd version
systemctl is-active entmoot-join.service entmoot-esp.service
curl -fsS https://esp.entmoot.xyz/healthz
curl -fsS https://entmoot.xyz/api/entmoot/explore/capabilities
```

Expected signal: Entmoot is on the intended release, both services are active,
the ESP health endpoint returns success, and the web proxy returns JSON
capabilities.
