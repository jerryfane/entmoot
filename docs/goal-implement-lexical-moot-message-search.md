# Goal Prompt: Implement Lexical Moot Message Search

Use this prompt with `/goal` to implement first-class lexical full-text search
for moot messages across Entmoot core, ESP HTTP APIs, the iOS app, and
entmoot-web public Explore.

This goal intentionally follows the same operating style as
`docs/goal-pr-task-workflow.md`, `docs/goal-the-ent-moot-default-public-moot.md`,
`docs/goal-public-moot-directory-and-policies.md`, and
`docs/goal-implement-public-moot-directory-and-policies.md`: task-by-task
branches, one PR per dependent task, review-fix loops, current contract
verification, release gates, deployment gates, and real runtime validation where
the change affects running agents, apps, or public infrastructure.

Treat the lexical-search plan from the planning turn as the design plan and
this file as the executable goal prompt.

```text
Implement lexical full-text search for moot messages task by task.

Primary objective:
- Agents and mobile/web users can search message text inside one moot.
- Search works across historical messages without requiring the client to first
  page through all history.
- Search is lexical only in this goal: no semantic embeddings, no Turbovec, no
  vector index, and no external model dependency.
- Normal history pagination remains unchanged.
- Search state remains separate from normal loaded chat history in app/web
  clients.

Product rules:
- Search is per moot for v1. Do not add global cross-moot search in this goal.
- Search indexes message content only. Topic is an optional filter, not part of
  query semantics.
- Results are newest-first for stable chat UX and cursor pagination.
- Search must require the same group read authorization as `/history`.
- Search must not expose private/unjoined group messages through public
  directory listings.
- Search must not mutate mailbox cursors, read state, group membership, policy,
  invites, or live replies.
- Search should return enough message metadata to render existing message cards.
- Search snippets are useful but must not become a separate source of truth; the
  canonical message remains the signed message returned with the result.

Architecture decision:
- Use SQLite FTS5 for the primary index because Entmoot already stores messages
  in per-group `messages.sqlite` files through `modernc.org/sqlite`.
- The current `modernc.org/sqlite` build supports FTS5; still verify this with
  a local runtime probe before implementation.
- Do not add Bleve or another index library unless FTS5 is proven unavailable in
  the target build.
- Do not expand `MessageStore` directly. Add an optional `MessageSearcher`
  interface plus store-level helper/fallback so existing stores remain
  compatible.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. Extract small reusable helpers for repeated search
  query normalization, FTS query building, cursor encoding/decoding, message
  DTO mapping, API path encoding, Swift app state, web store pagination, and
  test fixtures when that matches existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, Xcode DerivedData, `.xcuserdata`, or large
  runtime dumps unless the task explicitly says they are intended tracked
  fixtures.
- Never print, paste, commit, or summarize credentials, private keys, tokens, or
  live secrets.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes SQLite
  FTS5 behavior, `modernc.org/sqlite` behavior, ESP HTTP behavior, iOS SwiftUI
  search/scroll APIs, entmoot-web proxy behavior, GitHub CLI actions, release
  tooling, service launchers, and deployment paths.

Fresh-session and resume preflight:
1. Inspect current repo state for every repo that may be touched:
   - `/root/entmoot`
   - `/root/entmoot/repos/entmoot-app` when iOS work begins
   - `/root/entmoot-web` when web work begins
   - `git status --short --branch`
   - current branch
   - current remotes
   - latest relevant local and remote commits
   - latest tags/releases if a release may be needed
2. If there are uncommitted changes, identify whether they belong to the current
   task. Do not overwrite or revert unrelated user changes. Stop and ask only
   if dirty state makes task commits ambiguous.
3. Verify which previous tasks, PRs, and commits are already merged before
   skipping any task. Do not rely only on memory.
4. If resuming from an interrupted branch, finish the active task first unless
   the branch is clearly abandoned or the user explicitly redirects.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - the remote resolves to the expected GitHub repository.
6. Inspect and follow current Entmoot patterns before editing:
   - `docs/goal-pr-task-workflow.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - `src/go.mod`
   - `src/pkg/entmoot/store/store.go`
   - `src/pkg/entmoot/store/sqlite.go`
   - `src/pkg/entmoot/store/memory.go`
   - `src/pkg/entmoot/store/jsonl.go`
   - `src/pkg/entmoot/mailbox/sync.go`
   - `src/pkg/entmoot/esphttp/handler.go`
   - existing store, mailbox, ESP handler, cursor, and history tests
7. Before touching iOS, inspect:
   - `/root/entmoot/repos/entmoot-app/ios/project.yml`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPModels.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPClient.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPAppModel.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/App/ContentView.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPModelsTests.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPAppModelTests.swift`
8. Before touching entmoot-web, inspect:
   - `/root/entmoot-web/server/src/entmoot`
   - `/root/entmoot-web/server/test`
   - `/root/entmoot-web/frontend/src/stores/explore.ts`
   - `/root/entmoot-web/frontend/src/pages`
   - `/root/entmoot-web/frontend/src/components`
   - `/root/entmoot-web/ecosystem.config.js`
9. Verify current external guidance before implementation:
   - SQLite FTS5: https://www.sqlite.org/fts5.html
   - modernc.org/sqlite: https://pkg.go.dev/modernc.org/sqlite
   - Apple SwiftUI `.searchable` and iOS 18 scroll APIs where relevant
   - Vue/Nest patterns already used by entmoot-web

Current known implementation facts to verify:
- Entmoot core module is under `/root/entmoot/src`.
- `modernc.org/sqlite` is already a dependency.
- The SQLite message store keeps one `messages.sqlite` database per group.
- ESP history route is `GET /v1/groups/{group_id}/history`.
- iOS uses `ESPClient.groupHistory(...)` and `ESPAppModel.messagesByGroup`.
- iOS branch recently raised the app target to iOS 18 for clean scroll APIs.
- entmoot-web proxies public Explore message history through
  `/api/entmoot/explore/groups/messages`.

Task 1: Core search types and store-level search interface
1. Add store-level search types in `pkg/entmoot/store`:
   - `SearchOptions` with `Limit`, optional cursor boundary, optional topic.
   - `SearchHit` with `Message` and optional `Snippet`.
   - `SearchResult` with `Hits`, `HasMore`, and optional next cursor boundary.
   - `SearchBoundary` with timestamp, author node id, message id, and any rank
     fields needed for stable newest-first pagination.
2. Add optional interface `MessageSearcher` implemented by stores that can do
   indexed search.
3. Add helper `SearchMessages(ctx, st, groupID, query, opts)`:
   - uses `MessageSearcher` when available;
   - falls back to a simple scan for `Memory` and `JSONL`.
4. Add query normalization and validation helpers:
   - trim whitespace;
   - cap query length to 256 characters;
   - reject empty queries;
   - tokenize to safe terms;
   - build safe FTS5 `MATCH` syntax without passing raw user input directly.
5. Add focused tests for query normalization, invalid queries, fallback search,
   newest-first ordering, topic filtering, and pagination boundary behavior.
6. Run focused store tests from `/root/entmoot/src`.

Task 2: SQLite FTS5 message index
1. Verify FTS5 support locally before editing:
   - create an in-memory SQLite FTS5 table with `modernc.org/sqlite`;
   - insert one row;
   - run a `MATCH` query.
2. Extend the per-group SQLite schema with:
   - `message_search_docs` for integer `doc_id`, message id, group id, author,
     timestamp, content text, and topics text;
   - `message_search_fts` using FTS5 over content and backed by
     `message_search_docs`.
3. Use SQLite triggers on `message_search_docs` to keep FTS entries synchronized
   on insert/update/delete.
4. On `Put`, index the searchable content in the same transaction after the
   message row insert. Duplicate `Put` must not duplicate search hits.
5. Add an idempotent backfill on group database open:
   - find messages missing `message_search_docs`;
   - decode `canonical_bytes`;
   - insert search docs using the same helper used by `Put`.
6. Update prune paths so deleted messages also delete search docs and FTS rows.
7. Implement `SQLite.SearchMessages` with newest-first ordering and stable
   cursor pagination.
8. Add SQLite tests for exact term search, multi-term AND search, topic filter,
   duplicate put, prune removal, reopen/backfill, snippets, and cursor paging.
9. Run `go test ./pkg/entmoot/store` from `/root/entmoot/src`.

Task 3: Mailbox service and ESP HTTP search API
1. Add mailbox/service method:
   - `Search(ctx, groupID, query, limit, cursor, topic)`.
2. Add HTTP route:
   - `GET /v1/groups/{group_id}/search?q=...&limit=50&cursor=...&topic=...&client_id=...`
3. Reuse the same read authorization and client-id fallback behavior as
   `/v1/groups/{group_id}/history`.
4. Response shape:
   - `group_id`;
   - `query`;
   - `count`;
   - `has_more`;
   - `next_cursor`;
   - `results`: array of `{ message, snippet }`.
5. Cursor payload must bind to:
   - version;
   - group id;
   - normalized query hash;
   - topic;
   - timestamp;
   - author node id;
   - message id.
6. Reject cursors whose group, topic, or query hash no longer match.
7. Enforce existing `maxListLimit`, defaulting to 50.
8. Add HTTP tests for auth, empty/bad query, escaped group ids, cursor paging,
   cursor mismatch rejection, topic filtering, and unchanged history behavior.
9. Update ESP HTTP reference docs.
10. Run focused ESP/mailbox/store tests and broader `go test ./...` when shared
    API behavior is complete.

Task 4: iOS client contract and app model state
1. In `/root/entmoot/repos/entmoot-app`, add Codable models:
   - `ESPGroupSearchResponse`;
   - `ESPGroupSearchHit`;
   - any search page-state type needed by the app model.
2. Add `ESPClient.searchGroupMessages(groupID:query:limit:cursor:topic:)`:
   - use existing percent-encoded group path helper;
   - include the authenticated device/client id as needed by current history
     conventions.
3. Extend `ESPAppModel` with search state separate from normal history:
   - results by group id plus normalized query/topic key;
   - page state by key;
   - loading/error state by key.
4. Add app-model actions:
   - start/refresh search;
   - load next search page;
   - clear search state for a group/query.
5. Do not mutate `messagesByGroup` when searching.
6. Add tests for URL encoding, response decoding, state separation from normal
   history, pagination, empty-query clearing, and failed search error handling.
7. On a Mac/Xcode host, run XcodeGen, build, and focused iOS tests. On Linux,
   document that iOS build cannot run and provide exact Mac validation commands.

Task 5: iOS moot feed search UI
1. Add a compact search field to the Feed tab in `MootDetailView`.
2. Use existing app styling and reusable search-field patterns where practical.
3. Debounce query changes by about 300 ms.
4. Empty query shows the normal live/history feed.
5. Non-empty query shows search results newest-first using existing `FeedCard`
   styling, preferring snippet text when present.
6. Add scroll-to-load-more for search results using the same iOS 18 scroll
   approach used by feed history pagination.
7. Do not add a visible "load more" button.
8. Preserve author display-name resolution for search results.
9. Add clear, loading, empty, and error states that fit the existing UI without
   creating a marketing-style page.
10. Re-run iOS tests/build where available.

Task 6: entmoot-web proxy and Explore search UI
1. In `/root/entmoot-web`, add server proxy endpoint:
   - `GET /api/entmoot/explore/groups/search?groupId=...&q=...&limit=...&cursor=...&topic=...`
2. Forward to ESP:
   - `GET /v1/groups/{group_id}/search`.
3. Add DTO mapping that reuses existing message mapping helpers where possible.
4. Add frontend store state separate from normal message history:
   - search results by group id + topic + query;
   - loading/error/page state;
   - next-cursor handling.
5. In public moot detail/feed UI, add message search inside the selected moot
   feed. Do not confuse it with public moot directory search.
6. Empty query shows normal feed. Non-empty query shows search results.
7. Keep normal history pagination untouched.
8. Add server tests for proxy path/query encoding and frontend/store tests for
   separate search/history state.
9. Run entmoot-web server tests, frontend tests/build, and public proxy smoke
   checks where available.

Task 7: Documentation, release, deployment, and live validation
1. Update Entmoot docs:
   - ESP HTTP reference for `/v1/groups/{group_id}/search`;
   - README or relevant usage docs with CLI/API examples;
   - operator notes for index rebuild/backfill behavior.
2. Update iOS and entmoot-web user-facing docs only where they already document
   Explore/feed behavior.
3. Run final review:
   - `codex exec review --uncommitted` in each changed repo;
   - resolve findings before committing.
4. Commit, push, open PRs, merge in dependency order:
   - Entmoot core first;
   - iOS app after core API contract;
   - entmoot-web after core API contract.
5. If core changes require a release:
   - cut a new Entmoot release following existing release docs/tooling;
   - update the live ESP to the released version;
   - restart/reload services safely;
   - verify `/v1/status`.
6. If entmoot-web changes are merged:
   - pull latest on the server;
   - rebuild with the current production env;
   - restart the correct PM2 process;
   - `pm2 save`;
   - verify public routes.
7. Live validation:
   - search Mars Hub for a known historical term;
   - verify results can include messages older than the currently loaded first
     history page;
   - verify iOS search does not require scrolling all history first;
   - verify entmoot-web search does not expose unavailable private history;
   - verify normal history pagination still works.

Acceptance criteria:
- A user can search text inside a moot from iOS.
- A public Explore user can search text inside a moot when message history is
  available.
- API search works with base64 group ids containing `/`, `+`, and `=`.
- Search returns stable newest-first pages with cursors.
- Existing history/topic/mailbox APIs continue to pass tests.
- No semantic/vector-search dependency is introduced.
- No secrets or runtime data are committed.
```
