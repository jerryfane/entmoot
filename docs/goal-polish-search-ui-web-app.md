# Goal Prompt: Polish and Validate Moot Search UI in Web and App

Use this prompt with `/goal` to finish the user-facing moot message search UI
across the iOS app and entmoot-web.

Important current-state note:
- Lexical message search has already been implemented and released in Entmoot
  core as `v1.5.80`.
- The current checkouts already contain iOS and web search UI code. Treat that
  as the baseline to verify, polish, and harden. Do not duplicate the already
  shipped implementation.

This goal follows the same operating style as `docs/goal-pr-task-workflow.md`,
`docs/goal-implement-lexical-moot-message-search.md`,
`docs/goal-public-moot-directory-and-policies.md`, and
`docs/goal-implement-public-moot-directory-and-policies.md`: task-by-task
branches, one PR per dependent task, review-fix loops, current contract
verification, clean commits, release/deploy gates when needed, and real runtime
validation where changes affect running agents, apps, or public infrastructure.

Treat this file as the executable goal prompt.

```text
Polish and validate the moot message search UI in the iOS app and entmoot-web
task by task.

Primary objective:
- iOS app users can search message text inside the currently open moot.
- entmoot-web Explore users can search message text inside a public moot when
  message history is available.
- Search works against historical ESP search results, not only currently loaded
  messages.
- Empty search returns to the normal feed without losing normal history state.
- Scrolling, not a visible "load more" button, loads older search result pages.
- App and web UX are consistent enough that agents/users understand they are
  searching message history inside one moot.

Out of scope:
- Do not reimplement Entmoot core search indexing or ESP `/v1/groups/{group_id}/search`.
- Do not add semantic/vector search.
- Do not add global cross-moot search.
- Do not expose private/unavailable history through public directory pages.
- Do not re-enable Fleet/task UI.
- Do not add a visible "Load older messages" button.

Product rules:
- Search is per moot for this goal.
- Search state must remain separate from normal feed history state.
- Topic filters and search queries must compose correctly.
- Results should prefer ESP snippets when present, but the signed message stays
  the source of truth.
- Results may be newest-first. If a feed component groups adjacent messages, it
  must handle newest-first and oldest-first rows correctly.
- Search errors must be recoverable without requiring an app restart or page
  reload.
- Directory-only public moots must show descriptor/info UI, not message search.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. Reuse or extract small helpers for search keys,
  debounced search execution, cursor pagination, feed-row mapping, snippet
  rendering, path/query encoding, and test fixtures when repeated logic appears.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, Xcode DerivedData, `.xcuserdata`, session
  archives, or large runtime dumps unless the task explicitly says they are
  intended tracked fixtures.
- Never print, paste, commit, or summarize credentials, private keys, tokens, or
  live secrets.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes Apple
  SwiftUI `.searchable`/scroll APIs, iOS deployment target behavior, Vue/Pinia
  patterns, Nest controller/query handling, Vite build/deploy behavior, GitHub
  CLI actions, PM2 process definitions, and live ESP/web routes.

Fresh-session and resume preflight:
1. Inspect current repo state for every repo that may be touched:
   - `/root/entmoot`
   - `/root/entmoot/repos/entmoot-app`
   - `/root/entmoot-web`
   - `git status --short --branch`
   - current branch
   - current remote
   - latest relevant local and remote commits
2. If there are uncommitted changes, identify whether they belong to the current
   task. Do not overwrite or revert unrelated user changes. Stop and ask only if
   dirty state makes task commits ambiguous.
3. Verify what is already implemented before adding code. Do not rely only on
   memory. Inspect at least:
   - `/root/entmoot/docs/goal-implement-lexical-moot-message-search.md`
   - `/root/entmoot/README.md`
   - `/root/entmoot/website/docs/reference/http-esp-api.md`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPClient.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPAppModel.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/App/ContentView.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPModelsTests.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPAppModelTests.swift`
   - `/root/entmoot-web/server/src/entmoot`
   - `/root/entmoot-web/server/test`
   - `/root/entmoot-web/frontend/src/api/entmoot.ts`
   - `/root/entmoot-web/frontend/src/stores/explore.ts`
   - `/root/entmoot-web/frontend/src/pages/GroupPage.vue`
   - `/root/entmoot-web/frontend/src/components/entmoot/MootMessageFeed.vue`
4. Verify PR tooling before the first PR:
   - `gh auth status`
   - each repo remote resolves to the expected GitHub repository.
5. Verify current public runtime before and after any deploy:
   - `entmootd version`
   - `systemctl is-active entmoot-join.service entmoot-esp.service`
   - `curl -fsS https://esp.entmoot.xyz/healthz`
   - `curl -fsS https://entmoot.xyz/api/entmoot/explore/capabilities`

Current known implementation facts to verify:
- Entmoot core `v1.5.80` exposes ESP lexical search.
- iOS has `ESPClient.searchGroupMessages(...)`, app-model search state, and a
  feed search field in `ContentView.swift`.
- entmoot-web has `/api/entmoot/explore/groups/search`, Explore store search
  state, and feed search UI in `GroupPage.vue`.
- Live Mars Hub search has previously worked for `q=mars` through both direct
  ESP and entmoot-web proxy.

Task 1: iOS search UI audit, polish, and regression coverage
1. In `/root/entmoot/repos/entmoot-app`, audit the existing feed search UI and
   app-model search state.
2. Confirm the UI behavior:
   - search field is visible only where message history is available;
   - empty query shows normal feed;
   - non-empty query uses ESP search results, not local-only filtering;
   - scroll-to-load-older search pages works without a visible button;
   - topic/member display names still resolve correctly;
   - error state can recover by retrying or changing the query;
   - clearing search does not destroy normal loaded history.
3. Patch any gaps with small local changes. Prefer existing components and
   styling. Do not redesign the app shell.
4. Add or update tests for:
   - search URL/path encoding;
   - query debounce or request coalescing if testable;
   - empty-query clear behavior;
   - failed search retry/recovery;
   - pagination state separation from normal history;
   - snippet fallback to full message when snippet is missing.
5. On Linux, do not pretend Xcode checks passed. Use GitHub macOS CI or clearly
   report the exact Mac commands required. If CI exists, open/merge the iOS PR
   only after CI passes.
6. Suggested branch: `task1/ios-search-ui-polish`
7. Suggested commit: `Polish iOS moot search UI`

Acceptance criteria:
- iOS search UI behavior is verified against the current model/client contract.
- Any discovered iOS UI/state bugs are fixed.
- iOS tests or CI cover the fixed behavior.

Task 2: entmoot-web search UI audit, polish, and regression coverage
1. In `/root/entmoot-web`, audit the existing web search API, store state, and
   Explore feed UI.
2. Confirm the UI behavior:
   - search field clearly searches message history inside the current moot;
   - directory-only moots do not show message search;
   - topic filters and search query compose correctly;
   - search result pagination is scroll-driven and loads older pages at the
     correct edge;
   - normal history pagination remains unchanged;
   - reverse-chronological search rows do not create incorrect message grouping;
   - count labels do not treat page count as a total match count;
   - failed searches can be retried.
3. Patch any gaps with small local changes. Reuse `EspClientService.mapMessage`,
   existing Explore store helpers, and the current `MootMessageFeed` pagination
   hooks where possible.
4. Add or update tests/checks:
   - server proxy path/query encoding test;
   - store-level search state tests if a frontend test harness exists;
   - otherwise document why frontend unit tests are unavailable and rely on
     `yarn build` plus live/proxy smoke checks.
5. Run:
   - `/root/entmoot-web/server`: `yarn test`, `yarn build`;
   - `/root/entmoot-web/frontend`: `yarn build`;
   - `/root/entmoot-web`: `git diff --check`;
   - `codex exec review --uncommitted`.
6. Suggested branch: `task2/web-search-ui-polish`
7. Suggested commit: `Polish web Explore message search`

Acceptance criteria:
- entmoot-web search UI behavior is verified against the current ESP proxy.
- Any discovered web UI/state bugs are fixed.
- Server tests and frontend build pass.

Task 3: Cross-client consistency docs and manual QA checklist
1. Update docs only where there is an existing appropriate place. Prefer a short
   operator/user QA section over broad documentation churn.
2. Document how to test:
   - iOS app moot search;
   - entmoot-web public moot search;
   - direct ESP search;
   - cursor pagination;
   - normal-history fallback after clearing search.
3. Include exact commands for Mars Hub live search using the public web proxy.
4. Do not duplicate the full ESP HTTP reference unless it is missing current
   behavior.
5. Suggested branch: `task3/search-ui-qa-docs`
6. Suggested commit: `Document moot search UI QA`

Acceptance criteria:
- A future operator can verify the app/web search UI without guessing commands
  or expected signals.
- Docs state known platform limits, especially if iOS build/test requires macOS.

Task 4: Release/deploy and live validation
1. Decide whether any core Entmoot release is required:
   - If only iOS/web UI changed, do not cut a new Entmoot core release.
   - If core ESP/API behavior changed, follow `docs/OPERATIONS.md` and cut the
     next patch release before relying on it in production.
2. For iOS:
   - ensure the final iOS PR is merged;
   - verify GitHub macOS CI if present;
   - state whether a TestFlight/App Store build is still required.
3. For entmoot-web:
   - pull latest on the server;
   - rebuild with production env (`VITE_SERVER_API=/api` unless current deploy
     contract has changed);
   - restart only `entmoot-web-api` and `entmoot-web-frontend`;
   - `pm2 save`;
   - verify PM2 status.
4. Live validation:
   - direct ESP search for Mars Hub `q=mars`;
   - entmoot-web proxy search for Mars Hub `q=mars`;
   - cursor pagination through the web proxy;
   - public Explore page loads the current built asset;
   - services remain healthy:
     `entmootd version`, `systemctl is-active entmoot-join.service
     entmoot-esp.service`, `curl -fsS https://esp.entmoot.xyz/healthz`.
5. Suggested branch/commit:
   - no branch if only deploying already-merged web changes;
   - if deployment docs/scripts change, use `task4/search-ui-release-validation`
     and commit `Validate search UI release path`.

Acceptance criteria:
- Search UI is verified live on web.
- iOS work is merged and CI-verified, or remaining app-store distribution work
  is explicitly stated.
- No unnecessary core release is cut for app/web-only UI changes.

Final response after all tasks:
- List completed tasks.
- For each task, list branch, PR URL, merge status, and merged commit hash.
- List tests/checks run.
- Include exact final raw `codex exec review --uncommitted` output for the last
  task/repo with uncommitted changes.
- List live validation commands and the observed success signals.
- Mention skipped checks, blockers, or residual risk.
- Do not claim interactive `/review` is clean. Say:
  `codex exec review is clean; ready for manual /review.`
```
