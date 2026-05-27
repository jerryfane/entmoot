# Goal Prompt: Implement Search Result Jump To Message Context

You are continuing Entmoot work in `/root/entmoot`.

Implement the feature that lets a user open a search result in its normal moot chat context. When a user searches messages in a moot and selects a matching result, Entmoot must load the target message with surrounding historical context, render the regular conversation view around it, scroll to the exact target message, and highlight it briefly or visibly. This must work even when the message is older than the currently loaded chat window.

Do not implement semantic search in this goal. Do not add a visible "Load older messages" button. The UX should feel like a normal chat app: searching finds messages, selecting a result jumps into that part of the conversation, and upward scrolling remains the way to continue loading older history.

## Ground Rules

- Read the current repo state before editing. Do not assume file names or branch state from memory.
- Preserve existing behavior for message history, lexical search, public moot discovery, policies, skills, and the default social-chat posture.
- Do not re-enable Fleet or Tasks surfaces.
- Keep changes organized around reusable helpers and existing local patterns. Avoid duplicate query, mapping, and client code.
- Treat signed message payloads as the source of truth. Search snippets are for search results only; the context view must render real messages.
- Keep public directory and unauthenticated surfaces separate from private moot history. Do not expose message context for groups where the caller cannot already read history/search.
- Add tests with each task. Do not defer coverage to a later task.
- Use focused branches and PRs, one task at a time, following `docs/goal-pr-task-workflow.md`.
- Run a review pass before each PR is considered done, fix valid findings, then rerun the relevant checks.
- Do not commit secrets, generated local state, or temporary logs.

## Fresh-Session Preflight

1. Confirm the checkout and branch:
   - `pwd`
   - `git status --short --branch`
   - `git fetch --all --prune`
   - `git pull --ff-only` if on the expected base branch and clean.
2. Read these files before planning edits:
   - `docs/goal-pr-task-workflow.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - `src/pkg/entmoot/esphttp/handler.go`
   - `src/pkg/entmoot/esphttp/handler_test.go`
   - `src/pkg/entmoot/mailbox/mailbox.go`
   - `src/pkg/entmoot/mailbox/mailbox_test.go`
   - `src/pkg/entmoot/store/search.go`
   - `src/pkg/entmoot/store/sqlite.go`
   - the current iOS app files under `app/`
   - the current web checkout under `/root/entmoot-web`
3. Re-run quick searches for current symbols before editing:
   - `rg "handleGroupSearch|searchMessages|SearchMessages|history" src app website README.md`
   - `rg "searchExploreMessages|latestGroupMessages|messageId|FeedEvent|searchHit" /root/entmoot-web app`
4. If external framework details are needed, use current official docs only. Prefer local code patterns when they are clear.

## Product Contract

Selecting a search result should:

- Exit or suspend the search-results list for that moot.
- Load a bounded message context window around the selected message.
- Render the normal chat/feed message cards, not a search-result card.
- Scroll to the target message after the context is rendered.
- Highlight the target message so the user can see the landing point.
- Allow normal upward scrolling to fetch older messages from the context window's oldest loaded message.
- Avoid duplicating messages already loaded in normal history.
- Show a clear empty/error state only when the message cannot be found or the caller cannot access it.

Recommended backend API:

`GET /v1/groups/{group_id}/message-context?client_id=<client>&message_id=<message_id>&before=25&after=25&topic=<optional>`

Use `message_id` as a query parameter, not a path segment, because message IDs may contain characters that make path encoding brittle.

Recommended response shape:

```json
{
  "group_id": "group id",
  "message_id": "target message id",
  "topic": "optional topic filter",
  "before": 25,
  "after": 25,
  "messages": [],
  "target_message_id": "target message id",
  "has_more_older": true,
  "older_cursor": "cursor for messages older than the oldest returned message"
}
```

The `messages` array should be sorted oldest-to-newest and must include the target message exactly once.

## Task 1: Core Message Context Store And Mailbox API

Branch: `task/search-message-context-core`

Implement the storage/mailbox primitive for loading context around a target message.

Details:

- Add a small typed options/result API rather than passing raw maps or ad hoc parameters.
- Support `group_id`, `message_id`, optional `topic`, `before`, and `after`.
- Clamp `before` and `after` to sane limits. Suggested defaults: `before=25`, `after=25`, max `100` each.
- Implement efficient SQLite lookup:
  - Find the target row by group and message ID.
  - Apply topic filtering consistently with search/history semantics.
  - Fetch older messages before the target with one extra row for `has_more_older`.
  - Fetch newer messages after the target up to `after`.
  - Compose the final array oldest-to-newest with target included once.
- Implement equivalent behavior for any in-memory/test store used by the repo.
- Reuse existing message decoding/mapping helpers. Do not duplicate signed-message parsing.

Tests:

- Target found with older and newer context.
- Target is the oldest message.
- Target is the newest message.
- Topic filter includes the target.
- Topic filter excludes the target and returns not found.
- Unknown message returns a not-found error.
- Limit clamping works.
- Result ordering is oldest-to-newest.
- `has_more_older` and `older_cursor` point at the correct oldest loaded boundary.

Acceptance:

- Focused store/mailbox tests pass.
- No HTTP, web, or app behavior changes yet.
- PR opened, reviewed, fixed, merged before Task 2.

## Task 2: ESP HTTP Message Context Endpoint

Branch: `task/search-message-context-http`

Expose the core primitive through the ESP HTTP API.

Details:

- Add `GET /v1/groups/{group_id}/message-context`.
- Require the same caller/client checks as history and search.
- Parse `message_id`, `client_id`, optional `topic`, `before`, and `after`.
- Return structured JSON matching the product contract.
- Return consistent errors:
  - `400` for malformed inputs.
  - `404` when the target message is absent or absent under the requested topic.
  - the existing auth/access status for unreadable groups.
- Keep route dispatch readable. Extract helpers if the group subrouter is getting too dense.
- Update API docs:
  - `website/docs/reference/http-esp-api.md`
  - `README.md` if search/history API examples are listed there.

Tests:

- HTTP success response includes target and context.
- Encoded/base64-ish `message_id` works as query input.
- Missing `message_id` fails.
- Unknown target fails with `404`.
- Topic-filtered miss fails with `404`.
- Bounds/defaults are reflected in response.
- Access behavior matches history/search.

Acceptance:

- Focused ESP HTTP tests pass.
- Docs describe the endpoint and response.
- PR opened, reviewed, fixed, merged before Task 3.

## Task 3: entmoot-web Proxy And Browser UI Jump

Branch: `task/search-result-jump-web`

Implement web support in `/root/entmoot-web`.

Details:

- Pull latest `entmoot-web` before editing and confirm its current branch/status.
- Add an ESP client method for message context.
- Add a server controller route under the existing Entmoot API proxy. Keep naming consistent with existing explore group routes.
- Add frontend API/store methods for opening message context.
- Keep normal history state and search-result state cleanly separated.
- When a user selects a search result:
  - request message context,
  - render normal message cards,
  - scroll to the target after render,
  - apply a target highlight,
  - allow upward scroll to continue from `older_cursor`.
- If the target message is already loaded in the normal feed, prefer scrolling/highlighting it without an unnecessary network request when practical.
- Do not add a visible load-older button.
- Preserve current design language. Use existing components/icons/helpers rather than introducing a new UI system.

Tests and checks:

- Add or update focused server/client tests if the repo has them.
- Run the existing web lint/build/test commands available in the repo.
- Manually smoke test with a local or live ESP group containing older messages.

Acceptance:

- Searching and selecting an older message lands on that message in context.
- Existing latest-feed loading still works.
- Public directory views do not gain private history access.
- PR opened, reviewed, fixed, merged before Task 4.

## Task 4: iOS App Search Result Jump

Branch: `task/search-result-jump-ios`

Implement iOS support in the Entmoot app code.

Details:

- Inspect the current deployment target and scroll implementation before choosing APIs.
- Add the message-context client models/method using existing networking conventions.
- Add app state for a context feed around a selected message.
- Wire search result selection to load context, show regular feed cards, scroll to the target, and highlight it.
- Keep search mode and context mode explicit in model state so clearing search or switching moots does not leave stale highlighted messages.
- Reuse existing feed rendering and message identity helpers.
- Avoid visible "load older" controls. Existing upward scrolling should continue to fetch older history from the context boundary.
- Handle loading, not found, and network error states without replacing the whole chat with a dead end.

Tests and checks:

- Add focused model/client tests where the app test structure supports them.
- Run the app's available test/build command.
- If simulator validation is practical in the environment, smoke test selecting a search hit older than the currently loaded window.

Acceptance:

- iOS search result tap opens the target in chat context.
- Target is highlighted and visible after the jump.
- Regular history scrolling still works.
- PR opened, reviewed, fixed, merged before Task 5.

## Task 5: Release, Deploy, And Live Validation

Branch: `task/search-result-jump-release`

Cut and deploy the pieces needed for the feature to work end to end.

Details:

- Because Task 2 adds an ESP API endpoint, cut a new Entmoot core release after merged tests are green.
- Update the production ESP service to the new version.
- Update and deploy `entmoot-web` after its PR is merged.
- Confirm the iOS app branch/build status and note whether App Store/TestFlight release steps remain manual.
- Keep deployment scoped to Entmoot services only.

Live validation:

- Use a moot with enough history, such as Mars Hub or another test moot.
- Search for a known older message.
- Select it in web and verify:
  - context endpoint returns the target,
  - browser shows normal chat cards,
  - target is highlighted,
  - older scrolling continues.
- Repeat equivalent validation on iOS if a simulator/device is available.
- Verify no regression in:
  - latest history load,
  - lexical search results,
  - public moot directory,
  - default moot descriptor/status,
  - Fleet/Tasks disabled surfaces.

Acceptance:

- Release version recorded.
- Production ESP version verified.
- Production web deployment verified.
- Remaining app distribution step, if any, is explicitly stated.

## Review Loop

For each task:

1. Implement only that task's scope.
2. Run focused tests.
3. Run broader checks that are reasonable for the changed surface.
4. Run `codex exec review --uncommitted` or the repo's standard review command.
5. Fix valid findings.
6. Re-run the relevant checks.
7. Commit with a clear message.
8. Push the branch.
9. Open the PR.
10. Merge only after the task meets acceptance criteria.

## Final Report

When the full goal is complete, report:

- PRs and commits shipped.
- Release version and deployment status.
- Exact tests/checks run.
- Live validation results.
- Any remaining manual app release step or known limitation.
