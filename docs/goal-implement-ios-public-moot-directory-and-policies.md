# Goal Prompt: Implement iOS Public Moot Directory and Policies

Use this prompt with `/goal` to implement iOS support for public moot discovery,
public moot publishing, and founder-managed default/custom policies.

This goal follows the same operating style as `docs/goal-pr-task-workflow.md`,
`docs/goal-the-ent-moot-default-public-moot.md`,
`docs/goal-public-moot-directory-and-policies.md`, and
`docs/goal-implement-public-moot-directory-and-policies.md`: task-by-task
branches, one PR per dependent task, review-fix loops, current contract
verification, clean commits, and real runtime validation where changes affect
running Entmoot agents or public infrastructure.

Treat this file as the executable goal prompt. The implementation scope spans:

- `/root/entmoot` for any missing core ESP/mobile API needed by the app.
- `/root/entmoot/repos/entmoot-app` for the iOS app implementation.

```text
Implement iOS support for public moot discovery, public listing publish/refresh,
and founder-managed default/custom policies task by task.

Primary objective:
- iOS users can discover public moots from the default ESP directory.
- Public directory visibility remains separate from membership.
- Open invite remains separate from public listing.
- Founders can create moots with visibility, join mode, and policy choices.
- Founders can inspect and update policy from the app.
- Founders can publish/refresh a public moot listing from the app.
- The phone never manufactures founder descriptor signatures itself; the local
  Entmoot daemon/core signs descriptors and policy updates after device
  authorization.

Product rules:
- `visibility=public` means eligible for directory listing.
- `join_mode=open_invite` means anyone with the descriptor/link can join.
- Do not make public imply open invite.
- Do not make open invite imply public listing.
- Public listing does not make the ESP a group member.
- Directory indexing does not imply message/history indexing.
- Show message history only when the ESP reports it is available.
- Do not silently enable live replies in any moot. Live participation remains a
  separate owner decision.
- Do not expose operator-only directory moderation controls in the iOS app.
- In v1, support publish/refresh. Do not add founder unpublish unless core gets
  a signed tombstone/delist contract in this goal.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized.
- Avoid code duplication. Extract reusable helpers for repeated policy parsing,
  policy summaries, descriptor building/signing/publishing, metadata
  normalization, visibility/join-mode validation, ESP DTO mapping, API path
  encoding, SwiftUI settings rows/forms, and test fixtures when that matches
  existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, Xcode DerivedData, `.xcuserdata`, or
  large runtime dumps unless the task explicitly says they are intended tracked
  fixtures.
- Never print, paste, commit, or summarize credentials, private keys, tokens, or
  live secrets.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes Apple
  SwiftUI APIs, iOS Human Interface Guidelines, ESP HTTP behavior, descriptor
  formats, signing/canonicalization, GitHub CLI actions, release tooling,
  service launchers, and third-party libraries.

Fresh-session and resume preflight:
1. Inspect current repo state for every repo that may be touched:
   - `/root/entmoot`
   - `/root/entmoot/repos/entmoot-app`
   - `git status --short --branch`
   - current branch
   - current remotes
   - latest relevant local and remote commits
   - latest tags/releases if a release may be needed
2. If there are uncommitted changes, identify whether they belong to the current
   task. Do not overwrite or revert unrelated user changes. Stop and ask only if
   dirty state makes task commits ambiguous.
3. Verify which prior public-moot/policy tasks, PRs, and commits are already
   merged before skipping any core work. Do not rely only on memory.
4. If resuming from an interrupted branch, finish the active task first unless
   the branch is clearly abandoned or the user explicitly redirects.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - the remote resolves to the expected GitHub repository.
6. Inspect and follow current repo patterns before editing:
   - `docs/goal-pr-task-workflow.md`
   - `docs/goal-public-moot-directory-and-policies.md`
   - `docs/goal-implement-public-moot-directory-and-policies.md`
   - `docs/OPERATIONS.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - existing `group`, `esp`, `policy`, `publicmoot`, `open-invite`,
     descriptor, sign-request, and release code
7. Inspect and follow current iOS app patterns before editing:
   - `/root/entmoot/repos/entmoot-app/ios/project.yml`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPModels.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPClient.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPAppModel.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/App/ContentView.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPModelsTests.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPAppModelTests.swift`
8. Verify current Apple guidance before UI implementation. Prefer official
   Apple docs/HIG for `TabView`, `NavigationStack`, `List`, `.searchable`,
   `.refreshable`, forms, pickers, toggles, and share/copy affordances.

Current known implementation facts to verify:
- Core already has public directory endpoints:
  - `GET /v1/public-moots`
  - `GET /v1/public-moots/{group_id}`
  - `POST /v1/public-moots`
  - operator-only `PATCH /v1/public-moots/{group_id}/index-status`
- Core ESP group create accepts `visibility` and `join_mode` in the payload.
- The Swift client currently does not send `visibility` or `join_mode`.
- The Swift app already has ESP auth, sign-request completion, moot creation,
  metadata editing, member/invite flows, and open-invite accept.
- The Swift app does not yet have public directory models, policy models,
  policy endpoints, public publish endpoints, or public directory UI.
- Public descriptor signing must remain daemon/core-owned because it uses the
  founder Entmoot identity.

Definitions:
- `visibility=private`: not listed, invite-only by default.
- `visibility=unlisted`: not listed, but joinable if a descriptor/link is
  shared.
- `visibility=public`: eligible for default ESP/entmoot.xyz directory listing.
- `join_mode=invite_only`: public page can exist, but joining still requires an
  invite.
- `join_mode=open_invite`: descriptor may include an open-invite descriptor or
  link.
- `mirror_state=none`: ESP indexes descriptor only; it is not a group member.
- `mirror_state=member`: ESP is a member and can expose member-scoped data.
- `mirror_state=hosted`: ESP intentionally hosts or mirrors messages.

Task 1: Core mobile API gaps for policies and public publish
1. In `/root/entmoot`, add or confirm the ESP/mobile APIs the iOS app needs.
2. Extend signed ESP `group_create` behavior so mobile-created moots match CLI
   defaults:
   - default `visibility=private`;
   - default `join_mode=invite_only`;
   - default `policy=preset:standard`.
3. Allow mobile `group_create` payloads to include:
   - `visibility`;
   - `join_mode`;
   - policy selection: `preset:standard`, `preset:relaxed`, `none`, or custom
     policy JSON.
4. If `join_mode=open_invite`, reuse existing core open-invite helpers and
   persist the open-invite descriptor in group metadata when the runtime can
   issue it safely. If it cannot, return a clear error and leave group state
   consistent.
5. Add signed group policy APIs:
   - `GET /v1/groups/{group_id}/policy`;
   - `PUT /v1/groups/{group_id}/policy`;
   - `DELETE /v1/groups/{group_id}/policy` or an equivalent explicit clear
     request if that better matches current ESP patterns.
6. Policy status output must match the CLI report shape closely:
   - group id;
   - policy configured flag;
   - effective mode;
   - policy object when configured;
   - policy summary;
   - source/preset when known;
   - sequence/update metadata when known;
   - publish/runtime status when known.
7. Add signed public publish API:
   - `POST /v1/groups/{group_id}/public-moot/publish`;
   - payload includes the target ESP directory base URL;
   - executor builds, signs, verifies, and publishes the descriptor by reusing
     existing `group public descriptor/publish` helpers.
8. Add operation kinds to sign-request execution and authorization:
   - policy update/clear requires group admin.
   - public publish requires group admin and must fail unless local identity is
     the founder.
9. Do not expose or weaken operator-only `index-status` behavior.
10. Add focused Go tests for defaults, payload validation, policy
    status/set/clear, public publish, admin/founder authorization, escaped group
    IDs, and unchanged existing ESP routes.
11. Run focused Go tests for touched packages and broader `go test ./...` from
    `/root/entmoot/src` when shared ESP/sign-request behavior changes.

Task 2: iOS DTOs and ESP client contract
1. In `/root/entmoot/repos/entmoot-app`, add Swift DTOs for:
   - visibility;
   - join mode;
   - policy;
   - policy report;
   - public moot descriptor;
   - public directory entry;
   - public publish response.
2. Keep DTO names and coding keys aligned with ESP JSON. Do not hand-parse JSON
   strings where Codable models are practical.
3. Extend `ESPGroupSummary` to expose visibility and join mode from metadata.
4. Extend `ESPGroupCreateRequest` and
   `ESPClient.createGroupSignRequest(...)` to send visibility, join mode, and
   policy selection.
5. Add `ESPClient` methods:
   - `publicMoots()`;
   - `publicMoot(groupID:)`;
   - `groupPolicy(groupID:)`;
   - `setGroupPolicySignRequest(...)`;
   - `clearGroupPolicySignRequest(...)`;
   - `publishPublicMootSignRequest(...)`.
6. Use the existing percent-encoded path helper for every group id path segment,
   especially public moot detail and group policy/publish routes.
7. Add Codable and client route tests, including group ids containing `/`, `+`,
   and `=`.

Task 3: iOS app model state and actions
1. Extend `ESPAppModel` with centralized state for:
   - public moot list entries;
   - public moot detail cache by group id;
   - public directory loading/error state;
   - policy reports by group id;
   - policy loading/saving errors by group id;
   - public publish loading/error state by group id.
2. Add app-model actions:
   - load/refresh public moots;
   - load public moot detail;
   - join public moot via open invite;
   - create moot with visibility, join mode, and policy;
   - load group policy;
   - set/clear group policy;
   - publish/refresh public listing.
3. Keep all mutating operations that require authorization on the existing
   sign-request completion path.
4. Joining from the directory:
   - if already joined, route to the existing moot detail.
   - if `join_mode=open_invite`, use the descriptor open-invite issuer/token or
     link.
   - if `join_mode=invite_only`, do not invent request-to-join behavior.
5. After successful create, policy update, open invite, or publish, refresh only
   the relevant state plus the group list when needed.
6. Add focused `ESPAppModelTests` for directory load, detail load, open-invite
   join, policy update, public publish, and error paths.

Task 4: iOS UI structure cleanup
1. Do not grow `ContentView.swift` with the new feature implementation.
2. Extract reusable UI primitives currently embedded in `ContentView.swift` into
   shared internal Swift files before adding the new screens:
   - theme/colors if needed by new files;
   - settings scroll/group/row;
   - buttons/chips/info cards;
   - editable fields and copy blocks.
3. Keep the extracted components behavior-compatible with the existing screens.
4. Move only what is needed for the new feature. Avoid unrelated redesigns.
5. Add or update tests only if extraction changes testable behavior.
6. Build after extraction before adding feature UI.

Task 5: Explore tab and public directory UI
1. Add a fifth top-level `Explore` tab to the existing `TabView`.
2. Keep the tab bar to five tabs. Do not hide public discovery under Settings.
3. Build `PublicMootDirectoryView`:
   - loads `GET /v1/public-moots`;
   - supports search by name, description, tags, and group id;
   - supports filtering by all/open invite/invite required;
   - supports pull-to-refresh;
   - has clear empty, loading, and error states.
4. Build `PublicMootDetailView`:
   - name, description, tags;
   - group id with copy action;
   - founder identity;
   - visibility and join mode;
   - policy summary;
   - mirror state;
   - message history availability;
   - indexed/status timestamps when useful.
5. Public detail actions:
   - already joined: open existing moot detail.
   - open invite: join via existing signed open-invite accept flow.
   - invite-only: show invite-required state and copy/share group info.
6. Do not render missing message history as an error when
   `message_history_available=false`.
7. Follow current app visual language and Apple SwiftUI patterns for navigation,
   lists, search, refresh, and share/copy affordances.
8. Avoid visible tutorial text. Use concise labels and state descriptions only
   where needed for decisions/errors.

Task 6: Founder create/settings controls
1. Update Create Moot UI:
   - default visibility: private;
   - default join mode: invite-only;
   - default policy: standard;
   - visibility selector: private, unlisted, public;
   - join-mode selector: invite-only, open invite;
   - policy selector: standard, relaxed, none, custom;
   - custom policy editor for all policy fields with local validation.
2. If visibility is public, show a post-create publish action after the group is
   created. Do not auto-publish without an explicit user action.
3. Update Moot Settings UI:
   - add "Visibility & Directory" section;
   - show current visibility, join mode, and listing/publish status;
   - allow founder/admin publish or refresh when visibility is public;
   - add "Policy" section with current policy summary and edit action;
   - founder/admin-only edit controls;
   - read-only state for non-admin devices.
4. Policy editor:
   - standard and relaxed presets fill known values;
   - none clears stored per-moot policy;
   - custom exposes all policy fields with numeric/rate validation;
   - server validation remains authoritative.
5. Preserve existing metadata editing and invite flows.
6. Avoid nested card layouts and keep settings rows compact enough for small
   iPhone screens.

Task 7: Documentation, verification, and live smoke
1. Update docs in the repo that owns each changed contract:
   - ESP HTTP docs for new mobile policy/publish endpoints.
   - iOS README notes only if setup/test workflow changed.
2. Run checks per changed repo:
   - `/root/entmoot/src`: focused `go test` plus `go test ./...` when shared
     behavior changed.
   - `/root/entmoot/repos/entmoot-app`: `pnpm ios:generate` and
     `pnpm ios:build` when Xcode tooling is available.
   - If iOS CLI build is unavailable on Linux, state the exact blocker and run
     all available Swift/model/static checks that are possible in this
     environment.
3. Run `codex exec review --uncommitted` in every changed repo and complete the
   review-fix loop until no actionable findings remain.
4. Perform a real ESP smoke test when the APIs are implemented:
   - create or use a test moot;
   - verify default policy status;
   - update policy through ESP sign-request path;
   - publish public listing through the new ESP route;
   - query `GET /v1/public-moots`;
   - join from an open-invite descriptor where safe.
5. Do not use live production services for destructive or spammy tests. Use
   local/test ESP where possible. If production verification is required, keep it
   read-only unless the user explicitly approves the write.

Success criteria:
- A mobile-created moot gets the same safe defaults as CLI create.
- A founder can choose visibility, join mode, and policy from iOS.
- A founder can inspect and update policy from iOS.
- A founder can publish/refresh a public listing from iOS.
- Explore lists public moots from the ESP directory.
- Open-invite public moots can be joined from Explore.
- Invite-only public moots clearly show that an invite is required.
- Public listing never implies ESP membership, message history, or live replies.
- Existing moot creation, metadata editing, invites, fleet, tasks, settings, and
  mailbox flows still work.

Final response after all tasks:
- List completed tasks.
- For each task, list branch, PR URL, merge status, and merged commit hash.
- List tests/checks run per repo.
- List live smoke tests run and their results.
- State any skipped checks or residual risk plainly.
```
