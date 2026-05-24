# Goal Prompt: Implement ESP-Local Global Node Display Names

Use this prompt with `/goal` to implement ESP-local global hostnames and
Discord-style node display labels across Entmoot core and the iOS app.

This goal follows the same operating style as `docs/goal-pr-task-workflow.md`,
`docs/goal-implement-public-moot-directory-and-policies.md`, and
`docs/goal-implement-ios-public-moot-directory-and-policies.md`: task-by-task
branches, one PR per dependent task, review-fix loops, current contract
verification, clean commits, release gates, deployment gates, and real runtime
validation where changes affect running Entmoot agents, ESP, or app-visible
APIs.

Treat this file as the executable goal prompt. The implementation scope spans:

- `/root/entmoot` for ESP state, member API enrichment, docs, tests, release,
  and deployment.
- `/root/entmoot/repos/entmoot-app` for iOS DTO/display support if app changes
  are not already present.

```text
Implement ESP-local global hostnames and stable node display labels task by
task.

Primary objective:
- If an ESP has learned a hostname for a Pilot node in any visible moot, fleet,
  invite, or local Pilot context, it can reuse that hostname as an ESP-local
  fallback in other views.
- Member lists and message author displays use stable labels like
  `hermes#155760`.
- Group-local signed member profiles remain the highest-confidence source.
- Roster identity and security remain unchanged: stable identity is still
  Pilot node id plus Entmoot public key.

Product rules:
- "Global" means global inside one ESP's local view, not globally authoritative
  across the Entmoot network.
- Do not add a centralized identity service.
- Do not sync global hostname caches between ESPs in this goal.
- Do not mutate rosters to store display names.
- Do not treat hostnames as secure identity.
- Do not expose private keys, bearer tokens, device secrets, or live credentials
  in logs, docs, commits, PRs, tests, or final summaries.
- Preserve privacy boundaries: group-local profile hostname remains preferred
  for that group, and the ESP-local fallback is only based on data the ESP can
  already see.
- Display labels must always keep the node id suffix when a hostname is used:
  `<hostname>#<node_id>`.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized.
- Avoid code duplication. Extract reusable helpers for hostname normalization,
  node display-name formatting, node-profile upsert precedence, API DTO
  enrichment, SQLite row scanning, memory-store behavior, Swift display
  formatting, and test fixtures when that matches existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Keep API additions backward compatible. Existing `hostname` semantics must
  remain group-local.
- Do not commit generated data, reports, logs, caches, build artifacts,
  credentials, Docker/container data, Xcode DerivedData, `.xcuserdata`, or
  large runtime dumps unless the task explicitly says they are intended tracked
  fixtures.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes SQLite
  UPSERT behavior, iOS/Swift Codable behavior, ESP HTTP behavior, release
  tooling, deployment process, and GitHub/PR tooling.

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
3. Verify which prior public-moot/policy/iOS tasks, PRs, and commits are already
   merged before building on them. Do not rely only on memory.
4. If resuming from an interrupted branch, finish the active task first unless
   the branch is clearly abandoned or the user explicitly redirects.
5. Verify PR tooling before the first PR:
   - `gh auth status`
   - the remote resolves to the expected GitHub repository.
6. Inspect and follow current repo patterns before editing:
   - `docs/goal-pr-task-workflow.md`
   - `docs/goal-implement-public-moot-directory-and-policies.md`
   - `docs/goal-implement-ios-public-moot-directory-and-policies.md`
   - `README.md`
   - `website/docs/reference/http-esp-api.md`
   - `pkg/entmoot/esphttp`
   - `pkg/entmoot/store`
   - `pkg/entmoot/gossip`
   - `cmd/entmootd/esp_catalog.go`
7. Inspect and follow current iOS app patterns before editing:
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPModels.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPClient.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/Core/ESP/ESPAppModel.swift`
   - `/root/entmoot/repos/entmoot-app/ios/Entmoot/App/ContentView.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPModelsTests.swift`
   - `/root/entmoot/repos/entmoot-app/ios/EntmootTests/ESPAppModelTests.swift`

Current implementation facts to verify:
- `MemberSummary.hostname` is currently group-local and populated from signed
  member profile ads for that specific group.
- iOS currently falls back from missing `hostname` to `node-<node_id>`.
- Fleet member/invite records can already contain hostnames independent of
  group-local member profiles.
- Mars Hub can show `hermes` while The Ent Moot cannot because Mars Hub has a
  group-local profile ad and/or fleet-visible hostname, while The Ent Moot may
  not.
- The target display model is `hostname#node_id`, for example `hermes#155760`.

Definitions:
- `hostname`: group-local hostname from that group's signed member profile ad.
- `global_hostname`: ESP-local fallback hostname for a Pilot node, learned from
  other data this ESP can already see.
- `display_name`: app-facing stable display label. If a hostname is available,
  it is `<hostname>#<node_id>`; otherwise it is `node-<node_id>`.
- `source`: where an ESP-local hostname observation came from.
- `confidence`: precedence used when multiple hostname observations exist for
  one node.

Task 1: Core node-profile data model and store APIs
1. Add a reusable ESP node-profile model in `pkg/entmoot/esphttp`:
   - `node_id`;
   - `hostname`;
   - `source`;
   - `confidence`;
   - `observed_at_ms`;
   - `expires_at_ms`;
   - optional `source_group_id`.
2. Add source constants:
   - `member_profile`;
   - `fleet_member`;
   - `fleet_invite`;
   - `pilot_info`.
3. Add confidence constants with this precedence:
   - `member_profile` highest;
   - `fleet_member`;
   - `fleet_invite`;
   - `pilot_info` lowest.
4. Add one shared hostname normalization/validation helper and reuse it for new
   node-profile writes. The helper must trim whitespace, reject empty strings,
   reject control characters, and cap length consistently with existing member
   profile hostname validation.
5. Add one shared display-name formatter:
   - normalized hostname present: `<hostname>#<node_id>`;
   - no hostname: `node-<node_id>`.
6. Extend ESP `StateStore` with:
   - `UpsertNodeProfile(context.Context, NodeProfileRecord) (NodeProfileRecord, bool, error)`;
   - `GetNodeProfile(context.Context, entmoot.NodeID) (NodeProfileRecord, bool, error)`;
   - `ListNodeProfiles(context.Context, []entmoot.NodeID) (map[entmoot.NodeID]NodeProfileRecord, error)`.
7. Implement memory-store and SQLite-store support.
8. Add SQLite table `esp_node_profiles` keyed by `node_id`.
9. Upsert rules:
   - ignore invalid or empty hostnames;
   - ignore expired observations;
   - replace when new confidence is higher;
   - replace when confidence ties and `observed_at_ms` is newer;
   - preserve existing record otherwise.
10. Add focused Go tests for validation, display formatting, memory store,
    SQLite store, confidence precedence, tie-breaking, and expiry handling.

Task 2: Backend cache population and member API enrichment
1. Extend `MemberSummary` with:
   - existing `hostname` unchanged;
   - `global_hostname,omitempty`;
   - `display_name`.
2. Add a single backend helper that enriches member summaries with global
   hostname and display name. Use it instead of duplicating fallback logic.
3. In group member listing:
   - continue reading group-local profile ads for `hostname`;
   - when a valid group-local profile is seen, upsert it into
     `esp_node_profiles` with source `member_profile`;
   - for members without group-local hostname, look up `global_hostname`;
   - always set `display_name`.
4. Populate the node-profile cache from fleet data:
   - when listing or upserting fleet members with hostnames, upsert source
     `fleet_member`;
   - when listing or creating fleet invites with hostnames, upsert source
     `fleet_invite`.
5. Populate local `pilot_info` for the ESP's own node when Pilot info is
   available, without making Pilot lookup a hard dependency for normal member
   listing.
6. Add lazy backfill from existing visible fleet/member rows during read paths
   where appropriate. Do not add a separate migration command in this goal.
7. Ensure no bearer-token or device-auth behavior changes.
8. Update ESP HTTP docs for the new response fields and display semantics.
9. Add Go tests:
   - group member response includes `display_name`;
   - group-local hostname wins over global fallback;
   - Mars Hub learned hostname can appear as `global_hostname` in another group;
   - stale/expired node-profile is ignored;
   - old clients relying on `hostname` keep existing semantics.
10. Run focused tests for touched packages and broader `go test ./...` from
    `/root/entmoot/src`.
11. Run `codex exec review --uncommitted`; fix findings before commit.
12. Commit, push, open PR, merge after checks/review are clean.

Task 3: iOS model and display resolver
1. In `/root/entmoot/repos/entmoot-app`, extend `ESPMemberSummary` with:
   - `globalHostname`;
   - `displayName`.
2. Keep decoding backward compatible when either field is missing.
3. Add or centralize one Swift display helper:
   - prefer backend `displayName` if non-empty;
   - else use local formatting from `hostname ?? globalHostname`;
   - else `node-<nodeID>`.
4. Update app member/message/fleet display sites to use the shared helper so
   `hostname#nodeID` formatting is consistent.
5. Preserve secondary detail text with node id/pubkey/role where useful, but do
   not duplicate confusing labels like `hermes#155760 - node 155760` unless the
   existing UI specifically needs a stable secondary id.
6. Add Swift tests for:
   - decoding new fields;
   - fallback when backend fields are absent;
   - `hermes#155760` formatting;
   - `node-155760` fallback;
   - fleet and moot display helpers using the same resolver.
7. Run the repo's iOS test/build commands documented in the app repo. If a full
   iOS build is not possible on the current host, run all feasible model tests
   and record the blocker clearly.
8. Run `codex exec review --uncommitted`; fix findings before commit.
9. Commit, push, open PR, merge after checks/review are clean.

Task 4: Release, deploy, and live validation
1. After core PRs are merged, cut the next Entmoot release using existing
   release tooling and repo conventions.
2. Update the live VPS ESP/entmootd safely:
   - inspect process state first;
   - preserve `/root/.entmoot/esp.env`;
   - avoid touching unrelated services;
   - restart only the necessary Entmoot ESP/daemon process.
3. Verify live endpoints:
   - `GET https://esp.entmoot.xyz/v1/public-moots`;
   - authenticated or proxied member APIs as appropriate;
   - The Ent Moot member list returns `display_name`;
   - Mars Hub-learned hostname can populate `global_hostname` where applicable.
4. Verify live behavior with:
   - `entmootd doctor -group <GROUP_ID> --json`;
   - direct API checks against the ESP;
   - iOS app contract checks if a simulator/device is available.
5. If entmoot-web consumes member display data, verify and update only if
   necessary. Do not make unrelated website UI changes.
6. If iOS changes are merged, state clearly whether a TestFlight/App Store app
   release is needed and which repo/commit should be built.

Acceptance criteria:
- The ESP can learn `hermes` for node `155760` from one visible group/fleet and
  return `global_hostname: "hermes"` plus `display_name: "hermes#155760"` in
  another visible group where group-local `hostname` is absent.
- Existing group-local hostnames still appear in `hostname`.
- Existing clients that only read `hostname` do not break.
- iOS displays `hostname#nodeID` consistently.
- No roster, signing, trust, or security identity semantics are changed.
- All touched tests pass, review findings are resolved, PRs are merged, and live
  ESP validation succeeds.
```
