# Goal Prompt: Implement Entmoot Codex and Claude Plugins

Use this prompt with `/goal` to implement Gitmoot-style Codex and Claude Code
plugin packaging for Entmoot.

This goal intentionally follows the same operating style as
`docs/goal-pr-task-workflow.md`, `docs/goal-the-ent-moot-default-public-moot.md`,
`docs/goal-public-moot-directory-and-policies.md`, and
`docs/goal-implement-public-moot-directory-and-policies.md`: task-by-task
branches, one PR per task when tasks are dependent, review-fix loops, current
contract verification, release gates, deployment gates, and real runtime
validation where the change affects running agents or public infrastructure.

Treat the plugin plan from the planning turn as the design plan and this file
as the executable goal prompt.

```text
Implement Codex and Claude Code plugin packaging for Entmoot task by task.

Primary objective:
- Entmoot has one canonical Agent Skills package with no duplicated skill
  content.
- The canonical skill package lives inside the Go module so entmootd can embed
  it and generate runtime plugin packages.
- entmootd can build, install, locate, and diagnose Codex and Claude Code
  plugin packages, following the Gitmoot plugin architecture.
- entmoot.xyz exposes the canonical skill at https://entmoot.xyz/SKILL.md.
- https://entmoot.xyz/skill.md remains a compatibility alias.

Product rules:
- Plugins package Entmoot guidance and discovery only. They do not start hosted
  services, run daemons, join moots, enable live replies, mutate ESP state, or
  install Pilot/Entmoot silently.
- The Entmoot CLI remains the operational engine. Plugins should guide Codex or
  Claude Code to inspect state first and call entmootd only when the user asks
  for setup, status, joining, publishing, diagnostics, ESP/mobile state, Fleet,
  or live-agent work.
- Do not duplicate `SKILL.md` or reference content in multiple canonical
  locations.
- The public canonical skill URL is uppercase `/SKILL.md`; lowercase
  `/skill.md` must keep working for old prompts.
- Existing Entmoot behavior, protocols, database formats, and ESP APIs must not
  change except where explicitly required for plugin packaging or public skill
  routing.

Core engineering rules:
- Follow `docs/goal-pr-task-workflow.md` unless this prompt is stricter.
- Work one task at a time in the listed order by default.
- Each dependent task must pass checks, pass `codex exec review --uncommitted`,
  be committed, pushed, opened as a PR, merged, and verified on the target base
  before starting the next dependent task.
- Parallelize only when tasks are independent, have disjoint write sets, and
  cannot create order-dependent assumptions.
- Keep changes clean, scoped, and organized. Avoid broad rewrites.
- Avoid code duplication. Extract small reusable helpers for repeated provider
  parsing, plugin manifest paths, generated-package detection, directory copy,
  JSON writing, marketplace payloads, subprocess execution, CLI output,
  doctor checks, path resolution, and smoke-test setup when that matches
  existing repo patterns.
- Preserve existing behavior unless the current task explicitly changes it.
- Do not commit generated plugin packages, local marketplaces, generated
  runtime homes, reports, logs, caches, build artifacts, credentials, Docker or
  container data, or large runtime dumps unless the task explicitly says they
  are intended tracked fixtures.
- Never print, paste, commit, or summarize credentials, private keys, tokens, or
  live secrets.
- When external contracts matter, verify them before editing with local
  commands and/or current official or primary sources. This includes Codex
  plugin manifests and marketplaces, Claude Code plugin manifests and
  marketplaces, Agent Skills spec behavior, release archive behavior, entmootd
  wrapper/data-home behavior, entmoot-web build/deploy behavior, GitHub CLI
  actions, and runtime CLI behavior.

Fresh-session and resume preflight:
1. Inspect current repo state for every repo that may be touched:
   - current Entmoot checkout, typically `/root/entmoot`;
   - current entmoot-web checkout, typically `/root/entmoot-web`, when website
     work begins;
   - `git status --short --branch`;
   - current branch;
   - current remotes;
   - latest relevant local and remote commits;
   - latest tags/releases if a release may be needed.
2. If there are uncommitted changes, identify whether they belong to the
   current task. Do not overwrite or revert unrelated user changes. Stop and ask
   only if dirty state makes task commits ambiguous.
3. Verify which tasks, PRs, and commits are already merged before skipping any
   task. Do not rely only on memory.
4. If resuming from an interrupted branch, finish the active task first unless
   the branch is clearly abandoned or the user explicitly redirects.
5. Verify PR tooling before the first PR:
   - `gh auth status`;
   - the remote resolves to the expected GitHub repository.
6. Inspect and follow current repo patterns before editing:
   - `docs/goal-pr-task-workflow.md`;
   - `docs/goal-implement-public-moot-directory-and-policies.md`;
   - `README.md`;
   - `.goreleaser.yaml`;
   - `src/cmd/entmootd/main.go`;
   - `src/cmd/entmootd/version.go`;
   - existing CLI command/test style under `src/cmd/entmootd`;
   - current skill package under `src/skills/entmoot` after Task 1, or
     `skills/entmoot` before Task 1 is complete.
7. Inspect Gitmoot plugin implementation before editing:
   - `/root/gitmoot/internal/pluginpack`;
   - `/root/gitmoot/internal/plugininstall`;
   - `/root/gitmoot/internal/cli/plugin.go`;
   - `/root/gitmoot/internal/cli/plugin_test.go`;
   - `/root/gitmoot/internal/pluginpack/pluginpack_test.go`;
   - `/root/gitmoot/internal/plugininstall/install_test.go`;
   - `/root/gitmoot/scripts/plugin-smoke.sh`;
   - `/root/gitmoot/docs/plugins.md`.
   If `/root/gitmoot` is unavailable, inspect
   `https://github.com/jerryfane/gitmoot` instead.
8. Verify official/current external contracts before editing:
   - Agent Skills spec: https://agentskills.io/specification
   - Codex plugin build/marketplace docs:
     https://developers.openai.com/codex/plugins/build
   - Codex plugin docs:
     https://developers.openai.com/codex/plugins
   - Claude Code plugin docs:
     https://code.claude.com/docs/en/plugins
   - Claude Code plugin reference:
     https://code.claude.com/docs/en/plugins-reference
   - Claude Code plugin marketplace docs:
     https://code.claude.com/docs/en/plugin-marketplaces
9. Verify local runtime tooling before plugin implementation:
   - `codex --version`;
   - `codex plugin --help`;
   - `codex plugin add --help`;
   - `codex plugin marketplace add --help`;
   - `claude --version`, if installed;
   - `claude plugin --help`, if installed;
   - `claude plugin install --help`, if installed;
   - `claude plugin marketplace add --help`, if installed;
   - `claude plugin validate --help`, if installed.
10. Before touching entmoot-web, inspect:
   - `frontend/vite.config.ts`;
   - `frontend/src/entmoot/links.ts`;
   - current production PM2 app names and deployment path.

Task 1: Move the canonical skill into the Go module
1. Move `skills/entmoot/**` to `src/skills/entmoot/**`.
2. Add `src/skills/assets.go` with an embedded filesystem for
   `entmoot/**`, following Gitmoot's `skills/assets.go` pattern.
3. Remove the old root `skills/entmoot` tracked copy. Do not leave duplicate
   canonical skill content.
4. Update repository references that point at `skills/entmoot/SKILL.md` so they
   point at `src/skills/entmoot/SKILL.md`.
5. Update `.goreleaser.yaml` so release archives include the full skill package
   including `references/*.md`, not only `SKILL.md`.
6. Run the Agent Skills validator against `src/skills/entmoot`.
7. Add or update focused tests/checks only where needed to prove the embed path
   and release archive paths are correct.

Task 2: Add runtime-neutral plugin package builder
1. Add `src/internal/pluginpack` modeled on Gitmoot, adapted to Entmoot:
   - plugin name: `entmoot`;
   - display name: `Entmoot`;
   - marketplace name: `entmoot-local`;
   - repository: `https://github.com/jerryfane/entmoot`;
   - homepage: `https://entmoot.xyz`;
   - privacy URL: `https://entmoot.xyz/privacy`;
   - terms URL: `https://entmoot.xyz/terms`.
2. Generate Codex packages with `.codex-plugin/plugin.json` and
   `skills/entmoot/**`.
3. Generate Claude Code packages with `.claude-plugin/plugin.json` and
   `skills/entmoot/**`.
4. Use shared helpers for provider parsing, default package paths, manifest
   paths, generated-package detection, skill validation, recursive copy, and
   JSON writing.
5. Refuse `--force` replacement of non-generated output paths unless the output
   is the default generated path or clearly contains the generated Entmoot
   plugin marker structure.
6. Add tests for Codex manifest shape, Claude manifest shape, copied skill
   references, default paths, unknown provider errors, missing skill errors,
   force replacement safety, and non-regular skill file rejection.

Task 3: Add plugin install, path, and doctor CLI
1. Add a small testable subprocess runner package if Entmoot does not already
   have one that fits the need.
2. Add `src/internal/plugininstall` based on Gitmoot's install flow.
3. Add top-level CLI command:
   - `entmootd plugin build codex|claude [--out <dir>] [--home <path>] [--force]`;
   - `entmootd plugin install codex|claude [--scope user|project|local] [--home <path>] [--force]`;
   - `entmootd plugin path codex|claude [--home <path>]`;
   - `entmootd plugin doctor [codex|claude] [--home <path>] [--json]`.
4. Default generated package paths:
   - `<entmoot-home>/plugins/build/codex/entmoot`;
   - `<entmoot-home>/plugins/build/claude/entmoot`.
5. Default marketplace roots:
   - `<entmoot-home>/plugins/marketplaces/codex`;
   - `<entmoot-home>/plugins/marketplaces/claude`.
6. Resolve `<entmoot-home>` consistently with existing Entmoot data/home
   behavior. If `--home` is provided, use that as the Entmoot home root for
   plugin generation without changing identity, group, or daemon state.
7. `install codex` must build the package, write a Codex local marketplace,
   run `codex plugin marketplace add <marketplace-root>`, and run
   `codex plugin add entmoot@entmoot-local` when the Codex CLI is available.
8. `install claude` must build the package, run
   `claude plugin validate <package-path>` when Claude CLI is available, write
   a Claude local marketplace, refresh any installed copy with `uninstall
   --keep-data` if needed, and run
   `claude plugin install entmoot@entmoot-local --scope <scope>`.
9. If a runtime CLI is missing, keep generated files and print exact manual
   install commands.
10. `doctor` must check canonical skill presence, generated package presence,
   copied skill presence, manifest JSON readability, marketplace path, runtime
   CLI availability, and runtime validation where supported.
11. Add tests for command parsing, help handling, default paths, install command
   order with fake runner, runtime-missing manual commands, Claude scope
   validation, marketplace JSON shape, doctor JSON output, and unhealthy
   runtime behavior.

Task 4: Document and smoke-test plugin workflows
1. Add `docs/plugins.md` for Entmoot plugin usage, mirroring Gitmoot's document
   shape:
   - what plugins do;
   - what plugins do not do;
   - install Entmoot/Pilot prerequisites;
   - install Codex plugin;
   - install Claude Code plugin;
   - verify with `entmootd plugin doctor`;
   - troubleshooting stale packages or missing runtime CLIs.
2. Update README and relevant operational docs to mention plugin commands
   without duplicating the full plugin guide.
3. Add `scripts/plugin-smoke.sh`, based on Gitmoot's smoke script, using
   isolated temporary Entmoot homes and runtime homes so it does not mutate real
   user plugin state.
4. The smoke script must:
   - build an entmootd binary into a temp dir when `ENTMOOTD_BIN` is not set;
   - build Codex and Claude plugin packages into temp output dirs;
   - assert both packages include `skills/entmoot/SKILL.md` and every tracked
     reference file;
   - run `claude plugin validate <temp-claude-plugin>` when `claude` exists;
   - run non-mutating Codex plugin list/marketplace checks with isolated `HOME`
     when `codex` exists;
   - run `entmootd plugin doctor` against the isolated home;
   - run install commands with isolated Entmoot and runtime homes.
5. Update release or beta smoke docs if present so plugin build/install checks
   are part of future release validation.

Task 5: Update entmoot-web public skill routing
1. In the entmoot-web checkout, update skill links so the canonical public URL
   is `https://entmoot.xyz/SKILL.md`.
2. Change the raw GitHub source URL to:
   `https://raw.githubusercontent.com/jerryfane/entmoot/main/src/skills/entmoot/SKILL.md`.
3. Update Vite middleware to serve both `/SKILL.md` and `/skill.md` as
   `text/markdown; charset=utf-8`.
4. Update visible app copy and `ENTMOOT_AGENT_PROMPT` to use uppercase
   `/SKILL.md`.
5. Keep lowercase `/skill.md` working as a compatibility alias.
6. Add focused frontend tests only if the repo already has a pattern for this
   middleware or link behavior; otherwise verify with build and HTTP checks.

Task 6: Release, deploy, and live verification
1. Decide whether plugin support requires a new Entmoot release. If entmootd
   plugin commands are added, cut a release before expecting installed users or
   ESP hosts to use the commands.
2. Follow existing Entmoot release workflow and verify release assets include
   the full skill package.
3. If server deployment is requested or required for the ESP/server runtime,
   update the installed entmootd using the existing release/update path and
   verify `entmootd version`.
4. Deploy entmoot-web after the web change is merged:
   - confirm PM2 app names and current state;
   - pull latest;
   - rebuild with `env VITE_SERVER_API=/api yarn build`;
   - restart only the relevant PM2 app(s);
   - `pm2 save`.
5. Live verification:
   - `curl -sSI https://entmoot.xyz/SKILL.md` returns
     `content-type: text/markdown`;
   - `curl -fsSL https://entmoot.xyz/SKILL.md | head` starts with Entmoot
     skill frontmatter;
   - `curl -sSI https://entmoot.xyz/skill.md` still returns markdown;
   - `entmootd plugin doctor codex` works on the updated host;
   - `entmootd plugin build codex --out <temp-dir>` and
     `entmootd plugin build claude --out <temp-dir>` produce valid packages.

Review loop for every task:
1. Run focused tests/checks for the files changed in that task.
2. Run `git diff --check`.
3. Run `codex exec review --uncommitted` in every repo with uncommitted
   changes.
4. Preserve exact raw review output per repo.
5. If review finds issues, do not only fix the literal line. First identify the
   underlying invariant/class of bug, then audit sibling paths for the same bug.
6. Make a concise fix plan using this prompt:
   "Review found these issues: <<PASTE RAW REVIEW RESULTS BY REPO>>.
   For each issue, identify the underlying invariant/class of bug, audit
   sibling paths for the same issue, and plan the smallest safe fix. Verify
   external assumptions with local commands and/or official sources. Preserve
   repo patterns, avoid unnecessary refactors, and list tests/checks per repo."
7. Execute the fix plan, rerun focused checks, rerun
   `codex exec review --uncommitted`, and repeat until review is clean or a
   finding is proven incorrect after verification.

Final gate before reporting each task complete:
- State changed files and why.
- State tests/checks run.
- Include exact final raw `codex exec review --uncommitted` output for each
  changed repo.
- State release/deploy status when relevant.
- State remaining risk.
- Do not claim that interactive `/review` is clean, because you cannot run
  `/review`. Say: "codex exec review is clean; ready for manual /review."
```
