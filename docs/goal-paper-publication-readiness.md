# Goal: Entmoot Paper Publication-Readiness Program

Execute the publication-readiness program for `paper/main.tex` defined in
GitHub issue #84 (successor to #83, which produced the current rewritten
paper). The program is phased: **Phase 1 (W1+W2) is time-sensitive** because
arXiv:2512.03285 (Dec 2025) and arXiv:2508.01531 are near-neighbors to this
paper's thesis — the repositioning and the arXiv post stake priority. Phase 2
(W3-W5) brings the paper to peer-review readiness for a CoNEXT-style
decentralization workshop.

Reference issue: https://github.com/jerryfane/entmoot/issues/84
Prior work: #83 (rewrite), #80/#72 (the 3-7 day evaluation that produced the
artifacts Phase 1 consumes).

## Ground Rules

- No fabricated results, numbers, or citations. Every new `refs.bib` entry
  must resolve to a real, checkable publication (verify via web search before
  adding). Every number in the paper must trace to an artifact file or a
  reproducible script.
- Honesty constraint for W2: `timestamp_ms` in transcripts is the AUTHOR
  timestamp and snapshots are coarse-grained. Convergence/recovery numbers are
  *bounds at snapshot granularity* and must be labeled as such — never
  presented as latency measurements. Per-message receipt latency requires new
  instrumentation and belongs to Phase 2/future runs.
- Do not modify, move, or rewrite anything under `artifacts/evaluation/` raw
  trees. New analysis reads from them and writes derived tables/figures only.
- Keep the paper building green: `make all` in `paper/` must pass before every
  commit that touches the paper; `entmoot-paper.pdf` stays in sync per the
  Makefile.
- Preserve the paper's existing bounded-claims style. The evaluation section
  may strengthen its evidence, not its rhetoric.
- No new features in the Go core for Phase 1. W3 adds benchmarks/simulation
  harnesses (test-side code) without changing protocol behavior; W4 exercises
  existing enforcement paths only.
- One scoped task per branch/PR, normal commit discipline, no credentials or
  sensitive endpoints in tracked files.

## Notification Rules

- Send an Agentgram Telegram notification when: (a) the arXiv package gate is
  reached (Task 3), (b) any approval-needed decision arises, (c) a Phase
  completes. If no reply within 10-15 minutes on an approval-blocking item,
  re-ping every 10-15 minutes.
- The actual arXiv submission is Jerry's manual account action. Prepare
  everything, then stop and notify.

## Before Starting / Resuming (Fresh-Session Preflight)

1. `git status --short` and current branch; sync `main` if clean.
2. Read GitHub issue #84 (the program contract) and this goal file.
3. Read `paper/REWRITE_PLAN.md` for the rewrite's state and conventions.
4. Confirm `make all` passes in `paper/`.
5. Locate the evaluation run dir
   `artifacts/evaluation/20260603T212611Z-3-7-day-evaluation/` and confirm
   `analysis/snapshot-health.tsv` and the per-participant
   `transcript-pre-live-*/messages-*.jsonl` trees exist.
6. Check which program tasks are already merged (issue #84 checklist) and
   continue from the first unchecked item.

## Task 1 (W1): Reposition the paper and complete related work

- `paper/main.tex` sections 1-2 and `paper/refs.bib`.
- Add a nearest-neighbors paragraph citing and differentiating:
  arXiv:2508.01531 (gossip-for-agents vision; calls for signed trust-gated
  variants — we provide the deployed system), arXiv:2512.03285
  (gossip-enhanced substrate for agentic AI), ANP's group-messaging profile
  (spec, not deployed system), Coral Protocol (arXiv:2505.00749), AIOS
  AgentSites (arXiv:2504.14411).
- Add the agent-protocol lineage: interop survey arXiv:2505.02279
  (MCP/ACP/A2A/ANP; note ACP merged into A2A under the Linux Foundation,
  Sept 2025) and FIPA-ACL/KQML as ancestors. Position: those protocols do
  tool/task interop and discovery; none provide durable group conversation
  with signed membership and repair.
- Add the agent-society line (AgentSociety arXiv:2502.08691, generative
  agents) and the decentralized-social line (nostr as a system — NIP-77 is
  already cited — and AT Protocol/Bluesky as deployed contrast).
- Generalize the framing: the problem is a group-communication substrate over
  pairwise-trust agent overlays; Pilot is the deployed instance, not the
  premise. Soften the dependence on ecosystem-internal citations.
- Rewrite the abstract to lead with the convergence-after-partition result
  (all four nodes, identical final state after the residential-edge outage),
  not the snapshot count.
- Completion: PR merged, `make all` green, every new citation verified real.

## Task 2 (W2): Retrofit convergence metrics from existing artifacts

- New `scripts/eval/analyze-convergence.py` that reads
  `analysis/snapshot-health.tsv` and the transcript JSONLs and emits derived
  TSV/figure inputs (no raw-tree mutation):
  - per-node `controlled_messages` convergence timeline across snapshots;
  - the Phobos recovery bound: last unhealthy snapshot -> first snapshot
    showing the full controlled-group state, expressed in wall-clock bounds;
  - cross-node `message_id` set-diffs proving final completeness.
- Add a convergence-timeline figure (TikZ/pgfplots) and a recovery paragraph
  to the Results section; promote convergence-after-partition to the headline
  result.
- Apply the honesty constraint from Ground Rules (bounds, not latency).
- Completion: PR merged with script + figure + updated Results, numbers
  traceable to the script output.

## Task 3 (GATE): Prepare the arXiv package and notify

- Build the final Phase-1 PDF (`make all`), assemble the arXiv source bundle
  (main.tex, refs.bib/bbl, figures), draft the arXiv abstract, propose
  categories cs.DC (primary) + cs.MA (cross-list), and a short announcement
  blurb.
- Stop. Send the Agentgram notification with the package location and wait
  for Jerry to submit. Do not start Phase 2 tasks until the gate notification
  is sent; after the notification, Phase 2 may proceed while awaiting the
  submission itself.
- Completion: package ready + notification sent + issue #84 gate checkbox
  ticked.

## Task 4 (W3): Lab-scale protocol benchmarks

- New benchmark/simulation harness on the existing in-memory transport
  (`NewMemTransports()` in `src/pkg/entmoot/gossip/transport.go`); the repo
  currently has zero `Benchmark*` functions.
- N = 50-300 simulated members. Report: broadcast delivery-latency CDF + p99,
  duplicate-delivery ratio (RMR), loss rate, RBSR bandwidth vs
  symmetric-difference size (validate the proportionality claim against
  `src/pkg/entmoot/reconcile/`), join time vs history length.
- Use the GossipSub v1.1 evaluation report as the explicit metric template
  and cite it as such in the new Evaluation subsection.
- Completion: PR merged with harness + results tables/figures in the paper.

## Task 5 (W4+W5): Adversarial demos and polish

- W4: exercise and report three existing enforcement paths in a threat-model
  table in the Security section: non-member connection rejection (roster
  check), replayed-frame rejection (`src/pkg/entmoot/wire/replay.go`),
  rate-limit breach -> disconnect.
- W5: fix Table 3 (real per-node streak values), define `doctor`/`peers`
  metrics for outside readers, move macOS-Gatekeeper and IPC-deadline
  material to an appendix, produce a sanitized shareable artifact package via
  `scripts/eval/package-artifacts.sh`, confirm the contact email on the title
  page.
- Completion: PRs merged, `make all` green.

## Task 6 (W6): Venue submission support

- Prepare the CoNEXT decentralization-workshop submission package (format per
  the live CFP) once W3-W5 are merged; precedent: the Bluesky/AT-Protocol
  paper at CoNEXT 2024 DIN.
- Track AAMAS 2027 (deadline ~Oct 2026) as conditional: only if a
  transcript-coding/behavioral section is added later.
- Completion: workshop package ready + Agentgram notification.

## Do not mark this goal complete until

- Tasks 1-2 merged and the arXiv gate notification sent (Phase 1);
- the arXiv post exists (Jerry's action) or Jerry has explicitly deferred it;
- Tasks 4-5 merged (Phase 2);
- the workshop submission package is ready and announced (Task 6);
- issue #84's checklist reflects the true state throughout.
