# Entmoot Phase 1 arXiv Gate Package

Primary category: cs.DC
Cross-list: cs.MA

## Files

- main.tex
- refs.bib
- main.bbl
- generated/evaluation/*.tsv
- entmoot-paper.pdf (local verification copy; arXiv source upload can omit it)

## Abstract

Entmoot is a durable group-communication substrate for autonomous agents that
already have pairwise identity and trust. Its main empirical result is
convergence after a real residential-edge partition: in a 120-hour live run
across a VPS, two container runtimes, and a Raspberry Pi, all four nodes
finished with healthy diagnostics and the same controlled group state
(4 members, 78 controlled-topic messages). The recovery window for the
longest Phobos outage is bounded only at snapshot granularity: the last
unhealthy checkpoint was 20260606T052015Z and the first subsequent checkpoint
showing the full controlled state was 20260606T054137Z, a 21.37-minute bound,
not a delivery-latency measurement. Entmoot represents a group, or
\emph{moot}, as signed membership state plus signed messages propagated by
gossip and repaired by anti-entropy reconciliation. The reference Go
implementation runs over Pilot Protocol as the deployed pairwise-trust
overlay, but the design target is more general: signed group conversation
above trust-gated agent transports. It provides private and public moots,
targeted and open invites, founder-managed policies, an Entmoot Service
Provider (ESP) surface for web and mobile clients, lexical search with
message-context jumps, and Agent Skills plus Codex/Claude plugin packaging for
agent-facing use.

## Announcement Blurb

Entmoot is a signed group-communication layer for autonomous agents running over pairwise-trust overlays. The Phase 1 paper positions it against recent gossip-for-agents and agent-protocol work, and reports a 120-hour heterogeneous live run where all four nodes converged to the same controlled group state after a residential-edge outage. The recovery result is reported as a snapshot-granularity bound, not latency.
