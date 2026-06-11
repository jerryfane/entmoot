# Submission Handoff

## Ready Now

- Current paper PDF: `paper/entmoot-paper.pdf`
- Website PDF copy: `website/static/papers/entmoot-main.pdf`
- LaTeX source: `paper/main.tex`
- Bibliography: `paper/refs.bib`
- Generated evaluation tables: `paper/generated/evaluation/`
- Generated benchmark tables: `paper/generated/benchmarks/`
- Generated security table: `paper/generated/security/adversarial.tsv`
- W5 sanitized local package:
  `artifacts/evaluation/packages/20260603T212611Z-3-7-day-evaluation-w5-shareable-package-20260611T145427Z`

## Exact Remaining Human/Editorial Work

1. Confirm whether the arXiv post has been submitted or explicitly defer it.
   The goal file still treats this as a required external account action.
2. Check the official CoNEXT 2026 accepted-workshops list/CFPs after
   June 15, 2026 and identify the actual target workshop.
3. Apply that workshop's specific page limit, template, anonymity, and artifact
   rules. If it gives no custom paper limit, use the CoNEXT short-paper envelope:
   10 pages of main content, references outside the limit, and up to two
   appendix pages.
4. Decide whether the submission is anonymous. The current paper is not
   anonymized: it includes author identity, contact email, repository paths,
   and self-identifying system context.
5. Decide whether the private five-day artifact package can be shared with
   reviewers. If yes, perform a final human privacy review before upload. If no,
   submit the paper with the current inspectability boundary and no artifact
   badge claim.
6. Submit the package through the workshop's chosen system by the live deadline.

## Suggested Submission Positioning

Use the workshop framing rather than the full systems-track framing:

- Entmoot is a deployed group-communication layer for pairwise-trust agent
  overlays.
- The paper contributes signed group state, trust-gated gossip, Merkle/RBSR
  anti-entropy, ESP-local UX separation, and a five-day heterogeneous live run.
- The claim is bounded: the live run proves small-scale convergence and
  operational survivability, while lab results exercise protocol mechanics; it
  does not claim Internet-scale deployment or social/behavioral outcomes.

## Local Package Command

Run from the repository root:

```sh
paper/workshop/conext2026/package.sh
```

The script writes an ignored `dist/entmoot-conext2026-workshop-package-*`
directory and a `.tar.gz` archive with checksums.
