# CoNEXT 2026 Workshop Submission Package

This directory is the W6 handoff package for the Entmoot paper.

The current live venue state, checked on 2026-06-11, is:

- ACM CoNEXT 2026 is scheduled for December 7-10, 2026 in Utrecht, The
  Netherlands.
- The main CoNEXT/PACMNET June research-paper deadline has passed
  (registration May 29, 2026; submission June 5, 2026).
- The CoNEXT 2026 workshop-proposal deadline has also passed
  (May 19, 2026). Selected workshops were scheduled to be announced on
  June 8, 2026, with accepted workshop CFPs posted before June 15, 2026.
- I did not find a public CoNEXT 2026 DIN/decentralization-workshop CFP during
  the 2026-06-11 check. The correct next action is to watch the official
  CoNEXT workshop list/CFPs and submit to the closest accepted workshop once
  its page is live.

Verified source pages:

- Official CoNEXT 2026 site: `https://conferences.sigcomm.org/co-next/2026/`
- Official CoNEXT 2026 CFP: `https://conferences.sigcomm.org/co-next/2026/#!/cfp`
- Official CoNEXT 2026 submission instructions:
  `https://conferences.sigcomm.org/co-next/2026/#!/submission`
- Official CoNEXT 2026 workshop proposal call:
  `https://conferences.sigcomm.org/co-next/2026/#!/cfw`
- Official CoNEXT 2026 artifact evaluation page:
  `https://conferences.sigcomm.org/co-next/2026/#!/artifact-submission`
- DIN precedent at CoNEXT 2024:
  `https://www.ietf.org/blog/acm-din-workshop-2024/`

Package contents:

- `VENUE-STATUS.md`: current venue facts and implications.
- `SUBMISSION-HANDOFF.md`: exact remaining human/editorial work.
- `SUBMISSION-SUMMARY.md`: title, abstract, keywords, and fit statement.
- `package.sh`: creates a local share bundle under `dist/`.

Build the package from the repository root:

```sh
paper/workshop/conext2026/package.sh
```

The generated `dist/` bundle is intentionally ignored. It contains the current
paper PDF, LaTeX source, bibliography, committed derived tables, and this W6
handoff material.
