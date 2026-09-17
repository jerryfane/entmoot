# Evaluation Artifacts

This directory is reserved for live Entmoot evaluation output.

Raw run directories are ignored by default:

```text
artifacts/evaluation/<run-id>/
```

Raw artifacts may contain message text, node IDs, hostnames, private group
metadata, operational logs, local paths, or other sensitive information. Do not
commit run output unless it has been reviewed and intentionally sanitized for
the research paper.

Tracked files in this directory are policy files only:

- `.gitignore` keeps raw output out of git by default.
- `README.md` documents the artifact policy.

The live-agent study runbook this pointed at was deleted with the
`agent-live` feature in 1.5.85. The `scripts/eval/*` tooling here does not
depend on that feature and still runs.
