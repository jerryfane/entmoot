#!/usr/bin/env python3
"""Derive convergence evidence from the Entmoot live-evaluation artifacts.

The script reads raw run artifacts and writes derived tables only. Snapshot
timestamps are coarse monitoring checkpoints; all recovery windows reported
here are bounds at snapshot granularity, not message-delivery latencies.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_RUN_DIR = Path(
    "artifacts/evaluation/20260603T212611Z-3-7-day-evaluation"
)
DEFAULT_OUT_DIR = Path("paper/generated/evaluation")
FINAL_MEMBERS = 4
FINAL_MESSAGES = 78
PARTICIPANT_ORDER = [
    "local-vps",
    "hermes-container",
    "deimos-openclaw-container",
    "phobos-pi-hermes",
]


def parse_snapshot(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def read_snapshot_health(run_dir: Path) -> list[dict[str, str]]:
    path = run_dir / "analysis" / "snapshot-health.tsv"
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def int_or_none(value: str) -> int | None:
    if value == "None" or value == "":
        return None
    return int(value)


def is_healthy(row: dict[str, str]) -> bool:
    return row["doctor_status"] == "0" and row["peers_status"] == "0"


def has_full_state(row: dict[str, str]) -> bool:
    return (
        int_or_none(row["controlled_members"]) == FINAL_MEMBERS
        and int_or_none(row["controlled_messages"]) == FINAL_MESSAGES
    )


def write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=fieldnames, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def convergence_timeline(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    first_ts = min(parse_snapshot(row["snapshot"]) for row in rows)
    output = []
    for row in rows:
        ts = parse_snapshot(row["snapshot"])
        output.append(
            {
                "snapshot": row["snapshot"],
                "elapsed_hours": f"{(ts - first_ts).total_seconds() / 3600:.3f}",
                "participant": row["participant"],
                "doctor_status": row["doctor_status"],
                "peers_status": row["peers_status"],
                "controlled_members": row["controlled_members"],
                "controlled_messages": row["controlled_messages"],
                "healthy": str(is_healthy(row)),
                "full_controlled_state": str(has_full_state(row)),
            }
        )
    return output


def final_state(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    final_snapshot = max(row["snapshot"] for row in rows)
    output = []
    for row in rows:
        if row["snapshot"] != final_snapshot:
            continue
        output.append(
            {
                "snapshot": final_snapshot,
                "participant": row["participant"],
                "doctor_status": row["doctor_status"],
                "peers_status": row["peers_status"],
                "controlled_members": row["controlled_members"],
                "controlled_messages": row["controlled_messages"],
                "healthy": str(is_healthy(row)),
                "full_controlled_state": str(has_full_state(row)),
            }
        )
    return sorted(output, key=lambda r: PARTICIPANT_ORDER.index(str(r["participant"])))


def convergence_plot(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    first_ts = min(parse_snapshot(row["snapshot"]) for row in rows)
    by_snapshot: dict[str, dict[str, str]] = defaultdict(dict)
    for row in rows:
        by_snapshot[row["snapshot"]][row["participant"]] = row["controlled_messages"]

    output: list[dict[str, object]] = []
    for snapshot in sorted(by_snapshot):
        ts = parse_snapshot(snapshot)
        item: dict[str, object] = {
            "snapshot": snapshot,
            "elapsed_hours": f"{(ts - first_ts).total_seconds() / 3600:.3f}",
        }
        for participant in PARTICIPANT_ORDER:
            value = by_snapshot[snapshot].get(participant, "None")
            item[participant] = "nan" if value in ("None", "") else value
        output.append(item)
    return output


def phobos_recovery_bound(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    phobos = [row for row in rows if row["participant"] == "phobos-pi-hermes"]
    streaks: list[list[dict[str, str]]] = []
    current: list[dict[str, str]] = []
    for row in phobos:
        if not is_healthy(row):
            current.append(row)
        elif current:
            streaks.append(current)
            current = []
    if current:
        streaks.append(current)

    longest = max(streaks, key=len)
    start = longest[0]
    last_unhealthy = longest[-1]
    last_index = phobos.index(last_unhealthy)
    first_full = next(row for row in phobos[last_index + 1 :] if has_full_state(row))
    start_ts = parse_snapshot(start["snapshot"])
    last_ts = parse_snapshot(last_unhealthy["snapshot"])
    full_ts = parse_snapshot(first_full["snapshot"])
    return [
        {
            "participant": "phobos-pi-hermes",
            "unhealthy_streak_rows": len(longest),
            "unhealthy_start_snapshot": start["snapshot"],
            "last_unhealthy_snapshot": last_unhealthy["snapshot"],
            "first_full_state_snapshot": first_full["snapshot"],
            "unhealthy_span_hours": f"{(last_ts - start_ts).total_seconds() / 3600:.3f}",
            "recovery_bound_seconds": int((full_ts - last_ts).total_seconds()),
            "recovery_bound_minutes": f"{(full_ts - last_ts).total_seconds() / 60:.2f}",
            "first_full_members": first_full["controlled_members"],
            "first_full_messages": first_full["controlled_messages"],
            "measurement_note": "snapshot-granularity bound, not delivery latency",
        }
    ]


def read_jsonl_message_ids(paths: list[Path]) -> set[str]:
    ids: set[str] = set()
    for path in paths:
        with path.open() as fh:
            for line in fh:
                if not line.strip():
                    continue
                obj = json.loads(line)
                message_id = obj.get("message_id")
                if message_id:
                    ids.add(message_id)
    return ids


def latest_pre_live_dirs(run_dir: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for participant in PARTICIPANT_ORDER:
        dirs = sorted((run_dir / participant).glob("transcript-pre-live-*"))
        if dirs:
            result[participant] = dirs[-1]
    return result


def final_smoke_query_files(run_dir: Path, final_snapshot: str) -> dict[str, list[Path]]:
    result: dict[str, list[Path]] = {}
    snapshot_dir = run_dir / "snapshots" / final_snapshot
    for participant in PARTICIPANT_ORDER:
        path = snapshot_dir / participant / "smoke-query.stdout"
        if path.exists():
            result[participant] = [path]
    return result


def message_id_diffs(run_dir: Path, snapshots: list[dict[str, str]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    pre_live = {
        participant: sorted(path.glob("messages-*.jsonl"))
        for participant, path in latest_pre_live_dirs(run_dir).items()
    }
    rows.extend(diff_scope("latest-pre-live-transcripts", pre_live))

    final_snapshot = max(row["snapshot"] for row in snapshots)
    rows.extend(diff_scope("final-smoke-query", final_smoke_query_files(run_dir, final_snapshot)))
    return rows


def diff_scope(scope: str, participant_paths: dict[str, list[Path]]) -> list[dict[str, object]]:
    by_participant = {
        participant: read_jsonl_message_ids(paths)
        for participant, paths in participant_paths.items()
    }
    union: set[str] = set()
    for ids in by_participant.values():
        union.update(ids)

    output: list[dict[str, object]] = []
    for participant in PARTICIPANT_ORDER:
        if participant not in by_participant:
            continue
        ids = by_participant[participant]
        missing = sorted(union - ids)
        output.append(
            {
                "scope": scope,
                "participant": participant,
                "observed_ids": len(ids),
                "union_ids": len(union),
                "missing_ids": len(missing),
                "complete": str(len(missing) == 0),
                "missing_id_sample": ",".join(missing[:3]),
            }
        )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    snapshots = read_snapshot_health(args.run_dir)
    write_tsv(
        args.out_dir / "convergence-timeline.tsv",
        [
            "snapshot",
            "elapsed_hours",
            "participant",
            "doctor_status",
            "peers_status",
            "controlled_members",
            "controlled_messages",
            "healthy",
            "full_controlled_state",
        ],
        convergence_timeline(snapshots),
    )
    write_tsv(
        args.out_dir / "phobos-recovery-bound.tsv",
        [
            "participant",
            "unhealthy_streak_rows",
            "unhealthy_start_snapshot",
            "last_unhealthy_snapshot",
            "first_full_state_snapshot",
            "unhealthy_span_hours",
            "recovery_bound_seconds",
            "recovery_bound_minutes",
            "first_full_members",
            "first_full_messages",
            "measurement_note",
        ],
        phobos_recovery_bound(snapshots),
    )
    write_tsv(
        args.out_dir / "final-controlled-state.tsv",
        [
            "snapshot",
            "participant",
            "doctor_status",
            "peers_status",
            "controlled_members",
            "controlled_messages",
            "healthy",
            "full_controlled_state",
        ],
        final_state(snapshots),
    )
    write_tsv(
        args.out_dir / "convergence-plot.tsv",
        ["snapshot", "elapsed_hours", *PARTICIPANT_ORDER],
        convergence_plot(snapshots),
    )
    write_tsv(
        args.out_dir / "message-id-set-diffs.tsv",
        [
            "scope",
            "participant",
            "observed_ids",
            "union_ids",
            "missing_ids",
            "complete",
            "missing_id_sample",
        ],
        message_id_diffs(args.run_dir, snapshots),
    )


if __name__ == "__main__":
    main()
