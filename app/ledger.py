"""Upload / correction service.

Rules implemented here:

* at most 100 distinct serials per side (including rows already stored);
* same serial overwrites the raw values; re-uploading identical content is a
  no-op for that row (no version bump, no group disruption);
* correcting a flow that belongs to a confirmed group disbands the whole
  group atomically: every member returns to ``unmatched``;
* candidates touching any corrected flow, and candidates of every member of a
  disbanded group, are removed in the same write transaction, so a stale
  combination can never be confirmed.
"""

from __future__ import annotations

import hashlib
import json

from .csv_parser import MAX_ROWS_PER_SIDE
from .db import Database
from .models import (
    BANK,
    MATCHED,
    UNMATCHED,
    CapacityExceeded,
    FlowRow,
    ParsedRow,
    UploadReport,
)
from .repositories import get_flow, table_for


def content_hash(row: ParsedRow) -> str:
    payload = json.dumps(
        [row.serial, row.name, row.txn_date, row.amount],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _invalidate_candidates_for(conn, serials_by_side: dict[str, list[str]]) -> None:
    for side, serials in serials_by_side.items():
        if not serials:
            continue
        placeholders = ",".join("?" for _ in serials)
        conn.execute(
            "DELETE FROM candidates WHERE id IN ("
            "  SELECT candidate_id FROM candidate_items "
            f" WHERE side = ? AND flow_serial IN ({placeholders}))",
            [side, *serials],
        )


def _disband_group(conn, group_id: int) -> tuple[list[str], list[str]]:
    """Undo a confirmed match; all members return to unmatched.

    Returns the affected (bank serials, internal serials).
    """
    affected: dict[str, list[str]] = {BANK: [], "internal": []}
    for side, table in ((BANK, "bank_flows"), ("internal", "internal_flows")):
        rows = conn.execute(
            f"SELECT serial FROM {table} WHERE group_id = ? AND status = ?",
            (group_id, MATCHED),
        ).fetchall()
        affected[side] = [r["serial"] for r in rows]
        conn.execute(
            f"UPDATE {table} SET status = ?, group_id = NULL "
            f"WHERE group_id = ? AND status = ?",
            (UNMATCHED, group_id, MATCHED),
        )
    conn.execute("DELETE FROM match_groups WHERE id = ?", (group_id,))
    return affected[BANK], affected["internal"]


def upload(db: Database, side: str, rows: list[ParsedRow]) -> UploadReport:
    table = table_for(side)
    report = UploadReport(side=side, received=len(rows), upserted=0, unchanged=0)
    incoming_serials = {r.serial for r in rows}

    with db.write_tx() as conn:
        total = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        existing = {
            r["serial"]
            for r in conn.execute(
                f"SELECT serial FROM {table} WHERE serial IN "
                f"({','.join('?' for _ in incoming_serials)})",
                list(incoming_serials),
            ).fetchall()
        }
        if total - len(existing) + len(incoming_serials) > MAX_ROWS_PER_SIDE:
            raise CapacityExceeded(
                f"{side} 侧流水总数超过上限 {MAX_ROWS_PER_SIDE} 条"
            )

        disbanded: list[int] = []
        invalidated: dict[str, list[str]] = {BANK: [], "internal": []}

        for row in rows:
            current: FlowRow | None = get_flow(conn, side, row.serial)
            new_hash = content_hash(row)
            if current is not None and current.content_hash == new_hash:
                # Identical re-upload: nothing changes at all.
                report.unchanged += 1
                report.serials.append(row.serial)
                continue

            if current is not None and current.group_id is not None:
                # Correction of a matched flow: undo the whole group first.
                bank_members, internal_members = _disband_group(
                    conn, current.group_id
                )
                disbanded.append(current.group_id)
                invalidated[BANK].extend(bank_members)
                invalidated["internal"].extend(internal_members)

            conn.execute(
                f"""
                INSERT INTO {table}
                    (serial, name, txn_date, amount, status, group_id,
                     version, content_hash)
                VALUES (?, ?, ?, ?, '{UNMATCHED}', NULL, 1, ?)
                ON CONFLICT(serial) DO UPDATE SET
                    name         = excluded.name,
                    txn_date     = excluded.txn_date,
                    amount       = excluded.amount,
                    status       = '{UNMATCHED}',
                    group_id     = NULL,
                    content_hash = excluded.content_hash
                """,
                (row.serial, row.name, row.txn_date, row.amount, new_hash),
            )
            invalidated[side].append(row.serial)
            report.upserted += 1
            report.serials.append(row.serial)

        # Raw correction, group undo and candidate invalidation commit together.
        _invalidate_candidates_for(
            conn,
            {
                BANK: sorted(set(invalidated[BANK])),
                "internal": sorted(set(invalidated["internal"])),
            },
        )
        report.disbanded_groups = sorted(set(disbanded))

    return report
