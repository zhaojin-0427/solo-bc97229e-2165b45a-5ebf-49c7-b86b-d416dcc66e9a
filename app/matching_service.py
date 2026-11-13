"""Confirmation and query service.

Confirming a candidate re-validates everything against the *latest* raw values
and claim status inside one ``BEGIN IMMEDIATE`` transaction:

* candidate exists and is not stale;
* every flow still carries the version the page saw;
* all flows are still unmatched;
* the business rule (same name, date within 2 days, equal total amount) still
  holds.

Only one concurrent confirmation of candidates sharing a flow can succeed;
the loser gets HTTP 409 together with the current values so the old page can
be refreshed. A single transaction flips all members and writes the group, so
a crash can never leave a one-sided match; committed groups survive restarts.
"""

from __future__ import annotations

import json
import sqlite3

from .db import Database
from .matching import validate_combination
from .models import (
    BANK,
    INTERNAL,
    MATCHED,
    UNMATCHED,
    FlowRow,
    NotFound,
    StaleCandidateError,
)
from .repositories import flow_dict, get_flow, load_flows


def _candidate_row(conn: sqlite3.Connection, candidate_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM candidates WHERE id = ?", (candidate_id,)
    ).fetchone()
    if row is None:
        raise NotFound(f"候选组合 {candidate_id} 不存在（可能已被更正或重新计算）")
    return row


def _candidate_items(
    conn: sqlite3.Connection, candidate_id: int
) -> tuple[str, list[str]]:
    rows = conn.execute(
        "SELECT side, flow_serial, ord FROM candidate_items "
        "WHERE candidate_id = ? ORDER BY side, ord",
        (candidate_id,),
    ).fetchall()
    bank = next(r["flow_serial"] for r in rows if r["side"] == BANK)
    internals = [r["flow_serial"] for r in rows if r["side"] == INTERNAL]
    return bank, internals


def _latest_payload(
    conn: sqlite3.Connection, bank_serial: str, internal_serials: list[str]
) -> dict:
    payload: dict = {"flows": []}
    bank = get_flow(conn, BANK, bank_serial)
    if bank:
        item = flow_dict(bank)
        item["side"] = BANK
        payload["flows"].append(item)
    for serial in internal_serials:
        flow = get_flow(conn, INTERNAL, serial)
        if flow:
            item = flow_dict(flow)
            item["side"] = INTERNAL
            payload["flows"].append(item)
    return payload


def confirm_candidate(
    db: Database,
    candidate_id: int,
    expected_versions: dict[str, int] | None = None,
) -> dict:
    """Atomically confirm a candidate; raises StaleCandidateError on conflict."""
    expected_versions = expected_versions or {}

    with db.write_tx() as conn:
        cand = _candidate_row(conn, candidate_id)
        bank_serial, internal_serials = _candidate_items(conn, candidate_id)

        if cand["stale"]:
            raise StaleCandidateError(
                f"候选组合 {candidate_id} 已过时：原始流水发生了变化，请刷新候选列表",
                latest=_latest_payload(conn, bank_serial, internal_serials),
            )

        bank = get_flow(conn, BANK, bank_serial)
        internals = [get_flow(conn, INTERNAL, s) for s in internal_serials]
        if bank is None or any(f is None for f in internals):
            raise StaleCandidateError(
                "候选组合引用的流水已不存在，请刷新候选列表",
                latest=_latest_payload(conn, bank_serial, internal_serials),
            )
        internals: list[FlowRow] = internals  # type: ignore[assignment]

        # 1) Old page must check against the latest versions.
        snapshot_versions = {bank.serial: cand["bank_version"]}
        snapshot_versions.update(json.loads(cand["internal_versions"]))
        for serial, expected in expected_versions.items():
            current_version = snapshot_versions.get(serial)
            if current_version is None:
                raise StaleCandidateError(
                    f"页面数据引用了不属于该组合的流水 {serial}，请刷新",
                    latest=_latest_payload(conn, bank_serial, internal_serials),
                )
            if int(expected) != current_version:
                raise StaleCandidateError(
                    f"流水 {serial} 的版本已变化（页面 v{expected} → "
                    f"当前 v{current_version}），请核对最新原始值与认领状态",
                    latest=_latest_payload(conn, bank_serial, internal_serials),
                )

        # 2) Latest raw values must still match the stored snapshot.
        if bank.version != cand["bank_version"] or any(
            f.version != snapshot_versions[f.serial] for f in internals
        ):
            raise StaleCandidateError(
                "候选组合基于过时的流水快照，迟到的计算结果不可确认",
                latest=_latest_payload(conn, bank_serial, internal_serials),
            )

        # 3) Nothing may already be claimed (mutual exclusion of shared flows).
        if bank.status != UNMATCHED or any(f.status != UNMATCHED for f in internals):
            claimed = [bank.serial] if bank.status == MATCHED else []
            claimed += [f.serial for f in internals if f.status == MATCHED]
            raise StaleCandidateError(
                "以下流水已被其他组合认领: " + ", ".join(claimed),
                latest=_latest_payload(conn, bank_serial, internal_serials),
            )

        # 4) Re-check the business rules against current values.
        try:
            validate_combination(bank, internals)
        except ValueError as exc:
            raise StaleCandidateError(
                f"最新数据不再满足匹配规则: {exc}",
                latest=_latest_payload(conn, bank_serial, internal_serials),
            ) from exc

        cur = conn.execute(
            "INSERT INTO match_groups(candidate_id) VALUES (?)", (candidate_id,)
        )
        group_id = cur.lastrowid
        conn.execute(
            "UPDATE bank_flows SET status = ?, group_id = ? WHERE serial = ?",
            (MATCHED, group_id, bank.serial),
        )
        for flow in internals:
            conn.execute(
                "UPDATE internal_flows SET status = ?, group_id = ? WHERE serial = ?",
                (MATCHED, group_id, flow.serial),
            )
        # The confirmed combination and every other combination sharing one of
        # its flows is now outdated; recomputation will only keep what remains
        # genuinely confirmable.
        conn.execute(
            "UPDATE candidates SET stale = 1 WHERE id IN ("
            "  SELECT DISTINCT ci.candidate_id FROM candidate_items ci"
            "  WHERE (ci.side = ? AND ci.flow_serial = ?)"
            "     OR (ci.side = ? AND ci.flow_serial IN ("
            "          SELECT flow_serial FROM candidate_items"
            "          WHERE candidate_id = ? AND side = ?)))",
            (BANK, bank.serial, INTERNAL, candidate_id, INTERNAL),
        )

    return get_group(db, group_id)


def _hydrate_candidate(conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
    bank_serial, internal_serials = _candidate_items(conn, row["id"])
    bank = get_flow(conn, BANK, bank_serial)
    internals = [get_flow(conn, INTERNAL, s) for s in internal_serials]
    return {
        "candidate_id": row["id"],
        "candidate_key": row["candidate_key"],
        "stale": bool(row["stale"]),
        "generation": row["generation"],
        "total_amount": row["total_amount"],
        "bank": flow_dict(bank) if bank else None,
        "internals": [flow_dict(f) for f in internals if f is not None],
        "expected_versions": {
            bank_serial: row["bank_version"],
            **json.loads(row["internal_versions"]),
        },
    }


def list_candidates(
    db: Database, include_stale: bool = False, bank_serial: str | None = None
) -> list[dict]:
    with db.read_tx() as conn:
        sql = "SELECT * FROM candidates WHERE 1=1"
        params: list = []
        if not include_stale:
            sql += " AND stale = 0"
        if bank_serial:
            sql += " AND bank_serial = ?"
            params.append(bank_serial)
        sql += " ORDER BY bank_serial, id"
        rows = conn.execute(sql, params).fetchall()
        return [_hydrate_candidate(conn, r) for r in rows]


def get_group(db: Database, group_id: int) -> dict:
    with db.read_tx() as conn:
        group = conn.execute(
            "SELECT * FROM match_groups WHERE id = ?", (group_id,)
        ).fetchone()
        if group is None:
            raise NotFound(f"匹配组 {group_id} 不存在")
        bank_rows = conn.execute(
            "SELECT * FROM bank_flows WHERE group_id = ? ORDER BY serial",
            (group_id,),
        ).fetchall()
        internal_rows = conn.execute(
            "SELECT * FROM internal_flows WHERE group_id = ? ORDER BY serial",
            (group_id,),
        ).fetchall()
        return {
            "group_id": group_id,
            "candidate_id": group["candidate_id"],
            "created_at": group["created_at"],
            "bank": [flow_dict(_row_like(r)) for r in bank_rows],
            "internals": [flow_dict(_row_like(r)) for r in internal_rows],
        }


def list_groups(db: Database) -> list[dict]:
    with db.read_tx() as conn:
        ids = [
            r["id"]
            for r in conn.execute(
                "SELECT id FROM match_groups ORDER BY id"
            ).fetchall()
        ]
    return [get_group(db, gid) for gid in ids]


def list_unmatched(db: Database) -> dict:
    with db.read_tx() as conn:
        bank = [flow_dict(f) for f in load_flows(conn, BANK) if f.status == UNMATCHED]
        internal = [
            flow_dict(f) for f in load_flows(conn, INTERNAL) if f.status == UNMATCHED
        ]
    return {"bank": bank, "internal": internal}


def list_flows(db: Database, side: str) -> list[dict]:
    with db.read_tx() as conn:
        return [flow_dict(f) for f in load_flows(conn, side)]


def _row_like(row: sqlite3.Row) -> FlowRow:
    return FlowRow(
        serial=row["serial"],
        name=row["name"],
        txn_date=row["txn_date"],
        amount=row["amount"],
        status=row["status"],
        group_id=row["group_id"],
        version=row["version"],
        content_hash=row["content_hash"],
        updated_at=row["updated_at"],
    )
