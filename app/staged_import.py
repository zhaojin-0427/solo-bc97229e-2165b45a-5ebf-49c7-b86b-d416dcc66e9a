"""Staged (two-phase) correction-import service.

The finance engineer first *previews* a one-side CSV: the parsed rows are
saved as a batch in dedicated staging tables without touching any formal flow,
match group or candidate. The preview lists every row as ``added`` /
``changed`` / ``unchanged`` and reports the original values of all members of
every confirmed group that confirmation would disband.

A later confirmation by batch id applies the whole batch through the same
core as the immediate upload (per-side capacity, identical-content no-op,
whole-group disbanding) inside one serialised write transaction. Before
applying anything it re-validates every *watched* flow:

* every flow referenced by the file — including serials that did not exist at
  preview time (they must still be absent);
* every member of every group listed for disbanding.

If any watched flow's original value (content hash / version), claim status
or group membership changed, the whole batch is rejected (status
``rejected``) and the conflicts are listed. Changes to unrelated flows do not
invalidate the preview while the 100-row capacity still holds.

The successful receipt, batch status and every business change become
effective together; a crash leaves no half-applied batch, and a pending batch
survives a restart because it lives in the same durable SQLite database.
Re-confirming an already confirmed batch returns the *first* frozen receipt
and never re-applies the content.
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
import threading
import uuid

from .csv_parser import MAX_ROWS_PER_SIDE
from .db import Database
from .ledger import apply_rows_in_tx, check_capacity, content_hash
from .models import (
    BANK,
    BATCH_CONFIRMED,
    BATCH_REJECTED,
    BATCH_STAGED,
    INTERNAL,
    BatchConflict,
    BatchNotFound,
    BatchNotStaged,
    CapacityExceeded,
    FlowRow,
    ParsedRow,
)
from .repositories import flow_dict, get_flow


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _new_batch_id() -> str:
    return uuid.uuid4().hex


def _watch_snapshot(flow: FlowRow | None, disband_group_id: int | None) -> dict:
    """Snapshot the exact fields confirm-time validation compares against."""
    if flow is None:
        return {
            "existed": 0,
            "version": None,
            "status": None,
            "group_id": None,
            "content_hash": None,
            "disband_group_id": disband_group_id,
        }
    return {
        "existed": 1,
        "version": flow.version,
        "status": flow.status,
        "group_id": flow.group_id,
        "content_hash": flow.content_hash,
        "disband_group_id": disband_group_id,
    }


def _group_members(conn, group_id: int) -> list[tuple[str, str]]:
    """All current (side, serial) members of a confirmed group."""
    out: list[tuple[str, str]] = []
    for side, table in ((BANK, "bank_flows"), (INTERNAL, "internal_flows")):
        rows = conn.execute(
            f"SELECT serial FROM {table} WHERE group_id = ?", (group_id,)
        ).fetchall()
        out.extend((side, r["serial"]) for r in rows)
    return out


def _conflict_for(
    side: str,
    serial: str,
    current: FlowRow | None,
    snapshot: sqlite3.Row,
    disband_group_id: int | None,
) -> dict:
    conflict: dict = {
        "side": side,
        "serial": serial,
        "reason": None,
        "preview": {
            "existed": bool(snapshot["existed"]),
            "version": snapshot["version"],
            "status": snapshot["status"],
            "group_id": snapshot["group_id"],
            "content_hash": snapshot["content_hash"],
        },
        "current": None,
    }
    if disband_group_id is not None:
        conflict["disband_group_id"] = disband_group_id

    if not snapshot["existed"]:
        if current is not None:
            conflict["reason"] = "flow_appeared"
            conflict["current"] = flow_dict(current)
            conflict["current"]["side"] = side
        return conflict

    if current is None:
        conflict["reason"] = "flow_missing"
        return conflict

    current_dict = flow_dict(current)
    current_dict["side"] = side
    conflict["current"] = current_dict
    if current.version != snapshot["version"]:
        # The version trigger fires on any content/status/group change; the
        # detailed reason makes the conflict self-explanatory.
        if current.content_hash != snapshot["content_hash"]:
            conflict["reason"] = "flow_changed"
        elif current.status != snapshot["status"]:
            conflict["reason"] = "status_changed"
        elif current.group_id != snapshot["group_id"]:
            conflict["reason"] = "membership_changed"
        else:
            conflict["reason"] = "flow_changed"
    elif (
        current.status != snapshot["status"]
        or current.group_id != snapshot["group_id"]
        or current.content_hash != snapshot["content_hash"]
    ):  # pragma: no cover - defensive: any change bumps version via trigger
        conflict["reason"] = "flow_changed"
    return conflict


# --------------------------------------------------------------------------
# Preview (phase 1)
# --------------------------------------------------------------------------

def create_batch(
    db: Database, side: str, rows: list[ParsedRow], filename: str | None
) -> dict:
    """Validate and persist a staged preview; formal data is never modified."""
    batch_id = _new_batch_id()
    with db.write_tx() as conn:
        # Capacity is enforced at preview time as well, using the same formula
        # as an immediate upload (a 422 means no batch is created).
        check_capacity(conn, side, rows)

        watched: dict[tuple[str, str], dict] = {}
        groups_to_disband: set[int] = set()

        stored_rows: list[dict] = []
        added = changed = unchanged = 0

        for ordinal, row in enumerate(rows):
            current = get_flow(conn, side, row.serial)
            new_hash = content_hash(row)
            disband_group_id: int | None = None

            if current is None:
                change_type = "added"
                added += 1
            elif current.content_hash == new_hash:
                change_type = "unchanged"
                unchanged += 1
            else:
                change_type = "changed"
                changed += 1
                if current.group_id is not None:
                    disband_group_id = current.group_id
                    groups_to_disband.add(current.group_id)

            stored_rows.append(
                {
                    "ord": ordinal,
                    "serial": row.serial,
                    "name": row.name,
                    "txn_date": row.txn_date,
                    "amount": row.amount,
                    "content_hash": new_hash,
                    "change_type": change_type,
                    "disband_group_id": disband_group_id,
                }
            )
            # Every file row is watched — even new serials (existence check).
            watched[(side, row.serial)] = _watch_snapshot(
                current, disband_group_id
            )

        # Every member of every disbanding group is watched, with the original
        # values that the engineer must review before confirming.
        for group_id in sorted(groups_to_disband):
            for member_side, member_serial in _group_members(conn, group_id):
                member = get_flow(conn, member_side, member_serial)
                key = (member_side, member_serial)
                if key in watched:
                    # A file row already covers this member; keep its entry but
                    # remember the group it belongs to.
                    watched[key]["disband_group_id"] = group_id
                else:
                    watched[key] = _watch_snapshot(member, group_id)

        conn.execute(
            "INSERT INTO import_batches(id, side, filename, status, received, "
            "added_count, changed_count, unchanged_count, disbanded_group_ids) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_id,
                side,
                filename,
                BATCH_STAGED,
                len(rows),
                added,
                changed,
                unchanged,
                json.dumps(sorted(groups_to_disband)),
            ),
        )
        for r in stored_rows:
            conn.execute(
                "INSERT INTO import_batch_rows(batch_id, ord, serial, name, "
                "txn_date, amount, content_hash, change_type, disband_group_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    r["ord"],
                    r["serial"],
                    r["name"],
                    r["txn_date"],
                    r["amount"],
                    r["content_hash"],
                    r["change_type"],
                    r["disband_group_id"],
                ),
            )
        for (watch_side, watch_serial), snap in watched.items():
            conn.execute(
                "INSERT INTO import_batch_watch(batch_id, side, serial, existed, "
                "version, status, group_id, content_hash, disband_group_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    watch_side,
                    watch_serial,
                    snap["existed"],
                    snap["version"],
                    snap["status"],
                    snap["group_id"],
                    snap["content_hash"],
                    snap["disband_group_id"],
                ),
            )

    return get_batch(db, batch_id)


# --------------------------------------------------------------------------
# Confirm (phase 2)
# --------------------------------------------------------------------------

def _detect_conflicts(conn, batch_id: str) -> list[dict]:
    snapshots = conn.execute(
        "SELECT * FROM import_batch_watch WHERE batch_id = ? "
        "ORDER BY side, serial",
        (batch_id,),
    ).fetchall()
    conflicts: list[dict] = []
    for snap in snapshots:
        current = get_flow(conn, snap["side"], snap["serial"])
        if not snap["existed"]:
            if current is not None:
                conflict = _conflict_for(
                    snap["side"], snap["serial"], current, snap,
                    snap["disband_group_id"],
                )
                conflicts.append(conflict)
            continue
        if current is None:
            conflicts.append(
                _conflict_for(
                    snap["side"], snap["serial"], None, snap,
                    snap["disband_group_id"],
                )
            )
            continue
        if (
            current.version != snap["version"]
            or current.status != snap["status"]
            or current.group_id != snap["group_id"]
            or current.content_hash != snap["content_hash"]
        ):
            conflicts.append(
                _conflict_for(
                    snap["side"],
                    snap["serial"],
                    current,
                    snap,
                    snap["disband_group_id"],
                )
            )
    return conflicts


def _mark_rejected(conn, batch_id: str, conflicts: list[dict]) -> dict:
    """Persist the terminal rejected state and return its full payload."""
    conn.execute(
        "UPDATE import_batches SET status = ?, conflicts_json = ?, "
        "confirmed_at = ? WHERE id = ?",
        (
            BATCH_REJECTED,
            json.dumps(conflicts, ensure_ascii=False),
            _now(),
            batch_id,
        ),
    )
    rejected_batch = conn.execute(
        "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
    ).fetchone()
    return _batch_payload(conn, rejected_batch)


class _Rejected:
    """Internal signal carrying a durably-committed rejection payload."""

    def __init__(self, payload: dict) -> None:
        self.payload = payload


# Per-batch confirmation locks. They make the validate-then-apply sequence of a
# single batch strictly serial, closing the gap between the two database
# transactions below for concurrent confirmations of the same batch.
_batch_locks: dict[str, threading.Lock] = {}
_batch_locks_guard = threading.Lock()


def _lock_for(batch_id: str) -> threading.Lock:
    with _batch_locks_guard:
        lock = _batch_locks.get(batch_id)
        if lock is None:
            lock = threading.Lock()
            _batch_locks[batch_id] = lock
        return lock


def confirm_batch(db: Database, batch_id: str) -> dict:
    """Apply a staged batch; idempotent and conflict-safe on retries.

    Two serialised write transactions are used, guarded by a per-batch lock:

    * **validation txn** — reads the batch, re-checks every watched flow and
      the per-side capacity, and on any conflict flips the batch to the
      terminal ``rejected`` state and *commits normally* (the conflict is
      surfaced as a 409 only after the transaction returns), so the rejected
      status with its conflict list is durable while no formal data changed;
    * **application txn** — re-reads the batch (a concurrent confirmation may
      have finished) and applies every row through the same core as an
      immediate upload, freezing the receipt and flipping the status in the
      same transaction. The receipt, batch status and all business changes
      therefore commit together: a crash leaves no half-applied batch.

    The per-batch lock means no other confirmation of this batch can interleave
    between the two transactions; different batches never block each other.
    """
    result = _confirm_batch(db, batch_id)
    if isinstance(result, _Rejected):
        # The terminal rejected state is already committed durably.
        raise BatchConflict(
            f"批次 {batch_id} 预审后存在 {len(result.payload['conflicts'])} 项冲突，"
            "整批拒绝",
            conflicts=result.payload["conflicts"],
        )
    return result


def _confirm_batch(db: Database, batch_id: str) -> dict | _Rejected:
    lock = _lock_for(batch_id)
    with lock:
        # ---- Transaction 1: validation (terminal rejection commits here) ----
        with db.write_tx() as conn:
            batch = conn.execute(
                "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise BatchNotFound(f"暂存批次 {batch_id} 不存在")
            if batch["status"] == BATCH_CONFIRMED:
                # First success already frozen: return it without touching
                # any business data.
                return _batch_payload(conn, batch)
            if batch["status"] == BATCH_REJECTED:
                raise BatchNotStaged(
                    f"暂存批次 {batch_id} 已因冲突被拒绝，请重新预审生成新批次"
                )

            side = batch["side"]
            conflicts = _detect_conflicts(conn, batch_id)
            if conflicts:
                # Commit the terminal status first; the 409 is raised only
                # after this transaction returns cleanly.
                return _Rejected(_mark_rejected(conn, batch_id, conflicts))

            row_records = conn.execute(
                "SELECT * FROM import_batch_rows WHERE batch_id = ? ORDER BY ord",
                (batch_id,),
            ).fetchall()
            parsed_rows = [
                ParsedRow(
                    serial=r["serial"],
                    name=r["name"],
                    txn_date=r["txn_date"],
                    amount=r["amount"],
                )
                for r in row_records
            ]
            # Unrelated flows may have been added meanwhile: a cap violation
            # is a listed conflict that rejects the terminal batch.
            try:
                check_capacity(conn, side, parsed_rows)
            except CapacityExceeded as exc:
                capacity_conflict = [
                    {
                        "side": side,
                        "serial": None,
                        "reason": "capacity_exceeded",
                        "message": str(exc),
                        "capacity": MAX_ROWS_PER_SIDE,
                    }
                ]
                return _Rejected(_mark_rejected(conn, batch_id, capacity_conflict))

        # ---- Transaction 2: atomic application + receipt freeze -------------
        # Reached only when validation passed and its transaction committed.
        with db.write_tx() as conn:
            batch = conn.execute(
                "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch["status"] == BATCH_CONFIRMED:
                return _batch_payload(conn, batch)
            if batch["status"] == BATCH_REJECTED:
                raise BatchNotStaged(
                    f"暂存批次 {batch_id} 已因冲突被拒绝，请重新预审生成新批次"
                )

            row_records = conn.execute(
                "SELECT * FROM import_batch_rows WHERE batch_id = ? ORDER BY ord",
                (batch_id,),
            ).fetchall()
            parsed_rows = [
                ParsedRow(
                    serial=r["serial"],
                    name=r["name"],
                    txn_date=r["txn_date"],
                    amount=r["amount"],
                )
                for r in row_records
            ]
            side = batch["side"]

            # Defensive re-validation inside the application transaction. With
            # the process-wide SQLite write lock plus the per-batch lock no
            # writer can have drifted here; this turns any unexpected change
            # into a clean committed rejection rather than an inconsistent
            # application.
            late_conflicts = _detect_conflicts(conn, batch_id)
            if late_conflicts:
                return _Rejected(_mark_rejected(conn, batch_id, late_conflicts))
            try:
                check_capacity(conn, side, parsed_rows)
            except CapacityExceeded as exc:
                capacity_conflict = [
                    {
                        "side": side,
                        "serial": None,
                        "reason": "capacity_exceeded",
                        "message": str(exc),
                        "capacity": MAX_ROWS_PER_SIDE,
                    }
                ]
                return _Rejected(_mark_rejected(conn, batch_id, capacity_conflict))

            outcome = apply_rows_in_tx(conn, side, parsed_rows)

            receipt = {
                "batch_id": batch_id,
                "side": side,
                "filename": batch["filename"],
                "status": BATCH_CONFIRMED,
                "received": batch["received"],
                "added": batch["added_count"],
                "changed": batch["changed_count"],
                "unchanged": batch["unchanged_count"],
                "applied_upserted": outcome["upserted"],
                "applied_unchanged": outcome["unchanged"],
                "disbanded_groups": outcome["disbanded_groups"],
                "serials": outcome["serials"],
                "confirmed_at": _now(),
            }
            conn.execute(
                "UPDATE import_batches SET status = ?, receipt_json = ?, "
                "confirmed_at = ? WHERE id = ?",
                (
                    BATCH_CONFIRMED,
                    json.dumps(receipt, ensure_ascii=False),
                    receipt["confirmed_at"],
                    batch_id,
                ),
            )
            refreshed = conn.execute(
                "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            return _batch_payload(conn, refreshed)


# --------------------------------------------------------------------------
# Queries
# --------------------------------------------------------------------------

def _rows_payload(conn, batch_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_batch_rows WHERE batch_id = ? ORDER BY ord",
        (batch_id,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "ord": r["ord"],
                "serial": r["serial"],
                "name": r["name"],
                "txn_date": r["txn_date"],
                "amount": r["amount"],
                "change_type": r["change_type"],
                "disband_group_id": r["disband_group_id"],
                "staged_content_hash": r["content_hash"],
            }
        )
    return out


def _watch_payload(conn, batch_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_batch_watch WHERE batch_id = ? "
        "ORDER BY disband_group_id IS NULL, disband_group_id, side, serial",
        (batch_id,),
    ).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "side": r["side"],
                "serial": r["serial"],
                "existed": bool(r["existed"]),
                "version": r["version"],
                "status": r["status"],
                "group_id": r["group_id"],
                "content_hash": r["content_hash"],
                "disband_group_id": r["disband_group_id"],
            }
        )
    return out


def _disband_preview(conn, batch_id: str, group_ids: list[int]) -> list[dict]:
    """Original values of every member of every group that will be undone."""
    if not group_ids:
        return []
    result = []
    for group_id in group_ids:
        members = []
        for side, table in ((BANK, "bank_flows"), (INTERNAL, "internal_flows")):
            for r in conn.execute(
                f"SELECT * FROM {table} WHERE group_id = ? ORDER BY serial",
                (group_id,),
            ).fetchall():
                item = flow_dict(_row_like(r))
                item["side"] = side
                members.append(item)
        result.append({"group_id": group_id, "members": members})
    return result


def _row_like(row) -> FlowRow:
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


def _batch_payload(conn, batch_row) -> dict:
    batch_id = batch_row["id"]
    disbanded_ids = json.loads(batch_row["disbanded_group_ids"])
    payload = {
        "batch_id": batch_id,
        "side": batch_row["side"],
        "filename": batch_row["filename"],
        "status": batch_row["status"],
        "received": batch_row["received"],
        "added": batch_row["added_count"],
        "changed": batch_row["changed_count"],
        "unchanged": batch_row["unchanged_count"],
        "created_at": batch_row["created_at"],
        "confirmed_at": batch_row["confirmed_at"],
        "rows": _rows_payload(conn, batch_id),
        "watched": _watch_payload(conn, batch_id),
        "disband_groups": _disband_preview(conn, batch_id, disbanded_ids),
        "disbanded_group_ids": disbanded_ids,
        "conflicts": json.loads(batch_row["conflicts_json"]),
        "receipt": (
            json.loads(batch_row["receipt_json"])
            if batch_row["receipt_json"]
            else None
        ),
    }
    return payload


def _batch_payload_with(db: Database, batch_id: str) -> dict:
    with db.read_tx() as conn:
        batch = conn.execute(
            "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise BatchNotFound(f"暂存批次 {batch_id} 不存在")
        return _batch_payload(conn, batch)


def get_batch(db: Database, batch_id: str) -> dict:
    return _batch_payload_with(db, batch_id)


def list_batches(db: Database) -> list[dict]:
    with db.read_tx() as conn:
        rows = conn.execute(
            "SELECT * FROM import_batches ORDER BY created_at DESC, rowid DESC"
        ).fetchall()
        summaries = []
        for r in rows:
            summaries.append(
                {
                    "batch_id": r["id"],
                    "side": r["side"],
                    "filename": r["filename"],
                    "status": r["status"],
                    "received": r["received"],
                    "added": r["added_count"],
                    "changed": r["changed_count"],
                    "unchanged": r["unchanged_count"],
                    "disbanded_group_ids": json.loads(r["disbanded_group_ids"]),
                    "created_at": r["created_at"],
                    "confirmed_at": r["confirmed_at"],
                    "conflict_count": len(json.loads(r["conflicts_json"])),
                }
            )
        return summaries
