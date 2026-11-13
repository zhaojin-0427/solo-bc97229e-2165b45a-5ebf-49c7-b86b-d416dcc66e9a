"""Thin data-access helpers mapping SQLite rows to domain models."""

from __future__ import annotations

import sqlite3

from .models import BANK, FlowRow

_FLOW_COLUMNS = (
    "serial, name, txn_date, amount, status, group_id, version, "
    "content_hash, updated_at"
)


def _to_flow(row: sqlite3.Row) -> FlowRow:
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


def table_for(side: str) -> str:
    return "bank_flows" if side == BANK else "internal_flows"


def load_flows(conn: sqlite3.Connection, side: str) -> list[FlowRow]:
    table = table_for(side)
    rows = conn.execute(
        f"SELECT {_FLOW_COLUMNS} FROM {table} ORDER BY serial"
    ).fetchall()
    return [_to_flow(r) for r in rows]


def get_flow(conn: sqlite3.Connection, side: str, serial: str) -> FlowRow | None:
    table = table_for(side)
    row = conn.execute(
        f"SELECT {_FLOW_COLUMNS} FROM {table} WHERE serial = ?", (serial,)
    ).fetchone()
    return _to_flow(row) if row else None


def flow_dict(flow: FlowRow) -> dict:
    return {
        "serial": flow.serial,
        "name": flow.name,
        "txn_date": flow.txn_date,
        "amount": flow.amount,
        "status": flow.status,
        "group_id": flow.group_id,
        "version": flow.version,
        "content_hash": flow.content_hash,
        "updated_at": flow.updated_at,
    }
