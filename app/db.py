"""SQLite connection management and schema.

Design points:

* WAL mode + a process-wide write lock (``threading.Lock``) serialize all
  write transactions. Reads never take the lock and never block uploads /
  confirmations.
* Every connection has a monotonically increasing per-row ``version`` trigger
  installed, so optimistic-concurrency checks can detect every content or
  status change of a flow.
* Recomputation (candidate matching) only opens short read transactions and
  performs its CPU work outside of any write transaction; it therefore never
  occupies the formal database write transaction.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS bank_flows (
    serial   TEXT PRIMARY KEY,
    name     TEXT NOT NULL,
    txn_date TEXT NOT NULL,
    amount   INTEGER NOT NULL CHECK (amount > 0),
    status   TEXT NOT NULL DEFAULT 'unmatched'
                 CHECK (status IN ('unmatched', 'matched')),
    group_id INTEGER REFERENCES match_groups(id) ON DELETE SET NULL,
    version  INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS internal_flows (
    serial   TEXT PRIMARY KEY,
    name     TEXT NOT NULL,
    txn_date TEXT NOT NULL,
    amount   INTEGER NOT NULL CHECK (amount > 0),
    status   TEXT NOT NULL DEFAULT 'unmatched'
                 CHECK (status IN ('unmatched', 'matched')),
    group_id INTEGER REFERENCES match_groups(id) ON DELETE SET NULL,
    version  INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS match_groups (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id INTEGER REFERENCES candidates(id) ON DELETE SET NULL,
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS candidates (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_key  TEXT NOT NULL UNIQUE,
    bank_serial    TEXT NOT NULL,
    total_amount   INTEGER NOT NULL,
    bank_version   INTEGER NOT NULL,
    internal_versions TEXT NOT NULL,
    stale          INTEGER NOT NULL DEFAULT 0,
    generation     INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_candidates_bank ON candidates(bank_serial);

CREATE TABLE IF NOT EXISTS candidate_items (
    candidate_id  INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
    side          TEXT NOT NULL CHECK (side IN ('bank', 'internal')),
    flow_serial   TEXT NOT NULL,
    ord           INTEGER NOT NULL,
    PRIMARY KEY (candidate_id, side, flow_serial)
);
CREATE INDEX IF NOT EXISTS idx_ci_flow ON candidate_items(side, flow_serial);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_bank_version
AFTER UPDATE ON bank_flows
FOR EACH ROW
BEGIN
    UPDATE bank_flows
       SET version = version + 1,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
     WHERE serial = OLD.serial AND version = OLD.version;
END;

CREATE TRIGGER IF NOT EXISTS trg_internal_version
AFTER UPDATE ON internal_flows
FOR EACH ROW
BEGIN
    UPDATE internal_flows
       SET version = version + 1,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
     WHERE serial = OLD.serial AND version = OLD.version;
END;
"""


class Database:
    """Thread-safe holder of SQLite connections."""

    def __init__(self, path: str) -> None:
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._local = threading.local()
        self._write_lock = threading.Lock()
        self.init_schema()

    def _new_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,  # manual transaction control
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._new_conn()
            self._local.conn = conn
        return conn

    def init_schema(self) -> None:
        # Split on statement boundaries (trigger bodies contain semicolons),
        # then execute each statement inside our own write transaction.
        statements: list[str] = []
        buf = ""
        for line in _SCHEMA.splitlines():
            buf += line + "\n"
            if sqlite3.complete_statement(buf):
                stmt = buf.strip().rstrip(";").strip()
                if stmt:
                    statements.append(stmt)
                buf = ""
        with self.write_tx() as conn:
            for stmt in statements:
                conn.execute(stmt)

    @contextmanager
    def write_tx(self) -> Iterator[sqlite3.Connection]:
        """Serialised immediate write transaction (no reader/writer deadlock)."""
        conn = self.conn()
        self._write_lock.acquire()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        finally:
            self._write_lock.release()

    @contextmanager
    def read_tx(self) -> Iterator[sqlite3.Connection]:
        """Deferred read transaction; sees a single consistent snapshot."""
        conn = self.conn()
        conn.execute("BEGIN")
        try:
            yield conn
        finally:
            conn.execute("COMMIT")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
