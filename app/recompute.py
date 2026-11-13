"""Candidate recomputation and its background scheduler.

Recomputation never holds a write transaction while the subset-sum search
runs: it snapshots the unmatched flows in a short read transaction, computes
on plain Python objects, then replaces the candidate table in one short write
transaction. Any flow change that happens in between simply bumps row
versions, and the confirm step re-validates the snapshot (a *late* result can
therefore never be confirmed as a still-valid combination).
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Optional

from .db import Database
from .matching import compute_candidates
from .models import BANK, INTERNAL, CandidateSpec
from .repositories import load_flows

logger = logging.getLogger("reconcile.recompute")

GEN_KEY = "candidate_generation"


def _get_generation(conn) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (GEN_KEY,)).fetchone()
    return int(row["value"]) if row else 0


def _set_generation(conn, value: int) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (GEN_KEY, str(value)),
    )


def _upsert_candidate(conn, spec: CandidateSpec, generation: int) -> None:
    conn.execute(
        """
        INSERT INTO candidates(candidate_key, bank_serial, total_amount,
                               bank_version, internal_versions, stale,
                               generation)
        VALUES (?, ?, ?, ?, ?, 0, ?)
        ON CONFLICT(candidate_key) DO UPDATE SET
            total_amount      = excluded.total_amount,
            bank_version      = excluded.bank_version,
            internal_versions = excluded.internal_versions,
            stale             = 0,
            generation        = excluded.generation
        """,
        (
            spec.candidate_key,
            spec.bank.serial,
            spec.total_amount,
            spec.bank.version,
            json.dumps(
                {f.serial: f.version for f in spec.internals},
                ensure_ascii=False,
                sort_keys=True,
            ),
            generation,
        ),
    )
    cid = conn.execute(
        "SELECT id FROM candidates WHERE candidate_key = ?", (spec.candidate_key,)
    ).fetchone()["id"]
    conn.execute("DELETE FROM candidate_items WHERE candidate_id = ?", (cid,))
    conn.execute(
        "INSERT INTO candidate_items(candidate_id, side, flow_serial, ord) "
        "VALUES (?, ?, ?, ?)",
        (cid, BANK, spec.bank.serial, 0),
    )
    for ordinal, flow in enumerate(spec.internals, start=1):
        conn.execute(
            "INSERT INTO candidate_items(candidate_id, side, flow_serial, ord) "
            "VALUES (?, ?, ?, ?)",
            (cid, INTERNAL, flow.serial, ordinal),
        )


def recompute(db: Database) -> int:
    """Recompute all candidates; returns the new candidate generation."""
    # Snapshot outside the write transaction.
    with db.read_tx() as conn:
        bank_snapshot = load_flows(conn, BANK)
        internal_snapshot = load_flows(conn, INTERNAL)
    specs = compute_candidates(bank_snapshot, internal_snapshot)

    with db.write_tx() as conn:
        generation = _get_generation(conn) + 1
        # Every candidate not produced by this snapshot becomes stale.
        conn.execute("UPDATE candidates SET stale = 1")
        for spec in specs:
            _upsert_candidate(conn, spec, generation)
        _set_generation(conn, generation)
    logger.info("recomputed candidates: %d (generation %d)", len(specs), generation)
    return generation


class RecomputeWorker:
    """Debounced single-worker background recomputation."""

    def __init__(self, db: Database, debounce_seconds: float = 0.15) -> None:
        self.db = db
        self.debounce = debounce_seconds
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def schedule(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._run, name="recompute-worker", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self.db.close()

    def run_once_now(self) -> int:
        """Synchronous recomputation (used by the refresh endpoint / tests)."""
        self._wake.clear()
        return recompute(self.db)

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self._wake.wait(timeout=1.0):
                continue
            # Debounce: coalesce bursts of uploads.
            while self._wake.wait(timeout=self.debounce):
                self._wake.clear()
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                recompute(self.db)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("background recompute failed")
