"""Framework-agnostic data models shared by the service layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

BANK = "bank"
INTERNAL = "internal"

UNMATCHED = "unmatched"
MATCHED = "matched"

# Staged-import batch lifecycle.
BATCH_STAGED = "staged"          # preview stored, awaiting confirmation
BATCH_CONFIRMED = "confirmed"    # whole batch applied; receipt frozen
BATCH_REJECTED = "rejected"      # confirm-time conflict check failed

# Per-row change classification produced by a staged preview.
CHANGE_ADDED = "added"           # serial does not exist yet
CHANGE_CHANGED = "changed"       # serial exists with different content
CHANGE_UNCHANGED = "unchanged"   # serial exists with identical content hash


@dataclass(frozen=True)
class FlowRow:
    serial: str
    name: str
    txn_date: str  # ISO YYYY-MM-DD
    amount: int
    status: str
    group_id: Optional[int]
    version: int
    content_hash: str
    updated_at: str


@dataclass(frozen=True)
class ParsedRow:
    serial: str
    name: str
    txn_date: str
    amount: int


@dataclass(frozen=True)
class CandidateSpec:
    """A freshly computed candidate, snapshotting flow versions."""

    bank: FlowRow
    internals: list[FlowRow]
    candidate_key: str
    total_amount: int


@dataclass
class UploadReport:
    side: str
    received: int
    upserted: int
    unchanged: int
    disbanded_groups: list[int] = field(default_factory=list)
    serials: list[str] = field(default_factory=list)


class ReconcileError(Exception):
    """Base error; ``status`` maps to an HTTP status in the API layer."""

    status = 400


class CsvFormatError(ReconcileError):
    status = 422


class StaleCandidateError(ReconcileError):
    """Candidate no longer reflects the latest raw values / claim state."""

    status = 409

    def __init__(self, message: str, latest: Optional[dict] = None) -> None:
        super().__init__(message)
        self.latest = latest or {}


class NotFound(ReconcileError):
    status = 404


class CapacityExceeded(ReconcileError):
    status = 422


class BatchConflict(ReconcileError):
    """Confirm-time validation found a changed watched flow / group member."""

    status = 409

    def __init__(self, message: str, conflicts: Optional[list] = None) -> None:
        super().__init__(message)
        self.conflicts = conflicts or []


class BatchNotFound(ReconcileError):
    status = 404


class BatchNotStaged(ReconcileError):
    """Batch is already confirmed (terminal) or rejected (terminal)."""

    status = 409
