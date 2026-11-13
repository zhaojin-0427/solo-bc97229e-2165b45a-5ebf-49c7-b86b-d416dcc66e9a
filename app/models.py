"""Framework-agnostic data models shared by the service layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

BANK = "bank"
INTERNAL = "internal"

UNMATCHED = "unmatched"
MATCHED = "matched"


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
