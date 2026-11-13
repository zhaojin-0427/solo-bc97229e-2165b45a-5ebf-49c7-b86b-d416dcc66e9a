"""Candidate matching engine (pure, side-effect free).

A candidate is one bank flow matched against 1-3 *currently unmatched*
internal flows where:

* account names are identical,
* every internal flow's date is within 2 days of the bank flow's date,
* the sum of internal amounts equals the bank amount.

The expensive enumeration runs on in-memory snapshots outside of any database
write transaction, so uploads and confirmations are never blocked by
recomputation.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from collections import defaultdict

from .models import CandidateSpec, FlowRow, INTERNAL

MAX_GROUP_SIZE = 3
DATE_TOLERANCE_DAYS = 2


def _date(value: str) -> _dt.date:
    return _dt.date.fromisoformat(value)


def candidate_key(bank_serial: str, internal_serials: list[str]) -> str:
    """Stable identity of a combination across recomputations."""
    payload = json.dumps(
        {"b": bank_serial, "i": sorted(internal_serials)},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _eligible_internals(bank: FlowRow, pool: list[FlowRow]) -> list[FlowRow]:
    b_date = _date(bank.txn_date)
    out: list[FlowRow] = []
    for flow in pool:
        if flow.name != bank.name:
            continue
        if abs((_date(flow.txn_date) - b_date).days) > DATE_TOLERANCE_DAYS:
            continue
        if flow.amount > bank.amount:
            continue
        out.append(flow)
    # Deterministic enumeration order.
    out.sort(key=lambda f: (f.txn_date, f.serial))
    return out


def _enumerate_subsets(
    items: list[FlowRow], target: int
) -> list[tuple[FlowRow, ...]]:
    """All subsets of size 1..MAX_GROUP_SIZE whose amounts sum to target."""
    results: list[tuple[FlowRow, ...]] = []

    def search(start: int, chosen: list[FlowRow], remaining: int) -> None:
        if remaining == 0 and chosen:
            results.append(tuple(chosen))
            return
        if len(chosen) == MAX_GROUP_SIZE:
            return
        for idx in range(start, len(items)):
            flow = items[idx]
            if flow.amount > remaining:
                continue  # amounts are positive; prune
            chosen.append(flow)
            search(idx + 1, chosen, remaining - flow.amount)
            chosen.pop()

    search(0, [], target)
    return results


def compute_candidates(
    bank_flows: list[FlowRow], internal_flows: list[FlowRow]
) -> list[CandidateSpec]:
    """Compute every currently valid candidate from flow snapshots."""
    unmatched_internal = [f for f in internal_flows if f.status == "unmatched"]
    by_name: dict[str, list[FlowRow]] = defaultdict(list)
    for flow in unmatched_internal:
        by_name[flow.name].append(flow)

    specs: list[CandidateSpec] = []
    for bank in sorted(bank_flows, key=lambda f: f.serial):
        if bank.status != "unmatched":
            continue
        eligible = _eligible_internals(bank, by_name.get(bank.name, []))
        for subset in _enumerate_subsets(eligible, bank.amount):
            internals = list(subset)
            specs.append(
                CandidateSpec(
                    bank=bank,
                    internals=internals,
                    candidate_key=candidate_key(
                        bank.serial, [f.serial for f in internals]
                    ),
                    total_amount=sum(f.amount for f in internals),
                )
            )
    specs.sort(key=lambda s: (s.bank.serial, s.candidate_key))
    return specs


def validate_combination(bank: FlowRow, internals: list[FlowRow]) -> None:
    """Re-check the business rules against the latest rows (raises ValueError)."""
    if not 1 <= len(internals) <= MAX_GROUP_SIZE:
        raise ValueError(f"内部流水数量必须为 1~{MAX_GROUP_SIZE} 条")
    b_date = _date(bank.txn_date)
    total = 0
    for flow in internals:
        if flow.name != bank.name:
            raise ValueError(f"户名不一致: 银行={bank.name!r} 内部={flow.name!r}")
        if abs((_date(flow.txn_date) - b_date).days) > DATE_TOLERANCE_DAYS:
            raise ValueError(f"内部流水 {flow.serial} 与银行流水日期相差超过 2 日")
        total += flow.amount
    if total != bank.amount:
        raise ValueError(f"金额合计 {total} 与银行金额 {bank.amount} 不相等")


def side_label(side: str) -> str:
    return "银行" if side != INTERNAL else "内部"
