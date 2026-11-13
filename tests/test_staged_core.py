"""Core tests for the staged (two-phase) correction import — stdlib only:

    python3 -I tests/test_staged_core.py

They use the real SQLite-backed services and cover: preview classification,
whole-group disband previews, confirm-time conflict detection (original value
/ claim status / group membership / appeared flow), unrelated-change
tolerance, capacity, atomicity, restart-durability of a pending batch, and
idempotent concurrent confirmation returning the first receipt.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.csv_parser import parse_flow_csv  # noqa: E402
from app.db import Database  # noqa: E402
from app.ledger import upload  # noqa: E402
from app.matching_service import (  # noqa: E402
    confirm_candidate,
    list_candidates,
    list_flows,
    list_groups,
)
from app.models import (  # noqa: E402
    BANK,
    INTERNAL,
    MATCHED,
    UNMATCHED,
    BatchConflict,
    BatchNotStaged,
    CapacityExceeded,
    ParsedRow,
)
from app.recompute import recompute  # noqa: E402
from app.staged_import import (  # noqa: E402
    BATCH_CONFIRMED,
    BATCH_REJECTED,
    BATCH_STAGED,
    confirm_batch,
    create_batch,
    get_batch,
    list_batches,
)


def rows(side, data: list[tuple]) -> list[ParsedRow]:
    return [ParsedRow(serial=s, name=n, txn_date=d, amount=a) for s, n, d, a in data]


BANK_CSV = (
    "流水号,户名,日期,金额(分)\n"
    "B1,甲公司,2026-11-10,10000\n"
    "B2,乙公司,2026-11-11,5000\n"
    "B3,丙公司,2026-11-12,9000\n"
)
INTERNAL_CSV = (
    "流水号,户名,日期,金额(分)\n"
    "I1,甲公司,2026-11-09,4000\n"
    "I2,甲公司,2026-11-11,6000\n"
    "I3,甲公司,2026-11-10,10000\n"
    "I4,乙公司,2026-11-10,2500\n"
    "I5,乙公司,2026-11-12,2500\n"
)


class StagedImportTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmp.name, "staged.db"))

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def seed(self) -> None:
        upload(self.db, BANK, parse_flow_csv(BANK_CSV))
        upload(self.db, INTERNAL, parse_flow_csv(INTERNAL_CSV))
        recompute(self.db)

    def confirm_b2(self) -> int:
        cand = next(c for c in list_candidates(self.db)
                    if c["bank"]["serial"] == "B2")
        group = confirm_candidate(self.db, cand["candidate_id"],
                                  cand["expected_versions"])
        return group["group_id"]

    # ---- preview classification -----------------------------------------
    def test_preview_classifies_added_changed_unchanged(self) -> None:
        self.seed()
        preview_csv = (
            "流水号,户名,日期,金额(分)\n"
            "B1,甲公司,2026-11-10,10000\n"          # unchanged
            "B2,乙公司,2026-11-11,5500\n"           # changed
            "B9,丁公司,2026-11-12,100\n"            # added
        )
        batch = create_batch(self.db, BANK, parse_flow_csv(preview_csv), "p.csv")
        self.assertEqual(batch["status"], BATCH_STAGED)
        self.assertEqual(batch["added"], 1)
        self.assertEqual(batch["changed"], 1)
        self.assertEqual(batch["unchanged"], 1)
        by_serial = {r["serial"]: r["change_type"] for r in batch["rows"]}
        self.assertEqual(
            by_serial, {"B1": "unchanged", "B2": "changed", "B9": "added"}
        )
        # Preview must not touch formal flows.
        flows = {f["serial"]: f for f in list_flows(self.db, BANK)}
        self.assertEqual(flows["B2"]["amount"], 5000)
        self.assertNotIn("B9", flows)
        self.assertEqual(flows["B1"]["version"], 1)
        # The staged batch is listable.
        ids = [b["batch_id"] for b in list_batches(self.db)]
        self.assertIn(batch["batch_id"], ids)

    def test_preview_does_not_change_candidates_or_groups(self) -> None:
        self.seed()
        gid = self.confirm_b2()
        before_cands = list_candidates(self.db)
        create_batch(
            self.db, BANK,
            rows(BANK, [("B2", "乙公司", "2026-11-11", 5500)]),
            "x.csv",
        )
        # Group remains confirmed, candidates untouched while merely staged.
        self.assertEqual([g["group_id"] for g in list_groups(self.db)], [gid])
        self.assertEqual(
            [c["candidate_id"] for c in list_candidates(self.db)],
            [c["candidate_id"] for c in before_cands],
        )

    # ---- disband-group preview with original member values --------------
    def test_preview_lists_disband_group_members_original_values(self) -> None:
        self.seed()
        gid = self.confirm_b2()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B2", "乙公司", "2026-11-11", 5500)]),
            "fix.csv",
        )
        self.assertEqual(batch["disbanded_group_ids"], [gid])
        self.assertEqual(len(batch["disband_groups"]), 1)
        group_preview = batch["disband_groups"][0]
        self.assertEqual(group_preview["group_id"], gid)
        member_serials = {(m["side"], m["serial"]) for m in group_preview["members"]}
        self.assertEqual(
            member_serials, {(BANK, "B2"), (INTERNAL, "I4"), (INTERNAL, "I5")}
        )
        # Original values are preserved (B2 still 5000 in the preview payload).
        b2 = next(m for m in group_preview["members"] if m["serial"] == "B2")
        self.assertEqual(b2["amount"], 5000)
        self.assertEqual(b2["status"], MATCHED)
        # All group members appear in the watch list.
        watched = {(w["side"], w["serial"]) for w in batch["watched"]}
        for member in ((BANK, "B2"), (INTERNAL, "I4"), (INTERNAL, "I5")):
            self.assertIn(member, watched)

    # ---- happy path confirmation ----------------------------------------
    def test_confirm_applies_batch_and_disbands_group(self) -> None:
        self.seed()
        gid = self.confirm_b2()
        batch = create_batch(
            self.db, BANK,
            rows(
                BANK,
                [
                    ("B1", "甲公司", "2026-11-10", 10000),  # unchanged no-op
                    ("B2", "乙公司", "2026-11-11", 5500),   # changed -> disband
                    ("B9", "丁公司", "2026-11-12", 100),    # added
                ],
            ),
            "fix.csv",
        )
        payload = confirm_batch(self.db, batch["batch_id"])
        self.assertEqual(payload["status"], BATCH_CONFIRMED)
        receipt = payload["receipt"]
        self.assertEqual(receipt["disbanded_groups"], [gid])
        self.assertEqual(receipt["applied_upserted"], 2)  # B2 + B9
        self.assertEqual(receipt["applied_unchanged"], 1)  # B1
        self.assertEqual(receipt["serials"], ["B1", "B2", "B9"])

        flows = {f["serial"]: f for f in list_flows(self.db, BANK)}
        self.assertEqual(flows["B2"]["amount"], 5500)
        self.assertEqual(flows["B2"]["status"], UNMATCHED)
        self.assertIsNone(flows["B2"]["group_id"])
        self.assertIn("B9", flows)
        self.assertEqual(flows["B9"]["version"], 1)
        self.assertEqual(flows["B1"]["version"], 1)  # unchanged row untouched
        # Group gone, internal members back to unmatched.
        self.assertEqual(list_groups(self.db), [])
        internal = {f["serial"]: f for f in list_flows(self.db, INTERNAL)}
        for serial in ("I4", "I5"):
            self.assertEqual(internal[serial]["status"], UNMATCHED)
            self.assertIsNone(internal[serial]["group_id"])

    def test_confirm_schedules_clean_state_for_recompute(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B8", "丁公司", "2026-11-12", 700)]),
            "n.csv",
        )
        confirm_batch(self.db, batch["batch_id"])
        recompute(self.db)
        # New flow B8 has no matching internal -> no candidate, no error.
        self.assertTrue(all(c["bank"]["serial"] != "B8"
                            for c in list_candidates(self.db)))

    # ---- conflict: original value changes after preview -----------------
    def test_conflict_when_watched_flow_value_changes(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 7777)]),
            "c.csv",
        )
        # Meanwhile the original upload API changes B1 to something else.
        upload(self.db, BANK, rows(BANK, [("B1", "甲公司", "2026-11-10", 1111)]))
        with self.assertRaises(BatchConflict) as ctx:
            confirm_batch(self.db, batch["batch_id"])
        self.assertEqual(ctx.exception.status, 409)
        reasons = {(c["side"], c["serial"]): c["reason"]
                   for c in ctx.exception.conflicts}
        self.assertEqual(reasons[(BANK, "B1")], "flow_changed")
        rejected = get_batch(self.db, batch["batch_id"])
        self.assertEqual(rejected["status"], BATCH_REJECTED)
        self.assertTrue(rejected["conflicts"])
        # Nothing from the batch was applied: B1 keeps the later correction.
        flow = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B1")
        self.assertEqual(flow["amount"], 1111)
        # A rejected batch cannot be confirmed later.
        with self.assertRaises(BatchNotStaged):
            confirm_batch(self.db, batch["batch_id"])

    # ---- conflict: claim status changes after preview -------------------
    def test_conflict_when_flow_claim_status_changes(self) -> None:
        self.seed()
        # Preview touches B1 while it is unmatched.
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 10000)]),
            "s.csv",
        )
        # Meanwhile B1 gets claimed by a confirmed group (content unchanged,
        # only status/group change -> version bump).
        cand = next(c for c in list_candidates(self.db)
                    if c["bank"]["serial"] == "B1")
        confirm_candidate(self.db, cand["candidate_id"],
                          cand["expected_versions"])
        with self.assertRaises(BatchConflict) as ctx:
            confirm_batch(self.db, batch["batch_id"])
        conflict = next(c for c in ctx.exception.conflicts
                        if c["serial"] == "B1")
        self.assertIn(conflict["reason"], ("status_changed", "membership_changed"))
        self.assertEqual(conflict["current"]["status"], MATCHED)
        self.assertEqual(get_batch(self.db, batch["batch_id"])["status"],
                         BATCH_REJECTED)

    # ---- conflict: group member (not in file) changes -------------------
    def test_conflict_when_disband_group_member_changes(self) -> None:
        self.seed()
        gid = self.confirm_b2()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B2", "乙公司", "2026-11-11", 5500)]),
            "g.csv",
        )
        # Another engineer corrects an internal member of the same group,
        # which itself disbands the group before our confirmation.
        upload(self.db, INTERNAL,
               rows(INTERNAL, [("I4", "乙公司", "2026-11-10", 3000)]))
        with self.assertRaises(BatchConflict) as ctx:
            confirm_batch(self.db, batch["batch_id"])
        serials = {c["serial"] for c in ctx.exception.conflicts}
        # B2 (group membership changed) and I4 (value changed) both listed.
        self.assertIn("I4", serials)
        self.assertIn("B2", serials)
        self.assertEqual(get_batch(self.db, batch["batch_id"])["status"],
                         BATCH_REJECTED)
        # Batch's B2 correction must not overwrite the later state.
        b2 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B2")
        self.assertEqual(b2["amount"], 5000)
        self.assertNotIn(gid, [g["group_id"] for g in list_groups(self.db)])

    # ---- conflict: serial absent at preview appears later ---------------
    def test_conflict_when_new_serial_appears(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B9", "丁公司", "2026-11-12", 100)]),
            "n.csv",
        )
        watch = next(w for w in batch["watched"] if w["serial"] == "B9")
        self.assertFalse(watch["existed"])
        # The same serial is created via the original upload meanwhile.
        upload(self.db, BANK, rows(BANK, [("B9", "丁公司", "2026-11-12", 200)]))
        with self.assertRaises(BatchConflict) as ctx:
            confirm_batch(self.db, batch["batch_id"])
        conflict = next(c for c in ctx.exception.conflicts
                        if c["serial"] == "B9")
        self.assertEqual(conflict["reason"], "flow_appeared")
        self.assertEqual(conflict["current"]["amount"], 200)
        # Batch value (100) must not overwrite the later value (200).
        b9 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B9")
        self.assertEqual(b9["amount"], 200)

    # ---- unrelated changes do not invalidate the preview ---------------
    def test_unrelated_change_keeps_preview_valid(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 7777)]),
            "u.csv",
        )
        # An unrelated flow (not in the file, no shared group) changes.
        upload(self.db, BANK, rows(BANK, [("B3", "丙公司", "2026-11-12", 1234)]))
        payload = confirm_batch(self.db, batch["batch_id"])
        self.assertEqual(payload["status"], BATCH_CONFIRMED)
        b1 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B1")
        b3 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B3")
        self.assertEqual(b1["amount"], 7777)
        self.assertEqual(b3["amount"], 1234)  # unrelated change preserved

    def test_unrelated_capacity_growth_causes_rejection(self) -> None:
        self.seed()  # 3 bank flows
        # Preview adds one flow: 3 + 1 = 4 <= 100.
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B9", "丁公司", "2026-11-12", 100)]),
            "cap.csv",
        )
        # Fill the side to the cap with unrelated new serials (need 96 more).
        others = rows(
            BANK,
            [(f"X{i:03d}", "丁公司", "2026-11-12", 100) for i in range(96)],
        )
        upload(self.db, BANK, others)  # 3 + 96 = 99; B9 would make 100 -> fine
        # One more unrelated serial reaches the cap of 100.
        upload(self.db, BANK, rows(BANK, [("X100", "丁公司", "2026-11-12", 100)]))
        with self.assertRaises(BatchConflict) as ctx:
            confirm_batch(self.db, batch["batch_id"])
        conflict = ctx.exception.conflicts[0]
        self.assertEqual(conflict["reason"], "capacity_exceeded")
        self.assertIn("上限", conflict["message"])
        self.assertEqual(get_batch(self.db, batch["batch_id"])["status"],
                         BATCH_REJECTED)

    def test_preview_capacity_exceeded_is_422_and_creates_no_batch(self) -> None:
        self.seed()
        before = len(list_batches(self.db))
        overflow = rows(
            BANK,
            [(f"Y{i:03d}", "丁公司", "2026-11-12", 100) for i in range(98)],
        )  # 3 existing + 98 new = 101
        with self.assertRaises(CapacityExceeded):
            create_batch(self.db, BANK, overflow, "big.csv")
        self.assertEqual(len(list_batches(self.db)), before)

    # ---- idempotent / concurrent confirmation ---------------------------
    def test_retry_confirm_returns_first_receipt_without_reapplying(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 7777)]),
            "r.csv",
        )
        first = confirm_batch(self.db, batch["batch_id"])
        # A later correction via the original API must survive a retry.
        upload(self.db, BANK, rows(BANK, [("B1", "甲公司", "2026-11-10", 2222)]))
        retry = confirm_batch(self.db, batch["batch_id"])
        self.assertEqual(retry["receipt"], first["receipt"])
        b1 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B1")
        self.assertEqual(b1["amount"], 2222)  # later correction not overwritten

    def test_concurrent_confirm_only_one_application(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 7777)]),
            "t.csv",
        )
        receipts: list = []
        errors: list = []

        def worker() -> None:
            try:
                receipts.append(confirm_batch(self.db, batch["batch_id"]))
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(worker) for _ in range(4)]
            for f in futs:
                f.result(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(len(receipts), 4)
        unique = {json_key(r["receipt"]) for r in receipts}
        self.assertEqual(len(unique), 1)
        b1 = next(f for f in list_flows(self.db, BANK) if f["serial"] == "B1")
        self.assertEqual(b1["amount"], 7777)

    # ---- restart: a pending batch can still be confirmed ----------------
    def test_pending_batch_survives_restart(self) -> None:
        self.seed()
        gid = self.confirm_b2()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B2", "乙公司", "2026-11-11", 5500)]),
            "d.csv",
        )
        path = self.db.path
        batch_id = batch["batch_id"]
        self.db.close()
        db2 = Database(path)  # simulate process restart
        try:
            reopened = get_batch(db2, batch_id)
            self.assertEqual(reopened["status"], BATCH_STAGED)
            self.assertEqual(reopened["disbanded_group_ids"], [gid])
            payload = confirm_batch(db2, batch_id)
            self.assertEqual(payload["status"], BATCH_CONFIRMED)
            b2 = next(f for f in list_flows(db2, BANK) if f["serial"] == "B2")
            self.assertEqual(b2["amount"], 5500)
            self.assertEqual(list_groups(db2), [])
        finally:
            db2.close()

    def test_conflict_after_restart_still_detected(self) -> None:
        self.seed()
        batch = create_batch(
            self.db, BANK,
            rows(BANK, [("B1", "甲公司", "2026-11-10", 7777)]),
            "x.csv",
        )
        batch_id = batch["batch_id"]
        path = self.db.path
        self.db.close()
        db2 = Database(path)
        try:
            upload(db2, BANK, rows(BANK, [("B1", "甲公司", "2026-11-10", 1111)]))
            with self.assertRaises(BatchConflict):
                confirm_batch(db2, batch_id)
            self.assertEqual(
                get_batch(db2, batch_id)["status"], BATCH_REJECTED
            )
        finally:
            db2.close()

    # ---- original API keeps working during pending batches --------------
    def test_original_upload_and_confirm_unaffected_while_pending(self) -> None:
        self.seed()
        create_batch(
            self.db, BANK,
            rows(BANK, [("B9", "丁公司", "2026-11-12", 100)]),
            "p.csv",
        )
        # Original upload path still applies immediately.
        report = upload(
            self.db, INTERNAL,
            rows(INTERNAL, [("I9", "丁公司", "2026-11-12", 100)]),
        )
        self.assertEqual(report.upserted, 1)
        # Candidate confirmation still works.
        cand = next(c for c in list_candidates(self.db)
                    if c["bank"]["serial"] == "B2")
        group = confirm_candidate(self.db, cand["candidate_id"],
                                  cand["expected_versions"])
        self.assertTrue(group["group_id"])

    def test_unknown_batch_raises_not_found(self) -> None:
        from app.models import BatchNotFound

        with self.assertRaises(BatchNotFound):
            get_batch(self.db, "deadbeef")
        with self.assertRaises(BatchNotFound):
            confirm_batch(self.db, "deadbeef")


def json_key(obj) -> str:
    import json

    return json.dumps(obj, sort_keys=True, ensure_ascii=False)


if __name__ == "__main__":
    unittest.main(verbosity=2)
