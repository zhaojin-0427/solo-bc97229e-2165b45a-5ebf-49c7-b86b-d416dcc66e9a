#!/usr/bin/env python3
"""End-to-end demonstration using only the Python standard library.

Walks the full lifecycle against a real SQLite database in ./data:

  1. upload both CSVs (same content twice -> second upload is a no-op),
  2. recompute candidates in the background-style path,
  3. list candidates and confirm one,
  4. show confirmed correspondences and unmatched details,
  5. correct a matched flow -> whole group disbands atomically,
  6. reopen the database like a process restart -> data survives,
  7. staged two-phase correction: preview (no formal change), conflict
     rejection, and a successful whole-batch confirmation.

Run:  python3 -I scripts/demo.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.csv_parser import parse_flow_csv  # noqa: E402
from app.db import Database  # noqa: E402
from app.ledger import upload  # noqa: E402
from app.matching_service import (  # noqa: E402
    confirm_candidate,
    list_candidates,
    list_flows,
    list_groups,
    list_unmatched,
)
from app.models import (  # noqa: E402
    BANK,
    INTERNAL,
    BatchConflict,
    BatchNotStaged,
    StaleCandidateError,
)
from app.recompute import recompute  # noqa: E402
from app.staged_import import confirm_batch, create_batch, get_batch  # noqa: E402


def banner(title: str) -> None:
    print("\n" + "=" * 72 + f"\n {title}\n" + "=" * 72)


def main() -> None:
    db_path = os.path.join(ROOT, "data", "demo.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    db = Database(db_path)

    banner("1. 上传两侧 CSV（第二次同内容重传应全部 unchanged）")
    for side, path in (
        (BANK, os.path.join(ROOT, "data", "sample_bank.csv")),
        (INTERNAL, os.path.join(ROOT, "data", "sample_internal.csv")),
    ):
        with open(path, "rb") as fh:
            rows = parse_flow_csv(fh.read())
        first = upload(db, side, rows)
        second = upload(db, side, rows)
        print(f"{side:8s} 首次: upserted={first.upserted} unchanged={first.unchanged}")
        print(f"{side:8s} 重传: upserted={second.upserted} unchanged={second.unchanged}")

    banner("2. 后台路径重算候选（计算不占用写事务）")
    gen = recompute(db)
    print(f"generation={gen}")

    banner("3. 全部候选组合")
    candidates = list_candidates(db)
    for c in candidates:
        internals = ", ".join(
            f"{f['serial']}({f['amount']}分,v{f['version']})" for f in c["internals"]
        )
        print(f"  #{c['candidate_id']} 银行 {c['bank']['serial']} "
              f"({c['bank']['amount']}分) ↔ [{internals}] 合计={c['total_amount']}")

    banner("4. 核对员确认一个组合（带页面 version 快照）")
    target = next(c for c in candidates if c["bank"]["serial"] == "B002")
    group = confirm_candidate(db, target["candidate_id"], target["expected_versions"])
    print(f"已确认匹配组 #{group['group_id']}: "
          f"{[f['serial'] for f in group['bank']]} ↔ "
          f"{[f['serial'] for f in group['internals']]}")

    banner("5. 旧页面确认已过时组合必须 409（含最新原始值回传）")
    leftover = [
        c
        for c in list_candidates(db, include_stale=True)
        if c["bank"]["serial"] == "B002"
    ]
    if leftover:
        stale = next(
            (c for c in leftover if c["candidate_id"] != group["candidate_id"]),
            leftover[0],
        )
        try:
            confirm_candidate(db, stale["candidate_id"], stale["expected_versions"])
        except StaleCandidateError as exc:
            print(f"预期冲突 HTTP {exc.status}: {exc}")
            print(
                "  回传最新流水:",
                [
                    (f["side"], f["serial"], f["status"], f"v{f['version']}")
                    for f in exc.latest["flows"]
                ],
            )

    banner("6. 已确认对应关系 & 未匹配明细")
    for g in list_groups(db):
        print(f"  组#{g['group_id']}: 银行 {[f['serial'] for f in g['bank']]}"
              f" | 内部 {[f['serial'] for f in g['internals']]}")
    unmatched = list_unmatched(db)
    print("  未匹配银行:", [f["serial"] for f in unmatched["bank"]])
    print("  未匹配内部:", [f["serial"] for f in unmatched["internal"]])

    banner("7. 更正已匹配流水 B002 -> 整组撤销，成员回到未匹配")
    corrected = parse_flow_csv("流水号,户名,日期,金额(分)\nB002,江南供应链,2026-11-11,5500\n")
    report = upload(db, BANK, corrected)
    print(f"disbanded_groups={report.disbanded_groups}, upserted={report.upserted}")
    recompute(db)
    print("剩余匹配组:", list_groups(db))
    print("未匹配银行:", [f["serial"] for f in list_unmatched(db)["bank"]])

    banner("8. 模拟重启：关闭后重新打开数据库，已确认数据仍在（先再确认一组）")
    cand = next(c for c in list_candidates(db) if c["bank"]["serial"] == "B001")
    kept = confirm_candidate(db, cand["candidate_id"], cand["expected_versions"])
    db.close()
    db = Database(db_path)
    groups = list_groups(db)
    summary = [
        (
            g["group_id"],
            [f["serial"] for f in g["bank"]],
            [f["serial"] for f in g["internals"]],
        )
        for g in groups
    ]
    print("重启后匹配组:", summary)
    assert any(g["group_id"] == kept["group_id"] for g in groups), "确认数据丢失!"
    db.close()

    # ------------------------------------------------------------------
    banner("9. 暂存更正预审：逐条标注 新增/变更/不变，正式流水不变")
    db = Database(db_path)
    preview_csv = (
        "流水号,户名,日期,金额(分)\n"
        "B001,华北贸易公司,2026-11-10,10000\n"   # unchanged
        "B003,西部零售集团,2026-11-12,7777\n"    # changed
        "B010,新客户有限公司,2026-11-12,888\n"   # added
    )
    batch = create_batch(db, BANK, parse_flow_csv(preview_csv), "correction.csv")
    bid = batch["batch_id"]
    print(f"批次号: {bid} 状态: {batch['status']}")
    print(
        "逐条判定:",
        [(r["serial"], r["change_type"]) for r in batch["rows"]],
    )
    print(
        "新增/变更/不变 =",
        batch["added"], batch["changed"], batch["unchanged"],
    )
    flows = {f["serial"]: f for f in list_flows(db, BANK)}
    assert flows["B003"]["amount"] == 8800, "预审不得修改正式流水"
    assert "B010" not in flows, "预审不得插入正式流水"

    banner("10. 预审后受监视流水被原 API 改动 → 确认整批拒绝并列出冲突")
    upload(db, BANK, parse_flow_csv(
        "流水号,户名,日期,金额(分)\nB003,西部零售集团,2026-11-12,4321\n"
    ))
    try:
        confirm_batch(db, bid)
    except BatchConflict as exc:
        print(f"预期冲突 HTTP {exc.status}: {exc}")
        for c in exc.conflicts:
            print(
                "  冲突:", c["side"], c["serial"], c["reason"],
                "当前值:", None if c["current"] is None else c["current"]["amount"],
            )
    rejected = get_batch(db, bid)
    assert rejected["status"] == "rejected"
    # Rejected batch is terminal.
    try:
        confirm_batch(db, bid)
    except BatchNotStaged:
        print("  已拒绝批次不可再次确认（终态）")

    banner("11. 重新预审 → 确认整批应用（回执、状态与业务改动同事务生效）")
    batch2 = create_batch(db, BANK, parse_flow_csv(preview_csv), "correction2.csv")
    payload = confirm_batch(db, batch2["batch_id"])
    receipt = payload["receipt"]
    print("成功回执:", receipt)
    flows = {f["serial"]: f for f in list_flows(db, BANK)}
    assert flows["B003"]["amount"] == 7777
    assert flows["B010"]["amount"] == 888 and flows["B010"]["version"] == 1
    # Re-confirming returns the exact first receipt and never re-applies.
    retry = confirm_batch(db, batch2["batch_id"])
    assert retry["receipt"] == receipt, "重试必须返回首次回执"
    print("重复确认返回首次回执 OK；applied_upserted =", receipt["applied_upserted"])

    banner("12. 待确认批次可跨重启继续确认")
    pending = create_batch(
        db, BANK,
        parse_flow_csv("流水号,户名,日期,金额(分)\nB004,华北贸易公司,2026-11-12,3300\n"),
        "after-restart.csv",
    )
    db.close()
    db = Database(db_path)  # simulate restart while a batch is pending
    reopened = get_batch(db, pending["batch_id"])
    print("重启后批次状态:", reopened["status"], "（仍为待确认）")
    assert reopened["status"] == "staged"
    done = confirm_batch(db, pending["batch_id"])
    print("重启后确认成功:", done["receipt"]["status"], done["receipt"]["serials"])
    db.close()
    print("\n演示完成，数据库位于:", db_path)


if __name__ == "__main__":
    main()
