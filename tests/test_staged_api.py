"""HTTP-layer tests for staged (two-phase) imports (run where FastAPI is
installed, e.g. inside Docker):

    pytest -q tests/test_staged_api.py
"""

from __future__ import annotations

import io
import os
import tempfile

import pytest

_tmp = tempfile.mkdtemp()
os.environ["RECONCILE_DB"] = os.path.join(_tmp, "staged_api_test.db")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

BANK_CSV = (
    "流水号,户名,日期,金额(分)\n"
    "B1,甲公司,2026-11-10,10000\n"
    "B2,乙公司,2026-11-11,5000\n"
)
INTERNAL_CSV = (
    "流水号,户名,日期,金额(分)\n"
    "I1,甲公司,2026-11-09,4000\n"
    "I2,甲公司,2026-11-11,6000\n"
    "I3,乙公司,2026-11-10,2500\n"
    "I4,乙公司,2026-11-12,2500\n"
)


@pytest.fixture()
def client():
    # Fresh database file per test for full isolation.
    db_path = os.path.join(tempfile.mkdtemp(), "staged_api_test.db")
    os.environ["RECONCILE_DB"] = db_path
    with TestClient(app) as c:
        yield c


def _upload(client, side, content):
    return client.post(
        f"/api/uploads/{side}",
        files={"file": (f"{side}.csv", io.BytesIO(content.encode()), "text/csv")},
    )


def _preview(client, side, content, name="staged.csv"):
    return client.post(
        f"/api/imports/{side}/preview",
        files={"file": (name, io.BytesIO(content.encode()), "text/csv")},
    )


def _seed(client):
    assert _upload(client, "bank", BANK_CSV).status_code == 200
    assert _upload(client, "internal", INTERNAL_CSV).status_code == 200
    client.post("/api/recompute")


def test_preview_classifies_and_does_not_change_flows(client):
    _seed(client)
    staged = (
        "流水号,户名,日期,金额(分)\n"
        "B1,甲公司,2026-11-10,10000\n"          # unchanged
        "B2,乙公司,2026-11-11,5500\n"           # changed
        "B9,丁公司,2026-11-12,100\n"            # added
    )
    r = _preview(client, "bank", staged)
    assert r.status_code == 200, r.text
    batch = r.json()
    assert batch["status"] == "staged"
    assert (batch["added"], batch["changed"], batch["unchanged"]) == (1, 1, 1)
    # Formal flows untouched.
    flows = {f["serial"]: f for f in client.get("/api/flows/bank").json()["flows"]}
    assert flows["B2"]["amount"] == 5000
    assert "B9" not in flows
    # Listed.
    ids = [b["batch_id"] for b in client.get("/api/imports").json()["batches"]]
    assert batch["batch_id"] in ids
    assert client.get(f"/api/imports/{batch['batch_id']}").json()["status"] == "staged"


def test_preview_rejects_bad_csv_with_422(client):
    bad = "流水号,户名,日期,金额(分)\nX1,丁,2026-11-10,0\n"
    assert _preview(client, "bank", bad).status_code == 422


def test_confirm_happy_path_disbands_group(client):
    _seed(client)
    # Confirm B2 <-> I3+I4 first.
    cands = client.get("/api/candidates").json()["candidates"]
    cand = next(c for c in cands if c["bank"]["serial"] == "B2")
    group = client.post(
        f"/api/candidates/{cand['candidate_id']}/confirm",
        json={"expected_versions": cand["expected_versions"]},
    ).json()
    gid = group["group_id"]

    staged = "流水号,户名,日期,金额(分)\nB2,乙公司,2026-11-11,5500\n"
    batch = _preview(client, "bank", staged).json()
    assert batch["disbanded_group_ids"] == [gid]
    members = {
        (m["side"], m["serial"])
        for m in batch["disband_groups"][0]["members"]
    }
    assert members == {("bank", "B2"), ("internal", "I3"), ("internal", "I4")}

    done = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert done.status_code == 200, done.text
    receipt = done.json()["receipt"]
    assert receipt["disbanded_groups"] == [gid]
    assert receipt["applied_upserted"] == 1
    assert client.get("/api/matches").json()["matches"] == []
    flows = {f["serial"]: f for f in client.get("/api/flows/bank").json()["flows"]}
    assert flows["B2"]["amount"] == 5500
    assert flows["B2"]["status"] == "unmatched"


def test_confirm_retry_returns_same_receipt(client):
    _seed(client)
    staged = "流水号,户名,日期,金额(分)\nB1,甲公司,2026-11-10,7777\n"
    batch = _preview(client, "bank", staged).json()
    first = client.post(f"/api/imports/{batch['batch_id']}/confirm").json()
    # Later correction via original API.
    _upload(client, "bank", "流水号,户名,日期,金额(分)\nB1,甲公司,2026-11-10,2222\n")
    retry = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert retry.status_code == 200
    assert retry.json()["receipt"] == first["receipt"]
    flows = {f["serial"]: f for f in client.get("/api/flows/bank").json()["flows"]}
    assert flows["B1"]["amount"] == 2222  # later correction preserved


def test_confirm_conflict_rejects_batch_and_lists_conflicts(client):
    _seed(client)
    staged = "流水号,户名,日期,金额(分)\nB1,甲公司,2026-11-10,9999\n"
    batch = _preview(client, "bank", staged).json()
    # Concurrent change via the original API.
    _upload(client, "bank", "流水号,户名,日期,金额(分)\nB1,甲公司,2026-11-10,1111\n")
    resp = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert detail["conflicts"][0]["serial"] == "B1"
    assert detail["conflicts"][0]["reason"] == "flow_changed"
    # Terminal rejected state is persisted.
    got = client.get(f"/api/imports/{batch['batch_id']}").json()
    assert got["status"] == "rejected"
    again = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert again.status_code == 409
    # Batch content never applied.
    flows = {f["serial"]: f for f in client.get("/api/flows/bank").json()["flows"]}
    assert flows["B1"]["amount"] == 1111


def test_new_serial_appearing_is_a_conflict(client):
    _seed(client)
    staged = "流水号,户名,日期,金额(分)\nBN,丁公司,2026-11-12,100\n"
    batch = _preview(client, "bank", staged).json()
    _upload(client, "bank", "流水号,户名,日期,金额(分)\nBN,丁公司,2026-11-12,200\n")
    resp = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert resp.status_code == 409
    conflict = resp.json()["detail"]["conflicts"][0]
    assert conflict["reason"] == "flow_appeared"
    assert conflict["current"]["amount"] == 200


def test_unrelated_change_does_not_block_confirm(client):
    _seed(client)
    staged = "流水号,户名,日期,金额(分)\nBU,丁公司,2026-11-12,100\n"
    batch = _preview(client, "bank", staged).json()
    # Unrelated existing flow changes meanwhile.
    _upload(client, "bank", "流水号,户名,日期,金额(分)\nB2,乙公司,2026-11-11,4321\n")
    resp = client.post(f"/api/imports/{batch['batch_id']}/confirm")
    assert resp.status_code == 200, resp.text
    flows = {f["serial"]: f for f in client.get("/api/flows/bank").json()["flows"]}
    assert flows["BU"]["amount"] == 100
    assert flows["B2"]["amount"] == 4321


def test_unknown_batch_404(client):
    assert client.get("/api/imports/nope").status_code == 404
    assert client.post("/api/imports/nope/confirm").status_code == 404
