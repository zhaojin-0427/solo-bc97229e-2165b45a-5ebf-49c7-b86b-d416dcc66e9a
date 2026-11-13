"""HTTP-layer tests (run where FastAPI is installed, e.g. inside Docker):

    pytest -q
"""

from __future__ import annotations

import io
import os
import tempfile

import pytest

# Point the app at a throwaway database before importing it.
_tmp = tempfile.mkdtemp()
os.environ["RECONCILE_DB"] = os.path.join(_tmp, "api_test.db")

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


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def _upload(client, side, content):
    return client.post(
        f"/api/uploads/{side}",
        files={"file": (f"{side}.csv", io.BytesIO(content.encode()), "text/csv")},
    )


def test_health_and_console(client):
    assert client.get("/api/health").json() == {"status": "ok"}
    assert client.get("/").status_code == 200


def test_full_workflow(client):
    r1 = _upload(client, "bank", BANK_CSV)
    r2 = _upload(client, "internal", INTERNAL_CSV)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["upserted"] == 2

    # identical re-upload changes nothing
    again = _upload(client, "bank", BANK_CSV).json()
    assert again["upserted"] == 0 and again["unchanged"] == 2

    rec = client.post("/api/recompute").json()
    assert rec["generation"] >= 1

    cands = client.get("/api/candidates").json()["candidates"]
    by_bank = {c["bank"]["serial"]: c for c in cands}
    assert set(by_bank) == {"B1", "B2"}

    group = client.post(
        f"/api/candidates/{by_bank['B2']['candidate_id']}/confirm",
        json={"expected_versions": by_bank["B2"]["expected_versions"]},
    )
    assert group.status_code == 200
    gid = group.json()["group_id"]

    matches = client.get("/api/matches").json()["matches"]
    assert [m["group_id"] for m in matches] == [gid]
    unmatched = client.get("/api/unmatched").json()
    assert "B2" not in [f["serial"] for f in unmatched["bank"]]


def test_stale_confirm_returns_409_with_latest(client):
    cands = client.get("/api/candidates").json()["candidates"]
    cand = next(c for c in cands if c["bank"]["serial"] == "B1")
    old = dict(cand["expected_versions"])
    key = next(iter(old))
    old[key] -= 1
    resp = client.post(
        f"/api/candidates/{cand['candidate_id']}/confirm",
        json={"expected_versions": old},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["latest"]["flows"]


def test_concurrent_confirm_mutual_exclusion(client):
    import threading

    cands = client.get("/api/candidates").json()["candidates"]
    b1 = [c for c in cands if c["bank"]["serial"] == "B1"]
    statuses = []
    lock = threading.Lock()

    def fire(candidate):
        resp = client.post(
            f"/api/candidates/{candidate['candidate_id']}/confirm",
            json={"expected_versions": candidate["expected_versions"]},
        )
        with lock:
            statuses.append(resp.status_code)

    threads = [threading.Thread(target=fire, args=(c,)) for c in b1]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(statuses) == [200, 409]


def test_correction_disbands_group(client):
    matches_before = len(client.get("/api/matches").json()["matches"])
    assert matches_before >= 1
    corrected = "流水号,户名,日期,金额(分)\nB2,乙公司,2026-11-11,9999\n"
    report = _upload(client, "bank", corrected).json()
    assert report["disbanded_groups"]
    assert client.get("/api/matches").json()["matches"] == []
    unmatched = client.get("/api/unmatched").json()
    assert "B2" in [f["serial"] for f in unmatched["bank"]]


def test_bad_csv_rejected(client):
    bad = "流水号,户名,日期,金额(分)\nX1,丁,2026-11-10,0\n"
    assert _upload(client, "bank", bad).status_code == 422


def test_unknown_candidate_404(client):
    assert client.post("/api/candidates/999999/confirm", json={}).status_code == 404
