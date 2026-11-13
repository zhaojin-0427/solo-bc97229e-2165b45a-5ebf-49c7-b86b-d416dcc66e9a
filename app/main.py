"""FastAPI HTTP layer (thin wrapper over the framework-agnostic services).

Endpoints
---------
POST   /api/uploads/{side}                upload bank/internal CSV
GET    /api/flows/{side}                  raw flows with versions & status
GET    /api/candidates                    all confirmable candidate combos
POST   /api/candidates/{id}/confirm       confirm one combination
GET    /api/matches                       confirmed correspondences
GET    /api/matches/{group_id}            one confirmed correspondence
GET    /api/unmatched                     unmatched flow details
POST   /api/recompute                     force a synchronous recomputation
GET    /api/health                        liveness probe
GET    /                                  interactive demo console
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .csv_parser import parse_flow_csv
from .db import Database
from .ledger import upload
from .matching_service import (
    confirm_candidate,
    get_group,
    list_candidates,
    list_flows,
    list_groups,
    list_unmatched,
)
from .models import BANK, INTERNAL, ReconcileError
from .recompute import RecomputeWorker

DB_PATH = os.environ.get("RECONCILE_DB", "/data/reconcile.db")
STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")


class ConfirmRequest(BaseModel):
    expected_versions: dict[str, int] = Field(
        default_factory=dict,
        description="页面看到的各流水 version；与最新值不一致将返回 409",
    )


class RecomputeResponse(BaseModel):
    generation: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = Database(DB_PATH)
    app.state.worker = RecomputeWorker(app.state.db)
    app.state.worker.start()
    # Build an initial candidate set without blocking request serving.
    app.state.worker.schedule()
    try:
        yield
    finally:
        app.state.worker.stop()


app = FastAPI(
    title="银行入账 × 内部收款 核对账 API",
    version="1.0.0",
    description=(
        "上传两类 CSV（各 ≤100 条），系统枚举候选组合（1 条银行流水对 "
        "1~3 条内部流水，同名、日期差 ≤2 日、金额合计相等），核对员确认后"
        "可查询对应关系与未匹配明细。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(ReconcileError)
async def reconcile_error_handler(request: Request, exc: ReconcileError):
    detail = {"message": str(exc)}
    latest = getattr(exc, "latest", None)
    if latest:
        detail["latest"] = latest
    return JSONResponse(status_code=exc.status, content={"detail": detail})


@app.post("/api/uploads/{side}", tags=["uploads"])
async def upload_csv(
    side: Literal["bank", "internal"], file: UploadFile = File(...)
):
    raw = await file.read()
    rows = parse_flow_csv(raw)
    report = upload(app.state.db, side, rows)
    # Recomputation runs in the background thread, never inside the upload tx.
    app.state.worker.schedule()
    return {
        "side": side,
        "file": file.filename,
        "received": report.received,
        "upserted": report.upserted,
        "unchanged": report.unchanged,
        "disbanded_groups": report.disbanded_groups,
        "serials": report.serials,
    }


@app.get("/api/flows/{side}", tags=["flows"])
async def get_flows(side: Literal["bank", "internal"]):
    return {"side": side, "flows": list_flows(app.state.db, side)}


@app.get("/api/candidates", tags=["candidates"])
async def get_candidates(
    include_stale: bool = False, bank_serial: Optional[str] = None
):
    return {
        "candidates": list_candidates(
            app.state.db, include_stale=include_stale, bank_serial=bank_serial
        )
    }


@app.post("/api/candidates/{candidate_id}/confirm", tags=["candidates"])
async def post_confirm(candidate_id: int, body: ConfirmRequest | None = None):
    versions = body.expected_versions if body is not None else {}
    return confirm_candidate(app.state.db, candidate_id, versions)


@app.get("/api/matches", tags=["matches"])
async def get_matches():
    return {"matches": list_groups(app.state.db)}


@app.get("/api/matches/{group_id}", tags=["matches"])
async def get_match(group_id: int):
    try:
        return get_group(app.state.db, group_id)
    except ReconcileError as exc:
        raise HTTPException(status_code=exc.status, detail=str(exc)) from exc


@app.get("/api/unmatched", tags=["matches"])
async def get_unmatched():
    return list_unmatched(app.state.db)


@app.post("/api/recompute", tags=["candidates"], response_model=RecomputeResponse)
async def post_recompute():
    return RecomputeResponse(generation=app.state.worker.run_once_now())


@app.get("/api/health", tags=["meta"])
async def health():
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
async def console():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
