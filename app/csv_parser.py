"""CSV parsing and validation for the two flow feeds.

Each side accepts at most 100 rows. Required columns (Chinese or English
headers accepted): unique serial, account name, date, positive integer amount
in *cents* (fen).
"""

from __future__ import annotations

import csv
import datetime as _dt
import io
from typing import Iterable

from .models import CsvFormatError, ParsedRow

MAX_ROWS_PER_SIDE = 100

_HEADER_ALIASES = {
    "serial": {"serial", "流水号", "唯一流水号", "编号", "id", "no"},
    "name": {"name", "户名", "账户名", "account name", "accountname", "account"},
    "date": {"date", "日期", "交易日期", "txn_date", "txndate"},
    "amount": {"amount", "金额", "金额(分)", "金额（分）", "amount(cents)", "cents"},
}


def _normalise(header: str) -> str:
    return (header or "").strip().lower().replace(" ", "").replace("_", "")


def _map_headers(fieldnames: Iterable[str] | None) -> dict[str, int]:
    if not fieldnames:
        raise CsvFormatError("CSV 缺少表头，需要列：流水号、户名、日期、金额(分)")
    lookup: dict[str, int] = {}
    for idx, raw in enumerate(fieldnames):
        key = _normalise(raw)
        for canonical, aliases in _HEADER_ALIASES.items():
            if key in {_normalise(a) for a in aliases}:
                lookup[canonical] = idx
    missing = [c for c in ("serial", "name", "date", "amount") if c not in lookup]
    if missing:
        raise CsvFormatError(
            "CSV 缺少必需列: " + ", ".join(missing)
            + "；表头需包含 流水号/户名/日期/金额(分)"
        )
    return lookup


def _parse_date(value: str, line: int) -> str:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            return _dt.datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    raise CsvFormatError(f"第 {line} 行日期格式非法: {value!r}，应为 YYYY-MM-DD")


def _parse_amount(value: str, line: int) -> int:
    value = (value or "").strip().replace(",", "")
    if not value:
        raise CsvFormatError(f"第 {line} 行金额为空")
    if not value.isdigit():
        raise CsvFormatError(f"第 {line} 行金额必须为正整数（单位: 分）: {value!r}")
    amount = int(value)
    if amount <= 0:
        raise CsvFormatError(f"第 {line} 行金额必须为正整数（单位: 分）: {amount}")
    return amount


def parse_flow_csv(raw: bytes | str) -> list[ParsedRow]:
    """Parse an uploaded CSV into validated rows."""
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CsvFormatError(f"文件必须为 UTF-8 编码: {exc}") from exc
    else:
        text = raw

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvFormatError("CSV 文件为空") from exc

    cols = _map_headers(header)
    rows: list[ParsedRow] = []
    seen: set[str] = set()

    for line_no, record in enumerate(reader, start=2):
        if not record or all(not (c or "").strip() for c in record):
            continue  # blank line
        if len(record) <= max(cols.values()):
            raise CsvFormatError(f"第 {line_no} 行列数不足")
        serial = record[cols["serial"]].strip()
        name = record[cols["name"]].strip()
        if not serial:
            raise CsvFormatError(f"第 {line_no} 行流水号为空")
        if not name:
            raise CsvFormatError(f"第 {line_no} 行户名为空")
        if serial in seen:
            raise CsvFormatError(f"第 {line_no} 行流水号在文件内重复: {serial}")
        seen.add(serial)
        rows.append(
            ParsedRow(
                serial=serial,
                name=name,
                txn_date=_parse_date(record[cols["date"]], line_no),
                amount=_parse_amount(record[cols["amount"]], line_no),
            )
        )

    if not rows:
        raise CsvFormatError("CSV 没有任何数据行")
    if len(rows) > MAX_ROWS_PER_SIDE:
        raise CsvFormatError(
            f"单次上传 {len(rows)} 条，超过每侧上限 {MAX_ROWS_PER_SIDE} 条"
        )
    return rows
