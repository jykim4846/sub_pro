from __future__ import annotations

import csv
import glob
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .db import connect, upsert_bid_result, upsert_contract, upsert_notice


_HEADER_HINTS = ("입찰공고번호", "입찰공고차수", "수요기관코드")
_CATEGORY_MAP = {
    "공사": "construction",
    "기술용역": "service",
    "물품(내자)": "goods",
    "물품(외자)": "goods",
    "일반용역": "service",
}


@dataclass
class ContractCsvFileImportResult:
    path: str
    rows_seen: int
    notices_upserted: int
    results_upserted: int
    contracts_upserted: int
    skipped_unknown_category: int
    skipped_missing_notice_id: int


@dataclass
class ContractCsvImportResult:
    files: list[ContractCsvFileImportResult]
    files_seen: int
    rows_seen: int
    notices_upserted: int
    results_upserted: int
    contracts_upserted: int
    skipped_unknown_category: int
    skipped_missing_notice_id: int


def import_contract_history_csvs(
    db_path: str | Path,
    paths: Iterable[str | Path],
) -> ContractCsvImportResult:
    resolved_paths = _resolve_input_paths(paths)
    if not resolved_paths:
        raise FileNotFoundError("No CSV files found to import.")

    file_results: list[ContractCsvFileImportResult] = []
    with connect(db_path) as conn:
        for path in resolved_paths:
            file_results.append(_import_single_file(conn, path))

    return ContractCsvImportResult(
        files=file_results,
        files_seen=len(file_results),
        rows_seen=sum(item.rows_seen for item in file_results),
        notices_upserted=sum(item.notices_upserted for item in file_results),
        results_upserted=sum(item.results_upserted for item in file_results),
        contracts_upserted=sum(item.contracts_upserted for item in file_results),
        skipped_unknown_category=sum(item.skipped_unknown_category for item in file_results),
        skipped_missing_notice_id=sum(item.skipped_missing_notice_id for item in file_results),
    )


def _resolve_input_paths(paths: Iterable[str | Path]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[Path] = set()

    for raw in paths:
        text = str(raw)
        matches: list[Path] = []
        candidate = Path(text).expanduser()

        if candidate.exists():
            if candidate.is_dir():
                matches.extend(sorted(candidate.glob("*.csv")))
            elif candidate.is_file():
                matches.append(candidate)
        else:
            matches.extend(sorted(Path(item) for item in glob.glob(text)))

        for match in matches:
            if match in seen:
                continue
            seen.add(match)
            resolved.append(match)

    return resolved


def _import_single_file(conn, path: Path) -> ContractCsvFileImportResult:
    rows_seen = 0
    notices_upserted = 0
    results_upserted = 0
    contracts_upserted = 0
    skipped_unknown_category = 0
    skipped_missing_notice_id = 0

    with _open_dict_reader(path) as reader:
        for row in reader:
            if not any((value or "").strip() for value in row.values()):
                continue

            rows_seen += 1
            category = _category_from_row(row)
            if not category:
                skipped_unknown_category += 1
                continue

            notice_id = _notice_id_from_row(row)
            if not notice_id:
                skipped_missing_notice_id += 1
                continue

            agency_name = _first_text(row, "수요기관", "공고기관")
            agency_code = _first_text(row, "수요기관코드", "공고기관코드")
            contract_method = _first_text(row, "표준계약방법", "입찰방법명", "낙찰방법명")
            region = _first_text(row, "수요기관법정동명")
            opened_at = _first_text(row, "공고게시일자")
            base_amount = _to_float(row.get("입찰추정가격")) or 0.0

            upsert_notice(
                conn=conn,
                notice_id=notice_id,
                agency_name=agency_name,
                agency_code=agency_code,
                category=category,
                contract_method=contract_method,
                region=region,
                base_amount=base_amount,
                estimated_amount=base_amount or None,
                floor_rate=None,
                opened_at=opened_at or None,
            )
            notices_upserted += 1

            contract_amount = _to_float(row.get("계약금액"))
            contract_id = _first_text(row, "계약번호", "계약요청접수번호", "이용자문서번호")
            contract_date = _first_text(row, "기준일자")
            winning_company = _first_text(row, "계약시점대표업체명")

            if contract_id and contract_amount is not None:
                upsert_contract(
                    conn=conn,
                    contract_id=contract_id,
                    notice_id=notice_id,
                    contract_amount=contract_amount,
                    contract_date=contract_date or None,
                    changed_amount=contract_amount,
                    category=category,
                )
                contracts_upserted += 1

            if contract_amount is not None and base_amount > 0:
                upsert_bid_result(
                    conn=conn,
                    notice_id=notice_id,
                    award_amount=contract_amount,
                    bid_rate=round((contract_amount / base_amount) * 100.0, 6),
                    bidder_count=0,
                    winning_company=winning_company,
                    result_status="awarded",
                    category=category,
                )
                results_upserted += 1

    return ContractCsvFileImportResult(
        path=str(path),
        rows_seen=rows_seen,
        notices_upserted=notices_upserted,
        results_upserted=results_upserted,
        contracts_upserted=contracts_upserted,
        skipped_unknown_category=skipped_unknown_category,
        skipped_missing_notice_id=skipped_missing_notice_id,
    )


class _DictReaderContext:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.file = None
        self.reader = None

    def __enter__(self):
        encoding, header_line = _detect_csv_layout(self.path)
        self.file = self.path.open(encoding=encoding, newline="")
        for _ in range(header_line - 1):
            next(self.file)
        self.reader = csv.DictReader(self.file, delimiter="\t")
        return self.reader

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.file is not None:
            self.file.close()


def _open_dict_reader(path: Path) -> _DictReaderContext:
    return _DictReaderContext(path)


def _detect_csv_layout(path: Path) -> tuple[str, int]:
    encodings = ("utf-16", "utf-16le", "cp949", "utf-8-sig")
    for encoding in encodings:
        try:
            with path.open(encoding=encoding) as handle:
                for line_no, line in enumerate(handle, 1):
                    if all(token in line for token in _HEADER_HINTS):
                        return encoding, line_no
        except UnicodeError:
            continue
    raise ValueError(f"Unable to locate CSV header row in {path}")


def _category_from_row(row: dict[str, str]) -> str:
    raw = (row.get("조달업무구분") or "").strip()
    return _CATEGORY_MAP.get(raw, "")


def _notice_id_from_row(row: dict[str, str]) -> str:
    number = (row.get("입찰공고번호") or "").strip()
    order = (row.get("입찰공고차수") or "").strip()
    if not number:
        return ""
    if "-" in number:
        return number
    return f"{number}-{order.zfill(3) if order else '000'}"


def _first_text(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
