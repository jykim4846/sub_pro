"""Floor-rate normalisation for the v2 within-notice 평균가 auction model.

Why this exists:
- 한국 조달청 복수예가/낙찰하한제 룰은 "기준가격 = 예정가격 × 하한율" 을 사용.
  v2 시뮬레이션의 기준가격(T) 는 historical bid_rate 에서 샘플링하므로
  floor_rate 자체가 시뮬에 직접 들어가진 않지만, ``simulation.generate_customer_bids``
  의 ``floor_guard`` 가 0 이 되면 하한 제약이 무력화돼 디스쿼리 판정이 빠짐.
- API/CSV 출처마다 floor_rate 가 NULL/0/outlier 로 채워지지 않은 행이 많음.
  진단 결과 "전자입찰" 카테고리의 NULL/0 비율이 운영상 가장 critical.

Strategy:
- 대상 contract_method (default: 전자입찰) 의 (category, contract_method)
  별 modal floor_rate 를 산출 (충분한 표본 ``min_modal_n`` 이상에서만).
- 같은 scope 의 NULL / 0 / outlier (< 50% 또는 > 99%) 행을 그 modal 로 채움.
- Idempotent — 다시 돌려도 변화 없음.

다른 contract_method 는 하한율 룰 자체가 없어 의미 없으므로 기본 대상에서 빠짐.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .db import connect


DEFAULT_TARGET_METHODS: tuple[str, ...] = ("전자입찰",)
OUTLIER_LOWER = 50.0
OUTLIER_UPPER = 99.0
DEFAULT_MIN_MODAL_N = 30


def cleanup_floor_rates(
    db_path: str | Path,
    contract_methods: Iterable[str] = DEFAULT_TARGET_METHODS,
    min_modal_n: int = DEFAULT_MIN_MODAL_N,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Fill NULL/0/outlier floor_rate values with the scope's modal value.

    Returns a summary including the chosen modal per scope and per-scope
    update counts so the caller (CLI) can log clearly.
    """
    methods = tuple(m for m in contract_methods if m)
    if not methods:
        raise ValueError("contract_methods must contain at least one value")
    if min_modal_n < 1:
        raise ValueError(f"min_modal_n must be >= 1, got {min_modal_n}")

    placeholders = ",".join("?" for _ in methods)
    summary: dict[str, Any] = {
        "contract_methods": list(methods),
        "min_modal_n": min_modal_n,
        "dry_run": bool(dry_run),
        "scopes": [],
        "rows_updated": 0,
        "rows_skipped_no_modal": 0,
    }

    with connect(db_path) as conn:
        scope_rows = conn.execute(
            f"""
            SELECT category, contract_method
            FROM bid_notices
            WHERE category <> '' AND contract_method IN ({placeholders})
            GROUP BY category, contract_method
            """,
            methods,
        ).fetchall()

        for scope in scope_rows:
            cat = scope["category"]
            method = scope["contract_method"]
            modal_row = conn.execute(
                f"""
                SELECT floor_rate, COUNT(*) AS n
                FROM bid_notices
                WHERE category = ? AND contract_method = ?
                  AND floor_rate IS NOT NULL
                  AND floor_rate >= ? AND floor_rate <= ?
                GROUP BY floor_rate
                ORDER BY n DESC
                LIMIT 1
                """,
                (cat, method, OUTLIER_LOWER, OUTLIER_UPPER),
            ).fetchone()

            if modal_row is None or int(modal_row["n"]) < min_modal_n:
                summary["rows_skipped_no_modal"] += 1
                summary["scopes"].append(
                    {
                        "category": cat,
                        "contract_method": method,
                        "modal": None,
                        "modal_n": int(modal_row["n"]) if modal_row else 0,
                        "updated": 0,
                        "skipped": True,
                    }
                )
                continue

            modal = float(modal_row["floor_rate"])
            modal_n = int(modal_row["n"])

            target_count_row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM bid_notices
                WHERE category = ? AND contract_method = ?
                  AND (floor_rate IS NULL OR floor_rate = 0
                       OR floor_rate < ? OR floor_rate > ?)
                """,
                (cat, method, OUTLIER_LOWER, OUTLIER_UPPER),
            ).fetchone()
            target_count = int(target_count_row["n"] or 0)

            if target_count and not dry_run:
                conn.execute(
                    """
                    UPDATE bid_notices
                    SET floor_rate = ?
                    WHERE category = ? AND contract_method = ?
                      AND (floor_rate IS NULL OR floor_rate = 0
                           OR floor_rate < ? OR floor_rate > ?)
                    """,
                    (modal, cat, method, OUTLIER_LOWER, OUTLIER_UPPER),
                )

            summary["rows_updated"] += target_count
            summary["scopes"].append(
                {
                    "category": cat,
                    "contract_method": method,
                    "modal": modal,
                    "modal_n": modal_n,
                    "updated": target_count,
                    "skipped": False,
                }
            )

    return summary
