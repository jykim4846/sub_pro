#!/usr/bin/env bash
# Daily incremental collection from 조달청 OpenAPI.
# - Pulls only data newer than the DB's latest opened_at per category.
# - Uses the same service key from .env / DATA_GO_KR_SERVICE_KEY env var.
# - Safe to run more than once a day (idempotent per notice_id).
#
# Usage:
#   bash scripts/daily-api-collect.sh
#
# Environment overrides:
#   DB_PATH            default: data/bids.db
#   CATEGORIES         default: "service goods construction"
#   SOURCES            default: "notices,results,contracts"
#   FALLBACK_DAYS      default: 30  (only used when DB empty for a category)
#   SLEEP_BETWEEN      default: 2   (seconds; be gentle with the API)
#   LOG_DIR            default: .omc/logs

set -euo pipefail

cd "$(dirname "$0")/.."

DB_PATH="${DB_PATH:-data/bids.db}"
CATEGORIES="${CATEGORIES:-service goods construction}"
SOURCES="${SOURCES:-notices,results,contracts}"
FALLBACK_DAYS="${FALLBACK_DAYS:-30}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-2}"
LOG_DIR="${LOG_DIR:-.omc/logs}"
PYTHON="${PYTHON:-python3.11}"

mkdir -p "${LOG_DIR}"
TS="$(date +%Y%m%dT%H%M%S)"
LOG_FILE="${LOG_DIR}/daily-api-collect-${TS}.log"

{
    echo "[start] $(date -Iseconds)"
    echo "db=${DB_PATH}  categories='${CATEGORIES}'  sources=${SOURCES}  fallback_days=${FALLBACK_DAYS}"
    for CAT in ${CATEGORIES}; do
        echo "---"
        echo "[collect] category=${CAT}"
        ${PYTHON} -m g2b_bid_reco.cli collect-recent \
            --db-path "${DB_PATH}" \
            --category "${CAT}" \
            --sources "${SOURCES}" \
            --fallback-days "${FALLBACK_DAYS}" \
            || echo "[warn] collect-recent failed for ${CAT}"
        sleep "${SLEEP_BETWEEN}"
    done
    echo "---"
    echo "[snapshot] weekly metrics"
    ${PYTHON} - <<'PY'
from g2b_bid_reco.db import take_weekly_snapshot, auto_generate_suggestions
sid = take_weekly_snapshot("data/bids.db", fee_rate=0.0005)
print(f"snapshot_id={sid}, auto_suggestions={auto_generate_suggestions('data/bids.db')}")
PY
    echo "[done] $(date -Iseconds)"
} | tee -a "${LOG_FILE}"
