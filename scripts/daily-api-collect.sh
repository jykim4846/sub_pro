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
#   AUTO_BID_ENABLED   default: 1   (generate/update auto mock-bid portfolios after collection)
#   AUTO_BID_CUSTOMERS default: 5
#   AUTO_BID_TOP_K     default: 10
#   AUTO_BID_LIMIT     default: 500 (set 0 to process all pending; heavy)
#   AUTO_BID_TARGET_WIN default: 0.75
#   SYNC_DEMAND_AGENCIES default: 1
#   G2B_USER_INFO_ENDPOINT optional exact demand-agency operation URL
#   DEMAND_AGENCY_SINCE optional YYYYMMDD or YYYYMMDDHHMM for agency sync window start
#   DEMAND_AGENCY_UNTIL optional YYYYMMDD or YYYYMMDDHHMM for agency sync window end

set -euo pipefail

cd "$(dirname "$0")/.."

DB_PATH="${DB_PATH:-data/bids.db}"
CATEGORIES="${CATEGORIES:-service goods construction}"
SOURCES="${SOURCES:-notices,results,contracts}"
FALLBACK_DAYS="${FALLBACK_DAYS:-30}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-2}"
LOG_DIR="${LOG_DIR:-.omc/logs}"
if [ -n "${PYTHON:-}" ]; then
    PYTHON_BIN="${PYTHON}"
elif command -v python3.11 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3.11)"
elif command -v /usr/local/bin/python3.11 >/dev/null 2>&1; then
    PYTHON_BIN="/usr/local/bin/python3.11"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
else
    echo "[error] python executable not found" >&2
    exit 127
fi
AUTO_BID_ENABLED="${AUTO_BID_ENABLED:-1}"
AUTO_BID_CUSTOMERS="${AUTO_BID_CUSTOMERS:-5}"
AUTO_BID_TOP_K="${AUTO_BID_TOP_K:-10}"
AUTO_BID_LIMIT="${AUTO_BID_LIMIT:-500}"
AUTO_BID_TARGET_WIN="${AUTO_BID_TARGET_WIN:-0.75}"
SYNC_DEMAND_AGENCIES="${SYNC_DEMAND_AGENCIES:-1}"
G2B_USER_INFO_ENDPOINT="${G2B_USER_INFO_ENDPOINT:-}"
DEMAND_AGENCY_SINCE="${DEMAND_AGENCY_SINCE:-}"
DEMAND_AGENCY_UNTIL="${DEMAND_AGENCY_UNTIL:-}"

mkdir -p "${LOG_DIR}"
TS="$(date +%Y%m%dT%H%M%S)"
LOG_FILE="${LOG_DIR}/daily-api-collect-${TS}.log"

{
    echo "[start] $(date -Iseconds)"
    echo "db=${DB_PATH}  categories='${CATEGORIES}'  sources=${SOURCES}  fallback_days=${FALLBACK_DAYS}"
    for CAT in ${CATEGORIES}; do
        echo "---"
        echo "[collect] category=${CAT}"
        "${PYTHON_BIN}" -m g2b_bid_reco.cli collect-recent \
            --db-path "${DB_PATH}" \
            --category "${CAT}" \
            --sources "${SOURCES}" \
            --fallback-days "${FALLBACK_DAYS}" \
            || echo "[warn] collect-recent failed for ${CAT}"
        sleep "${SLEEP_BETWEEN}"
    done
    if [ "${SYNC_DEMAND_AGENCIES}" = "1" ]; then
        echo "---"
        echo "[agency-sync] syncing demand agency master"
        AGENCY_ARGS=( -m g2b_bid_reco.cli sync-demand-agencies --db-path "${DB_PATH}" )
        if [ -n "${G2B_USER_INFO_ENDPOINT}" ]; then
            AGENCY_ARGS+=( --endpoint "${G2B_USER_INFO_ENDPOINT}" )
        fi
        if [ -n "${DEMAND_AGENCY_SINCE}" ]; then
            AGENCY_ARGS+=( --since "${DEMAND_AGENCY_SINCE}" )
        fi
        if [ -n "${DEMAND_AGENCY_UNTIL}" ]; then
            AGENCY_ARGS+=( --until "${DEMAND_AGENCY_UNTIL}" )
        fi
        "${PYTHON_BIN}" "${AGENCY_ARGS[@]}" || echo "[warn] sync-demand-agencies failed"
    fi
    if [ "${AUTO_BID_ENABLED}" = "1" ]; then
        echo "---"
        echo "[auto-bid] generating pending-notice portfolios"
        "${PYTHON_BIN}" -m g2b_bid_reco.cli auto-bid-pending \
            --db-path "${DB_PATH}" \
            --limit "${AUTO_BID_LIMIT}" \
            --num-customers "${AUTO_BID_CUSTOMERS}" \
            --top-k "${AUTO_BID_TOP_K}" \
            --target-win-probability "${AUTO_BID_TARGET_WIN}" \
            || echo "[warn] auto-bid-pending failed"
    fi
    echo "---"
    echo "[snapshot] weekly metrics"
    export G2B_DAILY_DB_PATH="${DB_PATH}"
    "${PYTHON_BIN}" - <<'PY'
import os
from g2b_bid_reco.db import take_weekly_snapshot, auto_generate_suggestions
db_path = os.environ["G2B_DAILY_DB_PATH"]
sid = take_weekly_snapshot(db_path, fee_rate=0.0005)
print(f"snapshot_id={sid}, auto_suggestions={auto_generate_suggestions(db_path)}")
PY
    echo "[done] $(date -Iseconds)"
} | tee -a "${LOG_FILE}"
