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
#   AUTO_BID_SINCE_DAYS default: 14 (daily operational mode; limits auto-bid
#                         to recent pending notices after the initial full load)
#   AUTO_BID_TARGET_WIN default: 0.75
#   EVALUATE_MOCK_BIDS default: 1
#   STRATEGY_CALIBRATE_ENABLED default: 0  (opt-in Path C EMA — turn on
#                         once mock_bid_evaluations has accrued enough
#                         decided rows; recommended weekly, not daily)
#   STRATEGY_CALIBRATE_ALPHA default: 0.1
#   STRATEGY_CALIBRATE_MIN_DECIDED default: 20
#   FLOOR_RATE_CLEANUP_ENABLED default: 1  (fill NULL/0/outlier floor_rate
#                         on 전자입찰 scope with the scope modal; idempotent)
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
AUTO_BID_SINCE_DAYS="${AUTO_BID_SINCE_DAYS:-14}"
AUTO_BID_TARGET_WIN="${AUTO_BID_TARGET_WIN:-0.75}"
EVALUATE_MOCK_BIDS="${EVALUATE_MOCK_BIDS:-1}"
STRATEGY_CALIBRATE_ENABLED="${STRATEGY_CALIBRATE_ENABLED:-0}"
STRATEGY_CALIBRATE_ALPHA="${STRATEGY_CALIBRATE_ALPHA:-0.1}"
STRATEGY_CALIBRATE_MIN_DECIDED="${STRATEGY_CALIBRATE_MIN_DECIDED:-20}"
FLOOR_RATE_CLEANUP_ENABLED="${FLOOR_RATE_CLEANUP_ENABLED:-1}"
SYNC_DEMAND_AGENCIES="${SYNC_DEMAND_AGENCIES:-1}"
G2B_USER_INFO_ENDPOINT="${G2B_USER_INFO_ENDPOINT:-}"
DEMAND_AGENCY_SINCE="${DEMAND_AGENCY_SINCE:-}"
DEMAND_AGENCY_UNTIL="${DEMAND_AGENCY_UNTIL:-}"

mkdir -p "${LOG_DIR}"
TS="$(date +%Y%m%dT%H%M%S)"
LOG_FILE="${LOG_DIR}/daily-api-collect-${TS}.log"

# Turso sync is optional: only runs if turso CLI is installed and TURSO_SYNC
# is not explicitly disabled. Failures are soft — a missing/broken sync
# must not prevent the daily batch from running.
TURSO_SYNC="${TURSO_SYNC:-1}"

{
    echo "[start] $(date -Iseconds)"
    echo "db=${DB_PATH}  categories='${CATEGORIES}'  sources=${SOURCES}  fallback_days=${FALLBACK_DAYS}"
    if [ "${TURSO_SYNC}" = "1" ] && command -v turso >/dev/null 2>&1; then
        echo "---"
        echo "[turso-pull] fetching latest cloud snapshot before batch"
        bash scripts/turso-pull.sh || echo "[warn] turso-pull failed — continuing with local DB"
    fi
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
    if [ "${FLOOR_RATE_CLEANUP_ENABLED}" = "1" ]; then
        echo "---"
        echo "[floor-rate] filling NULL/0/outlier floor_rate on 전자입찰 scope"
        "${PYTHON_BIN}" -m g2b_bid_reco.cli cleanup-floor-rates \
            --db-path "${DB_PATH}" \
            || echo "[warn] cleanup-floor-rates failed"
    fi
    if [ "${EVALUATE_MOCK_BIDS}" = "1" ]; then
        echo "---"
        echo "[evaluate] materializing mock-bid verdicts for today's collected results"
        "${PYTHON_BIN}" -m g2b_bid_reco.cli evaluate-mock-bids \
            --db-path "${DB_PATH}" \
            --today-results-only \
            || echo "[warn] evaluate-mock-bids failed"
    fi
    if [ "${STRATEGY_CALIBRATE_ENABLED}" = "1" ]; then
        echo "---"
        echo "[strategy-calibrate] EMA calibration on strategy_tables (Path C)"
        "${PYTHON_BIN}" -m g2b_bid_reco.cli update-strategy-tables \
            --db-path "${DB_PATH}" \
            --alpha "${STRATEGY_CALIBRATE_ALPHA}" \
            --min-decided "${STRATEGY_CALIBRATE_MIN_DECIDED}" \
            || echo "[warn] update-strategy-tables failed"
    fi
    if [ "${AUTO_BID_ENABLED}" = "1" ]; then
        echo "---"
        echo "[auto-bid] generating pending-notice portfolios"
        AUTO_BID_ARGS=(
            -m g2b_bid_reco.cli auto-bid-pending
            --db-path "${DB_PATH}"
            --limit "${AUTO_BID_LIMIT}"
            --num-customers "${AUTO_BID_CUSTOMERS}"
            --top-k "${AUTO_BID_TOP_K}"
            --target-win-probability "${AUTO_BID_TARGET_WIN}"
        )
        # After the initial bootstrap/backfill, daily operation should touch
        # only recent pending notices. Older pending notices were already
        # covered by the full load and can be reprocessed explicitly if needed.
        if [ -n "${AUTO_BID_SINCE_DAYS}" ] && [ "${AUTO_BID_SINCE_DAYS}" -gt 0 ] 2>/dev/null; then
            AUTO_BID_ARGS+=( --since-days "${AUTO_BID_SINCE_DAYS}" )
        fi
        "${PYTHON_BIN}" "${AUTO_BID_ARGS[@]}" || echo "[warn] auto-bid-pending failed"
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
    if [ "${TURSO_SYNC}" = "1" ] && command -v turso >/dev/null 2>&1; then
        echo "---"
        echo "[turso-push] uploading updated DB to cloud"
        bash scripts/turso-push.sh || echo "[warn] turso-push failed — local DB intact"
    fi
    echo "[done] $(date -Iseconds)"
} | tee -a "${LOG_FILE}"
