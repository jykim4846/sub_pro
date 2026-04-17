#!/usr/bin/env bash
set -euo pipefail

# Enrich stub notices for one or more categories in sequence, logging stub
# counts before/after each pass. Safe to interrupt and rerun: `enrich-stubs`
# only touches rows that still look like stubs.
#
# Usage:
#   bash scripts/enrich-all-stubs.sh                # default: goods then construction
#   bash scripts/enrich-all-stubs.sh service        # just one category
#   DB_PATH=path/to/bids.db SLEEP_SEC=1.5 bash scripts/enrich-all-stubs.sh

DB_PATH="${DB_PATH:-data/bids.db}"
SLEEP_SEC="${SLEEP_SEC:-1.0}"

if [ $# -eq 0 ]; then
    CATEGORIES=(goods construction)
else
    CATEGORIES=("$@")
fi

count_stubs() {
    sqlite3 "${DB_PATH}" "
        SELECT COUNT(*) FROM bid_notices n
        JOIN bid_results r ON n.notice_id = r.notice_id
        WHERE n.category = '$1'
          AND (n.agency_name = '' OR n.contract_method = '' OR n.base_amount <= 0 OR r.bid_rate <= 0);
    "
}

count_usable() {
    sqlite3 "${DB_PATH}" "
        SELECT COUNT(*) FROM bid_notices n
        JOIN bid_results r ON n.notice_id = r.notice_id
        WHERE n.category = '$1'
          AND n.agency_name != '' AND n.contract_method != ''
          AND n.base_amount > 0 AND r.bid_rate > 0;
    "
}

for cat in "${CATEGORIES[@]}"; do
    echo "=========================================="
    echo " enriching category: ${cat}"
    echo "=========================================="
    stubs_before="$(count_stubs "${cat}")"
    usable_before="$(count_usable "${cat}")"
    echo "stubs  before: ${stubs_before}"
    echo "usable before: ${usable_before}"

    python3 -m g2b_bid_reco.cli enrich-stubs \
        --db-path "${DB_PATH}" \
        --category "${cat}" \
        --sleep-sec "${SLEEP_SEC}"

    stubs_after="$(count_stubs "${cat}")"
    usable_after="$(count_usable "${cat}")"
    echo "stubs  after:  ${stubs_after}"
    echo "usable after:  ${usable_after}"
    delta=$((usable_after - usable_before))
    echo "usable delta:  +${delta}"
    echo
done

echo "Done. All requested categories processed."
