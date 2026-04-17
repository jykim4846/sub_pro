#!/usr/bin/env bash
set -euo pipefail

# Restore the local SQLite DB from the published db-snapshot release.
# Existing data/bids.db is moved to data/bids.db.bak before overwriting.
#
# Usage:
#   bash scripts/pull-db-snapshot.sh
#
# Environment overrides:
#   DB_PATH   destination DB path (default: data/bids.db)
#   TAG       release tag to pull from (default: db-snapshot)
#   REPO      owner/repo (default: inferred from `gh repo view`)

DB_PATH="${DB_PATH:-data/bids.db}"
TAG="${TAG:-db-snapshot}"
REPO="${REPO:-$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)}"

if [ -z "${REPO}" ]; then
    echo "REPO not resolved. Set REPO=owner/repo or run inside a gh-authenticated repo." >&2
    exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
    echo "gh CLI is required. See https://cli.github.com/" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "Downloading ${TAG} asset from ${REPO} ..."
gh release download "${TAG}" --repo "${REPO}" -p bids.db.gz --dir "${TMP_DIR}"

mkdir -p "$(dirname "${DB_PATH}")"
if [ -f "${DB_PATH}" ]; then
    BACKUP="${DB_PATH}.bak"
    mv "${DB_PATH}" "${BACKUP}"
    echo "Previous DB moved to ${BACKUP}"
fi

gunzip -c "${TMP_DIR}/bids.db.gz" > "${DB_PATH}"
echo "Restored DB to ${DB_PATH} ($(du -h "${DB_PATH}" | awk '{print $1}'))"
