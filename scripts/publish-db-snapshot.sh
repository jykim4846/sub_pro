#!/usr/bin/env bash
set -euo pipefail

# Publish the local SQLite DB as a fixed "db-snapshot" GitHub Release asset.
# Overwrites the previous snapshot so the latest asset URL never changes.
#
# Usage:
#   bash scripts/publish-db-snapshot.sh
#
# Environment overrides:
#   DB_PATH   path to the SQLite DB (default: data/bids.db)
#   TAG       release tag to reuse (default: db-snapshot)
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

if [ ! -f "${DB_PATH}" ]; then
    echo "DB file not found: ${DB_PATH}" >&2
    exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT
GZ_PATH="${TMP_DIR}/bids.db.gz"
META_PATH="${TMP_DIR}/snapshot.txt"

echo "Compressing ${DB_PATH} ..."
gzip -9 -c "${DB_PATH}" > "${GZ_PATH}"

GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DB_SIZE="$(du -h "${DB_PATH}" | awk '{print $1}')"
GZ_SIZE="$(du -h "${GZ_PATH}" | awk '{print $1}')"
SHA="$(shasum -a 256 "${GZ_PATH}" | awk '{print $1}')"

cat > "${META_PATH}" <<EOF
# DB snapshot

* Generated: ${GENERATED_AT}
* Source DB: ${DB_PATH}
* Raw size: ${DB_SIZE}
* Compressed size: ${GZ_SIZE}
* SHA-256 (bids.db.gz): ${SHA}

Restore on another machine:

\`\`\`bash
bash scripts/pull-db-snapshot.sh
\`\`\`

Or manually:

\`\`\`bash
mkdir -p data
gh release download ${TAG} --repo ${REPO} -p bids.db.gz --dir /tmp
gunzip -c /tmp/bids.db.gz > data/bids.db
\`\`\`
EOF

if gh release view "${TAG}" --repo "${REPO}" >/dev/null 2>&1; then
    echo "Removing previous ${TAG} release from ${REPO} ..."
    gh release delete "${TAG}" --cleanup-tag --yes --repo "${REPO}"
fi

echo "Publishing ${TAG} to ${REPO} ..."
gh release create "${TAG}" "${GZ_PATH}" "${META_PATH}" \
    --repo "${REPO}" \
    --title "DB snapshot ${GENERATED_AT}" \
    --notes-file "${META_PATH}"

echo
echo "Done. Latest snapshot asset: https://github.com/${REPO}/releases/download/${TAG}/bids.db.gz"
