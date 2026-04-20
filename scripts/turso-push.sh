#!/usr/bin/env bash
# Push local SQLite DB to Turso cloud (full replace).
#
# Pre-requisites:
#   - .env with TURSO_DATABASE_URL, TURSO_AUTH_TOKEN
#   - turso CLI installed and authed (turso auth login)
#   - Turso DB already exists (turso db create <name>)
#
# Safety:
#   - Local backup is written to data/backups/ before push.
#   - No concurrency protection — do NOT run simultaneously on multiple PCs.
#     If two PCs push near-simultaneously, the later push wins and earlier
#     writes are LOST. Convention: pull → work → push, never skip pull.

set -euo pipefail

DB_NAME="${TURSO_DB_NAME:-g2bdb}"
DB_PATH="${DB_PATH:-data/bids.db}"
BACKUP_DIR="${BACKUP_DIR:-data/backups}"

if [ ! -f "$DB_PATH" ]; then
    echo "✗ local DB not found: $DB_PATH" >&2
    exit 1
fi

mkdir -p "$BACKUP_DIR"
TS=$(date +%Y%m%dT%H%M%S)
BACKUP="$BACKUP_DIR/bids.pre-push.$TS.db"
cp "$DB_PATH" "$BACKUP"
echo "✓ local backup → $BACKUP"

# Take a consistent snapshot via SQLite online backup API. This handles WAL
# correctly even when another process is writing concurrently (e.g., daily
# batch or enrich-stubs). Reading the .db file directly would miss any
# committed-but-not-yet-checkpointed WAL frames.
SNAPSHOT="/tmp/bids.snapshot.$TS.db"
trap 'rm -f "$SNAPSHOT"' EXIT
echo "→ snapshotting local DB (online backup, WAL-safe)..."
sqlite3 "$DB_PATH" ".backup $SNAPSHOT"
SNAPSHOT_SIZE=$(wc -c < "$SNAPSHOT")
echo "  snapshot size: $SNAPSHOT_SIZE bytes"

# Turso's `db create --from-file` uploads a SQLite file directly (binary,
# ~10x faster than streaming a SQL dump). We destroy+recreate with the same
# name so the URL stays stable.
echo "→ destroying cloud copy (if present) to allow full replacement..."
turso db destroy "$DB_NAME" --yes 2>&1 | grep -v "not found" || true

echo "→ uploading snapshot → Turso ($DB_NAME)..."
turso db create "$DB_NAME" --from-file "$SNAPSHOT"

echo "✓ push complete: $DB_NAME"
echo "  verify URL unchanged with: turso db show --url $DB_NAME"
