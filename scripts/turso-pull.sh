#!/usr/bin/env bash
# Pull latest DB snapshot from Turso cloud to local SQLite file.
#
# Pre-requisites:
#   - .env with TURSO_DATABASE_URL, TURSO_AUTH_TOKEN
#   - turso CLI installed and authed
#
# Safety:
#   - Existing local DB is backed up to data/backups/ before overwrite.

set -euo pipefail

DB_NAME="${TURSO_DB_NAME:-g2bdb}"
DB_PATH="${DB_PATH:-data/bids.db}"
BACKUP_DIR="${BACKUP_DIR:-data/backups}"

mkdir -p "$(dirname "$DB_PATH")" "$BACKUP_DIR"
TS=$(date +%Y%m%dT%H%M%S)

if [ -f "$DB_PATH" ]; then
    BACKUP="$BACKUP_DIR/bids.pre-pull.$TS.db"
    cp "$DB_PATH" "$BACKUP"
    echo "✓ local backup → $BACKUP"
fi

echo "→ exporting Turso DB ($DB_NAME)..."
turso db export "$DB_NAME" --output-file "$DB_PATH" --overwrite

# turso db export writes <db>.db-wal alongside; drop it so our SQLite opens clean
WAL="${DB_PATH}-wal"
if [ -f "$WAL" ]; then
    rm "$WAL"
fi

echo "✓ pull complete: $DB_PATH ($(ls -lh "$DB_PATH" | awk '{print $5}'))"
