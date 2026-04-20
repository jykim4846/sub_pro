#!/usr/bin/env bash
# Disable the com.sub_pro.daily-collect launchd job on this Mac.
# Idempotent — safe to run when already disabled or never installed.
#
# Why this exists: as of 2026-04-21 the daily batch runs ONLY on the home PC
# (10:00 KST). Running it on the company PC at the same hour would race on
# turso push (last-push-wins). Run this once on the company PC after the
# decision was made, or any time you want to hand primary control to a
# different machine.

set -euo pipefail

LABEL="com.sub_pro.daily-collect"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

if [ ! -f "$PLIST" ]; then
    echo "✓ no plist installed at $PLIST — nothing to disable"
    exit 0
fi

if launchctl print "gui/$(id -u)/$LABEL" >/dev/null 2>&1; then
    echo "→ booting out $LABEL..."
    launchctl bootout "gui/$(id -u)" "$PLIST"
    echo "✓ $LABEL removed from launchd"
else
    echo "✓ $LABEL already not loaded in launchd"
fi

echo "note: $PLIST still present on disk. To also remove it:"
echo "    rm $PLIST"
