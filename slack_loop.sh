#!/usr/bin/env bash
# slack_loop.sh — Slack ingest + listener loop (v2.67.57).
#
# Runs as a Render background worker. Two responsibilities:
#   1. slack_sync.py poll  — pull new messages into the local DB
#   2. slack_listener.py once — classify + respond to unprocessed
#
# Why not a single Python process: the two scripts have different
# failure modes. Ingest is cheap and reliable (just SQL writes);
# the listener calls Anthropic (slower, costs money, can fail).
# Running them as separate processes means an LLM blip doesn't
# stop us from ingesting messages, and a SQL hiccup doesn't lose
# already-composed responses.
#
# Cadence: 60s. Tunable via SLACK_LOOP_INTERVAL env var.
#
# No-op when SLACK_BOT_TOKEN is empty — safe to leave enabled
# in environments where Slack isn't configured.

set -uo pipefail

DATA_DIR="${DATA_DIR:-/data}"
LOG="${DATA_DIR}/output/slack_loop.log"
mkdir -p "${DATA_DIR}/output"

INTERVAL="${SLACK_LOOP_INTERVAL:-60}"

stamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

echo "[$(stamp)] slack_loop starting (interval=${INTERVAL}s)" >> "$LOG"

if [ -z "${SLACK_BOT_TOKEN:-}" ]; then
    echo "[$(stamp)] SLACK_BOT_TOKEN not set — exiting cleanly" >> "$LOG"
    exit 0
fi

while true; do
    # Ingest: pull new messages → slack_messages table.
    python slack_sync.py poll >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_sync.poll failed (continuing)" >> "$LOG"

    # Listen: classify + respond to unprocessed.
    python slack_listener.py once >> "$LOG" 2>&1 || \
        echo "[$(stamp)] slack_listener.once failed (continuing)" >> "$LOG"

    sleep "$INTERVAL"
done
