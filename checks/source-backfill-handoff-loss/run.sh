#!/usr/bin/env bash
# run.sh: fails if a view created on a paused shared Kafka source loses rows at its backfill hand-off (risingwavelabs/risingwave#27976).
set -euo pipefail
cd "$(dirname "$0")"
: "${RW_IMAGE:?set RW_IMAGE}"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
trap 'docker compose down -v --remove-orphans >/dev/null 2>&1' EXIT
docker compose up -d --wait kafka risingwave
RW_SERVICE=risingwave PYTHONUNBUFFERED=1 uv run --quiet repro.py
