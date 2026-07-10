#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
COMPOSE_FILE="$ROOT_DIR/docker/data-pipeline/docker-compose.yaml"

BASE_URL=${BASE_URL:-http://localhost:8000}
TARGET_RECORDS=${TARGET_RECORDS:-1296000}
SEED_DAYS=${SEED_DAYS:-30}
SEED_BATCH_SIZE=${SEED_BATCH_SIZE:-10000}
SEED_TIMEOUT=${SEED_TIMEOUT:-1800}
K6_VUS=${K6_VUS:-5}
K6_DURATION=${K6_DURATION:-30s}
PYTHON_BIN=${PYTHON_BIN:-python3}
RESULT_DIR=${RESULT_DIR:-${TMPDIR:-/tmp}/real-time-data-stream-performance/$(date +%Y%m%d-%H%M%S)}
SEED_STATUS_FILE=${SEED_STATUS_FILE:-}

for command in "$PYTHON_BIN" curl docker hyperfine k6; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 1
  fi
done

mkdir -p "$RESULT_DIR"

if [[ -n "$SEED_STATUS_FILE" ]]; then
  seed_status=$(<"$SEED_STATUS_FILE")
else
  seed_status=$(
    "$PYTHON_BIN" "$SCRIPT_DIR/seed.py" \
      --base-url "$BASE_URL" \
      --total-records "$TARGET_RECORDS" \
      --days "$SEED_DAYS" \
      --batch-size "$SEED_BATCH_SIZE" \
      --timeout "$SEED_TIMEOUT"
  )
fi
printf '%s\n' "$seed_status" >"$RESULT_DIR/seed-status.json"

start_time=$(
  printf '%s' "$seed_status" | "$PYTHON_BIN" -c \
    'import json, sys; print(json.load(sys.stdin)["rangeStart"])'
)
end_time=$(
  printf '%s' "$seed_status" | "$PYTHON_BIN" -c \
    'import json, sys; print(json.load(sys.stdin)["rangeEnd"])'
)

docker compose -f "$COMPOSE_FILE" exec -T postgres-stock sh -c \
  'psql --set=ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" -v start_time="$1" -v end_time="$2"' \
  sh "$start_time" "$end_time" \
  <"$SCRIPT_DIR/explain.sql" \
  | tee "$RESULT_DIR/explain.txt"

stock_url="${BASE_URL%/}/api/v1/stock"
raw_command="curl --get --fail --silent --show-error --output /dev/null --data-urlencode 'startTime=$start_time' --data-urlencode 'endTime=$end_time' --data-urlencode 'limit=1000' '$stock_url'"
aggregate_command="curl --get --fail --silent --show-error --output /dev/null --data-urlencode 'startTime=$start_time' --data-urlencode 'endTime=$end_time' --data-urlencode 'limit=1000' --data-urlencode 'granularity=minute' '$stock_url'"

hyperfine \
  --warmup 3 \
  --runs 20 \
  --export-json "$RESULT_DIR/hyperfine.json" \
  --command-name raw "$raw_command" \
  --command-name aggregate "$aggregate_command"

k6 run \
  --summary-export "$RESULT_DIR/k6-summary.json" \
  -e "BASE_URL=$BASE_URL" \
  -e "START_TIME=$start_time" \
  -e "END_TIME=$end_time" \
  -e "VUS=$K6_VUS" \
  -e "DURATION=$K6_DURATION" \
  "$SCRIPT_DIR/load.js"

echo "performance results: $RESULT_DIR"
