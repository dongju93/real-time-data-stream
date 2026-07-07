#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
connect_url=${CONNECT_URL:-http://localhost:8083}
connector_file=${CONNECTOR_FILE:-${script_dir}/postgres-stock-connector.json}
max_attempts=${CONNECT_MAX_ATTEMPTS:-60}
attempt=1

until curl -fsS "${connect_url}/connectors" >/dev/null 2>&1; do
  if [ "${attempt}" -ge "${max_attempts}" ]; then
    echo "Kafka Connect is not ready: ${connect_url}" >&2
    exit 1
  fi

  attempt=$((attempt + 1))
  sleep 2
done

status_code=$(
  curl -sS -o /tmp/register-connector-response -w "%{http_code}" -X POST \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    "${connect_url}/connectors/" \
    -d @"${connector_file}"
)

case "${status_code}" in
  200|201)
    echo "Debezium connector registered."
    ;;
  409)
    echo "Debezium connector already exists."
    ;;
  *)
    echo "Debezium connector registration failed: HTTP ${status_code}" >&2
    exit 1
    ;;
esac
