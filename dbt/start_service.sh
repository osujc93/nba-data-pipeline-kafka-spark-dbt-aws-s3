#!/bin/bash
# File: start_service.sh
# ----------------------

set -e

# Make sure the logs folder exists:
mkdir -p /dbt/logs
mkdir -p /dbt/target

# Function to find an available port
find_available_port() {
  start_port=8585
  end_port=8600

  for port in $(seq $start_port $end_port); do
    (echo >/dev/tcp/localhost/$port) &>/dev/null
    if [ $? -ne 0 ]; then
      echo $port
      return
    fi
  done

  echo "No available port found in the range $start_port-$end_port"
  exit 1
}

AVAILABLE_PORT=$(find_available_port)
echo "[dbt-service] Will serve dbt docs on port: $AVAILABLE_PORT"

# Serve the docs (no browser)
exec dbt docs serve --port "$AVAILABLE_PORT" --no-browser
