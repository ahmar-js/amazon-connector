#!/usr/bin/env bash
set -euo pipefail

###############################################
# wait_for: Utility function to wait for a service
# Usage: wait_for "redis:6379" 10
#   - param1 = host:port (e.g. redis:6379)
#   - param2 = max retries (default: 30)
###############################################
wait_for() {
  host="${1%:*}"   # everything before ":"
  port="${1##*:}"  # everything after ":"
  retries="${2:-30}"  # default max retries = 30
  count=0

  if [ -z "$host" ] || [ -z "$port" ]; then
    echo "[wait_for] Invalid host/port: $1"
    return 0
  fi

  echo "[wait_for] Waiting for $host:$port (max $retries retries)..."
  until nc -z "$host" "$port"; do
    count=$((count+1))
    if [ "$count" -ge "$retries" ]; then
      echo "[wait_for] WARNING: $host:$port not reachable after $retries attempts, continuing anyway."
      return 1
    fi
    sleep 1
  done
  echo "[wait_for] SUCCESS: Connected to $host:$port"
}

###############################################
# If WAIT_FOR is set, check each dependency
# Example: WAIT_FOR=redis:6379
###############################################
if [ -n "${WAIT_FOR:-}" ]; then
  IFS=',' read -ra ADDR <<< "$WAIT_FOR"
  for service in "${ADDR[@]}"; do
    wait_for "$service" 20   # retry up to 20 times (~20s)
  done
else
  echo "[entrypoint] No WAIT_FOR dependencies configured."
fi

###############################################
# Run Django migrations
###############################################
echo "[entrypoint] Running Django migrations..."
python /app/amazon_connector/manage.py migrate --noinput

###############################################
# Collect static files (idempotent)
###############################################
if [ "${DJANGO_COLLECTSTATIC:-1}" != "0" ]; then
  echo "[entrypoint] Collecting static files..."
  python /app/amazon_connector/manage.py collectstatic --noinput
fi

###############################################
# Finally exec the CMD passed by Dockerfile/Compose
###############################################
echo "[entrypoint] Starting main process: $*"
exec "$@"
