#!/usr/bin/env bash
# Compose variant of the failover test (used by CI; no k8s needed).
# Starts a long run, kills the worker container that owns it mid-execution, and
# asserts the run is reclaimed by the other worker (XAUTOCLAIM) and completes.
#
# Assumes the full stack is already up: docker compose --profile full up -d
# If API_TOKEN is not provided, the script registers a throwaway test user.
set -euo pipefail

B="${API_BASE:-http://localhost:8000}/api/v1"
TEST_USER="${TEST_USER:-failover-$(date +%s)-$RANDOM}"
TEST_PASSWORD="${TEST_PASSWORD:-failover-password}"
SLEEP_SECS="${SLEEP_SECS:-12}"

api_raw() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local tmp status
  tmp=$(mktemp)
  if [ -n "$body" ]; then
    status=$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$B$path" \
      -H 'Content-Type: application/json' \
      -H "Authorization: Bearer $API_TOKEN" \
      -d "$body")
  else
    status=$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$B$path" \
      -H "Authorization: Bearer $API_TOKEN")
  fi
  if [ "$status" -lt 200 ] || [ "$status" -ge 300 ]; then
    echo ">> API $method $path failed with HTTP $status" >&2
    cat "$tmp" >&2
    echo >&2
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

bootstrap_token() {
  if [ -n "${API_TOKEN:-}" ]; then
    return
  fi

  echo ">> registering throwaway test user $TEST_USER"
  API_TOKEN=$(curl -sS -X POST "$B/auth/register" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$TEST_USER\",\"password\":\"$TEST_PASSWORD\"}" \
    | jq -r .access_token)

  if [ -z "$API_TOKEN" ] || [ "$API_TOKEN" = "null" ]; then
    echo ">> failed to obtain API token; check /auth/register and AUTH_SECRET_KEY" >&2
    exit 1
  fi
}

container_for_worker() {
  local worker_id="$1"
  local container_id hostname
  for container_id in $(docker compose --profile full ps -q worker); do
    hostname=$(docker inspect -f '{{.Config.Hostname}}' "$container_id")
    case "$worker_id" in
      "$hostname"-*) echo "$container_id"; return 0;;
    esac
  done
  return 1
}

bootstrap_token

echo ">> creating long-running shell job"
JOB=$(api_raw POST /jobs "{\"name\":\"failover-$(date +%s)\",\"task_type\":\"shell\",\"timeout_sec\":300,\"task_spec\":{\"command\":\"sh\",\"args\":[\"-c\",\"echo start; sleep ${SLEEP_SECS}; echo done\"]}}" | jq -r .id)
RUN=$(api_raw POST "/jobs/$JOB/trigger" | jq -r .id)
echo ">> job=$JOB run=$RUN"

st=""
for _ in $(seq 1 30); do
  st=$(api_raw GET "/runs/$RUN" | jq -r .status)
  [ "$st" = "running" ] && break
  sleep 1
done
if [ "$st" != "running" ]; then
  echo ">> FAIL: run did not start; last status=$st" >&2
  exit 1
fi
echo ">> status before kill: $st"

WORKER=$(api_raw GET "/runs/$RUN" | jq -r .worker_id)
if [ -z "$WORKER" ] || [ "$WORKER" = "null" ]; then
  echo ">> FAIL: run is running but worker_id is empty" >&2
  exit 1
fi
CID=$(container_for_worker "$WORKER" || true)
if [ -z "$CID" ]; then
  echo ">> FAIL: could not map worker_id=$WORKER to a worker container" >&2
  docker compose --profile full ps worker >&2
  exit 1
fi

echo ">> killing worker container $CID (worker_id=$WORKER)"
docker kill "$CID"

for _ in $(seq 1 90); do
  st=$(api_raw GET "/runs/$RUN" | jq -r .status)
  case "$st" in
    succeeded) echo ">> PASS: run reclaimed and completed despite worker kill"; exit 0;;
    failed|timed_out|canceled) echo ">> FAIL: run ended as $st"; exit 1;;
  esac
  sleep 2
done
echo ">> FAIL: run did not finish in time"; exit 1
