#!/usr/bin/env bash
# Resilience test: kill the worker that owns a run mid-execution and verify the
# run is reclaimed by another worker via XAUTOCLAIM without being lost.
#
# Needs a running k8s deployment and kubectl access (sudo on k3s).
#
# Usage:
#   API_BASE=http://<node-ip> KCTL="sudo k3s kubectl" ./tests/resilience/worker_failover.sh
#   API_TOKEN=<token> ./tests/resilience/worker_failover.sh
#
# If API_TOKEN is not provided, the script registers a throwaway test user.
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"
KCTL="${KCTL:-sudo k3s kubectl}"
NS="${NS:-job-scheduler}"
B="$API_BASE/api/v1"

# A job that runs long enough for us to kill its worker before it finishes.
# CLAIM_MIN_IDLE_MS (default 30s) governs how fast another worker reclaims it.
SLEEP_SECS="${SLEEP_SECS:-50}"
TEST_USER="${TEST_USER:-failover-$(date +%s)-$RANDOM}"
TEST_PASSWORD="${TEST_PASSWORD:-failover-password}"

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

pod_for_worker() {
  local worker_id="$1"
  local pod
  for pod in $($KCTL -n "$NS" get pods -l app=worker -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}'); do
    case "$worker_id" in
      "$pod"-*) echo "$pod"; return 0;;
    esac
  done
  return 1
}

bootstrap_token

echo ">> creating long-running shell job (sleep ${SLEEP_SECS}s)"
JOB=$(api_raw POST /jobs "{\"name\":\"failover-$(date +%s)\",\"task_type\":\"shell\",\"timeout_sec\":300,\"task_spec\":{\"command\":\"sh\",\"args\":[\"-c\",\"echo start; sleep ${SLEEP_SECS}; echo done\"]}}" | jq -r .id)
RUN=$(api_raw POST "/jobs/$JOB/trigger" | jq -r .id)
echo ">> job=$JOB run=$RUN"

echo ">> waiting for the run to start (status=running)"
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

WORKER=$(api_raw GET "/runs/$RUN" | jq -r .worker_id)
if [ -z "$WORKER" ] || [ "$WORKER" = "null" ]; then
  echo ">> FAIL: run is running but worker_id is empty" >&2
  exit 1
fi
echo ">> run is running on worker_id=$WORKER"

echo ">> killing the worker pod that owns this run"
POD=$(pod_for_worker "$WORKER" || true)
if [ -z "$POD" ]; then
  echo ">> FAIL: could not map worker_id=$WORKER to a worker pod" >&2
  $KCTL -n "$NS" get pods -l app=worker -o wide >&2
  exit 1
fi
$KCTL -n "$NS" delete pod "$POD" --grace-period=0 --force
echo ">> deleted pod $POD; another worker should reclaim the run after the idle timeout"

echo ">> waiting for the run to reach a terminal state"
for _ in $(seq 1 120); do
  st=$(api_raw GET "/runs/$RUN" | jq -r .status)
  case "$st" in
    succeeded) echo ">> PASS: run $RUN completed (status=$st) despite the worker kill"; exit 0;;
    failed|timed_out|canceled) echo ">> FAIL: run ended as $st"; exit 1;;
  esac
  sleep 2
done
echo ">> FAIL: run did not finish in time"; exit 1
