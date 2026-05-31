#!/usr/bin/env bash
# Resilience test: kill a worker mid-execution and verify the run still completes
# (reclaimed by another worker via XAUTOCLAIM) without being lost.
#
# Needs a running k8s deployment and kubectl access (sudo on k3s).
#
# Usage:
#   API_BASE=http://<node-ip> KCTL="sudo k3s kubectl" ./tests/resilience/worker_failover.sh
#
# Defaults assume the API is reachable on localhost via port-forward or ingress.
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"
KCTL="${KCTL:-sudo k3s kubectl}"
NS="${NS:-job-scheduler}"
B="$API_BASE/api/v1"

# A job that runs long enough for us to kill its worker before it finishes.
# CLAIM_MIN_IDLE_MS (default 30s) governs how fast another worker reclaims it.
SLEEP_SECS="${SLEEP_SECS:-50}"

echo ">> creating long-running shell job (sleep ${SLEEP_SECS}s)"
JOB=$(curl -s -X POST "$B/jobs" -H 'Content-Type: application/json' \
  -d "{\"name\":\"failover-$(date +%s)\",\"task_type\":\"shell\",\"timeout_sec\":300,\"task_spec\":{\"command\":\"sh\",\"args\":[\"-c\",\"echo start; sleep ${SLEEP_SECS}; echo done\"]}}" \
  | jq -r .id)
RUN=$(curl -s -X POST "$B/jobs/$JOB/trigger" | jq -r .id)
echo ">> job=$JOB run=$RUN"

echo ">> waiting for the run to start (status=running)"
for _ in $(seq 1 30); do
  st=$(curl -s "$B/runs/$RUN" | jq -r .status)
  [ "$st" = "running" ] && break
  sleep 1
done
WORKER=$(curl -s "$B/runs/$RUN" | jq -r .worker_id)
echo ">> run is running on worker_id=$WORKER"

echo ">> killing one worker pod to simulate a crash"
POD=$($KCTL -n "$NS" get pods -l app=worker -o jsonpath='{.items[0].metadata.name}')
$KCTL -n "$NS" delete pod "$POD" --grace-period=0 --force
echo ">> deleted pod $POD; another worker should reclaim the run after the idle timeout"

echo ">> waiting for the run to reach a terminal state"
for _ in $(seq 1 120); do
  st=$(curl -s "$B/runs/$RUN" | jq -r .status)
  case "$st" in
    succeeded) echo ">> PASS: run $RUN completed (status=$st) despite the worker kill"; exit 0;;
    failed|timed_out|canceled) echo ">> FAIL: run ended as $st"; exit 1;;
  esac
  sleep 2
done
echo ">> FAIL: run did not finish in time"; exit 1
