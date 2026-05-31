#!/usr/bin/env bash
# Compose variant of the failover test (used by CI; no k8s needed).
# Starts a long run, kills one worker container mid-execution, and asserts the
# run is reclaimed by the other worker (XAUTOCLAIM) and still completes.
#
# Assumes the full stack is already up:  docker compose --profile full up -d
set -euo pipefail

B="${API_BASE:-http://localhost:8000}/api/v1"

echo ">> creating long-running shell job"
JOB=$(curl -s -X POST "$B/jobs" -H 'Content-Type: application/json' \
  -d "{\"name\":\"failover-$(date +%s)\",\"task_type\":\"shell\",\"timeout_sec\":300,\"task_spec\":{\"command\":\"sh\",\"args\":[\"-c\",\"echo start; sleep 12; echo done\"]}}" \
  | jq -r .id)
RUN=$(curl -s -X POST "$B/jobs/$JOB/trigger" | jq -r .id)
echo ">> job=$JOB run=$RUN"

st=""
for _ in $(seq 1 30); do
  st=$(curl -s "$B/runs/$RUN" | jq -r .status)
  [ "$st" = "running" ] && break
  sleep 1
done
echo ">> status before kill: $st"

CID=$(docker compose --profile full ps -q worker | head -1)
echo ">> killing worker container $CID"
docker kill "$CID"

for _ in $(seq 1 90); do
  st=$(curl -s "$B/runs/$RUN" | jq -r .status)
  case "$st" in
    succeeded) echo ">> PASS: run reclaimed and completed despite worker kill"; exit 0;;
    failed|timed_out|canceled) echo ">> FAIL: run ended as $st"; exit 1;;
  esac
  sleep 2
done
echo ">> FAIL: run did not finish in time"; exit 1
