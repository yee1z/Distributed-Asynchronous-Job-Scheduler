#!/usr/bin/env bash
# Staged k3s rollout: restart containerd if stuck, migrate first, then app tiers.
# Requires sudo for k3s restart and kubectl (kubeconfig at /etc/rancher/k3s/k3s.yaml).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS=job-scheduler
KUBECTL="${KUBECTL:-sudo k3s kubectl}"

restart_k3s() {
  echo "==> Restarting k3s (clears stuck containerd name reservations)..."
  sudo systemctl restart k3s
  echo "==> Waiting for node Ready..."
  for _ in $(seq 1 60); do
    if $KUBECTL get node art2 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | grep -q True; then
      echo "    node art2 Ready"
      return 0
    fi
    sleep 2
  done
  echo "ERROR: node art2 not Ready after 120s" >&2
  exit 1
}

reimport_image() {
  local ver="${IMAGE_TAG:-0.1.0}"
  local platform="${IMAGE_PLATFORM:-linux/amd64}"
  local repo_root="$(cd "$ROOT/../.." && pwd)"
  echo "==> Re-importing job-scheduler:$ver into k3s containerd..."
  if [[ "${REBUILD_IMAGE:-1}" == "1" ]] || ! docker image inspect "job-scheduler:$ver" &>/dev/null; then
    echo "    building single-platform image for $platform..."
    docker buildx build \
      --load \
      --platform "$platform" \
      --provenance=false \
      --sbom=false \
      -t "job-scheduler:$ver" \
      "$repo_root"
  fi
  docker save "job-scheduler:$ver" | sudo k3s ctr images import -
  sudo k3s ctr images ls | grep job-scheduler || {
    echo "ERROR: job-scheduler image not visible in containerd" >&2
    exit 1
  }
}

dump_migrate_failure() {
  echo "ERROR: migrate job did not complete." >&2
  $KUBECTL -n "$NS" get job migrate -o wide 2>&1 || true
  $KUBECTL -n "$NS" get pods -l app=migrate -o wide 2>&1 || true
  $KUBECTL -n "$NS" describe pod -l app=migrate 2>&1 | tail -20 || true
  cat >&2 <<'EOF'

Likely causes:
  - Init:CreateContainerError  -> containerd stuck; run: RESTART_K3S=1 ./deploy-staged.sh
  - migrate container Failed   -> re-import app image (script does this automatically now)
  - CrashLoopBackOff on alembic -> check logs: sudo k3s kubectl -n job-scheduler logs -l app=migrate -c migrate

Manual migrate via docker (if k8s job keeps failing):
  sudo k3s kubectl -n job-scheduler port-forward svc/postgres 5432:5432 &
  docker run --rm --network host \
    -e DATABASE_URL=postgresql+psycopg://scheduler:scheduler@127.0.0.1:5432/scheduler \
    job-scheduler:0.1.0 alembic upgrade head
EOF
}

wait_migrate() {
  local timeout="${MIGRATE_TIMEOUT:-300s}"
  echo "==> Waiting for migrate job (timeout $timeout)..."
  if $KUBECTL -n "$NS" wait --for=condition=complete job/migrate --timeout="$timeout"; then
    return 0
  fi
  dump_migrate_failure
  exit 1
}

if [[ "${RESTART_K3S:-1}" == "1" ]]; then
  restart_k3s
fi

echo "==> Scaling app tiers to 0..."
$KUBECTL -n "$NS" scale deployment api scheduler worker --replicas=0 2>/dev/null || true
$KUBECTL -n "$NS" delete job migrate --ignore-not-found

echo "==> Base stack (namespace/config/postgres/redis)..."
$KUBECTL apply -f "$ROOT/00-namespace.yaml" -f "$ROOT/01-config.yaml" \
  -f "$ROOT/02-postgres.yaml" -f "$ROOT/03-redis.yaml"

echo "==> Waiting for postgres + redis..."
$KUBECTL -n "$NS" rollout status deployment/postgres --timeout=300s
$KUBECTL -n "$NS" rollout status deployment/redis --timeout=300s

if [[ "${REIMPORT_IMAGE:-1}" == "1" ]]; then
  reimport_image
fi

echo "==> Migrate (single pod, avoids containerd create storm)..."
$KUBECTL apply -f "$ROOT/04-migrate-job.yaml"
wait_migrate

echo "==> App tiers (sequential apply, 15s gap)..."
$KUBECTL apply -f "$ROOT/05-api.yaml"
sleep 15
$KUBECTL apply -f "$ROOT/06-scheduler.yaml"
sleep 15
$KUBECTL apply -f "$ROOT/07-worker.yaml"
$KUBECTL apply -f "$ROOT/08-ingress.yaml"

echo "==> Rollout status..."
$KUBECTL -n "$NS" rollout status deployment/api --timeout=300s
$KUBECTL -n "$NS" rollout status deployment/scheduler --timeout=300s
$KUBECTL -n "$NS" rollout status deployment/worker --timeout=300s

echo "==> Done."
$KUBECTL -n "$NS" get pods -o wide
