# Distributed Asynchronous Job Scheduler

統一、高可用、可水平擴展的任務排程平台：**API + Scheduler + Worker + PostgreSQL + Redis(Streams)**。

- 技術棧：Python 3.12 / FastAPI / SQLAlchemy 2.0 / PostgreSQL 16 / Redis 7
- 進度與規劃：見 [docs/PROGRESS.md](docs/PROGRESS.md)
- 前端串接文件：見 [docs/API.md](docs/API.md)

## 專案結構

```
backend/        後端應用程式碼
  common/       共用：models / schemas / db / redis / config / metrics / logging
  api/          FastAPI REST API（jobs / runs / health）
  scheduler/    排程派發：cron/interval 計算、advisory-lock 選主、延遲重試推進
  worker/       執行引擎：consumer group 消費、failover、idempotent 執行、executors
deploy/k8s/     k3s 部署 manifests + deploy-staged.sh（分階段 rollout）
migrations/     Alembic 資料庫遷移
docs/           文件
Dockerfile          multi-stage build，依賴 baked-in、non-root
docker-compose.yml  本機完整 stack（含 Postgres / Redis / migrate）
```

## 本機開發（Docker Compose）

```bash
# 啟動 Postgres + Redis + API
docker compose up --build

# 含 scheduler / worker 的完整 pipeline（stages A/B 完成後）
docker compose --profile full up --build
```

API 預設在 http://localhost:8000 （Swagger UI 於 `/docs`）。

## 部署到 k3s

k3s 使用 containerd，需把 docker build 的 image 匯入：

```bash
VER=0.1.0
PLATFORM=linux/amd64
docker buildx build --load --platform "$PLATFORM" --provenance=false --sbom=false -t job-scheduler:$VER .
docker save job-scheduler:$VER | sudo k3s ctr images import -
```

> **為何一定要 `--provenance=false --sbom=false` 並指定單一 `--platform`**：buildx 預設會輸出 OCI image index（manifest list），並額外附帶 attestation / provenance manifest（其 platform 為 `unknown/unknown`）。這種「多 manifest」格式匯入 k3s 後，containerd 的 CRI 在建立容器時解不出可執行的單平台 image，會讓所有使用此 image 的 pod 卡在 `Init:CreateContainerError`。關掉 attestation 並鎖定單一平台，輸出乾淨的單一 manifest，容器才能正常建立。

manifests 以 `image: job-scheduler:0.1.0` + `imagePullPolicy: Never` 引用：

```bash
kubectl apply -k deploy/k8s
# 大量 pod 同時建立而卡住時，改用分階段部署：
./deploy/k8s/deploy-staged.sh
```


### Worker 動態自動伸縮

`worker` 由 KEDA `ScaledObject/worker-autoscaler` 依 Redis Stream 的 job backlog 自動伸縮：

- 最少 `2` pods，保留高可用與基本吞吐。
- 最多 `20` pods，可依節點容量調整 `deploy/k8s/10-worker-autoscaling.yaml` 的 `maxReplicaCount`。
- 每 `20` 個 Redis Stream lag / pending entries 約增加 1 個 worker pod；目前 `WORKER_CONCURRENCY=10`，所以 2 pods 約可同時處理 20 個 run。
- scaler 連續失敗時 fallback 到 `2` pods，避免 autoscaler 指標故障造成 worker 被縮掉。
- KEDA operator 跑在 `keda` namespace，Redis address 使用 `redis.job-scheduler.svc.cluster.local:6379`。

第一次使用前先安裝 KEDA CRD/controller：

```bash
kubectl apply --server-side \
  -f https://github.com/kedacore/keda/releases/download/v2.20.0/keda-2.20.0.yaml

kubectl apply -k deploy/k8s
```

若用 `deploy-staged.sh`，腳本會在偵測到 `scaledobjects.keda.sh` CRD 時自動套用 autoscaling；尚未安裝 KEDA 時會略過並提示。

緊急手動調整 replica 時，不要直接長期改 `Deployment/worker`，因為 KEDA/HPA 會接手改回來。請先暫停 autoscaling 並指定 replica：

```bash
kubectl -n job-scheduler annotate scaledobject worker-autoscaler \
  autoscaling.keda.sh/paused-replicas="6" --overwrite

kubectl -n job-scheduler get deploy worker
```

解除緊急模式、恢復自動伸縮：

```bash
kubectl -n job-scheduler annotate scaledobject worker-autoscaler \
  autoscaling.keda.sh/paused-replicas-
```

> **為何需要分階段部署（`deploy-staged.sh`）**：一次套用會讓 7 個 pod（api×2 / scheduler×2 / worker×2 + migrate）同時對 containerd 發出 CreateContainer。在 native snapshotter（逐層複製、較慢）下，容易踩到 containerd 的 container name reservation 競態，出現 `failed to reserve container name ... another CreateContainer request is in progress`，使 pod 持續 `Init:CreateContainerError`。`deploy-staged.sh` 先重啟 k3s 清掉卡住的 reservation，再「先 migrate、後依序 api → scheduler → worker（中間留 gap）」逐步部署，藉此避開這個瞬間的 create storm。

詳見 [docs/PROGRESS.md](docs/PROGRESS.md) 階段 D / E。

## 測試

```bash
pip install -r requirements-dev.txt

# 單元測試（cron / 退避 / schema 驗證，免外部服務）
pytest tests/unit

# 整合測試（需先啟動完整 stack；未啟動時自動 skip）
docker compose --profile full up --build      # 另一個終端機
pytest tests/integration -m integration

# 韌性測試：殺掉 worker pod，驗證 run 仍由其他 worker 接手完成（需 k8s）
API_BASE=http://<node-ip> ./tests/resilience/worker_failover.sh
```
