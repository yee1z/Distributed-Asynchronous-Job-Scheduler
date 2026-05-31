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
  scheduler/    排程派發（規劃中）
  worker/       執行引擎（規劃中）
deploy/k8s/     k3s 部署 manifests（規劃中）
docs/           文件
Dockerfile          multi-stage build，依賴 baked-in、non-root
docker-compose.yml  本機完整 stack（含 Postgres / Redis）
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
docker build -t job-scheduler:$VER .
docker save job-scheduler:$VER | sudo k3s ctr images import -
```

manifests 以 `image: job-scheduler:0.1.0` + `imagePullPolicy: Never` 引用。
詳見 [docs/PROGRESS.md](docs/PROGRESS.md) 階段 D / E。
