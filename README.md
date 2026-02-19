# Job Search Monorepo

This repository is now split into:

- `backend/`: Python pipeline, storage, API server, operational scripts, tests, config/data/output.
- `frontend/`: React + TypeScript + Tailwind SPA (Dashboard, Workspace, Board, Runs).

## Quick Start

### 1) Backend

```bash
cd /Users/bene/job_search/backend
python3 scripts/serve_api.py --host 127.0.0.1 --port 8787
```

### 2) Frontend (dev)

```bash
cd /Users/bene/job_search/frontend
npm install
npm run dev
```

Vite runs on `http://127.0.0.1:5173` and proxies API calls to `http://127.0.0.1:8787` via `/api/*`.

## Production-like local run

Build frontend and let backend serve the static SPA:

```bash
cd /Users/bene/job_search/frontend
npm install
npm run build

cd /Users/bene/job_search/backend
python3 scripts/serve_api.py --host 127.0.0.1 --port 8787 --frontend-dist ../frontend/dist
```

## Backend pipeline

```bash
cd /Users/bene/job_search/backend
python3 scripts/run_pipeline.py
```

## Backend tests

```bash
cd /Users/bene/job_search
PYTHONPATH=backend pytest -q backend/tests
```

See full backend documentation in `/Users/bene/job_search/backend/README.md`.
