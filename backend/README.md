# Job Search Automation (Innsbruck + Remote)

Semi-automated pipeline for discovering and ranking software engineering jobs.

## Goal
Find roles that are either:
1. Onsite/hybrid in **Innsbruck/Tyrol/Austria**, or
2. **Fully remote** and compatible with working from Innsbruck (CET/Europe)

## What this does
- Fetches jobs from RSS/API sources
- Normalizes postings
- Scores relevance using your profile
- Produces a shortlist report (`output/latest_report.md`)
- Stores raw/normalized data (`data/`)

## Architecture (v1.3 refactor)
- Entrypoint: `scripts/run_pipeline.py`
- Orchestrator: `job_search/pipeline.py`
- Source ingestion/parsing: `job_search/ingestion.py`
- LLM parse+score engine + cache: `job_search/llm_parsing.py`
- Cover-letter draft generation: `job_search/cover_letter.py`
- Weekly digest builder: `job_search/ops_digest.py`
- Report rendering: `job_search/reporting.py`
- Run metadata persistence: `job_search/run_metadata.py`
- Models: `job_search/models.py`
- DB migration/persistence helpers: `job_search/storage/db.py`
- DB repository layer: `job_search/storage/repository.py`
- API server: `job_search/api_server.py` + `scripts/serve_api.py`

## Run
```bash
cd ~/job_search/backend
python3 scripts/run_pipeline.py
```
Source fetching uses retry settings from `config/runtime.json`.
Source health/circuit-breaker behavior is configured in `config/runtime.json` (`source_health`).

## Mark a job as applied
```bash
cd ~/job_search/backend
python3 scripts/mark_applied.py "<job_url>" "<job_title>" "<optional_company>"
```
Applied jobs are tracked in `data/applied_jobs.json` and automatically skipped in future scans.

## Tests
```bash
cd ~/job_search/backend
python3 -m unittest discover -s tests -v
```
Includes pipeline integration tests with mocked LLM evaluation (`tests/test_pipeline_compat.py`).

## Database migrations (scaffold)
```bash
cd ~/job_search/backend
python3 scripts/migrate_db.py
```
By default this creates/updates `data/job_search.sqlite` using SQL files in `db/migrations/`.
DB metadata writes are controlled by `config/database.json`.

## View Run History
```bash
cd ~/job_search/backend
python3 scripts/show_run_history.py --limit 10
```
Shows recent pipeline runs plus per-source fetch attempt metrics from SQLite.

## View Source Health
```bash
cd ~/job_search/backend
python3 scripts/show_source_health.py --window-runs 12 --stale-after-hours 72
```
Displays source success rates, health score, and stale status.

## Weekly Ops Loop
```bash
cd ~/job_search/backend
python3 scripts/run_weekly_ops.py
```
Runs the pipeline and writes a digest to `output/weekly_digest.md` with:
- New jobs vs previous run
- Priority jobs (LLM-ranked)
- Funnel and follow-up metrics
- Source health alerts

## Serve JSON API
```bash
cd ~/job_search/backend
python3 scripts/serve_api.py --host 127.0.0.1 --port 8787
```
The API supports prefixed routes (`/api/...`) for the new frontend and keeps root aliases for most legacy endpoints.
Run-control endpoints are API-prefixed only.
Available endpoints: `/health`, `/api/runs`, `/api/runs/active`, `/api/runs/start`, `/api/runs/<run_id>`, `/api/runs/<run_id>/sources`, `/api/jobs`, `/api/applications`, `/api/applications/metrics`, `/api/applications/followups`, `/api/applications/workspace`, `/api/feedback`, `/api/cover-letters`, `/api/sources/health`, `/api/metrics`.
Write endpoints: `POST /applications` (status + follow-up updates), `POST /applications/bulk` (batch status updates), `POST /applications/followup`, `POST /feedback`, `POST /cover-letters/generate`.

`/jobs` supports filters and paging:
- `run_id`, `tier`, `q`, `company`, `source`, `source_type`, `location`
- `remote=true|false`, `min_score`, `max_score`, `application_status`
- `sort=score_desc|score_asc|newest|oldest|company|title`
- `limit`, `offset`, `include_diagnostics=true`

Job payloads include LLM-generated scoring rationale (`reasons`, `llm_summary`) and quality diagnostics
(`quality_flags`, `parse_confidence`, `scored_by`).

`/dashboard` is an enhanced shortlist UI for:
- deep-linkable filters and selected job
- saved views (local browser storage)
- diagnostic tags and rich detail panel
- quick status chips and follow-up presets
- bulk status updates for selected jobs
- funnel and follow-up KPIs

`/workspace` is an application-centric UI for:
- deep-linkable application selection and filters
- richer timeline + cover-letter history context
- quick status chips + follow-up presets
- generating/regenerating cover-letter drafts

`/board` is a kanban-style application board for:
- status columns (`saved`, `applied`, `interview`, `offer`, `rejected`)
- one-click card moves between statuses
- multi-select bulk status updates

## Auth + Multi-User API
- Configure keys in `config/auth.json`.
- Generate keys:
```bash
cd ~/job_search/backend
python3 scripts/seed_auth_keys.py --user user-a --user user-b --replace-user-keys
```
- When enabled, pass API key using one of:
  - `Authorization: Bearer <token>`
  - `X-API-Key: <token>`
  - `?api_key=<token>` query parameter
- Application/feedback/cover-letter data is partitioned by `user_id` mapped from the key.
- `scripts/serve_api.py` validates `config/auth.json` before starting and exits on invalid config.

## Backfill Existing JSON Data Into DB
```bash
cd ~/job_search/backend
python3 scripts/backfill_db.py
```
This imports current `data/` + `output/` artifacts into SQLite for the transition to DB-first runtime persistence.

## Outputs
- `output/latest_report.md`
- `output/latest_report.json`
- `output/weekly_digest.md`
- `data/jobs_normalized.json`
- `data/last_errors.json`
- `data/llm_parse_cache.json` (memoized LLM parse+score results)
- `data/pipeline_runs.jsonl` (append-only run metadata log)
- `data/source_health.json` (if source health is enabled)
- `data/metrics.jsonl` (structured metrics events)
- `data/alerts.jsonl` (alert events including pipeline failures)
- `data/runtime_metrics.json` (latest snapshot)

## Docker Deploy
```bash
cd ~/job_search/backend
docker compose up --build -d
```
Services:
- `api`: JSON API + dashboard/workspace/board UI
- `ops`: scheduler loop for periodic pipeline runs + weekly digest generation

## Operational Utilities
```bash
cd ~/job_search/backend
python3 scripts/check_failed_runs.py --limit 5
python3 scripts/show_source_health.py --window-runs 12 --stale-after-hours 72
```

## Filtering behavior (prototype)
- Pipeline skips jobs already marked as applied.
- Final ranking and invalid-item filtering are handled by the LLM parse+score stage.

## Sources
- RSS: RemoteOK, WeWorkRemotely, Jobicy
- HTML: Karriere.at (Innsbruck/Tirol/Austria)
- Browser-rendered (optional): StepStone (Innsbruck query)
- Company watchlist feeds (optional): Greenhouse + Lever (`greenhouse_sources`, `lever_sources` in `config/sources.json`)

## StepStone source strategy
StepStone now uses a generic backend strategy per source (`fetch_strategy` in `config/sources.json`):
- listing backends: `http`, `curl_cffi`, `playwright_cli`, `openclaw_snapshot`
- detail backends: `curl_cffi`, `playwright_cli`, `http`, `openclaw_snapshot`

This means:
- fast path: parse listing data from StepStone preloaded JSON in raw HTML
- rich path: enrich with detail pages via JSON-LD `JobPosting` when available
- fallback path: use Playwright/OpenClaw only if lighter backends fail

To enable the Playwright backend:
```bash
cd ~/job_search/backend
npm install
```

## LLM Parse+Score (gpt-5-mini)
- Configure in `config/scoring.json`.
- Pipeline uses one LLM call per candidate job to parse fields and score fit.
- Results are memoized in `data/llm_parse_cache.json` and reused on later runs.
- Cache key includes job content + model + prompt version.

## Next improvements
- Add ATS export adapters (Notion/Sheets/Airtable sync)
- Add interview prep briefs (company + role specific)
- Add per-source anomaly detection alerts (job volume spikes/drops)
