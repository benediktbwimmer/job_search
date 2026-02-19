CREATE UNIQUE INDEX IF NOT EXISTS idx_job_rankings_run_job
  ON job_rankings(run_id, job_id);

CREATE INDEX IF NOT EXISTS idx_jobs_url
  ON jobs(url);

CREATE INDEX IF NOT EXISTS idx_jobs_source_type
  ON jobs(source_type);

CREATE INDEX IF NOT EXISTS idx_applications_user_status
  ON applications(user_id, status);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
  ON pipeline_runs(started_at);
