CREATE TABLE IF NOT EXISTS feedback_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  job_url TEXT NOT NULL,
  action TEXT NOT NULL,
  value TEXT,
  source TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feedback_events_user_created
  ON feedback_events(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_feedback_events_action
  ON feedback_events(action);

CREATE INDEX IF NOT EXISTS idx_feedback_events_job
  ON feedback_events(job_url);
