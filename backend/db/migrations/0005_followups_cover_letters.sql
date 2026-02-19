ALTER TABLE applications ADD COLUMN next_action_at TEXT;
ALTER TABLE applications ADD COLUMN next_action_type TEXT;

CREATE TABLE IF NOT EXISTS cover_letters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  job_url TEXT NOT NULL,
  job_id TEXT,
  run_id TEXT,
  cv_variant TEXT,
  language TEXT,
  style TEXT,
  company TEXT,
  title TEXT,
  body TEXT NOT NULL,
  generated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_cover_letters_user_generated
  ON cover_letters(user_id, generated_at DESC);

CREATE INDEX IF NOT EXISTS idx_cover_letters_job_url
  ON cover_letters(job_url);
