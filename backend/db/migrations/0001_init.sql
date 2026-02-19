CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  email TEXT UNIQUE,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_profiles (
  user_id TEXT PRIMARY KEY,
  profile_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  url TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  source TEXT,
  source_type TEXT,
  title TEXT,
  company TEXT,
  location TEXT,
  remote_hint INTEGER,
  url TEXT,
  description TEXT,
  published TEXT,
  fetched_at TEXT,
  normalized_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS job_rankings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  job_id TEXT,
  score INTEGER NOT NULL,
  tier TEXT NOT NULL,
  rule_score INTEGER,
  reasons_json TEXT,
  skill_hits_json TEXT,
  llm_summary TEXT,
  llm_pros_json TEXT,
  llm_risks_json TEXT,
  scored_by TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS applications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT,
  job_url TEXT NOT NULL,
  title TEXT,
  company TEXT,
  status TEXT NOT NULL DEFAULT 'applied',
  applied_at TEXT,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_id, job_url)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  ended_at TEXT NOT NULL,
  status TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  total_jobs INTEGER NOT NULL,
  a_tier INTEGER NOT NULL,
  b_tier INTEGER NOT NULL,
  c_tier INTEGER NOT NULL,
  skipped_applied INTEGER NOT NULL,
  llm_enabled INTEGER NOT NULL,
  llm_model TEXT,
  llm_scored_live INTEGER NOT NULL,
  llm_cache_hits INTEGER NOT NULL,
  llm_failed INTEGER NOT NULL,
  source_errors INTEGER NOT NULL,
  error_message TEXT,
  summary_json TEXT NOT NULL
);
