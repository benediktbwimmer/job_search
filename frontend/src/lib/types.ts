export type Tier = 'A' | 'B' | 'C'

export type RunRecord = {
  run_id: string
  started_at: string
  ended_at: string
  status: string
  duration_ms: number
  total_jobs: number
  a_tier: number
  b_tier: number
  c_tier: number
  skipped_applied: number
  llm_enabled: number
  llm_model: string | null
  llm_scored_live: number
  llm_cache_hits: number
  llm_failed: number
  source_errors: number
  error_message?: string | null
  summary?: Record<string, unknown>
}

export type JobItem = {
  run_id?: string
  job_id?: string
  score: number
  tier: Tier
  rule_score?: number | null
  scored_by?: string
  source?: string
  source_type?: string
  title?: string
  company?: string
  location?: string
  url: string
  description?: string
  published?: string
  fetched_at?: string
  remote_hint?: number
  application_status?: string | null
  reasons?: string[]
  llm_summary?: string
  salary?: {
    annual_min_eur?: number
    annual_max_eur?: number
  }
  cv_variant?: string
  diagnostics?: {
    base_score?: number
    adaptive_bonus?: number
    adaptive_reasons?: string[]
    final_score?: number
    rule_score?: number | null
  }
}

export type ApplicationItem = {
  user_id: string
  job_url: string
  title: string
  company: string
  status: string
  applied_at?: string | null
  notes?: string | null
  next_action_at?: string | null
  next_action_type?: string | null
}

export type FeedbackItem = {
  id?: number
  user_id?: string
  job_url: string
  action: string
  value?: string
  source?: string
  created_at?: string
}

export type CoverLetterItem = {
  id?: number
  user_id?: string
  job_url: string
  job_id?: string
  run_id?: string
  cv_variant: string
  language: string
  style: string
  company: string
  title: string
  body: string
  generated_at: string
  version?: number
}

export type SourceEvent = {
  source_name: string
  source_kind: string
  source_type: string
  source_url: string
  attempts: number
  success: number
  jobs_fetched: number
  duration_ms: number
  error_message?: string | null
  created_at?: string
}

export type ActiveRun = {
  running: boolean
  status: string
  run_id: string
  started_at: string
  ended_at: string
  elapsed_seconds: number
  pid: number | null
  progress: {
    processed: number
    total: number
    live: number
    cache: number
    failed: number
    filtered: number
  }
  logs: string[]
  exit_code: number | null
}
