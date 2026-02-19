import { FormEvent, useEffect, useMemo, useState } from 'react'
import { apiGet, apiPost, fmtSalary } from '../lib/api'
import type { ApplicationItem, CoverLetterItem, FeedbackItem, JobItem } from '../lib/types'

type MetricsPayload = {
  metrics?: {
    total_applications?: number
    funnel?: { applied_or_beyond?: number; interview_or_beyond?: number; offers?: number }
    followups?: { due_today?: number; overdue?: number }
  }
}

type JobsPayload = { run_id?: string; jobs: JobItem[]; total?: number }

type WorkspacePayload = {
  workspace: {
    application?: ApplicationItem | null
    feedback?: FeedbackItem[]
    cover_letters?: CoverLetterItem[]
  } | null
}

export function DashboardPage() {
  const [jobs, setJobs] = useState<JobItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [selectedUrl, setSelectedUrl] = useState('')
  const [selectedSet, setSelectedSet] = useState<Set<string>>(new Set())
  const [metrics, setMetrics] = useState<MetricsPayload['metrics']>({})
  const [workspace, setWorkspace] = useState<WorkspacePayload['workspace'] | null>(null)
  const [coverStyle, setCoverStyle] = useState('concise')
  const [coverVariant, setCoverVariant] = useState('en_short')
  const [coverContext, setCoverContext] = useState('')

  const [q, setQ] = useState('')
  const [tier, setTier] = useState('')
  const [remote, setRemote] = useState('')
  const [minScore, setMinScore] = useState('')
  const [applicationStatus, setApplicationStatus] = useState('')
  const [sort, setSort] = useState('score_desc')

  const selectedJob = useMemo(() => jobs.find((j) => j.url === selectedUrl) || null, [jobs, selectedUrl])

  async function loadMetrics() {
    const payload = await apiGet<MetricsPayload>('/applications/metrics?days=30')
    setMetrics(payload.metrics || {})
  }

  function jobQueryString(): string {
    const params = new URLSearchParams({ limit: '80', include_diagnostics: 'true', sort })
    if (q.trim()) params.set('q', q.trim())
    if (tier) params.set('tier', tier)
    if (remote) params.set('remote', remote)
    if (minScore) params.set('min_score', minScore)
    if (applicationStatus) params.set('application_status', applicationStatus)
    return params.toString()
  }

  async function loadJobs() {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet<JobsPayload>(`/jobs?${jobQueryString()}`)
      const incoming = payload.jobs || []
      setJobs(incoming)
      setSelectedSet(new Set())
      if (incoming.length > 0) {
        if (!incoming.some((j) => j.url === selectedUrl)) setSelectedUrl(incoming[0].url)
      } else {
        setSelectedUrl('')
      }
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function loadWorkspace(jobUrl: string) {
    if (!jobUrl) {
      setWorkspace(null)
      return
    }
    try {
      const payload = await apiGet<WorkspacePayload>(`/applications/workspace?job_url=${encodeURIComponent(jobUrl)}`)
      setWorkspace(payload.workspace)
      const latest = payload.workspace?.cover_letters?.[0]
      if (latest) {
        setCoverStyle(String(latest.style || 'concise'))
        setCoverVariant(String(latest.cv_variant || 'en_short'))
      }
    } catch {
      setWorkspace(null)
    }
  }

  useEffect(() => {
    void Promise.all([loadMetrics(), loadJobs()])
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (selectedUrl) void loadWorkspace(selectedUrl)
  }, [selectedUrl])

  async function applyFilters(e?: FormEvent) {
    e?.preventDefault()
    await loadJobs()
  }

  function toggleSelection(url: string) {
    const next = new Set(selectedSet)
    if (next.has(url)) next.delete(url)
    else next.add(url)
    setSelectedSet(next)
  }

  async function updateStatus(status: string, urls: string[]) {
    if (!urls.length) return
    setLoading(true)
    setError('')
    try {
      if (urls.length === 1) {
        const job = jobs.find((j) => j.url === urls[0])
        await apiPost('/applications', {
          job_url: urls[0],
          status,
          title: job?.title || '',
          company: job?.company || '',
        })
      } else {
        await apiPost('/applications/bulk', {
          items: urls.map((url) => {
            const job = jobs.find((j) => j.url === url)
            return {
              job_url: url,
              status,
              title: job?.title || '',
              company: job?.company || '',
            }
          }),
        })
      }
      await Promise.all([loadMetrics(), loadJobs()])
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function setFollowup(hours: number, type: string) {
    if (!selectedJob?.url) return
    const due = new Date(Date.now() + hours * 3600 * 1000).toISOString()
    setLoading(true)
    setError('')
    try {
      await apiPost('/applications/followup', {
        job_url: selectedJob.url,
        next_action_type: type,
        next_action_at: due,
      })
      await loadWorkspace(selectedJob.url)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function generateCover() {
    if (!selectedJob?.url) return
    setLoading(true)
    setError('')
    try {
      await apiPost('/cover-letters/generate', {
        job_url: selectedJob.url,
        cv_variant: coverVariant,
        style: coverStyle,
        additional_context: coverContext,
        regenerate: true,
      })
      await loadWorkspace(selectedJob.url)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const latestCover = workspace?.cover_letters?.[0]

  return (
    <section className="grid gap-3 lg:grid-cols-[1.1fr_0.9fr]">
      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel lg:col-span-2">
        <h1 className="text-xl font-semibold">Job Search Dashboard</h1>
        <p className="text-sm text-muted">Shortlist, filter, score, and take action fast.</p>

        <div className="mt-2 grid grid-cols-2 gap-2 text-sm md:grid-cols-6">
          <Kpi label="Tracked" value={metrics?.total_applications || 0} />
          <Kpi label="Applied+" value={metrics?.funnel?.applied_or_beyond || 0} />
          <Kpi label="Interview+" value={metrics?.funnel?.interview_or_beyond || 0} />
          <Kpi label="Offers" value={metrics?.funnel?.offers || 0} />
          <Kpi label="Due" value={metrics?.followups?.due_today || 0} />
          <Kpi label="Overdue" value={metrics?.followups?.overdue || 0} />
        </div>

        <form onSubmit={applyFilters} className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-7">
          <input className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" placeholder="search" value={q} onChange={(e) => setQ(e.target.value)} />
          <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={tier} onChange={(e) => setTier(e.target.value)}>
            <option value="">Any tier</option>
            <option value="A">A</option>
            <option value="B">B</option>
            <option value="C">C</option>
          </select>
          <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={remote} onChange={(e) => setRemote(e.target.value)}>
            <option value="">Remote + onsite</option>
            <option value="true">Remote</option>
            <option value="false">Onsite</option>
          </select>
          <input className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" placeholder="Min score" value={minScore} onChange={(e) => setMinScore(e.target.value)} />
          <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={applicationStatus} onChange={(e) => setApplicationStatus(e.target.value)}>
            <option value="">Any status</option>
            <option value="saved">saved</option>
            <option value="applied">applied</option>
            <option value="interview">interview</option>
            <option value="offer">offer</option>
            <option value="rejected">rejected</option>
          </select>
          <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={sort} onChange={(e) => setSort(e.target.value)}>
            <option value="score_desc">Best score</option>
            <option value="newest">Newest</option>
            <option value="company">Company</option>
            <option value="title">Title</option>
          </select>
          <button className="rounded-md border border-accent bg-accent px-3 py-2 text-sm font-medium text-white">Apply</button>
        </form>

        {error ? <p className="mt-2 text-sm text-red-700">{error}</p> : null}
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-semibold">Jobs</h2>
          <span className="text-xs text-muted">{loading ? 'loading...' : `${jobs.length} rows`}</span>
        </div>
        <div className="mb-2 flex flex-wrap gap-1">
          <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void updateStatus('saved', [...selectedSet])}>Bulk saved</button>
          <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void updateStatus('applied', [...selectedSet])}>Bulk applied</button>
          <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void updateStatus('interview', [...selectedSet])}>Bulk interview</button>
          <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => setSelectedSet(new Set())}>Clear ({selectedSet.size})</button>
        </div>

        <div className="max-h-[68vh] space-y-2 overflow-auto pr-1">
          {jobs.map((job) => {
            const active = job.url === selectedUrl
            const salary = fmtSalary(job.salary?.annual_min_eur, job.salary?.annual_max_eur)
            const jobDateLabel = formatJobRecency(job.published || job.fetched_at)
            return (
              <div key={job.url} className={`rounded-lg border bg-white p-2 ${active ? 'border-accent' : 'border-black/10'}`}>
                <div className="flex items-start gap-2">
                  <input type="checkbox" checked={selectedSet.has(job.url)} onChange={() => toggleSelection(job.url)} className="mt-1" />
                  <button className="flex-1 text-left" onClick={() => setSelectedUrl(job.url)}>
                    <div className="flex items-start justify-between gap-2">
                      <div className="text-sm font-medium">{job.title || 'Untitled role'}</div>
                      {jobDateLabel ? <span className="shrink-0 rounded-full border border-black/10 px-2 py-0.5 text-[11px] text-muted">{jobDateLabel}</span> : null}
                    </div>
                    <div className="text-xs text-muted">{job.company || 'Unknown'} | score {job.score} | tier {job.tier}</div>
                    <div className="mt-1 flex flex-wrap gap-1">
                      {job.cv_variant ? <span className="rounded-full border border-black/10 px-2 py-0.5 text-[11px]">{job.cv_variant}</span> : null}
                      {salary ? <span className="rounded-full border border-black/10 px-2 py-0.5 text-[11px]">{salary}</span> : null}
                      {job.application_status ? (
                        <span className="rounded-full border border-black/10 px-2 py-0.5 text-[11px]">app:{job.application_status}</span>
                      ) : null}
                    </div>
                  </button>
                </div>
              </div>
            )
          })}
          {!jobs.length ? <p className="text-sm text-muted">No jobs for this filter.</p> : null}
        </div>
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        {!selectedJob ? (
          <p className="text-sm text-muted">Select a job for detail actions.</p>
        ) : (
          <>
            <h2 className="text-lg font-semibold">{selectedJob.title || 'Untitled role'}</h2>
            <p className="text-sm text-muted">{selectedJob.company || 'Unknown'} | {selectedJob.location || 'n/a'}</p>
            <div className="mt-2 flex flex-wrap gap-1">
              {['saved', 'applied', 'interview', 'offer', 'rejected'].map((status) => (
                <button
                  key={status}
                  className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs"
                  onClick={() => void updateStatus(status, [selectedJob.url])}
                >
                  {status}
                </button>
              ))}
            </div>

            <div className="mt-3 flex flex-wrap gap-1">
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(24, 'follow_up_email')}>Follow-up 1d</button>
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(72, 'follow_up_email')}>Follow-up 3d</button>
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(168, 'interview_prep')}>Prep 7d</button>
            </div>

            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              <input className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" placeholder="cv variant" value={coverVariant} onChange={(e) => setCoverVariant(e.target.value)} />
              <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={coverStyle} onChange={(e) => setCoverStyle(e.target.value)}>
                <option value="concise">concise</option>
                <option value="detailed">detailed</option>
              </select>
            </div>
            <textarea
              className="mt-2 min-h-20 w-full rounded-md border border-black/10 bg-white px-2 py-2 text-sm"
              placeholder="additional context for this draft"
              value={coverContext}
              onChange={(e) => setCoverContext(e.target.value)}
            />
            <div className="mt-2 flex flex-wrap gap-2">
              <button className="rounded-md border border-accent bg-accent px-3 py-1.5 text-sm text-white" onClick={() => void generateCover()}>
                Generate Draft (gpt-5.2)
              </button>
              <a className="rounded-md border border-black/10 bg-white px-3 py-1.5 text-sm" href={selectedJob.url} target="_blank" rel="noreferrer">
                Open Job
              </a>
            </div>

            <section className="mt-3 space-y-2">
              <h3 className="text-sm font-semibold">Why matched</h3>
              <ul className="list-disc space-y-1 pl-4 text-sm text-ink/90">
                {(selectedJob.reasons || []).slice(0, 8).map((reason) => (
                  <li key={reason}>{reason}</li>
                ))}
                {!selectedJob.reasons?.length ? <li>(none)</li> : null}
              </ul>
            </section>

            <section className="mt-3">
              <h3 className="text-sm font-semibold">Latest Cover Draft</h3>
              <pre className="mt-1 max-h-64 overflow-auto rounded-md border border-black/10 bg-slate-50 p-2 text-xs whitespace-pre-wrap">
                {latestCover?.body || 'No draft generated yet.'}
              </pre>
            </section>

            <section className="mt-3">
              <h3 className="text-sm font-semibold">Description</h3>
              <p className="mt-1 max-h-60 overflow-auto rounded-md border border-black/10 bg-slate-50 p-2 text-xs whitespace-pre-wrap">
                {selectedJob.description || '(no description)'}
              </p>
            </section>
          </>
        )}
      </article>
    </section>
  )
}

function Kpi({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-black/10 bg-white p-2">
      <div className="text-lg font-semibold leading-tight">{value}</div>
      <div className="text-[11px] uppercase tracking-wide text-muted">{label}</div>
    </div>
  )
}

function formatJobRecency(value?: string | null): string {
  if (!value) return ''
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return ''
  const now = new Date()
  const dayMs = 24 * 60 * 60 * 1000
  const diffDays = Math.floor((now.getTime() - dt.getTime()) / dayMs)
  if (diffDays <= 0) return 'today'
  if (diffDays === 1) return '1d ago'
  if (diffDays < 14) return `${diffDays}d ago`
  const sameYear = dt.getFullYear() === now.getFullYear()
  return dt.toLocaleDateString(undefined, sameYear ? { month: 'short', day: 'numeric' } : { year: 'numeric', month: 'short', day: 'numeric' })
}
