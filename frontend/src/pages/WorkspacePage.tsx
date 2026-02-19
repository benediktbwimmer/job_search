import { useEffect, useMemo, useState } from 'react'
import { apiGet, apiPost, fmtDate } from '../lib/api'
import type { ApplicationItem, CoverLetterItem, FeedbackItem, JobItem } from '../lib/types'

type ApplicationsPayload = { applications: ApplicationItem[] }
type WorkspacePayload = {
  workspace: {
    application?: ApplicationItem | null
    job?: JobItem | null
    feedback?: FeedbackItem[]
    cover_letters?: CoverLetterItem[]
  } | null
}

export function WorkspacePage() {
  const [apps, setApps] = useState<ApplicationItem[]>([])
  const [selectedJobUrl, setSelectedJobUrl] = useState('')
  const [workspace, setWorkspace] = useState<WorkspacePayload['workspace'] | null>(null)
  const [statusFilter, setStatusFilter] = useState('')
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [coverStyle, setCoverStyle] = useState('concise')
  const [coverVariant, setCoverVariant] = useState('en_short')
  const [coverContext, setCoverContext] = useState('')

  const filteredApps = useMemo(() => {
    const q = search.trim().toLowerCase()
    return apps.filter((app) => {
      if (statusFilter && app.status !== statusFilter) return false
      if (!q) return true
      const hay = `${app.title || ''} ${app.company || ''} ${app.job_url || ''}`.toLowerCase()
      return hay.includes(q)
    })
  }, [apps, search, statusFilter])

  async function loadApplications() {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet<ApplicationsPayload>('/applications?limit=400')
      setApps(payload.applications || [])
      if (!selectedJobUrl && payload.applications?.length) {
        setSelectedJobUrl(payload.applications[0].job_url)
      }
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function loadWorkspace(jobUrl: string) {
    if (!jobUrl) return
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet<WorkspacePayload>(`/applications/workspace?job_url=${encodeURIComponent(jobUrl)}`)
      setWorkspace(payload.workspace)
      const latest = payload.workspace?.cover_letters?.[0]
      if (latest) {
        setCoverStyle(String(latest.style || 'concise'))
        setCoverVariant(String(latest.cv_variant || 'en_short'))
      }
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadApplications()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (selectedJobUrl) void loadWorkspace(selectedJobUrl)
  }, [selectedJobUrl])

  async function updateStatus(status: string) {
    if (!selectedJobUrl) return
    setLoading(true)
    setError('')
    try {
      const app = apps.find((x) => x.job_url === selectedJobUrl)
      await apiPost('/applications', {
        job_url: selectedJobUrl,
        status,
        title: app?.title || workspace?.job?.title || '',
        company: app?.company || workspace?.job?.company || '',
      })
      await Promise.all([loadApplications(), loadWorkspace(selectedJobUrl)])
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function setFollowup(hours: number, type: string) {
    if (!selectedJobUrl) return
    setLoading(true)
    setError('')
    try {
      const due = new Date(Date.now() + hours * 3600 * 1000).toISOString()
      await apiPost('/applications/followup', {
        job_url: selectedJobUrl,
        next_action_type: type,
        next_action_at: due,
      })
      await loadWorkspace(selectedJobUrl)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  async function generateCover() {
    if (!selectedJobUrl) return
    setLoading(true)
    setError('')
    try {
      await apiPost('/cover-letters/generate', {
        job_url: selectedJobUrl,
        cv_variant: coverVariant,
        style: coverStyle,
        additional_context: coverContext,
        regenerate: true,
      })
      await loadWorkspace(selectedJobUrl)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  const latestCover = workspace?.cover_letters?.[0]

  return (
    <section className="grid gap-3 lg:grid-cols-[0.9fr_1.1fr]">
      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        <div className="mb-2 flex items-center justify-between">
          <h1 className="text-lg font-semibold">Application Workspace</h1>
          <span className="text-xs text-muted">{loading ? 'loading...' : `${filteredApps.length} items`}</span>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <input className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" placeholder="filter title/company" value={search} onChange={(e) => setSearch(e.target.value)} />
          <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="">Any status</option>
            <option value="saved">saved</option>
            <option value="applied">applied</option>
            <option value="interview">interview</option>
            <option value="offer">offer</option>
            <option value="rejected">rejected</option>
          </select>
        </div>

        {error ? <p className="mt-2 text-sm text-red-700">{error}</p> : null}

        <div className="mt-3 max-h-[72vh] space-y-2 overflow-auto pr-1">
          {filteredApps.map((app) => (
            <button
              key={app.job_url}
              className={`w-full rounded-md border px-2 py-2 text-left ${selectedJobUrl === app.job_url ? 'border-accent bg-slate-50' : 'border-black/10 bg-white'}`}
              onClick={() => setSelectedJobUrl(app.job_url)}
            >
              <div className="text-sm font-medium">{app.title || app.job_url}</div>
              <div className="text-xs text-muted">{app.company || 'Unknown'} | {app.status || 'n/a'}</div>
              <div className="text-[11px] text-muted">{app.next_action_at ? `next ${fmtDate(app.next_action_at)}` : 'no follow-up set'}</div>
            </button>
          ))}
          {!filteredApps.length ? <p className="text-sm text-muted">No applications for this filter.</p> : null}
        </div>
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        {!selectedJobUrl ? (
          <p className="text-sm text-muted">Select an application to inspect details.</p>
        ) : (
          <>
            <h2 className="text-lg font-semibold">{workspace?.job?.title || workspace?.application?.title || 'Selected application'}</h2>
            <p className="text-sm text-muted">{workspace?.job?.company || workspace?.application?.company || 'Unknown'} | {workspace?.application?.status || 'n/a'}</p>

            <div className="mt-2 flex flex-wrap gap-1">
              {['saved', 'applied', 'interview', 'offer', 'rejected'].map((status) => (
                <button key={status} className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void updateStatus(status)}>
                  {status}
                </button>
              ))}
            </div>

            <div className="mt-2 flex flex-wrap gap-1">
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(24, 'follow_up_email')}>Follow-up 1d</button>
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(72, 'follow_up_email')}>Follow-up 3d</button>
              <button className="rounded-full border border-black/10 bg-white px-2 py-1 text-xs" onClick={() => void setFollowup(168, 'interview_prep')}>Prep 7d</button>
            </div>

            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              <input className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={coverVariant} onChange={(e) => setCoverVariant(e.target.value)} placeholder="cv variant" />
              <select className="rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={coverStyle} onChange={(e) => setCoverStyle(e.target.value)}>
                <option value="concise">concise</option>
                <option value="detailed">detailed</option>
              </select>
            </div>
            <textarea className="mt-2 min-h-20 w-full rounded-md border border-black/10 bg-white px-2 py-2 text-sm" value={coverContext} onChange={(e) => setCoverContext(e.target.value)} placeholder="additional context" />
            <button className="mt-2 rounded-md border border-accent bg-accent px-3 py-1.5 text-sm text-white" onClick={() => void generateCover()}>
              Generate Draft (gpt-5.2)
            </button>

            <section className="mt-3">
              <h3 className="text-sm font-semibold">Cover Preview</h3>
              <pre className="mt-1 max-h-64 overflow-auto rounded-md border border-black/10 bg-slate-50 p-2 text-xs whitespace-pre-wrap">
                {latestCover?.body || 'No cover letter generated yet.'}
              </pre>
            </section>

            <section className="mt-3 grid gap-2 md:grid-cols-2">
              <div>
                <h3 className="text-sm font-semibold">Feedback Timeline</h3>
                <ul className="mt-1 max-h-48 list-disc space-y-1 overflow-auto pl-4 text-xs">
                  {(workspace?.feedback || []).slice(0, 12).map((f, idx) => (
                    <li key={`${f.created_at || ''}-${idx}`}>{fmtDate(f.created_at)} | {f.action} {f.value || ''}</li>
                  ))}
                  {!workspace?.feedback?.length ? <li>(none)</li> : null}
                </ul>
              </div>
              <div>
                <h3 className="text-sm font-semibold">Description</h3>
                <p className="mt-1 max-h-48 overflow-auto rounded-md border border-black/10 bg-slate-50 p-2 text-xs whitespace-pre-wrap">
                  {workspace?.job?.description || '(no description)'}
                </p>
              </div>
            </section>
          </>
        )}
      </article>
    </section>
  )
}
