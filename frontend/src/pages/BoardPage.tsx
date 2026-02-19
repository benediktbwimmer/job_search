import { useEffect, useMemo, useState } from 'react'
import { apiGet, apiPost } from '../lib/api'
import type { ApplicationItem } from '../lib/types'

type ApplicationsPayload = { applications: ApplicationItem[] }

const COLUMNS = ['saved', 'applied', 'interview', 'offer', 'rejected'] as const

export function BoardPage() {
  const [apps, setApps] = useState<ApplicationItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const grouped = useMemo(() => {
    const base: Record<string, ApplicationItem[]> = Object.fromEntries(COLUMNS.map((c) => [c, []]))
    for (const app of apps) {
      const key = COLUMNS.includes(app.status as (typeof COLUMNS)[number]) ? app.status : 'saved'
      base[key].push(app)
    }
    return base
  }, [apps])

  async function load() {
    setLoading(true)
    setError('')
    try {
      const payload = await apiGet<ApplicationsPayload>('/applications?limit=500')
      setApps(payload.applications || [])
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  async function move(jobUrl: string, status: string, title?: string, company?: string) {
    setLoading(true)
    setError('')
    try {
      await apiPost('/applications', {
        job_url: jobUrl,
        status,
        title: title || '',
        company: company || '',
      })
      await load()
    } catch (err) {
      setError((err as Error).message)
      setLoading(false)
    }
  }

  return (
    <section className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
      <header className="mb-3 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">Application Board</h1>
          <p className="text-sm text-muted">Kanban-style progression across application stages.</p>
        </div>
        <span className="text-xs text-muted">{loading ? 'syncing...' : `${apps.length} applications`}</span>
      </header>

      {error ? <p className="mb-2 text-sm text-red-700">{error}</p> : null}

      <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-5">
        {COLUMNS.map((col) => (
          <div key={col} className="rounded-lg border border-black/10 bg-white p-2">
            <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">{col}</div>
            <div className="space-y-2">
              {grouped[col].map((app) => (
                <article key={app.job_url} className="rounded-md border border-black/10 bg-slate-50 p-2">
                  <a href={app.job_url} target="_blank" rel="noreferrer" className="block text-sm font-medium hover:text-accent">
                    {app.title || app.job_url}
                  </a>
                  <div className="text-xs text-muted">{app.company || 'Unknown company'}</div>
                  <div className="mt-1 grid grid-cols-2 gap-1">
                    {COLUMNS.filter((x) => x !== col).slice(0, 2).map((next) => (
                      <button
                        key={next}
                        className="rounded border border-black/10 bg-white px-1 py-1 text-[11px]"
                        onClick={() => void move(app.job_url, next, app.title, app.company)}
                      >
                        {next}
                      </button>
                    ))}
                  </div>
                </article>
              ))}
              {!grouped[col].length ? <p className="text-xs text-muted">No cards.</p> : null}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}
