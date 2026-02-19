import { useEffect, useMemo, useRef, useState } from 'react'
import { apiGet, apiPost, fmtDate, fmtDurationMs } from '../lib/api'
import type { ActiveRun, RunRecord, SourceEvent } from '../lib/types'

type RunsPayload = { runs: RunRecord[] }
type ActivePayload = { run: ActiveRun }
type RunPayload = { run: RunRecord & { summary?: Record<string, unknown> } }
type SourcePayload = { run_id: string; source_events: SourceEvent[] }

export function RunsPage() {
  const [runs, setRuns] = useState<RunRecord[]>([])
  const [active, setActive] = useState<ActiveRun | null>(null)
  const [selectedRunId, setSelectedRunId] = useState('')
  const [selectedRun, setSelectedRun] = useState<(RunRecord & { summary?: Record<string, unknown> }) | null>(null)
  const [sources, setSources] = useState<SourceEvent[]>([])
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const timerRef = useRef<number | null>(null)

  const activeProgressPct = useMemo(() => {
    const total = Number(active?.progress?.total || 0)
    const processed = Number(active?.progress?.processed || 0)
    if (total <= 0) return 0
    return Math.max(0, Math.min(100, Math.round((processed / total) * 100)))
  }, [active])

  async function loadRuns() {
    const payload = await apiGet<RunsPayload>('/runs?limit=40')
    const items = payload.runs || []
    setRuns(items)
    if (!selectedRunId && items.length) setSelectedRunId(items[0].run_id)
  }

  async function loadActive() {
    const payload = await apiGet<ActivePayload>('/runs/active')
    setActive(payload.run || null)
    const activeRunId = payload.run?.run_id
    if (activeRunId && !selectedRunId) setSelectedRunId(activeRunId)
  }

  async function loadRunDetail(runId: string) {
    if (!runId) {
      setSelectedRun(null)
      setSources([])
      return
    }
    const [runData, sourceData] = await Promise.all([
      apiGet<RunPayload>(`/runs/${encodeURIComponent(runId)}`),
      apiGet<SourcePayload>(`/runs/${encodeURIComponent(runId)}/sources`),
    ])
    setSelectedRun(runData.run || null)
    setSources(sourceData.source_events || [])
  }

  async function refreshAll() {
    setLoading(true)
    setError('')
    try {
      await Promise.all([loadRuns(), loadActive()])
      if (selectedRunId) {
        await loadRunDetail(selectedRunId)
      }
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void refreshAll()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    if (!selectedRunId) return
    void loadRunDetail(selectedRunId).catch((err) => setError((err as Error).message))
  }, [selectedRunId])

  useEffect(() => {
    if (!autoRefresh) {
      if (timerRef.current) window.clearInterval(timerRef.current)
      timerRef.current = null
      return
    }
    timerRef.current = window.setInterval(() => {
      void refreshAll()
    }, 10000)
    return () => {
      if (timerRef.current) window.clearInterval(timerRef.current)
      timerRef.current = null
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoRefresh, selectedRunId])

  async function startRun() {
    setLoading(true)
    setError('')
    try {
      const resp = await apiPost<{ started: boolean; run: ActiveRun; message?: string }>('/runs/start', {})
      if (!resp.started && resp.message) {
        setError(resp.message)
      }
      setActive(resp.run)
      if (resp.run?.run_id) setSelectedRunId(resp.run.run_id)
      await refreshAll()
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <section className="grid gap-3 lg:grid-cols-[1fr_1fr]">
      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel lg:col-span-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <h1 className="text-lg font-semibold">Runs</h1>
            <p className="text-sm text-muted">Trigger pipeline runs, monitor active progress, inspect history and source diagnostics.</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="rounded-md border border-accent bg-accent px-3 py-1.5 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-60"
              disabled={Boolean(active?.running) || loading}
              onClick={() => void startRun()}
            >
              Start Pipeline Run
            </button>
            <button className="rounded-md border border-black/10 bg-white px-3 py-1.5 text-sm" onClick={() => void refreshAll()}>
              Refresh
            </button>
            <label className="flex items-center gap-1 rounded-md border border-black/10 bg-white px-2 py-1 text-xs">
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
              Auto-refresh
            </label>
          </div>
        </div>
        {error ? <p className="mt-2 text-sm text-red-700">{error}</p> : null}
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel lg:col-span-2">
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-muted">Active Run</h2>
        {active?.running ? (
          <>
            <div className="grid gap-2 sm:grid-cols-4">
              <Metric label="Run id" value={active.run_id || 'starting...'} mono />
              <Metric label="Status" value={active.status || 'running'} />
              <Metric label="Started" value={fmtDate(active.started_at)} />
              <Metric label="Elapsed" value={`${active.elapsed_seconds || 0}s`} />
            </div>
            <div className="mt-3 rounded-md border border-black/10 bg-white p-2">
              <div className="mb-1 flex items-center justify-between text-xs text-muted">
                <span>Progress</span>
                <span>{active.progress.processed}/{active.progress.total}</span>
              </div>
              <div className="h-2 overflow-hidden rounded bg-slate-200">
                <div className="h-full bg-accent" style={{ width: `${activeProgressPct}%` }} />
              </div>
              <div className="mt-2 grid grid-cols-2 gap-2 text-xs md:grid-cols-4">
                <Metric label="Live" value={String(active.progress.live || 0)} />
                <Metric label="Cache" value={String(active.progress.cache || 0)} />
                <Metric label="Failed" value={String(active.progress.failed || 0)} />
                <Metric label="Filtered" value={String(active.progress.filtered || 0)} />
              </div>
            </div>
            <div className="mt-3">
              <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted">Live Logs</h3>
              <pre className="max-h-48 overflow-auto rounded-md border border-black/10 bg-slate-50 p-2 text-[11px] whitespace-pre-wrap">
                {(active.logs || []).slice(-30).join('\n') || '(waiting for output)'}
              </pre>
            </div>
          </>
        ) : (
          <div className="grid gap-2 sm:grid-cols-4">
            <Metric label="Status" value={active?.status || 'idle'} />
            <Metric label="Run id" value={active?.run_id || 'n/a'} mono />
            <Metric label="Ended" value={fmtDate(active?.ended_at || '')} />
            <Metric label="Exit" value={active?.exit_code === null || active?.exit_code === undefined ? 'n/a' : String(active.exit_code)} />
          </div>
        )}
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        <div className="mb-2 flex items-center justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted">Run History</h2>
          <span className="text-xs text-muted">{loading ? 'loading...' : `${runs.length} runs`}</span>
        </div>
        <div className="max-h-[68vh] space-y-2 overflow-auto pr-1">
          {runs.map((run) => (
            <button
              key={run.run_id}
              className={`w-full rounded-md border px-2 py-2 text-left ${run.run_id === selectedRunId ? 'border-accent bg-slate-50' : 'border-black/10 bg-white'}`}
              onClick={() => setSelectedRunId(run.run_id)}
            >
              <div className="text-xs font-semibold uppercase text-muted">{run.status}</div>
              <div className="text-sm font-medium">{run.run_id}</div>
              <div className="text-xs text-muted">{fmtDate(run.started_at)} | {fmtDurationMs(run.duration_ms)}</div>
              <div className="mt-1 text-[11px] text-muted">jobs {run.total_jobs} | A/B/C {run.a_tier}/{run.b_tier}/{run.c_tier} | live/cache {run.llm_scored_live}/{run.llm_cache_hits}</div>
            </button>
          ))}
          {!runs.length ? <p className="text-sm text-muted">No run history found.</p> : null}
        </div>
      </article>

      <article className="rounded-xl border border-black/10 bg-panel p-3 shadow-panel">
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-muted">Run Detail</h2>
        {!selectedRun ? (
          <p className="text-sm text-muted">Select a run to inspect details.</p>
        ) : (
          <>
            <div className="grid gap-2 sm:grid-cols-2">
              <Metric label="Run id" value={selectedRun.run_id} mono />
              <Metric label="Status" value={selectedRun.status} />
              <Metric label="Started" value={fmtDate(selectedRun.started_at)} />
              <Metric label="Ended" value={fmtDate(selectedRun.ended_at)} />
              <Metric label="Duration" value={fmtDurationMs(selectedRun.duration_ms)} />
              <Metric label="Jobs" value={String(selectedRun.total_jobs)} />
              <Metric label="Tier split" value={`${selectedRun.a_tier}/${selectedRun.b_tier}/${selectedRun.c_tier}`} />
              <Metric label="LLM live/cache/failed" value={`${selectedRun.llm_scored_live}/${selectedRun.llm_cache_hits}/${selectedRun.llm_failed}`} />
            </div>

            <section className="mt-3">
              <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-muted">Source Events</h3>
              <div className="max-h-64 overflow-auto rounded-md border border-black/10 bg-white">
                <table className="min-w-full text-left text-xs">
                  <thead className="bg-slate-100">
                    <tr>
                      <th className="px-2 py-1">Source</th>
                      <th className="px-2 py-1">Attempts</th>
                      <th className="px-2 py-1">Fetched</th>
                      <th className="px-2 py-1">Duration</th>
                      <th className="px-2 py-1">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sources.map((s, idx) => (
                      <tr key={`${s.source_name}-${idx}`} className="border-t border-black/5">
                        <td className="px-2 py-1">{s.source_name}</td>
                        <td className="px-2 py-1">{s.attempts}</td>
                        <td className="px-2 py-1">{s.jobs_fetched}</td>
                        <td className="px-2 py-1">{fmtDurationMs(s.duration_ms)}</td>
                        <td className="px-2 py-1">{s.success ? 'ok' : (s.error_message || 'failed')}</td>
                      </tr>
                    ))}
                    {!sources.length ? (
                      <tr>
                        <td className="px-2 py-2 text-muted" colSpan={5}>No source events for this run.</td>
                      </tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </section>
          </>
        )}
      </article>
    </section>
  )
}

function Metric({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="rounded-md border border-black/10 bg-white p-2">
      <div className="text-[11px] uppercase tracking-wide text-muted">{label}</div>
      <div className={`text-sm font-medium ${mono ? 'font-mono' : ''}`}>{value}</div>
    </div>
  )
}
