const API_PREFIX = '/api'

function joinApi(path: string): string {
  const p = path.startsWith('/') ? path : `/${path}`
  return `${API_PREFIX}${p}`
}

export async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(joinApi(path), { credentials: 'same-origin' })
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    const message = (data && typeof data.message === 'string' && data.message) || `GET ${path} failed`
    throw new Error(message)
  }
  return data as T
}

export async function apiPost<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetch(joinApi(path), {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload ?? {}),
  })
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    const message = (data && typeof data.message === 'string' && data.message) || `POST ${path} failed`
    throw new Error(message)
  }
  return data as T
}

export function fmtDate(value?: string | null): string {
  if (!value) return 'n/a'
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return value
  return dt.toLocaleString()
}

export function fmtDurationMs(value?: number | null): string {
  const ms = Number(value || 0)
  if (ms <= 0) return '0s'
  const sec = Math.floor(ms / 1000)
  const min = Math.floor(sec / 60)
  if (min <= 0) return `${sec}s`
  const rem = sec % 60
  return `${min}m ${rem}s`
}

export function fmtSalary(min?: number, max?: number): string {
  if (!min) return ''
  const minText = `EUR ${Math.round(min).toLocaleString()}`
  if (!max) return `${minText}/year+`
  return `${minText} - EUR ${Math.round(max).toLocaleString()}/year`
}
