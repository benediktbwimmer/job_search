import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { DashboardPage } from './pages/DashboardPage'
import { WorkspacePage } from './pages/WorkspacePage'
import { BoardPage } from './pages/BoardPage'
import { RunsPage } from './pages/RunsPage'

const tabs = [
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/workspace', label: 'Workspace' },
  { to: '/board', label: 'Board' },
  { to: '/runs', label: 'Runs' },
]

export default function App() {
  return (
    <main className="mx-auto max-w-[1360px] p-4 md:p-5">
      <header className="mb-3 rounded-xl border border-black/10 bg-panel px-3 py-2 shadow-panel">
        <nav className="flex flex-wrap gap-2">
          {tabs.map((tab) => (
            <NavLink
              key={tab.to}
              to={tab.to}
              className={({ isActive }) =>
                [
                  'rounded-md border px-3 py-1.5 text-sm transition',
                  isActive
                    ? 'border-accent bg-accent text-white'
                    : 'border-black/10 bg-white text-ink hover:border-accent/50 hover:text-accent',
                ].join(' ')
              }
            >
              {tab.label}
            </NavLink>
          ))}
        </nav>
      </header>

      <Routes>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/workspace" element={<WorkspacePage />} />
        <Route path="/board" element={<BoardPage />} />
        <Route path="/runs" element={<RunsPage />} />
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </main>
  )
}
