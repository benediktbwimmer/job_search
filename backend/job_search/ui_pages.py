def dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Job Search Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg1: #f6f0e4;
      --bg2: #dff3eb;
      --ink: #1f2d33;
      --muted: #5f7178;
      --card: rgba(255,255,255,.9);
      --line: rgba(31,45,51,.13);
      --good: #146f4c;
      --warn: #a56a00;
      --bad: #8e2b2b;
      --accent: #17444c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", system-ui, sans-serif;
      color: var(--ink);
      background: radial-gradient(1100px 560px at 0 -18%, #f8d3a8, transparent 58%),
                  radial-gradient(900px 460px at 100% -26%, var(--bg2), transparent 58%),
                  var(--bg1);
      min-height: 100vh;
    }
    a { color: #1b5c7d; text-decoration: none; }
    .wrap { max-width: 1320px; margin: 0 auto; padding: 18px; }
    .topbar {
      display: flex; align-items: center; justify-content: space-between;
      background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; margin-bottom: 12px;
    }
    .tabs { display: flex; gap: 6px; flex-wrap: wrap; }
    .tab {
      display: inline-block; padding: 8px 10px; border-radius: 8px;
      border: 1px solid var(--line); background: #fff; color: var(--ink);
      font-size: 13px;
    }
    .tab.active { background: #1e4e57; color: #fff; border-color: #1e4e57; }
    .grid { display: grid; grid-template-columns: 1.1fr .9fr; gap: 12px; }
    .card {
      background: var(--card); border: 1px solid var(--line); border-radius: 14px;
      padding: 12px; box-shadow: 0 6px 16px rgba(20,30,40,.05);
    }
    .kpi { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 10px; }
    .k {
      min-width: 105px; border: 1px solid var(--line); border-radius: 10px; padding: 8px;
      background: rgba(255,255,255,.9);
    }
    .k .v { font-size: 22px; font-weight: 700; line-height: 1; }
    .k .l { font-size: 11px; color: var(--muted); margin-top: 3px; text-transform: uppercase; letter-spacing: .03em; }
    .filters {
      display: grid; grid-template-columns: repeat(8, minmax(0,1fr)); gap: 8px; margin-bottom: 8px;
    }
    input, select, button {
      width: 100%; border: 1px solid var(--line); border-radius: 9px; padding: 9px 10px; font: inherit; color: var(--ink); background: #fff;
    }
    button { cursor: pointer; background: var(--accent); color: #fff; border-color: var(--accent); }
    button.secondary { background: #fff; color: var(--ink); border-color: var(--line); }
    button.warn { background: #7a5420; border-color: #7a5420; }
    .row { display: flex; gap: 8px; align-items: center; }
    .jobs { max-height: 72vh; overflow: auto; display: grid; gap: 8px; }
    .job {
      border: 1px solid var(--line); border-radius: 10px; background: rgba(255,255,255,.92);
      padding: 10px; cursor: pointer;
    }
    .job.active { border-color: #1f5b7c; box-shadow: inset 0 0 0 1px rgba(31,91,124,.25); }
    .meta { font-size: 12px; color: var(--muted); margin-top: 3px; }
    .chips { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
    .chip {
      font-size: 11px; border: 1px solid var(--line); border-radius: 999px; padding: 3px 8px; background: #edf4f6;
    }
    .status-actions { display: flex; gap: 6px; flex-wrap: wrap; }
    .status-actions button {
      width: auto; padding: 6px 8px; font-size: 12px; border-radius: 999px;
      background: #fff; color: var(--ink); border: 1px solid var(--line);
    }
    .status-actions button:hover { border-color: #215f8a; color: #215f8a; }
    .presets { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 6px; }
    .presets button { width: auto; padding: 6px 8px; font-size: 12px; background: #fff; color: var(--ink); border: 1px solid var(--line); }
    .bulkbar {
      display: flex; gap: 8px; align-items: center; flex-wrap: wrap;
      border: 1px solid var(--line); border-radius: 10px; padding: 8px; margin-bottom: 8px; background: rgba(255,255,255,.82);
    }
    .small { font-size: 12px; color: var(--muted); }
    .error { color: var(--bad); font-size: 13px; min-height: 18px; margin: 4px 0; }
    .loading { color: var(--muted); font-size: 12px; }
    .toast {
      position: fixed; right: 16px; bottom: 16px; background: #142f35; color: #fff; border-radius: 9px;
      padding: 10px 12px; font-size: 13px; box-shadow: 0 8px 24px rgba(10,20,30,.2); display: none; z-index: 20;
    }
    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .cover-preview {
      margin: 8px 0 0;
      border: 1px solid var(--line);
      background: #fbfdff;
      border-radius: 10px;
      padding: 10px;
      white-space: pre-wrap;
      line-height: 1.42;
      font-size: 13px;
      max-height: 320px;
      overflow: auto;
    }
    @media (max-width: 1120px) {
      .grid { grid-template-columns: 1fr; }
      .filters { grid-template-columns: repeat(2, minmax(0,1fr)); }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <div class="topbar">
      <div class="tabs">
        <a class="tab active" href="/dashboard">Dashboard</a>
        <a class="tab" href="/workspace">Workspace</a>
        <a class="tab" href="/board">Board</a>
      </div>
      <div class="small" id="loading">idle</div>
    </div>

    <div class="card">
      <h1 style="margin:0 0 6px;">Job Search Dashboard</h1>
      <div class="small">Deep-linkable shortlist with quick actions, saved views, and rich detail pane.</div>
      <div class="kpi" id="kpis"></div>
      <div class="error" id="error"></div>
      <div class="filters">
        <input id="q" placeholder="Search title/company/description">
        <select id="tier"><option value="">Any tier</option><option>A</option><option>B</option><option>C</option></select>
        <select id="remote"><option value="">Remote + onsite</option><option value="true">Remote only</option><option value="false">Onsite only</option></select>
        <input id="minScore" type="number" min="0" max="100" placeholder="Min score">
        <select id="applicationStatus"><option value="">Any application status</option><option>saved</option><option>applied</option><option>interview</option><option>offer</option><option>rejected</option></select>
        <select id="sort"><option value="score_desc">Best score</option><option value="newest">Newest</option><option value="company">Company</option><option value="title">Title</option></select>
        <input id="viewName" placeholder="Saved view name">
        <select id="savedViews"><option value="">Saved views</option></select>
      </div>
      <div class="row" style="margin-bottom:8px;">
        <button id="applyFilters">Apply</button>
        <button id="saveView" class="secondary">Save View</button>
        <button id="deleteView" class="secondary">Delete View</button>
        <button id="copyLink" class="secondary">Copy Deep Link</button>
      </div>
    </div>

    <div class="grid" style="margin-top:10px;">
      <section class="card">
        <div class="bulkbar">
          <strong id="selectedCount">0 selected</strong>
          <button class="secondary" data-bulk-status="saved">Bulk saved</button>
          <button class="secondary" data-bulk-status="applied">Bulk applied</button>
          <button class="secondary" data-bulk-status="interview">Bulk interview</button>
          <button class="secondary" data-bulk-status="offer">Bulk offer</button>
          <button class="secondary warn" id="clearSelection">Clear</button>
        </div>
        <div id="jobs" class="jobs"></div>
      </section>

      <section class="card">
        <div id="detail">
          <div class="small">Select a job to see rich details and quick actions.</div>
        </div>
      </section>
    </div>
  </main>
  <div class="toast" id="toast"></div>
  <script>
    const state = {
      jobs: [],
      selectedJobUrl: '',
      selectedSet: new Set(),
      latestCoverBody: '',
      coverContextByJob: {},
      viewStorageKey: 'job_search_saved_views_v1',
    };

    function $(id) { return document.getElementById(id); }
    function setLoading(text) { $('loading').textContent = text; }
    function setError(text) { $('error').textContent = text || ''; }
    function toast(msg, isError=false) {
      const el = $('toast');
      el.textContent = msg;
      el.style.background = isError ? '#7b2f2f' : '#142f35';
      el.style.display = 'block';
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(() => { el.style.display = 'none'; }, 2200);
    }
    function escapeHtml(text) {
      return String(text || '').replace(/[&<>"']/g, (ch) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
      }[ch]));
    }
    function formatSalary(salary) {
      if (!salary || !salary.annual_min_eur) return '';
      const min = Number(salary.annual_min_eur).toLocaleString();
      const max = salary.annual_max_eur ? Number(salary.annual_max_eur).toLocaleString() : '';
      return max ? `EUR ${min} - ${max}/year` : `EUR ${min}/year+`;
    }
    function readFilters() {
      return {
        q: $('q').value.trim(),
        tier: $('tier').value,
        remote: $('remote').value,
        min_score: $('minScore').value.trim(),
        application_status: $('applicationStatus').value,
        sort: $('sort').value,
      };
    }
    function applyFiltersToInputs(f) {
      $('q').value = f.q || '';
      $('tier').value = f.tier || '';
      $('remote').value = f.remote || '';
      $('minScore').value = f.min_score || '';
      $('applicationStatus').value = f.application_status || '';
      $('sort').value = f.sort || 'score_desc';
    }
    function paramsFromState() {
      const f = readFilters();
      const p = new URLSearchParams({ limit: '60', include_diagnostics: 'true', sort: f.sort || 'score_desc' });
      Object.entries(f).forEach(([k,v]) => { if (v && k !== 'sort') p.set(k, v); });
      if (state.selectedJobUrl) p.set('job_url', state.selectedJobUrl);
      return p;
    }
    function writeUrl() {
      const p = paramsFromState();
      history.replaceState({}, '', '/dashboard?' + p.toString());
    }
    function parseUrl() {
      const p = new URLSearchParams(location.search);
      applyFiltersToInputs({
        q: p.get('q') || '',
        tier: p.get('tier') || '',
        remote: p.get('remote') || '',
        min_score: p.get('min_score') || '',
        application_status: p.get('application_status') || '',
        sort: p.get('sort') || 'score_desc',
      });
      state.selectedJobUrl = p.get('job_url') || '';
      const view = p.get('view');
      if (view) loadViewByName(view, true);
    }
    function getSavedViews() {
      try {
        const raw = localStorage.getItem(state.viewStorageKey);
        const obj = raw ? JSON.parse(raw) : {};
        return (obj && typeof obj === 'object') ? obj : {};
      } catch (_) { return {}; }
    }
    function setSavedViews(obj) {
      localStorage.setItem(state.viewStorageKey, JSON.stringify(obj || {}));
    }
    function refreshSavedViews() {
      const views = getSavedViews();
      const sel = $('savedViews');
      const current = sel.value;
      sel.innerHTML = '<option value="">Saved views</option>' + Object.keys(views).sort().map(v => `<option value="${v}">${v}</option>`).join('');
      if (current && views[current]) sel.value = current;
    }
    function saveCurrentView() {
      const name = $('viewName').value.trim();
      if (!name) return toast('Enter a view name', true);
      const views = getSavedViews();
      views[name] = readFilters();
      setSavedViews(views);
      refreshSavedViews();
      $('savedViews').value = name;
      toast('View saved');
    }
    function loadViewByName(name, silent=false) {
      if (!name) return;
      const views = getSavedViews();
      if (!views[name]) return;
      applyFiltersToInputs(views[name]);
      $('savedViews').value = name;
      if (!silent) toast('View loaded');
      loadJobs(true);
    }
    function deleteView() {
      const name = $('savedViews').value;
      if (!name) return toast('Select a saved view', true);
      const views = getSavedViews();
      delete views[name];
      setSavedViews(views);
      refreshSavedViews();
      toast('View deleted');
    }
    async function copyLink() {
      writeUrl();
      await navigator.clipboard.writeText(location.href);
      toast('Deep link copied');
    }
    function selectedCountText() {
      $('selectedCount').textContent = `${state.selectedSet.size} selected`;
    }
    function toggleSelected(url) {
      if (state.selectedSet.has(url)) state.selectedSet.delete(url);
      else state.selectedSet.add(url);
      selectedCountText();
    }
    async function bulkUpdate(status) {
      const urls = Array.from(state.selectedSet);
      if (!urls.length) return toast('No jobs selected', true);
      setLoading('bulk update...');
      setError('');
      try {
        const payload = { items: urls.map(url => ({ job_url: url, status })) };
        const resp = await fetch('/applications/bulk', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.message || 'bulk update failed');
        toast(`Updated ${data.updated || 0} applications`);
        state.selectedSet.clear();
        selectedCountText();
        await Promise.all([loadMetrics(), loadJobs(false)]);
      } catch (e) {
        setError(String(e.message || e));
        toast('Bulk update failed', true);
      } finally {
        setLoading('idle');
      }
    }
    async function loadMetrics() {
      const r = await fetch('/applications/metrics?days=30');
      const data = await r.json();
      const m = data.metrics || {};
      const f = m.funnel || {};
      const fu = m.followups || {};
      $('kpis').innerHTML = [
        ['Tracked', m.total_applications || 0],
        ['Applied+', f.applied_or_beyond || 0],
        ['Interview+', f.interview_or_beyond || 0],
        ['Offers', f.offers || 0],
        ['Due', fu.due_today || 0],
        ['Overdue', fu.overdue || 0],
      ].map(([l,v]) => `<div class="k"><div class="v">${v}</div><div class="l">${l}</div></div>`).join('');
    }
    function jobCard(job) {
      const active = job.url === state.selectedJobUrl ? 'active' : '';
      const checked = state.selectedSet.has(job.url) ? 'checked' : '';
      const salary = formatSalary(job.salary);
      const chips = [
        job.cv_variant ? `<span class="chip">${job.cv_variant}</span>` : '',
        salary ? `<span class="chip">${salary}</span>` : '',
        job.application_status ? `<span class="chip">app:${job.application_status}</span>` : '',
        (job.diagnostics && job.diagnostics.adaptive_bonus) ? `<span class="chip">adaptive ${job.diagnostics.adaptive_bonus > 0 ? '+' : ''}${job.diagnostics.adaptive_bonus}</span>` : '',
      ].join('');
      return `<article class="job ${active}" data-url="${job.url}">
        <div class="row">
          <input type="checkbox" class="pick" data-url="${job.url}" ${checked} style="width:auto;">
          <strong>${job.title || 'Untitled role'}</strong>
        </div>
        <div class="meta">${job.company || 'Unknown company'} | score ${job.score} | tier ${job.tier}</div>
        <div class="chips">${chips}</div>
      </article>`;
    }
    async function loadJobs(resetSelection=true) {
      setLoading('loading jobs...');
      setError('');
      if (resetSelection) state.selectedSet.clear();
      const query = paramsFromState().toString();
      writeUrl();
      try {
        const r = await fetch('/jobs?' + query);
        const data = await r.json();
        if (!r.ok) throw new Error(data.message || 'failed to load jobs');
        state.jobs = data.jobs || [];
        if (state.jobs.length && !state.selectedJobUrl) state.selectedJobUrl = state.jobs[0].url;
        if (state.selectedJobUrl && !state.jobs.find(j => j.url === state.selectedJobUrl)) {
          state.selectedJobUrl = state.jobs.length ? state.jobs[0].url : '';
        }
        $('jobs').innerHTML = state.jobs.length ? state.jobs.map(jobCard).join('') : '<div class="small">No jobs match this view.</div>';
        selectedCountText();
        document.querySelectorAll('#jobs .job').forEach(el => {
          el.addEventListener('click', (evt) => {
            if (evt.target.classList.contains('pick')) return;
            state.selectedJobUrl = el.dataset.url;
            writeUrl();
            renderDetails();
            document.querySelectorAll('#jobs .job').forEach(x => x.classList.remove('active'));
            el.classList.add('active');
          });
        });
        document.querySelectorAll('#jobs .pick').forEach(el => {
          el.addEventListener('click', (evt) => {
            evt.stopPropagation();
            toggleSelected(el.dataset.url);
          });
        });
        await renderDetails();
      } catch (e) {
        setError(String(e.message || e));
      } finally {
        setLoading('idle');
      }
    }
    async function updateSingleStatus(status) {
      const job = state.jobs.find(x => x.url === state.selectedJobUrl);
      if (!job) return;
      setLoading('updating...');
      try {
        const payload = { job_url: job.url, status, title: job.title || '', company: job.company || '' };
        const r = await fetch('/applications', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const data = await r.json();
        if (!r.ok) throw new Error(data.message || 'status update failed');
        toast('Status updated');
        await Promise.all([loadMetrics(), loadJobs(false)]);
      } catch (e) {
        setError(String(e.message || e));
        toast('Status update failed', true);
      } finally {
        setLoading('idle');
      }
    }
    async function setFollowupPreset(hours, type) {
      const job = state.jobs.find(x => x.url === state.selectedJobUrl);
      if (!job) return;
      const due = new Date(Date.now() + hours * 3600 * 1000).toISOString();
      const r = await fetch('/applications/followup', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_url: job.url, next_action_type: type, next_action_at: due }),
      });
      const data = await r.json();
      if (!r.ok) {
        setError(data.message || 'failed to set follow-up');
        return toast('Follow-up failed', true);
      }
      toast('Follow-up set');
      await loadJobs(false);
    }
    async function generateCoverDraft() {
      const job = state.jobs.find(x => x.url === state.selectedJobUrl);
      if (!job) return toast('Select a job first', true);
      const cvVariant = ($('dashCvVariant')?.value || '').trim() || 'en_short';
      const style = ($('dashCoverStyle')?.value || '').trim() || 'concise';
      const additionalContext = ($('dashCoverContext')?.value || '').trim();
      setLoading('generating cover letter...');
      setError('');
      try {
        const payload = {
          job_url: job.url,
          cv_variant: cvVariant,
          style,
          additional_context: additionalContext,
          regenerate: true,
        };
        const r = await fetch('/cover-letters/generate', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await r.json();
        if (!r.ok) throw new Error(data.message || 'cover letter generation failed');
        toast(`Cover letter generated (${data.model || 'gpt-5.2'})`);
        await renderDetails();
      } catch (e) {
        setError(String(e.message || e));
        toast('Cover generation failed', true);
      } finally {
        setLoading('idle');
      }
    }
    async function copyLatestCover() {
      if (!state.latestCoverBody) return toast('No draft to copy', true);
      await navigator.clipboard.writeText(state.latestCoverBody);
      toast('Cover letter copied');
    }
    async function renderDetails() {
      const job = state.jobs.find(x => x.url === state.selectedJobUrl);
      if (!job) {
        $('detail').innerHTML = '<div class="small">Select a job to see details.</div>';
        return;
      }
      setLoading('loading detail...');
      try {
        const r = await fetch('/applications/workspace?job_url=' + encodeURIComponent(job.url));
        const data = await r.json();
        const w = data.workspace || {};
        const app = w.application || {};
        const feedback = w.feedback || [];
        const letters = w.cover_letters || [];
        const salary = formatSalary(job.salary);
        const reasons = (job.reasons || []).slice(0, 8);
        const feedbackHtml = feedback.length ? feedback.slice(0, 8).map(x => `<li>${x.created_at}: ${x.action} ${x.value || ''}</li>`).join('') : '<li>(none)</li>';
        const letterHtml = letters.length ? letters.slice(0, 5).map(x => `<li>${x.generated_at} v${x.version} (${x.cv_variant})</li>`).join('') : '<li>(none)</li>';
        const latestLetter = letters.length ? letters[0] : null;
        state.latestCoverBody = latestLetter ? String(latestLetter.body || '').trim() : '';
        const coverMeta = latestLetter
          ? `${latestLetter.generated_at} | v${latestLetter.version} | ${latestLetter.cv_variant} | ${latestLetter.style || 'concise'}`
          : 'No draft generated yet.';
        const coverPreview = state.latestCoverBody
          ? escapeHtml(state.latestCoverBody)
          : 'Generate a draft to preview it here.';
        const defaultVariant = latestLetter ? String(latestLetter.cv_variant || '') : (job.cv_variant || 'en_short');
        const selectedStyle = latestLetter ? String(latestLetter.style || 'concise') : 'concise';
        const contextValue = String(state.coverContextByJob[job.url] || '');
        $('detail').innerHTML = `
          <h2 style="margin:0 0 6px;">${job.title || 'Untitled role'}</h2>
          <div class="meta">${job.company || 'Unknown company'} | ${job.location || ''}</div>
          <div class="chips">
            <span class="chip">score ${job.score}</span>
            <span class="chip">tier ${job.tier}</span>
            ${job.cv_variant ? `<span class="chip">${job.cv_variant}</span>` : ''}
            ${salary ? `<span class="chip">${salary}</span>` : ''}
            ${app.status ? `<span class="chip">application: ${app.status}</span>` : '<span class="chip">not tracked yet</span>'}
          </div>
          <div class="status-actions" style="margin-top:8px;">
            <button data-status="saved">saved</button>
            <button data-status="applied">applied</button>
            <button data-status="interview">interview</button>
            <button data-status="offer">offer</button>
            <button data-status="rejected">rejected</button>
          </div>
          <div class="presets">
            <button data-follow="24:follow_up_email">follow up in 1d</button>
            <button data-follow="72:follow_up_email">follow up in 3d</button>
            <button data-follow="168:interview_prep">prep in 7d</button>
          </div>
          <div class="split" style="margin-top:10px;">
            <div>
              <h3 style="margin:6px 0;">Why matched</h3>
              <ul>${reasons.length ? reasons.map(r => `<li>${r}</li>`).join('') : '<li>(none)</li>'}</ul>
              <h3 style="margin:6px 0;">Feedback timeline</h3>
              <ul>${feedbackHtml}</ul>
            </div>
            <div>
              <h3 style="margin:6px 0;">Cover letters</h3>
              <ul>${letterHtml}</ul>
              <h3 style="margin:8px 0 4px;">Cover letter draft</h3>
              <div class="small">${coverMeta}</div>
              <div class="row" style="margin-top:6px;">
                <input id="dashCvVariant" placeholder="en_short" value="${defaultVariant}">
                <select id="dashCoverStyle">
                  <option value="concise" ${selectedStyle === 'concise' ? 'selected' : ''}>concise</option>
                  <option value="detailed" ${selectedStyle === 'detailed' ? 'selected' : ''}>detailed</option>
                </select>
              </div>
              <textarea id="dashCoverContext" rows="4" placeholder="Optional context for this draft, e.g. highlight prior Temporal experience at Company X with workflow orchestration." style="margin-top:6px;">${escapeHtml(contextValue)}</textarea>
              <div class="row" style="margin-top:6px;">
                <button id="dashGenCover">Generate Draft (gpt-5.2)</button>
                <button id="dashCopyCover" class="secondary">Copy Draft</button>
              </div>
              <pre class="cover-preview">${coverPreview}</pre>
              <div class="row">
                <a class="tab" href="/workspace?job_url=${encodeURIComponent(job.url)}">Open Workspace</a>
                <a class="tab" href="${job.url}" target="_blank" rel="noreferrer">Open Job</a>
              </div>
              <h3 style="margin:8px 0 4px;">Description</h3>
              <div class="small">${(job.description || '').slice(0, 1200) || '(no description)'}</div>
            </div>
          </div>
        `;
        document.querySelectorAll('#detail [data-status]').forEach(el => {
          el.onclick = () => updateSingleStatus(el.dataset.status);
        });
        document.querySelectorAll('#detail [data-follow]').forEach(el => {
          const [h, type] = el.dataset.follow.split(':');
          el.onclick = () => setFollowupPreset(Number(h), type);
        });
        const ctxInput = $('dashCoverContext');
        if (ctxInput) {
          ctxInput.oninput = () => {
            state.coverContextByJob[job.url] = ctxInput.value;
          };
        }
        const genBtn = $('dashGenCover');
        if (genBtn) genBtn.onclick = generateCoverDraft;
        const copyBtn = $('dashCopyCover');
        if (copyBtn) copyBtn.onclick = copyLatestCover;
      } finally {
        setLoading('idle');
      }
    }
    function bindEvents() {
      $('applyFilters').onclick = () => loadJobs(true);
      $('saveView').onclick = saveCurrentView;
      $('deleteView').onclick = deleteView;
      $('savedViews').onchange = () => loadViewByName($('savedViews').value);
      $('copyLink').onclick = copyLink;
      $('clearSelection').onclick = () => { state.selectedSet.clear(); selectedCountText(); };
      document.querySelectorAll('[data-bulk-status]').forEach(el => {
        el.onclick = () => bulkUpdate(el.dataset.bulkStatus);
      });
      ['q','tier','remote','minScore','applicationStatus','sort'].forEach(id => {
        $(id).addEventListener('keydown', (e) => { if (e.key === 'Enter') loadJobs(true); });
      });
    }
    parseUrl();
    refreshSavedViews();
    bindEvents();
    Promise.all([loadMetrics(), loadJobs(true)]);
  </script>
</body>
</html>"""


def workspace_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Application Workspace</title>
  <style>
    :root { --ink:#17212b; --muted:#617587; --line:#dce4ef; --card:#fff; --bg:#f4f7fb; --accent:#1f4ea9; --danger:#8a2d2d; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: "Space Grotesk", ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--ink); }
    .wrap { max-width: 1280px; margin: 0 auto; padding: 16px; }
    .top { display:flex; justify-content:space-between; align-items:center; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:10px 12px; margin-bottom:10px; }
    .tabs { display:flex; gap:6px; }
    .tab { padding:8px 10px; border:1px solid var(--line); border-radius:8px; text-decoration:none; color:var(--ink); background:#fff; font-size:13px; }
    .tab.active { background: var(--accent); color:#fff; border-color:var(--accent); }
    .grid { display:grid; grid-template-columns: 340px 1fr; gap:10px; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:10px; }
    .item { border:1px solid #e6edf7; border-radius:10px; padding:9px; margin-bottom:7px; cursor:pointer; }
    .item.active { border-color:#2c67c5; background:#f2f7ff; }
    .meta { font-size:12px; color:var(--muted); }
    .row { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
    input, select, textarea, button { width:100%; font:inherit; border:1px solid var(--line); border-radius:8px; padding:8px; }
    button { cursor:pointer; background:var(--accent); color:#fff; border-color:var(--accent); }
    button.secondary { background:#fff; color:var(--ink); border-color:var(--line); }
    .chips { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
    .chip { border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:11px; background:#eef3fb; }
    .status-actions { display:flex; gap:6px; flex-wrap:wrap; margin:8px 0; }
    .status-actions button { width:auto; padding:6px 8px; font-size:12px; background:#fff; color:var(--ink); border:1px solid var(--line); }
    .error { min-height:18px; color:var(--danger); font-size:13px; margin-bottom:6px; }
    .toast { position:fixed; right:16px; bottom:16px; background:#1f3244; color:#fff; border-radius:8px; padding:10px 12px; display:none; }
    .cover-preview {
      margin: 6px 0 0;
      border: 1px solid var(--line);
      background: #f8fbff;
      border-radius: 10px;
      padding: 10px;
      white-space: pre-wrap;
      line-height: 1.45;
      font-size: 13px;
      max-height: 320px;
      overflow: auto;
    }
    @media (max-width: 1000px) { .grid { grid-template-columns: 1fr; } .row { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <main class="wrap">
    <div class="top">
      <div class="tabs">
        <a class="tab" href="/dashboard">Dashboard</a>
        <a class="tab active" href="/workspace">Workspace</a>
        <a class="tab" href="/board">Board</a>
      </div>
      <div class="meta" id="loading">idle</div>
    </div>
    <div class="grid">
      <section class="card">
        <div class="row">
          <input id="search" placeholder="Filter by title/company">
          <select id="statusFilter"><option value="">All statuses</option><option>saved</option><option>applied</option><option>interview</option><option>offer</option><option>rejected</option></select>
        </div>
        <div class="error" id="error"></div>
        <div id="apps"></div>
      </section>
      <section class="card">
        <div id="detail" class="meta">Select an application.</div>
        <hr>
        <h3 style="margin:6px 0;">Quick Follow-up Presets</h3>
        <div class="chips">
          <button class="secondary" data-preset="24:follow_up_email">+1 day</button>
          <button class="secondary" data-preset="72:follow_up_email">+3 days</button>
          <button class="secondary" data-preset="168:interview_prep">+7 days</button>
        </div>
        <div class="row" style="margin-top:8px;">
          <input id="nextType" placeholder="next action type">
          <input id="nextAt" placeholder="2026-02-20T10:00:00Z">
        </div>
        <button id="saveFollowup">Save Follow-up</button>
        <hr>
        <h3 style="margin:6px 0;">Generate Cover Letter</h3>
        <div class="row">
          <input id="cvVariant" placeholder="en_short">
          <input id="coverStyle" placeholder="concise">
        </div>
        <textarea id="coverContext" rows="4" placeholder="Optional context for this draft, e.g. highlight prior Temporal experience at Company X with workflow orchestration." style="margin-top:8px;"></textarea>
        <button id="genCover">Generate Draft (gpt-5.2)</button>
      </section>
    </div>
  </main>
  <div class="toast" id="toast"></div>
  <script>
    const state = { apps: [], selectedJobUrl: '' };
    function $(id){ return document.getElementById(id); }
    function setLoading(t){ $('loading').textContent = t; }
    function setError(t){ $('error').textContent = t || ''; }
    function toast(msg, err=false){
      const el = $('toast'); el.textContent = msg; el.style.background = err ? '#7f3030' : '#1f3244';
      el.style.display = 'block'; clearTimeout(window.__tt); window.__tt = setTimeout(() => el.style.display='none', 2200);
    }
    function escapeHtml(text) {
      return String(text || '').replace(/[&<>"']/g, (ch) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
      }[ch]));
    }
    function parseUrl() {
      const p = new URLSearchParams(location.search);
      $('search').value = p.get('q') || '';
      $('statusFilter').value = p.get('status') || '';
      state.selectedJobUrl = p.get('job_url') || '';
    }
    function writeUrl() {
      const p = new URLSearchParams();
      const q = $('search').value.trim();
      const s = $('statusFilter').value;
      if (q) p.set('q', q);
      if (s) p.set('status', s);
      if (state.selectedJobUrl) p.set('job_url', state.selectedJobUrl);
      history.replaceState({}, '', '/workspace?' + p.toString());
    }
    function appCard(a) {
      const active = a.job_url === state.selectedJobUrl ? 'active' : '';
      return `<div class="item ${active}" data-url="${a.job_url}">
        <div><strong>${a.title || a.job_url}</strong></div>
        <div class="meta">${a.company || 'Unknown'} | ${a.status || ''}</div>
        <div class="meta">next: ${a.next_action_type || '-'} ${a.next_action_at || ''}</div>
      </div>`;
    }
    async function loadApps() {
      setLoading('loading apps...');
      setError('');
      try {
        const resp = await fetch('/applications?limit=200');
        const data = await resp.json();
        const all = data.applications || [];
        const q = $('search').value.trim().toLowerCase();
        const st = $('statusFilter').value;
        state.apps = all.filter(a => {
          const okQ = !q || `${a.title || ''} ${a.company || ''}`.toLowerCase().includes(q);
          const okS = !st || (a.status || '') === st;
          return okQ && okS;
        });
        if (!state.selectedJobUrl && state.apps.length) state.selectedJobUrl = state.apps[0].job_url;
        if (state.selectedJobUrl && !state.apps.find(a => a.job_url === state.selectedJobUrl)) {
          state.selectedJobUrl = state.apps.length ? state.apps[0].job_url : '';
        }
        $('apps').innerHTML = state.apps.length ? state.apps.map(appCard).join('') : '<div class="meta">No applications for current filter.</div>';
        document.querySelectorAll('#apps .item').forEach(el => {
          el.onclick = () => { state.selectedJobUrl = el.dataset.url; writeUrl(); refreshDetail(); };
        });
        await refreshDetail();
      } catch (e) {
        setError(String(e.message || e));
      } finally { setLoading('idle'); }
    }
    async function refreshDetail() {
      if (!state.selectedJobUrl) { $('detail').innerHTML = '<div class="meta">Select an application.</div>'; return; }
      setLoading('loading detail...');
      const resp = await fetch('/applications/workspace?job_url=' + encodeURIComponent(state.selectedJobUrl));
      const data = await resp.json();
      const w = data.workspace || {};
      const a = w.application || {};
      const j = w.job || {};
      const feedback = w.feedback || [];
      const letters = w.cover_letters || [];
      const fb = feedback.length ? feedback.slice(0,10).map(x => `<li>${x.created_at}: ${x.action} ${x.value || ''}</li>`).join('') : '<li>(none)</li>';
      const cl = letters.length ? letters.slice(0,8).map(x => `<li>${x.generated_at} v${x.version} (${x.cv_variant})</li>`).join('') : '<li>(none)</li>';
      const latestLetter = letters.length ? letters[0] : null;
      const latestBody = latestLetter ? String(latestLetter.body || '').trim() : '';
      const latestMeta = latestLetter
        ? `${latestLetter.generated_at} | v${latestLetter.version} | ${latestLetter.cv_variant} | ${latestLetter.style || 'concise'}`
        : 'No draft generated yet.';
      $('detail').innerHTML = `
        <h2 style="margin:0 0 4px;">${j.title || a.title || state.selectedJobUrl}</h2>
        <div class="meta">${j.company || a.company || ''} | status: ${a.status || 'n/a'}</div>
        <div class="status-actions">
          <button data-status="saved">saved</button>
          <button data-status="applied">applied</button>
          <button data-status="interview">interview</button>
          <button data-status="offer">offer</button>
          <button data-status="rejected">rejected</button>
        </div>
        <div class="chips">
          ${j.cv_variant ? `<span class="chip">${j.cv_variant}</span>` : ''}
          ${j.salary && j.salary.annual_min_eur ? `<span class="chip">EUR ${Number(j.salary.annual_min_eur).toLocaleString()}/year+</span>` : ''}
        </div>
        <p>${(j.description || '').slice(0, 1200)}</p>
        <h3>Timeline</h3><ul>${fb}</ul>
        <h3>Latest Cover Letter Draft</h3>
        <div class="meta">${latestMeta}</div>
        <pre class="cover-preview">${latestBody ? escapeHtml(latestBody) : 'Generate a draft to preview it here.'}</pre>
        <h3>Cover Letter History</h3><ul>${cl}</ul>
      `;
      document.querySelectorAll('#detail [data-status]').forEach(el => {
        el.onclick = () => setStatus(el.dataset.status);
      });
      setLoading('idle');
    }
    async function setStatus(status) {
      const app = state.apps.find(x => x.job_url === state.selectedJobUrl) || {};
      const payload = { job_url: state.selectedJobUrl, status, title: app.title || '', company: app.company || '' };
      const r = await fetch('/applications', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const data = await r.json();
      if (!r.ok) { setError(data.message || 'failed'); return toast('Status update failed', true); }
      toast('Status updated');
      await loadApps();
    }
    async function saveFollowup(hoursPreset=null, typePreset=null) {
      if (!state.selectedJobUrl) return toast('Select an application', true);
      const nextType = typePreset || $('nextType').value.trim() || 'follow_up';
      const nextAt = hoursPreset ? new Date(Date.now() + hoursPreset * 3600 * 1000).toISOString() : $('nextAt').value.trim();
      const payload = { job_url: state.selectedJobUrl, next_action_type: nextType, next_action_at: nextAt };
      const r = await fetch('/applications/followup', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const data = await r.json();
      if (!r.ok) { setError(data.message || 'failed'); return toast('Follow-up failed', true); }
      toast('Follow-up saved');
      await loadApps();
    }
    async function generateCover() {
      if (!state.selectedJobUrl) return toast('Select an application', true);
      const payload = {
        job_url: state.selectedJobUrl,
        cv_variant: $('cvVariant').value.trim() || 'en_short',
        style: $('coverStyle').value.trim() || 'concise',
        additional_context: $('coverContext').value.trim(),
        regenerate: true,
      };
      const r = await fetch('/cover-letters/generate', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const data = await r.json();
      if (!r.ok) { setError(data.message || 'failed'); return toast('Generation failed', true); }
      toast(`Cover letter generated (${data.model || 'gpt-5.2'})`);
      await refreshDetail();
    }
    function bind() {
      $('search').addEventListener('keydown', (e) => { if (e.key === 'Enter') { writeUrl(); loadApps(); } });
      $('statusFilter').onchange = () => { writeUrl(); loadApps(); };
      $('saveFollowup').onclick = () => saveFollowup();
      $('genCover').onclick = generateCover;
      document.querySelectorAll('[data-preset]').forEach(el => {
        const [h, t] = el.dataset.preset.split(':');
        el.onclick = () => saveFollowup(Number(h), t);
      });
    }
    parseUrl();
    bind();
    loadApps();
  </script>
</body>
</html>"""


def board_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Application Board</title>
  <style>
    :root { --line:#dbe3ee; --card:#fff; --bg:#f3f7fb; --ink:#1b2731; --muted:#657687; --accent:#1f4ea9; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:"Space Grotesk", ui-sans-serif, system-ui, sans-serif; color:var(--ink); background:var(--bg); }
    .wrap { max-width: 1500px; margin: 0 auto; padding: 14px; }
    .top { display:flex; justify-content:space-between; align-items:center; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:10px; margin-bottom:10px; }
    .tabs { display:flex; gap:6px; }
    .tab { padding:8px 10px; border:1px solid var(--line); border-radius:8px; background:#fff; color:var(--ink); text-decoration:none; font-size:13px; }
    .tab.active { background:var(--accent); color:#fff; border-color:var(--accent); }
    .bar { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:10px; margin-bottom:10px; display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .board { display:grid; grid-template-columns: repeat(5, minmax(250px, 1fr)); gap:10px; }
    .col { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:8px; min-height: 70vh; }
    .col h3 { margin:0 0 8px; font-size:14px; text-transform:uppercase; letter-spacing:.03em; color:var(--muted); }
    .card-item { border:1px solid #e5ecf5; border-radius:10px; padding:8px; margin-bottom:8px; background:#fff; }
    .meta { font-size:12px; color:var(--muted); }
    .chips { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
    .chip { border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:11px; background:#eef3fb; cursor:pointer; }
    input, select, button { border:1px solid var(--line); border-radius:8px; padding:8px; font:inherit; }
    button { background:var(--accent); color:#fff; cursor:pointer; border-color:var(--accent); }
    button.secondary { background:#fff; color:var(--ink); border-color:var(--line); }
    .toast { position:fixed; right:16px; bottom:16px; background:#1f3244; color:#fff; border-radius:8px; padding:10px 12px; display:none; }
    @media (max-width: 1280px) { .board { grid-template-columns: repeat(2, minmax(250px,1fr)); } }
    @media (max-width: 760px) { .board { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <main class="wrap">
    <div class="top">
      <div class="tabs">
        <a class="tab" href="/dashboard">Dashboard</a>
        <a class="tab" href="/workspace">Workspace</a>
        <a class="tab active" href="/board">Board</a>
      </div>
      <div class="meta" id="loading">idle</div>
    </div>
    <div class="bar">
      <strong id="count">0 selected</strong>
      <button class="secondary" data-status="saved">bulk saved</button>
      <button class="secondary" data-status="applied">bulk applied</button>
      <button class="secondary" data-status="interview">bulk interview</button>
      <button class="secondary" data-status="offer">bulk offer</button>
      <button class="secondary" data-status="rejected">bulk rejected</button>
      <button class="secondary" id="clear">clear</button>
      <span class="meta">Quick move: click chips on each card.</span>
    </div>
    <div id="board" class="board"></div>
  </main>
  <div class="toast" id="toast"></div>
  <script>
    const statuses = ['saved','applied','interview','offer','rejected'];
    const labels = {saved:'Saved', applied:'Applied', interview:'Interview', offer:'Offer', rejected:'Rejected'};
    const selected = new Set();
    function $(id){ return document.getElementById(id); }
    function setLoading(t){ $('loading').textContent = t; }
    function toast(msg, err=false){ const el=$('toast'); el.textContent=msg; el.style.background = err ? '#7f3030' : '#1f3244'; el.style.display='block'; clearTimeout(window.__tt); window.__tt=setTimeout(()=>el.style.display='none', 2200); }
    function refreshCount(){ $('count').textContent = `${selected.size} selected`; }
    function card(app){
      const checked = selected.has(app.job_url) ? 'checked' : '';
      return `<div class="card-item">
        <div><input type="checkbox" data-pick="${app.job_url}" ${checked} style="margin-right:6px;"> <strong>${app.title || app.job_url}</strong></div>
        <div class="meta">${app.company || 'Unknown'} ${app.next_action_at ? `| next ${app.next_action_at}` : ''}</div>
        <div class="chips">
          ${statuses.filter(s => s !== app.status).map(s => `<span class="chip" data-move="${app.job_url}:${s}">${s}</span>`).join('')}
        </div>
      </div>`;
    }
    async function loadBoard(){
      setLoading('loading board...');
      const r = await fetch('/applications?limit=500');
      const data = await r.json();
      const apps = data.applications || [];
      const grouped = Object.fromEntries(statuses.map(s => [s, []]));
      apps.forEach(a => { const st = statuses.includes(a.status) ? a.status : 'saved'; grouped[st].push(a); });
      $('board').innerHTML = statuses.map(s => `<section class="col"><h3>${labels[s]} (${grouped[s].length})</h3>${grouped[s].map(card).join('') || '<div class="meta">empty</div>'}</section>`).join('');
      document.querySelectorAll('[data-pick]').forEach(el => {
        el.onchange = () => { const u=el.dataset.pick; if (el.checked) selected.add(u); else selected.delete(u); refreshCount(); };
      });
      document.querySelectorAll('[data-move]').forEach(el => {
        el.onclick = async () => {
          const [url, st] = el.dataset.move.split(':');
          await fetch('/applications', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ job_url:url, status:st }) });
          toast('Moved');
          await loadBoard();
        };
      });
      refreshCount();
      setLoading('idle');
    }
    async function bulkMove(st){
      const urls = Array.from(selected);
      if (!urls.length) return toast('No selected cards', true);
      setLoading('bulk update...');
      const payload = { items: urls.map(u => ({ job_url: u, status: st })) };
      const r = await fetch('/applications/bulk', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
      const data = await r.json();
      if (!r.ok) return toast(data.message || 'bulk failed', true);
      toast(`Updated ${data.updated}`);
      selected.clear();
      await loadBoard();
    }
    document.querySelectorAll('[data-status]').forEach(el => el.onclick = () => bulkMove(el.dataset.status));
    $('clear').onclick = () => { selected.clear(); refreshCount(); loadBoard(); };
    loadBoard();
  </script>
</body>
</html>"""
