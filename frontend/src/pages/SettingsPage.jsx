import { useState, useEffect, useCallback } from 'react'
import styles from './SettingsPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const DATE_FMT = { day: 'numeric', month: 'short', year: 'numeric' }
function fmtTs(ts) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleDateString('en-GB', DATE_FMT)
}
function fmtBytes(n) {
  if (n == null) return '—'
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(0)} KB`
  return `${(n / 1024 ** 2).toFixed(1)} MB`
}

// ---------------------------------------------------------------------------
// Notification trigger row
// ---------------------------------------------------------------------------
function TriggerRow({ label, prefKey, rangeKey, prefs, onChange }) {
  const enabled = prefs[prefKey] === 'true'
  const rangeVal = prefs[rangeKey] ?? ''

  return (
    <div className={styles.triggerRow}>
      <label className={styles.toggle}>
        <input
          type="checkbox"
          checked={enabled}
          onChange={e => onChange(prefKey, e.target.checked ? 'true' : 'false')}
        />
        <span className={styles.toggleLabel}>{label}</span>
      </label>
      {rangeKey && (
        <div className={styles.rangeGroup}>
          <span className={styles.rangeLabel}>within</span>
          <input
            type="number"
            className={styles.rangeInput}
            placeholder="any"
            min="0"
            step="1"
            value={rangeVal}
            onChange={e => onChange(rangeKey, e.target.value)}
            disabled={!enabled}
          />
          <span className={styles.rangeUnit}>nm</span>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Triggers section
// ---------------------------------------------------------------------------
function TriggersSection() {
  const [prefs, setPrefs] = useState({})
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    fetch(`${API_BASE}/api/notify/prefs`)
      .then(r => r.ok ? r.json() : {})
      .then(setPrefs)
      .catch(() => {})
  }, [])

  const handleChange = useCallback(async (key, value) => {
    setPrefs(p => ({ ...p, [key]: value }))
    setSaving(true)
    try {
      await fetch(`${API_BASE}/api/notify/prefs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, value }),
      })
    } finally {
      setSaving(false)
    }
  }, [])

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.cardTitle}>Notification triggers</span>
        {saving && <span className={styles.saving}>Saving…</span>}
      </div>
      <p className={styles.hint}>
        Leave range blank to notify regardless of distance.
        Requires ntfy and/or email to be configured in <code>.env</code>.
      </p>
      <div className={styles.triggerList}>
        <TriggerRow label="Emergency squawk (7700/7600/7500)" prefKey="notify_emergency"  rangeKey=""                        prefs={prefs} onChange={handleChange} />
        <TriggerRow label="ACAS/TCAS resolution advisory"    prefKey="notify_acas"        rangeKey="acas_max_range_nm"        prefs={prefs} onChange={handleChange} />
        <TriggerRow label="Military aircraft"                prefKey="notify_military"     rangeKey="military_max_range_nm"    prefs={prefs} onChange={handleChange} />
        <TriggerRow label="Interesting aircraft"             prefKey="notify_interesting"  rangeKey="interesting_max_range_nm" prefs={prefs} onChange={handleChange} />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Watchlist section
// ---------------------------------------------------------------------------
function WatchlistSection() {
  const [list, setList]       = useState([])
  const [loading, setLoading] = useState(true)
  const [adding, setAdding]   = useState(false)
  const [newIcao, setNewIcao] = useState('')
  const [newRange, setNewRange] = useState('')
  const [error, setError]     = useState(null)

  const load = useCallback(() => {
    setLoading(true)
    fetch(`${API_BASE}/api/notify/watchlist`)
      .then(r => r.ok ? r.json() : [])
      .then(d => { setList(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  useEffect(load, [load])

  const add = async () => {
    const icao = newIcao.trim().toUpperCase()
    if (!/^[0-9A-F]{6}$/.test(icao)) {
      setError('Enter a valid 6-character hex ICAO')
      return
    }
    setAdding(true); setError(null)
    try {
      const r = await fetch(`${API_BASE}/api/notify/watchlist/${icao}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ max_range_nm: newRange ? parseFloat(newRange) : null }),
      })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      setNewIcao(''); setNewRange('')
      load()
    } catch (e) {
      setError(String(e))
    } finally {
      setAdding(false)
    }
  }

  const remove = async (icao) => {
    await fetch(`${API_BASE}/api/notify/watchlist/${icao}`, { method: 'DELETE' })
    load()
  }

  const updateRange = async (icao, label, max_range_nm) => {
    await fetch(`${API_BASE}/api/notify/watchlist/${icao}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label, max_range_nm: max_range_nm ? parseFloat(max_range_nm) : null }),
    })
  }

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.cardTitle}>Watchlist</span>
        <span className={styles.count}>{list.length}</span>
      </div>
      <p className={styles.hint}>
        Individual aircraft to always notify on. Add ICAOs here or use the Watch button
        in the aircraft detail panel. Leave range blank to notify at any distance.
      </p>

      <div className={styles.addRow}>
        <input
          className={styles.icaoInput}
          placeholder="ICAO hex"
          value={newIcao}
          onChange={e => setNewIcao(e.target.value.toUpperCase().slice(0, 6))}
          onKeyDown={e => e.key === 'Enter' && add()}
          maxLength={6}
          spellCheck={false}
        />
        <input
          type="number"
          className={styles.rangeInput}
          placeholder="range (nm, blank=any)"
          min="0"
          step="1"
          value={newRange}
          onChange={e => setNewRange(e.target.value)}
        />
        <button className={styles.addBtn} onClick={add} disabled={adding}>
          {adding ? 'Adding…' : 'Add'}
        </button>
        {error && <span className={styles.errorMsg}>{error}</span>}
      </div>

      {loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : list.length === 0 ? (
        <div className={styles.empty}>No aircraft on the watchlist.</div>
      ) : (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>ICAO</th>
              <th>Reg</th>
              <th>Type</th>
              <th>Operator</th>
              <th>Country</th>
              <th>Added</th>
              <th>Range limit</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {list.map(ac => (
              <WatchlistRow key={ac.icao} ac={ac} onRemove={remove} onUpdateRange={updateRange} />
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function WatchlistRow({ ac, onRemove, onUpdateRange }) {
  const [range, setRange] = useState(ac.max_range_nm != null ? String(ac.max_range_nm) : '')

  const handleRangeBlur = () => {
    const val = range.trim()
    onUpdateRange(ac.icao, ac.label, val || null)
  }

  return (
    <tr>
      <td className={styles.icao}>{ac.icao}</td>
      <td>{ac.registration ?? '—'}</td>
      <td>{ac.type_code ?? '—'}</td>
      <td className={styles.operator}>{ac.operator ?? '—'}</td>
      <td>{ac.country ?? '—'}</td>
      <td className={styles.muted}>{fmtTs(ac.added_ts)}</td>
      <td>
        <div className={styles.inlineRange}>
          <input
            type="number"
            className={styles.rangeInputSm}
            placeholder="any"
            min="0"
            step="1"
            value={range}
            onChange={e => setRange(e.target.value)}
            onBlur={handleRangeBlur}
          />
          <span className={styles.rangeUnit}>nm</span>
        </div>
      </td>
      <td>
        <button className={styles.removeBtn} onClick={() => onRemove(ac.icao)}>Remove</button>
      </td>
    </tr>
  )
}

// ---------------------------------------------------------------------------
// Backup section
// ---------------------------------------------------------------------------
function BackupSection() {
  const [prefs, setPrefs]     = useState({ backup_path: '', backup_retain: '7' })
  const [files, setFiles]     = useState(null)   // null = not yet loaded
  const [loading, setLoading] = useState(true)
  const [saving, setSaving]   = useState(false)
  const [running, setRunning] = useState(false)
  const [msg, setMsg]         = useState(null)

  // Local edit state so user can type before saving
  const [editPath, setEditPath]     = useState('')
  const [editRetain, setEditRetain] = useState('')
  const [dirty, setDirty]           = useState(false)

  const loadPrefs = useCallback(() => {
    fetch(`${API_BASE}/api/notify/prefs`)
      .then(r => r.ok ? r.json() : {})
      .then(p => {
        setPrefs(p)
        setEditPath(p.backup_path ?? '')
        setEditRetain(p.backup_retain ?? '7')
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  const loadFiles = useCallback(() => {
    fetch(`${API_BASE}/api/status`)
      .then(r => r.ok ? r.json() : null)
      .then(d => setFiles(d?.backup?.files ?? []))
      .catch(() => setFiles([]))
  }, [])

  useEffect(() => { loadPrefs(); loadFiles() }, [loadPrefs, loadFiles])

  const save = async () => {
    setSaving(true); setMsg(null)
    try {
      await Promise.all([
        fetch(`${API_BASE}/api/notify/prefs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key: 'backup_path', value: editPath.trim() }),
        }),
        fetch(`${API_BASE}/api/notify/prefs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ key: 'backup_retain', value: editRetain.trim() || '7' }),
        }),
      ])
      setDirty(false)
      setMsg({ ok: true, text: 'Settings saved.' })
    } catch (e) {
      setMsg({ ok: false, text: String(e) })
    } finally {
      setSaving(false)
    }
  }

  const runBackup = async () => {
    setRunning(true); setMsg(null)
    try {
      const r = await fetch(`${API_BASE}/api/notify/backup`, { method: 'POST' })
      const d = await r.json()
      if (r.ok) {
        setMsg({ ok: true, text: `Backup written: ${d.path}` })
        loadFiles()
      } else {
        setMsg({ ok: false, text: d.detail ?? 'Backup failed' })
      }
    } catch (e) {
      setMsg({ ok: false, text: String(e) })
    } finally {
      setRunning(false)
    }
  }

  if (loading) return null

  const hasPath = editPath.trim().length > 0

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.cardTitle}>Database backup</span>
        {hasPath
          ? <span className={styles.badge}>Enabled</span>
          : <span className={styles.badgeDim}>Disabled</span>}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: '0.5rem' }}>
          {dirty && (
            <button className={styles.saveBtn} onClick={save} disabled={saving}>
              {saving ? 'Saving…' : 'Save'}
            </button>
          )}
          {hasPath && (
            <button className={styles.backupBtn} onClick={runBackup} disabled={running || dirty}>
              {running ? 'Running…' : 'Backup now'}
            </button>
          )}
        </div>
      </div>

      <p className={styles.hint}>
        Set a backup directory to enable nightly backups at midnight. Leave blank to disable.
        Overrides <code>BACKUP_PATH</code> in <code>.env</code>.
      </p>

      <div className={styles.backupForm}>
        <div className={styles.backupField}>
          <label className={styles.fieldLabel}>Backup directory</label>
          <input
            className={styles.pathInput}
            placeholder="/path/to/backup/dir"
            value={editPath}
            onChange={e => { setEditPath(e.target.value); setDirty(true); setMsg(null) }}
          />
        </div>
        <div className={styles.backupField}>
          <label className={styles.fieldLabel}>Retain (days)</label>
          <input
            type="number"
            className={styles.rangeInput}
            min="1"
            step="1"
            value={editRetain}
            onChange={e => { setEditRetain(e.target.value); setDirty(true); setMsg(null) }}
          />
        </div>
      </div>

      {msg && (
        <div className={msg.ok ? styles.msgOk : styles.msgErr}>{msg.text}</div>
      )}

      {hasPath && files !== null && (
        files.length === 0 ? (
          <div className={styles.empty}>No backups yet — first runs at midnight or click Backup now.</div>
        ) : (
          <table className={styles.table}>
            <thead><tr><th>File</th><th className={styles.num}>Size</th></tr></thead>
            <tbody>
              {files.map(f => (
                <tr key={f.name}>
                  <td>{f.name}</td>
                  <td className={styles.num}>{fmtBytes(f.size_bytes)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Maintenance
// ---------------------------------------------------------------------------
function MaintenanceSection() {
  const [gapMins, setGapMins] = useState(10)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState(null)  // null | { merged, max_gap_mins } | 'error'

  const run = async () => {
    setRunning(true)
    setResult(null)
    try {
      const r = await fetch(`${API_BASE}/api/history/visits/cleanup?max_gap_mins=${gapMins}`, { method: 'POST' })
      if (!r.ok) throw new Error()
      setResult(await r.json())
    } catch {
      setResult('error')
    } finally {
      setRunning(false)
    }
  }

  return (
    <section className={styles.card}>
      <h2 className={styles.cardTitle}>Maintenance</h2>
      <p className={styles.hint}>
        Merge visit records that were split by a backend restart or brief signal loss.
        Visits for the same aircraft with a gap smaller than the threshold and matching
        (or absent) callsigns are joined into a single record.
      </p>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap', marginTop: '0.75rem' }}>
        <label className={styles.rangeLabel} style={{ minWidth: 'unset' }}>Max gap</label>
        <input
          type="number"
          className={styles.rangeInput}
          value={gapMins}
          min={1} max={60}
          onChange={e => setGapMins(Number(e.target.value))}
          style={{ width: '4rem' }}
        />
        <span className={styles.rangeLabel}>minutes</span>
        <button className={styles.saveBtn} onClick={run} disabled={running}>
          {running ? 'Running…' : 'Merge short visits'}
        </button>
        {result === 'error' && <span style={{ color: '#f85149', fontSize: '0.82rem' }}>Failed</span>}
        {result && result !== 'error' && (
          <span style={{ color: result.merged > 0 ? '#3fb950' : '#8b949e', fontSize: '0.82rem' }}>
            {result.merged > 0 ? `${result.merged} visit${result.merged !== 1 ? 's' : ''} merged` : 'Nothing to merge'}
          </span>
        )}
      </div>
    </section>
  )
}

// ---------------------------------------------------------------------------
// Page root
// ---------------------------------------------------------------------------
export default function SettingsPage() {
  return (
    <main className={styles.main}>
      <TriggersSection />
      <WatchlistSection />
      <BackupSection />
      <MaintenanceSection />
    </main>
  )
}
