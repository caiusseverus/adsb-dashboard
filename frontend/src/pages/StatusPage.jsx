import { useState, useEffect, useCallback } from 'react'
import styles from './StatusPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const TABLE_DESCRIPTIONS = {
  minute_stats:           { label: 'Minute stats',          desc: 'Per-minute aggregate receiver metrics (msg rate, aircraft count, signal)' },
  minute_df_counts:       { label: 'DF counts / minute',    desc: 'Per-minute breakdown of Mode-S downlink format message counts' },
  minute_type_counts:     { label: 'Type counts / minute',  desc: 'Per-minute aircraft count by ICAO type code' },
  minute_operator_counts: { label: 'Operator counts / min', desc: 'Per-minute aircraft count by operator' },
  daily_aircraft_seen:    { label: 'Daily aircraft seen',   desc: 'One row per aircraft per day; used to compute unique daily counts' },
  day_stats:              { label: 'Day stats',             desc: 'Daily rollup of aggregate metrics; kept permanently' },
  aircraft_registry:      { label: 'Aircraft registry',     desc: 'All aircraft ever seen; enrichment data, flags; kept permanently' },
  coverage_samples:       { label: 'Coverage samples',      desc: 'Per-minute range/bearing/altitude samples; used for polar plot and range charts' },
  acas_events:            { label: 'ACAS events',           desc: 'Decoded TCAS/ACAS Resolution Advisory events' },
}

function fmtBytes(n) {
  if (n == null) return '—'
  if (n < 1024) return `${n} B`
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`
  return `${(n / 1024 ** 3).toFixed(2)} GB`
}

function fmtRows(n) {
  return n?.toLocaleString() ?? '—'
}

function fmtTs(ts) {
  if (!ts) return '—'
  // Could be a Unix timestamp (integer) or a date string
  if (typeof ts === 'string') return ts
  return new Date(ts * 1000).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
}

function RetentionBadge({ expires, retainDays }) {
  if (!expires) {
    return <span className={styles.permanent}>Permanent</span>
  }
  return <span className={styles.expiring}>{retainDays}d retention</span>
}

export default function StatusPage() {
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    fetch(`${API_BASE}/api/status`)
      .then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(d => { setStatus(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [])

  if (loading) return <main className={styles.main}><div className={styles.empty}>Loading…</div></main>
  if (error)   return <main className={styles.main}><div className={styles.empty}>Error: {error}</div></main>
  if (!status) return null

  const { db_size_bytes, tables, config: cfg } = status

  return (
    <main className={styles.main}>
      {/* Summary header */}
      <div className={styles.summaryRow}>
        <div className={styles.summaryCard}>
          <div className={styles.summaryLabel}>Database size</div>
          <div className={styles.summaryValue}>{fmtBytes(db_size_bytes)}</div>
        </div>
        <div className={styles.summaryCard}>
          <div className={styles.summaryLabel}>Total aircraft</div>
          <div className={styles.summaryValue}>
            {fmtRows(tables.find(t => t.table === 'aircraft_registry')?.rows)}
          </div>
        </div>
        <div className={styles.summaryCard}>
          <div className={styles.summaryLabel}>ACAS events</div>
          <div className={styles.summaryValue}>
            {fmtRows(tables.find(t => t.table === 'acas_events')?.rows)}
          </div>
        </div>
        <div className={styles.summaryCard}>
          <div className={styles.summaryLabel}>Coverage samples</div>
          <div className={styles.summaryValue}>
            {fmtRows(tables.find(t => t.table === 'coverage_samples')?.rows)}
          </div>
        </div>
      </div>

      {/* Table breakdown */}
      <div className={styles.card}>
        <div className={styles.cardHeader}>
          <span className={styles.cardTitle}>Storage breakdown by table</span>
        </div>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Table</th>
              <th>Description</th>
              <th className={styles.num}>Rows</th>
              <th>Oldest record</th>
              <th>Newest record</th>
              <th>Retention</th>
            </tr>
          </thead>
          <tbody>
            {tables.map(t => {
              const meta = TABLE_DESCRIPTIONS[t.table] ?? { label: t.table, desc: '' }
              return (
                <tr key={t.table}>
                  <td className={styles.tableName}>{meta.label}</td>
                  <td className={styles.desc}>{meta.desc}</td>
                  <td className={styles.num}>{fmtRows(t.rows)}</td>
                  <td className={styles.muted}>{fmtTs(t.oldest)}</td>
                  <td className={styles.muted}>{fmtTs(t.newest)}</td>
                  <td><RetentionBadge expires={t.expires} retainDays={t.retain_days} /></td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Aircraft debug lookup */}
      <AircraftDebug />

      {/* Config */}
      <div className={styles.card}>
        <div className={styles.cardHeader}>
          <span className={styles.cardTitle}>Active configuration</span>
        </div>
        <div className={styles.configGrid}>
          <ConfigRow label="Minute stats retention"  value={`${cfg.minute_stats_retention_days} days`} />
          <ConfigRow label="Coverage retention"      value={`${cfg.coverage_retention_days} days`} />
          <ConfigRow label="ACAS events retention"   value={`${cfg.acas_retention_days} days`} />
          <ConfigRow label="Ghost filter threshold"  value={cfg.ghost_filter_msgs > 0 ? `${cfg.ghost_filter_msgs} messages` : 'Disabled'} />
          <ConfigRow label="Rare type threshold"     value={`≤ ${cfg.rare_threshold} aircraft of type`} />
        </div>
      </div>
    </main>
  )
}

function ConfigRow({ label, value }) {
  return (
    <div className={styles.configRow}>
      <span className={styles.configLabel}>{label}</span>
      <span className={styles.configValue}>{value}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Aircraft debug lookup
// ---------------------------------------------------------------------------

const DEBUG_FIELDS = [
  { key: 'country',       label: 'Country' },
  { key: 'registration',  label: 'Registration' },
  { key: 'type_code',     label: 'Type code' },
  { key: 'operator',      label: 'Operator' },
  { key: 'manufacturer',  label: 'Manufacturer' },
  { key: 'year',          label: 'Year' },
  { key: 'military',      label: 'Military' },
]

// Map each source's field names to the canonical field keys
function extractField(source, sourceKey, field) {
  if (!source || Object.keys(source).length === 0) return null
  if (sourceKey === 'icao_block') {
    return field === 'country' ? (source.country || null) : null
  }
  if (sourceKey === 'adsbexchange') {
    const map = { country: null, registration: source.reg, type_code: source.icaotype,
      operator: source.ownop, manufacturer: source.manufacturer, year: source.year,
      military: source.mil != null ? (source.mil ? 'Yes' : 'No') : null }
    return map[field] ?? null
  }
  if (sourceKey === 'hexdb') {
    const map = { country: source.Country, registration: source.Registration,
      type_code: source.ICAOTypeCode, operator: source.RegisteredOwners,
      manufacturer: null, year: null, military: null }
    return map[field] ?? null
  }
  if (sourceKey === 'tar1090') {
    const map = { country: null, registration: source.Registration,
      type_code: source.ICAOTypeCode, operator: source.RegisteredOwners,
      manufacturer: null, year: null, military: null }
    return map[field] ?? null
  }
  if (sourceKey === 'registry') {
    const v = source[field]
    if (field === 'military') return v != null ? (v ? 'Yes' : 'No') : null
    return v ?? null
  }
  return null
}

const SOURCES = [
  { key: 'icao_block',   label: 'ICAO block',    overrideable: true  },
  { key: 'adsbexchange', label: 'ADSBExchange',  overrideable: true  },
  { key: 'hexdb',        label: 'hexdb.io',       overrideable: true  },
  { key: 'tar1090',      label: 'tar1090-db',     overrideable: true  },
  { key: 'registry',     label: 'Registry (current)', overrideable: false },
]

function AircraftDebug() {
  const [input,    setInput]    = useState('')
  const [result,   setResult]   = useState(null)
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState(null)
  const [override, setOverride] = useState(null)  // {field, value, status}

  const lookup = useCallback(async () => {
    const icao = input.trim().toUpperCase()
    if (!/^[0-9A-F]{6}$/.test(icao)) {
      setError('Enter a 6-character hex ICAO (e.g. 3C6444)')
      return
    }
    setLoading(true); setError(null); setResult(null); setOverride(null)
    try {
      const r = await fetch(`${API_BASE}/api/debug/aircraft/${icao}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      setResult(await r.json())
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [input])

  const applyOverride = useCallback(async (field, value) => {
    if (!result) return
    setOverride({ field, value, status: 'saving' })
    try {
      const r = await fetch(`${API_BASE}/api/debug/aircraft/${result.icao}/override`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ field, value }),
      })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      setOverride({ field, value, status: 'ok' })
      // Refresh registry row
      const fresh = await fetch(`${API_BASE}/api/debug/aircraft/${result.icao}`)
      if (fresh.ok) setResult(await fresh.json())
    } catch (e) {
      setOverride({ field, value, status: 'error', msg: String(e) })
    }
  }, [result])

  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.cardTitle}>Aircraft data source debug</span>
      </div>
      <div style={{ display: 'flex', gap: '0.6rem', marginBottom: '1.25rem', alignItems: 'center' }}>
        <input
          className={styles.icaoInput}
          placeholder="ICAO hex (e.g. 3C6444)"
          value={input}
          onChange={e => setInput(e.target.value.toUpperCase().slice(0, 6))}
          onKeyDown={e => e.key === 'Enter' && lookup()}
          maxLength={6}
          spellCheck={false}
        />
        <button className={styles.lookupBtn} onClick={lookup} disabled={loading}>
          {loading ? 'Looking up…' : 'Look up'}
        </button>
      </div>

      {error && <div className={styles.debugError}>{error}</div>}

      {override?.status === 'ok' && (
        <div className={styles.debugOk}>
          Override saved: {override.field} = {String(override.value)}
        </div>
      )}
      {override?.status === 'error' && (
        <div className={styles.debugError}>Override failed: {override.msg}</div>
      )}

      {result && (
        <div style={{ overflowX: 'auto' }}>
          <table className={styles.debugTable}>
            <thead>
              <tr>
                <th className={styles.debugFieldCol}>Field</th>
                {SOURCES.map(s => <th key={s.key}>{s.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {DEBUG_FIELDS.map(({ key, label }) => (
                <tr key={key}>
                  <td className={styles.debugFieldName}>{label}</td>
                  {SOURCES.map(src => {
                    const val = extractField(result[src.key], src.key, key)
                    const isRegistry = src.key === 'registry'
                    const canOverride = src.overrideable && val && !isRegistry
                    return (
                      <td key={src.key}
                        className={`${styles.debugCell} ${isRegistry ? styles.debugRegistry : ''}`}
                        title={canOverride ? `Click to apply "${val}" to registry` : undefined}
                        style={{ cursor: canOverride ? 'pointer' : 'default' }}
                        onClick={canOverride ? () => applyOverride(key, val) : undefined}
                      >
                        {val != null
                          ? <span className={canOverride ? styles.debugApplyable : undefined}>{String(val)}</span>
                          : <span className={styles.debugMissing}>—</span>
                        }
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ marginTop: '0.6rem', fontSize: '0.72rem', color: '#484f58' }}>
            Click a value in any source column to write it to the registry.
          </div>
        </div>
      )}
    </div>
  )
}
