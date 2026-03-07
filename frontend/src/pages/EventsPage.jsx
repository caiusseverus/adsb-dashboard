import { useState, useEffect, useMemo, Fragment } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, CartesianGrid,
  LineChart, Line, ReferenceLine, ResponsiveContainer, Legend,
} from 'recharts'
import styles from './EventsPage.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

function fmtTs(unix) {
  if (!unix) return '—'
  return new Date(unix * 1000).toLocaleString()
}

function fmtAlt(alt) {
  if (alt == null) return '—'
  return alt.toLocaleString() + ' ft'
}

function fmtTime(ts) {
  const d = new Date(ts * 1000)
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

const RA_FILTERS = [
  { value: 'all',        label: 'All' },
  { value: 'corrective', label: 'Corrective' },
  { value: 'preventive', label: 'Preventive' },
]

const TIMEFRAMES = [
  { value: 7,  label: '7d' },
  { value: 30, label: '30d' },
  { value: 90, label: '90d' },
]

const COLUMNS = [
  { key: 'ts',              label: 'Time' },
  { key: 'icao',            label: 'ICAO' },
  { key: 'registration',    label: 'Reg' },
  { key: 'type_code',       label: 'Type' },
  { key: 'operator',        label: 'Operator' },
  { key: 'country',         label: 'Country' },
  { key: 'ra_description',  label: 'RA' },
  { key: 'threat_icao',     label: 'Threat' },
  { key: 'threat_reg',      label: 'Thr Reg' },
  { key: 'threat_type_code', label: 'Thr Type' },
  { key: 'altitude',        label: 'Alt' },
  { key: 'sensitivity_level', label: 'SL' },
  { key: 'mte',             label: 'MTE' },
]

function sortRows(rows, col, asc) {
  return [...rows].sort((a, b) => {
    let av = a[col], bv = b[col]
    if (av == null && bv == null) return 0
    if (av == null) return 1
    if (bv == null) return -1
    const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv
    return asc ? cmp : -cmp
  })
}

function RaBadge({ ev }) {
  const cls = ev.ra_corrective ? styles.correctiveBadge : styles.preventiveBadge
  return <span className={cls}>{ev.ra_description}</span>
}

function ContextRow({ event, onClose }) {
  const [ctx, setCtx] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch(`${API_BASE}/api/acas/context/${event.id}`)
      .then(r => r.ok ? r.json() : {})
      .then(d => { setCtx(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [event.id])

  const tracks = ctx?.tracks ?? []
  const ownTracks    = tracks.filter(t => t.icao === event.icao)
  const threatTracks = tracks.filter(t => t.icao === event.threat_icao)

  // Build timeline by merging timestamps
  const allTs = [...new Set(tracks.map(t => t.ts))].sort((a, b) => a - b)
  const chartData = allTs.map(ts => {
    const own    = ownTracks.find(t => t.ts === ts)
    const threat = threatTracks.find(t => t.ts === ts)
    return {
      ts,
      label: fmtTime(ts),
      own:    own?.altitude    ?? null,
      threat: threat?.altitude ?? null,
    }
  })

  return (
    <tr>
      <td colSpan={COLUMNS.length} className={styles.contextRow}>
        <div className={styles.contextInner}>
          <button className={styles.contextClose} onClick={onClose}>✕ Close</button>
          <div className={styles.contextMeta}>
            <span>{fmtTs(event.ts)}</span>
            <RaBadge ev={event} />
            {event.threat_icao && <span>vs {event.threat_reg || event.threat_icao}</span>}
            {event.altitude != null && <span>{fmtAlt(event.altitude)}</span>}
          </div>

          {loading ? (
            <div className={styles.contextEmpty}>Loading track data…</div>
          ) : chartData.length === 0 ? (
            <div className={styles.contextEmpty}>No altitude track data for this window.</div>
          ) : (
            <div className={styles.contextChart}>
              <ResponsiveContainer width="100%" height={180}>
                <LineChart data={chartData} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
                  <CartesianGrid stroke="#21262d" strokeDasharray="3 3" />
                  <XAxis dataKey="label" tick={{ fill: '#484f58', fontSize: 10 }} />
                  <YAxis tick={{ fill: '#484f58', fontSize: 10 }}
                         tickFormatter={v => v != null ? `${(v / 1000).toFixed(0)}k` : ''} />
                  <Tooltip
                    contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
                    formatter={(v, name) => [v != null ? fmtAlt(v) : '—', name]}
                  />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <ReferenceLine x={fmtTime(event.ts)} stroke="#d29922" strokeDasharray="4 2" label={{ value: 'RA', fill: '#d29922', fontSize: 10 }} />
                  <Line type="monotone" dataKey="own" name={event.icao}
                    stroke="#388bfd" dot={false} connectNulls strokeWidth={2} />
                  {event.threat_icao && (
                    <Line type="monotone" dataKey="threat" name={event.threat_icao}
                      stroke="#f85149" dot={false} connectNulls strokeWidth={2} />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      </td>
    </tr>
  )
}

function StatsPanel({ days }) {
  const [timeline, setTimeline] = useState([])
  const [stats, setStats]       = useState(null)

  useEffect(() => {
    fetch(`${API_BASE}/api/acas/timeline?days=${days}`)
      .then(r => r.ok ? r.json() : [])
      .then(setTimeline)
      .catch(() => {})

    fetch(`${API_BASE}/api/acas/stats?days=${days}`)
      .then(r => r.ok ? r.json() : null)
      .then(setStats)
      .catch(() => {})
  }, [days])

  return (
    <div className={styles.statsGrid}>
      {/* Timeline bar chart */}
      <div className={styles.statsCard}>
        <div className={styles.statsTitle}>Events per day</div>
        {timeline.length === 0 ? (
          <div className={styles.statsEmpty}>No events recorded yet.</div>
        ) : (
          <ResponsiveContainer width="100%" height={140}>
            <BarChart data={timeline} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
              <CartesianGrid stroke="#21262d" strokeDasharray="3 3" />
              <XAxis dataKey="day" tick={{ fill: '#484f58', fontSize: 9 }}
                     tickFormatter={d => d?.slice(5)} />
              <YAxis tick={{ fill: '#484f58', fontSize: 9 }} allowDecimals={false} />
              <Tooltip contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 11 }} />
              <Bar dataKey="corrective" stackId="s" fill="#f85149" name="Corrective" />
              <Bar dataKey="preventive" stackId="s" fill="#bc8cff" name="Preventive" />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Stats breakdowns */}
      <div className={styles.statsCard}>
        <div className={styles.statsTitle}>
          {stats ? `${stats.total} events — ${stats.corrective} corrective, ${stats.preventive} preventive` : 'Loading…'}
        </div>
        {stats && (
          <div className={styles.statsSub}>
            {/* By country */}
            {stats.by_country.length > 0 && (
              <div className={styles.miniChart}>
                <div className={styles.miniChartTitle}>By country</div>
                <ResponsiveContainer width="100%" height={80}>
                  <BarChart data={stats.by_country} layout="vertical"
                            margin={{ top: 2, right: 8, bottom: 2, left: 55 }}>
                    <XAxis type="number" tick={{ fill: '#484f58', fontSize: 9 }} allowDecimals={false} />
                    <YAxis type="category" dataKey="country" tick={{ fill: '#8b949e', fontSize: 9 }} width={55} />
                    <Tooltip contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 11 }} />
                    <Bar dataKey="count" fill="#388bfd" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
            {/* By operator */}
            {stats.by_operator.length > 0 && (
              <div className={styles.miniChart}>
                <div className={styles.miniChartTitle}>By operator (top 10)</div>
                <ResponsiveContainer width="100%" height={80}>
                  <BarChart data={stats.by_operator} layout="vertical"
                            margin={{ top: 2, right: 8, bottom: 2, left: 80 }}>
                    <XAxis type="number" tick={{ fill: '#484f58', fontSize: 9 }} allowDecimals={false} />
                    <YAxis type="category" dataKey="operator" tick={{ fill: '#8b949e', fontSize: 9 }} width={80}
                           tickFormatter={op => formatOperator(op)?.slice(0, 14) ?? op} />
                    <Tooltip contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 11 }} />
                    <Bar dataKey="count" fill="#3fb950" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

export default function EventsPage({ onSelectIcao }) {
  const [days, setDays]         = useState(30)
  const [raFilter, setRaFilter] = useState('all')
  const [events, setEvents]     = useState([])
  const [loading, setLoading]   = useState(true)
  const [sortCol, setSortCol]   = useState('ts')
  const [sortAsc, setSortAsc]   = useState(false)
  const [expanded, setExpanded] = useState(null)  // event id

  useEffect(() => {
    setLoading(true)
    fetch(`${API_BASE}/api/acas/events?days=${days}&limit=200`)
      .then(r => r.ok ? r.json() : [])
      .then(d => { setEvents(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [days])

  function handleSort(key) {
    if (key === sortCol) setSortAsc(v => !v)
    else { setSortCol(key); setSortAsc(key === 'ts' ? false : true) }
  }

  const filtered = useMemo(() => {
    if (raFilter === 'corrective') return events.filter(e => e.ra_corrective)
    if (raFilter === 'preventive') return events.filter(e => !e.ra_corrective)
    return events
  }, [events, raFilter])

  const sorted = useMemo(() => sortRows(filtered, sortCol, sortAsc), [filtered, sortCol, sortAsc])

  function toggleExpand(ev) {
    setExpanded(prev => prev === ev.id ? null : ev.id)
  }

  return (
    <main className={styles.page}>
      {/* Limitations notice */}
      <details className={styles.notice}>
        <summary>About ACAS/TCAS event data</summary>
        <p>
          Reception of DF16 (air-to-air surveillance) by a ground receiver is opportunistic —
          it depends on antenna geometry between the two aircraft. Captured events are a lower
          bound on actual TCAS activations in the coverage area. Track data requires both
          aircraft to have been visible within <code>coverage_samples</code> in the event
          window (1-minute resolution).
        </p>
      </details>

      <StatsPanel days={days} />

      {/* Events table */}
      <div className={styles.tableCard}>
        <div className={styles.tableHeader}>
          <span className={styles.tableTitle}>ACAS Events</span>
          <span className={styles.count}>{filtered.length}</span>
          <div className={styles.controls}>
            {RA_FILTERS.map(f => (
              <button key={f.value}
                className={raFilter === f.value ? styles.btnActive : styles.btn}
                onClick={() => setRaFilter(f.value)}>{f.label}</button>
            ))}
            <div className={styles.sep} />
            {TIMEFRAMES.map(t => (
              <button key={t.value}
                className={days === t.value ? styles.btnActive : styles.btn}
                onClick={() => setDays(t.value)}>{t.label}</button>
            ))}
          </div>
        </div>

        {loading ? (
          <div className={styles.empty}>Loading…</div>
        ) : sorted.length === 0 ? (
          <div className={styles.empty}>
            No ACAS events recorded yet. Events are captured from DF16 messages — reception
            depends on geometry and may take time or never occur depending on your location.
          </div>
        ) : (
          <div className={styles.scrollWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  {COLUMNS.map(({ key, label }) => (
                    <th key={key} className={styles.sortable} onClick={() => handleSort(key)}>
                      {label}
                      <span className={styles.sortIcon}>
                        {sortCol === key ? (sortAsc ? ' ▲' : ' ▼') : ' ⇅'}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map(ev => (
                  <Fragment key={ev.id}>
                    <tr
                      className={`${styles.eventRow} ${ev.ra_corrective ? styles.corrRow : styles.prevRow} ${expanded === ev.id ? styles.expandedRow : ''}`}
                      onClick={() => toggleExpand(ev)}
                    >
                      <td className={styles.muted}>{fmtTs(ev.ts)}</td>
                      <td className={styles.icao} onClick={e => { e.stopPropagation(); onSelectIcao?.(ev.icao) }}>
                        {ev.icao}
                      </td>
                      <td>{ev.registration ?? '—'}</td>
                      <td>{ev.type_code ?? '—'}</td>
                      <td className={styles.operator} title={formatOperator(ev.operator) ?? undefined}>
                        {formatOperator(ev.operator) ?? '—'}
                      </td>
                      <td>{ev.country ?? '—'}</td>
                      <td><RaBadge ev={ev} /></td>
                      <td>
                        {ev.threat_icao
                          ? <span className={styles.threatIcao}
                              onClick={e => { e.stopPropagation(); onSelectIcao?.(ev.threat_icao) }}>
                              {ev.threat_icao}
                            </span>
                          : '—'
                        }
                      </td>
                      <td>{ev.threat_reg ?? '—'}</td>
                      <td>{ev.threat_type_code ?? '—'}</td>
                      <td>{fmtAlt(ev.altitude)}</td>
                      <td>{ev.sensitivity_level ?? '—'}</td>
                      <td>{ev.mte ? <span className={styles.mteBadge}>MTE</span> : '—'}</td>
                    </tr>
                    {expanded === ev.id && (
                      <ContextRow event={ev} onClose={() => setExpanded(null)} />
                    )}
                  </Fragment>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </main>
  )
}
