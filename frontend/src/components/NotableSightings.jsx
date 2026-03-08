import { useState, useEffect, useMemo } from 'react'
import styles from './NotableSightings.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

function flagScore(ac) {
  return (ac.foreign_military ? 8 : 0) + (ac.interesting ? 4 : 0) +
         (ac.rare ? 2 : 0) + (ac.first_seen_flag || ac.sighting_count === 1 ? 1 : 0)
}

const COLUMNS = [
  { key: 'icao',          label: 'ICAO',       cmp: (a, b) => a.icao.localeCompare(b.icao) },
  { key: 'registration',  label: 'Reg',        cmp: (a, b) => (a.registration ?? '').localeCompare(b.registration ?? '') },
  { key: 'type_code',     label: 'Type',       cmp: (a, b) => (a.type_code ?? '').localeCompare(b.type_code ?? '') },
  { key: 'operator',      label: 'Operator',   cmp: (a, b) => (formatOperator(a.operator) ?? '').localeCompare(formatOperator(b.operator) ?? '') },
  { key: 'year',          label: 'Year',       cmp: (a, b) => (a.year ?? '').localeCompare(b.year ?? '') },
  { key: 'country',       label: 'Country',    cmp: (a, b) => (a.country ?? '').localeCompare(b.country ?? '') },
  { key: 'flags',         label: 'Flags',      cmp: (a, b) => flagScore(b) - flagScore(a) },
  { key: 'first_seen',    label: 'First seen', cmp: (a, b) => (a.first_seen ?? 0) - (b.first_seen ?? 0) },
  { key: 'last_seen',     label: 'Last seen',  cmp: (a, b) => (a.last_seen ?? 0) - (b.last_seen ?? 0) },
  { key: 'sighting_count',label: 'Sessions',   cmp: (a, b) => (a.sighting_count ?? 0) - (b.sighting_count ?? 0) },
]

const FLAGS = [
  { value: 'all',              label: 'All notable' },
  { value: 'foreign_military', label: 'Foreign military' },
  { value: 'home_military',    label: 'Home military' },
  { value: 'interesting',      label: 'Interesting' },
  { value: 'rare',             label: 'Rare' },
  { value: 'unique_sighting',  label: 'Unique' },
]

const TIMEFRAMES = [
  { value: 1,    label: '24h' },
  { value: 7,    label: '7d' },
  { value: 30,   label: '30d' },
  { value: 90,   label: '90d' },
  { value: null, label: 'All' },
]

function fmtTime(ts) {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  })
}

function FlagBadge({ label, color }) {
  return (
    <span className={styles.badge} style={{ background: `${color}22`, color }}>
      {label}
    </span>
  )
}

function AircraftRow({ ac, onSelectIcao }) {
  const flags = []
  if (ac.foreign_military) flags.push(<FlagBadge key="fm"      label="Foreign Military" color="#f85149" />)
  if (ac.interesting)      flags.push(<FlagBadge key="int"     label="Interesting"      color="#d29922" />)
  if (ac.rare)             flags.push(<FlagBadge key="rare"    label="Rare"             color="#bc8cff" />)
  if (ac.sighting_count === 1 || ac.first_seen_flag)
                           flags.push(<FlagBadge key="unique"  label="Unique"           color="#3fb950" />)

  return (
    <tr
      className={`${ac.military ? styles.militaryRow : ''} ${styles.clickable}`}
      onClick={() => onSelectIcao?.(ac.icao)}
    >
      <td className={styles.icao}>{ac.icao}</td>
      <td>{ac.registration ?? '—'}</td>
      <td title={[ac.type_code, ac.type_desc].filter(Boolean).join(' · ') || undefined}>
        {ac.type_code ?? '—'}
        {ac.type_count != null && (
          <span className={styles.typeCount}>×{ac.type_count}</span>
        )}
      </td>
      <td className={styles.operator} title={formatOperator(ac.operator) ?? undefined}>{formatOperator(ac.operator) ?? '—'}</td>
      <td>{ac.year ?? '—'}</td>
      <td>{ac.country ?? '—'}</td>
      <td>{flags}</td>
      <td className={styles.muted}>{fmtTime(ac.first_seen)}</td>
      <td className={styles.muted}>{fmtTime(ac.last_seen)}</td>
      <td className={styles.num}>{ac.sighting_count}</td>
    </tr>
  )
}

export default function NotableSightings({ onSelectIcao, refreshKey }) {
  const [flag, setFlag] = useState('all')
  const [days, setDays] = useState(30)
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(true)
  const [sortKey, setSortKey] = useState('last_seen')
  const [sortAsc, setSortAsc] = useState(false)

  useEffect(() => {
    setLoading(true)
    const daysParam = days != null ? `&days=${days}` : ''
    fetch(`${API_BASE}/api/history/notable?flag=${flag}&limit=200${daysParam}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [flag, days, refreshKey])

  function handleSort(key) {
    if (key === sortKey) setSortAsc(v => !v)
    else { setSortKey(key); setSortAsc(key === 'last_seen' ? false : true) }
  }

  const sorted = useMemo(() => {
    const col = COLUMNS.find(c => c.key === sortKey)
    if (!col) return data
    return [...data].sort((a, b) => sortAsc ? col.cmp(a, b) : col.cmp(b, a))
  }, [data, sortKey, sortAsc])

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <div className={styles.titleRow}>
          <span className={styles.heading}>Notable Sightings</span>
          <span className={styles.count}>{data.length}</span>
        </div>
        <div className={styles.controls}>
          {FLAGS.map(f => (
            <button
              key={f.value}
              className={flag === f.value ? styles.btnActive : styles.btn}
              onClick={() => setFlag(f.value)}
            >{f.label}</button>
          ))}
          <div className={styles.sep} />
          {TIMEFRAMES.map(t => (
            <button
              key={String(t.value)}
              className={days === t.value ? styles.btnActive : styles.btn}
              onClick={() => setDays(t.value)}
            >{t.label}</button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : data.length === 0 ? (
        <div className={styles.empty}>No notable aircraft recorded yet.</div>
      ) : (
        <div className={styles.scrollWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                {COLUMNS.map(({ key, label }) => (
                  <th key={key} className={styles.sortable} onClick={() => handleSort(key)}>
                    {label}
                    <span className={styles.sortIcon}>
                      {sortKey === key ? (sortAsc ? ' ▲' : ' ▼') : ' ⇅'}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sorted.map(ac => <AircraftRow key={ac.icao} ac={ac} onSelectIcao={onSelectIcao} />)}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
