import { useState, useEffect } from 'react'
import styles from './NotableSightings.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const FLAGS = [
  { value: 'all',              label: 'All notable' },
  { value: 'foreign_military', label: 'Foreign military' },
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

  useEffect(() => {
    setLoading(true)
    const daysParam = days != null ? `&days=${days}` : ''
    fetch(`${API_BASE}/api/history/notable?flag=${flag}&limit=200${daysParam}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [flag, days, refreshKey])

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
                <th>ICAO</th>
                <th>Reg</th>
                <th>Type</th>
                <th>Operator</th>
                <th>Year</th>
                <th>Country</th>
                <th>Flags</th>
                <th>First seen</th>
                <th>Last seen</th>
                <th>Sessions</th>
              </tr>
            </thead>
            <tbody>
              {data.map(ac => <AircraftRow key={ac.icao} ac={ac} onSelectIcao={onSelectIcao} />)}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
