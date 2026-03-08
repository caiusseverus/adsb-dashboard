import { useState } from 'react'
import styles from './AircraftTable.module.css'
import { formatOperator } from '../utils/formatOperator'

const WTC_CLASS = { L: styles.wtcL, M: styles.wtcM, H: styles.wtcH, J: styles.wtcJ }

const EMERGENCY_SQUAWKS = {
  '7700': 'General emergency',
  '7600': 'Radio failure',
  '7500': 'Hijack',
}

function WtcBadge({ wtc }) {
  if (!wtc) return null
  return <span className={`${styles.wtcBadge} ${WTC_CLASS[wtc] ?? ''}`}>{wtc}</span>
}

function fmtTypeTooltip(ac) {
  const parts = []
  if (ac.type_full_name) parts.push(ac.type_full_name)
  if (ac.type_category)  parts.push(ac.type_category)
  if (ac.type_desc && ac.type_desc !== ac.type_full_name) parts.push(ac.type_desc)
  return parts.join(' · ') || undefined
}

function fmtAlt(alt) {
  if (alt == null) return '—'
  return alt.toLocaleString() + ' ft'
}

function fmtAge(age) {
  if (age < 5)  return { label: `${age}s`, fresh: true }
  if (age < 30) return { label: `${age}s`, fresh: false }
  return { label: `${age}s`, fresh: false, stale: true }
}

function SignalBar({ value }) {
  // Beast RSSI byte: 0=strongest, 255=weakest (log scale, -0.5*value dBFS)
  // Convert to 0-100% for display (invert so higher = stronger)
  const pct = value != null ? Math.max(0, Math.min(100, Math.round((255 - value) / 2.55))) : 0
  const colour = pct > 66 ? '#3fb950' : pct > 33 ? '#d29922' : '#f85149'
  return (
    <div className={styles.signalWrap} title={`${pct}%`}>
      <div className={styles.signalBar} style={{ width: `${pct}%`, background: colour }} />
    </div>
  )
}

const COLUMNS = [
  { key: 'icao',         label: 'ICAO' },
  { key: 'registration', label: 'Reg' },
  { key: 'callsign',     label: 'Callsign' },
  { key: 'type_code',    label: 'Type' },
  { key: 'operator',     label: 'Operator' },
  { key: 'year',         label: 'Year' },
  { key: 'country',      label: 'Country' },
  { key: 'altitude',     label: 'Altitude' },
  { key: 'range_nm',     label: 'Range' },
  { key: 'airspeed_kts', label: 'Speed' },
  { key: 'heading_deg',  label: 'Hdg' },
  { key: 'squawk',       label: 'Squawk' },
  { key: 'signal',       label: 'Signal' },
  { key: 'msg_count',    label: 'Msgs' },
  { key: 'age',          label: 'Last seen' },
]

function sortAircraft(aircraft, col, asc) {
  return [...aircraft].sort((a, b) => {
    // Emergencies always pinned to top
    const ae = a.squawk && EMERGENCY_SQUAWKS[a.squawk]
    const be = b.squawk && EMERGENCY_SQUAWKS[b.squawk]
    if (ae && !be) return -1
    if (!ae && be) return 1

    let av = a[col], bv = b[col]
    // nulls last regardless of direction
    if (av == null && bv == null) return 0
    if (av == null) return 1
    if (bv == null) return -1
    // signal: lower raw value = stronger, so invert for intuitive "best first"
    if (col === 'signal') { av = -av; bv = -bv }
    const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv
    return asc ? cmp : -cmp
  })
}

const FILTERS = [
  { value: 'all',         label: 'All' },
  { value: 'military',    label: 'Military' },
  { value: 'interesting', label: 'Interesting' },
  { value: 'acas',        label: 'ACAS' },
]

export default function AircraftTable({ aircraft, onSelectIcao, queueSize = 0 }) {
  const [sortCol, setSortCol] = useState('icao')
  const [sortAsc, setSortAsc] = useState(true)
  const [filter, setFilter] = useState('all')

  function handleSort(key) {
    if (key === sortCol) {
      setSortAsc(v => !v)
    } else {
      setSortCol(key)
      setSortAsc(true)
    }
  }

  const threatSet = new Set(aircraft.filter(a => a.acas_threat_icao).map(a => a.acas_threat_icao))

  const filtered = filter === 'military'    ? aircraft.filter(ac => ac.military)
                 : filter === 'interesting' ? aircraft.filter(ac => ac.interesting)
                 : filter === 'acas'        ? aircraft.filter(ac => ac.acas_ra_active || threatSet.has(ac.icao))
                 : aircraft

  const sorted = sortAircraft(filtered, sortCol, sortAsc)

  return (
    <div className={styles.container}>
      <div className={styles.heading}>
        Live Aircraft
        <span className={styles.count}>{filtered.length}</span>
        {filter === 'all' && aircraft.length !== filtered.length && null}
        <span className={styles.filterGroup}>
          {FILTERS.map(f => (
            <button
              key={f.value}
              className={filter === f.value ? styles.filterBtnActive : styles.filterBtn}
              onClick={() => setFilter(f.value)}
            >{f.label}</button>
          ))}
        </span>
        {queueSize > 0 && (
          <span className={styles.queueHint}>{queueSize} lookup{queueSize !== 1 ? 's' : ''} pending</span>
        )}
      </div>
      <div className={styles.scrollWrap}>
        <table className={styles.table}>
          <thead>
            <tr>
              {COLUMNS.map(({ key, label }) => (
                <th
                  key={key}
                  className={styles.sortable}
                  onClick={() => handleSort(key)}
                >
                  {label}
                  <span className={styles.sortIcon}>
                    {sortCol === key ? (sortAsc ? ' ▲' : ' ▼') : ' ⇅'}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map(ac => {
              const { label, fresh, stale } = fmtAge(ac.age)
              const emergency = ac.squawk ? EMERGENCY_SQUAWKS[ac.squawk] : null
              const rowClass = emergency ? styles.emergencyRow
                : ac.acas_ra_active          ? styles.acasRow
                : ac.military                ? styles.militaryRow
                : ac.interesting             ? styles.interestingRow
                : undefined
              return (
                <tr
                  key={ac.icao}
                  className={`${rowClass ?? ''} ${styles.clickable}`}
                  onClick={() => onSelectIcao?.(ac.icao)}
                >
                  <td className={styles.icao}>
                    {ac.icao}
                    {ac.military       && <span className={styles.milBadge}>MIL</span>}
                    {ac.interesting    && <span className={styles.intBadge}>INT</span>}
                    {ac.sighting_count === 1 && <span className={styles.newBadge}>NEW</span>}
                    {ac.acas_ra_active && (
                      <span className={styles.acasBadge} title={ac.acas_ra_desc ?? 'ACAS RA active'}>ACAS</span>
                    )}
                    {!ac.acas_ra_active && threatSet.has(ac.icao) && (
                      <span className={styles.thrBadge} title="Threat aircraft in active RA">THR</span>
                    )}
                  </td>
                  <td>{ac.registration ?? '—'}</td>
                  <td className={styles.callsign}>{ac.callsign ?? '—'}</td>
                  <td title={fmtTypeTooltip(ac)}>
                    <div className={styles.typeCell}>
                      <span>{ac.type_code ?? '—'}</span>
                      <WtcBadge wtc={ac.wtc} />
                    </div>
                  </td>
                  <td className={styles.operator} title={formatOperator(ac.operator) ?? undefined}>{formatOperator(ac.operator) ?? '—'}</td>
                  <td>{ac.year ?? '—'}</td>
                  <td>{ac.country ?? '—'}</td>
                  <td>{fmtAlt(ac.altitude)}</td>
                  <td>{ac.range_nm != null ? `${ac.range_nm} nm` : '—'}</td>
                  <td>{ac.airspeed_kts != null ? `${ac.airspeed_kts} ${ac.airspeed_type ?? ''}`.trim() : '—'}</td>
                  <td>{ac.heading_deg != null ? `${ac.heading_deg}°` : '—'}</td>
                  <td>
                    {ac.squawk ?? '—'}
                    {emergency && <span className={styles.emergencyBadge} title={emergency}>EMER</span>}
                  </td>
                  <td><SignalBar value={ac.signal} /></td>
                  <td className={styles.msgCount}>{ac.msg_count}</td>
                  <td className={fresh ? styles.fresh : stale ? styles.stale : undefined}>
                    {label}
                  </td>
                </tr>
              )
            })}
            {aircraft.length === 0 && (
              <tr>
                <td colSpan={15} className={styles.empty}>No aircraft tracked yet</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
