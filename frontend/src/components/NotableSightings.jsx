import { useState, useEffect, useRef } from 'react'
import styles from './NotableSightings.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const PAGE_SIZE = 50

function flagScore(ac) {
  return (ac.foreign_military ? 8 : 0) + (ac.interesting ? 4 : 0) +
         (ac.rare ? 2 : 0) + (ac.first_seen_flag || ac.sighting_count === 1 ? 1 : 0)
}

const COLUMNS = [
  { key: 'icao',          label: 'ICAO'       },
  { key: 'registration',  label: 'Reg'        },
  { key: 'type_code',     label: 'Type'       },
  { key: 'operator',      label: 'Operator'   },
  { key: 'year',          label: 'Year'       },
  { key: 'country',       label: 'Country'    },
  { key: 'flags',         label: 'Flags'      },
  { key: 'first_seen',    label: 'First seen' },
  { key: 'last_seen',     label: 'Last seen'  },
  { key: 'sighting_count',label: 'Sessions'   },
]

const FLAGS = [
  { value: 'all',              label: 'All notable' },
  { value: 'all_aircraft',     label: 'All aircraft' },
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
  if (ac.foreign_military) flags.push(<FlagBadge key="fm"     label="Foreign Military" color="#f85149" />)
  if (ac.interesting)      flags.push(<FlagBadge key="int"    label="Interesting"      color="#d29922" />)
  if (ac.rare)             flags.push(<FlagBadge key="rare"   label="Rare"             color="#bc8cff" />)
  if (ac.sighting_count === 1 || ac.first_seen_flag)
                           flags.push(<FlagBadge key="unique" label="Unique"           color="#3fb950" />)

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
  const [flag, setFlag]         = useState('all')
  const [days, setDays]         = useState(1)
  const [typeInput, setTypeInput] = useState('')
  const [typeFilter, setTypeFilter] = useState('')
  const [typeOptions, setTypeOptions] = useState([])
  const [data, setData]         = useState([])
  const [total, setTotal]       = useState(0)
  const [loading, setLoading]   = useState(true)
  const [page, setPage]         = useState(0)
  const [sortKey, setSortKey]   = useState('last_seen')
  const [sortAsc, setSortAsc]   = useState(false)

  // Fetch type options once on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/history/heatmap/options`)
      .then(r => r.json())
      .then(d => setTypeOptions([...( d.types ?? [])].sort()))
      .catch(() => {})
  }, [])

  // Reset page to 0 when any filter or sort changes
  const fetchKey = `${flag}|${days}|${typeFilter}|${sortKey}|${sortAsc}`
  const prevFetchKey = useRef(fetchKey)
  useEffect(() => {
    if (prevFetchKey.current !== fetchKey) {
      setPage(0)
      prevFetchKey.current = fetchKey
    }
  }, [fetchKey])

  useEffect(() => {
    setLoading(true)
    const daysParam   = days != null ? `&days=${days}` : ''
    const typeParam   = typeFilter   ? `&type_code=${encodeURIComponent(typeFilter)}` : ''
    const sortParam   = `&sort_col=${sortKey}&sort_dir=${sortAsc ? 'asc' : 'desc'}`
    const offsetParam = `&offset=${page * PAGE_SIZE}`
    fetch(`${API_BASE}/api/history/notable?flag=${flag}&limit=${PAGE_SIZE}${offsetParam}${daysParam}${typeParam}${sortParam}`)
      .then(r => r.json())
      .then(d => {
        setData(d.items ?? [])
        setTotal(d.total ?? 0)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [flag, days, typeFilter, page, sortKey, sortAsc, refreshKey])

  function handleSort(key) {
    if (key === sortKey) setSortAsc(v => !v)
    else { setSortKey(key); setSortAsc(key === 'last_seen' ? false : true) }
  }

  function applyType(val) {
    const v = val.trim().toUpperCase()
    setTypeFilter(v)
    setTypeInput(v)
  }

  function clearType() {
    setTypeFilter('')
    setTypeInput('')
  }

  const totalPages  = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const showingFrom = total === 0 ? 0 : page * PAGE_SIZE + 1
  const showingTo   = Math.min((page + 1) * PAGE_SIZE, total)

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <div className={styles.titleRow}>
          <span className={styles.heading}>Notable Sightings</span>
          <span className={styles.count}>{total.toLocaleString()}</span>
        </div>
        <div className={styles.controls}>
          {/* Category flag buttons */}
          {FLAGS.map(f => (
            <button
              key={f.value}
              className={flag === f.value ? styles.btnActive : styles.btn}
              onClick={() => setFlag(f.value)}
            >{f.label}</button>
          ))}

          <div className={styles.sep} />

          {/* Type filter — datalist autocomplete matching the heatmap */}
          <div className={styles.searchGroup}>
            <input
              className={styles.searchInput}
              list="notable-type-list"
              placeholder="Filter by type…"
              value={typeInput}
              onChange={e => setTypeInput(e.target.value)}
              onBlur={e => { if (e.target.value) applyType(e.target.value) }}
              onKeyDown={e => {
                if (e.key === 'Enter') applyType(e.target.value)
                if (e.key === 'Escape') clearType()
              }}
            />
            <datalist id="notable-type-list">
              {typeOptions.map(t => <option key={t} value={t} />)}
            </datalist>
            {typeFilter && (
              <button className={styles.clearBtn} onClick={clearType} title="Clear type filter">×</button>
            )}
          </div>

          <div className={styles.sep} />

          {/* Timeframe buttons */}
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
      ) : total === 0 ? (
        <div className={styles.empty}>No aircraft match the selected filters.</div>
      ) : (
        <>
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
                {data.map(ac => <AircraftRow key={ac.icao} ac={ac} onSelectIcao={onSelectIcao} />)}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className={styles.pagination}>
              <button className={styles.pageBtn} onClick={() => setPage(0)} disabled={page === 0}>«</button>
              <button className={styles.pageBtn} onClick={() => setPage(p => p - 1)} disabled={page === 0}>‹</button>
              <span className={styles.pageInfo}>{showingFrom}–{showingTo} of {total.toLocaleString()}</span>
              <button className={styles.pageBtn} onClick={() => setPage(p => p + 1)} disabled={page >= totalPages - 1}>›</button>
              <button className={styles.pageBtn} onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1}>»</button>
            </div>
          )}
        </>
      )}
    </div>
  )
}
