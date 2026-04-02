import { useState, useEffect, useRef, useCallback } from 'react'
import s from './FleetTypesTable.module.css'

const PAGE_SIZE = 50

function fmtAge(ts) {
  if (!ts) return '—'
  const diff = Math.floor(Date.now() / 1000) - ts
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function fmtNum(n) {
  if (n == null) return '—'
  return Number(n).toLocaleString()
}

function WtcBadge({ wtc }) {
  if (!wtc) return null
  const cls = { L: s.wtcL, M: s.wtcM, H: s.wtcH, J: s.wtcJ }[wtc] || ''
  return <span className={`${s.wtcBadge} ${cls}`}>{wtc}</span>
}

function AirframeRows({ typeCodes, since, milFilter, onSelectIcao }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const ctrl = new AbortController()
    setLoading(true)
    const params = new URLSearchParams({ type_codes: typeCodes })
    if (since) params.set('since', since)
    if (milFilter === 'civil') params.set('military', 0)
    else if (milFilter === 'military') params.set('military', 1)

    fetch(`/api/fleet/type_airframes?${params}`, { signal: ctrl.signal })
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => {})
    return () => ctrl.abort()
  }, [typeCodes, since, milFilter])

  if (loading) {
    return (
      <tr className={s.loadingRow}>
        <td colSpan={12}>Loading…</td>
      </tr>
    )
  }
  if (!data || data.length === 0) {
    return (
      <tr className={s.emptyRow}>
        <td colSpan={12}>No airframes found.</td>
      </tr>
    )
  }
  return data.map(af => (
    <tr key={af.icao} style={{ cursor: onSelectIcao ? 'pointer' : undefined }} onClick={() => onSelectIcao?.(af.icao)}>
      <td className={s.icao}>{af.icao}</td>
      <td>{af.registration || '—'}</td>
      <td className={s.operator}>{af.operator_display || af.operator || '—'}</td>
      <td>{af.country || '—'}</td>
      <td>{af.year || '—'}</td>
      <td>{af.military ? <span className={s.milBadge}>MIL</span> : null}</td>
      <td className={s.num}>{af.sighting_count}</td>
      <td className={s.num}>{af.visit_count}</td>
      <td>{fmtAge(af.last_seen)}</td>
      <td className={s.num}>{af.max_altitude != null ? `${fmtNum(af.max_altitude)} ft` : '—'}</td>
      <td className={s.num}>{fmtNum(af.total_messages)}</td>
      <td>
        {af.top_routes && af.top_routes.map((r, i) => (
          <span key={i} className={s.routeBadge}>{r}</span>
        ))}
      </td>
    </tr>
  ))
}

export default function FleetTypesTable({ since, onSelectIcao }) {
  const [page, setPage] = useState(0)
  const [sortCol, setSortCol] = useState('airframes')
  const [sortDir, setSortDir] = useState('desc')
  const [milFilter, setMilFilter] = useState('all')
  const [searchInput, setSearchInput] = useState('')
  const [search, setSearch] = useState('')
  const [groupByMfr, setGroupByMfr] = useState(false)
  const [expandedRow, setExpandedRow] = useState(null)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const searchTimer = useRef(null)

  // Debounce search input
  const handleSearchChange = useCallback(e => {
    const val = e.target.value
    setSearchInput(val)
    clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => {
      setSearch(val)
      setPage(0)
    }, 300)
  }, [])

  // Reset page when filters change
  const prevFilters = useRef({ sortCol, sortDir, milFilter, search, groupByMfr, since })
  useEffect(() => {
    const prev = prevFilters.current
    if (
      prev.sortCol !== sortCol || prev.sortDir !== sortDir ||
      prev.milFilter !== milFilter || prev.search !== search ||
      prev.groupByMfr !== groupByMfr || prev.since !== since
    ) {
      setPage(0)
      prevFilters.current = { sortCol, sortDir, milFilter, search, groupByMfr, since }
    }
  }, [sortCol, sortDir, milFilter, search, groupByMfr, since])

  // Main fetch
  useEffect(() => {
    const ctrl = new AbortController()
    setLoading(true)
    const params = new URLSearchParams({
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
      sort_col: sortCol,
      sort_dir: sortDir,
    })
    if (since) params.set('since', since)
    if (milFilter === 'civil') params.set('military', 0)
    else if (milFilter === 'military') params.set('military', 1)
    if (search) params.set('search', search)
    if (groupByMfr) params.set('group_by', 'manufacturer')

    fetch(`/api/fleet/types_table?${params}`, { signal: ctrl.signal })
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => {})
    return () => ctrl.abort()
  }, [page, sortCol, sortDir, milFilter, search, groupByMfr, since])

  function toggleSort(col) {
    if (sortCol === col) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc')
    } else {
      setSortCol(col)
      setSortDir('desc')
    }
  }

  function toggleExpand(key) {
    setExpandedRow(prev => prev === key ? null : key)
  }

  function sortIcon(col) {
    if (sortCol !== col) return <span className={s.sortIcon}> ⇅</span>
    return <span className={s.sortIcon}>{sortDir === 'desc' ? ' ↓' : ' ↑'}</span>
  }

  const total = data?.total ?? 0
  const items = data?.items ?? []
  const totalPages = Math.ceil(total / PAGE_SIZE)
  const showFrom = total === 0 ? 0 : page * PAGE_SIZE + 1
  const showTo = Math.min((page + 1) * PAGE_SIZE, total)

  return (
    <div className={s.container}>
      <div className={s.header}>
        <div className={s.titleRow}>
          <span className={s.heading}>Aircraft Types</span>
          {!loading && <span className={s.count}>{total}</span>}
        </div>
        <div className={s.controls}>
          <input
            className={s.searchInput}
            placeholder="Search type / mfr…"
            value={searchInput}
            onChange={handleSearchChange}
          />
          <span className={s.sep} />
          {['all', 'civil', 'military'].map(f => (
            <button
              key={f}
              className={milFilter === f ? s.btnActive : s.btn}
              onClick={() => setMilFilter(f)}
            >
              {f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}
          <span className={s.sep} />
          <button
            className={groupByMfr ? s.btnActive : s.btn}
            onClick={() => { setGroupByMfr(g => !g); setExpandedRow(null) }}
          >
            Group by manufacturer
          </button>
        </div>
      </div>

      <div className={s.scrollWrap}>
        <table className={s.table}>
          <thead>
            <tr>
              <th style={{ width: 20 }} />
              <th className={s.sortable} onClick={() => toggleSort('type_code')}>
                {groupByMfr ? 'Manufacturer / Group' : 'Type Code'}{sortIcon('type_code')}
              </th>
              {!groupByMfr && <th>Type Name</th>}
              {!groupByMfr && <th>WTC</th>}
              {groupByMfr && <th>Types</th>}
              <th className={s.sortable} onClick={() => toggleSort('airframes')}>
                Airframes{sortIcon('airframes')}
              </th>
              <th className={s.sortable} onClick={() => toggleSort('flights')}>
                Flights{sortIcon('flights')}
              </th>
              <th className={s.sortable} onClick={() => toggleSort('last_seen')}>
                Last Seen{sortIcon('last_seen')}
              </th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr className={s.loadingRow}>
                <td colSpan={7}>Loading…</td>
              </tr>
            )}
            {!loading && items.length === 0 && (
              <tr className={s.emptyRow}>
                <td colSpan={7}>No types found.</td>
              </tr>
            )}
            {!loading && items.map(row => {
              const key = row.group_key
              const isExpanded = expandedRow === key
              return [
                <tr key={key} onClick={() => toggleExpand(key)} style={{ cursor: 'pointer' }}>
                  <td>
                    <span className={`${s.expandToggle}${isExpanded ? ' ' + s.expandToggleOpen : ''}`}>▶</span>
                  </td>
                  <td className={s.typeCode}>
                    {key}
                  </td>
                  {!groupByMfr && (
                    <td className={s.typeName}>{row.type_name || <span className={s.muted}>—</span>}</td>
                  )}
                  {!groupByMfr && (
                    <td><WtcBadge wtc={row.wtc} /></td>
                  )}
                  {groupByMfr && (
                    <td>
                      {row.type_codes.split(',').slice(0, 8).map(tc => (
                        <span key={tc} className={s.typeBadge}>{tc.trim()}</span>
                      ))}
                      {row.type_codes.split(',').length > 8 && (
                        <span className={s.muted}>+{row.type_codes.split(',').length - 8}</span>
                      )}
                    </td>
                  )}
                  <td className={s.num}>{row.airframes}</td>
                  <td className={s.num}>{row.flights}</td>
                  <td>{fmtAge(row.last_seen)}</td>
                </tr>,
                isExpanded && (
                  <tr key={`${key}-exp`} className={s.expandedRow}>
                    <td colSpan={groupByMfr ? 6 : 7}>
                      <div className={s.subTableWrap}>
                        <table className={s.subTable}>
                          <thead>
                            <tr>
                              <th>ICAO</th>
                              <th>Registration</th>
                              <th>Operator</th>
                              <th>Country</th>
                              <th>Year</th>
                              <th>Mil</th>
                              <th>Sightings</th>
                              <th>Flights</th>
                              <th>Last Seen</th>
                              <th>Max Alt</th>
                              <th>Messages</th>
                              <th>Top Routes</th>
                            </tr>
                          </thead>
                          <tbody>
                            <AirframeRows
                              typeCodes={row.type_codes}
                              since={since}
                              milFilter={milFilter}
                              onSelectIcao={onSelectIcao}
                            />
                          </tbody>
                        </table>
                      </div>
                    </td>
                  </tr>
                )
              ]
            })}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className={s.pagination}>
          <button className={s.pageBtn} disabled={page === 0} onClick={() => setPage(0)}>«</button>
          <button className={s.pageBtn} disabled={page === 0} onClick={() => setPage(p => p - 1)}>‹</button>
          <span className={s.pageInfo}>{showFrom}–{showTo} of {total}</span>
          <button className={s.pageBtn} disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}>›</button>
          <button className={s.pageBtn} disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>»</button>
        </div>
      )}
    </div>
  )
}
