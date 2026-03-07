import { useState, useEffect, useMemo, useRef } from 'react'
import styles from './CalendarHeatmap.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const METRICS = [
  { value: 'ac_peak',           label: 'Peak aircraft',    url: null },
  { value: 'ac_civil_peak',     label: 'Peak civil',       url: null },
  { value: 'ac_military_peak',  label: 'Peak military',    url: null },
  { value: 'unique_aircraft',   label: 'Unique aircraft',  url: null },
  { value: 'new_aircraft',      label: 'New aircraft',     url: '/api/history/calendar/new_aircraft' },
  { value: 'military_aircraft', label: 'Military/day',     url: '/api/history/calendar/military_aircraft' },
  { value: 'notable_sightings', label: 'Notable/day',      url: '/api/history/calendar/notable_sightings' },
  { value: 'msg_total',         label: 'Total messages',   url: null },
  { value: 'msg_max',           label: 'Peak msg/s',       url: null },
]

const GROUPS = [
  { value: 'widebody',   label: 'Widebody',    types: ['B744','B748','B763','B764','B772','B773','B77W','B77L','B788','B789','B78X','A332','A333','A342','A343','A359','A35K','A388'] },
  { value: 'narrowbody', label: 'Narrowbody',  types: ['A318','A319','A320','A321','A20N','A21N','B735','B736','B737','B738','B739','B38M','B39M','B752','B753','B757','E195','E290'] },
  { value: 'regional',   label: 'Regional',    types: ['CRJ2','CRJ7','CRJ9','CRJX','E170','E175','E190','AT72','AT75','AT76','DH8A','DH8B','DH8C','DH8D','SF34','J328','E120'] },
  { value: 'bizjet',     label: 'Biz jet',     types: ['C25A','C25B','C25C','C510','C525','C550','C560','C56X','C650','C680','C68A','C700','C750','GL5T','GLEX','GLF4','GLF5','GLF6','E55P','PC24','F2TH','F900','FA7X','F7X','LJ35','LJ40','LJ45','LJ55','LJ60'] },
  { value: 'pistonGA',   label: 'Piston GA',   types: ['C172','C152','C182','C206','C208','PA28','PA32','PA34','PA44','DA40','DA42','SR20','SR22','C150','BE36','BE58','M20P','M20T','C210'] },
  { value: 'rotary',     label: 'Rotary',      category: 'H' },
  { value: 'milfast',    label: 'Mil jets',    types: ['F16','FA18','F15','F35','EUFI','RFAL','GRIF','HAWK','MB339','L39','PC21','PC9','T38','F86'] },
]

const MONTH_OPTIONS = [3, 6, 12, 18, 24]
const DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

function buildWeeks(dataByDate, months) {
  const end = new Date()
  const start = new Date()
  start.setMonth(start.getMonth() - months)
  start.setHours(0, 0, 0, 0)

  // Rewind to Monday
  const dow = start.getDay()
  const back = dow === 0 ? 6 : dow - 1
  start.setDate(start.getDate() - back)

  const weeks = []
  const cur = new Date(start)
  while (cur <= end) {
    const week = []
    for (let d = 0; d < 7; d++) {
      const dateStr = cur.toISOString().slice(0, 10)
      week.push({
        date: dateStr,
        value: dataByDate[dateStr] ?? null,
        future: cur > new Date(),
      })
      cur.setDate(cur.getDate() + 1)
    }
    weeks.push(week)
  }
  return weeks
}

function buildMonthLabels(weeks) {
  const labels = []
  let lastMonth = null
  weeks.forEach((week, wi) => {
    const month = week[0].date.slice(0, 7)
    if (month !== lastMonth) {
      const d = new Date(week[0].date + 'T00:00:00Z')
      labels.push({
        col: wi,
        label: d.toLocaleDateString(undefined, { month: 'short', timeZone: 'UTC' }),
      })
      lastMonth = month
    }
  })
  return labels
}

// 5-level GitHub-style colour thresholds
function cellColor(value, maxVal) {
  if (value === null) return '#161b22'   // future / no data
  if (value === 0)    return '#21262d'   // zero
  const t = value / maxVal
  if (t < 0.25) return 'rgba(56,139,253,0.25)'
  if (t < 0.50) return 'rgba(56,139,253,0.50)'
  if (t < 0.75) return 'rgba(56,139,253,0.75)'
  return '#388bfd'
}

function buildGroupUrl(group, months) {
  if (!group) return null
  if (group.category) return `${API_BASE}/api/history/calendar/group?months=${months}&category=${group.category}`
  return `${API_BASE}/api/history/calendar/group?months=${months}&types=${group.types.join(',')}`
}

export default function CalendarHeatmap() {
  const [metric, setMetric] = useState('ac_peak')
  const [group, setGroup] = useState(null)
  const [months, setMonths] = useState(12)
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(true)
  const [tooltip, setTooltip] = useState(null)
  const containerRef = useRef(null)

  useEffect(() => {
    setLoading(true)
    let url
    if (group) {
      url = buildGroupUrl(group, months)
    } else {
      const m = METRICS.find(x => x.value === metric)
      url = m?.url
        ? `${API_BASE}${m.url}?months=${months}`
        : `${API_BASE}/api/history/calendar?metric=${metric}&months=${months}`
    }
    fetch(url)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [metric, group, months])

  const dataByDate = useMemo(() => {
    const m = {}
    data.forEach(({ date, value }) => { m[date] = value })
    return m
  }, [data])

  const maxVal = useMemo(() => Math.max(...data.map(d => d.value), 1), [data])
  const weeks = useMemo(() => buildWeeks(dataByDate, months), [dataByDate, months])
  const monthLabels = useMemo(() => buildMonthLabels(weeks), [weeks])

  function handleCellMouse(e, date, value) {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, date, value })
  }

  const activeLabel = group
    ? (GROUPS.find(g => g.value === group.value)?.label ?? '')
    : (METRICS.find(m => m.value === metric)?.label ?? metric)

  return (
    <div className={styles.container} ref={containerRef}>
      <div className={styles.header}>
        <span className={styles.heading}>Activity Calendar</span>
        <div className={styles.controls}>
          <select
            className={styles.select}
            value={group ? `group:${group.value}` : metric}
            onChange={e => {
              const val = e.target.value
              if (val.startsWith('group:')) {
                setGroup(GROUPS.find(g => g.value === val.slice(6)))
              } else {
                setMetric(val)
                setGroup(null)
              }
            }}
          >
            <optgroup label="Metrics">
              {METRICS.map(m => <option key={m.value} value={m.value}>{m.label}</option>)}
            </optgroup>
            <optgroup label="Type groups">
              {GROUPS.map(g => <option key={g.value} value={`group:${g.value}`}>{g.label}</option>)}
            </optgroup>
          </select>
          <select
            className={styles.select}
            value={months}
            onChange={e => setMonths(Number(e.target.value))}
          >
            {MONTH_OPTIONS.map(m => <option key={m} value={m}>{m} months</option>)}
          </select>
        </div>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : data.length === 0 ? (
        <div className={styles.empty}>No data yet — check back after the app has been running for a while.</div>
      ) : (
        <div className={styles.scrollWrap}>
          <div className={styles.calWrap}>
            {/* Month labels */}
            <div className={styles.monthRow}>
              <div className={styles.dayNameSpacer} />
              <div className={styles.monthLabels}>
                {monthLabels.map(({ col, label }) => (
                  <span
                    key={col}
                    className={styles.monthLabel}
                    style={{ left: `${(col / weeks.length) * 100}%` }}
                  >{label}</span>
                ))}
              </div>
            </div>
            {/* Day rows */}
            <div className={styles.body}>
              <div className={styles.dayNames}>
                {DAY_NAMES.map(d => (
                  <div key={d} className={styles.dayName}>{d}</div>
                ))}
              </div>
              <div className={styles.weekGrid}>
                {weeks.map((week, wi) => (
                  <div key={wi} className={styles.week}>
                    {week.map(({ date, value, future }) => (
                      <div
                        key={date}
                        className={future ? styles.futureCell : styles.cell}
                        style={{ background: future ? 'transparent' : cellColor(value, maxVal) }}
                        onMouseEnter={future ? undefined : e => handleCellMouse(e, date, value)}
                        onMouseMove={future ? undefined : e => handleCellMouse(e, date, value)}
                        onMouseLeave={() => setTooltip(null)}
                      />
                    ))}
                  </div>
                ))}
              </div>
            </div>
            {/* Legend */}
            <div className={styles.legend}>
              <span className={styles.legendLabel}>Less</span>
              {[0, 0.25, 0.5, 0.75, 1].map(t => (
                <div
                  key={t}
                  className={styles.legendCell}
                  style={{ background: t === 0 ? '#21262d' : `rgba(56,139,253,${t})` }}
                />
              ))}
              <span className={styles.legendLabel}>More</span>
            </div>
          </div>
        </div>
      )}

      {tooltip && (
        <div
          className={styles.tooltip}
          style={{ left: tooltip.x + 12, top: tooltip.y - 48 }}
        >
          <div className={styles.tooltipDate}>{tooltip.date}</div>
          <div className={styles.tooltipVal}>
            {tooltip.value == null ? 'No data' : `${tooltip.value} — ${activeLabel}`}
          </div>
        </div>
      )}
    </div>
  )
}
