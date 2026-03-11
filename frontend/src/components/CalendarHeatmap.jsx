import { useState, useEffect, useMemo, useRef } from 'react'
import styles from './CalendarHeatmap.module.css'
import { TYPE_GROUPS } from '../utils/typeGroups'

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

const GROUPS = TYPE_GROUPS

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

// Continuous HSL gradient matching scatter plots, reversed: purple (low) → blue → yellow → green (high)
function cellColor(value, maxVal) {
  if (value === null) return '#161b22'   // future / no data
  if (value === 0)    return '#21262d'   // zero
  const t = Math.max(0, Math.min(1, value / maxVal))
  let h
  if (t < 0.444) {
    h = 280 - (t / 0.444) * 70           // purple → blue (280→210)
  } else if (t < 0.778) {
    h = 210 - ((t - 0.444) / 0.334) * 150 // blue → yellow (210→60)
  } else {
    h = 60 + ((t - 0.778) / 0.222) * 60   // yellow → green (60→120)
  }
  return `hsl(${Math.round(h)},80%,55%)`
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

      {!data.length && loading ? (
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
                  style={{ background: cellColor(t, 1) }}
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
