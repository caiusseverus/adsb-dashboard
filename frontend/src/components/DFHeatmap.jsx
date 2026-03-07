/**
 * DF message-type hourly heatmap.
 * Same layout as HourlyHeatmap but filtered to a single DF type via dropdown.
 * Data source: /api/history/heatmap/df?df=17&days=30&bucket=60
 */
import { useState, useEffect, useRef } from 'react'
import styles from './HourlyHeatmap.module.css'   // reuse identical styles

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const DAYS = 30
const GRID_HEIGHT = 480

const DF_OPTIONS = [
  { value: null, label: 'All types — Total' },
  { value: 17,   label: 'DF17 — ADS-B' },
  { value: 11,   label: 'DF11 — All-Call Reply' },
  { value: 4,    label: 'DF4  — Surveillance Alt' },
  { value: 5,    label: 'DF5  — Surveillance ID' },
  { value: 20,   label: 'DF20 — Comm-B Alt' },
  { value: 21,   label: 'DF21 — Comm-B ID' },
  { value: 0,    label: 'DF0  — Short ACAS' },
  { value: 16,   label: 'DF16 — Long ACAS' },
  { value: 18,   label: 'DF18 — TIS-B' },
  { value: 24,   label: 'DF24 — Comm-D' },
]

const BUCKETS = [
  { value: 15, label: '15 min' },
  { value: 60, label: '1 hr' },
]

function formatBucket(bucket, bucketMins) {
  const totalMins = bucket * bucketMins
  return `${String(Math.floor(totalMins / 60)).padStart(2, '0')}:${String(totalMins % 60).padStart(2, '0')}`
}

function buildDayList() {
  const days = []
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  for (let i = DAYS - 1; i >= 0; i--) {
    const d = new Date(today)
    d.setDate(d.getDate() - i)
    days.push(d.toISOString().slice(0, 10))
  }
  return days
}

// Continuous HSL gradient matching scatter plots, reversed: purple (low) → blue → yellow → green (high)
function cellColor(value, maxVal) {
  if (!value) return '#21262d'
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

const ALL_DAYS = buildDayList()

const DAY_LABELS = ALL_DAYS.map((d, i) => {
  const parsed = new Date(d + 'T00:00:00Z')
  if (i === 0 || parsed.getDate() === 1)
    return parsed.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })
  if (i % 7 === 0)
    return parsed.toLocaleDateString(undefined, { day: 'numeric', timeZone: 'UTC' })
  return ''
})

export default function DFHeatmap() {
  const [df, setDf]               = useState(null)
  const [bucketMins, setBucketMins] = useState(15)
  const [data, setData]           = useState([])
  const [loading, setLoading]     = useState(true)
  const [tooltip, setTooltip]     = useState(null)
  const containerRef = useRef(null)

  useEffect(() => {
    setLoading(true)
    const url = df === null
      ? `${API_BASE}/api/history/heatmap/df?days=${DAYS}&bucket=${bucketMins}`
      : `${API_BASE}/api/history/heatmap/df?df=${df}&days=${DAYS}&bucket=${bucketMins}`
    fetch(url)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [df, bucketMins])

  const numBuckets = Math.round((24 * 60) / bucketMins)
  const byKey = {}
  data.forEach(({ day, bucket, value }) => { byKey[`${day}-${bucket}`] = value })
  const maxVal = Math.max(...data.map(d => d.value), 1)

  const hourLabels = Array.from({ length: 24 }, (_, h) => ({
    label: `${String(h).padStart(2, '0')}:00`,
    pct: ((23 - h) / 24) * 100,
  }))

  function handleMouseEnter(e, day, bucket, value) {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    setTooltip({ x: e.clientX - rect.left, y: e.clientY - rect.top, day, bucket, value })
  }

  function handleMouseMove(e) {
    if (!tooltip) return
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return
    setTooltip(t => t ? { ...t, x: e.clientX - rect.left, y: e.clientY - rect.top } : null)
  }

  const dfLabel = DF_OPTIONS.find(o => o.value === df)?.label ?? (df === null ? 'All types' : `DF${df}`)

  return (
    <div className={styles.container} ref={containerRef} onMouseMove={handleMouseMove}>
      <div className={styles.header}>
        <span className={styles.heading}>Message Type by Hour of Day — {DAYS} days</span>
        <div className={styles.controls}>
          <select
            className={styles.searchInput}
            value={df ?? ''}
            onChange={e => setDf(e.target.value === '' ? null : Number(e.target.value))}
            style={{ width: 'auto', paddingRight: '1rem' }}
          >
            {DF_OPTIONS.map(o => (
              <option key={o.value ?? 'all'} value={o.value ?? ''}>{o.label}</option>
            ))}
          </select>

          <div className={styles.sep} />

          {BUCKETS.map(b => (
            <button
              key={b.value}
              className={bucketMins === b.value ? styles.btnActive : styles.btn}
              onClick={() => setBucketMins(b.value)}
            >{b.label}</button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : data.length === 0 ? (
        <div className={styles.empty}>No data yet for {dfLabel}.</div>
      ) : (
        <div className={styles.heatmapWrap} style={{ height: GRID_HEIGHT }}>
          <div className={styles.corner} />
          <div className={styles.xAxis}>
            {ALL_DAYS.map((d, i) => (
              <div key={d} className={styles.dayLabel} title={d}>{DAY_LABELS[i]}</div>
            ))}
          </div>
          <div className={styles.yAxis}>
            {hourLabels.map(({ label, pct }) => (
              <div key={label} className={styles.hourLabel} style={{ top: `${pct}%` }}>{label}</div>
            ))}
          </div>
          <div className={styles.cells} style={{ '--rows': numBuckets, '--cols': DAYS }}>
            {Array.from({ length: numBuckets }, (_, i) => {
              const bucket = numBuckets - 1 - i  // reversed: midnight at bottom
              return ALL_DAYS.map(day => {
                const val = byKey[`${day}-${bucket}`] ?? 0
                return (
                  <div
                    key={`${bucket}-${day}`}
                    className={styles.cell}
                    style={{ background: cellColor(val, maxVal) }}
                    onMouseEnter={e => handleMouseEnter(e, day, bucket, val)}
                    onMouseLeave={() => setTooltip(null)}
                  />
                )
              })
            })}
          </div>
        </div>
      )}

      {tooltip && (
        <div className={styles.tooltip} style={{ left: tooltip.x + 14, top: tooltip.y - 44 }}>
          <div className={styles.tooltipTime}>
            {tooltip.day} · {formatBucket(tooltip.bucket, bucketMins)}
          </div>
          <div className={styles.tooltipVal}>{tooltip.value.toLocaleString()}</div>
        </div>
      )}
    </div>
  )
}
