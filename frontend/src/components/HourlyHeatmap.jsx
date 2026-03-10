import { useState, useEffect, useRef } from 'react'
import styles from './HourlyHeatmap.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const DAYS = 30
const GRID_HEIGHT = 480  // px — fixed regardless of bucket size

const METRICS = [
  { value: 'ac_total',    label: 'Total aircraft' },
  { value: 'ac_civil',    label: 'Civil' },
  { value: 'ac_military', label: 'Military' },
]
const BUCKETS = [
  { value: 15, label: '15 min' },
  { value: 60, label: '1 hr' },
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
  if (i === 0 || parsed.getDate() === 1) {
    return parsed.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })
  }
  if (i % 7 === 0) {
    return parsed.toLocaleDateString(undefined, { day: 'numeric', timeZone: 'UTC' })
  }
  return ''
})

export default function HourlyHeatmap() {
  const [metric, setMetric]         = useState('ac_total')
  const [bucketMins, setBucketMins] = useState(15)
  const [group, setGroup]           = useState(null)
  const [typeCode, setTypeCode]     = useState('')
  const [operator, setOperator]     = useState('')
  const [typeInput, setTypeInput]   = useState('')
  const [opInput, setOpInput]       = useState('')
  const [options, setOptions]       = useState({ types: [], operators: [] })
  const [data, setData]             = useState([])
  const [loading, setLoading]       = useState(true)
  const [tooltip, setTooltip]       = useState(null)
  const containerRef = useRef(null)

  // Load autocomplete options once
  useEffect(() => {
    fetch(`${API_BASE}/api/history/heatmap/options`)
      .then(r => r.json())
      .then(d => setOptions({ types: [...d.types].sort(), operators: [...d.operators].sort() }))
      .catch(() => {})
  }, [])

  // Derive active filter mode
  const filterMode = typeCode ? 'type' : operator ? 'operator' : group ? 'group' : 'metric'

  // Fetch heatmap data whenever any param changes
  useEffect(() => {
    setLoading(true)
    let url
    if (filterMode === 'type') {
      url = `${API_BASE}/api/history/heatmap/type?type_code=${encodeURIComponent(typeCode)}&days=${DAYS}&bucket=${bucketMins}`
    } else if (filterMode === 'operator') {
      url = `${API_BASE}/api/history/heatmap/operator?operator=${encodeURIComponent(operator)}&days=${DAYS}&bucket=${bucketMins}`
    } else if (filterMode === 'group') {
      const q = group.category
        ? `category=${encodeURIComponent(group.category)}`
        : `types=${encodeURIComponent(group.types.join(','))}`
      url = `${API_BASE}/api/history/heatmap/group?${q}&days=${DAYS}&bucket=${bucketMins}`
    } else {
      url = `${API_BASE}/api/history/heatmap?metric=${metric}&days=${DAYS}&bucket=${bucketMins}`
    }
    fetch(url)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [metric, bucketMins, group, typeCode, operator, filterMode])

  const numBuckets = Math.round((24 * 60) / bucketMins)

  const byKey = {}
  data.forEach(({ day, bucket, value }) => { byKey[`${day}-${bucket}`] = value })
  const maxVal = Math.max(...data.map(d => d.value), 1)

  // Y-axis: one label per hour, positioned as % of grid height.
  // Midnight (00:00) is at the bottom; 23:00 at the top.
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

  function applyType(val) {
    const v = val.trim().toUpperCase()
    setTypeCode(v)
    setOperator('')
    setOpInput('')
    setGroup(null)
  }

  function applyOperator(val) {
    const v = val.trim()
    setOperator(v)
    setTypeCode('')
    setTypeInput('')
    setGroup(null)
  }

  function clearFilters() {
    setTypeCode(''); setTypeInput('')
    setOperator(''); setOpInput('')
    setGroup(null)
  }

  return (
    <div className={styles.container} ref={containerRef} onMouseMove={handleMouseMove}>
      <div className={styles.header}>
        <span className={styles.heading}>Traffic by Hour of Day — {DAYS} days</span>
        <div className={styles.controls}>
          {/* Metric buttons — hidden when a filter is active */}
          {filterMode === 'metric' && METRICS.map(m => (
            <button
              key={m.value}
              className={metric === m.value ? styles.btnActive : styles.btn}
              onClick={() => setMetric(m.value)}
            >{m.label}</button>
          ))}

          {filterMode !== 'metric' && (
            <span className={styles.filterLabel}>
              {filterMode === 'type'     ? `Type: ${typeCode}`       :
               filterMode === 'operator' ? `Operator: ${operator}`   :
               `Group: ${group.label}`}
            </span>
          )}

          {/* Group preset dropdown — hidden when type/operator filter active */}
          {filterMode !== 'type' && filterMode !== 'operator' && (
            <select
              className={styles.select}
              value={group?.value ?? ''}
              onChange={e => {
                const val = e.target.value
                setGroup(val ? GROUPS.find(g => g.value === val) : null)
              }}
            >
              <option value="">— Type group —</option>
              {GROUPS.map(g => <option key={g.value} value={g.value}>{g.label}</option>)}
            </select>
          )}

          <div className={styles.sep} />

          {/* Type autocomplete */}
          <div className={styles.searchGroup}>
            <input
              id="hm-type-input"
              className={styles.searchInput}
              list="hm-type-list"
              placeholder="Filter by type…"
              value={typeInput}
              onChange={e => setTypeInput(e.target.value)}
              onBlur={e => { if (e.target.value) applyType(e.target.value) }}
              onKeyDown={e => { if (e.key === 'Enter') applyType(e.target.value) }}
            />
            <datalist id="hm-type-list">
              {options.types.map(t => <option key={t} value={t} />)}
            </datalist>
          </div>

          {/* Operator autocomplete */}
          <div className={styles.searchGroup}>
            <input
              id="hm-op-input"
              className={styles.searchInput}
              list="hm-op-list"
              placeholder="Filter by operator…"
              value={opInput}
              onChange={e => setOpInput(e.target.value)}
              onBlur={e => { if (e.target.value) applyOperator(e.target.value) }}
              onKeyDown={e => { if (e.key === 'Enter') applyOperator(e.target.value) }}
            />
            <datalist id="hm-op-list">
              {options.operators.map(o => <option key={o} value={o} />)}
            </datalist>
          </div>

          {filterMode !== 'metric' && (
            <button className={styles.clearBtn} onClick={clearFilters}>Clear</button>
          )}

          <div className={styles.sep} />

          {/* Bucket size */}
          {BUCKETS.map(b => (
            <button
              key={b.value}
              className={bucketMins === b.value ? styles.btnActive : styles.btn}
              onClick={() => setBucketMins(b.value)}
            >{b.label}</button>
          ))}
        </div>
      </div>

      {!data.length && loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : data.length === 0 ? (
        <div className={styles.empty}>No data yet — check back after the app has been running for a while.</div>
      ) : (
        /* Fixed-height heatmap: cells resize to fill the space */
        <div className={styles.heatmapWrap} style={{ height: GRID_HEIGHT }}>
          {/* Top-left corner */}
          <div className={styles.corner} />

          {/* X-axis: day labels */}
          <div className={styles.xAxis}>
            {ALL_DAYS.map((d, i) => (
              <div key={d} className={styles.dayLabel} title={d}>{DAY_LABELS[i]}</div>
            ))}
          </div>

          {/* Y-axis: hour labels positioned absolutely */}
          <div className={styles.yAxis}>
            {hourLabels.map(({ label, pct }) => (
              <div key={label} className={styles.hourLabel} style={{ top: `${pct}%` }}>
                {label}
              </div>
            ))}
          </div>

          {/* Data cells — CSS grid fills remaining space */}
          <div
            className={styles.cells}
            style={{ '--rows': numBuckets, '--cols': DAYS }}
          >
            {Array.from({ length: numBuckets }, (_, i) => {
              const bucket = numBuckets - 1 - i  // reversed: row 0 = last bucket (end of day)
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
        <div
          className={styles.tooltip}
          style={{ left: tooltip.x + 14, top: tooltip.y - 44 }}
        >
          <div className={styles.tooltipTime}>
            {tooltip.day} · {formatBucket(tooltip.bucket, bucketMins)}
          </div>
          <div className={styles.tooltipVal}>{tooltip.value.toFixed(1)}</div>
        </div>
      )}
    </div>
  )
}
