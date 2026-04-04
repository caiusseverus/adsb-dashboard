import { useState, useMemo, useRef, useEffect, useCallback } from 'react'
import {
  ScatterChart, ComposedChart, Scatter, Line, XAxis, YAxis, ZAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell,
  AreaChart, Area, Legend,
} from 'recharts'
import { useFetch } from '../utils/useFetch'
import DFHeatmap from '../components/DFHeatmap'
import SignalHeatmap from '../components/SignalHeatmap'
import styles from './ReceiverPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const TIMING_WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/timing`
  : 'ws://localhost:8000/ws/timing'
const INTERROGATOR_WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/interrogators`
  : 'ws://localhost:8000/ws/interrogators'

// Beast RSSI byte: 0=strongest (0 dBFS), 255=weakest (-127.5 dBFS)
// airspy_adsb encodes as raw = -2 * dBFS, so dBFS = -(raw / 2)
function rawToDbfs(raw) {
  if (raw == null) return null
  const clamped = Math.max(0, Math.min(255, Number(raw)))
  return Math.round(-clamped / 2 * 10) / 10
}
function fmtDbfs(raw) {
  const v = rawToDbfs(raw)
  return v != null ? `${v.toFixed(1)} dBFS` : '—'
}
// Keep % for colour coding only
function rssiByte(raw) {
  return raw != null ? Math.max(0, Math.min(100, Math.round((255 - raw) / 2.55))) : null
}
function signalColour(raw) {
  const pct = rssiByte(raw)
  if (pct == null) return '#484f58'
  if (pct > 66) return '#3fb950'
  if (pct > 33) return '#d29922'
  return '#f85149'
}

function formatAgeShort(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return '—'
  if (seconds < 1) return 'just now'
  if (seconds < 60) return `${Math.round(seconds)}s ago`
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`
  return `${Math.round(seconds / 3600)}h ago`
}

function normalizePercentTriplet(adsbRaw, mlatRaw, noPosRaw) {
  const values = [adsbRaw, mlatRaw, noPosRaw].map(v => {
    const n = Number(v)
    return Number.isFinite(n) ? n : 0
  })
  const total = values[0] + values[1] + values[2]
  const scale = total > 0 && total <= 1.5 ? 100 : 1
  return values.map(v => Math.max(0, Math.min(100, Math.round(v * scale * 10) / 10)))
}


function Card({ title, children, controls }) {
  return (
    <div className={styles.card}>
      <div className={styles.cardHeader}>
        <span className={styles.cardTitle}>{title}</span>
        {controls && <div className={styles.cardControls}>{controls}</div>}
      </div>
      {children}
    </div>
  )
}

function Empty({ loading }) {
  return (
    <div className={styles.empty}>
      {loading ? 'Loading…' : 'No data yet — stats accumulate after a few minutes of operation.'}
    </div>
  )
}

function DaySelect({ value, onChange, options }) {
  return (
    <select className={styles.select} value={value} onChange={e => onChange(Number(e.target.value))}>
      {options.map(d => <option key={d} value={d}>{d}d</option>)}
    </select>
  )
}

const AGE_COLOURS = [
  { maxAgeDays: 1,   colour: '#3fb950' }, // today     — green
  { maxAgeDays: 2,   colour: '#388bfd' }, // yesterday — blue
  { maxAgeDays: 7,   colour: '#d29922' }, // this week — amber
  { maxAgeDays: 30,  colour: '#bc8cff' }, // this month — purple
  { maxAgeDays: Infinity, colour: '#6e7681' }, // older — grey
]
function ageColour(ts) {
  if (ts == null) return '#484f58'
  const ageDays = (Date.now() / 1000 - ts) / 86400
  return (AGE_COLOURS.find(b => ageDays < b.maxAgeDays) ?? AGE_COLOURS.at(-1)).colour
}

// ---------------------------------------------------------------------------
// 1. Scatter: aircraft count vs messages/sec, coloured by signal or age
// ---------------------------------------------------------------------------
function ScatterPlot({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/scatter?days=${days}`)
  const [colourMode, setColourMode] = useState(
    () => localStorage.getItem('scatter_colour_mode') || 'signal'
  )

  const points = useMemo(() => (data || []).map(d => ({
    ac: d.ac, msgs: Math.round((d.msgs / 60) * 10) / 10, signal: d.signal, ts: d.ts,
  })), [data])

  function toggleColour() {
    const next = colourMode === 'signal' ? 'age' : 'signal'
    setColourMode(next)
    localStorage.setItem('scatter_colour_mode', next)
  }

  const controls = (
    <>
      <button className={styles.btn} onClick={toggleColour}>
        {colourMode === 'signal' ? 'Colour: Signal' : 'Colour: Age'}
      </button>
      <DaySelect value={days} onChange={onDaysChange} options={[1, 3, 7, 14, 30]} />
    </>
  )

  return (
    <Card title="Aircraft count vs messages/sec" controls={controls}>
      {!points.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={280}>
          <ScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="ac" name="Aircraft" type="number"
              label={{ value: 'Aircraft', position: 'insideBottom', offset: -12, fill: '#484f58', fontSize: 11 }}
              tick={{ fill: '#484f58', fontSize: 11 }} />
            <YAxis dataKey="msgs" name="Messages/sec" type="number"
              tick={{ fill: '#484f58', fontSize: 11 }} width={50} />
            <ZAxis range={[20, 20]} />
            <Tooltip cursor={{ stroke: '#30363d' }}
              content={({ payload }) => {
                if (!payload?.length) return null
                const d = payload[0].payload
                const date = d.ts ? new Date(d.ts * 1000).toLocaleDateString() : '—'
                return (
                  <div className={styles.tooltip}>
                    <div>Aircraft: {d.ac}</div>
                    <div>Msgs/sec: {d.msgs}</div>
                    <div>Signal avg: {fmtDbfs(d.signal)}</div>
                    <div>Date: {date}</div>
                  </div>
                )
              }}
            />
            <Scatter data={points} isAnimationActive={false}>
              {points.map((p, i) => (
                <Cell key={i}
                  fill={colourMode === 'age' ? ageColour(p.ts) : signalColour(p.signal)}
                  fillOpacity={0.7} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 2. Signal strength percentile bands (dBFS) — coverage edge is the key line
// ---------------------------------------------------------------------------
function RangeTrend({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/coverage/range_trend?days=${days}`)

  const points = useMemo(() => (data || []).map(d => ({
    label:  d.date,
    max_nm: d.max_nm,
    avg_nm: d.avg_nm,
  })), [data])

  return (
    <Card
      title="Daily max range (nm)"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[30, 90, 180, 365]} />}
    >
      {!points.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={points} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="label" tick={{ fill: '#484f58', fontSize: 10 }}
              interval={Math.max(0, Math.floor(points.length / 8))} />
            <YAxis tick={{ fill: '#484f58', fontSize: 11 }} width={44}
              tickFormatter={v => `${v}`} unit=" nm" />
            <Tooltip
              contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={(v, name) => [`${v} nm`, name]}
            />
            <Area type="monotone" dataKey="max_nm" name="Max range" stroke="#388bfd"
              fill="#388bfd18" strokeWidth={2} dot={false} isAnimationActive={false} />
            <Area type="monotone" dataKey="avg_nm" name="Mean range" stroke="#3fb950"
              fill="#3fb95010" strokeWidth={1.5} dot={false} isAnimationActive={false}
              strokeDasharray="4 2" />
            <Legend wrapperStyle={{ fontSize: 11, color: '#8b949e' }} />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 3. Live DF type breakdown (current minute message counts by DF type)
// ---------------------------------------------------------------------------
const DF_LABELS = {
  0: 'DF0 — Short ACAS', 4: 'DF4 — Surv Alt', 5: 'DF5 — Surv ID',
  11: 'DF11 — All-Call', 16: 'DF16 — Long ACAS', 17: 'DF17 — ADS-B',
  18: 'DF18 — TIS-B', 20: 'DF20 — Comm-B Alt', 21: 'DF21 — Comm-B ID',
  24: 'DF24 — Comm-D',
}

function LiveDFBreakdown({ snapshot }) {
  const data = useMemo(() => {
    const history = snapshot?.df_history ?? []
    if (!history.length) return []

    // Build a rolling 60-second window by blending the current partial minute
    // with a proportional share of the previous completed minute.
    const cur = history[history.length - 1]
    const prev = history.length >= 2 ? history[history.length - 2] : null

    const curCounts = cur?.counts ?? {}
    const prevCounts = prev?.counts ?? {}

    // Seconds elapsed since current minute started
    const curMinStart = (cur?.minute ?? 0) * 60
    const secsIntoCurMin = Math.min(59, Math.max(0, Math.floor(Date.now() / 1000) - curMinStart))
    const prevWeight = (60 - secsIntoCurMin) / 60

    const allDfs = new Set([...Object.keys(curCounts), ...Object.keys(prevCounts)])
    const counts = {}
    allDfs.forEach(df => {
      counts[df] = Math.round((curCounts[df] ?? 0) + (prevCounts[df] ?? 0) * prevWeight)
    })

    return Object.entries(counts)
      .map(([df, count]) => ({ label: DF_LABELS[Number(df)] ?? `DF${df}`, count, df: Number(df) }))
      .sort((a, b) => b.count - a.count)
  }, [snapshot?.df_history])

  return (
    <Card title="Live message types — rolling 60s">
      {!data.length ? <Empty loading={false} /> : (
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={data} layout="vertical" margin={{ top: 4, right: 48, bottom: 4, left: 0 }}>
            <CartesianGrid stroke="#21262d" horizontal={false} />
            <XAxis type="number" tick={{ fill: '#484f58', fontSize: 10 }} allowDecimals={false} />
            <YAxis type="category" dataKey="label" width={148} tick={{ fill: '#8b949e', fontSize: 10 }} tickLine={false} />
            <Tooltip
              contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={v => [v.toLocaleString(), 'msgs']}
            />
            <Bar dataKey="count" fill="#388bfd" isAnimationActive={false} radius={[0, 3, 3, 0]} />
          </BarChart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 4. Range vs altitude scatter (live, from WebSocket) — coloured by WTC
// ---------------------------------------------------------------------------
const WTC_COLOUR = { L: '#3fb950', M: '#388bfd', H: '#d29922', J: '#f85149' }
const WTC_LABEL  = { L: 'Light', M: 'Medium', H: 'Heavy', J: 'Super' }
function wtcColour(wtc) { return WTC_COLOUR[wtc] ?? '#484f58' }

// Radio horizon: range_nm = 1.23 × sqrt(alt_ft)  →  alt_ft = (range_nm / 1.23)²
// Aircraft ABOVE this curve are in the expected visible zone; those BELOW flag
// impossible reception geometry (likely bad decodes or ground reflections).
function buildHorizonSeries(maxRange) {
  const pts = []
  for (let r = 0; r <= maxRange; r += 5) {
    pts.push({ range: r, horizon: Math.round((r / 1.23) ** 2) })
  }
  return pts
}

function RangeAltScatter({ aircraft }) {
  const points = useMemo(() =>
    (aircraft || [])
      .filter(ac => ac.range_nm != null && ac.altitude != null)
      .map(ac => ({ range: ac.range_nm, alt: ac.altitude, wtc: ac.wtc, callsign: ac.callsign, type_code: ac.type_code })),
  [aircraft])

  const maxRange = useMemo(() => Math.max(50, ...points.map(p => p.range)), [points])
  const horizonSeries = useMemo(() => buildHorizonSeries(maxRange), [maxRange])

  return (
    <Card title="Range vs altitude — live (nm vs ft, coloured by WTC)">
      {!points.length ? (
        <div className={styles.empty}>
          {aircraft?.length
            ? 'No position data yet — set RECEIVER_LAT/RECEIVER_LON for faster decoding, or wait for even+odd CPR frame pairs.'
            : 'No aircraft tracked.'}
        </div>
      ) : (
        <>
          <ResponsiveContainer width="100%" height={260}>
            <ComposedChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
              <CartesianGrid stroke="#21262d" />
              <XAxis dataKey="range" name="Range" type="number"
                label={{ value: 'Range (nm)', position: 'insideBottom', offset: -12, fill: '#484f58', fontSize: 11 }}
                tick={{ fill: '#484f58', fontSize: 11 }} />
              <YAxis name="Altitude" type="number"
                tick={{ fill: '#484f58', fontSize: 11 }} width={56}
                tickFormatter={v => `${(v / 1000).toFixed(0)}k`} />
              <ZAxis range={[24, 24]} />
              <Tooltip cursor={{ stroke: '#30363d' }}
                content={({ payload }) => {
                  if (!payload?.length) return null
                  const d = payload[0].payload
                  if (d.horizon != null) return null
                  return (
                    <div className={styles.tooltip}>
                      <div>Range: {d.range} nm</div>
                      <div>Altitude: {d.alt.toLocaleString()} ft</div>
                      {d.wtc && <div>WTC: {WTC_LABEL[d.wtc] ?? d.wtc}</div>}
                      {d.type_code && <div>Type: {d.type_code}</div>}
                    </div>
                  )
                }}
              />
              <Line data={horizonSeries} dataKey="horizon" dot={false}
                stroke="#484f58" strokeWidth={1} strokeDasharray="4 3"
                isAnimationActive={false} legendType="none" />
              <Scatter data={points} dataKey="alt" isAnimationActive={false}>
                {points.map((p, i) => (
                  <Cell key={i} fill={wtcColour(p.wtc)} fillOpacity={0.75} />
                ))}
              </Scatter>
            </ComposedChart>
          </ResponsiveContainer>
          <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', marginTop: '0.5rem', flexWrap: 'wrap' }}>
            {Object.entries(WTC_LABEL).map(([k, v]) => (
              <span key={k} style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: '#8b949e' }}>
                <span style={{ width: 10, height: 10, borderRadius: '50%', background: WTC_COLOUR[k], display: 'inline-block' }} />
                {k} — {v}
              </span>
            ))}
            <span style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: '#8b949e' }}>
              <span style={{ width: 10, height: 10, borderRadius: '50%', background: '#484f58', display: 'inline-block' }} />
              Unknown
            </span>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 5. Range percentiles by bearing
// ---------------------------------------------------------------------------
function RangePercentiles({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/coverage/range_percentiles?days=${days}`)

  return (
    <Card
      title="Range percentiles by bearing (p50 / p90 / p95)"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[7, 14, 30, 90]} />}
    >
      {!data?.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={data} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="bearing" tick={{ fill: '#484f58', fontSize: 10 }}
              tickFormatter={v => `${v}°`}
              ticks={[0, 45, 90, 135, 180, 225, 270, 315, 360]} />
            <YAxis tick={{ fill: '#484f58', fontSize: 11 }} width={52}
              tickFormatter={v => `${v} nm`} />
            <Tooltip
              contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={(v, name) => [`${v} nm`, name]}
              labelFormatter={v => `Bearing: ${v}°`}
            />
            <Area type="monotone" dataKey="p95" name="p95" stroke="#bc8cff"
              fill="#bc8cff18" strokeWidth={1} dot={false} isAnimationActive={false} />
            <Area type="monotone" dataKey="p90" name="p90" stroke="#d29922"
              fill="#d2992218" strokeWidth={1.5} dot={false} isAnimationActive={false} />
            <Area type="monotone" dataKey="p50" name="Median (p50)" stroke="#3fb950"
              fill="#3fb95018" strokeWidth={2} dot={false} isAnimationActive={false} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#8b949e' }} />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 6. Polar coverage heatmap — binned arc cells, max-range envelope
// ---------------------------------------------------------------------------
function arcPath(cx, cy, r1, r2, aDeg1, aDeg2) {
  const a1 = (aDeg1 - 90) * Math.PI / 180
  const a2 = (aDeg2 - 90) * Math.PI / 180
  const c1 = Math.cos(a1), s1 = Math.sin(a1)
  const c2 = Math.cos(a2), s2 = Math.sin(a2)
  const large = (aDeg2 - aDeg1) > 180 ? 1 : 0
  if (r1 < 0.5) {
    return `M ${cx} ${cy} L ${cx + r2*c1} ${cy + r2*s1} A ${r2} ${r2} 0 ${large} 1 ${cx + r2*c2} ${cy + r2*s2} Z`
  }
  return [
    `M ${cx + r1*c1} ${cy + r1*s1}`,
    `A ${r1} ${r1} 0 ${large} 1 ${cx + r1*c2} ${cy + r1*s2}`,
    `L ${cx + r2*c2} ${cy + r2*s2}`,
    `A ${r2} ${r2} 0 ${large} 0 ${cx + r2*c1} ${cy + r2*s1}`,
    'Z',
  ].join(' ')
}

function binColour(count, maxCount) {
  if (!count || !maxCount) return 'transparent'
  const t = count / maxCount
  return `hsl(210,80%,${Math.round(10 + t * 65)}%)`
}

const COMPASS = [
  { label: 'N',   deg: 0     }, { label: 'NNE', deg: 22.5  },
  { label: 'NE',  deg: 45    }, { label: 'ENE', deg: 67.5  },
  { label: 'E',   deg: 90    }, { label: 'ESE', deg: 112.5 },
  { label: 'SE',  deg: 135   }, { label: 'SSE', deg: 157.5 },
  { label: 'S',   deg: 180   }, { label: 'SSW', deg: 202.5 },
  { label: 'SW',  deg: 225   }, { label: 'WSW', deg: 247.5 },
  { label: 'W',   deg: 270   }, { label: 'WNW', deg: 292.5 },
  { label: 'NW',  deg: 315   }, { label: 'NNW', deg: 337.5 },
]

function PolarCoverage({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/coverage/polar_bins?days=${days}&sectors=32`)
  const [hover, setHover] = useState(null)
  const wrapperRef = useRef(null)

  const SIZE = 500
  const CX = SIZE / 2
  const CY = SIZE / 2
  const R  = SIZE / 2 - 48

  const { bins, maxRange, sectors, bands } = useMemo(() => ({
    bins:     data?.bins     ?? [],
    maxRange: data?.max_range ?? 0,
    sectors:  data?.sectors  ?? 36,
    bands:    data?.bands    ?? 10,
  }), [data])

  const sectorWidth = 360 / sectors
  const maxCount = useMemo(() => bins.reduce((m, b) => Math.max(m, b.count), 1), [bins])

  const rings = useMemo(() => {
    if (!maxRange) return []
    const out = []
    for (let r = 25; r <= maxRange; r += 25) out.push(r)
    return out
  }, [maxRange])

  return (
    <Card
      title="Polar coverage — directional heatmap"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[7, 14, 30, 90]} />}
    >
      {loading ? <div className={styles.empty}>Loading…</div>
       : !bins.length ? (
        <div className={styles.empty}>
          No coverage data yet — requires RECEIVER_LAT/RECEIVER_LON and at least one minute write with aircraft positions.
        </div>
       ) : (
        <>
          <div ref={wrapperRef} style={{ position: 'relative' }}>
          <svg
            viewBox={`0 0 ${SIZE} ${SIZE}`}
            style={{ width: '100%', maxWidth: 600, aspectRatio: '1', display: 'block', margin: '0 auto' }}
            onMouseLeave={() => setHover(null)}
          >
            {/* Heatmap cells — one arc per (bearing sector × range band) bin, skip 360°+ wrapping */}
            {bins.filter(({ b }) => b * sectorWidth < 360).map(({ b, r, count }) => (
              <path
                key={`${b}-${r}`}
                d={arcPath(CX, CY, (r / bands) * R, ((r + 1) / bands) * R,
                           b * sectorWidth, (b + 1) * sectorWidth)}
                fill={binColour(count, maxCount)}
                stroke="none"
                onMouseEnter={e => {
                  const rect = wrapperRef.current.getBoundingClientRect()
                  setHover({
                    count,
                    bearing1: Math.round(b * sectorWidth),
                    bearing2: Math.round((b + 1) * sectorWidth),
                    range1: Math.round(r * (data.max_range / bands)),
                    range2: Math.round((r + 1) * (data.max_range / bands)),
                    x: e.clientX - rect.left,
                    y: e.clientY - rect.top,
                  })
                }}
              />
            ))}
            {/* Range rings at 25nm intervals */}
            {rings.map(r => (
              <g key={r}>
                <circle cx={CX} cy={CY} r={(r / maxRange) * R}
                  fill="none" stroke="#21262d" strokeWidth={1} />
                <text x={CX + 4} y={CY - (r / maxRange) * R + 14}
                  fill="#484f58" fontSize={10} textAnchor="start">{r}</text>
              </g>
            ))}
            {/* Bearing lines at 22.5° intervals */}
            {COMPASS.map(({ deg }) => {
              const ang = (deg - 90) * Math.PI / 180
              return <line key={deg} x1={CX} y1={CY}
                x2={CX + R * Math.cos(ang)} y2={CY + R * Math.sin(ang)}
                stroke="#21262d" strokeWidth={1} />
            })}
            {/* 16-point compass labels */}
            {COMPASS.map(({ label, deg }) => {
              const rad = (deg - 90) * Math.PI / 180
              const offset = label.length > 1 ? R + 22 : R + 18
              const fontSize = label.length > 2 ? 9 : label.length > 1 ? 10 : 14
              const fontWeight = label.length === 1 ? 700 : 400
              return (
                <text
                  key={label}
                  x={CX + offset * Math.cos(rad)}
                  y={CY + offset * Math.sin(rad)}
                  fill={label.length === 1 ? '#8b949e' : '#484f58'}
                  fontSize={fontSize}
                  fontWeight={fontWeight}
                  textAnchor="middle"
                  dominantBaseline="middle"
                >{label}</text>
              )
            })}
          </svg>
          {hover && (
            <div style={{
              position: 'absolute', left: hover.x + 14, top: hover.y - 8,
              background: '#161b22', border: '1px solid #30363d', borderRadius: 4,
              padding: '4px 8px', fontSize: 12, color: '#c9d1d9', pointerEvents: 'none',
              whiteSpace: 'nowrap',
            }}>
              <div>{hover.bearing1}°–{hover.bearing2}°</div>
              <div>{hover.range1}–{hover.range2} nm</div>
              <div style={{ color: '#8b949e' }}>{hover.count.toLocaleString()} positions received</div>
            </div>
          )}
          </div>
          {/* Legend */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', justifyContent: 'center', marginTop: '0.75rem' }}>
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>Few</span>
            <div style={{ background: 'linear-gradient(to right, hsl(210,80%,10%), hsl(210,80%,45%), hsl(210,80%,75%))', height: 8, borderRadius: 4, width: 120 }} />
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>Many</span>
          </div>
        </>
       )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 7. Performance distribution box plots (msgs, aircraft, range, signal)
// ---------------------------------------------------------------------------
const BP_WINDOWS = ['1d', '7d', '30d', '365d']
const BP_W = 300, BP_H = 160
const BP_ML = 50, BP_MR = 12, BP_MT = 14, BP_MB = 28
const BP_PLOT_W = BP_W - BP_ML - BP_MR
const BP_PLOT_H = BP_H - BP_MT - BP_MB
const BP_COL_W  = BP_PLOT_W / BP_WINDOWS.length
const BP_BOX_H  = BP_COL_W * 0.22

// Returns nice rounded tick values between lo and hi (1/2/5 × power-of-10 steps)
function niceTicks(lo, hi) {
  const rawStep = (hi - lo) / 4
  const mag = Math.pow(10, Math.floor(Math.log10(rawStep || 1)))
  const norm = rawStep / mag
  const step = norm <= 1.5 ? mag : norm <= 3.5 ? 2 * mag : norm <= 7.5 ? 5 * mag : 10 * mag
  const ticks = []
  for (let t = Math.ceil(lo / step) * step; t <= hi + step * 0.01; t += step) ticks.push(t)
  return ticks
}

function BoxPlotSVG({ data, formatVal, minZero = true }) {
  const allVals = BP_WINDOWS.flatMap(w => {
    const d = data?.[w]
    return d ? [d.p5, d.p95].filter(v => v != null) : []
  })
  if (!allVals.length) return <div className={styles.empty}>No data yet</div>

  const lo = Math.min(...allVals), hi = Math.max(...allVals)
  const pad = (hi - lo) * 0.15 || 1
  const yLo = (minZero ? Math.max(0, lo - pad) : lo - pad), yHi = hi + pad
  const sy = v => BP_MT + BP_PLOT_H - ((v - yLo) / (yHi - yLo)) * BP_PLOT_H
  const ticks = niceTicks(yLo, yHi)

  return (
    <svg viewBox={`0 0 ${BP_W} ${BP_H}`} style={{ width: '100%', height: BP_H }}>
      {ticks.map((t, i) => (
        <g key={i}>
          <line x1={BP_ML} x2={BP_ML + BP_PLOT_W} y1={sy(t)} y2={sy(t)} stroke="#21262d" strokeWidth={1} />
          <text x={BP_ML - 5} y={sy(t)} fill="#484f58" fontSize={9} textAnchor="end" dominantBaseline="middle">
            {formatVal(t)}
          </text>
        </g>
      ))}
      {BP_WINDOWS.map((w, i) => {
        const d = data?.[w]
        const cx = BP_ML + (i + 0.5) * BP_COL_W
        if (!d || d.p50 == null) return (
          <text key={w} x={cx} y={BP_MT + BP_PLOT_H / 2} fill="#484f58" fontSize={10}
            textAnchor="middle" dominantBaseline="middle">—</text>
        )
        const x1 = cx - BP_BOX_H, x2 = cx + BP_BOX_H
        const cap1 = cx - BP_BOX_H * 0.5, cap2 = cx + BP_BOX_H * 0.5
        return (
          <g key={w}>
            {/* Whiskers p5–p25 and p75–p95 */}
            <line x1={cx} x2={cx} y1={sy(d.p5)}  y2={sy(d.p25)} stroke="#484f58" strokeWidth={1.5} />
            <line x1={cap1} x2={cap2} y1={sy(d.p5)}  y2={sy(d.p5)}  stroke="#484f58" strokeWidth={1} />
            <line x1={cx} x2={cx} y1={sy(d.p75)} y2={sy(d.p95)} stroke="#484f58" strokeWidth={1.5} />
            <line x1={cap1} x2={cap2} y1={sy(d.p95)} y2={sy(d.p95)} stroke="#484f58" strokeWidth={1} />
            {/* IQR box p25–p75 */}
            <rect x={x1} y={sy(d.p75)} width={BP_BOX_H * 2}
              height={Math.max(1, sy(d.p25) - sy(d.p75))}
              fill="#1c2128" stroke="#388bfd" strokeWidth={1.5} rx={2} />
            {/* Median */}
            <line x1={x1} x2={x2} y1={sy(d.p50)} y2={sy(d.p50)} stroke="#388bfd" strokeWidth={2.5} />
            {/* Mean dot */}
            {d.mean != null && <circle cx={cx} cy={sy(d.mean)} r={3} fill="#d29922" />}
            {/* Window label */}
            <text x={cx} y={BP_H - 6} fill="#8b949e" fontSize={11} textAnchor="middle">{w}</text>
          </g>
        )
      })}
    </svg>
  )
}

const DIST_METRICS = [
  { key: 'msgs',     label: 'Messages per second', fmt: v => v.toFixed(1) },
  { key: 'aircraft', label: 'Aircraft visible',    fmt: v => Math.round(v) },
  { key: 'range',    label: 'Range (nm)',           fmt: v => `${Math.round(v)} nm` },
]

function DistributionStats() {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/distributions`)

  return (
    <Card title="Performance distributions — 1d / 7d / 30d">
      {loading || !data ? <Empty loading={loading} /> : (
        <>
          {DIST_METRICS.map(({ key, label, fmt }, i) => (
            <div key={key}>
              {i > 0 && <div style={{ borderTop: '1px solid #21262d', margin: '0.75rem 0 0.5rem' }} />}
              <div style={{ fontSize: '0.72rem', color: '#8b949e', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: '0.25rem' }}>
                {label}
              </div>
              <BoxPlotSVG data={data[key]} formatVal={fmt} />
            </div>
          ))}
          <div style={{ display: 'flex', gap: '1.25rem', marginTop: '0.75rem', fontSize: '0.72rem', color: '#484f58', flexWrap: 'wrap' }}>
            <span><span style={{ display: 'inline-block', width: 20, height: 2.5, background: '#388bfd', verticalAlign: 'middle', marginRight: 4 }} />Median</span>
            <span><span style={{ display: 'inline-block', width: 12, height: 12, border: '1.5px solid #388bfd', background: '#1c2128', verticalAlign: 'middle', marginRight: 4 }} />IQR p25–p75</span>
            <span><span style={{ display: 'inline-block', width: 7, height: 7, borderRadius: '50%', background: '#d29922', verticalAlign: 'middle', marginRight: 4 }} />Mean</span>
            <span>Whiskers: p5–p95</span>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 8. Baseline comparison
// ---------------------------------------------------------------------------
function BaselineComparison({ snapshot }) {
  const { data: baseline, loading } = useFetch(`${API_BASE}/api/history/receiver/baseline`)

  const currentHour = new Date().getHours()
  const base = baseline?.find(b => b.hour === currentHour)
  const aircraft = snapshot?.aircraft || []

  const current = snapshot ? {
    ac:      snapshot.aircraft_count,
    msgs:    snapshot.msg_per_sec,
    mlat:    snapshot.mlat_aircraft_count,
    withPos: aircraft.filter(a => a.lat != null).length,
    today:   snapshot.unique_today,
  } : null

  function Stat({ label, current, baseline, unit = '' }) {
    if (current == null) return null
    const fmt = v => Math.round(v)
    const diff = baseline != null ? current - baseline : null
    const pct  = diff != null && Math.abs(baseline) > 0.1 ? Math.round((diff / Math.abs(baseline)) * 100) : null
    const colour = diff == null ? '#8b949e' : diff >= 0 ? '#3fb950' : '#f85149'
    return (
      <div className={styles.baselineStat}>
        <div className={styles.baselineLabel}>{label}</div>
        <div className={styles.baselineCurrent}>{fmt(current)}{unit}</div>
        {baseline != null && <div className={styles.baselineRef}>30d avg: {fmt(baseline)}{unit}</div>}
        {pct != null && (
          <div className={styles.baselineDiff} style={{ color: colour }}>
            {diff >= 0 ? '+' : ''}{pct}%
          </div>
        )}
      </div>
    )
  }

  return (
    <Card title={`Current vs 30-day baseline — hour ${currentHour}:00`}>
      {loading || !base || !current ? <Empty loading={loading} /> : (
        <div className={styles.baselineGrid}>
          <Stat label="Aircraft"    current={current.ac}      baseline={base.ac_avg} />
          <Stat label="Msgs/s"      current={current.msgs}    baseline={base.msg_avg} />
          <Stat label="MLAT"        current={current.mlat}    baseline={base.mlat_avg} />
          <Stat label="With pos"    current={current.withPos} />
          <Stat label="Unique today" current={current.today} />
        </div>
      )}
    </Card>
  )
}


// ---------------------------------------------------------------------------
// 10. Reception completeness
// ---------------------------------------------------------------------------
function ReceptionCompleteness({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/completeness?days=${days}`)
  return (
    <Card
      title="Reception completeness — % of minutes with data"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[30, 90, 365]} />}
    >
      {!data?.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="date" tick={{ fill: '#484f58', fontSize: 10 }}
              interval={Math.max(0, Math.floor(data.length / 6))} />
            <YAxis domain={[0, 100]} tick={{ fill: '#484f58', fontSize: 11 }} width={44}
              tickFormatter={v => `${v}%`} />
            <Tooltip contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={v => [`${v}%`, 'completeness']} />
            <Area type="monotone" dataKey="pct" name="Completeness" stroke="#388bfd"
              fill="#388bfd20" strokeWidth={2} dot={false} isAnimationActive={false} />
          </AreaChart>
        </ResponsiveContainer>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 11. Position decode rate
// ---------------------------------------------------------------------------
function PositionDecodeRate({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/position_decode_rate?days=${days}`)

  const chartData = useMemo(() => {
    if (!data?.length) return []
    return data.map(d => ({
      date: d.date,
      ...(() => {
        const [adsb, mlat, no_pos] = normalizePercentTriplet(d.adsb_pct, d.mlat_pct, d.no_pos_pct)
        return { adsb, mlat, no_pos }
      })(),
    }))
  }, [data])

  const hasMLAT = useMemo(() => chartData.some(d => d.mlat > 0), [chartData])

  return (
    <Card
      title="Position decode rate — ADS-B / MLAT / no position"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[30, 90, 365]} />}
    >
      {!chartData.length ? <Empty loading={loading} /> : (
        <>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={chartData} margin={{ top: 8, right: 12, bottom: 8, left: 8 }}
              barCategoryGap="20%">
              <CartesianGrid stroke="#21262d" vertical={false} />
              <XAxis dataKey="date" tick={{ fill: '#484f58', fontSize: 10 }}
                interval={Math.max(0, Math.floor(chartData.length / 6))} />
              <YAxis domain={[0, 100]} ticks={[0, 25, 50, 75, 100]}
                tick={{ fill: '#484f58', fontSize: 11 }} width={52}
                tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
                formatter={(v, name) => [`${v.toFixed(1)}%`, name]}
              />
              <Bar dataKey="adsb"   name="ADS-B"       stackId="s" fill="#388bfd" isAnimationActive={false} />
              {hasMLAT && <Bar dataKey="mlat" name="MLAT" stackId="s" fill="#bc8cff" isAnimationActive={false} />}
              <Bar dataKey="no_pos" name="No position"  stackId="s" fill="#21262d" isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
          <div style={{ display: 'flex', gap: '1rem', justifyContent: 'center', marginTop: '0.5rem', fontSize: '0.72rem', color: '#484f58' }}>
            <span><span style={{ color: '#388bfd' }}>■</span> ADS-B positioned</span>
            {hasMLAT && <span><span style={{ color: '#bc8cff' }}>■</span> MLAT positioned</span>}
            <span><span style={{ color: '#30363d' }}>■</span> No position</span>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Interrogator codes panel
// ---------------------------------------------------------------------------
export function InterrogatorCodes({ streamData = null, windowS: windowSProp = undefined, onWindowSChange = undefined }) {
  const [windowSLocal, setWindowSLocal] = useState(600)
  const windowS = windowSProp ?? windowSLocal
  const handleWindowChange = (v) => { setWindowSLocal(v); onWindowSChange?.(v) }
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchData = useCallback((showLoading = false) => {
    if (showLoading) setLoading(true)
    fetch(`${API_BASE}/api/interrogators?window_s=${windowS}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (d) setData(d)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [windowS])

  useEffect(() => {
    if (streamData !== null) { setLoading(false); return }
    setData(null)
    fetchData(true)
    const id = setInterval(() => fetchData(false), 1000)
    return () => clearInterval(id)
  }, [fetchData, streamData])

  useEffect(() => {
    if (!streamData) return
    const lanes = streamData.lanes ?? []
    const nowUs = Number(streamData.now_us ?? 0)
    const wallNow = Date.now() / 1000
    const codes = lanes
      .filter(lane => lane.arrivals_us.length > 0)
      .map(lane => {
        const lastArrivalUs = lane.arrivals_us[lane.arrivals_us.length - 1]
        const ageS = nowUs > 0 && lastArrivalUs ? (nowUs - lastArrivalUs) / 1e6 : 0
        return { iid: lane.iid, count: lane.arrivals_us.length, last_seen: wallNow - ageS, latest_icao: lane.latest_icao }
      })
      .sort((a, b) => b.count - a.count)
    const total = codes.reduce((s, c) => s + c.count, 0)
    setData({ window_s: streamData.window_s, total, codes })
    setLoading(false)
  }, [streamData])

  const codes = data?.codes ?? []
  const sortedCodes = useMemo(
    () => [...codes].sort((a, b) => a.iid - b.iid),
    [codes]
  )
  const total = data?.total ?? 0
  const maxCount = Math.max(1, ...codes.map(c => c.count))
  const now = Date.now() / 1000

  return (
    <Card
      title="DF11 Interrogator Codes (IID)"
      controls={
        <select className={styles.select} value={windowS} onChange={e => handleWindowChange(Number(e.target.value))}>
          {streamData !== null ? (
            <>
              <option value={5}>5 s</option>
              <option value={10}>10 s</option>
              <option value={20}>20 s</option>
              <option value={30}>30 s</option>
              <option value={60}>60 s</option>
            </>
          ) : (
            <>
              <option value={60}>1 min</option>
              <option value={300}>5 min</option>
              <option value={600}>10 min</option>
              <option value={1800}>30 min</option>
              <option value={3600}>1 hr</option>
            </>
          )}
        </select>
      }
    >
      {loading && <Empty loading />}
      {!loading && codes.length === 0 && (
        <Empty />
      )}
      {!loading && codes.length > 0 && (
        <>
          <p style={{ fontSize: '0.75rem', color: '#484f58', margin: '0 0 0.6rem' }}>
            {total.toLocaleString()} DF11 replies · {codes.length} IID{codes.length !== 1 ? 's' : ''} active
          </p>
          <ResponsiveContainer width="100%" height={Math.min(sortedCodes.length * 28 + 24, 300)}>
            <BarChart
              data={sortedCodes.map(c => ({ name: `IID ${c.iid}`, count: c.count }))}
              layout="vertical"
              margin={{ top: 4, right: 40, bottom: 4, left: 52 }}
            >
              <CartesianGrid stroke="#21262d" horizontal={false} />
              <XAxis type="number" tick={{ fill: '#484f58', fontSize: 10 }}
                domain={[0, maxCount]}
                tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(1)}k` : v}
              />
              <YAxis type="category" dataKey="name" width={52}
                tick={{ fill: '#8b949e', fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
                formatter={v => [v.toLocaleString(), 'replies']}
              />
              <Bar dataKey="count" fill="#388bfd" radius={2} isAnimationActive={false}>
                {sortedCodes.map(c => (
                  <Cell key={c.iid} fill={c.iid === 0 ? '#3fb950' : '#388bfd'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className={styles.iidTableWrap}>
            <table className={styles.iidTable}>
              <thead>
                <tr>
                  <th>IID</th>
                  <th>Replies</th>
                  <th>Last seen</th>
                  <th>Aircraft</th>
                </tr>
              </thead>
              <tbody>
                {sortedCodes.map(code => (
                  <tr key={code.iid}>
                    <td>
                      <span
                        className={styles.iidCode}
                        style={{ color: code.iid === 0 ? '#3fb950' : '#58a6ff' }}
                      >
                        IID {code.iid}
                      </span>
                    </td>
                    <td className={styles.iidNum}>{code.count.toLocaleString()}</td>
                    <td>{formatAgeShort(now - Number(code.last_seen ?? 0))}</td>
                    <td className={styles.iidCode}>{code.latest_icao || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p style={{ fontSize: '0.7rem', color: '#484f58', margin: '0.4rem 0 0' }}>
            IID 0 (green) = no interrogator code / civil ATC · IID 1–127 = specific SSR interrogator
          </p>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Interrogator timing lanes (canvas)
// ---------------------------------------------------------------------------

const LANE_H    = 22   // px per IID lane
const LANE_PAD  = 4    // top/bottom padding inside each lane
const LABEL_W   = 52   // px for IID label on the left
const TICK_W    = 2    // px tick width
const TICK_H    = LANE_H - LANE_PAD * 2  // tick height
const LIVE_RENDER_HOLDBACK_US = 650_000

export function InterrogatorTimeline({ onSelectIcao, streamData = null, windowS: windowSProp = undefined, onWindowSChange = undefined }) {
  const canvasRef  = useRef(null)
  const lanesRef   = useRef(Array.from({ length: 128 }, (_, iid) => ({
    iid,
    latest_icao: '',
    arrivals_us: [],
    last_active_us: 0,
  })))
  const nowUsRef   = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const rafRef     = useRef(null)
  const [windowSLocal, setWindowSLocal] = useState(10)
  const windowS = windowSProp ?? windowSLocal
  const handleWindowChange = (v) => { setWindowSLocal(v); onWindowSChange?.(v) }
  const [data,    setData]    = useState(null)
  const retryRef  = useRef(null)

  const ingestInterrogatorPacket = useCallback((d) => {
    const nextNowUs = Number(d?.now_us ?? nowUsRef.current)
    nowUsRef.current = nextNowUs
    nowUsWallRef.current = performance.now()
    const winUs = Number(d?.window_s ?? windowS) * 1_000_000
    const cutoffUs = nextNowUs - winUs - LIVE_RENDER_HOLDBACK_US - 500_000
    const nextByIid = new Map((d?.lanes ?? []).map(lane => [lane.iid, lane]))
    lanesRef.current = lanesRef.current.map(prev => {
      const incoming = nextByIid.get(prev.iid)
      if (!incoming) {
        const arrivals = prev.arrivals_us.filter(arrivalUs => arrivalUs >= cutoffUs)
        return {
          ...prev,
          arrivals_us: arrivals,
          latest_icao: arrivals.length ? prev.latest_icao : '',
        }
      }
      const mergedArrivals = [...prev.arrivals_us, ...(incoming.arrivals_us ?? [])]
      const dedupedArrivals = [...new Set(mergedArrivals)]
        .filter(arrivalUs => arrivalUs >= cutoffUs)
        .sort((a, b) => a - b)
      return {
        iid: prev.iid,
        latest_icao: incoming.latest_icao || prev.latest_icao || '',
        arrivals_us: dedupedArrivals,
        last_active_us: dedupedArrivals.length ? dedupedArrivals[dedupedArrivals.length - 1] : prev.last_active_us,
      }
    })
    setData(d)
  }, [windowS])

  useEffect(() => {
    if (streamData) return
    let ws
    let closed = false

    const connect = () => {
      if (closed) return
      ws = new WebSocket(INTERROGATOR_WS_URL)

      ws.onopen = () => {
        ws.send(JSON.stringify({ window_s: windowS }))
      }

      ws.onmessage = (event) => {
        try {
          ingestInterrogatorPacket(JSON.parse(event.data))
        } catch {
          // ignore malformed frames
        }
      }

      ws.onclose = () => {
        if (closed) return
        retryRef.current = setTimeout(connect, 1000)
      }

      ws.onerror = () => ws.close()
    }

    connect()
    return () => {
      closed = true
      clearTimeout(retryRef.current)
      ws?.close()
    }
  }, [ingestInterrogatorPacket, streamData, windowS])

  useEffect(() => {
    if (!streamData) return
    ingestInterrogatorPacket(streamData)
  }, [ingestInterrogatorPacket, streamData])

  // Draw canvas
  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas || !data?.lanes) return

      const lanes  = lanesRef.current
      const nowUs  = nowUsRef.current + Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUs - LIVE_RENDER_HOLDBACK_US)
      const winUs  = data.window_s * 1_000_000
      const h      = Math.max(LANE_H * lanes.length, LANE_H)
      const w      = canvas.offsetWidth || 600
      canvas.width  = w
      canvas.height = h

      const ctx = canvas.getContext('2d')
      ctx.clearRect(0, 0, w, h)
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const plotW = w - LABEL_W
      const tToX = arrivalUs => LABEL_W + ((arrivalUs - (renderNowUs - winUs)) / winUs) * plotW

      lanes.forEach((lane, row) => {
        const y0 = row * LANE_H
        const yC = y0 + LANE_H / 2

        ctx.fillStyle = row % 2 === 0 ? '#0f1117' : '#0b0c10'
        ctx.fillRect(0, y0, w, LANE_H)

        const active = lane.arrivals_us.length > 0
        ctx.fillStyle = !active ? '#484f58' : lane.iid === 0 ? '#3fb950' : '#388bfd'
        ctx.font = '10px monospace'
        ctx.textAlign = 'right'
        ctx.textBaseline = 'middle'
        ctx.fillText(`IID ${lane.iid}`, LABEL_W - 4, yC)

        ctx.fillStyle = lane.iid === 0 ? 'rgba(63,185,80,0.75)' : 'rgba(56,139,253,0.75)'
        for (const arrivalUs of lane.arrivals_us) {
          if (arrivalUs > renderNowUs) continue
          const x = tToX(arrivalUs)
          if (x < LABEL_W || x > w) continue
          ctx.fillRect(x - TICK_W / 2, yC - TICK_H / 2, TICK_W, TICK_H)
        }
      })

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      ctx.beginPath()
      ctx.moveTo(LABEL_W, h - 0.5)
      ctx.lineTo(w, h - 0.5)
      ctx.stroke()
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [data])

  if (!data) return null

  const activeCount = lanesRef.current.filter(lane => lane.arrivals_us.length > 0).length
  if (activeCount === 0) return (
    <Card title="Interrogator Timing Lanes">
      <p style={{ fontSize: '0.8rem', color: '#484f58', padding: '0.5rem 0' }}>
        No DF11 messages received yet
      </p>
    </Card>
  )

  const canvasH = Math.max(LANE_H * lanesRef.current.length, LANE_H)

  return (
    <Card
      title="Interrogator Timing Lanes"
      controls={
        <select className={styles.select} value={windowS} onChange={e => handleWindowChange(Number(e.target.value))}>
          <option value={5}>5 s</option>
          <option value={10}>10 s</option>
          <option value={20}>20 s</option>
          <option value={30}>30 s</option>
          <option value={60}>60 s</option>
        </select>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Each tick = one DF11 reply. Regular spacing reveals the SSR rotation period (~4 s for most sites).
      </p>
      <canvas
        ref={canvasRef}
        style={{ width: '100%', height: canvasH, display: 'block', borderRadius: 4, cursor: onSelectIcao ? 'pointer' : 'default' }}
        onClick={onSelectIcao ? (e) => {
          const canvas = canvasRef.current
          if (!canvas) return
          const rect = canvas.getBoundingClientRect()
          const y = e.clientY - rect.top
          const row = Math.floor(y / LANE_H)
          const lane = lanesRef.current[row]
          if (lane?.latest_icao) onSelectIcao(lane.latest_icao)
        } : undefined}
      />
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Message timing scroll plot (canvas)
// ---------------------------------------------------------------------------

// DF type colour map — consistent with DFHeatmap colours
const DF_COLOURS = {
  17: '#388bfd',   // ADS-B — blue
  18: '#57a6ff',   // TIS-B — lighter blue
  11: '#3fb950',   // All-Call — green
   4: '#d29922',   // Surv Alt — amber
   5: '#e3b341',   // Surv ID  — amber-yellow
  20: '#bc8cff',   // Comm-B Alt — purple
  21: '#d2a8ff',   // Comm-B ID  — lighter purple
   0: '#f85149',   // Short ACAS — red
  16: '#ff6b6b',   // Long ACAS  — lighter red
  24: '#6e7681',   // Comm-D     — gray
}
// Short labels for the timing canvas (compact)
const DF_SHORT = {
  17:'DF17 ADS-B', 18:'DF18 TIS-B', 11:'DF11 All-Call',
   4:'DF4 Alt',     5:'DF5 ID',     20:'DF20 Comm-B',
  21:'DF21 Comm-B', 0:'DF0 ACAS',  16:'DF16 ACAS', 24:'DF24 Comm-D',
}
const DEFAULT_DF_LANES = Object.keys(DF_SHORT).map(Number).sort((a, b) => a - b)
const PLOT_WIN_US  = 5_000_000
const TLANE_H      = 52    // px per DF lane — sized for 12-level stacking at 3px slot height
const TLABEL_W     = 88    // px label column
const MAX_BUF      = 15000 // client-side event cap
const TIMING_POLL_FALLBACK_MS = 250
const STACK_LEVELS = 12

export function MessageTimingPlot({ streamPacket = null }) {
  const canvasRef  = useRef(null)
  const bufRef     = useRef([])     // [{seq, arrival_us, df, msg_len}] rolling buffer
  const rafRef     = useRef(null)
  const activeRef  = useRef(true)   // Page Visibility guard
  const retryRef   = useRef(null)
  const pollRef    = useRef(null)
  const lastEventWallRef = useRef(0)
  const sinceSeqRef = useRef(0)
  const nowUsRef   = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const [laneOrder, setLaneOrder] = useState(DEFAULT_DF_LANES)

  // Polling — only when tab is visible
  useEffect(() => {
    const onVisibility = () => { activeRef.current = !document.hidden }
    document.addEventListener('visibilitychange', onVisibility)
    return () => document.removeEventListener('visibilitychange', onVisibility)
  }, [])

  const ingestTimingPacket = useCallback((d) => {
    nowUsRef.current = Number(d?.now_us ?? nowUsRef.current)
    nowUsWallRef.current = performance.now()
    const cutoff = nowUsRef.current - PLOT_WIN_US - LIVE_RENDER_HOLDBACK_US - 500_000
    const newEvents = (d?.events ?? []).map(ev => {
      const [seq, arrival_us, df, msg_len] = ev
      return { seq, arrival_us, df, msg_len: msg_len ?? null }
    })
    if (newEvents.length) {
      sinceSeqRef.current = Math.max(sinceSeqRef.current, newEvents[newEvents.length - 1].seq)
      lastEventWallRef.current = performance.now()
    }
    const mergedMap = new Map()
    for (const ev of bufRef.current) {
      if (ev.arrival_us > cutoff) mergedMap.set(ev.seq, ev)
    }
    for (const ev of newEvents) {
      if (ev.arrival_us > cutoff) mergedMap.set(ev.seq, ev)
    }
    const merged = [...mergedMap.values()].sort((a, b) => a.seq - b.seq)
    if (merged.length > MAX_BUF) merged.splice(0, merged.length - MAX_BUF)
    bufRef.current = merged

    const observed = new Set(DEFAULT_DF_LANES)
    for (const e of merged) observed.add(e.df)
    setLaneOrder([...observed].sort((a, b) => a - b))
  }, [])

  useEffect(() => {
    if (streamPacket) return
    let ws
    let closed = false

    const pollOnce = () => {
      if (closed || !activeRef.current) return
      fetch(`${API_BASE}/api/timing/events?since_seq=${sinceSeqRef.current}`)
        .then(r => r.ok ? r.json() : null)
        .then(d => { if (d) ingestTimingPacket(d) })
        .catch(() => {})
    }

    pollOnce()
    pollRef.current = setInterval(() => {
      if (closed) return
      if (performance.now() - lastEventWallRef.current > 1500) {
        pollOnce()
      }
    }, TIMING_POLL_FALLBACK_MS)

    const connect = () => {
      if (closed) return
      ws = new WebSocket(TIMING_WS_URL)

      ws.onmessage = (event) => {
        let d = null
        try {
          d = JSON.parse(event.data)
        } catch {
          return
        }
        ingestTimingPacket(d)
      }

      ws.onclose = () => {
        if (closed) return
        retryRef.current = setTimeout(connect, 1000)
      }

      ws.onerror = () => ws.close()
    }

    connect()
    return () => {
      closed = true
      clearTimeout(retryRef.current)
      clearInterval(pollRef.current)
      ws?.close()
    }
  }, [ingestTimingPacket, streamPacket])

  useEffect(() => {
    if (!streamPacket) return
    ingestTimingPacket(streamPacket)
  }, [ingestTimingPacket, streamPacket])

  // rAF-driven canvas draw
  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas || !activeRef.current || laneOrder.length === 0) return

      const nowUs  = nowUsRef.current + Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUs - LIVE_RENDER_HOLDBACK_US)
      const cutoff = renderNowUs - PLOT_WIN_US
      const buf    = bufRef.current
      const w = canvas.offsetWidth
      const h = TLANE_H * laneOrder.length
      if (!w || !h) return
      canvas.width  = w
      canvas.height = h

      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const plotW  = w - TLABEL_W
      const tToX   = arrivalUs => TLABEL_W + ((arrivalUs - cutoff) / PLOT_WIN_US) * plotW

      const laneEvents = {}
      for (const ev of buf) {
        if (!laneEvents[ev.df]) laneEvents[ev.df] = []
        laneEvents[ev.df].push(ev)
      }

      laneOrder.forEach((df, row) => {
        const colour = DF_COLOURS[df] ?? '#484f58'
        const y0 = row * TLANE_H

        // Row background
        ctx.fillStyle = row % 2 === 0 ? '#0f1117' : '#0b0c10'
        ctx.fillRect(0, y0, w, TLANE_H)

        // Label
        ctx.fillStyle = colour
        ctx.font = '10px monospace'
        ctx.textAlign = 'right'
        ctx.textBaseline = 'middle'
        ctx.fillText(DF_SHORT[df] ?? `DF${df}`, TLABEL_W - 4, y0 + TLANE_H / 2)

        // Message bars with width derived from message length and simple overlap stacking.
        const events = laneEvents[df] ?? []
        const levelLastEnd = new Array(STACK_LEVELS).fill(-Infinity)
        const slotGap = 1
        const innerTop = y0 + 2
        const innerHeight = TLANE_H - 4
        const slotHeight = Math.max(2, Math.floor((innerHeight - (STACK_LEVELS - 1) * slotGap) / STACK_LEVELS))
        const overflowY = innerTop + (STACK_LEVELS - 1) * (slotHeight + slotGap)
        const overflowH = Math.max(2, slotHeight - 1)

        for (const ev of events) {
          if (ev.arrival_us > renderNowUs) continue
          const x = tToX(ev.arrival_us)
          const barW = (ev.msg_len ?? 14) <= 7 ? 1 : 2
          if (x + barW < TLABEL_W || x > w) continue

          let level = levelLastEnd.findIndex(endX => x > endX + 1)
          if (level === -1) level = STACK_LEVELS - 1
          levelLastEnd[level] = x + barW

          const barY = level < STACK_LEVELS - 1
            ? innerTop + level * (slotHeight + slotGap)
            : overflowY
          const barH = level < STACK_LEVELS - 1 ? slotHeight : overflowH

          ctx.fillStyle = level < STACK_LEVELS - 1 ? colour : 'rgba(248,81,73,0.9)'
          ctx.fillRect(x, barY, barW, barH)
        }
      })

      // Time ruler ticks every 1s
      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      const startSec = Math.ceil(cutoff / 1_000_000)
      const endSec = Math.floor(renderNowUs / 1_000_000)
      for (let sec = startSec; sec <= endSec; sec++) {
        const x = tToX(sec * 1_000_000)
        ctx.beginPath()
        ctx.moveTo(x, 0)
        ctx.lineTo(x, h)
        ctx.stroke()
      }
    }
    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [laneOrder])

  const canvasH = Math.max(TLANE_H * laneOrder.length, TLANE_H)

  return (
    <Card title="Message Timing Scroll Plot">
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Live scroll — last 5 s in Beast-relative time · one lane per DF type · ticks = individual messages
      </p>
      {laneOrder.length === 0
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for messages…</p>
        : <canvas
            ref={canvasRef}
            style={{ width: '100%', height: canvasH, display: 'block', borderRadius: 4 }}
          />
      }
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Page layout
// ---------------------------------------------------------------------------
export default function ReceiverPage({ snapshot, onSelectIcao }) {
  const [scatterDays, setScatterDays]   = useState(1)
  const [rangeTrendDays, setRangeTrendDays] = useState(90)
  const [polarDays,   setPolarDays]     = useState(30)
  const [rangePctDays, setRangePctDays] = useState(30)
  const [completeDays, setCompleteDays] = useState(90)
  const [decodeDays,   setDecodeDays]   = useState(30)

  const aircraft = snapshot?.aircraft || []

  return (
    <main className={styles.main}>
      <div className={styles.row}>
        <ScatterPlot    days={scatterDays} onDaysChange={setScatterDays} />
        <BaselineComparison snapshot={snapshot} />
      </div>
      <div className={styles.row}>
        <RangeAltScatter aircraft={aircraft} />
        <LiveDFBreakdown snapshot={snapshot} />
      </div>
      <div className={styles.row}>
        <RangePercentiles days={rangePctDays} onDaysChange={setRangePctDays} />
        <RangeTrend days={rangeTrendDays} onDaysChange={setRangeTrendDays} />
      </div>
      <div className={styles.row}>
        <PolarCoverage days={polarDays} onDaysChange={setPolarDays} />
        <DistributionStats />
      </div>
      <div className={styles.row}>
        <ReceptionCompleteness days={completeDays} onDaysChange={setCompleteDays} />
        <PositionDecodeRate    days={decodeDays}   onDaysChange={setDecodeDays} />
      </div>
      <DFHeatmap />
      <SignalHeatmap />
    </main>
  )
}
