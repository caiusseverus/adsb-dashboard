import { useState, useEffect, useMemo, useRef } from 'react'
import {
  ScatterChart, Scatter, XAxis, YAxis, ZAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell, AreaChart, Area, Legend,
} from 'recharts'
import DFHeatmap from '../components/DFHeatmap'
import styles from './ReceiverPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

// Beast RSSI byte: 0=strongest, 255=weakest
// dBFS = -(raw / 2)  →  0 dBFS is full scale, -127.5 dBFS is minimum
function rawToDbfs(raw) {
  return raw != null ? Math.round(-raw / 2 * 10) / 10 : null
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

function useFetch(url) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    setLoading(true)
    setData(null)
    fetch(url)
      .then(r => r.ok ? r.json() : null)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [url])
  return { data, loading }
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

// ---------------------------------------------------------------------------
// 1. Scatter: aircraft count vs messages/min, coloured by signal strength
// ---------------------------------------------------------------------------
function ScatterPlot({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/scatter?days=${days}`)

  const points = useMemo(() => (data || []).map(d => ({
    ac: d.ac, msgs: d.msgs, signal: d.signal,
  })), [data])

  return (
    <Card
      title="Aircraft count vs messages per minute"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[1, 3, 7, 14, 30]} />}
    >
      {!points.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={280}>
          <ScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="ac" name="Aircraft" type="number"
              label={{ value: 'Aircraft', position: 'insideBottom', offset: -12, fill: '#484f58', fontSize: 11 }}
              tick={{ fill: '#484f58', fontSize: 11 }} />
            <YAxis dataKey="msgs" name="Messages/min" type="number"
              tick={{ fill: '#484f58', fontSize: 11 }} width={50} />
            <ZAxis range={[20, 20]} />
            <Tooltip cursor={{ stroke: '#30363d' }}
              content={({ payload }) => {
                if (!payload?.length) return null
                const d = payload[0].payload
                return (
                  <div className={styles.tooltip}>
                    <div>Aircraft: {d.ac}</div>
                    <div>Msgs/min: {d.msgs}</div>
                    <div>Signal avg: {fmtDbfs(d.signal)}</div>
                  </div>
                )
              }}
            />
            <Scatter data={points} isAnimationActive={false}>
              {points.map((p, i) => (
                <Cell key={i} fill={signalColour(p.signal)} fillOpacity={0.7} />
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
function SignalPercentiles({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/signal?days=${days}`)

  // Backend now returns dBFS values directly (negative numbers, 0=strongest)
  const points = useMemo(() => (data || []).map(d => ({
    label:  new Date(d.ts * 1000).toLocaleDateString(undefined, { month: 'short', day: 'numeric' }),
    strong: d.strong,
    median: d.median,
    weak:   d.weak,
  })), [data])

  // Domain: find actual range, pad slightly
  const allVals = points.flatMap(p => [p.strong, p.median, p.weak]).filter(v => v != null)
  const yMin = allVals.length ? Math.floor(Math.min(...allVals) / 5) * 5 - 5 : -100
  const yMax = allVals.length ? Math.ceil(Math.max(...allVals) / 5) * 5 + 5 : 0

  return (
    <Card
      title="Signal strength percentiles — dBFS (weak line = coverage edge)"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[7, 14, 30, 60, 90]} />}
    >
      {!points.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={points} margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="label" tick={{ fill: '#484f58', fontSize: 10 }}
              interval={Math.max(0, Math.floor(points.length / 8))} />
            <YAxis domain={[yMin, yMax]} tick={{ fill: '#484f58', fontSize: 11 }} width={52}
              tickFormatter={v => `${v} dB`} />
            <Tooltip
              contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={(v, name) => [`${v} dBFS`, name]}
            />
            <Area type="monotone" dataKey="strong" name="Strong (p90)" stroke="#3fb950"
              fill="#3fb95018" strokeWidth={1} dot={false} isAnimationActive={false} />
            <Area type="monotone" dataKey="median" name="Median (p50)" stroke="#388bfd"
              fill="#388bfd18" strokeWidth={2} dot={false} isAnimationActive={false} />
            <Area type="monotone" dataKey="weak" name="Weak / edge (p10)" stroke="#f85149"
              fill="#f8514918" strokeWidth={1.5} dot={false} isAnimationActive={false}
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
    const secsIntoCurMin = Math.min(59, Math.max(1, Math.floor(Date.now() / 1000) - curMinStart))
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

function RangeAltScatter({ aircraft }) {
  const points = useMemo(() =>
    (aircraft || [])
      .filter(ac => ac.range_nm != null && ac.altitude != null)
      .map(ac => ({ range: ac.range_nm, alt: ac.altitude, wtc: ac.wtc, callsign: ac.callsign, type_code: ac.type_code })),
  [aircraft])

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
            <ScatterChart margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
              <CartesianGrid stroke="#21262d" />
              <XAxis dataKey="range" name="Range" type="number"
                label={{ value: 'Range (nm)', position: 'insideBottom', offset: -12, fill: '#484f58', fontSize: 11 }}
                tick={{ fill: '#484f58', fontSize: 11 }} />
              <YAxis dataKey="alt" name="Altitude" type="number"
                tick={{ fill: '#484f58', fontSize: 11 }} width={56}
                tickFormatter={v => `${(v / 1000).toFixed(0)}k`} />
              <ZAxis range={[24, 24]} />
              <Tooltip cursor={{ stroke: '#30363d' }}
                content={({ payload }) => {
                  if (!payload?.length) return null
                  const d = payload[0].payload
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
              <Scatter data={points} isAnimationActive={false}>
                {points.map((p, i) => (
                  <Cell key={i} fill={wtcColour(p.wtc)} fillOpacity={0.75} />
                ))}
              </Scatter>
            </ScatterChart>
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
function BoxPlotSVG({ data, formatVal, minZero = true }) {
  const WINDOWS = ['1d', '7d', '30d', '365d']
  const W = 300, H = 160
  const ML = 50, MR = 12, MT = 14, MB = 28
  const plotW = W - ML - MR
  const plotH = H - MT - MB
  const colW  = plotW / WINDOWS.length
  const BOX_H = colW * 0.22

  const allVals = WINDOWS.flatMap(w => {
    const d = data?.[w]
    return d ? [d.p5, d.p95].filter(v => v != null) : []
  })
  if (!allVals.length) return <div className={styles.empty}>No data yet</div>

  const lo = Math.min(...allVals), hi = Math.max(...allVals)
  const pad = (hi - lo) * 0.15 || 1
  const yLo = (minZero ? Math.max(0, lo - pad) : lo - pad), yHi = hi + pad
  const sy = v => MT + plotH - ((v - yLo) / (yHi - yLo)) * plotH
  const ticks = [0, 1, 2, 3].map(i => yLo + (i / 3) * (yHi - yLo))

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: H }}>
      {ticks.map((t, i) => (
        <g key={i}>
          <line x1={ML} x2={ML + plotW} y1={sy(t)} y2={sy(t)} stroke="#21262d" strokeWidth={1} />
          <text x={ML - 5} y={sy(t)} fill="#484f58" fontSize={9} textAnchor="end" dominantBaseline="middle">
            {formatVal(t)}
          </text>
        </g>
      ))}
      {WINDOWS.map((w, i) => {
        const d = data?.[w]
        const cx = ML + (i + 0.5) * colW
        if (!d || d.p50 == null) return (
          <text key={w} x={cx} y={MT + plotH / 2} fill="#484f58" fontSize={10}
            textAnchor="middle" dominantBaseline="middle">—</text>
        )
        const x1 = cx - BOX_H, x2 = cx + BOX_H
        const cap1 = cx - BOX_H * 0.5, cap2 = cx + BOX_H * 0.5
        return (
          <g key={w}>
            {/* Whiskers p5–p25 and p75–p95 */}
            <line x1={cx} x2={cx} y1={sy(d.p5)}  y2={sy(d.p25)} stroke="#484f58" strokeWidth={1.5} />
            <line x1={cap1} x2={cap2} y1={sy(d.p5)}  y2={sy(d.p5)}  stroke="#484f58" strokeWidth={1} />
            <line x1={cx} x2={cx} y1={sy(d.p75)} y2={sy(d.p95)} stroke="#484f58" strokeWidth={1.5} />
            <line x1={cap1} x2={cap2} y1={sy(d.p95)} y2={sy(d.p95)} stroke="#484f58" strokeWidth={1} />
            {/* IQR box p25–p75 */}
            <rect x={x1} y={sy(d.p75)} width={BOX_H * 2}
              height={Math.max(1, sy(d.p25) - sy(d.p75))}
              fill="#1c2128" stroke="#388bfd" strokeWidth={1.5} rx={2} />
            {/* Median */}
            <line x1={x1} x2={x2} y1={sy(d.p50)} y2={sy(d.p50)} stroke="#388bfd" strokeWidth={2.5} />
            {/* Mean dot */}
            {d.mean != null && <circle cx={cx} cy={sy(d.mean)} r={3} fill="#d29922" />}
            {/* Window label */}
            <text x={cx} y={H - 6} fill="#8b949e" fontSize={11} textAnchor="middle">{w}</text>
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
    msgs:    snapshot.msg_per_sec * 60,
    signal:  snapshot.rate_history?.slice(-1)[0]?.signal_avg,
    adsbPct: aircraft.length ? Math.round(aircraft.filter(a => a.callsign).length / aircraft.length * 100) : null,
    regPct:  aircraft.length ? Math.round(aircraft.filter(a => a.registration).length / aircraft.length * 100) : null,
  } : null

  function Stat({ label, current, baseline, unit = '', isDbfs = false }) {
    if (current == null) return null
    const fmt = v => isDbfs ? `${v.toFixed(1)}` : Math.round(v)
    const diff = baseline != null ? current - baseline : null
    const pct  = diff != null && Math.abs(baseline) > 0.1 ? Math.round((diff / Math.abs(baseline)) * 100) : null
    // For dBFS: less negative = stronger = better, so positive diff = better
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

  // Convert raw RSSI byte to dBFS for display
  const currentSigDbfs = current?.signal != null ? rawToDbfs(current.signal) : null
  const baseSigDbfs    = base?.sig_avg   != null ? rawToDbfs(base.sig_avg)   : null

  return (
    <Card title={`Current vs 30-day baseline — hour ${currentHour}:00`}>
      {loading || !base || !current ? <Empty loading={loading} /> : (
        <div className={styles.baselineGrid}>
          <Stat label="Aircraft"       current={current.ac}          baseline={base.ac_avg} />
          <Stat label="Msgs / min"     current={current.msgs}        baseline={base.msg_avg * 60} />
          <Stat label="Signal"         current={currentSigDbfs}      baseline={baseSigDbfs}
            unit=" dB" isDbfs />
          <Stat label="ADS-B equipped" current={current.adsbPct}     unit="%" />
          <Stat label="Reg resolved"   current={current.regPct}      unit="%" />
        </div>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 9. Unique aircraft per day
// ---------------------------------------------------------------------------
function UniqueAircraftPerDay({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/history/receiver/unique_aircraft?days=${days}`)
  return (
    <Card
      title="Unique aircraft per day"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[30, 90, 365]} />}
    >
      {!data?.length ? <Empty loading={loading} /> : (
        <ResponsiveContainer width="100%" height={200}>
          <AreaChart data={data} margin={{ top: 8, right: 12, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#21262d" />
            <XAxis dataKey="date" tick={{ fill: '#484f58', fontSize: 10 }}
              interval={Math.max(0, Math.floor(data.length / 6))} />
            <YAxis tick={{ fill: '#484f58', fontSize: 11 }} width={44} allowDecimals={false} />
            <Tooltip contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
              formatter={v => [v, 'aircraft']} />
            <Area type="monotone" dataKey="count" name="Aircraft" stroke="#3fb950"
              fill="#3fb95020" strokeWidth={2} dot={false} isAnimationActive={false} />
          </AreaChart>
        </ResponsiveContainer>
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
      date:   d.date,
      adsb:   d.adsb_pct   ?? 0,
      mlat:   d.mlat_pct   ?? 0,
      no_pos: d.no_pos_pct ?? 0,
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
              <YAxis domain={[0, 100]} tick={{ fill: '#484f58', fontSize: 11 }} width={44}
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
// Page layout
// ---------------------------------------------------------------------------
export default function ReceiverPage({ snapshot }) {
  const [scatterDays, setScatterDays]   = useState(1)
  const [signalDays,  setSignalDays]    = useState(14)
  const [polarDays,   setPolarDays]     = useState(30)
  const [rangePctDays, setRangePctDays] = useState(30)
  const [uniqueDays,   setUniqueDays]   = useState(90)
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
        <SignalPercentiles days={signalDays} onDaysChange={setSignalDays} />
      </div>
      <div className={styles.row}>
        <PolarCoverage days={polarDays} onDaysChange={setPolarDays} />
        <DistributionStats />
      </div>
      <div className={styles.row3}>
        <UniqueAircraftPerDay  days={uniqueDays}   onDaysChange={setUniqueDays} />
        <ReceptionCompleteness days={completeDays} onDaysChange={setCompleteDays} />
        <PositionDecodeRate    days={decodeDays}   onDaysChange={setDecodeDays} />
      </div>
      <DFHeatmap />
    </main>
  )
}
