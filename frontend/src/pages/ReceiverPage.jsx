import { useState, useEffect, useMemo } from 'react'
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
// Continuous HSL gradient: green (0 ft) → yellow (10k) → blue (25k) → purple (45k+)
function altColour(alt) {
  if (alt == null) return '#484f58'
  const t = Math.max(0, Math.min(1, alt / 45000))
  let h
  if (t < 0.222) {        // 0–10k ft: green → yellow (120→60)
    h = 120 - (t / 0.222) * 60
  } else if (t < 0.556) { // 10k–25k ft: yellow → blue (60→210)
    h = 60 + ((t - 0.222) / 0.334) * 150
  } else {                // 25k–45k ft: blue → purple (210→280)
    h = 210 + ((t - 0.556) / 0.444) * 70
  }
  return `hsl(${Math.round(h)},80%,55%)`
}
const ALT_GRADIENT = 'linear-gradient(to right, hsl(120,80%,55%), hsl(60,80%,55%), hsl(210,80%,55%), hsl(280,80%,55%))'

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
    const counts = snapshot?.df_history?.slice(-1)[0]?.counts ?? {}
    return Object.entries(counts)
      .map(([df, count]) => ({ label: DF_LABELS[Number(df)] ?? `DF${df}`, count, df: Number(df) }))
      .sort((a, b) => b.count - a.count)
  }, [snapshot?.df_history])

  return (
    <Card title="Live message types — current minute">
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
// 6. Polar coverage scatter with max-range envelope
// ---------------------------------------------------------------------------
function PolarCoverage({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/coverage/polar?days=${days}`)
  const { data: envelopeData } = useFetch(`${API_BASE}/api/coverage/max_range?days=${days}`)

  const SIZE = 560
  const CX = SIZE / 2
  const CY = SIZE / 2
  const R  = SIZE / 2 - 48

  const maxRange = useMemo(() => {
    const m1 = data?.length ? Math.max(...data.map(p => p.range)) : 0
    const m2 = envelopeData?.length ? Math.max(...envelopeData.map(p => p.max_range)) : 0
    return Math.ceil(Math.max(m1, m2, 1) / 50) * 50
  }, [data, envelopeData])

  const rings = useMemo(() => {
    const step = maxRange <= 200 ? 50 : maxRange <= 400 ? 100 : 200
    const out = []
    for (let r = step; r <= maxRange; r += step) out.push(r)
    return out
  }, [maxRange])

  function toXY(bearing, range) {
    const ang = (bearing - 90) * Math.PI / 180
    const pr  = (range / maxRange) * R
    return [CX + pr * Math.cos(ang), CY + pr * Math.sin(ang)]
  }

  const envelopePoints = useMemo(() => {
    if (!envelopeData?.length || !maxRange) return ''
    return [...envelopeData]
      .sort((a, b) => a.bearing - b.bearing)
      .map(p => {
        const ang = (p.bearing - 90) * Math.PI / 180
        const pr  = (p.max_range / maxRange) * R
        return `${CX + pr * Math.cos(ang)},${CY + pr * Math.sin(ang)}`
      })
      .join(' ')
  }, [envelopeData, maxRange])

  const cardinals = [
    { label: 'N', dx: 0,      dy: -R - 20 },
    { label: 'E', dx: R + 20, dy: 0 },
    { label: 'S', dx: 0,      dy: R + 24 },
    { label: 'W', dx: -R - 20, dy: 0 },
  ]

  return (
    <Card
      title="Polar coverage — altitude coloured, max-range envelope"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[7, 14, 30, 90]} />}
    >
      {loading ? <div className={styles.empty}>Loading…</div>
       : !data?.length ? (
        <div className={styles.empty}>
          No coverage data yet — requires RECEIVER_LAT/RECEIVER_LON and at least one minute write with aircraft positions.
        </div>
       ) : (
        <>
          <svg
            viewBox={`0 0 ${SIZE} ${SIZE}`}
            style={{ width: '100%', maxWidth: 700, aspectRatio: '1', display: 'block', margin: '0 auto' }}
          >
            {/* Range rings */}
            {rings.map(r => (
              <g key={r}>
                <circle cx={CX} cy={CY} r={(r / maxRange) * R}
                  fill="none" stroke="#21262d" strokeWidth={1} />
                <text x={CX + 4} y={CY - (r / maxRange) * R + 14}
                  fill="#484f58" fontSize={12} textAnchor="start">{r} nm</text>
              </g>
            ))}
            {/* Bearing lines */}
            {[0, 45, 90, 135, 180, 225, 270, 315].map(b => {
              const [x2, y2] = toXY(b, maxRange)
              return <line key={b} x1={CX} y1={CY} x2={x2} y2={y2}
                stroke="#21262d" strokeWidth={1} />
            })}
            {/* Cardinal labels */}
            {cardinals.map(({ label, dx, dy }) => (
              <text key={label} x={CX + dx} y={CY + dy}
                fill="#8b949e" fontSize={16} fontWeight={600}
                textAnchor="middle" dominantBaseline="middle">{label}</text>
            ))}
            {/* Max-range envelope */}
            {envelopePoints && (
              <polygon points={envelopePoints}
                fill="#388bfd" fillOpacity={0.10}
                stroke="#388bfd" strokeWidth={1.5} strokeOpacity={0.7}
                strokeLinejoin="round" />
            )}
            {/* Data points coloured by altitude */}
            {data.map((p, i) => {
              const [x, y] = toXY(p.bearing, p.range)
              return <circle key={i} cx={x} cy={y} r={1.5}
                fill={altColour(p.alt)} fillOpacity={0.7} />
            })}
          </svg>
          {/* Altitude legend — continuous gradient */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', justifyContent: 'center', marginTop: '0.75rem', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>0 ft</span>
            <div style={{ background: ALT_GRADIENT, height: 8, borderRadius: 4, width: 160 }} />
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>45k+ ft</span>
            {envelopePoints && (
              <span style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: '#8b949e', marginLeft: '0.75rem' }}>
                <span style={{ width: 18, height: 2, background: '#388bfd', display: 'inline-block', flexShrink: 0 }} />
                Max range envelope
              </span>
            )}
          </div>
        </>
       )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 6. Azimuth vs elevation scatter (coloured by range)
// ---------------------------------------------------------------------------
function rangeColour(range) {
  const t = Math.max(0, Math.min(1, range / 250))
  let h
  if (t < 0.2)      h = 120
  else if (t < 0.6) h = 120 + ((t - 0.2) / 0.4) * 90   // green → blue
  else              h = 210 + ((t - 0.6) / 0.4) * 70    // blue → purple
  return `hsl(${Math.round(h)},80%,55%)`
}
const RANGE_GRADIENT = 'linear-gradient(to right, hsl(120,80%,55%), hsl(210,80%,55%), hsl(280,80%,55%))'

function AzimuthElevation({ days, onDaysChange }) {
  const { data, loading } = useFetch(`${API_BASE}/api/coverage/azimuth_elevation?days=${days}`)

  const points = useMemo(() => (data || []).map(d => ({
    bearing: d.bearing, elevation: d.elevation, range: d.range,
  })), [data])

  const maxEl = 15  // zoom to horizon — most aircraft are <10°; clipping high-elevation is acceptable

  return (
    <Card
      title="Azimuth vs elevation — coloured by range"
      controls={<DaySelect value={days} onChange={onDaysChange} options={[7, 14, 30, 90]} />}
    >
      {!points.length ? <Empty loading={loading} /> : (
        <>
          <ResponsiveContainer width="100%" height={340}>
            <ScatterChart margin={{ top: 8, right: 16, bottom: 32, left: 8 }}>
              <CartesianGrid stroke="#21262d" />
              <XAxis dataKey="bearing" name="Azimuth" type="number"
                domain={[0, 360]} allowDataOverflow
                ticks={[0, 45, 90, 135, 180, 225, 270, 315, 360]}
                tickFormatter={v => `${v}°`}
                label={{ value: 'Azimuth (°)', position: 'insideBottom', offset: -18, fill: '#484f58', fontSize: 11 }}
                tick={{ fill: '#484f58', fontSize: 10 }}
              />
              <YAxis dataKey="elevation" name="Elevation" type="number"
                domain={[0, maxEl]} allowDataOverflow
                tickFormatter={v => `${v}°`}
                label={{ value: 'Elevation (°)', angle: -90, position: 'insideLeft', offset: 8, fill: '#484f58', fontSize: 11 }}
                tick={{ fill: '#484f58', fontSize: 11 }} width={48}
              />
              <ZAxis range={[4, 4]} />
              <Tooltip cursor={{ stroke: '#30363d' }}
                content={({ payload }) => {
                  if (!payload?.length) return null
                  const d = payload[0].payload
                  return (
                    <div className={styles.tooltip}>
                      <div>Azimuth: {d.bearing}°</div>
                      <div>Elevation: {d.elevation}°</div>
                      <div>Range: {d.range} nm</div>
                    </div>
                  )
                }}
              />
              <Scatter data={points} isAnimationActive={false}>
                {points.map((p, i) => (
                  <Cell key={i} fill={rangeColour(p.range)} fillOpacity={0.55} />
                ))}
              </Scatter>
            </ScatterChart>
          </ResponsiveContainer>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', justifyContent: 'center', marginTop: '0.5rem' }}>
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>Near (0 nm)</span>
            <div style={{ background: RANGE_GRADIENT, height: 8, borderRadius: 4, width: 160 }} />
            <span style={{ fontSize: '0.72rem', color: '#484f58' }}>Far (250+ nm)</span>
          </div>
        </>
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// 7. Baseline comparison
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
// Page layout
// ---------------------------------------------------------------------------
export default function ReceiverPage({ snapshot }) {
  const [scatterDays, setScatterDays]   = useState(7)
  const [signalDays,  setSignalDays]    = useState(14)
  const [polarDays,   setPolarDays]     = useState(30)
  const [rangePctDays, setRangePctDays] = useState(30)
  const [azElDays,    setAzElDays]      = useState(30)

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
      <PolarCoverage days={polarDays} onDaysChange={setPolarDays} />
      <AzimuthElevation days={azElDays} onDaysChange={setAzElDays} />
      <DFHeatmap />
    </main>
  )
}
