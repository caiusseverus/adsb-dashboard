import { useState, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell,
} from 'recharts'
import { formatOperator } from '../utils/formatOperator'
import styles from './FleetPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const SINCE_OPTIONS = [
  { value: null, label: 'All time' },
  { value: 90,   label: '90d' },
  { value: 30,   label: '30d' },
  { value: 7,    label: '7d' },
  { value: 1,    label: '24h' },
]

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
      {loading ? 'Loading…' : 'No data yet — check back after the app has been running for a while.'}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Shared since-param helper
// ---------------------------------------------------------------------------
function appendSince(url, since) {
  if (since == null) return url
  return url + (url.includes('?') ? '&' : '?') + `since=${since}`
}

// ---------------------------------------------------------------------------
// Summary stat cards
// ---------------------------------------------------------------------------
function SummaryRow({ since }) {
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/summary`, since))

  if (loading || !data) return <Card title="Fleet summary"><Empty loading={loading} /></Card>

  const regPct = data.total > 0 ? Math.round(data.with_registration / data.total * 100) : 0

  return (
    <Card title="Fleet summary">
      <div className={styles.summaryGrid}>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Total aircraft</div>
          <div className={styles.statValue}>{data.total?.toLocaleString()}</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Reg resolved</div>
          <div className={styles.statValue}>{regPct}%</div>
          <div className={styles.statSub}>{data.with_registration?.toLocaleString()} aircraft</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Military</div>
          <div className={styles.statValue}>{data.military?.toLocaleString()}</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Foreign mil</div>
          <div className={styles.statValue}>{data.foreign_military?.toLocaleString()}</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Interesting</div>
          <div className={styles.statValue}>{data.interesting?.toLocaleString()}</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>Rare</div>
          <div className={styles.statValue}>{data.rare?.toLocaleString()}</div>
        </div>
        <div className={styles.statBlock}>
          <div className={styles.statLabel}>First sightings</div>
          <div className={styles.statValue}>{data.first_seen?.toLocaleString()}</div>
        </div>
      </div>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Horizontal bar chart (shared)
// ---------------------------------------------------------------------------
function HBarChart({ data, labelKey, valueKey, colorFn, height, tooltipContent, onBarClick }) {
  if (!data?.length) return <Empty loading={false} />
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart layout="vertical" data={data}
        margin={{ top: 4, right: 40, bottom: 4, left: 0 }}>
        <CartesianGrid stroke="#21262d" horizontal={false} />
        <XAxis type="number" tick={{ fill: '#484f58', fontSize: 10 }} allowDecimals={false} />
        <YAxis type="category" dataKey={labelKey} width={160}
          tick={{ fill: '#8b949e', fontSize: 11 }} tickLine={false} interval={0} />
        <Tooltip
          contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
          content={tooltipContent}
        />
        <Bar dataKey={valueKey} radius={[0, 3, 3, 0]} isAnimationActive={false}
          onClick={onBarClick ? (barData) => onBarClick(barData) : undefined}
          style={onBarClick ? { cursor: 'pointer' } : undefined}>
          {data.map((d, i) => (
            <Cell key={i} fill={colorFn ? colorFn(d) : '#388bfd'} fillOpacity={0.85} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// ---------------------------------------------------------------------------
// Shared civil/military filter controls
// ---------------------------------------------------------------------------
function MilFilter({ value, onChange }) {
  return ['all', 'civil', 'military'].map(f => (
    <button key={f} className={value === f ? styles.btnActive : styles.btn}
      onClick={() => onChange(f)}>
      {f.charAt(0).toUpperCase() + f.slice(1)}
    </button>
  ))
}

function milParam(filter) {
  if (filter === 'military') return '&military=1'
  if (filter === 'civil')    return '&military=0'
  return ''
}

// ---------------------------------------------------------------------------
// Top aircraft types
// ---------------------------------------------------------------------------
function TopTypes({ since }) {
  const [filter, setFilter] = useState('all')
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/types?limit=20${milParam(filter)}`, since))

  const chartData = useMemo(() =>
    (data || []).map(d => ({
      label: d.type_code,
      count: d.count,
      type_name: d.type_name,
      type_category: d.type_category,
      wtc: d.wtc,
    })),
  [data])

  return (
    <Card title="Top aircraft types" controls={<MilFilter value={filter} onChange={setFilter} />}>
      {!chartData.length ? <Empty loading={loading} /> : (
        <HBarChart
          data={chartData}
          labelKey="label"
          valueKey="count"
          height={Math.max(200, chartData.length * 22 + 30)}
          tooltipContent={({ payload }) => {
            if (!payload?.length) return null
            const d = payload[0].payload
            return (
              <div className={styles.tooltip}>
                <div><strong>{d.label}</strong>{d.wtc ? ` · WTC ${d.wtc}` : ''}</div>
                {d.type_name && <div>{d.type_name}</div>}
                {d.type_category && <div>{d.type_category}</div>}
                <div>{d.count.toLocaleString()} aircraft</div>
              </div>
            )
          }}
        />
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Top operators
// ---------------------------------------------------------------------------
function TopOperators({ since }) {
  const [filter, setFilter] = useState('all')
  const milParam = filter === 'military' ? '&military=1' : filter === 'civil' ? '&military=0' : ''
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/operators?limit=20${milParam}`, since))

  const chartData = useMemo(() =>
    (data || []).map(d => ({
      label: formatOperator(d.operator_display || d.operator) || d.operator,
      count: d.count,
      military: d.military_count > 0,
    })),
  [data])

  const controls = (
    <>
      {['all', 'civil', 'military'].map(f => (
        <button key={f} className={filter === f ? styles.btnActive : styles.btn}
          onClick={() => setFilter(f)}>
          {f.charAt(0).toUpperCase() + f.slice(1)}
        </button>
      ))}
    </>
  )

  return (
    <Card title="Top operators" controls={controls}>
      {!chartData.length ? <Empty loading={loading} /> : (
        <HBarChart
          data={chartData}
          labelKey="label"
          valueKey="count"
          height={Math.max(200, chartData.length * 22 + 30)}
          colorFn={d => d.military ? '#bc8cff' : '#388bfd'}
          tooltipContent={({ payload }) => {
            if (!payload?.length) return null
            const d = payload[0].payload
            return (
              <div className={styles.tooltip}>
                <div><strong>{d.label}</strong>{d.military ? ' · Military' : ''}</div>
                <div>{d.count.toLocaleString()} aircraft</div>
              </div>
            )
          }}
        />
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Country of registration
// ---------------------------------------------------------------------------
function TopCountries({ since }) {
  const [filter, setFilter] = useState('all')
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/countries${milParam(filter) ? `?${milParam(filter).slice(1)}` : ''}`, since))

  const chartData = useMemo(() =>
    (data || []).map(d => ({
      label: d.country,
      count: d.count,
      military_count: d.military_count,
    })),
  [data])

  return (
    <Card title="Registration country" controls={<MilFilter value={filter} onChange={setFilter} />}>
      {!chartData.length ? <Empty loading={loading} /> : (
        <HBarChart
          data={chartData}
          labelKey="label"
          valueKey="count"
          height={Math.max(200, chartData.length * 22 + 30)}
          tooltipContent={({ payload }) => {
            if (!payload?.length) return null
            const d = payload[0].payload
            return (
              <div className={styles.tooltip}>
                <div><strong>{d.label}</strong></div>
                <div>{d.count.toLocaleString()} aircraft</div>
                {d.military_count > 0 && <div>{d.military_count} military</div>}
              </div>
            )
          }}
        />
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Type category breakdown
// ---------------------------------------------------------------------------

// Decode type_category codes: first char = airframe, second = engine count,
// third = engine type. E.g. "L2J" = landplane, 2 engines, jet.
const AIRFRAME = { L: 'Landplane', H: 'Helicopter', A: 'Amphibian', G: 'Gyrocopter', T: 'Tiltrotor', S: 'Seaplane' }
const ENGINE_TYPE = { P: 'Piston', T: 'Turboprop', J: 'Jet', E: 'Electric' }

function describeCategory(cat) {
  if (!cat || cat.length < 1) return cat
  const frame = AIRFRAME[cat[0]] || cat[0]
  const engType = cat.length >= 3 ? ENGINE_TYPE[cat[2]] : null
  const engCount = cat.length >= 2 && /\d/.test(cat[1]) ? cat[1] : null
  const parts = [frame]
  if (engCount) parts.push(`${engCount}-engine`)
  if (engType) parts.push(engType)
  return parts.join(' ')
}

function TypeCategories({ since }) {
  const [filter, setFilter] = useState('all')
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/categories${milParam(filter) ? `?${milParam(filter).slice(1)}` : ''}`, since))

  const chartData = useMemo(() =>
    (data || []).map(d => ({
      label: d.type_category,
      count: d.count,
      desc: describeCategory(d.type_category),
    })),
  [data])

  return (
    <Card title="Type categories" controls={<MilFilter value={filter} onChange={setFilter} />}>
      {!chartData.length ? <Empty loading={loading} /> : (
        <HBarChart
          data={chartData}
          labelKey="label"
          valueKey="count"
          height={Math.max(200, chartData.length * 22 + 30)}
          tooltipContent={({ payload }) => {
            if (!payload?.length) return null
            const d = payload[0].payload
            return (
              <div className={styles.tooltip}>
                <div><strong>{d.label}</strong></div>
                <div>{d.desc}</div>
                <div>{d.count.toLocaleString()} aircraft</div>
              </div>
            )
          }}
        />
      )}
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Year of manufacture histogram
// ---------------------------------------------------------------------------
function ManufactureYears({ since }) {
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/ages`, since))

  if (loading) return <Card title="Year of manufacture"><Empty loading /></Card>
  if (!data?.length) return null  // hide entirely if no year data

  return (
    <Card title="Year of manufacture">
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={data} margin={{ top: 8, right: 16, bottom: 24, left: 0 }}>
          <CartesianGrid stroke="#21262d" vertical={false} />
          <XAxis dataKey="year" tick={{ fill: '#484f58', fontSize: 10 }} angle={-45}
            textAnchor="end" interval={4} />
          <YAxis tick={{ fill: '#484f58', fontSize: 11 }} width={40} allowDecimals={false} />
          <Tooltip
            contentStyle={{ background: '#161b22', border: '1px solid #30363d', fontSize: 12 }}
            formatter={v => [v, 'Aircraft']}
          />
          <Bar dataKey="count" fill="#388bfd" isAnimationActive={false} radius={[2, 2, 0, 0]}>
            {data.map((d, i) => {
              const yr = parseInt(d.year, 10)
              const opacity = yr < 1970 ? 0.5 : yr < 1990 ? 0.7 : 1
              return <Cell key={i} fill="#388bfd" fillOpacity={opacity} />
            })}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Most seen individual aircraft (shared component)
// ---------------------------------------------------------------------------
function TopAircraftChart({ title, onSelectIcao, loading, data }) {
  const chartData = useMemo(() =>
    (data || []).map(d => ({
      label: d.registration || d.icao,
      count: d.sighting_count,
      icao: d.icao,
      registration: d.registration,
      type_code: d.type_code,
      operator: d.operator_display || d.operator,
      country: d.country,
      military: !!d.military,
    })),
  [data])

  return (
    <Card title={title}>
      {!chartData.length ? <Empty loading={loading} /> : (
        <HBarChart
          data={chartData}
          labelKey="label"
          valueKey="count"
          height={Math.max(200, chartData.length * 22 + 30)}
          colorFn={d => d.military ? '#bc8cff' : '#388bfd'}
          onBarClick={d => onSelectIcao?.(d.icao)}
          tooltipContent={({ payload }) => {
            if (!payload?.length) return null
            const d = payload[0].payload
            return (
              <div className={styles.tooltip}>
                <div><strong>{d.icao}</strong>{d.registration ? ` · ${d.registration}` : ''}</div>
                {d.type_code && <div>{d.type_code}</div>}
                {d.operator && <div>{d.operator}</div>}
                {d.country && <div>{d.country}</div>}
                <div>{d.count.toLocaleString()} sightings</div>
              </div>
            )
          }}
        />
      )}
    </Card>
  )
}

function TopAircraft({ since, onSelectIcao }) {
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/top_aircraft?limit=20`, since))
  return <TopAircraftChart title="Most seen aircraft" data={data} loading={loading} onSelectIcao={onSelectIcao} />
}

function TopMilitaryAircraft({ since, onSelectIcao }) {
  const { data, loading } = useFetch(appendSince(`${API_BASE}/api/fleet/top_aircraft?limit=20&military=1`, since))
  return <TopAircraftChart title="Most seen military aircraft" data={data} loading={loading} onSelectIcao={onSelectIcao} />
}

// ---------------------------------------------------------------------------
// Page layout
// ---------------------------------------------------------------------------
export default function FleetPage({ onSelectIcao }) {
  const [since, setSince] = useState(null)

  return (
    <main className={styles.main}>
      <div className={styles.sinceBar}>
        <span className={styles.sinceLabel}>Timeframe</span>
        {SINCE_OPTIONS.map(opt => (
          <button
            key={String(opt.value)}
            className={since === opt.value ? styles.btnActive : styles.btn}
            onClick={() => setSince(opt.value)}
          >{opt.label}</button>
        ))}
      </div>
      <SummaryRow since={since} />
      <div className={styles.row}>
        <TopTypes since={since} />
        <TopOperators since={since} />
      </div>
      <div className={styles.row}>
        <TopCountries since={since} />
        <TypeCategories since={since} />
      </div>
      <ManufactureYears since={since} />
      <div className={styles.row}>
        <TopAircraft since={since} onSelectIcao={onSelectIcao} />
        <TopMilitaryAircraft since={since} onSelectIcao={onSelectIcao} />
      </div>
    </main>
  )
}
