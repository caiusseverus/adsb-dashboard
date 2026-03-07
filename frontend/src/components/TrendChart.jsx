import { useState, useEffect } from 'react'
import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from 'recharts'
import styles from './TrendChart.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const DAY_OPTIONS = [30, 90, 180, 365]

function formatDate(dateStr) {
  const d = new Date(dateStr + 'T00:00:00Z')
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' })
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  const byKey = Object.fromEntries(payload.map(p => [p.dataKey, p.value]))
  return (
    <div style={{
      background: '#161b22', border: '1px solid #30363d',
      borderRadius: 6, padding: '0.4rem 0.75rem', fontSize: 12, color: '#c9d1d9',
    }}>
      <div style={{ color: '#484f58', marginBottom: 4 }}>{label}</div>
      <div><span style={{ color: '#388bfd' }}>●</span> Total {byKey.total}</div>
      <div><span style={{ color: '#3fb950' }}>●</span> Civil {byKey.civil}</div>
      <div><span style={{ color: '#bc8cff' }}>●</span> Military {byKey.military}</div>
    </div>
  )
}

export default function TrendChart() {
  const [days, setDays] = useState(90)
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetch(`${API_BASE}/api/history/trend?days=${days}`)
      .then(r => r.json())
      .then(d => {
        setData(d.map(row => ({ ...row, time: formatDate(row.date) })))
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [days])

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <span className={styles.heading}>Aircraft Count Trend</span>
        <div className={styles.controls}>
          {DAY_OPTIONS.map(d => (
            <button
              key={d}
              className={days === d ? styles.btnActive : styles.btn}
              onClick={() => setDays(d)}
            >{d}d</button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className={styles.empty}>Loading…</div>
      ) : data.length === 0 ? (
        <div className={styles.empty}>No data yet.</div>
      ) : (
        <>
        <div className={styles.legend}>
          <span className={styles.legendItem}><span className={styles.dot} style={{background:'#388bfd'}} /> Total</span>
          <span className={styles.legendItem}><span className={styles.dot} style={{background:'#3fb950'}} /> Civil</span>
          <span className={styles.legendItem}><span className={styles.dot} style={{background:'#bc8cff'}} /> Military</span>
        </div>
        <ResponsiveContainer width="100%" height={180}>
          <ComposedChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="totalGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%"  stopColor="#388bfd" stopOpacity={0.15} />
                <stop offset="95%" stopColor="#388bfd" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#21262d" vertical={false} />
            <XAxis
              dataKey="time"
              tick={{ fill: '#484f58', fontSize: 11 }}
              tickLine={false}
              axisLine={{ stroke: '#21262d' }}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fill: '#484f58', fontSize: 11 }}
              tickLine={false}
              axisLine={false}
              width={36}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="total"
              stroke="none"
              fill="url(#totalGrad)"
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="total"
              stroke="#388bfd"
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="civil"
              stroke="#3fb950"
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="military"
              stroke="#bc8cff"
              strokeWidth={1.5}
              dot={false}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
        </>
      )}
    </div>
  )
}
