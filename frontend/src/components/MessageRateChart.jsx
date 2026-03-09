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
import styles from './MessageRateChart.module.css'

function formatMinute(minuteTs) {
  // minuteTs is unix-seconds / 60; multiply back to get epoch ms
  const d = new Date(minuteTs * 60 * 1000)
  return d.toUTCString().slice(17, 22) // "HH:MM"
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  const byKey = Object.fromEntries(payload.map(p => [p.dataKey, p.value]))
  return (
    <div style={{
      background: '#161b22',
      border: '1px solid #30363d',
      borderRadius: 6,
      padding: '0.4rem 0.75rem',
      fontSize: 12,
      color: '#c9d1d9',
    }}>
      <div style={{ color: '#484f58', marginBottom: 4 }}>{label}</div>
      <div><span style={{ color: '#388bfd' }}>●</span> mean {byKey.mean} msg/s</div>
      <div style={{ color: '#8b949e', fontSize: 11, marginTop: 2 }}>
        max {byKey.max} · min {byKey.min} msg/s
      </div>
      {byKey.ac != null && (
        <div style={{ color: '#3fb950', marginTop: 2 }}>
          <span style={{ color: '#3fb950' }}>●</span> {byKey.ac} aircraft
        </div>
      )}
    </div>
  )
}

const WINDOW = 60 // minutes

function buildWindow(data) {
  const byMinute = {}
  data.forEach(d => { byMinute[d.minute] = d })

  const nowMinute = Math.floor(Date.now() / 60000)
  const slots = []
  for (let i = WINDOW - 1; i >= 0; i--) {
    const m = nowMinute - i
    const d = byMinute[m]
    slots.push({
      time: formatMinute(m),
      min:  d?.min      ?? null,
      max:  d?.max      ?? null,
      mean: d?.mean     ?? null,
      ac:   d?.ac_total ?? null,
    })
  }
  return slots
}

export default function MessageRateChart({ data }) {
  const formatted = buildWindow(data)

  return (
    <div className={styles.container}>
      <div className={styles.heading}>Message Rate — msgs / sec</div>
      <ResponsiveContainer width="100%" height={180}>
        <ComposedChart data={formatted} margin={{ top: 4, right: 40, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id="bandGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="#388bfd" stopOpacity={0.20} />
              <stop offset="95%" stopColor="#388bfd" stopOpacity={0.04} />
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
            yAxisId="msgs"
            tick={{ fill: '#388bfd', fontSize: 11 }}
            tickLine={false}
            axisLine={false}
            width={40}
          />
          <YAxis
            yAxisId="ac"
            orientation="right"
            tick={{ fill: '#3fb950', fontSize: 10 }}
            tickLine={false}
            axisLine={false}
            width={32}
            allowDecimals={false}
          />
          <Tooltip content={<CustomTooltip />} />
          {/* Band: fill 0→max with gradient, then mask 0→min with background */}
          <Area
            yAxisId="msgs"
            type="monotone"
            dataKey="max"
            stroke="none"
            fill="url(#bandGrad)"
            dot={false}
            legendType="none"
            activeDot={false}
            isAnimationActive={false}
          />
          <Area
            yAxisId="msgs"
            type="monotone"
            dataKey="min"
            stroke="none"
            fill="#161b22"
            dot={false}
            legendType="none"
            activeDot={false}
            isAnimationActive={false}
          />
          <Line
            yAxisId="msgs"
            type="monotone"
            dataKey="mean"
            stroke="#388bfd"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, fill: '#388bfd', stroke: '#0b0c10', strokeWidth: 2 }}
            isAnimationActive={false}
          />
          <Line
            yAxisId="ac"
            type="monotone"
            dataKey="ac"
            stroke="#3fb950"
            strokeWidth={1.5}
            dot={false}
            strokeDasharray="4 2"
            activeDot={{ r: 3, fill: '#3fb950' }}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
