import { useEffect, useMemo, useState } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import styles from './PositionQualityPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const fmt = (value, digits = 1) => {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  return Number(value).toFixed(digits)
}

export default function PositionQualityPage() {
  const [rows, setRows] = useState([])
  const [updatedAt, setUpdatedAt] = useState(null)
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [detail, setDetail] = useState(null)

  useEffect(() => {
    let alive = true
    const pull = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/position-quality`)
        const json = await res.json()
        if (!alive) return
        setRows(json.aircraft ?? [])
        setUpdatedAt(json.updated_at ?? null)
      } catch {
        if (alive) setRows([])
      }
    }
    pull()
    const id = setInterval(pull, 1000)
    return () => {
      alive = false
      clearInterval(id)
    }
  }, [])

  useEffect(() => {
    if (!selectedIcao) return
    let alive = true
    const pull = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/position-quality/${selectedIcao}`)
        if (!res.ok) return
        const json = await res.json()
        if (alive) setDetail(json)
      } catch {
        // ignored
      }
    }
    pull()
    const id = setInterval(pull, 1000)
    return () => {
      alive = false
      clearInterval(id)
    }
  }, [selectedIcao])

  const chartData = useMemo(
    () =>
      (detail?.history ?? []).map((point) => ({
        t: new Date(point.ts * 1000).toLocaleTimeString(),
        internalAltitude: point.internal_altitude,
        readsbAltitude: point.readsb_altitude,
      })),
    [detail]
  )

  const offsetHistory = useMemo(() => {
    return (detail?.history ?? [])
      .map((point) => {
        const internalLat = point.internal_lat
        const internalLon = point.internal_lon
        const readsbLat = point.readsb_lat
        const readsbLon = point.readsb_lon
        if ([internalLat, internalLon, readsbLat, readsbLon].some((v) => v === null || v === undefined)) {
          return null
        }
        return {
          dx: (internalLon - readsbLon) * 111_320 * Math.cos((readsbLat * Math.PI) / 180),
          dy: (internalLat - readsbLat) * 111_320,
          t: new Date(point.ts * 1000).toLocaleTimeString(),
        }
      })
      .filter(Boolean)
  }, [detail])

  const currentOffset = useMemo(() => {
    if (!offsetHistory.length) return []
    return [offsetHistory[offsetHistory.length - 1]]
  }, [offsetHistory])

  return (
    <main className={styles.main}>
      <div className={styles.headerRow}>
        <h2>Position Quality Checker</h2>
        <span className={styles.timestamp}>
          Last update: {updatedAt ? new Date(updatedAt * 1000).toLocaleTimeString() : '—'}
        </span>
      </div>
      {selectedIcao && detail && (
        <section className={styles.detailBox}>
          <div className={styles.detailHeader}>
            <h3>{selectedIcao} comparison</h3>
            <button onClick={() => { setSelectedIcao(null); setDetail(null) }}>Close</button>
          </div>
          <div className={styles.statsGrid}>
            <div>Samples: {detail.summary?.samples ?? 0}</div>
            <div>Avg pos error: {fmt(detail.summary?.avg_horizontal_error_m)} m</div>
            <div>Max pos error: {fmt(detail.summary?.max_horizontal_error_m)} m</div>
            <div>Avg |Δ alt|: {fmt(detail.summary?.avg_abs_altitude_delta_ft)} ft</div>
          </div>
          <div className={styles.chartGrid}>
            <div className={styles.chartCard}>
              <h4>Altitude history (last few minutes)</h4>
              <ResponsiveContainer width="100%" height={240}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="t" minTickGap={24} />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="internalAltitude" name="Internal" stroke="#22c55e" dot={false} />
                  <Line type="monotone" dataKey="readsbAltitude" name="readsb" stroke="#38bdf8" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className={styles.chartCard}>
              <h4>Relative internal offset from readsb (0,0 = match)</h4>
              <ResponsiveContainer width="100%" height={240}>
                <ScatterChart>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" dataKey="dx" name="East/West" unit="m" />
                  <YAxis type="number" dataKey="dy" name="North/South" unit="m" />
                  <ReferenceLine x={0} stroke="#64748b" strokeDasharray="3 3" />
                  <ReferenceLine y={0} stroke="#64748b" strokeDasharray="3 3" />
                  <Tooltip
                    cursor={{ strokeDasharray: '3 3' }}
                    formatter={(value, name) => [fmt(value, 1), name === 'dx' ? 'East/West' : 'North/South']}
                    labelFormatter={(_, payload) => payload?.[0]?.payload?.t || ''}
                  />
                  <Scatter
                    data={offsetHistory}
                    fill="#f97316"
                    line={{ stroke: '#f97316', strokeWidth: 2 }}
                    name="Offset history"
                  />
                  <Scatter data={currentOffset} fill="#ef4444" name="Current" />
                </ScatterChart>
              </ResponsiveContainer>
            </div>
          </div>
        </section>
      )}

      <div className={styles.tableWrap}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>ICAO</th>
              <th>Callsign</th>
              <th>Internal Lat/Lon</th>
              <th>readsb Lat/Lon</th>
              <th>Δ Alt (ft)</th>
              <th>Δ Pos (m)</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.icao}
                className={selectedIcao === row.icao ? styles.selected : ''}
                onClick={() => setSelectedIcao(row.icao)}
              >
                <td>{row.icao}</td>
                <td>{row.callsign || '—'}</td>
                <td>{fmt(row.internal.lat, 4)}, {fmt(row.internal.lon, 4)}</td>
                <td>{fmt(row.readsb.lat, 4)}, {fmt(row.readsb.lon, 4)}</td>
                <td>{row.altitude_delta_ft ?? '—'}</td>
                <td>{fmt(row.horizontal_error_m, 1)}</td>
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td colSpan={6} className={styles.empty}>No matched aircraft between internal state and readsb.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </main>
  )
}
