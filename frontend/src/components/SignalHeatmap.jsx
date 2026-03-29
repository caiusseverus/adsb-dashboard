import { useEffect, useRef, useState } from 'react'
import styles from './SignalHeatmap.module.css'

// dbfs_bucket is the positive magnitude of readsb dBFS:
// 0 = 0 dBFS (strongest), larger buckets are weaker. Y axis puts 0 dBFS at top.
const NORMAL_CEIL_DB = 80   // show 0 to -80 dBFS in normal view
const PX_PER_DB      = 4    // canvas rows per 1 dBFS bucket → 320px normal height
const TIME_AXIS_H    = 22
const YAXIS_W        = 60

function heatColor(t) {
  if (t <= 0) return [11, 12, 16]
  const stops = [
    [0.00, [ 30,  10,  60]],
    [0.15, [ 20,  45, 140]],
    [0.35, [  0, 180, 220]],
    [0.55, [ 60, 220, 120]],
    [0.75, [240, 220,   0]],
    [1.00, [255, 255, 255]],
  ]
  for (let i = 1; i < stops.length; i++) {
    const [t0, c0] = stops[i - 1]
    const [t1, c1] = stops[i]
    if (t <= t1) {
      const f = (t - t0) / (t1 - t0)
      return [
        Math.round(c0[0] + f * (c1[0] - c0[0])),
        Math.round(c0[1] + f * (c1[1] - c0[1])),
        Math.round(c0[2] + f * (c1[2] - c0[2])),
      ]
    }
  }
  return [255, 255, 255]
}

function drawYAxis(canvas, ceilDb) {
  const H = ceilDb * PX_PER_DB
  canvas.height = H
  const ctx = canvas.getContext('2d')
  ctx.fillStyle = '#0b0c10'
  ctx.fillRect(0, 0, YAXIS_W, H)

  ctx.fillStyle = '#484f58'
  ctx.font = '10px monospace'
  ctx.textAlign = 'right'
  ctx.textBaseline = 'middle'

  // Label every 20 dBFS; bucket index equals the magnitude (0=0 dBFS, 20=-20 dBFS, etc.)
  for (let db = 0; db <= ceilDb; db += 20) {
    const y = db * PX_PER_DB
    ctx.fillText(db === 0 ? '0' : `-${db}`, YAXIS_W - 12, y)
    ctx.fillStyle = '#30363d'
    ctx.fillRect(YAXIS_W - 10, y, 8, 1)
    ctx.fillStyle = '#484f58'
  }
}

function drawHeatmap(canvas, data, ceilDb) {
  const { min_ts, minutes, cells } = data
  const W = Math.max(minutes, 1)
  const H = ceilDb * PX_PER_DB

  canvas.width  = W
  canvas.height = H + TIME_AXIS_H

  const ctx = canvas.getContext('2d')
  ctx.fillStyle = '#0b0c10'
  ctx.fillRect(0, 0, W, H + TIME_AXIS_H)

  if (!cells.length) return

  let maxCount = 0
  for (const [,, c] of cells) if (c > maxCount) maxCount = c
  if (maxCount === 0) return

  const imageData = ctx.createImageData(W, H)
  const px = imageData.data

  for (let i = 0; i < px.length; i += 4) {
    px[i] = 11; px[i + 1] = 12; px[i + 2] = 16; px[i + 3] = 255
  }

  for (const [minIdx, dbfsBucket, count] of cells) {
    if (minIdx < 0 || minIdx >= W)           continue
    if (dbfsBucket < 0 || dbfsBucket >= ceilDb) continue

    const t = Math.sqrt(count / maxCount)
    const [r, g, b] = heatColor(t)
    // bucket 0 (0 dBFS) → y=0 (top); bucket increases downward
    const yBase = dbfsBucket * PX_PER_DB
    for (let dy = 0; dy < PX_PER_DB; dy++) {
      const y = yBase + dy
      if (y < 0 || y >= H) continue
      const idx = (y * W + minIdx) * 4
      px[idx] = r; px[idx + 1] = g; px[idx + 2] = b; px[idx + 3] = 255
    }
  }

  ctx.putImageData(imageData, 0, 0)

  // Time axis
  ctx.fillStyle = '#30363d'
  ctx.fillRect(0, H, W, 1)
  ctx.fillStyle = '#484f58'
  ctx.font = '9px monospace'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'top'

  const firstHourTs = Math.ceil(min_ts / 3600) * 3600
  for (let ts = firstHourTs; ts <= min_ts + W * 60; ts += 3600) {
    const m = (ts - min_ts) / 60
    if (m < 0 || m >= W) continue
    const label = new Date(ts * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    ctx.fillStyle = '#30363d'
    ctx.fillRect(m, H, 1, 4)
    ctx.fillStyle = '#484f58'
    ctx.fillText(label, m, H + 5)
  }
}

export default function SignalHeatmap() {
  const yAxisRef   = useRef(null)
  const heatmapRef = useRef(null)
  const dataRef    = useRef(null)

  const [hours,    setHours]    = useState(24)
  const [extended, setExtended] = useState(false)
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)

  // Extended ceiling: max observed bucket rounded up to next 10, minimum 100
  const extendedCeil = dataRef.current?.max_dbfs_bucket
    ? Math.max(100, Math.ceil(dataRef.current.max_dbfs_bucket / 10) * 10)
    : 127
  const ceilDb = extended ? extendedCeil : NORMAL_CEIL_DB

  useEffect(() => {
    if (!dataRef.current) return
    if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilDb)
    if (heatmapRef.current) drawHeatmap(heatmapRef.current, dataRef.current, ceilDb)
  }, [ceilDb])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`/api/history/signal_heatmap?hours=${hours}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(data => {
        if (cancelled) return
        dataRef.current = data
        setLoading(false)
        if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilDb)
        if (heatmapRef.current) drawHeatmap(heatmapRef.current, data, ceilDb)
      })
      .catch(e => { if (!cancelled) { setLoading(false); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hours])

  return (
    <section className={styles.section}>
      <div className={styles.titleRow}>
        <h2 className={styles.title}>Signal Distribution</h2>
        <div className={styles.controls}>
          {[6, 12, 24, 48].map(h => (
            <button key={h}
              className={hours === h ? styles.btnActive : styles.btn}
              onClick={() => setHours(h)}
            >{h}h</button>
          ))}
          <span className={styles.sep} />
          <button
            className={!extended ? styles.btnActive : styles.btn}
            onClick={() => setExtended(false)}
          >Normal</button>
          <button
            className={extended ? styles.btnActive : styles.btn}
            onClick={() => setExtended(true)}
          >Extended</button>
        </div>
      </div>
      <p className={styles.subtitle}>
        Aircraft count per 1 dBFS band per minute — 1 px = 1 min × 1 dBFS, strongest at top, newest on right
        {extended && dataRef.current?.max_dbfs_bucket > 0 &&
          ` · floor −${extendedCeil} dBFS`}
      </p>

      {error   && <div className={styles.status}>Error: {error}</div>}
      {loading && !error && <div className={styles.status}>Loading…</div>}

      <div className={styles.chartWrap}>
        <canvas
          ref={yAxisRef}
          width={YAXIS_W}
          height={ceilDb * PX_PER_DB}
          className={styles.yAxis}
          style={{ height: `${ceilDb * PX_PER_DB}px` }}
        />
        <div className={styles.scrollArea}>
          <canvas
            ref={heatmapRef}
            className={styles.heatmap}
            style={{ height: `${ceilDb * PX_PER_DB + TIME_AXIS_H}px` }}
          />
        </div>
      </div>

      <div className={styles.legend}>
        <span>dBFS</span>
        <span className={styles.spacer} />
        <span>0 aircraft</span>
        <div className={styles.legendGradient} />
        <span>many</span>
      </div>
    </section>
  )
}
