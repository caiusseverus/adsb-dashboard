import { useEffect, useRef, useState } from 'react'
import styles from './AltHeatmap.module.css'

const NORMAL_CEIL_FT = 45000
const TIME_AXIS_H    = 22     // px below heatmap for hour labels
const YAXIS_W        = 52

// Thermal colour scale — stops match the CSS legend gradient.
// t ∈ [0,1], caller should sqrt-scale for low-count visibility.
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

function drawYAxis(canvas, ceilFt) {
  const H = ceilFt / 100
  canvas.height = H
  const ctx = canvas.getContext('2d')
  ctx.fillStyle = '#0b0c10'
  ctx.fillRect(0, 0, YAXIS_W, H)

  ctx.fillStyle = '#484f58'
  ctx.font = '10px monospace'
  ctx.textAlign = 'right'
  ctx.textBaseline = 'middle'

  // Label every 5,000ft
  for (let alt = 0; alt <= ceilFt; alt += 5000) {
    const y = H - (alt / 100)
    ctx.fillText(alt === 0 ? '0' : `${alt / 1000}k`, YAXIS_W - 12, y)
    ctx.fillStyle = '#30363d'
    ctx.fillRect(YAXIS_W - 10, y, 8, 1)
    ctx.fillStyle = '#484f58'
  }
}

function drawHeatmap(canvas, data, ceilFt) {
  const { min_ts, minutes, cells } = data
  const W = Math.max(minutes, 1)
  const H = ceilFt / 100

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

  for (const [minIdx, altFt, count] of cells) {
    if (minIdx < 0 || minIdx >= W)   continue
    if (altFt  < 0 || altFt  > ceilFt) continue

    const t = Math.sqrt(count / maxCount)
    const [r, g, b] = heatColor(t)
    const y   = H - Math.floor(altFt / 100) - 1
    const idx = (y * W + minIdx) * 4
    px[idx] = r; px[idx + 1] = g; px[idx + 2] = b; px[idx + 3] = 255
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

export default function AltHeatmap() {
  const yAxisRef   = useRef(null)
  const heatmapRef = useRef(null)
  const dataRef    = useRef(null)   // last successful fetch; re-used on range toggle

  const [hours,    setHours]    = useState(24)
  const [extended, setExtended] = useState(false)
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)

  // Compute ceiling: normal = 45,000ft; extended = max observed rounded up
  // to the next 5,000ft, minimum 50,000ft.
  const extendedCeil = dataRef.current?.max_alt_observed
    ? Math.max(50000, Math.ceil(dataRef.current.max_alt_observed / 5000) * 5000)
    : 60000
  const ceilFt = extended ? extendedCeil : NORMAL_CEIL_FT
  const heatH  = ceilFt / 100

  // Re-render when range changes (no fetch needed — data already in ref)
  useEffect(() => {
    if (!dataRef.current) return
    if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilFt)
    if (heatmapRef.current) drawHeatmap(heatmapRef.current, dataRef.current, ceilFt)
  }, [ceilFt])

  // Fetch when hours changes
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`/api/history/alt_heatmap?hours=${hours}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(data => {
        if (cancelled) return
        dataRef.current = data
        setLoading(false)
        if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilFt)
        if (heatmapRef.current) drawHeatmap(heatmapRef.current, data, ceilFt)
      })
      .catch(e => { if (!cancelled) { setLoading(false); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hours])  // ceilFt intentionally excluded — range toggle handled by the effect above

  return (
    <section className={styles.section}>
      <div className={styles.titleRow}>
        <h2 className={styles.title}>Altitude Distribution</h2>
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
        Aircraft count per 100 ft altitude band per minute — 1 px = 1 min × 100 ft, newest on right
        {extended && dataRef.current?.max_alt_observed > 0 &&
          ` · ceiling ${(extendedCeil / 1000).toFixed(0)}k ft`}
      </p>

      {error   && <div className={styles.status}>Error: {error}</div>}
      {loading && !error && <div className={styles.status}>Loading…</div>}

      <div className={styles.chartWrap}>
        <canvas
          ref={yAxisRef}
          width={YAXIS_W}
          height={heatH}
          className={styles.yAxis}
          style={{ height: `${heatH}px` }}
        />
        <div className={styles.scrollArea}>
          <canvas
            ref={heatmapRef}
            className={styles.heatmap}
            style={{ height: `${heatH + TIME_AXIS_H}px` }}
          />
        </div>
      </div>

      <div className={styles.legend}>
        <span>0 aircraft</span>
        <div className={styles.legendGradient} />
        <span>many</span>
      </div>
    </section>
  )
}
