import { useEffect, useRef, useState } from 'react'
import styles from './RangeHeatmap.module.css'

const NORMAL_CEIL_NM = 250
const PX_PER_NM      = 2      // canvas rows per 1 nm bucket — scales chart to ~500px
const TIME_AXIS_H    = 22     // px below heatmap for hour labels
const YAXIS_W        = 46

// Same thermal colour scale as AltHeatmap.
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

function drawYAxis(canvas, ceilNm) {
  const H = ceilNm * PX_PER_NM
  canvas.height = H
  const ctx = canvas.getContext('2d')
  ctx.fillStyle = '#0b0c10'
  ctx.fillRect(0, 0, YAXIS_W, H)

  ctx.fillStyle = '#484f58'
  ctx.font = '10px monospace'
  ctx.textAlign = 'right'
  ctx.textBaseline = 'middle'

  const step = ceilNm > 300 ? 50 : 25
  for (let r = 0; r <= ceilNm; r += step) {
    const y = H - r * PX_PER_NM
    ctx.fillText(`${r}`, YAXIS_W - 12, y)
    ctx.fillStyle = '#30363d'
    ctx.fillRect(YAXIS_W - 10, y, 8, 1)
    ctx.fillStyle = '#484f58'
  }
}

function drawHeatmap(canvas, data, ceilNm) {
  const { min_ts, minutes, cells } = data
  const W = Math.max(minutes, 1)
  const H = ceilNm * PX_PER_NM

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

  for (const [minIdx, rangeNm, count] of cells) {
    if (minIdx < 0 || minIdx >= W)        continue
    if (rangeNm < 0 || rangeNm >= ceilNm) continue

    const t = Math.sqrt(count / maxCount)
    const [r, g, b] = heatColor(t)
    const yBase = H - (rangeNm + 1) * PX_PER_NM
    for (let dy = 0; dy < PX_PER_NM; dy++) {
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

export default function RangeHeatmap() {
  const yAxisRef   = useRef(null)
  const heatmapRef = useRef(null)
  const dataRef    = useRef(null)

  const [hours,    setHours]    = useState(24)
  const [extended, setExtended] = useState(false)
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)

  const extendedCeil = dataRef.current?.max_range_observed
    ? Math.max(300, Math.ceil(dataRef.current.max_range_observed / 25) * 25)
    : 350
  const ceilNm = extended ? extendedCeil : NORMAL_CEIL_NM

  // Re-render when ceiling changes (no fetch needed)
  useEffect(() => {
    if (!dataRef.current) return
    if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilNm)
    if (heatmapRef.current) drawHeatmap(heatmapRef.current, dataRef.current, ceilNm)
  }, [ceilNm])

  // Fetch when hours changes
  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    fetch(`/api/history/range_heatmap?hours=${hours}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(data => {
        if (cancelled) return
        dataRef.current = data
        setLoading(false)
        if (yAxisRef.current)   drawYAxis(yAxisRef.current, ceilNm)
        if (heatmapRef.current) drawHeatmap(heatmapRef.current, data, ceilNm)
      })
      .catch(e => { if (!cancelled) { setLoading(false); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hours])

  return (
    <section className={styles.section}>
      <div className={styles.titleRow}>
        <h2 className={styles.title}>Range Distribution</h2>
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
        Aircraft count per 1 nm range band per minute — 1 px = 1 min × 1 nm, newest on right
        {extended && dataRef.current?.max_range_observed > 0 &&
          ` · ceiling ${extendedCeil} nm`}
      </p>

      {error   && <div className={styles.status}>Error: {error}</div>}
      {loading && !error && <div className={styles.status}>Loading…</div>}

      <div className={styles.chartWrap}>
        <canvas
          ref={yAxisRef}
          width={YAXIS_W}
          height={ceilNm * PX_PER_NM}
          className={styles.yAxis}
          style={{ height: `${ceilNm * PX_PER_NM}px` }}
        />
        <div className={styles.scrollArea}>
          <canvas
            ref={heatmapRef}
            className={styles.heatmap}
            style={{ height: `${ceilNm * PX_PER_NM + TIME_AXIS_H}px` }}
          />
        </div>
      </div>

      <div className={styles.legend}>
        <div className={styles.legendNote}>0 nm = receiver · {NORMAL_CEIL_NM} nm normal ceiling</div>
        <span className={styles.spacer} />
        <span>0 aircraft</span>
        <div className={styles.legendGradient} />
        <span>many</span>
      </div>
    </section>
  )
}
