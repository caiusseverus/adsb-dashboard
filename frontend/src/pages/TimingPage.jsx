import { useEffect, useRef, useState } from 'react'
import { InterrogatorCodes, InterrogatorTimeline, MessageTimingPlot } from './ReceiverPage'
import {
  buildAirspaceMicroTimelineRows,
  getMicroTimelineRankingWindowUs,
  MICRO_TIMELINE_REFRESH_US,
  MICRO_TIMELINE_ROW_LIMIT,
} from '../utils/timingMicroTimeline'
import { signalBucketIndex, signalColour } from '../utils/signal'
import { buildDfWaterfallRows, WATERFALL_DF_BUCKETS } from '../utils/timingWaterfall'
import styles from './ReceiverPage.module.css'

const TIMING_PAGE_WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/timing-page`
  : 'ws://localhost:8000/ws/timing-page'

const CADENCE_ORDER = ['ADS-B', 'TIS-B', 'All-Call', 'Comm-B', 'Surveillance', 'ACAS', 'Other']
const CADENCE_COLOURS = {
  'ADS-B': '#388bfd',
  'TIS-B': '#57a6ff',
  'All-Call': '#3fb950',
  'Comm-B': '#bc8cff',
  'Surveillance': '#d29922',
  'ACAS': '#f85149',
  'Other': '#6e7681',
}
const SOURCE_ORDER = [6, 5, 4, 3, 0]
const SOURCE_LABELS = {
  6: 'ADS-B',
  5: 'ADS-R/TIS-B',
  4: 'All-Call',
  3: 'Mode S',
  0: 'Other',
}
const SOURCE_COLOURS = {
  6: '#58a6ff',
  5: '#3fb950',
  4: '#d29922',
  3: '#f78166',
  0: '#6e7681',
}
const RAW_BEARING_COLOUR_MODES = {
  strength: 'Strength',
  df: 'Message Type',
  source: 'Source Class',
}
const DF_COLOURS = {
  17: '#388bfd',
  18: '#57a6ff',
  11: '#3fb950',
  4: '#d29922',
  5: '#e3b341',
  20: '#bc8cff',
  21: '#d2a8ff',
  0: '#f85149',
  16: '#ff6b6b',
  24: '#6e7681',
}

const RENDER_HOLDBACK_US = 650_000
const TIMING_BUFFER_MAX = 60_000
const TIMING_WINDOW_US = 5_000_000
const SIGNAL_BUCKET_COUNT = 12
const SIGNAL_BIN_US = 100_000
const SOURCE_BIN_US = 100_000
const BEARING_BUCKET_COUNT = 36
const BEARING_BIN_US = 125_000
const RAW_BEARING_DEG_PER_PX = 0.5
const RAW_BEARING_PLOT_H = 720
const MICRO_TIMELINE_LANE_H = 22
const MICRO_TIMELINE_LABEL_W = 84
const MICRO_TIMELINE_COUNT_W = 34
const MESSAGE_WATERFALL_SLICE_OPTIONS_MS = [20, 50, 100]

function withAlpha(hex, alpha) {
  const safeAlpha = Math.max(0, Math.min(1, alpha))
  return `${hex}${Math.round(safeAlpha * 255).toString(16).padStart(2, '0')}`
}

function waterfallCellAlpha(count, sliceMs) {
  if (!count) return 0
  const perSecondRate = count * (1000 / Math.max(1, sliceMs))
  const normalized = Math.log1p(perSecondRate) / Math.log1p(180)
  return 0.12 + Math.max(0, Math.min(1, normalized)) * 0.88
}

function waterfallBucketColour(bucketKey) {
  if (bucketKey === 'other') return '#6e7681'
  return DF_COLOURS[Number(bucketKey)] ?? '#6e7681'
}

function rawBearingEventColour(ev, colourMode) {
  if (colourMode === 'source') {
    return SOURCE_COLOURS[SOURCE_LABELS[ev.source_class] ? ev.source_class : 0] ?? SOURCE_COLOURS[0]
  }
  if (colourMode === 'df') {
    return DF_COLOURS[ev.df] ?? '#6e7681'
  }
  return signalColour(ev.signal_dbfs)
}

function Panel({ title, controls, children }) {
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

function MessageWaterfall({ timingView, timingWindowUs, sliceMs, onSliceMsChange }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  const cacheRef = useRef({
    eventsRef: null,
    newestVisibleSliceIndex: null,
    rowCount: null,
    sliceUs: null,
    rows: [],
  })
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    cacheRef.current = {
      eventsRef: null,
      newestVisibleSliceIndex: null,
      rowCount: null,
      sliceUs: null,
      rows: [],
    }
  }, [timingWindowUs, sliceMs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = 250
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const sliceUs = sliceMs * 1000
      const left = 60
      const top = 12
      const bottom = 28
      const plotW = w - left - 8
      const plotH = h - top - bottom
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const rowCount = Math.max(1, Math.floor(timingWindowUs / sliceUs))
      const newestVisibleSliceIndex = Math.floor(renderNowUs / sliceUs) - 2

      if (
        cacheRef.current.eventsRef !== tv.events
        || cacheRef.current.newestVisibleSliceIndex !== newestVisibleSliceIndex
        || cacheRef.current.rowCount !== rowCount
        || cacheRef.current.sliceUs !== sliceUs
      ) {
        const next = buildDfWaterfallRows({
          events: tv.events,
          renderNowUs,
          visibleWindowUs: timingWindowUs,
          sliceUs,
        })
        cacheRef.current = {
          eventsRef: tv.events,
          newestVisibleSliceIndex,
          rowCount: next.rowCount,
          sliceUs,
          rows: next.rows,
        }
      }

      const { rows } = cacheRef.current
      if (!rows.length) return

      const cellW = plotW / WATERFALL_DF_BUCKETS.length
      const cellH = plotH / rows.length

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let col = 0; col <= WATERFALL_DF_BUCKETS.length; col += 1) {
        const x = left + col * cellW
        ctx.beginPath()
        ctx.moveTo(x, top)
        ctx.lineTo(x, top + plotH)
        ctx.stroke()
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.textBaseline = 'top'
      ctx.fillText('newest', left - 8, top)
      ctx.textBaseline = 'bottom'
      ctx.fillText('older', left - 8, top + plotH)

      for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
        const row = rows[rowIndex]
        const y = top + rowIndex * cellH
        ctx.fillStyle = '#0d1117'
        ctx.fillRect(left, y, plotW, Math.max(1, cellH))

        row.counts.forEach((count, familyIndex) => {
          const x = left + familyIndex * cellW
          if (!count) return
          const bucket = WATERFALL_DF_BUCKETS[familyIndex]
          const baseColour = waterfallBucketColour(bucket.key)
          const alpha = waterfallCellAlpha(count, sliceMs)
          ctx.fillStyle = withAlpha(baseColour, alpha)
          ctx.fillRect(x + 1, y + 0.5, Math.max(1, cellW - 2), Math.max(1, cellH - 1))
        })
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'center'
      ctx.textBaseline = 'alphabetic'
      WATERFALL_DF_BUCKETS.forEach((bucket, familyIndex) => {
        const x = left + familyIndex * cellW + cellW / 2
        ctx.fillText(bucket.label, x, h - 8)
      })

      ctx.textAlign = 'left'
      ctx.fillText(`${sliceMs} ms slices`, left, 10)
      ctx.textAlign = 'right'
      ctx.fillText(`${rows.length} completed rows`, w - 8, 10)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [sliceMs, timingWindowUs])

  return (
    <Panel
      title="Message Waterfall"
      controls={
        <select className={styles.select} value={sliceMs} onChange={e => onSliceMsChange(Number(e.target.value))}>
          {MESSAGE_WATERFALL_SLICE_OPTIONS_MS.map(value => (
            <option key={value} value={value}>{value} ms</option>
          ))}
        </select>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Compact raw-DF texture from the shared timing stream. Each row is one completed short slice, so bursts and regime changes show up as stable vertical pattern shifts instead of another scrolling lane plot.
      </p>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for timing events…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 250, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function useTimingPageStream({ burstBinMs, cadenceBinMs, interrogatorWindowS = 10, timingWindowS = 5 }) {
  const [data, setData] = useState(null)
  const retryRef = useRef(null)
  const wsRef = useRef(null)

  useEffect(() => {
    let closed = false

    const sendConfig = () => {
      const ws = wsRef.current
      if (!ws || ws.readyState !== WebSocket.OPEN) return
      ws.send(JSON.stringify({
        timing_window_s: timingWindowS,
        interrogator_window_s: interrogatorWindowS,
        burst_bin_ms: burstBinMs,
        cadence_bin_ms: cadenceBinMs,
      }))
    }

    const connect = () => {
      if (closed) return
      const ws = new WebSocket(TIMING_PAGE_WS_URL)
      wsRef.current = ws

      ws.onopen = () => {
        sendConfig()
      }

      ws.onmessage = event => {
        try {
          const payload = JSON.parse(event.data)
          setData(payload)
        } catch {
          // ignore malformed frames
        }
      }

      ws.onclose = () => {
        if (closed) return
        if (wsRef.current === ws) wsRef.current = null
        retryRef.current = setTimeout(connect, 1000)
      }

      ws.onerror = () => ws.close()
    }

    connect()
    return () => {
      closed = true
      clearTimeout(retryRef.current)
      wsRef.current?.close()
      wsRef.current = null
    }
  }, [])

  useEffect(() => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    ws.send(JSON.stringify({
      timing_window_s: timingWindowS,
      interrogator_window_s: interrogatorWindowS,
      burst_bin_ms: burstBinMs,
      cadence_bin_ms: cadenceBinMs,
    }))
  }, [burstBinMs, cadenceBinMs, interrogatorWindowS, timingWindowS])

  return data
}

function useTimingEventBuffer(timingPacket, windowUs = TIMING_WINDOW_US) {
  const [view, setView] = useState({ nowUs: 0, events: [] })
  const bufRef = useRef([])

  useEffect(() => {
    if (!timingPacket) return
    const nowUs = Number(timingPacket.now_us ?? 0)
    const cutoffUs = Math.max(0, nowUs - windowUs - 1_000_000)
    const newEvents = (timingPacket.events ?? []).map(ev => {
      const [seq, arrival_us, df, msg_len, signal_dbfs, source_class, icao, bearing_deg] = ev
      return {
        seq,
        arrival_us,
        df,
        msg_len,
        signal_dbfs,
        source_class,
        icao: `${icao ?? ''}`.toUpperCase(),
        bearing_deg: Number.isFinite(Number(bearing_deg)) ? Number(bearing_deg) : null,
      }
    })
    const mergedMap = new Map()
    for (const ev of bufRef.current) {
      if (ev.arrival_us >= cutoffUs) mergedMap.set(ev.seq, ev)
    }
    for (const ev of newEvents) {
      if (ev.arrival_us >= cutoffUs) mergedMap.set(ev.seq, ev)
    }
    const merged = [...mergedMap.values()].sort((a, b) => a.seq - b.seq)
    if (merged.length > TIMING_BUFFER_MAX) merged.splice(0, merged.length - TIMING_BUFFER_MAX)
    bufRef.current = merged
    setView({ nowUs, events: merged })
  }, [timingPacket, windowUs])

  return view
}

function BearingTimeSweepHeatmap({ timingView, timingWindowUs }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = 220
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const left = 42
      const top = 10
      const plotW = w - left - 8
      const plotH = h - top - 28
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs
      const firstBin = Math.floor(cutoffUs / BEARING_BIN_US)
      const lastBin = Math.floor(renderNowUs / BEARING_BIN_US)
      const cells = Array.from({ length: BEARING_BUCKET_COUNT }, () => new Map())
      let maxCount = 0
      let attributedCount = 0

      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        if (!Number.isFinite(ev.bearing_deg)) continue
        const bearingNorm = ((ev.bearing_deg % 360) + 360) % 360
        const bucket = Math.max(0, Math.min(BEARING_BUCKET_COUNT - 1, Math.floor((bearingNorm / 360) * BEARING_BUCKET_COUNT)))
        const bin = Math.floor(ev.arrival_us / BEARING_BIN_US)
        const row = cells[bucket]
        const nextCount = (row.get(bin) ?? 0) + 1
        row.set(bin, nextCount)
        maxCount = Math.max(maxCount, nextCount)
        attributedCount += 1
      }

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let i = 0; i <= 4; i++) {
        const y = top + (plotH * i) / 4
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      const cellH = plotH / BEARING_BUCKET_COUNT
      maxCount = Math.max(1, maxCount)
      for (let bucket = 0; bucket < BEARING_BUCKET_COUNT; bucket++) {
        const row = cells[bucket]
        for (let bin = firstBin; bin <= lastBin; bin++) {
          const count = row.get(bin) ?? 0
          if (!count) continue
          const x = left + ((bin * BEARING_BIN_US - cutoffUs) / timingWindowUs) * plotW
          const nextX = left + (((bin + 1) * BEARING_BIN_US - cutoffUs) / timingWindowUs) * plotW
          const xClipped = Math.max(left, x)
          const wClipped = Math.max(0, Math.min(w, nextX) - xClipped)
          if (wClipped <= 0) continue
          const y = top + plotH - (bucket + 1) * cellH
          const intensity = count / maxCount
          const alpha = 0.14 + intensity * 0.86
          const lightness = 22 + intensity * 44
          ctx.fillStyle = `hsla(194, 100%, ${lightness}%, ${alpha})`
          ctx.fillRect(xClipped, y + 1, wClipped, Math.max(1, cellH - 2))
        }
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.textBaseline = 'middle'
      for (const [label, fraction] of [['360', 0], ['270', 0.25], ['180', 0.5], ['90', 0.75], ['0', 1]]) {
        ctx.fillText(label, left - 6, top + plotH * fraction)
      }
      ctx.textBaseline = 'alphabetic'
      ctx.textAlign = 'left'
      ctx.fillText('bearing deg', left, h - 8)
      ctx.textAlign = 'right'
      ctx.fillText(`${attributedCount} attributed msgs`, w - 8, h - 8)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [timingWindowUs])

  const hasBearingEvents = (timingView?.events ?? []).some(ev => Number.isFinite(ev.bearing_deg))

  return (
    <Panel title="Bearing-Time Sweep Heatmap">
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Recent activity by receiver-relative azimuth over the shared timing window. Only events with inline bearing attribution are plotted; unattributed traffic remains visible in the other timing panels.
      </p>
      {!hasBearingEvents
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for bearing-attributed timing events…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 220, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function RawBearingRaster({ timingView, timingWindowUs, colourMode, onColourModeChange, phosphor, onPhosphorChange }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = RAW_BEARING_PLOT_H + 30
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const left = 42
      const top = 10
      const plotW = w - left - 8
      const plotH = RAW_BEARING_PLOT_H
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs
      let plottedCount = 0

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (const deg of [360, 315, 270, 225, 180, 135, 90, 45, 0]) {
        const y = top + ((360 - deg) / 360) * plotH
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        if (!Number.isFinite(ev.bearing_deg)) continue
        const ageRatio = (renderNowUs - ev.arrival_us) / Math.max(1, timingWindowUs)
        const x = left + ((ev.arrival_us - cutoffUs) / timingWindowUs) * plotW
        const y = top + ((360 - ev.bearing_deg) / 360) * plotH
        if (x < left || x > left + plotW || y < top || y > top + plotH) continue
        const colour = rawBearingEventColour(ev, colourMode)
        const alpha = phosphor
          ? Math.max(0.05, 0.95 * (1 - ageRatio))
          : 0.85
        ctx.fillStyle = `${colour}${Math.round(alpha * 255).toString(16).padStart(2, '0')}`
        const markW = ev.msg_len >= 14 ? 2 : 1
        ctx.fillRect(Math.round(x), Math.round(y), markW, 1)
        if (phosphor) {
          ctx.fillStyle = `${colour}${Math.round(alpha * 90).toString(16).padStart(2, '0')}`
          ctx.fillRect(Math.round(x), Math.max(top, Math.round(y) - 1), 1, 3)
        }
        plottedCount += 1
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.textBaseline = 'middle'
      for (const deg of [360, 315, 270, 225, 180, 135, 90, 45, 0]) {
        const y = top + ((360 - deg) / 360) * plotH
        ctx.fillText(`${deg}`, left - 6, y)
      }
      ctx.textBaseline = 'alphabetic'
      ctx.textAlign = 'left'
      ctx.fillText('0.5° / px exact-bearing raster', left, h - 8)
      ctx.textAlign = 'right'
      ctx.fillText(`${plottedCount} plotted msgs`, w - 8, h - 8)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [timingWindowUs, colourMode, phosphor])

  const hasBearingEvents = (timingView?.events ?? []).some(ev => Number.isFinite(ev.bearing_deg))

  return (
    <Panel
      title="Raw Bearing Raster"
      controls={
        <>
          <select className={styles.select} value={colourMode} onChange={e => onColourModeChange(e.target.value)}>
            {Object.entries(RAW_BEARING_COLOUR_MODES).map(([value, label]) => (
              <option key={value} value={value}>{label}</option>
            ))}
          </select>
          <label style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.75rem', color: '#8b949e' }}>
            <input type="checkbox" checked={phosphor} onChange={e => onPhosphorChange(e.target.checked)} />
            Phosphor
          </label>
        </>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Exact per-message bearing marks on a tall raster. With phosphor enabled, older points fade as they scroll left so repeated activity at the same azimuth builds brighter streaks instead of flat speckle.
      </p>
      {!hasBearingEvents
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for bearing-attributed timing events…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: RAW_BEARING_PLOT_H + 30, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function SignalFloorShimmer({ timingView, timingWindowUs }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = 190
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const left = 44
      const top = 10
      const plotW = w - left - 8
      const plotH = h - top - 22
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs
      const firstBin = Math.floor(cutoffUs / SIGNAL_BIN_US)
      const lastBin = Math.floor(renderNowUs / SIGNAL_BIN_US)
      const bucketRows = Array.from({ length: SIGNAL_BUCKET_COUNT }, () => new Map())

      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        const bin = Math.floor(ev.arrival_us / SIGNAL_BIN_US)
        const bucket = signalBucketIndex(ev.signal_dbfs, SIGNAL_BUCKET_COUNT)
        const row = bucketRows[bucket]
        row.set(bin, (row.get(bin) ?? 0) + 1)
      }

      let maxCount = 0
      for (const row of bucketRows) {
        for (const count of row.values()) maxCount = Math.max(maxCount, count)
      }
      maxCount = Math.max(1, maxCount)

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let i = 0; i <= SIGNAL_BUCKET_COUNT; i++) {
        const y = top + (plotH * i) / SIGNAL_BUCKET_COUNT
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      const cellH = plotH / SIGNAL_BUCKET_COUNT
      for (let bucket = 0; bucket < SIGNAL_BUCKET_COUNT; bucket++) {
        const row = bucketRows[bucket]
        for (let bin = firstBin; bin <= lastBin; bin++) {
          const count = row.get(bin) ?? 0
          if (!count) continue
          const x = left + ((bin * SIGNAL_BIN_US - cutoffUs) / timingWindowUs) * plotW
          const nextX = left + (((bin + 1) * SIGNAL_BIN_US - cutoffUs) / timingWindowUs) * plotW
          const xClipped = Math.max(left, x)
          const wClipped = Math.max(0, Math.min(w, nextX) - xClipped)
          if (wClipped <= 0) continue
          const alpha = 0.12 + (count / maxCount) * 0.88
          const hue = 210 - (bucket / Math.max(1, SIGNAL_BUCKET_COUNT - 1)) * 165
          const y = top + plotH - (bucket + 1) * cellH
          ctx.fillStyle = `hsla(${hue}, 90%, 58%, ${alpha})`
          ctx.fillRect(xClipped, y + 1, wClipped, Math.max(1, cellH - 2))
        }
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.fillText('-48', left - 6, top + 8)
      ctx.fillText('0', left - 6, top + plotH)
      ctx.textAlign = 'left'
      ctx.fillText('signal dBFS', left, h - 6)
      ctx.textAlign = 'right'
      ctx.fillText(`${Math.round(timingWindowUs / 1_000_000)} s window`, w - 8, h - 6)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [timingWindowUs])

  return (
    <Panel title="Signal-Floor Shimmer">
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Recent per-message dBFS distribution. Short-term shifts reveal desense, overload, attenuation, and traffic-mix changes that static minute summaries hide.
      </p>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for signal samples…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 190, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function SourceMixPulseMonitor({ timingView, timingWindowUs }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = 190
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const left = 34
      const top = 10
      const plotW = w - left - 8
      const plotH = h - top - 34
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs
      const firstBin = Math.floor(cutoffUs / SOURCE_BIN_US)
      const lastBin = Math.floor(renderNowUs / SOURCE_BIN_US)
      const byBin = new Map()
      let peakTotal = 0

      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        const bin = Math.floor(ev.arrival_us / SOURCE_BIN_US)
        const sourceClass = SOURCE_LABELS[ev.source_class] ? ev.source_class : 0
        let counts = byBin.get(bin)
        if (!counts) {
          counts = new Map()
          byBin.set(bin, counts)
        }
        counts.set(sourceClass, (counts.get(sourceClass) ?? 0) + 1)
      }

      for (const counts of byBin.values()) {
        let total = 0
        for (const count of counts.values()) total += count
        peakTotal = Math.max(peakTotal, total)
      }
      peakTotal = Math.max(1, peakTotal)

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let i = 0; i <= 4; i++) {
        const y = top + (plotH * i) / 4
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      for (let bin = firstBin; bin <= lastBin; bin++) {
        const counts = byBin.get(bin)
        if (!counts) continue
        const total = SOURCE_ORDER.reduce((sum, sourceClass) => sum + (counts.get(sourceClass) ?? 0), 0)
        if (!total) continue
        const x = left + ((bin * SOURCE_BIN_US - cutoffUs) / timingWindowUs) * plotW
        const nextX = left + (((bin + 1) * SOURCE_BIN_US - cutoffUs) / timingWindowUs) * plotW
        const xClipped = Math.max(left, x)
        const wClipped = Math.max(0, Math.min(w, nextX) - xClipped)
        if (wClipped <= 0) continue
        let y = top + plotH
        const totalH = (total / peakTotal) * plotH
        for (const sourceClass of SOURCE_ORDER) {
          const count = counts.get(sourceClass) ?? 0
          if (!count) continue
          const segH = totalH * (count / total)
          y -= segH
          ctx.fillStyle = SOURCE_COLOURS[sourceClass]
          ctx.fillRect(xClipped, y, wClipped, Math.max(1, segH))
        }
      }

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.fillText(`${peakTotal}`, left - 4, top + 8)
      ctx.fillText('0', left - 4, top + plotH)
      ctx.textAlign = 'left'
      ctx.fillText('100 ms bins', left, h - 6)
      ctx.textAlign = 'right'
      ctx.fillText(`${Math.round(timingWindowUs / 1_000_000)} s window`, w - 8, h - 6)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [timingWindowUs])

  return (
    <Panel title="Source-Mix Pulse Monitor">
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.35rem' }}>
        Short-timescale source composition from the shared timing stream. This first pass tracks accepted primary-stream traffic classes only; rejected-frame accounting remains deferred.
      </p>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.65rem', marginBottom: '0.45rem', fontSize: '0.68rem', color: '#6e7681' }}>
        {SOURCE_ORDER.map(sourceClass => (
          <span key={sourceClass} style={{ display: 'inline-flex', alignItems: 'center', gap: '0.3rem' }}>
            <span style={{ width: 8, height: 8, borderRadius: 999, background: SOURCE_COLOURS[sourceClass], display: 'inline-block' }} />
            {SOURCE_LABELS[sourceClass]}
          </span>
        ))}
      </div>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for source activity…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 190, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function AirspaceMicroTimeline({ timingView, timingWindowUs }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  const rankingStateRef = useRef({ rows: [], graceByIcao: {}, lastRefreshUs: 0 })
  const [canvasH, setCanvasH] = useState(MICRO_TIMELINE_LANE_H * 4 + 22)
  const canvasHRef = useRef(MICRO_TIMELINE_LANE_H * 4 + 22)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    rankingStateRef.current = { rows: [], graceByIcao: {}, lastRefreshUs: 0 }
  }, [timingWindowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas) return

      const w = canvas.offsetWidth || 600
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const { rows, state, meta } = buildAirspaceMicroTimelineRows({
        events: tv?.events ?? [],
        renderNowUs,
        visibleWindowUs: timingWindowUs,
        previousState: rankingStateRef.current,
        rankingWindowUs: getMicroTimelineRankingWindowUs(timingWindowUs),
        rowLimit: MICRO_TIMELINE_ROW_LIMIT,
        refreshUs: MICRO_TIMELINE_REFRESH_US,
      })
      rankingStateRef.current = state

      const rowCount = Math.max(1, rows.length)
      const h = rowCount * MICRO_TIMELINE_LANE_H + 22
      if (h !== canvasHRef.current) {
        canvasHRef.current = h
        setCanvasH(h)
      }
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      if (!rows.length) return

      const cutoffUs = Math.max(0, renderNowUs - timingWindowUs)
      const plotX = MICRO_TIMELINE_LABEL_W
      const plotW = Math.max(60, w - plotX - MICRO_TIMELINE_COUNT_W)

      rows.forEach((row, index) => {
        const y0 = index * MICRO_TIMELINE_LANE_H
        const laneMidY = y0 + MICRO_TIMELINE_LANE_H / 2
        const rowColour = SOURCE_COLOURS[row.dominantSourceClass] ?? SOURCE_COLOURS[0]
        ctx.fillStyle = index % 2 === 0 ? '#0f1117' : '#0b0c10'
        ctx.fillRect(0, y0, w, MICRO_TIMELINE_LANE_H)
        ctx.fillStyle = '#11151b'
        ctx.fillRect(plotX, y0 + 2, plotW, MICRO_TIMELINE_LANE_H - 4)
        ctx.fillStyle = rowColour
        ctx.font = '11px monospace'
        ctx.textAlign = 'right'
        ctx.textBaseline = 'middle'
        ctx.fillText(row.icao, plotX - 8, laneMidY)
        ctx.fillStyle = '#6e7681'
        ctx.fillText(`${row.visibleCount}`, w - 6, laneMidY)

        for (const ev of row.events) {
          const x = plotX + ((ev.arrival_us - cutoffUs) / timingWindowUs) * plotW
          if (x < plotX || x > plotX + plotW) continue
          const tickW = ev.msg_len >= 14 ? 2.6 : 1.4
          const sourceClass = SOURCE_LABELS[ev.source_class] ? ev.source_class : row.dominantSourceClass
          ctx.fillStyle = SOURCE_COLOURS[sourceClass] ?? rowColour
          ctx.fillRect(x, y0 + 3, tickW, MICRO_TIMELINE_LANE_H - 6)
        }
      })

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      ctx.beginPath()
      ctx.moveTo(plotX, h - 20.5)
      ctx.lineTo(plotX + plotW, h - 20.5)
      ctx.stroke()

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'left'
      ctx.textBaseline = 'alphabetic'
      ctx.fillText(`${Math.round(timingWindowUs / 1_000_000)} s visible`, plotX, h - 6)
      ctx.textAlign = 'right'
      ctx.fillText(
        `${Math.round(meta.rankingWindowUs / 1_000_000)} s rank / ${MICRO_TIMELINE_ROW_LIMIT} rows / ${Math.round(MICRO_TIMELINE_REFRESH_US / 1_000_000)} s refresh`,
        w - 6,
        h - 6,
      )
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [timingWindowUs])

  const hasIdentityEvents = (timingView?.events ?? []).some(ev => ev.icao)

  return (
    <Panel title="Micro-timeline of Airspace Activity">
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Recent per-aircraft activity from the shared timing stream. Rows follow a slower ranking window with sticky membership so dominant talkers surface without thrashing on every burst.
      </p>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.65rem', marginBottom: '0.45rem', fontSize: '0.68rem', color: '#6e7681' }}>
        {SOURCE_ORDER.map(sourceClass => (
          <span key={sourceClass} style={{ display: 'inline-flex', alignItems: 'center', gap: '0.3rem' }}>
            <span style={{ width: 8, height: 8, borderRadius: 999, background: SOURCE_COLOURS[sourceClass], display: 'inline-block' }} />
            {SOURCE_LABELS[sourceClass]}
          </span>
        ))}
      </div>
      {!hasIdentityEvents
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for aircraft-identified timing events…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: canvasH, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}


function BurstRateStripChart({ timingView, burstBinMs, onBurstBinMsChange, timingWindowUs, onTimingWindowUsChange }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const h = 180
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const left = 34
      const top = 10
      const plotW = w - left - 8
      const plotH = h - top - 22
      const binUs = burstBinMs * 1000
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs

      // Bin events on absolute time grid — bin boundaries never move, events never jump
      const counts = new Map()
      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        const b = Math.floor(ev.arrival_us / binUs)
        counts.set(b, (counts.get(b) ?? 0) + 1)
      }
      const maxCount = Math.max(1, ...counts.values())

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let i = 0; i <= 4; i++) {
        const y = top + (plotH * i) / 4
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      // Each bin at its absolute time position — slides smoothly as cutoffUs advances
      const firstBin = Math.floor(cutoffUs / binUs)
      const lastBin = Math.floor(renderNowUs / binUs)
      ctx.strokeStyle = '#58a6ff'
      ctx.lineWidth = 1.5
      ctx.beginPath()
      let first = true
      for (let b = firstBin; b <= lastBin; b++) {
        const binMidUs = (b + 0.5) * binUs
        const x = left + ((binMidUs - cutoffUs) / timingWindowUs) * plotW
        const y = top + plotH - ((counts.get(b) ?? 0) / maxCount) * plotH
        if (first) { ctx.moveTo(x, y); first = false } else ctx.lineTo(x, y)
      }
      ctx.stroke()

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.fillText(`${maxCount}`, left - 4, top + 8)
      ctx.fillText('0', left - 4, top + plotH)

      const windowS = Math.round(timingWindowUs / 1_000_000)
      ctx.textAlign = 'left'
      ctx.fillText(`${windowS} s window`, left, h - 6)
      ctx.textAlign = 'right'
      ctx.fillText(`${burstBinMs} ms bins`, w - 8, h - 6)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [burstBinMs, timingWindowUs])

  return (
    <Panel
      title="Burst-Rate Strip Chart"
      controls={
        <>
          <select className={styles.select} value={timingWindowUs / 1_000_000} onChange={e => onTimingWindowUsChange(Number(e.target.value) * 1_000_000)}>
            <option value={2}>2 s</option>
            <option value={5}>5 s</option>
            <option value={10}>10 s</option>
          </select>
          <select className={styles.select} value={burstBinMs} onChange={e => onBurstBinMsChange(Number(e.target.value))}>
            <option value={10}>10 ms</option>
            <option value={20}>20 ms</option>
            <option value={50}>50 ms</option>
          </select>
        </>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Instantaneous message rate. Spikes reveal radar sweeps, bursty periods, and stalls sooner than per-second averages.
      </p>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for messages…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 180, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function DFCadenceLanes({ timingView, cadenceBinMs, onCadenceBinMsChange, timingWindowUs }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  timingViewRef.current = timingView
  const peakMaxRef = useRef({})
  const [canvasH, setCanvasH] = useState(22 * CADENCE_ORDER.length)
  const canvasHRef = useRef(22 * CADENCE_ORDER.length)

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    peakMaxRef.current = {}
  }, [cadenceBinMs, timingWindowUs])

  useEffect(() => {
    const laneH = 22
    const labelW = 92

    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const tv = timingViewRef.current
      if (!canvas || !(tv?.events?.length)) return

      const w = canvas.offsetWidth || 600
      const binUs = cadenceBinMs * 1000
      const interp = Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, nowUsRef.current + interp - RENDER_HOLDBACK_US)
      const cutoffUs = renderNowUs - timingWindowUs

      // Bin events on absolute time grid — boundaries fixed, no shimmer
      const families = Object.fromEntries(CADENCE_ORDER.map(name => [name, new Map()]))
      for (const ev of tv.events) {
        if (ev.arrival_us <= cutoffUs || ev.arrival_us > renderNowUs) continue
        const b = Math.floor(ev.arrival_us / binUs)
        const name = ev.df === 17 ? 'ADS-B'
          : ev.df === 18 ? 'TIS-B'
          : ev.df === 11 ? 'All-Call'
          : ev.df === 20 || ev.df === 21 ? 'Comm-B'
          : ev.df === 4 || ev.df === 5 ? 'Surveillance'
          : ev.df === 0 || ev.df === 16 ? 'ACAS'
          : 'Other'
        const m = families[name]
        m.set(b, (m.get(b) ?? 0) + 1)
      }

      const firstBin = Math.floor(cutoffUs / binUs)
      const lastBin = Math.floor(renderNowUs / binUs)

      const activeFamilies = CADENCE_ORDER.filter(name => families[name].size > 0)
      if (activeFamilies.length === 0) return

      // Update rolling peak (rises instantly, decays slowly over ~60 s)
      const peaks = peakMaxRef.current
      for (const name of activeFamilies) {
        const m = families[name]
        let frameMax = 0
        for (const v of m.values()) if (v > frameMax) frameMax = v
        peaks[name] = Math.max(frameMax, (peaks[name] ?? 0) * 0.9997)
      }

      const h = laneH * activeFamilies.length
      if (h !== canvasHRef.current) {
        canvasHRef.current = h
        setCanvasH(h)
      }
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const plotW = w - labelW

      activeFamilies.forEach((name, row) => {
        const bins = families[name]
        const laneMax = Math.max(1, peaks[name] ?? 1)
        const y0 = row * laneH
        ctx.fillStyle = row % 2 === 0 ? '#0f1117' : '#0b0c10'
        ctx.fillRect(0, y0, w, laneH)
        ctx.fillStyle = CADENCE_COLOURS[name] ?? '#8b949e'
        ctx.font = '10px monospace'
        ctx.textAlign = 'right'
        ctx.textBaseline = 'middle'
        ctx.fillText(name, labelW - 6, y0 + laneH / 2)
        // Each bin at its absolute time position — slides smoothly as cutoffUs advances
        const m = families[name]
        for (let b = firstBin; b <= lastBin; b++) {
          const count = m.get(b) ?? 0
          const x = labelW + ((b * binUs - cutoffUs) / timingWindowUs) * plotW
          const nextX = labelW + (((b + 1) * binUs - cutoffUs) / timingWindowUs) * plotW
          const xClipped = Math.max(labelW, x)
          const wClipped = Math.max(0, Math.min(w, nextX) - xClipped)
          if (wClipped <= 0) continue
          const alpha = count <= 0 ? 0 : Math.min(1, count / laneMax)
          ctx.fillStyle = count <= 0 ? '#11151b' : `${CADENCE_COLOURS[name]}${Math.round((0.15 + alpha * 0.75) * 255).toString(16).padStart(2, '0')}`
          ctx.fillRect(xClipped, y0 + 2, wClipped, laneH - 4)
        }
      })
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [cadenceBinMs, timingWindowUs])

  return (
    <Panel
      title="DF Cadence Lanes"
      controls={
        <select className={styles.select} value={cadenceBinMs} onChange={e => onCadenceBinMsChange(Number(e.target.value))}>
          <option value={20}>20 ms</option>
          <option value={50}>50 ms</option>
        </select>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Compressed DF-family activity density. This is cheaper to read than the full message plot but still exposes regime changes and SSR-heavy periods.
      </p>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for DF activity…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: canvasH, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}


export default function TimingPage({ onSelectIcao }) {
  const [burstBinMs, setBurstBinMs] = useState(20)
  const [cadenceBinMs, setCadenceBinMs] = useState(50)
  const [interrogatorWindowS, setInterrogatorWindowS] = useState(10)
  const [timingWindowS, setTimingWindowS] = useState(5)
  const timingWindowUs = timingWindowS * 1_000_000
  const pageStream = useTimingPageStream({ burstBinMs, cadenceBinMs, interrogatorWindowS, timingWindowS })
  const timingView = useTimingEventBuffer(pageStream?.timing ?? null, timingWindowUs)

  return (
    <main className={styles.main}>
      <div className={styles.card}>
        <p style={{ fontSize: '0.9rem', color: '#8b949e', margin: 0, lineHeight: 1.5 }}>
          Timing stays focused on cadence, interrogator rhythm, burst behaviour, and pipeline observability.
          Exact-point message-field experimentation now lives on the dedicated <strong style={{ color: '#c9d1d9' }}>Message Field</strong> page.
        </p>
      </div>
      <div className={styles.row}>
        <InterrogatorCodes
          streamData={pageStream?.interrogators ?? null}
          windowS={interrogatorWindowS}
          onWindowSChange={setInterrogatorWindowS}
        />
        <InterrogatorTimeline
          onSelectIcao={onSelectIcao}
          streamData={pageStream?.interrogators ?? null}
          windowS={interrogatorWindowS}
          onWindowSChange={setInterrogatorWindowS}
        />
      </div>
      <MessageTimingPlot streamPacket={pageStream?.timing ?? null} />
      <AirspaceMicroTimeline
        timingView={timingView}
        timingWindowUs={timingWindowUs}
      />
      <div className={styles.row}>
        <BurstRateStripChart
          timingView={timingView}
          burstBinMs={burstBinMs}
          onBurstBinMsChange={setBurstBinMs}
          timingWindowUs={timingWindowUs}
          onTimingWindowUsChange={us => setTimingWindowS(Math.round(us / 1_000_000))}
        />
        <DFCadenceLanes
          timingView={timingView}
          cadenceBinMs={cadenceBinMs}
          onCadenceBinMsChange={setCadenceBinMs}
          timingWindowUs={timingWindowUs}
        />
      </div>
      <div className={styles.row}>
        <SignalFloorShimmer
          timingView={timingView}
          timingWindowUs={timingWindowUs}
        />
        <SourceMixPulseMonitor
          timingView={timingView}
          timingWindowUs={timingWindowUs}
        />
      </div>
    </main>
  )
}
