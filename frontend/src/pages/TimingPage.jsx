import { useEffect, useRef, useState } from 'react'
import { InterrogatorCodes, InterrogatorTimeline, MessageTimingPlot } from './ReceiverPage'
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
const LATENCY_TRACES = [
  { key: 'decode_ms', label: 'Decode', color: '#58a6ff' },
  { key: 'lock_wait_ms', label: 'Lock Wait', color: '#d29922' },
  { key: 'snapshot_ms', label: 'Snapshot', color: '#3fb950' },
  { key: 'serialize_ms', label: 'Serialize', color: '#bc8cff' },
  { key: 'broadcast_ms', label: 'Broadcast', color: '#f85149' },
]
const AGGREGATE_RENDER_HOLDBACK_US = 400_000
const TIMING_BUFFER_MAX = 20000
const TIMING_WINDOW_US = 5_000_000

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

function useTimingPageStream({ burstBinMs, cadenceBinMs, latencyWindowS, interrogatorWindowS = 10, timingWindowS = 5 }) {
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
        latency_window_s: latencyWindowS,
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
      latency_window_s: latencyWindowS,
    }))
  }, [burstBinMs, cadenceBinMs, latencyWindowS, interrogatorWindowS, timingWindowS])

  return data
}

function useTimingEventBuffer(timingPacket) {
  const [view, setView] = useState({ nowUs: 0, events: [] })
  const bufRef = useRef([])

  useEffect(() => {
    if (!timingPacket) return
    const nowUs = Number(timingPacket.now_us ?? 0)
    const cutoffUs = Math.max(0, nowUs - TIMING_WINDOW_US - 1_000_000)
    const newEvents = (timingPacket.events ?? []).map(ev => {
      const [seq, arrival_us, df, msg_len] = ev
      return { seq, arrival_us, df, msg_len }
    })
    const merged = [...bufRef.current, ...newEvents]
      .filter(ev => ev.arrival_us >= cutoffUs)
    if (merged.length > TIMING_BUFFER_MAX) merged.splice(0, merged.length - TIMING_BUFFER_MAX)
    bufRef.current = merged
    setView({ nowUs, events: merged })
  }, [timingPacket])

  return view
}

function buildBurstCounts(events, anchorUs, binUs) {
  const cutoffUs = Math.max(0, anchorUs - TIMING_WINDOW_US)
  const bucketCount = Math.max(1, Math.ceil(TIMING_WINDOW_US / binUs))
  const counts = new Array(bucketCount).fill(0)
  for (const ev of events) {
    if (ev.arrival_us < cutoffUs || ev.arrival_us > anchorUs) continue
    const idx = Math.min(bucketCount - 1, Math.max(0, Math.floor((ev.arrival_us - cutoffUs) / binUs)))
    counts[idx] += 1
  }
  return counts
}

function buildCadenceCounts(events, anchorUs, binUs) {
  const cutoffUs = Math.max(0, anchorUs - TIMING_WINDOW_US)
  const bucketCount = Math.max(1, Math.ceil(TIMING_WINDOW_US / binUs))
  const families = Object.fromEntries(CADENCE_ORDER.map(name => [name, new Array(bucketCount).fill(0)]))
  for (const ev of events) {
    if (ev.arrival_us < cutoffUs || ev.arrival_us > anchorUs) continue
    const idx = Math.min(bucketCount - 1, Math.max(0, Math.floor((ev.arrival_us - cutoffUs) / binUs)))
    const familyName = ev.df === 17 ? 'ADS-B'
      : ev.df === 18 ? 'TIS-B'
      : ev.df === 11 ? 'All-Call'
      : ev.df === 20 || ev.df === 21 ? 'Comm-B'
      : ev.df === 4 || ev.df === 5 ? 'Surveillance'
      : ev.df === 0 || ev.df === 16 ? 'ACAS'
      : 'Other'
    families[familyName][idx] += 1
  }
  return families
}

function BurstRateStripChart({ timingView, burstBinMs, onBurstBinMsChange }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const countsRef = useRef([])
  const anchorUsRef = useRef(0)

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    countsRef.current = []
    anchorUsRef.current = 0
  }, [burstBinMs, timingView])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas || !(timingView?.events?.length)) return

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
      const interpolatedNowUs = nowUsRef.current + Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, interpolatedNowUs - AGGREGATE_RENDER_HOLDBACK_US)
      const displayNowUs = Math.max(0, renderNowUs - binUs)
      const anchorUs = Math.floor(displayNowUs / binUs) * binUs
      if (anchorUs !== anchorUsRef.current || countsRef.current.length === 0) {
        anchorUsRef.current = anchorUs
        countsRef.current = buildBurstCounts(timingView?.events ?? [], anchorUs, binUs)
      }
      const counts = countsRef.current
      const phaseBins = Math.max(0, Math.min(1, (displayNowUs - anchorUs) / binUs))
      const maxCount = Math.max(1, ...counts)

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let i = 0; i <= 4; i++) {
        const y = top + (plotH * i) / 4
        ctx.beginPath()
        ctx.moveTo(left, y)
        ctx.lineTo(left + plotW, y)
        ctx.stroke()
      }

      ctx.strokeStyle = '#58a6ff'
      ctx.lineWidth = 1.5
      ctx.beginPath()
      counts.forEach((count, idx) => {
        const shiftedIdx = idx - phaseBins
        const x = left + (shiftedIdx / Math.max(1, counts.length - 1)) * plotW
        const y = top + plotH - (count / maxCount) * plotH
        if (idx === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      })
      ctx.stroke()

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.fillText(`${maxCount}`, left - 4, top + 8)
      ctx.fillText('0', left - 4, top + plotH)

      ctx.textAlign = 'left'
      ctx.fillText(`5 s window`, left, h - 6)
      ctx.textAlign = 'right'
      ctx.fillText(`${burstBinMs} ms bins`, w - 8, h - 6)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [burstBinMs, timingView])

  return (
    <Panel
      title="Burst-Rate Strip Chart"
      controls={
        <select className={styles.select} value={burstBinMs} onChange={e => onBurstBinMsChange(Number(e.target.value))}>
          <option value={10}>10 ms</option>
          <option value={20}>20 ms</option>
          <option value={50}>50 ms</option>
        </select>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Instantaneous message rate over the last 5 s. Spikes reveal radar sweeps, bursty periods, and stalls sooner than per-second averages.
      </p>
      {!timingView?.events?.length
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for messages…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: 180, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function DFCadenceLanes({ timingView, cadenceBinMs, onCadenceBinMsChange }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const nowUsRef = useRef(0)
  const nowUsWallRef = useRef(performance.now())
  const familiesRef = useRef({})
  const anchorUsRef = useRef(0)

  useEffect(() => {
    nowUsRef.current = Number(timingView?.nowUs ?? 0)
    nowUsWallRef.current = performance.now()
  }, [timingView?.nowUs])

  useEffect(() => {
    familiesRef.current = {}
    anchorUsRef.current = 0
  }, [cadenceBinMs, timingView])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas || !(timingView?.events?.length)) return

      const laneH = 22
      const labelW = 92
      const w = canvas.offsetWidth || 600
      const binUs = cadenceBinMs * 1000
      const interpolatedNowUs = nowUsRef.current + Math.max(0, performance.now() - nowUsWallRef.current) * 1000
      const renderNowUs = Math.max(0, interpolatedNowUs - AGGREGATE_RENDER_HOLDBACK_US)
      const displayNowUs = Math.max(0, renderNowUs - binUs)
      const anchorUs = Math.floor(displayNowUs / binUs) * binUs
      if (anchorUs !== anchorUsRef.current || !Object.keys(familiesRef.current).length) {
        anchorUsRef.current = anchorUs
        familiesRef.current = buildCadenceCounts(timingView?.events ?? [], anchorUs, binUs)
      }
      const families = familiesRef.current
      const phaseBins = Math.max(0, Math.min(1, (displayNowUs - anchorUs) / binUs))
      const activeFamilies = CADENCE_ORDER.filter(name => families[name]?.some(v => v > 0))
      if (activeFamilies.length === 0) return

      const h = laneH * activeFamilies.length
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const plotW = w - labelW

      activeFamilies.forEach((name, row) => {
        const bins = families[name]
        const laneMax = Math.max(1, ...bins)
        const y0 = row * laneH
        ctx.fillStyle = row % 2 === 0 ? '#0f1117' : '#0b0c10'
        ctx.fillRect(0, y0, w, laneH)
        ctx.fillStyle = CADENCE_COLOURS[name] ?? '#8b949e'
        ctx.font = '10px monospace'
        ctx.textAlign = 'right'
        ctx.textBaseline = 'middle'
        ctx.fillText(name, labelW - 6, y0 + laneH / 2)
        bins.forEach((count, idx) => {
          const shiftedIdx = idx - phaseBins
          const x = labelW + (shiftedIdx / bins.length) * plotW
          const nextX = labelW + ((shiftedIdx + 1) / bins.length) * plotW
          const alpha = count <= 0 ? 0 : Math.min(1, count / laneMax)
          ctx.fillStyle = count <= 0 ? '#11151b' : `${CADENCE_COLOURS[name]}${Math.round((0.15 + alpha * 0.75) * 255).toString(16).padStart(2, '0')}`
          ctx.fillRect(x, y0 + 2, Math.max(1, nextX - x), laneH - 4)
        })
      })
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [cadenceBinMs, timingView])

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
        : <canvas ref={canvasRef} style={{ width: '100%', height: `${22 * CADENCE_ORDER.length}px`, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

function PipelineLatencyOscilloscope({ data, latencyWindowS, onLatencyWindowSChange }) {
  const canvasRef = useRef(null)

  useEffect(() => {
    const canvas = canvasRef.current
    const latency = data?.latency
    if (!canvas || !latency) return

    const traces = LATENCY_TRACES.filter(trace => (latency[trace.key] ?? []).length > 0)
    if (traces.length === 0) return

    const rowH = 44
    const labelW = 92
    const w = canvas.offsetWidth || 600
    const h = rowH * traces.length
    canvas.width = w
    canvas.height = h
    const ctx = canvas.getContext('2d')
    ctx.fillStyle = '#0b0c10'
    ctx.fillRect(0, 0, w, h)

    const plotW = w - labelW - 8
    traces.forEach((trace, row) => {
      const values = latency[trace.key] ?? []
      const maxValue = Math.max(1, ...values)
      const y0 = row * rowH
      const plotTop = y0 + 6
      const plotH = rowH - 12
      ctx.fillStyle = row % 2 === 0 ? '#0f1117' : '#0b0c10'
      ctx.fillRect(0, y0, w, rowH)

      ctx.fillStyle = '#1b2330'
      ctx.fillRect(labelW, plotTop + plotH * 0.66, plotW, plotH * 0.34)
      ctx.fillStyle = '#33241a'
      ctx.fillRect(labelW, plotTop + plotH * 0.33, plotW, plotH * 0.33)
      ctx.fillStyle = '#30181d'
      ctx.fillRect(labelW, plotTop, plotW, plotH * 0.33)

      ctx.fillStyle = trace.color
      ctx.font = '10px monospace'
      ctx.textAlign = 'right'
      ctx.textBaseline = 'middle'
      ctx.fillText(trace.label, labelW - 6, y0 + rowH / 2)

      ctx.strokeStyle = trace.color
      ctx.lineWidth = 1.25
      ctx.beginPath()
      values.forEach((value, idx) => {
        const x = labelW + (idx / Math.max(1, values.length - 1)) * plotW
        const y = plotTop + plotH - (value / maxValue) * plotH
        if (idx === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      })
      ctx.stroke()

      ctx.fillStyle = '#8b949e'
      ctx.font = '9px monospace'
      ctx.fillText(`${maxValue.toFixed(1)} ms`, w - 6, y0 + 11)
    })
  }, [data, latencyWindowS])

  return (
    <Panel
      title="Pipeline Latency Oscilloscope"
      controls={
        <select className={styles.select} value={latencyWindowS} onChange={e => onLatencyWindowSChange(Number(e.target.value))}>
          <option value={10}>10 s</option>
          <option value={30}>30 s</option>
          <option value={60}>60 s</option>
        </select>
      }
    >
      <p style={{ fontSize: '0.72rem', color: '#484f58', margin: '0 0 0.4rem' }}>
        Runtime latency waveforms for decode and snapshot pipeline stages. Short spikes from contention or backlog should stand out immediately.
      </p>
      {!data?.latency
        ? <p style={{ fontSize: '0.8rem', color: '#484f58' }}>Waiting for latency samples…</p>
        : <canvas ref={canvasRef} style={{ width: '100%', height: `${44 * LATENCY_TRACES.length}px`, display: 'block', borderRadius: 4 }} />
      }
    </Panel>
  )
}

export default function TimingPage({ onSelectIcao }) {
  const [burstBinMs, setBurstBinMs] = useState(20)
  const [cadenceBinMs, setCadenceBinMs] = useState(50)
  const [latencyWindowS, setLatencyWindowS] = useState(30)
  const pageStream = useTimingPageStream({ burstBinMs, cadenceBinMs, latencyWindowS })
  const timingView = useTimingEventBuffer(pageStream?.timing ?? null)

  return (
    <main className={styles.main}>
      <div className={styles.row}>
        <InterrogatorCodes />
        <InterrogatorTimeline onSelectIcao={onSelectIcao} streamData={pageStream?.interrogators ?? null} />
      </div>
      <MessageTimingPlot streamPacket={pageStream?.timing ?? null} />
      <div className={styles.row}>
        <BurstRateStripChart timingView={timingView} burstBinMs={burstBinMs} onBurstBinMsChange={setBurstBinMs} />
        <DFCadenceLanes timingView={timingView} cadenceBinMs={cadenceBinMs} onCadenceBinMsChange={setCadenceBinMs} />
      </div>
      <PipelineLatencyOscilloscope data={pageStream?.aggregates ?? null} latencyWindowS={latencyWindowS} onLatencyWindowSChange={setLatencyWindowS} />
    </main>
  )
}
