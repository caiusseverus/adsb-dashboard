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

const RENDER_HOLDBACK_US = 650_000
const TIMING_BUFFER_MAX = 60_000
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
      const [seq, arrival_us, df, msg_len] = ev
      return { seq, arrival_us, df, msg_len }
    })
    const merged = [...bufRef.current, ...newEvents]
      .filter(ev => ev.arrival_us >= cutoffUs)
    if (merged.length > TIMING_BUFFER_MAX) merged.splice(0, merged.length - TIMING_BUFFER_MAX)
    bufRef.current = merged
    setView({ nowUs, events: merged })
  }, [timingPacket, windowUs])

  return view
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
    </main>
  )
}
