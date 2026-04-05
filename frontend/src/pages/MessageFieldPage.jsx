import { useEffect, useMemo, useRef, useState } from 'react'
import receiverStyles from './ReceiverPage.module.css'
import styles from './MessageFieldPage.module.css'
import { useTimingEventStream } from '../hooks/useTimingEventStream'
import { useTimingEventBuffer } from '../hooks/useTimingEventBuffer'
import {
  MESSAGE_FIELD_RENDER_HOLDBACK_US,
  MESSAGE_FIELD_SIGNAL_MIN_DBFS,
  messageFieldPointColour,
  selectMessageFieldEvents,
} from '../utils/messageField'
import { formatSignalDbfs } from '../utils/signal'

const PERSISTENCE_OPTIONS_S = [1, 3, 5, 10, 30]
const MESSAGE_FIELD_BUFFER_MAX = 240_000
const MODE_OPTIONS = [
  { value: 'bearing-time', label: 'Bearing × Time', geometries: ['cartesian'], defaultGeometry: 'cartesian' },
  { value: 'bearing-range', label: 'Bearing × Range', geometries: ['polar', 'cartesian'], defaultGeometry: 'polar' },
  { value: 'bearing-signal', label: 'Bearing × Signal', geometries: ['cartesian', 'polar'], defaultGeometry: 'cartesian' },
  { value: 'range-signal', label: 'Range × Signal', geometries: ['cartesian'], defaultGeometry: 'cartesian' },
]
const GEOMETRY_OPTIONS = [
  { value: 'polar', label: 'Polar' },
  { value: 'cartesian', label: 'Cartesian' },
]
const COLOUR_OPTIONS = [
  { value: 'df', label: 'DF' },
  { value: 'source', label: 'Source' },
  { value: 'iid', label: 'IID' },
  { value: 'signal', label: 'Signal' },
]
const TRAFFIC_OPTIONS = [
  { value: 'all', label: 'All traffic' },
  { value: 'df11', label: 'DF11' },
  { value: 'adsb', label: 'DF17 ADS-B' },
  { value: 'tisb', label: 'DF18 TIS-B' },
  { value: 'commb', label: 'DF20/21 Comm-B' },
  { value: 'surveillance', label: 'DF4/5 Surveillance' },
  { value: 'acas', label: 'DF0/16 ACAS' },
  { value: 'other', label: 'Other DF' },
]
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
const DF_LEGEND = [
  { df: 17, label: 'DF17', colour: '#388bfd' },
  { df: 18, label: 'DF18', colour: '#57a6ff' },
  { df: 11, label: 'DF11', colour: '#3fb950' },
  { df: 20, label: 'DF20/21', colour: '#bc8cff' },
  { df: 4, label: 'DF4/5', colour: '#d29922' },
]
const MAX_PERSISTENCE_US = Math.max(...PERSISTENCE_OPTIONS_S) * 1_000_000
const IID_OPTION_STICKY_US = 60_000_000
const MODE_CONFIG = Object.fromEntries(MODE_OPTIONS.map(option => [option.value, option]))
const AXIS_HEADROOM = 1.08
const AXIS_SHRINK_HOLD_MS = 45_000
const AXIS_SHRINK_TIME_CONSTANT_MS = 12_000

function smoothAxisMax(nextTarget, axisState, nowMs) {
  const safeTarget = Math.max(1, nextTarget)
  if (!axisState || !Number.isFinite(axisState.value) || axisState.value <= 0) {
    return { value: safeTarget, belowSinceMs: null, lastSampleMs: nowMs }
  }

  const previousValue = axisState.value
  const lastSampleMs = Number.isFinite(axisState.lastSampleMs) ? axisState.lastSampleMs : nowMs

  if (safeTarget >= previousValue) {
    return { value: safeTarget, belowSinceMs: null, lastSampleMs: nowMs }
  }

  const belowSinceMs = Number.isFinite(axisState.belowSinceMs) ? axisState.belowSinceMs : nowMs
  if (nowMs - belowSinceMs < AXIS_SHRINK_HOLD_MS) {
    return { value: previousValue, belowSinceMs, lastSampleMs: nowMs }
  }

  const deltaMs = Math.max(0, nowMs - lastSampleMs)
  const shrinkFraction = 1 - Math.exp(-deltaMs / AXIS_SHRINK_TIME_CONSTANT_MS)
  const nextValue = previousValue - (previousValue - safeTarget) * shrinkFraction
  return { value: nextValue, belowSinceMs, lastSampleMs: nowMs }
}

function formatAxisNumber(value, digits = 1) {
  const rounded = Number(value.toFixed(digits))
  return Number.isInteger(rounded) ? `${rounded}` : `${rounded}`
}

function formatTimeRemainingLabel(persistenceUs, fraction) {
  const seconds = (persistenceUs / 1_000_000) * (1 - fraction)
  return `${formatAxisNumber(seconds, 2)} s`
}

function Panel({ title, controls, children }) {
  return (
    <div className={receiverStyles.card}>
      <div className={receiverStyles.cardHeader}>
        <span className={receiverStyles.cardTitle}>{title}</span>
        {controls && <div className={receiverStyles.cardControls}>{controls}</div>}
      </div>
      {children}
    </div>
  )
}

function buildLegend(colourMode) {
  if (colourMode === 'df') return DF_LEGEND
  if (colourMode === 'source') {
    return Object.entries(SOURCE_LABELS).map(([sourceClass, label]) => ({
      label,
      colour: SOURCE_COLOURS[sourceClass] ?? SOURCE_COLOURS[0],
    }))
  }
  if (colourMode === 'iid') {
    return [
      { label: 'IID palette', colour: '#58a6ff' },
      { label: 'Unknown IID', colour: '#6e7681' },
    ]
  }
  return [
    { label: 'Strong', colour: '#3fb950' },
    { label: 'Mid', colour: '#d29922' },
    { label: 'Weak', colour: '#f85149' },
  ]
}

function drawCartesianAxes(ctx, left, top, plotW, plotH, xTicks, yTicks) {
  ctx.strokeStyle = '#21262d'
  ctx.lineWidth = 1
  for (const xTick of xTicks) {
    const x = left + xTick.fraction * plotW
    ctx.beginPath()
    ctx.moveTo(x, top)
    ctx.lineTo(x, top + plotH)
    ctx.stroke()
  }
  for (const yTick of yTicks) {
    const y = top + yTick.fraction * plotH
    ctx.beginPath()
    ctx.moveTo(left, y)
    ctx.lineTo(left + plotW, y)
    ctx.stroke()
  }
}

function drawPolarField({
  ctx,
  left,
  top,
  plotW,
  plotH,
  width,
  height,
  events,
  colourMode,
  renderNowUs,
  persistenceUs,
  radiusValue,
  maxRadiusValue,
  title,
  formatRingLabel,
  constantAlpha = false,
}) {
  const cx = left + plotW / 2
  const cy = top + plotH / 2
  const radius = Math.min(plotW / 2 - 8, plotH / 2 - 8)

  ctx.strokeStyle = '#21262d'
  ctx.lineWidth = 1
  for (let ring = 0.25; ring <= 1.0; ring += 0.25) {
    ctx.beginPath()
    ctx.arc(cx, cy, radius * ring, 0, Math.PI * 2)
    ctx.stroke()
    ctx.textAlign = 'left'
    ctx.textBaseline = 'middle'
    ctx.fillText(formatRingLabel(maxRadiusValue * ring), cx + 8, cy - radius * ring)
  }
  for (const deg of [0, 45, 90, 135, 180, 225, 270, 315]) {
    const rad = (deg - 90) * Math.PI / 180
    ctx.beginPath()
    ctx.moveTo(cx, cy)
    ctx.lineTo(cx + Math.cos(rad) * radius, cy + Math.sin(rad) * radius)
    ctx.stroke()
  }

  for (const ev of events) {
    if (!Number.isFinite(ev.bearing_deg)) continue
    const value = radiusValue(ev)
    if (!Number.isFinite(value)) continue
    const r = (value / Math.max(1e-6, maxRadiusValue)) * radius
    const theta = (ev.bearing_deg - 90) * Math.PI / 180
    const x = cx + Math.cos(theta) * r
    const y = cy + Math.sin(theta) * r
    const ageRatio = (renderNowUs - ev.arrival_us) / Math.max(1, persistenceUs)
    const alpha = constantAlpha ? 0.82 : 0.18 + (1 - Math.min(1, ageRatio)) * 0.8
    ctx.fillStyle = messageFieldPointColour(ev, colourMode)
    ctx.globalAlpha = alpha
    const size = ev.msg_len >= 14 ? 2.6 : 1.8
    ctx.fillRect(x - size / 2, y - size / 2, size, size)
    ctx.globalAlpha = 1
  }

  ctx.textAlign = 'left'
  ctx.textBaseline = 'alphabetic'
  ctx.fillText(title, left, height - 12)
  ctx.textAlign = 'right'
  ctx.fillText(`${events.length} points`, width - 12, height - 12)
}

function MessageFieldCanvas({
  timingView,
  persistenceUs,
  mode,
  geometry,
  colourMode,
  trafficFilter,
  iidFilter,
}) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const displayNowUsRef = useRef(0)
  const displayNowWallRef = useRef(performance.now())
  const timingViewRef = useRef(timingView)
  const rangeAxisMaxRef = useRef(null)
  const signalPolarMaxRef = useRef(null)
  timingViewRef.current = timingView

  useEffect(() => {
    const packetNowUs = Number(timingView?.nowUs ?? 0)
    const wallNowMs = performance.now()
    const projectedDisplayNowUs = displayNowUsRef.current > 0
      ? displayNowUsRef.current + Math.max(0, wallNowMs - displayNowWallRef.current) * 1000
      : packetNowUs
    displayNowUsRef.current = Math.max(projectedDisplayNowUs, packetNowUs)
    displayNowWallRef.current = wallNowMs
  }, [timingView?.nowUs])

  useEffect(() => {
    rangeAxisMaxRef.current = null
    signalPolarMaxRef.current = null
  }, [geometry, mode, persistenceUs, trafficFilter, iidFilter])

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      const timing = timingViewRef.current
      if (!canvas) return

      const w = canvas.offsetWidth || 1200
      const h = canvas.offsetHeight || 760
      canvas.width = w
      canvas.height = h
      const ctx = canvas.getContext('2d')
      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, w, h)

      const interpUs = Math.max(0, performance.now() - displayNowWallRef.current) * 1000
      const frameNowMs = performance.now()
      const renderClockUs = Math.max(0, displayNowUsRef.current + interpUs)
      const renderNowUs = Math.max(0, renderClockUs - MESSAGE_FIELD_RENDER_HOLDBACK_US)
      const events = selectMessageFieldEvents({
        events: timing?.events ?? [],
        renderNowUs,
        persistenceUs,
        trafficFilter,
        iidFilter,
      })

      const left = geometry === 'polar' ? 18 : 62
      const top = 18
      const right = geometry === 'polar' ? 18 : 18
      const bottom = geometry === 'polar' ? 28 : 38
      const plotW = Math.max(80, w - left - right)
      const plotH = Math.max(80, h - top - bottom)

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'

      if (mode === 'bearing-range' && geometry === 'polar') {
        const targetRangeMax = Math.max(50, Math.max(1, ...events.map(ev => ev.range_nm ?? 0)) * AXIS_HEADROOM)
        const nextRangeState = smoothAxisMax(targetRangeMax, rangeAxisMaxRef.current, frameNowMs)
        rangeAxisMaxRef.current = nextRangeState
        const maxRange = nextRangeState.value
        drawPolarField({
          ctx,
          left,
          top,
          plotW,
          plotH,
          width: w,
          height: h,
          events,
          colourMode,
          renderNowUs,
          persistenceUs,
          radiusValue: ev => ev.range_nm,
          maxRadiusValue: maxRange,
          title: 'Polar bearing × range',
          formatRingLabel: value => `${value.toFixed(value >= 10 ? 0 : 1)} nm`,
        })
        return
      }

      if (mode === 'bearing-signal' && geometry === 'polar') {
        const targetSignalMax = Math.max(
          10,
          Math.max(
            ...events.map(ev => Math.max(0, (Number(ev.signal_dbfs) || MESSAGE_FIELD_SIGNAL_MIN_DBFS) - MESSAGE_FIELD_SIGNAL_MIN_DBFS)),
            10,
          ) * AXIS_HEADROOM,
        )
        const nextSignalState = smoothAxisMax(targetSignalMax, signalPolarMaxRef.current, frameNowMs)
        signalPolarMaxRef.current = nextSignalState
        const signalRadiusMax = nextSignalState.value
        drawPolarField({
          ctx,
          left,
          top,
          plotW,
          plotH,
          width: w,
          height: h,
          events,
          colourMode,
          renderNowUs,
          persistenceUs,
          radiusValue: ev => Math.max(0, (Number(ev.signal_dbfs) || MESSAGE_FIELD_SIGNAL_MIN_DBFS) - MESSAGE_FIELD_SIGNAL_MIN_DBFS),
          maxRadiusValue: signalRadiusMax,
          title: 'Polar bearing × signal',
          formatRingLabel: value => `${(MESSAGE_FIELD_SIGNAL_MIN_DBFS + value).toFixed(0)} dBFS`,
        })
        return
      }

      const xTicks = []
      const yTicks = []
      let xValue = null
      let yValue = null
      let xLabel = ''
      let yLabel = ''
      let xMin = 0
      let xMax = 1
      let yMin = 0
      let yMax = 1

      if (mode === 'bearing-time') {
        xMin = Math.max(0, renderNowUs - persistenceUs)
        xMax = renderNowUs
        yMin = 0
        yMax = 360
        xLabel = 'time'
        yLabel = 'bearing deg'
        xValue = ev => ev.arrival_us
        yValue = ev => ev.bearing_deg
        for (let i = 0; i <= 5; i += 1) {
          xTicks.push({ fraction: i / 5, label: formatTimeRemainingLabel(persistenceUs, i / 5) })
        }
        for (const deg of [360, 270, 180, 90, 0]) {
          yTicks.push({ fraction: (360 - deg) / 360, label: `${deg}` })
        }
      } else if (mode === 'bearing-range') {
        xMin = 0
        xMax = 360
        yMin = 0
        const targetRangeMax = Math.max(50, Math.max(1, ...events.map(ev => ev.range_nm ?? 0)) * AXIS_HEADROOM)
        const nextRangeState = smoothAxisMax(targetRangeMax, rangeAxisMaxRef.current, frameNowMs)
        rangeAxisMaxRef.current = nextRangeState
        yMax = nextRangeState.value
        xLabel = 'bearing deg'
        yLabel = 'range nm'
        xValue = ev => ev.bearing_deg
        yValue = ev => ev.range_nm
        for (const deg of [0, 90, 180, 270, 360]) {
          xTicks.push({ fraction: deg / 360, label: `${deg}` })
        }
        for (let i = 0; i <= 4; i += 1) {
          const fraction = i / 4
          yTicks.push({ fraction, label: `${formatAxisNumber(yMax * (1 - fraction), 0)}` })
        }
      } else if (mode === 'bearing-signal') {
        xMin = 0
        xMax = 360
        yMin = MESSAGE_FIELD_SIGNAL_MIN_DBFS
        yMax = 0
        xLabel = 'bearing deg'
        yLabel = 'signal dBFS'
        xValue = ev => ev.bearing_deg
        yValue = ev => ev.signal_dbfs
        for (const deg of [0, 90, 180, 270, 360]) {
          xTicks.push({ fraction: deg / 360, label: `${deg}` })
        }
        for (const dbfs of [0, -10, -20, -30, -40, -45]) {
          yTicks.push({ fraction: (dbfs - yMax) / (yMin - yMax), label: `${dbfs}` })
        }
      } else {
        xMin = 0
        const targetRangeMax = Math.max(50, Math.max(1, ...events.map(ev => ev.range_nm ?? 0)) * AXIS_HEADROOM)
        const nextRangeState = smoothAxisMax(targetRangeMax, rangeAxisMaxRef.current, frameNowMs)
        rangeAxisMaxRef.current = nextRangeState
        xMax = nextRangeState.value
        yMin = MESSAGE_FIELD_SIGNAL_MIN_DBFS
        yMax = 0
        xLabel = 'range nm'
        yLabel = 'signal dBFS'
        xValue = ev => ev.range_nm
        yValue = ev => ev.signal_dbfs
        for (let i = 0; i <= 4; i += 1) {
          const fraction = i / 4
          xTicks.push({ fraction, label: `${formatAxisNumber(xMax * fraction, 0)}` })
        }
        for (const dbfs of [0, -10, -20, -30, -40, -45]) {
          yTicks.push({ fraction: (dbfs - yMax) / (yMin - yMax), label: `${dbfs}` })
        }
      }

      drawCartesianAxes(ctx, left, top, plotW, plotH, xTicks, yTicks)
      ctx.textAlign = 'right'
      ctx.textBaseline = 'middle'
      for (const tick of yTicks) {
        ctx.fillText(tick.label, left - 6, top + tick.fraction * plotH)
      }
      ctx.textAlign = 'center'
      ctx.textBaseline = 'alphabetic'
      for (const tick of xTicks) {
        ctx.fillText(tick.label, left + tick.fraction * plotW, h - 16)
      }
      ctx.textAlign = 'left'
      ctx.fillText(xLabel, left, h - 4)
      ctx.save()
      ctx.translate(12, top + plotH / 2)
      ctx.rotate(-Math.PI / 2)
      ctx.fillText(yLabel, 0, 0)
      ctx.restore()

      for (const ev of events) {
        const xRaw = xValue(ev)
        const yRaw = yValue(ev)
        if (!Number.isFinite(xRaw) || !Number.isFinite(yRaw)) continue
        if (xRaw < xMin || xRaw > xMax || yRaw < yMin || yRaw > yMax) continue
        const x = left + ((xRaw - xMin) / Math.max(1e-6, xMax - xMin)) * plotW
        const y = top + plotH - ((yRaw - yMin) / Math.max(1e-6, yMax - yMin)) * plotH
        const ageRatio = (renderNowUs - ev.arrival_us) / Math.max(1, persistenceUs)
        const alpha = mode === 'bearing-time'
          ? (persistenceUs <= 3_000_000 ? 0.96 : 0.9)
          : 0.12 + (1 - Math.min(1, ageRatio)) * 0.86
        ctx.fillStyle = messageFieldPointColour(ev, colourMode)
        ctx.globalAlpha = alpha
        const size = mode === 'bearing-time'
          ? (persistenceUs <= 3_000_000
            ? (ev.msg_len >= 14 ? 2.5 : 1.8)
            : (ev.msg_len >= 14 ? 2.2 : 1.5))
          : (ev.msg_len >= 14 ? 2.2 : 1.5)
        ctx.fillRect(x - size / 2, y - size / 2, size, size)
        ctx.globalAlpha = 1
      }

      ctx.textAlign = 'right'
      ctx.fillText(`${events.length} points`, w - 12, h - 4)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => { if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [colourMode, geometry, iidFilter, mode, persistenceUs, trafficFilter])

  return <canvas ref={canvasRef} className={styles.canvas} />
}

export default function MessageFieldPage() {
  const [mode, setMode] = useState('bearing-time')
  const [geometry, setGeometry] = useState(MODE_CONFIG['bearing-time'].defaultGeometry)
  const [persistenceS, setPersistenceS] = useState(3)
  const [colourMode, setColourMode] = useState('df')
  const [trafficFilter, setTrafficFilter] = useState('all')
  const [iidFilterRaw, setIidFilterRaw] = useState('all')
  const persistenceUs = persistenceS * 1_000_000

  const packet = useTimingEventStream()
  const timingView = useTimingEventBuffer(packet, MAX_PERSISTENCE_US, MESSAGE_FIELD_BUFFER_MAX)
  const iidSeenRef = useRef(new Map())

  const renderNowUs = useMemo(() => timingView.nowUs, [timingView.nowUs])
  const eventsForIidOptions = useMemo(() => selectMessageFieldEvents({
    events: timingView.events,
    renderNowUs,
    persistenceUs,
    trafficFilter,
    iidFilter: 'all',
  }), [persistenceUs, renderNowUs, timingView.events, trafficFilter])
  const filteredEvents = useMemo(() => selectMessageFieldEvents({
    events: timingView.events,
    renderNowUs,
    persistenceUs,
    trafficFilter,
    iidFilter: iidFilterRaw === 'all' || iidFilterRaw === 'exclude-zero'
      ? iidFilterRaw
      : Number(iidFilterRaw),
  }), [iidFilterRaw, persistenceUs, renderNowUs, timingView.events, trafficFilter])
  const activeIids = useMemo(() => {
    const seen = iidSeenRef.current
    const nowUs = Number(renderNowUs || 0)
    for (const ev of eventsForIidOptions) {
      if (Number.isInteger(ev.iid)) {
        seen.set(ev.iid, ev.arrival_us)
      }
    }
    for (const [iid, lastSeenUs] of seen.entries()) {
      if (nowUs > 0 && lastSeenUs < nowUs - IID_OPTION_STICKY_US) {
        seen.delete(iid)
      }
    }
    return [...seen.keys()].sort((a, b) => a - b)
  }, [eventsForIidOptions, renderNowUs])
  const iidFilter = iidFilterRaw === 'all' || iidFilterRaw === 'exclude-zero'
    ? iidFilterRaw
    : Number(iidFilterRaw)
  const availableGeometries = MODE_CONFIG[mode]?.geometries ?? GEOMETRY_OPTIONS.map(option => option.value)
  const geometryOptions = GEOMETRY_OPTIONS.filter(option => availableGeometries.includes(option.value))
  const legend = useMemo(() => buildLegend(colourMode), [colourMode])
  const strongestSignal = useMemo(() => {
    const signals = filteredEvents.map(ev => ev.signal_dbfs).filter(value => Number.isFinite(value))
    return signals.length ? Math.max(...signals) : null
  }, [filteredEvents])

  useEffect(() => {
    if (availableGeometries.includes(geometry)) return
    setGeometry(MODE_CONFIG[mode]?.defaultGeometry ?? availableGeometries[0] ?? 'cartesian')
  }, [availableGeometries, geometry, mode])

  return (
    <main className={receiverStyles.main}>
      <Panel title="Exact Message Field">
        <div className={styles.controlBar}>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>Plot Mode</span>
            <select className={receiverStyles.select} value={mode} onChange={e => setMode(e.target.value)}>
              {MODE_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>Projection</span>
            <select
              className={receiverStyles.select}
              value={geometry}
              onChange={e => setGeometry(e.target.value)}
              disabled={geometryOptions.length === 1}
            >
              {geometryOptions.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>Persistence</span>
            <select className={receiverStyles.select} value={persistenceS} onChange={e => setPersistenceS(Number(e.target.value))}>
              {PERSISTENCE_OPTIONS_S.map(value => <option key={value} value={value}>{value} s</option>)}
            </select>
          </label>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>Colour</span>
            <select className={receiverStyles.select} value={colourMode} onChange={e => setColourMode(e.target.value)}>
              {COLOUR_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>Traffic</span>
            <select className={receiverStyles.select} value={trafficFilter} onChange={e => setTrafficFilter(e.target.value)}>
              {TRAFFIC_OPTIONS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className={styles.controlGroup}>
            <span className={styles.controlLabel}>IID Filter</span>
            <select className={receiverStyles.select} value={iidFilterRaw} onChange={e => setIidFilterRaw(e.target.value)}>
              <option value="all">All IID</option>
              <option value="exclude-zero">Exclude IID 0</option>
              {activeIids.map(iid => <option key={iid} value={iid}>IID {iid}</option>)}
            </select>
          </label>
        </div>

        <div className={styles.meta}>
          <span>{filteredEvents.length.toLocaleString()} visible points</span>
          <span>{timingView.events.length.toLocaleString()} buffered points</span>
          <span>Strongest visible signal: {formatSignalDbfs(strongestSignal)}</span>
          <span>Projection: {geometryOptions.find(option => option.value === geometry)?.label ?? geometry}</span>
          <span>
            IID filter: {iidFilter === 'all'
              ? 'all active'
              : iidFilter === 'exclude-zero'
                ? 'exclude IID 0'
                : `IID ${iidFilter}`}
          </span>
          {geometryOptions.length === 1 && (
            <span>{MODE_CONFIG[mode]?.label} stays on a fixed projection</span>
          )}
          {filteredEvents.length === 0 && (
            <span style={{ marginLeft: 'auto' }}>No points in the active filter/window</span>
          )}
        </div>

        <div className={styles.canvasWrap}>
          <MessageFieldCanvas
            timingView={timingView}
            persistenceUs={persistenceUs}
            mode={mode}
            geometry={geometry}
            colourMode={colourMode}
            trafficFilter={trafficFilter}
            iidFilter={iidFilter}
          />
        </div>

        <div className={styles.legend}>
          {legend.map(item => (
            <span key={item.label} className={styles.legendItem}>
              <span className={styles.legendSwatch} style={{ background: item.colour }} />
              {item.label}
            </span>
          ))}
        </div>
      </Panel>
    </main>
  )
}
