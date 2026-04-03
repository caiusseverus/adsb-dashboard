import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import styles from './SkyView.module.css'
import { EMERGENCY_SQUAWKS } from '../utils/squawks'
import { useAircraftFilter } from '../hooks/useAircraftFilter'
import { TYPE_GROUPS, TYPE_GROUP_OTHER_COLOR, getTypeGroup, buildNameColorMap } from '../utils/typeGroups'

const FEET_PER_NM      = 6076.115
const TRACK_POLL_MS    = 5000
const CANVAS_SIZE      = 600
const MARGIN           = 44    // pixels around the polar disc for labels
const HOVER_RADIUS_PX  = 15   // max distance to register a hover/click
// Break a trail segment if consecutive points are more than this many seconds apart
const MAX_TRAIL_GAP_S  = 20

const HORIZON_W  = 900
const HORIZON_H  = 380   // 340px plot area + 40px bottom label zone
const HORIZON_ML = 38    // left margin for elevation labels
const HORIZON_MB = 24    // bottom margin for compass labels

// Y-axis scale functions: map elevation fraction [0,1] → display fraction [0,1]
// Fraction 0 = horizon, 1 = maxElev (top of plot)
const SCALES = {
  linear: f => f,
  sqrt:   f => Math.sqrt(f),
  log:    f => f <= 0 ? 0 : Math.log1p(f * Math.E) / Math.log1p(Math.E),
}

const SCALE_OPTIONS = [
  { value: 'linear', label: 'Linear' },
  { value: 'sqrt',   label: '√ Scale' },
  { value: 'log',    label: 'Log Scale' },
]

const FILTERS = [
  { value: 'all',         label: 'All' },
  { value: 'military',    label: 'Military' },
  { value: 'mlat',        label: 'MLAT' },
  { value: 'interesting', label: 'Interesting' },
  { value: 'emergency',   label: 'Emergency' },
]

const COLOR_MODES = [
  { value: 'classification', label: 'Classification' },
  { value: 'type_group',     label: 'Type' },
  { value: 'operator',       label: 'Operator' },
]

// Elevation angle in degrees above horizon. Returns null if inputs missing.
function elevDeg(altitude_ft, range_nm) {
  if (altitude_ft == null || range_nm == null || range_nm <= 0) return null
  return Math.atan2(altitude_ft, range_nm * FEET_PER_NM) * (180 / Math.PI)
}

// Aircraft colour based on active colour mode.
function acColor(ac, colorMode, operatorMap) {
  if (colorMode === 'type_group') {
    const g = getTypeGroup(ac.type_code, ac.type_category)
    return g ? g.color : TYPE_GROUP_OTHER_COLOR
  }
  if (colorMode === 'operator') {
    return (ac.operator && operatorMap[ac.operator]) ? operatorMap[ac.operator] : TYPE_GROUP_OTHER_COLOR
  }
  // classification (default)
  if (ac.squawk && EMERGENCY_SQUAWKS[ac.squawk]) return '#f85149'
  if (ac.military)    return '#bc8cff'
  if (ac.mlat)        return '#388bfd'
  if (ac.interesting) return '#d29922'
  return '#3fb950'
}

// Trail colour uses the last point's metadata
function trailColor(trailObj, colorMode, operatorMap) {
  return acColor(trailObj, colorMode, operatorMap)
}

function findNearestInList(list, mx, my) {
  let best = null, bestDist = HOVER_RADIUS_PX
  for (const h of list) {
    const d = Math.hypot(h.x - mx, h.y - my)
    if (d < bestDist) { best = h; bestDist = d }
  }
  return best
}

export default function SkyView({ snapshot, onSelectIcao }) {
  // ── Canvas refs ─────────────────────────────────────────────────────
  const canvasRef         = useRef(null)
  const horizonCanvasRef  = useRef(null)

  // ── Track / hit data ────────────────────────────────────────────────
  const tracksRef          = useRef({})
  const hoverRef           = useRef(null)
  const hitboxesRef        = useRef([])
  const horizonHoverRef    = useRef(null)
  const horizonHitboxesRef = useRef([])

  // ── Component state ─────────────────────────────────────────────────
  const [hoveredAc,      setHoveredAc]      = useState(null)
  const { filter, setFilter, passesFilter } = useAircraftFilter('all')
  const [colorMode,      setColorMode]      = useState('classification')
  const [typeGroup,      setTypeGroup]      = useState('all')
  const [typeCode,       setTypeCode]       = useState('all')
  const [maxElev,        setMaxElev]        = useState(30)
  const [horizonScale,   setHorizonScale]   = useState('sqrt')
  const [terrainRanges,  setTerrainRanges]  = useState(null)  // {ranges, profiles} or null
  const [histMode,       setHistMode]       = useState(false)
  const [histData,       setHistData]       = useState(null)
  const [histHours,      setHistHours]      = useState(24)

  // ── Fetch multi-range terrain profiles once on mount ────────────────
  useEffect(() => {
    fetch('/api/terrain/ranges')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.profiles) setTerrainRanges(d) })
      .catch(() => {})
  }, [])

  // ── Fetch Sky View history dots when enabled ─────────────────────────
  useEffect(() => {
    if (!histMode) { setHistData(null); return }
    setHistData(null)
    fetch(`/api/history/skyview/points?hours=${histHours}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.points) setHistData(d) })
      .catch(() => {})
  }, [histMode, histHours])

  // ── Poll track history every 5 s ────────────────────────────────────
  useEffect(() => {
    let cancelled = false
    const fetchTracks = async () => {
      try {
        const res = await fetch('/api/tracks')
        if (res.ok && !cancelled) tracksRef.current = await res.json()
      } catch { /* backend not ready */ }
    }
    fetchTracks()
    const id = setInterval(fetchTracks, TRACK_POLL_MS)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  // ── Filtered aircraft & operator colour map ──────────────────────────
  const baseAircraft = snapshot?.aircraft ?? []
  const aircraft = useMemo(() => baseAircraft
    .filter(ac => passesFilter(ac))
    .filter(ac => typeGroup === 'all' || getTypeGroup(ac.type_code, ac.type_category)?.value === typeGroup)
    .filter(ac => typeCode === 'all' || ac.type_code === typeCode),
  [snapshot, filter, typeGroup, typeCode])

  function matchesTypeFilter(ac) {
    return (typeGroup === 'all' || getTypeGroup(ac.type_code, ac.type_category)?.value === typeGroup)
      && (typeCode === 'all' || ac.type_code === typeCode)
  }

  // Unique type codes visible in current snapshot (for type code dropdown)
  const visibleTypeCodes = useMemo(() =>
    [...new Set(baseAircraft.map(ac => ac.type_code).filter(Boolean))].sort(),
  [snapshot])

  const operatorMap = useMemo(
    () => buildNameColorMap(baseAircraft, 'operator').map,
    [snapshot]
  )
  // Top operators for legend
  const topOperators = useMemo(
    () => buildNameColorMap(baseAircraft, 'operator').top,
    [snapshot]
  )

  // ── Helper: should we draw this trail given the current filter? ──────
  const trailPassesFilter = trailObj => passesFilter(trailObj) && matchesTypeFilter(trailObj)

  // ── Polar canvas draw ────────────────────────────────────────────────
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')

    const cx = CANVAS_SIZE / 2
    const cy = CANVAS_SIZE / 2
    const R  = CANVAS_SIZE / 2 - MARGIN

    ctx.fillStyle = '#0b0c10'
    ctx.fillRect(0, 0, CANVAS_SIZE, CANVAS_SIZE)

    // Elevation rings
    ;[0, 15, 30, 45, 60, 75].forEach(elev => {
      const r = (90 - elev) / 90 * R
      ctx.beginPath()
      ctx.arc(cx, cy, r, 0, Math.PI * 2)
      ctx.strokeStyle = elev === 0 ? '#30363d' : '#1c2128'
      ctx.lineWidth   = elev === 0 ? 1.5 : 1
      ctx.stroke()
      if (elev > 0) {
        ctx.fillStyle    = '#484f58'
        ctx.font         = '10px monospace'
        ctx.textAlign    = 'left'
        ctx.textBaseline = 'top'
        ctx.fillText(`${elev}°`, cx + 4, cy - r + 2)
      }
    })

    // Azimuth spokes & compass labels
    const spokes = [
      { deg: 0,   label: 'N',  cardinal: true  },
      { deg: 45,  label: 'NE', cardinal: false },
      { deg: 90,  label: 'E',  cardinal: true  },
      { deg: 135, label: 'SE', cardinal: false },
      { deg: 180, label: 'S',  cardinal: true  },
      { deg: 225, label: 'SW', cardinal: false },
      { deg: 270, label: 'W',  cardinal: true  },
      { deg: 315, label: 'NW', cardinal: false },
    ]
    spokes.forEach(({ deg, label, cardinal }) => {
      const rad = (deg - 90) * Math.PI / 180
      ctx.beginPath()
      ctx.moveTo(cx, cy)
      ctx.lineTo(cx + Math.cos(rad) * R, cy + Math.sin(rad) * R)
      ctx.strokeStyle = cardinal ? '#30363d' : '#1c2128'
      ctx.lineWidth   = 1
      ctx.stroke()
      const lr = R + 22
      ctx.fillStyle    = cardinal ? '#8b949e' : '#484f58'
      ctx.font         = cardinal ? 'bold 12px monospace' : '10px monospace'
      ctx.textAlign    = 'center'
      ctx.textBaseline = 'middle'
      ctx.fillText(label, cx + Math.cos(rad) * lr, cy + Math.sin(rad) * lr)
    })
    ctx.textBaseline = 'alphabetic'

    const toXY = (bearing_deg, range_nm, altitude_ft) => {
      const elev = elevDeg(altitude_ft, range_nm)
      if (elev === null) return null
      const rad   = (bearing_deg - 90) * Math.PI / 180
      const rFrac = (90 - Math.max(0, elev)) / 90
      return {
        x: cx + Math.cos(rad) * R * rFrac,
        y: cy + Math.sin(rad) * R * rFrac,
      }
    }

    // Trails
    const tracks = tracksRef.current
    Object.values(tracks).forEach(points => {
      if (points.length < 2) return
      const last = points[points.length - 1]
      if (!trailPassesFilter(last)) return
      const color = trailColor(last, colorMode, operatorMap)
      ctx.beginPath()
      let penDown = false
      for (let i = 0; i < points.length; i++) {
        const p   = points[i]
        const pos = toXY(p.bearing_deg, p.range_nm, p.altitude_ft)
        if (!pos || (i > 0 && p.ts - points[i - 1].ts > MAX_TRAIL_GAP_S)) {
          penDown = false
          continue
        }
        if (!penDown) { ctx.moveTo(pos.x, pos.y); penDown = true }
        else            ctx.lineTo(pos.x, pos.y)
      }
      ctx.globalAlpha = 0.35
      ctx.strokeStyle = color
      ctx.lineWidth   = 1.5
      ctx.stroke()
      ctx.globalAlpha = 1
    })

    // Live dots
    const hitboxes = []
    aircraft.forEach(ac => {
      if (ac.bearing_deg == null || ac.range_nm == null) return
      const pos = toXY(ac.bearing_deg, ac.range_nm, ac.altitude)
      if (!pos) return

      const color = acColor(ac, colorMode, operatorMap)
      const dotR  = 3

      const hover = hoverRef.current
      if (hover && Math.hypot(hover.x - pos.x, hover.y - pos.y) < HOVER_RADIUS_PX) {
        ctx.beginPath()
        ctx.arc(pos.x, pos.y, dotR + 4, 0, Math.PI * 2)
        ctx.strokeStyle = color
        ctx.lineWidth   = 1.5
        ctx.stroke()
      }

      ctx.beginPath()
      ctx.arc(pos.x, pos.y, dotR, 0, Math.PI * 2)
      ctx.fillStyle = color
      ctx.fill()

      hitboxes.push({ icao: ac.icao, x: pos.x, y: pos.y, ac })
    })
    hitboxesRef.current = hitboxes
  }, [snapshot, filter, colorMode, typeGroup, typeCode])

  // ── Horizon canvas draw ──────────────────────────────────────────────
  useEffect(() => {
    const canvas = horizonCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')

    const plotW   = HORIZON_W - HORIZON_ML
    const plotH   = HORIZON_H - HORIZON_MB
    const scaleFn = SCALES[horizonScale]

    // Convert elevation (degrees) to y pixel — 0° at bottom, maxElev at top
    const elevToY = elev => plotH * (1 - scaleFn(Math.max(0, Math.min(elev, maxElev)) / maxElev))

    ctx.fillStyle = '#0b0c10'
    ctx.fillRect(0, 0, HORIZON_W, HORIZON_H)

    // Map (bearing, range, altitude) → horizon canvas (x, y)
    const toHXY = (bearing_deg, range_nm, altitude_ft) => {
      const elev = elevDeg(altitude_ft, range_nm)
      if (elev === null || elev < 0) return null
      if (elev > maxElev) return null
      return {
        x: HORIZON_ML + (bearing_deg / 360) * plotW,
        y: elevToY(elev),
      }
    }

    // ── Terrain pseudo-3D: layered range bands (back → front) ──────────
    // profiles[0] = 0–25 nm cumulative max, profiles[3] = 0–100 nm.
    // Drawing back-to-front (farthest first) lets closer, darker layers
    // naturally occlude farther ones — correct hidden-surface removal.
    if (terrainRanges?.profiles?.length === 4) {
      const { profiles } = terrainRanges
      // Colours: lightest (farthest, 100 nm) → darkest (closest, 25 nm)
      const fills    = ['#19150f', '#1e190f', '#251f13', '#2e2416']
      const outlines = ['#3a2f1e', '#3e3220', '#433524', '#4a3728']
      // Draw farthest layer first, closest last
      for (let i = profiles.length - 1; i >= 0; i--) {
        const profile = profiles[i]
        if (!profile || profile.length !== 360) continue

        // Filled silhouette
        ctx.beginPath()
        ctx.moveTo(HORIZON_ML, plotH)
        for (let az = 0; az < 360; az++) {
          ctx.lineTo(HORIZON_ML + (az / 360) * plotW, elevToY(profile[az]))
        }
        ctx.lineTo(HORIZON_ML + plotW, plotH)
        ctx.closePath()
        ctx.fillStyle = fills[i]
        ctx.fill()

        // Ridge outline (only on the closest layer — would be invisible on others)
        if (i === 0) {
          ctx.beginPath()
          for (let az = 0; az < 360; az++) {
            const x = HORIZON_ML + (az / 360) * plotW
            const y = elevToY(profile[az])
            if (az === 0) ctx.moveTo(x, y)
            else          ctx.lineTo(x, y)
          }
          ctx.lineTo(HORIZON_ML + plotW, elevToY(profile[0]))
          ctx.strokeStyle = outlines[i]
          ctx.lineWidth   = 1
          ctx.stroke()
        }
      }
    }

    // ── Elevation grid lines ───────────────────────────────────────────
    // Draw at natural degree values regardless of scale
    const step = maxElev <= 30 ? 5 : 15
    for (let e = 0; e <= maxElev; e += step) {
      const y = elevToY(e)
      ctx.beginPath()
      ctx.moveTo(HORIZON_ML, y)
      ctx.lineTo(HORIZON_ML + plotW, y)
      ctx.strokeStyle = e === 0 ? '#30363d' : '#1c2128'
      ctx.lineWidth   = e === 0 ? 1.5 : 1
      ctx.stroke()
      ctx.fillStyle    = '#484f58'
      ctx.font         = '10px monospace'
      ctx.textAlign    = 'right'
      ctx.textBaseline = 'middle'
      ctx.fillText(`${e}°`, HORIZON_ML - 4, y)
    }

    // ── Azimuth vertical lines & compass labels ────────────────────────
    const compassPoints = [
      { az: 0,   label: 'N' },
      { az: 45,  label: 'NE' },
      { az: 90,  label: 'E' },
      { az: 135, label: 'SE' },
      { az: 180, label: 'S' },
      { az: 225, label: 'SW' },
      { az: 270, label: 'W' },
      { az: 315, label: 'NW' },
    ]
    compassPoints.forEach(({ az, label }) => {
      const x       = HORIZON_ML + (az / 360) * plotW
      const cardinal = label.length === 1
      ctx.beginPath()
      ctx.setLineDash([4, 4])
      ctx.moveTo(x, 0)
      ctx.lineTo(x, plotH)
      ctx.strokeStyle = cardinal ? '#30363d' : '#1c2128'
      ctx.lineWidth   = 1
      ctx.stroke()
      ctx.setLineDash([])
      ctx.fillStyle    = cardinal ? '#8b949e' : '#484f58'
      ctx.font         = cardinal ? 'bold 11px monospace' : '10px monospace'
      ctx.textAlign    = 'center'
      ctx.textBaseline = 'top'
      ctx.fillText(label, x, plotH + 4)
    })
    // N label also at right edge (360°)
    ctx.fillStyle    = '#8b949e'
    ctx.font         = 'bold 11px monospace'
    ctx.textAlign    = 'center'
    ctx.textBaseline = 'top'
    ctx.fillText('N', HORIZON_ML + plotW, plotH + 4)
    ctx.textBaseline = 'alphabetic'

    // ── History dots (individual coverage_samples points) ────────────
    if (histData?.points?.length) {
      for (const [bearing, range, alt, sigRaw] of histData.points) {
        const pos = toHXY(bearing, range, alt)
        if (!pos) continue
        // Map signal raw byte to colour: 0 (strongest) = green, 255 (weakest) = red
        const pct = Math.max(0, Math.min(255, sigRaw)) / 255
        const r = Math.round(60 + 180 * pct)
        const g = Math.round(180 - 120 * pct)
        ctx.fillStyle = `rgba(${r},${g},50,0.35)`
        ctx.beginPath()
        ctx.arc(pos.x, pos.y, 1.5, 0, Math.PI * 2)
        ctx.fill()
      }
    }

    // ── Trails ─────────────────────────────────────────────────────────
    const tracks = tracksRef.current
    Object.values(tracks).forEach(points => {
      if (points.length < 2) return
      const last = points[points.length - 1]
      if (!trailPassesFilter(last)) return
      const color = trailColor(last, colorMode, operatorMap)
      ctx.beginPath()
      let penDown = false
      for (let i = 0; i < points.length; i++) {
        const p   = points[i]
        const pos = toHXY(p.bearing_deg, p.range_nm, p.altitude_ft)
        // Break on time gap or azimuth wrap (crossing 0°/360°)
        const timeGap = i > 0 && p.ts - points[i - 1].ts > MAX_TRAIL_GAP_S
        const azWrap  = i > 0 && Math.abs(p.bearing_deg - points[i - 1].bearing_deg) > 180
        if (!pos || timeGap || azWrap) {
          penDown = false
          continue
        }
        if (!penDown) { ctx.moveTo(pos.x, pos.y); penDown = true }
        else            ctx.lineTo(pos.x, pos.y)
      }
      ctx.globalAlpha = 0.35
      ctx.strokeStyle = color
      ctx.lineWidth   = 1.5
      ctx.stroke()
      ctx.globalAlpha = 1
    })

    // ── Live dots ──────────────────────────────────────────────────────
    const hitboxes = []
    aircraft.forEach(ac => {
      if (ac.bearing_deg == null || ac.range_nm == null) return
      const pos = toHXY(ac.bearing_deg, ac.range_nm, ac.altitude)
      if (!pos) return

      const color = acColor(ac, colorMode, operatorMap)
      const dotR  = 3

      const hover = horizonHoverRef.current
      if (hover && Math.hypot(hover.x - pos.x, hover.y - pos.y) < HOVER_RADIUS_PX) {
        ctx.beginPath()
        ctx.arc(pos.x, pos.y, dotR + 4, 0, Math.PI * 2)
        ctx.strokeStyle = color
        ctx.lineWidth   = 1.5
        ctx.stroke()
      }

      ctx.beginPath()
      ctx.arc(pos.x, pos.y, dotR, 0, Math.PI * 2)
      ctx.fillStyle = color
      ctx.fill()

      hitboxes.push({ icao: ac.icao, x: pos.x, y: pos.y, ac })
    })
    horizonHitboxesRef.current = hitboxes
  }, [snapshot, maxElev, horizonScale, filter, colorMode, terrainRanges, histData, typeGroup, typeCode])

  // ── Pointer helpers ──────────────────────────────────────────────────
  const canvasCoords = (e, canvasEl) => {
    if (!canvasEl) return null
    const rect   = canvasEl.getBoundingClientRect()
    const w      = canvasEl.width
    const h      = canvasEl.height
    return {
      x: (e.clientX - rect.left) * (w / rect.width),
      y: (e.clientY - rect.top)  * (h / rect.height),
    }
  }

  const handleMouseMove = useCallback((e) => {
    const pos = canvasCoords(e, canvasRef.current)
    if (!pos) return
    hoverRef.current = pos
    setHoveredAc(findNearestInList(hitboxesRef.current, pos.x, pos.y)?.ac ?? null)
  }, [])

  const handleMouseLeave = useCallback(() => {
    hoverRef.current = null
    setHoveredAc(null)
  }, [])

  const handleClick = useCallback((e) => {
    const pos = canvasCoords(e, canvasRef.current)
    if (!pos) return
    const hit = findNearestInList(hitboxesRef.current, pos.x, pos.y)
    if (hit && onSelectIcao) onSelectIcao(hit.icao)
  }, [onSelectIcao])

  const handleHorizonMouseMove = useCallback((e) => {
    const pos = canvasCoords(e, horizonCanvasRef.current)
    if (!pos) return
    horizonHoverRef.current = pos
    setHoveredAc(findNearestInList(horizonHitboxesRef.current, pos.x, pos.y)?.ac ?? null)
  }, [])

  const handleHorizonMouseLeave = useCallback(() => {
    horizonHoverRef.current = null
    setHoveredAc(null)
  }, [])

  const handleHorizonClick = useCallback((e) => {
    const pos = canvasCoords(e, horizonCanvasRef.current)
    if (!pos) return
    const hit = findNearestInList(horizonHitboxesRef.current, pos.x, pos.y)
    if (hit && onSelectIcao) onSelectIcao(hit.icao)
  }, [onSelectIcao])

  // Tooltip renderer (shared between both canvases)
  const renderTooltip = (hRef, canvasEl, canvasW, canvasH) => {
    if (!hoveredAc || !hRef.current || !canvasEl) return null
    const rect   = canvasEl.getBoundingClientRect()
    const scaleX = rect.width  / canvasW
    const scaleY = rect.height / canvasH
    const elev   = elevDeg(hoveredAc.altitude, hoveredAc.range_nm)
    return (
      <div
        className={styles.tooltip}
        style={{
          left: `${hRef.current.x * scaleX + 14}px`,
          top:  `${hRef.current.y * scaleY - 8}px`,
        }}
      >
        <div className={styles.tooltipIcao}>{hoveredAc.icao}</div>
        {hoveredAc.callsign   && <div>{hoveredAc.callsign}</div>}
        {hoveredAc.altitude  != null && <div>{hoveredAc.altitude.toLocaleString()} ft</div>}
        {hoveredAc.range_nm  != null && <div>{hoveredAc.range_nm} nm</div>}
        {elev                != null && <div>Elev {elev.toFixed(1)}°</div>}
        {hoveredAc.type_desc && <div className={styles.tooltipMeta}>{hoveredAc.type_desc}</div>}
        <div className={styles.tooltipHint}>click to open</div>
      </div>
    )
  }

  const visible = aircraft.filter(
    ac => ac.bearing_deg != null && ac.range_nm != null && ac.altitude != null
  ).length

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>Sky View</h2>
        <p className={styles.subtitle}>
          {visible > 0 ? `${visible} aircraft with position` : 'No aircraft with position data'}
        </p>
      </div>

      {/* Filter bar */}
      <div className={styles.filterBar}>
        <div className={styles.filterRow}>
          {COLOR_MODES.map(m => (
            <button
              key={m.value}
              className={colorMode === m.value ? styles.btnActive : styles.btn}
              onClick={() => setColorMode(m.value)}
            >{m.label}</button>
          ))}
        </div>
        <div className={styles.filterRow}>
          {FILTERS.map(f => (
            <button
              key={f.value}
              className={filter === f.value ? styles.btnActive : styles.btn}
              onClick={() => setFilter(f.value)}
            >{f.label}</button>
          ))}
          <select className={styles.select} value={typeGroup} onChange={e => { setTypeGroup(e.target.value); setTypeCode('all') }}>
            <option value="all">All types</option>
            {TYPE_GROUPS.map(g => <option key={g.value} value={g.value}>{g.label}</option>)}
          </select>
          <select className={styles.select} value={typeCode} onChange={e => setTypeCode(e.target.value)}>
            <option value="all">All codes</option>
            {visibleTypeCodes.map(tc => <option key={tc} value={tc}>{tc}</option>)}
          </select>
        </div>
      </div>

      {/* Polar canvas */}
      <div className={styles.canvasWrap}>
        <canvas
          ref={canvasRef}
          width={CANVAS_SIZE}
          height={CANVAS_SIZE}
          className={styles.canvas}
          onMouseMove={handleMouseMove}
          onMouseLeave={handleMouseLeave}
          onClick={handleClick}
        />
        {renderTooltip(hoverRef, canvasRef.current, CANVAS_SIZE, CANVAS_SIZE)}
      </div>

      {/* Horizon canvas */}
      <div className={styles.horizonSection}>
        <div className={styles.horizonHeader}>
          <span className={styles.subtitle}>
            Azimuth (compass) × Elevation — N at both edges, S at centre
            {terrainRanges && <> · terrain (25/50/75/100 nm)</>}
          </span>
          <div className={styles.filterRow}>
            {SCALE_OPTIONS.map(s => (
              <button
                key={s.value}
                className={horizonScale === s.value ? styles.btnActive : styles.btnSmall}
                onClick={() => setHorizonScale(s.value)}
              >{s.label}</button>
            ))}
          </div>
          <button
            className={styles.btnSmall}
            onClick={() => setMaxElev(v => v === 30 ? 90 : 30)}
          >{maxElev === 30 ? 'Expand to 90°' : 'Zoom to 30°'}</button>
          <button
            className={histMode ? styles.btnActive : styles.btnSmall}
            onClick={() => setHistMode(v => !v)}
          >{histMode ? 'History on' : 'History'}</button>
          {histMode && (
            <select className={styles.select} value={histHours} onChange={e => setHistHours(Number(e.target.value))}>
              {[6, 12, 24, 48, 72].map(h => <option key={h} value={h}>{h}h</option>)}
            </select>
          )}
        </div>
        <div className={styles.canvasWrap}>
          <canvas
            ref={horizonCanvasRef}
            width={HORIZON_W}
            height={HORIZON_H}
            className={styles.horizonCanvas}
            onMouseMove={handleHorizonMouseMove}
            onMouseLeave={handleHorizonMouseLeave}
            onClick={handleHorizonClick}
          />
          {renderTooltip(horizonHoverRef, horizonCanvasRef.current, HORIZON_W, HORIZON_H)}
        </div>
      </div>

      {/* Legend — dynamic by colour mode */}
      {colorMode === 'classification' && (
        <div className={styles.legend}>
          <span className={styles.legendItem} style={{ color: '#f85149' }}>● Emergency</span>
          <span className={styles.legendItem} style={{ color: '#bc8cff' }}>● Military</span>
          <span className={styles.legendItem} style={{ color: '#388bfd' }}>● MLAT</span>
          <span className={styles.legendItem} style={{ color: '#d29922' }}>● Interesting</span>
          <span className={styles.legendItem} style={{ color: '#3fb950' }}>● ADS-B</span>
        </div>
      )}
      {colorMode === 'type_group' && (
        <div className={styles.legend}>
          {TYPE_GROUPS.map(g => (
            <span key={g.value} className={styles.legendItem} style={{ color: g.color }}>● {g.label}</span>
          ))}
          <span className={styles.legendItem} style={{ color: TYPE_GROUP_OTHER_COLOR }}>● Other</span>
        </div>
      )}
      {colorMode === 'operator' && topOperators.length > 0 && (
        <div className={styles.legend}>
          {topOperators.map(op => (
            <span key={op.name} className={styles.legendItem} style={{ color: op.color }}>● {op.name}</span>
          ))}
          <span className={styles.legendItem} style={{ color: TYPE_GROUP_OTHER_COLOR }}>● Other</span>
        </div>
      )}
    </div>
  )
}
