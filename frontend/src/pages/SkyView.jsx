import { useEffect, useRef, useState, useCallback } from 'react'
import styles from './SkyView.module.css'

const FEET_PER_NM      = 6076.115
const TRACK_POLL_MS    = 5000
const CANVAS_SIZE      = 600
const MARGIN           = 44    // pixels around the polar disc for labels
const HOVER_RADIUS_PX  = 15   // max distance to register a hover/click
// Break a trail segment if consecutive points are more than this many seconds apart
// (catches gaps from aircraft going off-screen then returning, or CPR glitches)
const MAX_TRAIL_GAP_S  = 20

const EMERGENCY_SQUAWKS = new Set(['7700', '7600', '7500'])

// Elevation angle in degrees above horizon. Returns null if inputs missing.
function elevDeg(altitude_ft, range_nm) {
  if (altitude_ft == null || range_nm == null || range_nm <= 0) return null
  return Math.atan2(altitude_ft, range_nm * FEET_PER_NM) * (180 / Math.PI)
}

// Colours match the live table badge/row scheme exactly.
// Priority: emergency > military > MLAT > interesting > standard ADS-B
function acColor(ac) {
  if (ac.squawk && EMERGENCY_SQUAWKS.has(ac.squawk)) return '#f85149'  // red
  if (ac.military)    return '#bc8cff'  // purple  (milBadge)
  if (ac.mlat)        return '#388bfd'  // blue    (mlatBadge)
  if (ac.interesting) return '#d29922'  // orange  (intBadge)
  return '#3fb950'                      // soft green — standard ADS-B
}

export default function SkyView({ snapshot, onSelectIcao }) {
  const canvasRef   = useRef(null)
  const tracksRef   = useRef({})
  const hoverRef    = useRef(null)       // { x, y } in canvas coordinate space
  const hitboxesRef = useRef([])
  const [hoveredAc, setHoveredAc] = useState(null)

  // Poll track history every 5 s
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

  // Draw — re-runs on every WebSocket snapshot (~1 s)
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')

    const cx = CANVAS_SIZE / 2
    const cy = CANVAS_SIZE / 2
    const R  = CANVAS_SIZE / 2 - MARGIN

    // ── Background ──────────────────────────────────────────────────
    ctx.fillStyle = '#0b0c10'
    ctx.fillRect(0, 0, CANVAS_SIZE, CANVAS_SIZE)

    // ── Elevation rings ──────────────────────────────────────────────
    // 0° ring = horizon (edge), labels show elevation above horizon
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

    // ── Azimuth spokes & compass labels ─────────────────────────────
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

    // ── Shared mapping: (bearing, range, altitude) → canvas (x, y) ──
    const toXY = (bearing_deg, range_nm, altitude_ft) => {
      const elev = elevDeg(altitude_ft, range_nm)
      if (elev === null) return null
      const rad   = (bearing_deg - 90) * Math.PI / 180
      const rFrac = (90 - Math.max(0, elev)) / 90   // clamp to horizon
      return {
        x: cx + Math.cos(rad) * R * rFrac,
        y: cy + Math.sin(rad) * R * rFrac,
      }
    }

    // ── Trails ───────────────────────────────────────────────────────
    // Break the path whenever the time gap between consecutive points exceeds
    // MAX_TRAIL_GAP_S — prevents long diagonal lines when an aircraft
    // disappears then reappears (or from CPR position glitches).
    const tracks = tracksRef.current
    Object.values(tracks).forEach(points => {
      if (points.length < 2) return
      const color = acColor(points[points.length - 1])
      ctx.beginPath()
      let penDown = false
      for (let i = 0; i < points.length; i++) {
        const p   = points[i]
        const pos = toXY(p.bearing_deg, p.range_nm, p.altitude_ft)
        // Break pen on missing position or time gap to previous point
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

    // ── Live dots ────────────────────────────────────────────────────
    const hitboxes = []
    const aircraft = snapshot?.aircraft ?? []

    aircraft.forEach(ac => {
      if (ac.bearing_deg == null || ac.range_nm == null) return
      const pos = toXY(ac.bearing_deg, ac.range_nm, ac.altitude)
      if (!pos) return

      const color = acColor(ac)
      const dotR  = 4

      // Hover ring
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
  }, [snapshot])

  // ── Pointer helpers ──────────────────────────────────────────────────
  const canvasCoords = (e) => {
    const canvas = canvasRef.current
    if (!canvas) return null
    const rect   = canvas.getBoundingClientRect()
    const scaleX = CANVAS_SIZE / rect.width
    const scaleY = CANVAS_SIZE / rect.height
    return {
      x: (e.clientX - rect.left) * scaleX,
      y: (e.clientY - rect.top)  * scaleY,
    }
  }

  const findNearest = (mx, my) => {
    let best = null, bestDist = HOVER_RADIUS_PX
    for (const h of hitboxesRef.current) {
      const d = Math.hypot(h.x - mx, h.y - my)
      if (d < bestDist) { best = h; bestDist = d }
    }
    return best
  }

  const handleMouseMove = useCallback((e) => {
    const pos = canvasCoords(e)
    if (!pos) return
    hoverRef.current = pos
    setHoveredAc(findNearest(pos.x, pos.y)?.ac ?? null)
  }, [])

  const handleMouseLeave = useCallback(() => {
    hoverRef.current = null
    setHoveredAc(null)
  }, [])

  const handleClick = useCallback((e) => {
    const pos = canvasCoords(e)
    if (!pos) return
    const hit = findNearest(pos.x, pos.y)
    if (hit && onSelectIcao) onSelectIcao(hit.icao)
  }, [onSelectIcao])

  // Aircraft count with a valid sky position
  const aircraft = snapshot?.aircraft ?? []
  const visible  = aircraft.filter(
    ac => ac.bearing_deg != null && ac.range_nm != null && ac.altitude != null
  ).length

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h2 className={styles.title}>Sky View</h2>
        <p className={styles.subtitle}>
          Azimuth (compass) × Elevation — centre = directly overhead, edge = horizon
          {visible > 0 && <> · {visible} aircraft with position</>}
        </p>
      </div>

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

        {hoveredAc && hoverRef.current && (() => {
          const canvas = canvasRef.current
          const rect   = canvas?.getBoundingClientRect()
          const scaleX = rect ? rect.width  / CANVAS_SIZE : 1
          const scaleY = rect ? rect.height / CANVAS_SIZE : 1
          const elev   = elevDeg(hoveredAc.altitude, hoveredAc.range_nm)
          return (
            <div
              className={styles.tooltip}
              style={{
                left: `${hoverRef.current.x * scaleX + 14}px`,
                top:  `${hoverRef.current.y * scaleY - 8}px`,
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
        })()}
      </div>

      <div className={styles.legend}>
        <span className={styles.legendItem} style={{ color: '#f85149' }}>● Emergency</span>
        <span className={styles.legendItem} style={{ color: '#bc8cff' }}>● Military</span>
        <span className={styles.legendItem} style={{ color: '#388bfd' }}>● MLAT</span>
        <span className={styles.legendItem} style={{ color: '#d29922' }}>● Interesting</span>
        <span className={styles.legendItem} style={{ color: '#3fb950' }}>● ADS-B</span>
      </div>
    </div>
  )
}
