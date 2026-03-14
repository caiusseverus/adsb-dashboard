import { useEffect, useRef, useState, useCallback } from 'react'
import * as THREE from 'three'
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js'
import styles from './CoveragePage.module.css'
import { NAMED_PALETTE, TYPE_GROUPS, TYPE_GROUP_OTHER_COLOR, getTypeGroup } from '../utils/typeGroups'

const VERT_EXAG     = 8
const FEET_PER_NM   = 6076.115
const ALT_SCALE_FT  = 45000   // fixed colour scale ceiling — not data-driven

// Maximum trail points kept per aircraft (~10 min at 1 Hz)
const MAX_TRAIL_PTS = 600
// Break a trail segment on time gap (seconds) or position jump thresholds
const MAX_TRAIL_GAP_S   = 20
const MAX_BEARING_JUMP  = 45   // degrees — impossible in 1 s for any real aircraft
const MAX_RANGE_JUMP_NM = 50   // nm — likewise
const MAX_ALT_RATE_FPM  = 6000
const MAX_ALT_JUMP_FT_WHEN_STATIC_XY = 2000
const MIN_XY_MOVE_FOR_ALT_JUMP_NM = 0.05
const MAX_TRAIL_IMPLIED_SPEED_KT = 900
const MAX_SOURCE_SWITCH_JUMP_NM = 3

function bearingDeltaDeg(a, b) {
  const d = Math.abs((a ?? 0) - (b ?? 0)) % 360
  return d > 180 ? 360 - d : d
}

function haversineNm(lat1, lon1, lat2, lon2) {
  if ([lat1, lon1, lat2, lon2].some(v => v == null)) return null
  const R_NM = 3440.065
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2
  return R_NM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

function isTrailSegmentValid(a, b) {
  const dt = b.ts - a.ts
  if (dt <= 0 || dt > MAX_TRAIL_GAP_S) return false
  if (bearingDeltaDeg(a.bearing, b.bearing) > MAX_BEARING_JUMP) return false
  const dRange = Math.abs((b.range ?? 0) - (a.range ?? 0))
  if (dRange > MAX_RANGE_JUMP_NM) return false

  const distNm = haversineNm(a.lat, a.lon, b.lat, b.lon)
  if (distNm != null) {
    const impliedSpeedKt = (distNm / dt) * 3600
    if (impliedSpeedKt > MAX_TRAIL_IMPLIED_SPEED_KT) return false
    if (!!a.mlat !== !!b.mlat && distNm > MAX_SOURCE_SWITCH_JUMP_NM) return false
  }

  const altA = a.alt ?? 0
  const altB = b.alt ?? 0
  const dAlt = Math.abs(altB - altA)
  const rateFpm = (dAlt / dt) * 60
  if (rateFpm > MAX_ALT_RATE_FPM) return false

  const xyMoveNm = distNm ?? dRange
  if (xyMoveNm < MIN_XY_MOVE_FOR_ALT_JUMP_NM && dAlt > MAX_ALT_JUMP_FT_WHEN_STATIC_XY) return false
  return true
}

// Tag colours — same palette as AircraftTable badges
const C_MILITARY    = new THREE.Color(0xbc8cff)
const C_INTERESTING = new THREE.Color(0xd29922)
const C_STANDARD    = new THREE.Color(0x3fb950)

// Trail colours — match SkyView / badge conventions
const C_TRAIL = {
  acas:        new THREE.Color(0xf85149),
  military:    new THREE.Color(0xbc8cff),
  mlat:        new THREE.Color(0x388bfd),
  interesting: new THREE.Color(0xd29922),
  standard:    new THREE.Color(0x3fb950),
}

// Operator palette — 10 distinct colours + dim grey for "other"
const C_OP_PALETTE   = NAMED_PALETTE.map(h => new THREE.Color(h))
const C_OP_OTHER     = new THREE.Color(0x484f58)

// Type group palette — one colour per group (0–7), plus other
const C_TG_PALETTE   = TYPE_GROUPS.map(g => new THREE.Color(g.color))
const C_TG_OTHER     = new THREE.Color(TYPE_GROUP_OTHER_COLOR)

// Altitude gradient stops: green → yellow → blue → purple
const ALT_STOPS = [
  [0.00, new THREE.Color(0x3fb950)],
  [0.33, new THREE.Color(0xd29922)],
  [0.67, new THREE.Color(0x388bfd)],
  [1.00, new THREE.Color(0xbc8cff)],
]

function altColor(alt_ft) {
  const t = Math.min(1, Math.max(0, alt_ft / ALT_SCALE_FT))
  for (let i = 1; i < ALT_STOPS.length; i++) {
    const [t0, c0] = ALT_STOPS[i - 1]
    const [t1, c1] = ALT_STOPS[i]
    if (t <= t1) return c0.clone().lerp(c1, (t - t0) / (t1 - t0))
  }
  return ALT_STOPS[ALT_STOPS.length - 1][1].clone()
}

// [bearing_deg, range_nm, alt_ft] → Three.js world coords
function toWorld(bearing_deg, range_nm, alt_ft) {
  const rad = bearing_deg * Math.PI / 180
  return [
    range_nm * Math.sin(rad),
    (alt_ft / FEET_PER_NM) * VERT_EXAG,
    -range_nm * Math.cos(rad),   // negate: North (0°) → −Z, into scene
  ]
}

// Build a Points object from (already-filtered) raw point data + colour mode
function buildPoints(points, colorMode) {
  const n = points.length
  const positions = new Float32Array(n * 3)
  const colors    = new Float32Array(n * 3)

  for (let i = 0; i < n; i++) {
    const [bearing, range, alt, military, interesting, op_idx, tg_idx] = points[i]
    const [x, y, z] = toWorld(bearing, range, alt)
    positions[i * 3]     = x
    positions[i * 3 + 1] = y
    positions[i * 3 + 2] = z

    let col
    if (colorMode === 'altitude') {
      col = altColor(alt)
    } else if (colorMode === 'operator') {
      col = (op_idx != null && op_idx < 10) ? C_OP_PALETTE[op_idx] : C_OP_OTHER
    } else if (colorMode === 'type_group') {
      col = (tg_idx != null && tg_idx < 8) ? C_TG_PALETTE[tg_idx] : C_TG_OTHER
    } else {
      col = military    ? C_MILITARY
          : interesting ? C_INTERESTING
          : C_STANDARD
    }

    colors[i * 3]     = col.r
    colors[i * 3 + 1] = col.g
    colors[i * 3 + 2] = col.b
  }

  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  geo.setAttribute('color',    new THREE.BufferAttribute(colors,    3))

  return new THREE.Points(geo, new THREE.PointsMaterial({
    size:            1.5,
    vertexColors:    true,
    sizeAttenuation: false,
    transparent:     true,
    opacity:         0.8,
  }))
}

// Colour for a live trail/dot based on colorMode.
// Altitude mode is handled per-vertex in buildTrails; this covers the rest.
function liveAcColor(pt, colorMode, operators) {
  if (colorMode === 'type_group') {
    const g = getTypeGroup(pt.type_code, pt.type_category)
    return g ? new THREE.Color(g.color) : C_TG_OTHER
  }
  if (colorMode === 'operator') {
    const idx = operators ? operators.indexOf(pt.operator) : -1
    return idx >= 0 ? C_OP_PALETTE[idx] : C_OP_OTHER
  }
  // tag / default (also used as fallback for altitude mode at the dot level)
  return pt.acas        ? C_TRAIL.acas
       : pt.military    ? C_TRAIL.military
       : pt.mlat        ? C_TRAIL.mlat
       : pt.interesting ? C_TRAIL.interesting
       : C_TRAIL.standard
}

// Build merged LineSegments for all live aircraft trails
function buildTrails(trails, colorMode, operators) {
  let totalSegs = 0
  for (const pts of Object.values(trails)) {
    if (pts.length < 2) continue
    for (let i = 0; i < pts.length - 1; i++) {
      if (isTrailSegmentValid(pts[i], pts[i + 1])) totalSegs++
    }
  }
  if (totalSegs === 0) return null

  const positions = new Float32Array(totalSegs * 6)   // 2 verts × 3 coords
  const colors    = new Float32Array(totalSegs * 6)   // 2 verts × 3 colours

  let idx = 0
  for (const pts of Object.values(trails)) {
    if (pts.length < 2) continue
    // For non-altitude modes, one colour per aircraft (from last point)
    const acCol = colorMode !== 'altitude' ? liveAcColor(pts[pts.length - 1], colorMode, operators) : null

    for (let i = 0; i < pts.length - 1; i++) {
      const a = pts[i], b = pts[i + 1]
      // Skip segment on time gap or impossible position jump (CPR glitch guard)
      if (!isTrailSegmentValid(a, b)) continue
      const [x0, y0, z0] = toWorld(a.bearing, a.range, a.alt ?? 0)
      const [x1, y1, z1] = toWorld(b.bearing, b.range, b.alt ?? 0)
      // Altitude mode: colour each vertex by its own altitude (gradient along trail)
      const colA = colorMode === 'altitude' ? altColor(a.alt ?? 0) : acCol
      const colB = colorMode === 'altitude' ? altColor(b.alt ?? 0) : acCol
      positions[idx * 6]     = x0; positions[idx * 6 + 1] = y0; positions[idx * 6 + 2] = z0
      positions[idx * 6 + 3] = x1; positions[idx * 6 + 4] = y1; positions[idx * 6 + 5] = z1
      colors[idx * 6]     = colA.r; colors[idx * 6 + 1] = colA.g; colors[idx * 6 + 2] = colA.b
      colors[idx * 6 + 3] = colB.r; colors[idx * 6 + 4] = colB.g; colors[idx * 6 + 5] = colB.b
      idx++
    }
  }

  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  geo.setAttribute('color',    new THREE.BufferAttribute(colors,    3))
  return new THREE.LineSegments(geo, new THREE.LineBasicMaterial({
    vertexColors: true,
    transparent:  true,
    opacity:      0.75,
  }))
}

// Build a Points object with one bright dot at each aircraft's current position
function buildLiveDots(trails, colorMode, operators) {
  const entries = Object.values(trails).filter(pts => pts.length > 0)
  if (entries.length === 0) return null

  const n = entries.length
  const positions = new Float32Array(n * 3)
  const colors    = new Float32Array(n * 3)

  entries.forEach((pts, i) => {
    const last = pts[pts.length - 1]
    const [x, y, z] = toWorld(last.bearing, last.range, last.alt ?? 0)
    positions[i * 3]     = x
    positions[i * 3 + 1] = y
    positions[i * 3 + 2] = z
    const col = colorMode === 'altitude' ? altColor(last.alt ?? 0)
              : liveAcColor(last, colorMode, operators)
    colors[i * 3]     = col.r
    colors[i * 3 + 1] = col.g
    colors[i * 3 + 2] = col.b
  })

  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  geo.setAttribute('color',    new THREE.BufferAttribute(colors,    3))
  return new THREE.Points(geo, new THREE.PointsMaterial({
    size:            5,
    vertexColors:    true,
    sizeAttenuation: false,
  }))
}

// Ground-plane range ring — brighter than before
function makeRing(radius) {
  const pts = []
  for (let i = 0; i <= 128; i++) {
    const a = (i / 128) * Math.PI * 2
    pts.push(new THREE.Vector3(Math.sin(a) * radius, 0, Math.cos(a) * radius))
  }
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color: 0x30363d }),
  )
}

// Cardinal spoke
function makeSpoke(bearing_deg, length) {
  const rad = bearing_deg * Math.PI / 180
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(0, 0, 0),
      new THREE.Vector3(Math.sin(rad) * length, 0, -Math.cos(rad) * length),
    ]),
    new THREE.LineBasicMaterial({ color: 0x30363d }),
  )
}

// Canvas-texture sprite for compass labels
function makeLabel(text, x, z) {
  const c = document.createElement('canvas')
  c.width = 128; c.height = 64
  const ctx = c.getContext('2d')
  ctx.font = 'bold 48px sans-serif'
  ctx.fillStyle = '#c9d1d9'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(text, 64, 32)
  const sprite = new THREE.Sprite(
    new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(c), transparent: true }),
  )
  sprite.position.set(x, 1, z)
  sprite.scale.set(30, 15, 1)
  return sprite
}

// Build ground-plane LineSegments for coastline + country borders
function buildCoastline(segments) {
  if (!segments || segments.length === 0) return null
  const positions = new Float32Array(segments.length * 6)
  let idx = 0
  for (const [b1, r1, b2, r2] of segments) {
    const [x0, , z0] = toWorld(b1, r1, 0)
    const [x1, , z1] = toWorld(b2, r2, 0)
    positions[idx++] = x0; positions[idx++] = 0; positions[idx++] = z0
    positions[idx++] = x1; positions[idx++] = 0; positions[idx++] = z1
  }
  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  return new THREE.LineSegments(geo, new THREE.LineBasicMaterial({
    color: 0x445566,
    transparent: true,
    opacity: 0.75,
  }))
}

// Strip redundant suffixes so labels fit compactly in the 3D view
function shortAirportName(name) {
  return name
    .replace(/\s+international\s+airport$/i, '')
    .replace(/\s+airport$/i, '')
    .replace(/\s+intl\.?$/i, '')
    .trim()
    .slice(0, 20)
}

// Build a THREE.Group containing amber dots + billboard name labels for airports
function buildAirports(airports) {
  if (!airports || airports.length === 0) return null
  const group = new THREE.Group()

  // Dots — single Points object for all airports
  const positions = new Float32Array(airports.length * 3)
  airports.forEach((ap, i) => {
    const [x, , z] = toWorld(ap.bearing, ap.range_nm, 0)
    positions[i * 3]     = x
    positions[i * 3 + 1] = 0.5  // slightly above ground to avoid z-fighting with coastline
    positions[i * 3 + 2] = z
  })
  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  group.add(new THREE.Points(geo, new THREE.PointsMaterial({
    color: 0xf0a000,
    size: 7,
    sizeAttenuation: false,
  })))

  // Labels — one sprite per airport
  airports.forEach(ap => {
    const label = shortAirportName(ap.name)
    const canvas = document.createElement('canvas')
    canvas.width = 256; canvas.height = 40
    const ctx = canvas.getContext('2d')
    ctx.font = 'bold 22px sans-serif'
    ctx.fillStyle = '#f0a000'
    ctx.textAlign = 'left'
    ctx.textBaseline = 'middle'
    ctx.fillText(label, 4, 20)
    const sprite = new THREE.Sprite(
      new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(canvas), transparent: true }),
    )
    const [x, , z] = toWorld(ap.bearing, ap.range_nm, 0)
    sprite.position.set(x + 2, 3, z)   // offset slightly right of dot, float above ground
    sprite.scale.set(16, 3, 1)
    group.add(sprite)
  })

  return group
}

// ── Timelapse helpers ────────────────────────────────────────────────────────

const TL_MAX_AC          = 500              // pre-allocated aircraft slots
const TRAIL_DURATION_S   = 480             // total trail length: 8 minutes of real time
const TRAIL_INTERVAL_S   = 5              // one interpolated dot every 5 real seconds
const TRAIL_DOTS_PER_AC  = TRAIL_DURATION_S / TRAIL_INTERVAL_S   // = 96
const TL_MAX_TRAIL_DOTS  = TL_MAX_AC * TRAIL_DOTS_PER_AC
const TL_SPEEDS          = [60, 120, 300, 600]
const TL_GAP_S           = 180             // gap > 3 min → stop trail, hide aircraft
// Background colour to fade trail dots toward (matches scene background #0b0c10)
const TL_BG_R = 11 / 255, TL_BG_G = 12 / 255, TL_BG_B = 16 / 255

function lerpBearing(b0, b1, alpha) {
  let d = b1 - b0
  if (d >  180) d -= 360
  if (d < -180) d += 360
  return (b0 + alpha * d + 360) % 360
}

// Binary search: index i where points[i][0] <= dt < points[i+1][0]
// Returns -1 if before first point, points.length-2 if at/after last
function findSegIdx(points, dt) {
  if (dt < points[0][0])                      return -1
  if (dt >= points[points.length - 1][0])     return points.length - 2
  let lo = 0, hi = points.length - 2
  while (lo < hi) {
    const mid = (lo + hi + 1) >> 1
    if (points[mid][0] <= dt) lo = mid; else hi = mid - 1
  }
  return lo
}

function tlTrackColor(track, colorMode) {
  if (colorMode === 'type_group') {
    return (track.tg_idx != null && track.tg_idx < 8) ? C_TG_PALETTE[track.tg_idx] : C_TG_OTHER
  }
  return track.military    ? C_TRAIL.military
       : track.interesting ? C_TRAIL.interesting
       : C_TRAIL.standard
}

function tlFormatTime(startTs, dt) {
  const d = new Date((startTs + dt) * 1000)
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mm = String(d.getUTCMinutes()).padStart(2, '0')
  return `${hh}:${mm} UTC`
}

function tlDefaultDate() {
  const d = new Date()
  d.setDate(d.getDate() - 1)
  return d.toISOString().slice(0, 10)
}

const DAY_OPTIONS      = [{ label: '24h', value: 1 }, { label: '7d', value: 7 }, { label: '30d', value: 30 }, { label: '90d', value: 90 }]
const MAX_POINT_OPTIONS = [50000, 100000, 200000, 500000]
const DEFAULT_MAX_POINTS = 100000

export default function CoveragePage({ aircraft = [] }) {
  const mountRef    = useRef(null)
  const sceneRef    = useRef(null)   // { scene, camera, renderer, controls, pointsObj, trailsObj, liveDotsObj }
  const dataRef     = useRef([])     // raw fetched points (unfiltered)
  const pendingRef  = useRef(null)   // points waiting to render after 'rendering' phase
  const trailsRef   = useRef({})     // live trail buffer: { icao: [{bearing,range,alt,...}] }
  const compassRef  = useRef(null)   // ref to inner compass ring div (rotated via JS, not React state)

  const [days,         setDays]         = useState(30)
  const [maxPoints,    setMaxPoints]    = useState(DEFAULT_MAX_POINTS)
  const [colorMode,    setColorMode]    = useState('type_group')
  const [typeFilter,   setTypeFilter]   = useState('all')
  const [showMode,     setShowMode]     = useState('both')  // 'both' | 'live' | 'history'
  const [loadingPhase, setLoadingPhase] = useState(null)  // null | 'fetching' | 'rendering'
  const [error,        setError]        = useState(null)
  const [fetchedN,     setFetchedN]     = useState(0)
  const [shownN,       setShownN]       = useState(0)
  const [operators,    setOperators]    = useState([])
  const [showCoastline, setShowCoastline] = useState(true)
  const showCoastlineRef = useRef(true)  // avoids stale closure in fetch callback
  const [showAirportsLarge,  setShowAirportsLarge]  = useState(true)
  const [showAirportsMedium, setShowAirportsMedium] = useState(false)
  const showAirportsLargeRef  = useRef(true)
  const showAirportsMediumRef = useRef(false)

  // ── Timelapse state ──────────────────────────────────────────────────
  const [tlActive,  setTlActive]  = useState(false)
  const [tlLoading, setTlLoading] = useState(false)
  const [tlError,   setTlError]   = useState(null)
  const [tlPlaying, setTlPlaying] = useState(false)
  const [tlSpeed,   setTlSpeed]   = useState(120)
  const [tlDate,    setTlDate]    = useState(tlDefaultDate)
  const [tlIsLive,  setTlIsLive]  = useState(true)   // true = hires last-24h; false = historical date

  // Refs — avoid stale closures in the RAF loop
  const tlDataRef      = useRef(null)   // loaded track data
  const tlCurrentDtRef = useRef(0)      // current playback offset (seconds)
  const tlPlayingRef   = useRef(false)
  const tlSpeedRef     = useRef(120)
  const tlRafRef       = useRef(null)
  const tlLastRafRef   = useRef(null)
  const tlGeosRef      = useRef(null)   // { dotGeo, trailGeo, dotsObj, trailsObj }
  const tlScrubberRef  = useRef(null)   // range <input> DOM node
  const tlTimeLabelRef = useRef(null)   // time display <span> DOM node
  const tlStartDtRef   = useRef(0)      // dt of first data point (scrubber start position)
  const tlIsLiveRef    = useRef(true)   // mirrors tlIsLive for use in RAF/timeout closures
  const colorModeRef   = useRef('type_group')
  const tlActiveRef    = useRef(false)  // mirrors tlActive for use in non-reactive closures

  // ── Initialise Three.js (once) ──────────────────────────────────────
  useEffect(() => {
    const container = mountRef.current
    if (!container) return
    const W = container.clientWidth, H = container.clientHeight

    const scene    = new THREE.Scene()
    scene.background = new THREE.Color(0x0b0c10)

    const camera = new THREE.PerspectiveCamera(50, W / H, 0.1, 5000)
    camera.position.set(0, 90, 260)

    const renderer = new THREE.WebGLRenderer({ antialias: true })
    renderer.setPixelRatio(window.devicePixelRatio)
    renderer.setSize(W, H)
    container.appendChild(renderer.domElement)

    const controls = new OrbitControls(camera, renderer.domElement)
    controls.target.set(0, 25, 0)
    controls.enableDamping = true
    controls.dampingFactor = 0.08
    controls.minDistance   = 10
    controls.maxDistance   = 800
    controls.update()

    for (let r = 50; r <= 250; r += 50) scene.add(makeRing(r))
    for (const b of [0, 90, 180, 270]) scene.add(makeSpoke(b, 270))

    const LR = 278
    scene.add(makeLabel('N',    0, -LR))
    scene.add(makeLabel('S',    0,  LR))
    scene.add(makeLabel('E',  LR,    0))
    scene.add(makeLabel('W', -LR,    0))

    const altTopY = (ALT_SCALE_FT / FEET_PER_NM) * VERT_EXAG
    scene.add(new THREE.Line(
      new THREE.BufferGeometry().setFromPoints([
        new THREE.Vector3(0, 0, 0),
        new THREE.Vector3(0, altTopY + 5, 0),
      ]),
      new THREE.LineBasicMaterial({ color: 0x484f58 }),
    ))

    // Update the 2D compass rose to match the camera's horizontal azimuth.
    // Direct DOM manipulation (not React state) to avoid a re-render every frame.
    const updateCompass = () => {
      if (!compassRef.current) return
      const angle = Math.atan2(camera.position.x, camera.position.z) * (180 / Math.PI)
      compassRef.current.style.transform = `rotate(${angle}deg)`
    }
    controls.addEventListener('change', updateCompass)
    updateCompass()  // set correct initial rotation

    let rafId
    const animate = () => { rafId = requestAnimationFrame(animate); controls.update(); renderer.render(scene, camera) }
    animate()

    const onResize = () => {
      const w = container.clientWidth, h = container.clientHeight
      camera.aspect = w / h
      camera.updateProjectionMatrix()
      renderer.setSize(w, h)
    }
    window.addEventListener('resize', onResize)

    sceneRef.current = { scene, camera, renderer, controls, pointsObj: null, trailsObj: null, liveDotsObj: null, coastlineObj: null, airportsLargeObj: null, airportsMediumObj: null }

    return () => {
      cancelAnimationFrame(rafId)
      window.removeEventListener('resize', onResize)
      controls.removeEventListener('change', updateCompass)
      renderer.dispose()
      if (container.contains(renderer.domElement)) container.removeChild(renderer.domElement)
      sceneRef.current = null
    }
  }, [])

  // ── Fetch and render coastline once ─────────────────────────────────
  useEffect(() => {
    fetch('/api/coverage/coastline')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        const ref = sceneRef.current
        if (!ref || !data?.segments?.length) return
        const obj = buildCoastline(data.segments)
        if (!obj) return
        obj.visible = showCoastlineRef.current
        ref.scene.add(obj)
        ref.coastlineObj = obj
      })
      .catch(() => {})  // coastline is optional — degrade silently
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Fetch and render airports (large + medium fetched separately) ────
  useEffect(() => {
    function fetchLayer(types, refKey, visibleRef) {
      fetch(`/api/coverage/airports?types=${types}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          const ref = sceneRef.current
          if (!ref || !data?.airports?.length) return
          const obj = buildAirports(data.airports)
          if (!obj) return
          obj.visible = visibleRef.current
          ref.scene.add(obj)
          ref[refKey] = obj
        })
        .catch(() => {})
    }
    fetchLayer('large_airport',  'airportsLargeObj',  showAirportsLargeRef)
    fetchLayer('medium_airport', 'airportsMediumObj', showAirportsMediumRef)
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Keep colorModeRef in sync for the timelapse RAF loop ────────────
  useEffect(() => { colorModeRef.current = colorMode }, [colorMode])

  // ── Timelapse: render one frame into pre-allocated geometries ────────
  const renderTlFrame = useCallback((dt) => {
    const geos = tlGeosRef.current
    const data = tlDataRef.current
    if (!geos || !data) return

    const { dotGeo, trailGeo } = geos
    const dp = dotGeo.attributes.position.array
    const dc = dotGeo.attributes.color.array
    const tp = trailGeo.attributes.position.array
    const tc = trailGeo.attributes.color.array
    const mode = colorModeRef.current

    let di = 0   // dot vertex index
    let ti = 0   // trail segment index

    for (const track of data.tracks) {
      const pts    = track.points
      const lastPt = pts[pts.length - 1]
      const si     = findSegIdx(pts, dt)
      if (si < 0) continue                               // before this track starts
      if (dt > lastPt[0] + TL_GAP_S) continue           // past end of track — aircraft gone

      const p0 = pts[si], p1 = pts[Math.min(si + 1, pts.length - 1)]
      if (p1[0] - p0[0] > TL_GAP_S) continue            // gap in data — don't show aircraft
      // Clamp alpha to [0,1]: prevents extrapolation past the final sample
      const alpha   = p0 === p1 ? 0 : Math.min(1, (dt - p0[0]) / (p1[0] - p0[0]))
      const bearing = lerpBearing(p0[1], p1[1], alpha)
      const range   = p0[2] + alpha * (p1[2] - p0[2])
      const alt     = p0[3] + alpha * (p1[3] - p0[3])

      if (di >= TL_MAX_AC) break
      const [x, y, z] = toWorld(bearing, range, alt)
      dp[di * 3] = x; dp[di * 3 + 1] = y; dp[di * 3 + 2] = z
      const col = mode === 'altitude' ? altColor(alt) : tlTrackColor(track, mode)
      dc[di * 3] = col.r; dc[di * 3 + 1] = col.g; dc[di * 3 + 2] = col.b
      di++

      // Trail as LineSegments: lead → dot₁ → dot₂ → … → dotN
      // Seeded from the lead dot position/colour so the first segment connects
      // seamlessly. Each step emits one segment [prev, current]; interior points
      // are not duplicated in memory — they're computed and forwarded as "prev".
      const anchor = Math.floor(dt / TRAIL_INTERVAL_S) * TRAIL_INTERVAL_S
      const acCol  = mode === 'altitude' ? null : tlTrackColor(track, mode)
      let sd = si
      let prevX = x, prevY = y, prevZ = z
      let prevFR = col.r, prevFG = col.g, prevFB = col.b   // lead is full brightness
      for (let step = 1; step <= TRAIL_DOTS_PER_AC && ti < TL_MAX_TRAIL_DOTS; step++) {
        const tDot = anchor - step * TRAIL_INTERVAL_S
        if (tDot < pts[0][0]) break                      // before track started
        while (sd > 0 && pts[sd][0] > tDot) sd--        // walk backward
        const q0 = pts[sd], q1 = pts[Math.min(sd + 1, pts.length - 1)]
        if (q1[0] - q0[0] > TL_GAP_S) break             // gap — stop trail
        const aD   = q0 === q1 ? 0 : Math.min(1, (tDot - q0[0]) / (q1[0] - q0[0]))
        const bD   = lerpBearing(q0[1], q1[1], aD)
        const rD   = q0[2] + aD * (q1[2] - q0[2])
        const altD = q0[3] + aD * (q1[3] - q0[3])
        const fade = 1 - (step * TRAIL_INTERVAL_S) / (TRAIL_DURATION_S + TRAIL_INTERVAL_S)
        const cD   = mode === 'altitude' ? altColor(altD) : acCol
        const [wx, wy, wz] = toWorld(bD, rD, altD)
        const fr = cD.r * fade + TL_BG_R * (1 - fade)
        const fg = cD.g * fade + TL_BG_G * (1 - fade)
        const fb = cD.b * fade + TL_BG_B * (1 - fade)
        // Emit segment [prev → current] as a LineSegments pair
        const base = ti * 6
        tp[base]     = prevX;  tp[base + 1] = prevY;  tp[base + 2] = prevZ
        tp[base + 3] = wx;     tp[base + 4] = wy;     tp[base + 5] = wz
        tc[base]     = prevFR; tc[base + 1] = prevFG; tc[base + 2] = prevFB
        tc[base + 3] = fr;     tc[base + 4] = fg;     tc[base + 5] = fb
        ti++
        prevX = wx; prevY = wy; prevZ = wz
        prevFR = fr; prevFG = fg; prevFB = fb
      }
    }

    dotGeo.setDrawRange(0, di)
    dotGeo.attributes.position.needsUpdate = true
    dotGeo.attributes.color.needsUpdate    = true
    trailGeo.setDrawRange(0, ti * 2)   // ti segments × 2 vertices each
    trailGeo.attributes.position.needsUpdate = true
    trailGeo.attributes.color.needsUpdate    = true
  }, [])  // all inputs via refs — no deps needed

  // ── Timelapse: RAF animation loop ────────────────────────────────────
  const tlAnimate = useCallback((rafTs) => {
    if (!tlPlayingRef.current) return
    const data = tlDataRef.current
    if (!data) return

    if (tlLastRafRef.current !== null) {
      const realDt = (rafTs - tlLastRafRef.current) / 1000
      const newDt  = tlCurrentDtRef.current + realDt * tlSpeedRef.current
      const maxDt  = data.end_ts - data.start_ts
      tlCurrentDtRef.current = Math.min(newDt, maxDt)

      // Update scrubber and time label via direct DOM (avoids React re-renders at 60fps)
      if (tlScrubberRef.current)
        tlScrubberRef.current.value = String(tlCurrentDtRef.current)
      if (tlTimeLabelRef.current)
        tlTimeLabelRef.current.textContent = tlFormatTime(data.start_ts, tlCurrentDtRef.current)

      renderTlFrame(tlCurrentDtRef.current)

      if (tlCurrentDtRef.current >= maxDt) {
        // End reached — pause 1.5 s then loop.  In live mode, silently refresh
        // the hires buffer first so each loop plays the latest accumulated data.
        tlLastRafRef.current = null
        if (tlRafRef.current) { cancelAnimationFrame(tlRafRef.current); tlRafRef.current = null }

        const doRestart = (startDt, d) => {
          if (!tlActiveRef.current || !tlPlayingRef.current) return
          tlCurrentDtRef.current = startDt
          tlStartDtRef.current   = startDt
          if (tlScrubberRef.current) {
            tlScrubberRef.current.min   = String(startDt)
            tlScrubberRef.current.max   = String(d.end_ts - d.start_ts)
            tlScrubberRef.current.value = String(startDt)
          }
          if (tlTimeLabelRef.current)
            tlTimeLabelRef.current.textContent = tlFormatTime(d.start_ts, startDt)
          renderTlFrame(startDt)
          tlLastRafRef.current = null
          tlRafRef.current = requestAnimationFrame(tlAnimate)
        }

        setTimeout(() => {
          if (!tlActiveRef.current || !tlPlayingRef.current) return
          if (tlIsLiveRef.current) {
            const end   = Math.floor(Date.now() / 1000)
            const start = end - 86400
            fetch(`/api/coverage/timelapse_hires?start_ts=${start}&end_ts=${end}`)
              .then(r => r.json())
              .then(freshData => {
                const minDt = freshData.tracks.length > 0
                  ? Math.min(...freshData.tracks.map(t => t.points[0]?.[0] ?? 0))
                  : tlStartDtRef.current
                tlDataRef.current = freshData
                doRestart(minDt, freshData)
              })
              .catch(() => doRestart(tlStartDtRef.current, tlDataRef.current))
          } else {
            doRestart(tlStartDtRef.current, tlDataRef.current)
          }
        }, 1500)
        return
      }
    }
    tlLastRafRef.current = rafTs
    tlRafRef.current = requestAnimationFrame(tlAnimate)
  }, [renderTlFrame])

  // ── Timelapse: activate / deactivate ────────────────────────────────
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return

    tlActiveRef.current = tlActive

    if (tlActive) {
      // Hide static point cloud and live trails while timelapse is active
      if (ref.pointsObj)   ref.pointsObj.visible   = false
      if (ref.trailsObj)   ref.trailsObj.visible   = false
      if (ref.liveDotsObj) ref.liveDotsObj.visible = false

      // Build pre-allocated geometries and add to scene
      const dotPositions   = new Float32Array(TL_MAX_AC * 3)
      const dotColors      = new Float32Array(TL_MAX_AC * 3)
      // 2 vertices per segment (LineSegments), so 2× the dot count
      const trailPositions = new Float32Array(TL_MAX_TRAIL_DOTS * 2 * 3)
      const trailColors    = new Float32Array(TL_MAX_TRAIL_DOTS * 2 * 3)

      const dotGeo = new THREE.BufferGeometry()
      dotGeo.setAttribute('position', new THREE.BufferAttribute(dotPositions, 3))
      dotGeo.setAttribute('color',    new THREE.BufferAttribute(dotColors,    3))
      dotGeo.setDrawRange(0, 0)

      const trailGeo = new THREE.BufferGeometry()
      trailGeo.setAttribute('position', new THREE.BufferAttribute(trailPositions, 3))
      trailGeo.setAttribute('color',    new THREE.BufferAttribute(trailColors,    3))
      trailGeo.setDrawRange(0, 0)

      const dotsObj = new THREE.Points(dotGeo, new THREE.PointsMaterial({
        size: 5, vertexColors: true, sizeAttenuation: false,
      }))
      const trailsObj = new THREE.LineSegments(trailGeo, new THREE.LineBasicMaterial({
        vertexColors: true,
      }))
      ref.scene.add(dotsObj)
      ref.scene.add(trailsObj)
      tlGeosRef.current = { dotGeo, trailGeo, dotsObj, trailsObj }
    } else {
      // Stop playback and dispose scene objects
      tlPlayingRef.current = false
      setTlPlaying(false)
      if (tlRafRef.current) { cancelAnimationFrame(tlRafRef.current); tlRafRef.current = null }
      tlLastRafRef.current = null
      tlDataRef.current    = null
      tlCurrentDtRef.current = 0
      const g = tlGeosRef.current
      if (g) {
        ref.scene.remove(g.dotsObj);  g.dotGeo.dispose();   g.dotsObj.material.dispose()
        ref.scene.remove(g.trailsObj); g.trailGeo.dispose(); g.trailsObj.material.dispose()
        tlGeosRef.current = null
      }
      // Restore static and live visibility
      if (ref.pointsObj)   ref.pointsObj.visible   = true
      if (ref.trailsObj)   ref.trailsObj.visible   = true
      if (ref.liveDotsObj) ref.liveDotsObj.visible = true
    }
  }, [tlActive])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Timelapse: start / stop RAF when tlPlaying changes ───────────────
  useEffect(() => {
    tlPlayingRef.current = tlPlaying
    if (tlPlaying) {
      tlLastRafRef.current = null
      tlRafRef.current = requestAnimationFrame(tlAnimate)
    } else {
      if (tlRafRef.current) { cancelAnimationFrame(tlRafRef.current); tlRafRef.current = null }
    }
  }, [tlPlaying, tlAnimate])

  // ── Timelapse: shared fetch helper ───────────────────────────────────
  const _fetchTl = useCallback((url) => {
    setTlLoading(true)
    setTlError(null)
    setTlPlaying(false)
    tlPlayingRef.current = false
    tlCurrentDtRef.current = 0
    fetch(url)
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e.detail)))
      .then(data => {
        // Find the earliest dt across all tracks so the scrubber starts at first data
        const minDt = data.tracks.length > 0
          ? Math.min(...data.tracks.map(t => t.points[0]?.[0] ?? 0))
          : 0
        tlDataRef.current        = data
        tlStartDtRef.current     = minDt
        tlCurrentDtRef.current   = minDt
        if (tlScrubberRef.current) {
          tlScrubberRef.current.min   = String(minDt)
          tlScrubberRef.current.max   = String(data.end_ts - data.start_ts)
          tlScrubberRef.current.value = String(minDt)
        }
        if (tlTimeLabelRef.current)
          tlTimeLabelRef.current.textContent = tlFormatTime(data.start_ts, minDt)
        renderTlFrame(minDt)
        setTlLoading(false)
      })
      .catch(e => { setTlError(String(e)); setTlLoading(false) })
  }, [renderTlFrame])

  // Load high-resolution data for the last 24 hours (in-memory buffer)
  const loadHires = useCallback(() => {
    setTlIsLive(true)
    tlIsLiveRef.current = true
    const end   = Math.floor(Date.now() / 1000)
    const start = end - 86400
    _fetchTl(`/api/coverage/timelapse_hires?start_ts=${start}&end_ts=${end}`)
  }, [_fetchTl])

  // Load historical data from the DB for the selected date
  const loadHistorical = useCallback(() => {
    setTlIsLive(false)
    tlIsLiveRef.current = false
    const start = Math.floor(new Date(tlDate + 'T00:00:00Z').getTime() / 1000)
    const end   = start + 86400
    _fetchTl(`/api/coverage/timelapse?start_ts=${start}&end_ts=${end}`)
  }, [tlDate, _fetchTl])

  // Auto-load hires when timelapse is activated
  useEffect(() => {
    if (tlActive) loadHires()
  }, [tlActive])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Timelapse: scrubber seek ─────────────────────────────────────────
  const handleScrub = useCallback((e) => {
    tlCurrentDtRef.current = parseFloat(e.target.value)
    const data = tlDataRef.current
    if (data && tlTimeLabelRef.current)
      tlTimeLabelRef.current.textContent = tlFormatTime(data.start_ts, tlCurrentDtRef.current)
    if (!tlPlayingRef.current) renderTlFrame(tlCurrentDtRef.current)
  }, [renderTlFrame])

  // ── Replace the point cloud in the scene ────────────────────────────
  const redraw = useCallback((points, mode, filter, show) => {
    const ref = sceneRef.current
    if (!ref) return

    // Live-only: remove historical points and bail out
    if (show === 'live') {
      if (ref.pointsObj) {
        ref.scene.remove(ref.pointsObj)
        ref.pointsObj.geometry.dispose()
        ref.pointsObj.material.dispose()
        ref.pointsObj = null
      }
      setShownN(0)
      return
    }

    const filtered = filter === 'military'    ? points.filter(p => p[3])
                   : filter === 'interesting' ? points.filter(p => p[4])
                   : points

    setShownN(filtered.length)

    if (ref.pointsObj) {
      ref.scene.remove(ref.pointsObj)
      ref.pointsObj.geometry.dispose()
      ref.pointsObj.material.dispose()
    }
    if (filtered.length === 0) { ref.pointsObj = null; return }
    const obj = buildPoints(filtered, mode)
    ref.scene.add(obj)
    ref.pointsObj = obj
    if (tlActiveRef.current) obj.visible = false   // keep hidden if timelapse is active
  }, [])

  // ── Fetch when days or maxPoints changes ────────────────────────────
  useEffect(() => {
    let cancelled = false
    setLoadingPhase('fetching')
    setError(null)

    fetch(`/api/coverage/points?days=${days}&max_points=${maxPoints}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(({ points, operators: ops, type_groups: tgs }) => {
        if (cancelled) return
        pendingRef.current = { points, ops, tgs }
        setFetchedN(points.length)
        setOperators(ops ?? [])
        // Transition to 'rendering' so React can paint the "Building scene…" message
        // before the synchronous buffer-build blocks the main thread.
        setLoadingPhase('rendering')
      })
      .catch(e => { if (!cancelled) { setLoadingPhase(null); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [days, maxPoints])

  // ── After 'rendering' phase is painted, do the expensive buffer build ──
  useEffect(() => {
    if (loadingPhase !== 'rendering') return
    const pending = pendingRef.current
    if (!pending) return

    const id = setTimeout(() => {
      dataRef.current = pending.points
      redraw(pending.points, colorMode, typeFilter, showMode)
      setLoadingPhase(null)
    }, 30)   // 30 ms gives React time to paint the "Building scene…" message

    return () => clearTimeout(id)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadingPhase])

  // ── Re-colour / re-filter from stored data (no fetch) ───────────────
  useEffect(() => {
    if (dataRef.current.length) redraw(dataRef.current, colorMode, typeFilter, showMode)
  }, [colorMode, typeFilter, showMode, redraw])

  // ── Accumulate live aircraft trails ─────────────────────────────────
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return

    // History-only: remove live layers and bail out
    if (showMode === 'history') {
      if (ref.trailsObj)   { ref.scene.remove(ref.trailsObj);   ref.trailsObj.geometry.dispose();   ref.trailsObj.material.dispose();   ref.trailsObj = null }
      if (ref.liveDotsObj) { ref.scene.remove(ref.liveDotsObj); ref.liveDotsObj.geometry.dispose(); ref.liveDotsObj.material.dispose(); ref.liveDotsObj = null }
      return
    }

    if (!aircraft.length) return

    const trails = trailsRef.current

    // Remove departed aircraft
    const activeSet = new Set(aircraft.map(ac => ac.icao))
    for (const icao of Object.keys(trails)) {
      if (!activeSet.has(icao)) delete trails[icao]
    }

    // Append new positions
    const nowS = Date.now() / 1000
    for (const ac of aircraft) {
      if (ac.bearing_deg == null || ac.range_nm == null || ac.altitude == null) continue
      if (!(ac.pos_global || ac.mlat || ac.pos_confident)) continue
      if (!trails[ac.icao]) trails[ac.icao] = []
      const trail = trails[ac.icao]

      // If this aircraft has been without position for a while, start a new trail segment.
      if (ac.last_pos_age != null && ac.last_pos_age > MAX_TRAIL_GAP_S) trail.length = 0

      let last = trail[trail.length - 1]
      // Skip if position hasn't moved meaningfully (avoids duplicate points for stationary aircraft)
      if (last &&
          Math.abs(last.bearing - ac.bearing_deg) < 0.05 &&
          Math.abs(last.range   - ac.range_nm)    < 0.05) continue

      // Hard break on reacquisition jumps so we never stitch unrealistic lines.
      if (last) {
        const dt = nowS - last.ts
        const distNm = haversineNm(last.lat, last.lon, ac.lat, ac.lon)
        const impliedSpeedKt = dt > 0 && distNm != null ? (distNm / dt) * 3600 : Infinity
        const sourceSwitched = !!last.mlat !== !!ac.mlat
        if (dt <= 0
            || dt > MAX_TRAIL_GAP_S
            || impliedSpeedKt > MAX_TRAIL_IMPLIED_SPEED_KT
            || (sourceSwitched && distNm != null && distNm > MAX_SOURCE_SWITCH_JUMP_NM)) {
          trail.length = 0
        }
      }

      trail.push({
        bearing:       ac.bearing_deg,
        range:         ac.range_nm,
        alt:           ac.altitude,
        lat:           ac.lat,
        lon:           ac.lon,
        ts:            nowS,
        military:      ac.military,
        mlat:          ac.mlat,
        interesting:   ac.interesting,
        acas:          ac.acas_ra_active,
        type_code:     ac.type_code,
        type_category: ac.type_category,
        operator:      ac.operator,
      })
      if (trail.length > MAX_TRAIL_PTS) trail.splice(0, trail.length - MAX_TRAIL_PTS)
    }

    // Rebuild trail lines and current-position dots
    const { scene } = ref
    if (ref.trailsObj) {
      scene.remove(ref.trailsObj)
      ref.trailsObj.geometry.dispose()
      ref.trailsObj.material.dispose()
      ref.trailsObj = null
    }
    if (ref.liveDotsObj) {
      scene.remove(ref.liveDotsObj)
      ref.liveDotsObj.geometry.dispose()
      ref.liveDotsObj.material.dispose()
      ref.liveDotsObj = null
    }
    const mesh = buildTrails(trails, colorMode, operators)
    if (mesh) { scene.add(mesh); ref.trailsObj = mesh; if (tlActiveRef.current) mesh.visible = false }
    const dots = buildLiveDots(trails, colorMode, operators)
    if (dots) { scene.add(dots); ref.liveDotsObj = dots; if (tlActiveRef.current) dots.visible = false }
  }, [aircraft, showMode, colorMode, operators])

  const toggleCoastline = useCallback(() => {
    const v = !showCoastlineRef.current
    showCoastlineRef.current = v
    setShowCoastline(v)
    const ref = sceneRef.current
    if (ref?.coastlineObj) ref.coastlineObj.visible = v
  }, [])

  const toggleAirportsLarge = useCallback(() => {
    const v = !showAirportsLargeRef.current
    showAirportsLargeRef.current = v
    setShowAirportsLarge(v)
    const ref = sceneRef.current
    if (ref?.airportsLargeObj) ref.airportsLargeObj.visible = v
  }, [])

  const toggleAirportsMedium = useCallback(() => {
    const v = !showAirportsMediumRef.current
    showAirportsMediumRef.current = v
    setShowAirportsMedium(v)
    const ref = sceneRef.current
    if (ref?.airportsMediumObj) ref.airportsMediumObj.visible = v
  }, [])

  const setTlSpeedAndRef = useCallback((s) => {
    tlSpeedRef.current = s
    setTlSpeed(s)
  }, [])

  const resetCamera = useCallback(() => {
    const ref = sceneRef.current
    if (!ref) return
    ref.camera.position.set(0, 90, 260)
    ref.controls.target.set(0, 25, 0)
    ref.controls.update()
  }, [])

  const isTagMode = colorMode === 'tag'
  const isOpMode  = colorMode === 'operator'
  const isTgMode  = colorMode === 'type_group'

  const loadingMsg = loadingPhase === 'fetching'   ? 'Fetching points from database…'
                   : loadingPhase === 'rendering'  ? 'Building 3D scene…'
                   : null

  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        <h2 className={styles.title}>3D Coverage</h2>

        {/* Days */}
        <div className={styles.controls}>
          {DAY_OPTIONS.map(({ label, value }) => (
            <button key={value} className={days === value ? styles.btnActive : styles.btn} onClick={() => setDays(value)}>{label}</button>
          ))}
        </div>

        <div className={styles.sep} />

        {/* Max points */}
        <div className={styles.controls}>
          {MAX_POINT_OPTIONS.map(n => (
            <button key={n} className={maxPoints === n ? styles.btnActive : styles.btn} onClick={() => setMaxPoints(n)}>
              {n >= 1000 ? `${n / 1000}k` : n}
            </button>
          ))}
        </div>

        <div className={styles.sep} />

        {/* Type filter */}
        <div className={styles.controls}>
          {['all', 'military', 'interesting'].map(f => (
            <button key={f} className={typeFilter === f ? styles.btnActive : styles.btn} onClick={() => setTypeFilter(f)}>
              {f.charAt(0).toUpperCase() + f.slice(1)}
            </button>
          ))}
        </div>

        <div className={styles.sep} />

        {/* Colour mode */}
        <div className={styles.controls}>
          <button className={colorMode === 'type_group' ? styles.btnActive : styles.btn} onClick={() => setColorMode('type_group')}>By type</button>
          <button className={colorMode === 'altitude'   ? styles.btnActive : styles.btn} onClick={() => setColorMode('altitude')}>By altitude</button>
          <button className={colorMode === 'operator'   ? styles.btnActive : styles.btn} onClick={() => setColorMode('operator')}>By operator</button>
          <button className={colorMode === 'tag'        ? styles.btnActive : styles.btn} onClick={() => setColorMode('tag')}>By tag</button>
        </div>

        <div className={styles.sep} />

        {/* Show mode */}
        <div className={styles.controls}>
          <button className={showMode === 'both'    ? styles.btnActive : styles.btn} onClick={() => setShowMode('both')}>Both</button>
          <button className={showMode === 'history' ? styles.btnActive : styles.btn} onClick={() => setShowMode('history')}>History</button>
          <button className={showMode === 'live'    ? styles.btnActive : styles.btn} onClick={() => setShowMode('live')}>Live</button>
        </div>

        <button className={styles.resetBtn} onClick={resetCamera}>Reset view</button>
        <button className={showCoastline ? styles.btnActive : styles.btn} onClick={toggleCoastline}>Coastline</button>
        <button className={showAirportsLarge  ? styles.btnActive : styles.btn} onClick={toggleAirportsLarge}>Airports</button>
        <button className={showAirportsMedium ? styles.btnActive : styles.btn} onClick={toggleAirportsMedium}>Med</button>
        <div className={styles.sep} />
        <button className={tlActive ? styles.btnActive : styles.btn} onClick={() => setTlActive(v => !v)}>Timelapse</button>
      </div>

      {tlActive && (
        <div className={styles.tlBar}>
          <button className={tlIsLive ? styles.btnActive : styles.tlBtn}
            onClick={loadHires} disabled={tlLoading}>
            {tlLoading && tlIsLive ? '…' : 'Latest 24h'}
          </button>
          <div className={styles.sep} />
          <input
            type="date"
            className={styles.tlDate}
            value={tlDate}
            max={new Date().toISOString().slice(0, 10)}
            min={(() => { const d = new Date(); d.setDate(d.getDate() - 89); return d.toISOString().slice(0, 10) })()}
            onChange={e => setTlDate(e.target.value)}
          />
          <button className={styles.tlBtn} onClick={loadHistorical} disabled={tlLoading}>
            {tlLoading && !tlIsLive ? '…' : 'Load date'}
          </button>
          <div className={styles.sep} />
          <button
            className={styles.tlBtn}
            onClick={() => setTlPlaying(v => !v)}
            disabled={!tlDataRef.current}
          >{tlPlaying ? '⏸' : '▶'}</button>
          <input
            type="range" className={styles.tlScrubber}
            ref={tlScrubberRef}
            min="0" max="86400" step="1" defaultValue="0"
            onChange={handleScrub}
          />
          <span className={styles.tlTime} ref={tlTimeLabelRef}>--:-- UTC</span>
          <div className={styles.sep} />
          {TL_SPEEDS.map(s => (
            <button key={s} className={tlSpeed === s ? styles.btnActive : styles.btn}
              onClick={() => setTlSpeedAndRef(s)}>{s}×</button>
          ))}
          {tlError && <span className={styles.tlError}>{tlError}</span>}
        </div>
      )}

      {!loadingPhase && !error && (
        <div className={styles.meta}>
          {shownN.toLocaleString()} points shown
          {shownN !== fetchedN && ` (${fetchedN.toLocaleString()} fetched)`}
          {' · drag to rotate · scroll to zoom · right-drag to pan'}
        </div>
      )}

      <div className={styles.canvasWrap} ref={mountRef}>
        {(loadingMsg || error) && (
          <div className={styles.overlay}>
            {error ? `Error: ${error}` : (
              <div className={styles.loadingBox}>
                <div className={styles.spinner} />
                <span>{loadingMsg}</span>
              </div>
            )}
          </div>
        )}

        {/* 2D compass rose — top-right. The inner ring rotates with camera azimuth
            via compassRef (direct DOM, not React state). N stays aligned with scene North. */}
        <div className={styles.compass} aria-hidden="true">
          <div className={styles.compassInner} ref={compassRef}>
            <span className={styles.compassN}>N</span>
            <span className={styles.compassE}>E</span>
            <span className={styles.compassS}>S</span>
            <span className={styles.compassW}>W</span>
            <div className={styles.compassNeedle} />
          </div>
        </div>
      </div>

      <div className={styles.legend}>
        {isTgMode ? (
          <>
            {TYPE_GROUPS.map((g) => (
              <span key={g.value} className={styles.legendItem}>
                <span className={styles.dot} style={{ background: g.color }} />{g.label}
              </span>
            ))}
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: TYPE_GROUP_OTHER_COLOR }} />Other</span>
          </>
        ) : isTagMode ? (
          <>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#bc8cff' }} />Military</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#d29922' }} />Interesting</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#3fb950' }} />Standard</span>
          </>
        ) : isOpMode ? (
          <>
            {operators.map((op, i) => (
              <span key={op} className={styles.legendItem}>
                <span className={styles.dot} style={{ background: NAMED_PALETTE[i] }} />{op}
              </span>
            ))}
            {operators.length > 0 && (
              <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#484f58' }} />Other</span>
            )}
          </>
        ) : (
          <>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#3fb950' }} />Low ({`<${ALT_SCALE_FT / 3000 | 0}k ft`})</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#d29922' }} />Mid</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#388bfd' }} />High</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#bc8cff' }} />{`>${Math.round(ALT_SCALE_FT * 0.67 / 1000)}k ft`}</span>
          </>
        )}
        <span className={styles.legendItem} style={{ marginLeft: 'auto', color: '#484f58' }}>
          rings = 50 nm · vertical ×{VERT_EXAG} · trails = live aircraft
        </span>
      </div>
    </div>
  )
}
