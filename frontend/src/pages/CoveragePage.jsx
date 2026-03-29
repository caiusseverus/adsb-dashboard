import { useEffect, useRef, useState, useCallback } from 'react'
import * as THREE from 'three'
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js'
import styles from './CoveragePage.module.css'
import { NAMED_PALETTE, TYPE_GROUPS, TYPE_GROUP_OTHER_COLOR, getTypeGroup } from '../utils/typeGroups'
import { buildTerrainMesh } from '../utils/terrain'
import { haversineNm } from '../utils/geo'

// Rendering parameters — altScale and curveMode — are owned by useRef inside the
// component and passed explicitly to all builder functions below.

const FEET_PER_NM   = 6076.115
const R_NM          = 3440.065   // Earth radius in NM
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

// Reused to avoid allocating a new THREE.Color on every call (called per point per frame)
const _altColorBuf = new THREE.Color()
function altColor(alt_ft) {
  const t = Math.min(1, Math.max(0, alt_ft / ALT_SCALE_FT))
  for (let i = 1; i < ALT_STOPS.length; i++) {
    const [t0, c0] = ALT_STOPS[i - 1]
    const [t1, c1] = ALT_STOPS[i]
    if (t <= t1) return _altColorBuf.copy(c0).lerp(c1, (t - t0) / (t1 - t0))
  }
  return _altColorBuf.copy(ALT_STOPS[ALT_STOPS.length - 1][1])
}

// [bearing_deg, range_nm, alt_ft] → Three.js world coords
function _toWorldFlat(bearing_deg, range_nm, alt_ft, altScale) {
  const rad = bearing_deg * Math.PI / 180
  return [
    range_nm * Math.sin(rad),
    (alt_ft / FEET_PER_NM) * altScale,
    -range_nm * Math.cos(rad),   // negate: North (0°) → −Z, into scene
  ]
}

function _toWorldCurved(bearing_deg, range_nm, alt_ft, altScale) {
  const R_eff  = R_NM / altScale   // k = altScale: correct relative horizon crossing
  const gc_rad = range_nm / R_eff
  const bRad   = bearing_deg * Math.PI / 180
  const sin_gc = Math.sin(gc_rad)
  const cos_gc = Math.cos(gc_rad)
  const r = R_eff + (alt_ft / FEET_PER_NM) * altScale   // scale before unit-vector multiply
  return [
    r * sin_gc * Math.sin(bRad),
    r * cos_gc - R_eff,            // receiver surface = y=0
    -r * sin_gc * Math.cos(bRad),
  ]
}

function toWorld(bearing_deg, range_nm, alt_ft, altScale, curveMode) {
  return curveMode
    ? _toWorldCurved(bearing_deg, range_nm, alt_ft, altScale)
    : _toWorldFlat(bearing_deg, range_nm, alt_ft, altScale)
}

// Helper: altitude axis line (rebuilt when altScale changes)
function makeAltAxis(altScale) {
  const altTopY = (ALT_SCALE_FT / FEET_PER_NM) * altScale + 5
  const geo = new THREE.BufferGeometry().setFromPoints([
    new THREE.Vector3(0, 0, 0),
    new THREE.Vector3(0, altTopY, 0),
  ])
  return new THREE.Line(geo, new THREE.LineBasicMaterial({ color: 0x484f58 }))
}

// Build a Points object from (already-filtered) raw point data + colour mode
function buildPoints(points, colorMode, altScale, curveMode) {
  const n = points.length
  const positions = new Float32Array(n * 3)
  const colors    = new Float32Array(n * 3)

  for (let i = 0; i < n; i++) {
    const [bearing, range, alt, military, interesting, op_idx, tg_idx, tc_idx] = points[i]
    const [x, y, z] = toWorld(bearing, range, alt, altScale, curveMode)
    positions[i * 3]     = x
    positions[i * 3 + 1] = y
    positions[i * 3 + 2] = z

    let col
    if (colorMode === 'altitude') {
      col = altColor(alt)
    } else if (colorMode === 'operator') {
      col = (op_idx != null && op_idx < 10) ? C_OP_PALETTE[op_idx] : C_OP_OTHER
    } else if (colorMode === 'type_code') {
      col = (tc_idx != null && tc_idx < 10) ? C_OP_PALETTE[tc_idx] : C_OP_OTHER
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
// Shared colour function for live aircraft dots/trails and timelapse tracks.
// obj may be a live aircraft snapshot or a timelapse track — both shapes supported.
function pointColor(obj, colorMode, operators = [], typeCodes = []) {
  if (colorMode === 'type_group') {
    // Timelapse tracks carry pre-computed tg_idx; live aircraft have type_code/category
    if (obj.tg_idx != null) return obj.tg_idx < 8 ? C_TG_PALETTE[obj.tg_idx] : C_TG_OTHER
    const g = getTypeGroup(obj.type_code, obj.type_category)
    return g ? new THREE.Color(g.color) : C_TG_OTHER
  }
  if (colorMode === 'type_code') {
    // History points carry pre-computed tc_idx; live aircraft carry type_code string
    const idx = obj.tc_idx != null ? obj.tc_idx
              : obj.type_code ? typeCodes.indexOf(obj.type_code) : -1
    return (idx >= 0 && idx < 10) ? C_OP_PALETTE[idx] : C_OP_OTHER
  }
  if (colorMode === 'operator') {
    // History points carry pre-computed op_idx; live aircraft carry operator string
    const idx = obj.op_idx != null ? obj.op_idx
              : obj.operator ? operators.indexOf(obj.operator) : -1
    return (idx >= 0 && idx < 10) ? C_OP_PALETTE[idx] : C_OP_OTHER
  }
  // tag / default
  return obj.acas        ? C_TRAIL.acas
       : obj.military    ? C_TRAIL.military
       : obj.mlat        ? C_TRAIL.mlat
       : obj.interesting ? C_TRAIL.interesting
       : C_TRAIL.standard
}

// Build merged LineSegments for all live aircraft trails
function buildTrails(trails, colorMode, operators, typeCodes, altScale, curveMode) {
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
    const acCol = colorMode !== 'altitude' ? pointColor(pts[pts.length - 1], colorMode, operators, typeCodes) : null

    for (let i = 0; i < pts.length - 1; i++) {
      const a = pts[i], b = pts[i + 1]
      // Skip segment on time gap or impossible position jump (CPR glitch guard)
      if (!isTrailSegmentValid(a, b)) continue
      const [x0, y0, z0] = toWorld(a.bearing, a.range, a.alt ?? 0, altScale, curveMode)
      const [x1, y1, z1] = toWorld(b.bearing, b.range, b.alt ?? 0, altScale, curveMode)
      // Altitude mode: colour each vertex by its own altitude (gradient along trail).
      // _altColorBuf is shared — capture A's rgb before calling altColor again for B.
      const colA = colorMode === 'altitude' ? altColor(a.alt ?? 0) : acCol
      const rA = colA.r, gA = colA.g, bA = colA.b
      const colB = colorMode === 'altitude' ? altColor(b.alt ?? 0) : acCol
      positions[idx * 6]     = x0; positions[idx * 6 + 1] = y0; positions[idx * 6 + 2] = z0
      positions[idx * 6 + 3] = x1; positions[idx * 6 + 4] = y1; positions[idx * 6 + 5] = z1
      colors[idx * 6]     = rA;    colors[idx * 6 + 1] = gA;    colors[idx * 6 + 2] = bA
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
function buildLiveDots(trails, colorMode, operators, typeCodes, altScale, curveMode) {
  const entries = Object.values(trails).filter(pts => pts.length > 0)
  if (entries.length === 0) return null

  const n = entries.length
  const positions = new Float32Array(n * 3)
  const colors    = new Float32Array(n * 3)

  entries.forEach((pts, i) => {
    const last = pts[pts.length - 1]
    const [x, y, z] = toWorld(last.bearing, last.range, last.alt ?? 0, altScale, curveMode)
    positions[i * 3]     = x
    positions[i * 3 + 1] = y
    positions[i * 3 + 2] = z
    const col = colorMode === 'altitude' ? altColor(last.alt ?? 0)
              : pointColor(last, colorMode, operators, typeCodes)
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

// Opaque earth sphere for curved mode — occludes aircraft/coastline behind the horizon.
// R_eff matches the coordinate system: receiver at y=0, earth centre at y=-R_eff.
// polygonOffset pushes it fractionally behind terrain/grid at the same depth.
function buildEarthSphere(altScale) {
  const R_eff = R_NM / altScale
  const geo = new THREE.SphereGeometry(R_eff, 72, 54)
  const mat = new THREE.MeshBasicMaterial({
    color: 0x0b0c10,   // match scene background — curved/flat modes look identical
    polygonOffset: true,
    polygonOffsetFactor: 2,
    polygonOffsetUnits: 1,
  })
  const mesh = new THREE.Mesh(geo, mat)
  mesh.position.set(0, -R_eff, 0)
  return mesh
}

// Range ring — uses toWorld so it curves correctly in curved mode
function makeRing(radius, altScale, curveMode) {
  const pts = []
  for (let i = 0; i <= 128; i++) {
    const bearing = (i / 128) * 360
    const [x, y, z] = toWorld(bearing, radius, 0, altScale, curveMode)
    pts.push(new THREE.Vector3(x, y, z))
  }
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color: 0x30363d }),
  )
}

// Cardinal spoke — subdivided so it follows Earth's curve in curved mode
function makeSpoke(bearing_deg, length, altScale, curveMode) {
  const STEPS = 32
  const pts = []
  for (let i = 0; i <= STEPS; i++) {
    const r = (i / STEPS) * length
    const [x, y, z] = toWorld(bearing_deg, r, 0, altScale, curveMode)
    pts.push(new THREE.Vector3(x, y, z))
  }
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color: 0x30363d }),
  )
}

// Rebuild all ring + spoke objects into the scene; dispose old ones first.
// Returns array of the new objects so they can be tracked in sceneRef.gridObjs.
function rebuildGridObjects(scene, existingObjs, altScale, curveMode) {
  for (const obj of existingObjs) {
    scene.remove(obj)
    obj.geometry.dispose()
    obj.material.dispose()
  }
  const objs = []
  for (let r = 50; r <= 250; r += 50) { const o = makeRing(r, altScale, curveMode); scene.add(o); objs.push(o) }
  for (const b of [0, 90, 180, 270]) { const o = makeSpoke(b, 270, altScale, curveMode); scene.add(o); objs.push(o) }
  return objs
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
function buildCoastline(segments, altScale, curveMode) {
  if (!segments || segments.length === 0) return null
  const positions = new Float32Array(segments.length * 6)
  let idx = 0
  for (const [b1, r1, b2, r2] of segments) {
    const [x0, y0, z0] = toWorld(b1, r1, 0, altScale, curveMode)
    const [x1, y1, z1] = toWorld(b2, r2, 0, altScale, curveMode)
    positions[idx++] = x0; positions[idx++] = y0; positions[idx++] = z0
    positions[idx++] = x1; positions[idx++] = y1; positions[idx++] = z1
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
function buildAirports(airports, altScale, curveMode) {
  if (!airports || airports.length === 0) return null
  const group = new THREE.Group()

  // Dots — single Points object for all airports
  const positions = new Float32Array(airports.length * 3)
  airports.forEach((ap, i) => {
    const [x, y, z] = toWorld(ap.bearing, ap.range_nm, 0, altScale, curveMode)
    positions[i * 3]     = x
    positions[i * 3 + 1] = y + 0.5  // slightly above surface (curved or flat)
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
    const [x, y, z] = toWorld(ap.bearing, ap.range_nm, 0, altScale, curveMode)
    sprite.position.set(x + 2, y + 3, z)  // offset right of dot, float above surface
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
const DEFAULT_MAX_POINTS = 50000

export default function CoveragePage({ aircraft = [], initialIcao = '' }) {
  const mountRef    = useRef(null)
  const sceneRef    = useRef(null)   // { scene, camera, renderer, controls, pointsObj, trailsObj, liveDotsObj }
  const dataRef     = useRef([])     // raw fetched points (unfiltered)
  const pendingRef  = useRef(null)   // points waiting to render after 'rendering' phase
  const trailsRef   = useRef({})     // live trail buffer: { icao: [{bearing,range,alt,...}] }

  const [terrainEnabled, setTerrainEnabled] = useState(true)
  useEffect(() => {
    fetch('/api/status')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.config?.terrain_enabled === false) setTerrainEnabled(false) })
      .catch(() => {})
  }, [])
  const compassRef  = useRef(null)   // ref to inner compass ring div (rotated via JS, not React state)

  const [days,         setDays]         = useState(1)
  const [maxPoints,    setMaxPoints]    = useState(DEFAULT_MAX_POINTS)
  const [colorMode,    setColorMode]    = useState('type_group')
  const [showMode,     setShowMode]     = useState('both')  // 'both' | 'live' | 'history'
  const [loadingPhase, setLoadingPhase] = useState(null)  // null | 'fetching' | 'rendering'
  const [error,        setError]        = useState(null)
  const [fetchedN,     setFetchedN]     = useState(0)
  const [shownN,       setShownN]       = useState(0)
  const [operators,    setOperators]    = useState([])
  const [typeCodes,    setTypeCodes]    = useState([])
  const allOperatorsRef = useRef([])   // full operator list from options endpoint; drives datalist
  const [selectedOperator,  setSelectedOperator]  = useState('')
  const [opInput,           setOpInput]           = useState('')
  const [selectedTypeGroup, setSelectedTypeGroup] = useState('')  // TYPE_GROUPS value
  const [selectedTypeCode,  setSelectedTypeCode]  = useState('')  // exact ICAO type code
  const [tcInput,           setTcInput]           = useState('')
  const allTypeCodesRef = useRef([])   // full type code list from options endpoint
  const [selectedIcao,  setSelectedIcao]  = useState(initialIcao)
  const [icaoInput,     setIcaoInput]     = useState('')

  const [showCoastline, setShowCoastline] = useState(true)
  const showCoastlineRef = useRef(true)  // avoids stale closure in fetch callback
  const [showAirportsLarge,  setShowAirportsLarge]  = useState(true)
  const [showAirportsMedium, setShowAirportsMedium] = useState(false)
  const showAirportsLargeRef  = useRef(true)
  const showAirportsMediumRef = useRef(false)
  const airportsLargeDataRef  = useRef(null)   // raw airport arrays for rebuild on mode change
  const airportsMediumDataRef = useRef(null)

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
  const tlOperatorsRef  = useRef([])   // top-10 operators from loaded timelapse window
  const tlTypeCodesRef  = useRef([])   // top-10 type codes from loaded timelapse window
  // Filter refs — kept in sync with state so renderTlFrame ([] deps) can read them
  const tlFilterOpRef   = useRef('')     // mirrors selectedOperator
  const tlFilterTcRef   = useRef('')     // mirrors selectedTypeCode
  const tlFilterTgRef   = useRef('')     // mirrors selectedTypeGroup
  const tlFilterMlatRef = useRef(false)  // mirrors mlatOnly
  const coastlineDataRef = useRef(null) // raw segments for coastline rebuild on mode change

  // ── Filter state (military / mlat — fetch unsampled subset) ─────────
  const [militaryOnly, setMilitaryOnly] = useState(false)
  const [mlatOnly,     setMlatOnly]     = useState(false)

  // Colour mode overrides when a filter narrows to one operator or one type:
  //  operator selected → colour by type_code (fleet composition)
  //  type filter active → colour by operator (which operators fly this type)
  //  military/mlat filter active → colour by type_code (fewer aircraft, broad groups aren't useful)
  const effectiveColorMode =
    (colorMode === 'operator' && selectedOperator) ? 'type_code' :
    (colorMode === 'type_group' && (selectedTypeGroup || selectedTypeCode)) ? 'operator' :
    (colorMode === 'type_group' && (militaryOnly || mlatOnly)) ? 'type_code' :
    colorMode

  // ── Receiver view state ──────────────────────────────────────────────
  const [receiverView,   setReceiverView]   = useState(false)
  const recvAzRef  = useRef(0)    // look azimuth degrees (0 = north)
  const recvElRef  = useRef(3)    // look elevation degrees above horizon
  // Center-cell terrain elevation in metres — populated by terrain fetch, 0 when no terrain
  const terrainCenterElevMRef = useRef(0)
  // When non-null, the RAF animate loop applies receiver camera instead of controls.update()
  const receiverCamRef = useRef(null)

  // ── Terrain / coordinate-mode state ─────────────────────────────────
  const [showTerrain,   setShowTerrain]   = useState(false)
  const [terrainWire,   setTerrainWire]   = useState(false)
  const terrainWireRef  = useRef(false)   // ref so async terrain fetch reads current value
  const [terrainHiRes,  setTerrainHiRes]  = useState(false)
  const [terrainLoading, setTerrainLoading] = useState(false)
  const [curveMode,    setCurveMode]    = useState(false)
  const [altScale,     setAltScale]     = useState(8)
  // Refs so RAF loop and async effects always read the current coordinate params
  // without being captured in stale closures.
  const altScaleRef  = useRef(8)
  const curveModeRef = useRef(false)
  // sceneVersion bumps to force a full redraw when coordinate params change
  const [sceneVersion, setSceneVersion] = useState(0)

  // ── Initialise Three.js (once) ──────────────────────────────────────
  useEffect(() => {
    const container = mountRef.current
    if (!container) return
    const W = container.clientWidth, H = container.clientHeight

    const scene    = new THREE.Scene()
    scene.background = new THREE.Color(0x0b0c10)

    // Lighting for terrain (MeshLambertMaterial). Points use MeshBasicMaterial so unaffected.
    const dirLight = new THREE.DirectionalLight(0xffffff, 1.0)
    dirLight.position.set(1, 3, 1)   // high-angle sun from NE
    scene.add(dirLight)
    scene.add(new THREE.AmbientLight(0x404050, 0.7))  // fill so shadowed faces aren't black

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

    const initialGridObjs = rebuildGridObjects(scene, [], altScaleRef.current, curveModeRef.current)

    const LR = 278
    scene.add(makeLabel('N',    0, -LR))
    scene.add(makeLabel('S',    0,  LR))
    scene.add(makeLabel('E',  LR,    0))
    scene.add(makeLabel('W', -LR,    0))

    const altAxis = makeAltAxis(altScaleRef.current)
    scene.add(altAxis)

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
    const animate = () => {
      rafId = requestAnimationFrame(animate)
      const rc = receiverCamRef.current
      if (rc) {
        // Receiver view: same altScale× coordinate space as aircraft and terrain.
        // Camera y = (terrain_elev + 10 ft) * altScale — both scale together so
        // the receiver always sits 10-ft*altScale above the terrain surface.
        // An aircraft is above the camera whenever alt_ft > terrain_elev_ft + 10,
        // which is true for any aircraft genuinely above the receiver location.
        const recvY = (Math.max(0, terrainCenterElevMRef.current) / 1852 + 10 / FEET_PER_NM) * altScaleRef.current
        const az = rc.az * Math.PI / 180
        const el = rc.el * Math.PI / 180
        camera.position.set(0, recvY, 0)
        camera.lookAt(
          Math.sin(az) * Math.cos(el) * 5000,
          recvY + Math.sin(el) * 5000,
          -Math.cos(az) * Math.cos(el) * 5000,
        )
      } else {
        controls.update()
      }
      renderer.render(scene, camera)
    }
    animate()

    const onResize = () => {
      const w = container.clientWidth, h = container.clientHeight
      camera.aspect = w / h
      camera.updateProjectionMatrix()
      renderer.setSize(w, h)
    }
    window.addEventListener('resize', onResize)

    sceneRef.current = { scene, camera, renderer, controls, pointsObj: null, trailsObj: null, liveDotsObj: null, coastlineObj: null, airportsLargeObj: null, airportsMediumObj: null, terrainObjs: [], earthObj: null, altAxisObj: altAxis, gridObjs: initialGridObjs }

    return () => {
      cancelAnimationFrame(rafId)
      window.removeEventListener('resize', onResize)
      controls.removeEventListener('change', updateCompass)
      renderer.dispose()
      if (container.contains(renderer.domElement)) container.removeChild(renderer.domElement)
      sceneRef.current = null
    }
  }, [])

  // ── Fetch coastline once, rebuild geometry when coordinate mode changes ─
  useEffect(() => {
    fetch('/api/coverage/coastline')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data?.segments?.length) return
        coastlineDataRef.current = data.segments   // store for rebuilds
        const ref = sceneRef.current
        if (!ref) return
        const obj = buildCoastline(data.segments, altScaleRef.current, curveModeRef.current)
        if (!obj) return
        obj.visible = showCoastlineRef.current
        ref.scene.add(obj)
        ref.coastlineObj = obj
      })
      .catch(() => {})  // coastline is optional — degrade silently
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Fetch and render airports (large + medium fetched separately) ────
  useEffect(() => {
    function fetchLayer(types, refKey, dataRef, visibleRef) {
      fetch(`/api/coverage/airports?types=${types}`)
        .then(r => r.ok ? r.json() : null)
        .then(data => {
          if (!data?.airports?.length) return
          dataRef.current = data.airports   // store for rebuild on mode change
          const ref = sceneRef.current
          if (!ref) return
          const obj = buildAirports(data.airports, altScaleRef.current, curveModeRef.current)
          if (!obj) return
          obj.visible = visibleRef.current
          ref.scene.add(obj)
          ref[refKey] = obj
        })
        .catch(() => {})
    }
    fetchLayer('large_airport',  'airportsLargeObj',  airportsLargeDataRef,  showAirportsLargeRef)
    fetchLayer('medium_airport', 'airportsMediumObj', airportsMediumDataRef, showAirportsMediumRef)
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Keep colorModeRef + filter refs in sync for the timelapse RAF loop ─
  useEffect(() => { colorModeRef.current    = effectiveColorMode }, [effectiveColorMode])
  useEffect(() => { tlFilterOpRef.current   = selectedOperator   }, [selectedOperator])
  useEffect(() => { tlFilterTcRef.current   = selectedTypeCode   }, [selectedTypeCode])
  useEffect(() => { tlFilterTgRef.current   = selectedTypeGroup  }, [selectedTypeGroup])
  useEffect(() => { tlFilterMlatRef.current = mlatOnly           }, [mlatOnly])

  // ── Sync coordinate params into refs, bump scene version ──
  // Must update refs BEFORE setSceneVersion so the redrawn scene sees new values.
  const bumpScene = useCallback((newAltScale, newCurveMode) => {
    altScaleRef.current  = newAltScale
    curveModeRef.current = newCurveMode
    setSceneVersion(v => v + 1)
  }, [])

  // ── Extend camera far plane in curved mode (scene extends much further) ─
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return
    curveModeRef.current = curveMode
    ref.camera.far = curveMode ? 20000 : 5000
    ref.camera.updateProjectionMatrix()
  }, [curveMode])

  // ── Rebuild altitude axis when altScale changes ──────────────────────
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref?.altAxisObj) return
    ref.scene.remove(ref.altAxisObj)
    ref.altAxisObj.geometry.dispose()
    ref.altAxisObj.material.dispose()
    const newAxis = makeAltAxis(altScale)
    ref.scene.add(newAxis)
    ref.altAxisObj = newAxis
  }, [altScale])

  // ── Rebuild rings/spokes and coastline when coordinate mode changes ──
  // (sceneVersion bumps whenever altScale or curveMode change)
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return
    const as = altScaleRef.current, cm = curveModeRef.current
    // Rings + spokes
    ref.gridObjs = rebuildGridObjects(ref.scene, ref.gridObjs ?? [], as, cm)
    // Earth sphere — opaque globe in curved mode so objects behind the horizon are occluded
    if (ref.earthObj) {
      ref.scene.remove(ref.earthObj)
      ref.earthObj.geometry.dispose()
      ref.earthObj.material.dispose()
      ref.earthObj = null
    }
    if (cm) {
      ref.earthObj = buildEarthSphere(as)
      ref.scene.add(ref.earthObj)
    }
    // Coastline — rebuild from stored segments so no re-fetch needed
    const segs = coastlineDataRef.current
    if (segs?.length) {
      if (ref.coastlineObj) {
        ref.scene.remove(ref.coastlineObj)
        ref.coastlineObj.geometry.dispose()
        ref.coastlineObj.material.dispose()
      }
      const obj = buildCoastline(segs, as, cm)
      if (obj) {
        obj.visible = showCoastlineRef.current
        ref.scene.add(obj)
        ref.coastlineObj = obj
      }
    }
    // Airports — rebuild so positions follow curved/flat coordinate change
    function rebuildAirportLayer(refKey, dataRef, visRef) {
      const existing = ref[refKey]
      if (existing) {
        ref.scene.remove(existing)
        existing.traverse(child => {
          if (child.geometry) child.geometry.dispose()
          if (child.material) { child.material.map?.dispose(); child.material.dispose() }
        })
      }
      const airports = dataRef.current
      if (airports?.length) {
        const obj = buildAirports(airports, as, cm)
        if (obj) { obj.visible = visRef.current; ref.scene.add(obj); ref[refKey] = obj }
      }
    }
    rebuildAirportLayer('airportsLargeObj',  airportsLargeDataRef,  showAirportsLargeRef)
    rebuildAirportLayer('airportsMediumObj', airportsMediumDataRef, showAirportsMediumRef)
  }, [sceneVersion])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Terrain: fetch LOD zones when showTerrain toggles or scene redraws ─
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return
    // Dispose all existing terrain meshes tracked from previous runs
    for (const obj of ref.terrainObjs) {
      ref.scene.remove(obj)
      obj.geometry.dispose()
      // traverse disposes child materials too (e.g. depthMesh's MeshBasicMaterial)
      obj.traverse(child => { if (child.material) child.material.dispose() })
    }
    ref.terrainObjs = []
    if (!showTerrain) {
      terrainCenterElevMRef.current = 0
      return
    }

    // Single uniform grid — consistent resolution across full 400nm radius
    let cancelled = false
    const localMeshes = []   // meshes added by this effect run, for cleanup on cancel
    ;(async () => {
      setTerrainLoading(true)
      try {
        const gridN = terrainHiRes ? 2048 : 512
        const resp = await fetch(`/api/terrain/grid?radius_nm=400&grid_n=${gridN}&min_radius_nm=0`)
        if (!resp.ok || cancelled) return
        const buf = await resp.arrayBuffer()
        if (cancelled) return
        const r2 = sceneRef.current
        if (!r2) return
        const data = {
          grid_n:    parseInt(resp.headers.get('X-Grid-N')),
          step_nm:   parseFloat(resp.headers.get('X-Step-Nm')),
          elevations: new Int16Array(buf),
        }
        // Store center-cell elevation for receiver-view camera floor
        const ci = Math.floor(data.grid_n / 2) * data.grid_n + Math.floor(data.grid_n / 2)
        terrainCenterElevMRef.current = Math.max(0, data.elevations[ci] ?? 0)
        const mesh = buildTerrainMesh(data, altScaleRef.current, curveModeRef.current, altScaleRef.current)
        mesh.material.wireframe = terrainWireRef.current
        r2.scene.add(mesh)
        r2.terrainObjs.push(mesh)
        localMeshes.push(mesh)
      } catch {
        // terrain is optional — degrade silently on download failure
      } finally {
        if (!cancelled) setTerrainLoading(false)
      }
    })()
    return () => {
      cancelled = true
      // Remove any meshes added before cancellation
      const r2 = sceneRef.current
      for (const obj of localMeshes) {
        if (r2) r2.scene.remove(obj)
        obj.geometry.dispose()
        obj.traverse(child => { if (child.material) child.material.dispose() })
      }
      if (r2) r2.terrainObjs = r2.terrainObjs.filter(o => !localMeshes.includes(o))
    }
  }, [showTerrain, terrainHiRes, sceneVersion])  // eslint-disable-line react-hooks/exhaustive-deps

  // ── Terrain wireframe toggle (no rebuild needed) ─────────────────────
  useEffect(() => {
    terrainWireRef.current = terrainWire
    const ref = sceneRef.current
    if (!ref) return
    for (const obj of ref.terrainObjs) obj.material.wireframe = terrainWire
  }, [terrainWire])

  // ── Receiver view — fixed camera at receiver, mouse look ─────────────
  // receiverCamRef drives the RAF loop: non-null = receiver view active.
  // The RAF reads it every frame so controls.update() is bypassed entirely —
  // no damping residual can override the camera orientation.
  useEffect(() => {
    const ref = sceneRef.current
    if (!ref) return

    if (!receiverView) {
      receiverCamRef.current = null
      ref.controls.enabled = true
      ref.camera.position.set(0, 90, 260)
      ref.controls.target.set(0, 25, 0)
      ref.controls.update()
      return
    }

    ref.controls.enabled = false
    // Initialise cam state; RAF picks it up next frame
    receiverCamRef.current = {
      az: recvAzRef.current,
      el: recvElRef.current,
    }

    let lastX = 0, lastY = 0, dragging = false
    function onDown(e) { dragging = true; lastX = e.clientX; lastY = e.clientY }
    function onMove(e) {
      if (!dragging) return
      recvAzRef.current = (recvAzRef.current + (e.clientX - lastX) * 0.3) % 360
      recvElRef.current = Math.max(-85, Math.min(85, recvElRef.current - (e.clientY - lastY) * 0.2))
      lastX = e.clientX; lastY = e.clientY
      // Update the ref the RAF reads — camera applied next frame
      receiverCamRef.current = {
        az: recvAzRef.current,
        el: recvElRef.current,
      }
    }
    function onUp() { dragging = false }

    const canvas = ref.renderer.domElement
    canvas.addEventListener('mousedown', onDown)
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup',   onUp)
    return () => {
      canvas.removeEventListener('mousedown', onDown)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup',   onUp)
    }
  }, [receiverView])  // sceneVersion intentionally excluded — RAF reads altScaleRef.current live

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

    const filterOp   = tlFilterOpRef.current
    const filterTc   = tlFilterTcRef.current
    const filterTg   = tlFilterTgRef.current
    const filterMlat = tlFilterMlatRef.current

    for (const track of data.tracks) {
      // Apply active filter — skip non-matching tracks
      if (filterMlat && !track.mlat) continue
      if (filterOp && track.operator !== filterOp) continue
      if (filterTc && track.type_code !== filterTc) continue
      if (filterTg) {
        const tgIdx = TYPE_GROUPS.findIndex(g => g.value === filterTg)
        if (tgIdx >= 0 && track.tg_idx !== tgIdx) continue
      }

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
      const [x, y, z] = toWorld(bearing, range, alt, altScaleRef.current, curveModeRef.current)
      dp[di * 3] = x; dp[di * 3 + 1] = y; dp[di * 3 + 2] = z
      const col = mode === 'altitude' ? altColor(alt) : pointColor(track, mode, tlOperatorsRef.current, tlTypeCodesRef.current)
      dc[di * 3] = col.r; dc[di * 3 + 1] = col.g; dc[di * 3 + 2] = col.b
      di++

      // Trail as LineSegments: lead → dot₁ → dot₂ → … → dotN
      // Seeded from the lead dot position/colour so the first segment connects
      // seamlessly. Each step emits one segment [prev, current]; interior points
      // are not duplicated in memory — they're computed and forwarded as "prev".
      const anchor = Math.floor(dt / TRAIL_INTERVAL_S) * TRAIL_INTERVAL_S
      const acCol  = mode === 'altitude' ? null : pointColor(track, mode, tlOperatorsRef.current, tlTypeCodesRef.current)
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
        const [wx, wy, wz] = toWorld(bD, rD, altD, altScaleRef.current, curveModeRef.current)
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
        // Find the earliest dt across all tracks so the scrubber starts at first data.
        // Avoid Math.min(...spread) — V8 has a ~65k argument limit that large timelapse
        // datasets can exceed, throwing a RangeError.
        const minDt = data.tracks.reduce((min, t) => {
          const ts = t.points[0]?.[0] ?? 0
          return ts < min ? ts : min
        }, data.tracks.length > 0 ? (data.tracks[0].points[0]?.[0] ?? 0) : 0)
        // Compute top-10 operators and type codes from this timelapse window
        const tlOpCounts = {}, tlTcCounts = {}
        for (const t of data.tracks) {
          if (t.operator)  tlOpCounts[t.operator]  = (tlOpCounts[t.operator]  || 0) + 1
          if (t.type_code) tlTcCounts[t.type_code] = (tlTcCounts[t.type_code] || 0) + 1
        }
        tlOperatorsRef.current = Object.entries(tlOpCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([op]) => op)
        tlTypeCodesRef.current = Object.entries(tlTcCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([tc]) => tc)

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
  const redraw = useCallback((points, mode, show) => {
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

    setShownN(points.length)

    if (ref.pointsObj) {
      ref.scene.remove(ref.pointsObj)
      ref.pointsObj.geometry.dispose()
      ref.pointsObj.material.dispose()
    }
    if (points.length === 0) { ref.pointsObj = null; return }
    const obj = buildPoints(points, mode, altScaleRef.current, curveModeRef.current)
    ref.scene.add(obj)
    ref.pointsObj = obj
    if (tlActiveRef.current) obj.visible = false   // keep hidden if timelapse is active
  }, [])

  // ── Fetch when days / maxPoints / militaryOnly / selectedOperator changes ──
  useEffect(() => {
    let cancelled = false
    setLoadingPhase('fetching')
    setError(null)

    const milParam  = militaryOnly ? '&military=true' : ''
    const mlatParam = mlatOnly     ? '&mlat=true'     : ''
    const opParam   = selectedOperator ? `&operator=${encodeURIComponent(selectedOperator)}` : ''
    const icaoParam = selectedIcao     ? `&icao=${encodeURIComponent(selectedIcao)}`         : ''
    let typeParam = ''
    if (selectedTypeCode) {
      typeParam = `&type_codes=${encodeURIComponent(selectedTypeCode)}`
    } else if (selectedTypeGroup) {
      const grp = TYPE_GROUPS.find(g => g.value === selectedTypeGroup)
      if (grp) {
        typeParam = grp.category
          ? `&type_category_prefix=${encodeURIComponent(grp.category)}`
          : `&type_codes=${encodeURIComponent(grp.types.join(','))}`
      }
    }
    fetch(`/api/coverage/points?days=${days}&max_points=${maxPoints}${milParam}${mlatParam}${opParam}${typeParam}${icaoParam}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(({ points, operators: ops, type_codes: tcs, type_groups: tgs }) => {
        if (cancelled) return
        pendingRef.current = { points, ops, tgs }
        setFetchedN(points.length)
        setOperators(ops ?? [])
        setTypeCodes(tcs ?? [])
        // Transition to 'rendering' so React can paint the "Building scene…" message
        // before the synchronous buffer-build blocks the main thread.
        setLoadingPhase('rendering')
      })
      .catch(e => { if (!cancelled) { setLoadingPhase(null); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [days, maxPoints, militaryOnly, mlatOnly, selectedOperator, selectedTypeGroup, selectedTypeCode, selectedIcao])

  // ── After 'rendering' phase is painted, do the expensive buffer build ──
  useEffect(() => {
    if (loadingPhase !== 'rendering') return
    const pending = pendingRef.current
    if (!pending) return

    const id = setTimeout(() => {
      dataRef.current = pending.points
      redraw(pending.points, effectiveColorMode, showMode)
      setLoadingPhase(null)
    }, 30)   // 30 ms gives React time to paint the "Building scene…" message

    return () => clearTimeout(id)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loadingPhase])

  // ── Re-colour / re-filter from stored data (no fetch) ───────────────
  useEffect(() => {
    if (dataRef.current.length) redraw(dataRef.current, effectiveColorMode, showMode)
  }, [colorMode, effectiveColorMode, showMode, sceneVersion, redraw])

  // ── Backfill trailsRef from hires buffer on first load ──────────────
  // Pre-populates trails so the live view shows history from server start,
  // not just from when this browser tab was opened.
  useEffect(() => {
    if (showMode === 'history') return
    const end   = Math.floor(Date.now() / 1000)
    const start = end - 3600   // last hour
    fetch(`/api/coverage/timelapse_hires?start_ts=${start}&end_ts=${end}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data?.tracks?.length) return
        const trails = trailsRef.current
        for (const track of data.tracks) {
          if (!track.points?.length) continue
          const pts = track.points.map(([dt, bearing, range, alt]) => ({
            bearing,
            range,
            alt,
            ts:          start + dt,
            military:    track.military,
            mlat:        track.mlat,
            interesting: track.interesting,
            acas:        false,
            type_code:   track.type_code,
            type_category: null,
            operator:    track.operator,
            lat:         null,
            lon:         null,
          }))
          // Prepend historical points; append any live points already collected
          const live = trails[track.icao]
          if (!live?.length) {
            trails[track.icao] = pts.slice(-MAX_TRAIL_PTS)
          } else {
            // live already has some points — prepend history that predates them
            const liveStart = live[0].ts
            const historical = pts.filter(p => p.ts < liveStart)
            if (historical.length) {
              const merged = [...historical, ...live]
              trails[track.icao] = merged.slice(-MAX_TRAIL_PTS)
            }
          }
        }
      })
      .catch(() => {})  // backfill is best-effort
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

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

    // Filter live trails to match active filters. ICAO is most specific — takes precedence.
    let filteredTrails = trails
    if (selectedIcao) {
      filteredTrails = Object.fromEntries(Object.entries(trails).filter(([icao]) =>
        icao === selectedIcao))
    } else {
    if (mlatOnly) {
      filteredTrails = Object.fromEntries(Object.entries(trails).filter(([, pts]) =>
        pts.length > 0 && pts[pts.length - 1].mlat))
    }
    if (militaryOnly) {
      filteredTrails = Object.fromEntries(Object.entries(filteredTrails).filter(([, pts]) =>
        pts.length > 0 && pts[pts.length - 1].military))
    }
    if (selectedOperator) {
      filteredTrails = Object.fromEntries(Object.entries(trails).filter(([, pts]) =>
        pts.length > 0 && pts[pts.length - 1].operator === selectedOperator))
    } else if (selectedTypeCode) {
      filteredTrails = Object.fromEntries(Object.entries(trails).filter(([, pts]) =>
        pts.length > 0 && pts[pts.length - 1].type_code === selectedTypeCode))
    } else if (selectedTypeGroup) {
      const grp = TYPE_GROUPS.find(g => g.value === selectedTypeGroup)
      if (grp) {
        filteredTrails = Object.fromEntries(Object.entries(trails).filter(([, pts]) => {
          if (!pts.length) return false
          const last = pts[pts.length - 1]
          return grp.category
            ? last.type_category?.startsWith(grp.category)
            : grp.types.includes(last.type_code)
        }))
      }
    }
    }  // end else (non-ICAO filters)

    // Compute top-10 operators and type codes from visible live aircraft for colouring.
    const opCounts = {}, tcCounts = {}
    for (const pts of Object.values(filteredTrails)) {
      if (!pts.length) continue
      const last = pts[pts.length - 1]
      if (last.operator)  opCounts[last.operator]  = (opCounts[last.operator]  || 0) + 1
      if (last.type_code) tcCounts[last.type_code]  = (tcCounts[last.type_code] || 0) + 1
    }
    const liveOps = Object.entries(opCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([op]) => op)
    const liveTcs = Object.entries(tcCounts).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([tc]) => tc)

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
    const mesh = buildTrails(filteredTrails, effectiveColorMode, liveOps, liveTcs, altScaleRef.current, curveModeRef.current)
    if (mesh) { scene.add(mesh); ref.trailsObj = mesh; if (tlActiveRef.current) mesh.visible = false }
    const dots = buildLiveDots(filteredTrails, effectiveColorMode, liveOps, liveTcs, altScaleRef.current, curveModeRef.current)
    if (dots) { scene.add(dots); ref.liveDotsObj = dots; if (tlActiveRef.current) dots.visible = false }
  }, [aircraft, showMode, colorMode, effectiveColorMode, militaryOnly, selectedIcao, selectedOperator, selectedTypeGroup, selectedTypeCode, sceneVersion])

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

  // ── Load full operator + type code lists once for autocomplete datalists ─
  useEffect(() => {
    fetch('/api/history/heatmap/options')
      .then(r => r.json())
      .then(d => {
        allOperatorsRef.current = [...d.operators].sort()
        allTypeCodesRef.current = [...d.types].sort()
      })
      .catch(() => {})
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

        {/* Colour mode */}
        <div className={styles.controls}>
          <button className={colorMode === 'type_group' ? styles.btnActive : styles.btn} onClick={() => setColorMode('type_group')}>By type</button>
          <button className={colorMode === 'altitude'   ? styles.btnActive : styles.btn} onClick={() => setColorMode('altitude')}>By altitude</button>
          <button className={colorMode === 'operator'   ? styles.btnActive : styles.btn} onClick={() => setColorMode('operator')}>By operator</button>
          <button className={colorMode === 'tag'        ? styles.btnActive : styles.btn} onClick={() => setColorMode('tag')}>By tag</button>
        </div>

        {/* Type filters — visible when "By type" colour mode is active */}
        {colorMode === 'type_group' && (<>
          {(selectedTypeGroup || selectedTypeCode) ? (
            <span className={styles.filterLabel}>
              {selectedTypeGroup
                ? TYPE_GROUPS.find(g => g.value === selectedTypeGroup)?.label
                : selectedTypeCode}
            </span>
          ) : (<>
            <select
              className={styles.searchInput}
              value={selectedTypeGroup}
              onChange={e => { setSelectedTypeGroup(e.target.value); setSelectedTypeCode(''); setTcInput('') }}
            >
              <option value="">— Type group —</option>
              {TYPE_GROUPS.map(g => <option key={g.value} value={g.value}>{g.label}</option>)}
            </select>
            <div className={styles.searchGroup}>
              <input
                className={styles.searchInput}
                list="cov-tc-list"
                placeholder="Filter type…"
                value={tcInput}
                onChange={e => setTcInput(e.target.value)}
                onBlur={e  => { const v = e.target.value.trim().toUpperCase(); if (v) { setSelectedTypeCode(v); setSelectedTypeGroup(''); setTcInput('') } }}
                onKeyDown={e => { if (e.key === 'Enter') { const v = tcInput.trim().toUpperCase(); if (v) { setSelectedTypeCode(v); setSelectedTypeGroup(''); setTcInput('') } } }}
              />
              <datalist id="cov-tc-list">
                {allTypeCodesRef.current.map(t => <option key={t} value={t} />)}
              </datalist>
            </div>
          </>)}
          {(selectedTypeGroup || selectedTypeCode) && (
            <button className={styles.clearBtn} onClick={() => { setSelectedTypeGroup(''); setSelectedTypeCode(''); setTcInput('') }}>Clear</button>
          )}
        </>)}

        {/* Operator filter — visible when "By operator" colour mode is active */}
        {colorMode === 'operator' && (<>
          {selectedOperator
            ? <span className={styles.filterLabel}>Operator: {selectedOperator}</span>
            : (
              <div className={styles.searchGroup}>
                <input
                  className={styles.searchInput}
                  list="cov-op-list"
                  placeholder="Filter operator…"
                  value={opInput}
                  onChange={e => setOpInput(e.target.value)}
                  onBlur={e  => { const v = e.target.value.trim(); if (v) { setSelectedOperator(v); setOpInput('') } }}
                  onKeyDown={e => { if (e.key === 'Enter') { const v = opInput.trim(); if (v) { setSelectedOperator(v); setOpInput('') } } }}
                />
                <datalist id="cov-op-list">
                  {allOperatorsRef.current.map(o => <option key={o} value={o} />)}
                </datalist>
              </div>
            )
          }
          {selectedOperator && (
            <button className={styles.clearBtn} onClick={() => { setSelectedOperator(''); setOpInput('') }}>Clear</button>
          )}
        </>)}

        <div className={styles.sep} />

        {/* Show mode */}
        <div className={styles.controls}>
          <button className={showMode === 'both'    ? styles.btnActive : styles.btn} onClick={() => setShowMode('both')}>Both</button>
          <button className={showMode === 'history' ? styles.btnActive : styles.btn} onClick={() => setShowMode('history')}>History</button>
          <button className={showMode === 'live'    ? styles.btnActive : styles.btn} onClick={() => setShowMode('live')}>Live</button>
        </div>

        {/* Filter: military / mlat requery */}
        <button className={militaryOnly ? styles.btnActive : styles.btn}
          onClick={() => setMilitaryOnly(v => !v)}>Military</button>
        <button className={mlatOnly ? styles.btnActive : styles.btn}
          onClick={() => setMlatOnly(v => !v)}>MLAT</button>

        {/* Filter: single ICAO */}
        {selectedIcao
          ? <span className={styles.filterLabel}>{selectedIcao}</span>
          : (
            <input
              className={styles.searchInput}
              placeholder="ICAO…"
              maxLength={6}
              value={icaoInput}
              onChange={e => setIcaoInput(e.target.value.toUpperCase())}
              onBlur={e  => { const v = e.target.value.trim().toUpperCase(); if (v) { setSelectedIcao(v); setIcaoInput('') } }}
              onKeyDown={e => { if (e.key === 'Enter') { const v = icaoInput.trim().toUpperCase(); if (v) { setSelectedIcao(v); setIcaoInput('') } } }}
            />
          )
        }
        {selectedIcao && (
          <button className={styles.clearBtn} onClick={() => { setSelectedIcao(''); setIcaoInput('') }}>Clear</button>
        )}
        <div className={styles.sep} />

        <button className={styles.resetBtn} onClick={resetCamera}>Reset view</button>
        <button className={showCoastline ? styles.btnActive : styles.btn} onClick={toggleCoastline}>Coastline</button>
        <button className={showAirportsLarge  ? styles.btnActive : styles.btn} onClick={toggleAirportsLarge}>Airports</button>
        <button className={showAirportsMedium ? styles.btnActive : styles.btn} onClick={toggleAirportsMedium}>Med</button>
        <div className={styles.sep} />

        {/* Terrain — hidden when TERRAIN_ENABLED=false on the server */}
        {terrainEnabled && (<>
          <button className={showTerrain ? styles.btnActive : styles.btn}
            onClick={() => setShowTerrain(v => !v)}>Terrain</button>
          {showTerrain && (<>
            <button className={terrainWire ? styles.btnActive : styles.btn}
              onClick={() => setTerrainWire(v => !v)}>Wire</button>
            <button className={terrainHiRes ? styles.btnActive : styles.btn}
              onClick={() => setTerrainHiRes(v => !v)}>Hi-res</button>
          </>)}
        </>)}
        <div className={styles.sep} />

        {/* Curved Earth */}
        <button className={curveMode ? styles.btnActive : styles.btn}
          onClick={() => {
            const next = !curveMode
            setCurveMode(next)
            bumpScene(altScaleRef.current, next)
          }}>Curved</button>
        <div className={styles.sep} />

        {/* Altitude scale slider */}
        <span className={styles.sliderLabel}>Alt ×{altScale}</span>
        <input type="range" className={styles.sceneSlider}
          min="1" max="20" step="1" value={altScale}
          onChange={e => {
            const v = +e.target.value
            setAltScale(v)
            bumpScene(v, curveModeRef.current)
          }} />

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

        {terrainLoading && (
          <div className={styles.terrainBadge}>
            <div className={styles.terrainSpinner} />
            <span>{terrainHiRes
              ? 'Building hi-res terrain — first run may take several minutes…'
              : 'Loading terrain…'}
            </span>
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
        {isTgMode && !(selectedTypeGroup || selectedTypeCode) && !(militaryOnly || mlatOnly) ? (
          <>
            {TYPE_GROUPS.map((g) => (
              <span key={g.value} className={styles.legendItem}>
                <span className={styles.dot} style={{ background: g.color }} />{g.label}
              </span>
            ))}
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: TYPE_GROUP_OTHER_COLOR }} />Other</span>
          </>
        ) : isTgMode && (militaryOnly || mlatOnly) && !(selectedTypeGroup || selectedTypeCode) ? (
          // Military/MLAT filter active — legend shows type codes (effectiveColorMode = 'type_code')
          <>
            {typeCodes.map((tc, i) => (
              <span key={tc} className={styles.legendItem}>
                <span className={styles.dot} style={{ background: NAMED_PALETTE[i] }} />{tc}
              </span>
            ))}
            {typeCodes.length > 0 && (
              <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#484f58' }} />Other</span>
            )}
          </>
        ) : isTgMode && (selectedTypeGroup || selectedTypeCode) ? (
          // Type filter active — legend shows operators (effectiveColorMode = 'operator')
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
        ) : isTagMode ? (
          <>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#bc8cff' }} />Military</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#d29922' }} />Interesting</span>
            <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#3fb950' }} />Standard</span>
          </>
        ) : isOpMode ? (
          <>
            {selectedOperator ? (
              // Operator selected → legend shows type codes (fleet composition)
              <>
                {typeCodes.map((tc, i) => (
                  <span key={tc} className={styles.legendItem}>
                    <span className={styles.dot} style={{ background: NAMED_PALETTE[i] }} />{tc}
                  </span>
                ))}
                {typeCodes.length > 0 && (
                  <span className={styles.legendItem}><span className={styles.dot} style={{ background: '#484f58' }} />Other</span>
                )}
              </>
            ) : (
              // No operator selected → legend shows operator names
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
          rings = 50 nm · vertical ×{altScale}{curveMode ? ' · curved' : ''} · trails = live aircraft
        </span>
      </div>
    </div>
  )
}
