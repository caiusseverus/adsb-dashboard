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

function bearingDeltaDeg(a, b) {
  const d = Math.abs((a ?? 0) - (b ?? 0)) % 360
  return d > 180 ? 360 - d : d
}

function isTrailSegmentValid(a, b) {
  if (b.ts - a.ts > MAX_TRAIL_GAP_S) return false
  if (bearingDeltaDeg(a.bearing, b.bearing) > MAX_BEARING_JUMP) return false
  if (Math.abs((b.range ?? 0) - (a.range ?? 0)) > MAX_RANGE_JUMP_NM) return false
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
  const [typeGroups,   setTypeGroups]   = useState([])

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

    sceneRef.current = { scene, camera, renderer, controls, pointsObj: null, trailsObj: null, liveDotsObj: null }

    return () => {
      cancelAnimationFrame(rafId)
      window.removeEventListener('resize', onResize)
      controls.removeEventListener('change', updateCompass)
      renderer.dispose()
      if (container.contains(renderer.domElement)) container.removeChild(renderer.domElement)
      sceneRef.current = null
    }
  }, [])

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
        setTypeGroups(tgs ?? TYPE_GROUPS.map(g => g.label))
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
    for (const ac of aircraft) {
      if (ac.bearing_deg == null || ac.range_nm == null || ac.altitude == null) continue
      if (!trails[ac.icao]) trails[ac.icao] = []
      const trail = trails[ac.icao]
      const last  = trail[trail.length - 1]
      // Skip if position hasn't moved meaningfully (avoids duplicate points for stationary aircraft)
      if (last &&
          Math.abs(last.bearing - ac.bearing_deg) < 0.05 &&
          Math.abs(last.range   - ac.range_nm)    < 0.05) continue
      trail.push({
        bearing:       ac.bearing_deg,
        range:         ac.range_nm,
        alt:           ac.altitude,
        ts:            Date.now() / 1000,
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
    if (mesh) { scene.add(mesh); ref.trailsObj = mesh }
    const dots = buildLiveDots(trails, colorMode, operators)
    if (dots) { scene.add(dots); ref.liveDotsObj = dots }
  }, [aircraft, showMode, colorMode, operators])

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
      </div>

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
