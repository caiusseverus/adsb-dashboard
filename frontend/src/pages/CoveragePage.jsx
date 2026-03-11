import { useEffect, useRef, useState, useCallback } from 'react'
import * as THREE from 'three'
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js'
import styles from './CoveragePage.module.css'
import { NAMED_PALETTE, TYPE_GROUPS, TYPE_GROUP_OTHER_COLOR } from '../utils/typeGroups'

const VERT_EXAG     = 8
const FEET_PER_NM   = 6076.115
const ALT_SCALE_FT  = 45000   // fixed colour scale ceiling — not data-driven

// Tag colours — same palette as AircraftTable badges
const C_MILITARY    = new THREE.Color(0xbc8cff)
const C_INTERESTING = new THREE.Color(0xd29922)
const C_STANDARD    = new THREE.Color(0x3fb950)

// Operator palette — 10 distinct colours + dim grey for "other"
const C_OP_PALETTE   = NAMED_PALETTE.map(h => new THREE.Color(h))
const C_OP_OTHER     = new THREE.Color(0x484f58)

// Type group palette — one colour per group (0–7), plus other
const C_TG_PALETTE   = TYPE_GROUPS.map(g => new THREE.Color(g.color))
const C_TG_OTHER     = new THREE.Color(TYPE_GROUP_OTHER_COLOR)

// Altitude gradient stops: green → yellow → blue → purple
// Scale is fixed 0–ALT_SCALE_FT so colours are consistent across datasets.
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
    range_nm * Math.cos(rad),
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
      // tag mode: military / interesting / standard
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
    sizeAttenuation: false,   // fixed screen-space size — stays sharp when zooming in
    transparent:     true,
    opacity:         0.8,
  }))
}

// Ground-plane range ring
function makeRing(radius) {
  const pts = []
  for (let i = 0; i <= 128; i++) {
    const a = (i / 128) * Math.PI * 2
    pts.push(new THREE.Vector3(Math.sin(a) * radius, 0, Math.cos(a) * radius))
  }
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints(pts),
    new THREE.LineBasicMaterial({ color: 0x21262d }),
  )
}

// Cardinal spoke
function makeSpoke(bearing_deg, length) {
  const rad = bearing_deg * Math.PI / 180
  return new THREE.Line(
    new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(0, 0, 0),
      new THREE.Vector3(Math.sin(rad) * length, 0, Math.cos(rad) * length),
    ]),
    new THREE.LineBasicMaterial({ color: 0x21262d }),
  )
}

// Canvas-texture sprite for compass labels
function makeLabel(text, x, z) {
  const c = document.createElement('canvas')
  c.width = 64; c.height = 32
  const ctx = c.getContext('2d')
  ctx.font = 'bold 18px monospace'
  ctx.fillStyle = '#484f58'
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.fillText(text, 32, 16)
  const sprite = new THREE.Sprite(
    new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(c), transparent: true }),
  )
  sprite.position.set(x, 1, z)
  sprite.scale.set(14, 7, 1)
  return sprite
}

const MAX_POINT_OPTIONS = [50000, 100000, 200000, 500000]
const DEFAULT_MAX_POINTS = 100000

export default function CoveragePage() {
  const mountRef  = useRef(null)
  const sceneRef  = useRef(null)   // { scene, camera, renderer, controls, pointsObj }
  const dataRef   = useRef([])     // raw fetched points (unfiltered)

  const [days,       setDays]       = useState(30)
  const [maxPoints,  setMaxPoints]  = useState(DEFAULT_MAX_POINTS)
  const [colorMode,  setColorMode]  = useState('type_group')
  const [typeFilter, setTypeFilter] = useState('all')  // 'all' | 'military' | 'interesting'
  const [loading,    setLoading]    = useState(true)
  const [error,      setError]      = useState(null)
  const [fetchedN,   setFetchedN]   = useState(0)
  const [shownN,     setShownN]     = useState(0)
  const [operators,  setOperators]  = useState([])   // top-10 operator names for legend
  const [typeGroups, setTypeGroups] = useState([])   // type group labels from API

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
      new THREE.LineBasicMaterial({ color: 0x30363d }),
    ))

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

    sceneRef.current = { scene, camera, renderer, controls, pointsObj: null }

    return () => {
      cancelAnimationFrame(rafId)
      window.removeEventListener('resize', onResize)
      renderer.dispose()
      if (container.contains(renderer.domElement)) container.removeChild(renderer.domElement)
      sceneRef.current = null
    }
  }, [])

  // ── Replace the point cloud in the scene ────────────────────────────
  const redraw = useCallback((points, mode, filter) => {
    const ref = sceneRef.current
    if (!ref) return

    // Apply type filter
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
    setLoading(true)
    setError(null)

    fetch(`/api/coverage/points?days=${days}&max_points=${maxPoints}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(({ points, operators: ops, type_groups: tgs }) => {
        if (cancelled) return
        dataRef.current = points
        setOperators(ops ?? [])
        setTypeGroups(tgs ?? TYPE_GROUPS.map(g => g.label))
        setFetchedN(points.length)
        setLoading(false)
        redraw(points, colorMode, typeFilter)
      })
      .catch(e => { if (!cancelled) { setLoading(false); setError(e.message) } })

    return () => { cancelled = true }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [days, maxPoints])

  // ── Re-colour / re-filter from stored data (no fetch) ───────────────
  useEffect(() => {
    if (dataRef.current.length) redraw(dataRef.current, colorMode, typeFilter)
  }, [colorMode, typeFilter, redraw])

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

  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        <h2 className={styles.title}>3D Coverage</h2>

        {/* Days */}
        <div className={styles.controls}>
          {[7, 30, 90].map(d => (
            <button key={d} className={days === d ? styles.btnActive : styles.btn} onClick={() => setDays(d)}>{d}d</button>
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

        <button className={styles.resetBtn} onClick={resetCamera}>Reset view</button>
      </div>

      {!loading && !error && (
        <div className={styles.meta}>
          {shownN.toLocaleString()} points shown
          {shownN !== fetchedN && ` (${fetchedN.toLocaleString()} fetched)`}
          {' · drag to rotate · scroll to zoom · right-drag to pan'}
        </div>
      )}

      <div className={styles.canvasWrap} ref={mountRef}>
        {(loading || error) && (
          <div className={styles.overlay}>
            {error ? `Error: ${error}` : 'Loading…'}
          </div>
        )}
      </div>

      <div className={styles.legend}>
        {isTgMode ? (
          <>
            {TYPE_GROUPS.map((g, i) => (
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
        <span className={styles.legendItem} style={{ marginLeft: 'auto', color: '#30363d' }}>
          N = top of scene · rings = 50 nm · vertical ×{VERT_EXAG}
        </span>
      </div>
    </div>
  )
}
