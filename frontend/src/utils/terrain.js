/**
 * Terrain mesh builder for the 3D coverage scene.
 *
 * Converts an SRTM elevation grid (from /api/terrain/grid) into a Three.js
 * Mesh using the same coordinate conventions as CoveragePage:
 *   - X axis  = east  (positive east)
 *   - Z axis  = south (positive south, i.e. North = −Z)
 *   - Y axis  = up    (elevation * altScale, in scene NM units)
 *
 * Both flat and curved Earth modes are supported. The mode is selected by the
 * curveMode parameter; k is the curvature exaggeration factor (R_eff = R / k).
 */

import * as THREE from 'three'

const R_NM       = 3440.065  // Earth radius in nautical miles
const M_PER_NM   = 1852.0    // exact

// Elevation colour ramp: [elev_m, r, g, b] — dark background palette
const _ELEV_STOPS = [
  [   0, 0.07, 0.18, 0.10],   // sea level: very dark green
  [ 150, 0.12, 0.30, 0.14],   // lowland
  [ 400, 0.22, 0.40, 0.18],   // rolling hills
  [ 800, 0.38, 0.40, 0.16],   // upland: olive
  [1500, 0.50, 0.38, 0.20],   // highland: brown
  [3000, 0.65, 0.55, 0.42],   // mountain: tan
  [5000, 0.82, 0.80, 0.78],   // summit: near-white
]

function _elevColor(elev_m) {
  const s = _ELEV_STOPS
  if (elev_m <= s[0][0]) return [s[0][1], s[0][2], s[0][3]]
  for (let i = 1; i < s.length; i++) {
    if (elev_m <= s[i][0]) {
      const t = (elev_m - s[i-1][0]) / (s[i][0] - s[i-1][0])
      return [
        s[i-1][1] + t * (s[i][1] - s[i-1][1]),
        s[i-1][2] + t * (s[i][2] - s[i-1][2]),
        s[i-1][3] + t * (s[i][3] - s[i-1][3]),
      ]
    }
  }
  return [s[s.length-1][1], s[s.length-1][2], s[s.length-1][3]]
}


/** Flat-Earth vertex: (x_nm, z_nm, elev_nm) → world [x, y, z] */
function _flatPos(x, z, elev_nm, s) {
  return [x, elev_nm * s, z]
}

/**
 * Curved-Earth vertex using spherical ECEF rebased to receiver.
 * Receiver surface = y=0; r = R_eff + elev_nm * s before unit-vector multiply.
 */
function _curvedPos(x, z, elev_nm, s, k) {
  const range_nm = Math.sqrt(x * x + z * z)
  // At the receiver origin, place point at (0, elev*s, 0)
  if (range_nm < 1e-6) return [0, elev_nm * s, 0]

  // bearing = atan2(x_east, -z_north) — matches CoveragePage toWorldCurved
  const bearing_rad = Math.atan2(x, -z)
  const R_eff = R_NM / k
  const gc_rad = range_nm / R_eff
  const sin_gc = Math.sin(gc_rad)
  const cos_gc = Math.cos(gc_rad)
  const r = R_eff + elev_nm * s

  return [
    r * sin_gc * Math.sin(bearing_rad),
    r * cos_gc - R_eff,            // receiver surface = y=0
    -r * sin_gc * Math.cos(bearing_rad),
  ]
}


/**
 * Build a Three.js Mesh for one terrain grid zone.
 *
 * @param {object} data      Response from /api/terrain/grid
 * @param {number} s         Altitude scale factor (same as altScale in scene)
 * @param {boolean} curveMode  true = spherical ECEF, false = flat ENU
 * @param {number} k         Curvature exaggeration (only used in curveMode)
 * @returns {THREE.Mesh}
 */
export function buildTerrainMesh(data, s, curveMode = false, k = 5) {
  const { grid_n, step_nm, elevations } = data
  const half = (grid_n - 1) / 2
  const nVerts = grid_n * grid_n

  const positions = new Float32Array(nVerts * 3)
  const colors    = new Float32Array(nVerts * 3)

  for (let row = 0; row < grid_n; row++) {
    for (let col = 0; col < grid_n; col++) {
      // Scene coordinates: east = +X, north = −Z (row 0 = north)
      const x       = (col - half) * step_nm
      const z       = (row - half) * step_nm   // row 0 → negative z (north)
      const raw     = elevations[row * grid_n + col]
      const isOcean = raw < 0
      const elev_m  = isOcean ? 0 : raw
      const elev_nm = elev_m / M_PER_NM

      const [px, py, pz] = curveMode
        ? _curvedPos(x, z, elev_nm, s, k)
        : _flatPos(x, z, elev_nm, s)

      const vi = (row * grid_n + col) * 3
      positions[vi]     = px
      positions[vi + 1] = py
      positions[vi + 2] = pz

      const [cr, cg, cb] = isOcean ? [0, 0, 0] : _elevColor(elev_m)
      colors[vi]     = cr
      colors[vi + 1] = cg
      colors[vi + 2] = cb
    }
  }

  // Triangle indices for an (n-1)×(n-1) grid of quads
  const nQuads = (grid_n - 1) * (grid_n - 1)
  const indices = new Uint32Array(nQuads * 6)
  let ii = 0
  for (let row = 0; row < grid_n - 1; row++) {
    for (let col = 0; col < grid_n - 1; col++) {
      const tl = row * grid_n + col
      const tr = tl + 1
      const bl = tl + grid_n
      const br = bl + 1
      // Two CCW triangles per quad
      indices[ii++] = tl; indices[ii++] = bl; indices[ii++] = tr
      indices[ii++] = tr; indices[ii++] = bl; indices[ii++] = br
    }
  }

  const geo = new THREE.BufferGeometry()
  geo.setAttribute('position', new THREE.BufferAttribute(positions, 3))
  geo.setAttribute('color',    new THREE.BufferAttribute(colors,    3))
  geo.setIndex(new THREE.BufferAttribute(indices, 1))
  geo.computeVertexNormals()

  const mat = new THREE.MeshLambertMaterial({
    vertexColors: true,
    side: THREE.FrontSide,
    wireframe: false,
  })

  const mesh = new THREE.Mesh(geo, mat)

  // Depth-only companion: writes depth buffer without writing colour.
  // Ensures terrain occludes aircraft/grid even in wireframe mode (where the
  // colour mesh leaves gaps between wires that don't write depth).
  const depthMesh = new THREE.Mesh(geo, new THREE.MeshBasicMaterial({
    colorWrite: false,
    depthWrite: true,
    side: THREE.FrontSide,
  }))
  mesh.add(depthMesh)  // child of colour mesh — moves with it, no extra geometry memory

  return mesh
}
