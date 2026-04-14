/**
 * Stage 3 Aircraft Localisation — full-size map page.
 *
 * Shows radar origins, calibrated bearing rays, pairwise-intersection seeds,
 * the selected aircraft fix, and a CEP uncertainty circle for any chosen
 * aircraft target.
 *
 * Controls:
 *   - Aircraft picker (from live snapshot, filterable)
 *   - Radar multi-select chips (each chip shows IID + calibration quality)
 *   - View toggles (rays / seeds / CEP / rejected)
 *   - Status strip: enabled, calibrated radars, active tracks, backlog
 *   - Manual trigger buttons: calibrate, localise, reset tracks
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import styles from './Stage3LocalisationPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

// ── Colours ───────────────────────────────────────────────────────────────────
const COLOUR = {
  radarOrigin:       '#bc8cff',  // purple
  radarOriginInact:  '#484f58',  // grey (calibrated but not selected)
  bearingRay:        '#388bfd',  // blue
  rejectedRay:       '#da3633',  // red
  seedIntersection:  '#d29922',  // gold
  selectedFix:       '#3fb950',  // green
  cepCircle:         '#3fb95044',
  cepBorder:         '#3fb950',
  truthAdsb:         '#8b949e',  // grey — reference/truth dot
}

// ── Earth radius in metres (for CEP circle rendering) ─────────────────────────
const R_EARTH_M = 6_371_000

// ── Basemaps (same as RadarPage) ──────────────────────────────────────────────
const BASEMAPS = {
  dark: {
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd', maxZoom: 19,
  },
  voyager: {
    url: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd', maxZoom: 19,
  },
  osm: {
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
    maxZoom: 19,
  },
}

/**
 * Render a CEP circle as a polygon (flat equirectangular projection) so
 * it matches what the backend computes — same approach as RadarPage.jsx.
 */
function cepCircleLatLngs(centerLat, centerLon, radiusM, n = 64) {
  const points = []
  for (let i = 0; i < n; i++) {
    const angle = (i / n) * 2 * Math.PI
    const dlat = (radiusM * Math.cos(angle)) / R_EARTH_M
    const dlon = (radiusM * Math.sin(angle)) / (R_EARTH_M * Math.cos(Math.PI / 180 * centerLat))
    points.push([
      centerLat + dlat * (180 / Math.PI),
      centerLon + dlon * (180 / Math.PI),
    ])
  }
  points.push(points[0])
  return points
}

// ── Ray endpoint computation ───────────────────────────────────────────────────
function bearingRayEndpoint(lat, lon, bearingDeg, distM) {
  const dr = distM / R_EARTH_M
  const br = bearingDeg * Math.PI / 180
  const lat1 = lat * Math.PI / 180
  const lon1 = lon * Math.PI / 180
  const lat2 = Math.asin(Math.sin(lat1) * Math.cos(dr) + Math.cos(lat1) * Math.sin(dr) * Math.cos(br))
  const lon2 = lon1 + Math.atan2(
    Math.sin(br) * Math.sin(dr) * Math.cos(lat1),
    Math.cos(dr) - Math.sin(lat1) * Math.sin(lat2),
  )
  return [lat2 * 180 / Math.PI, lon2 * 180 / Math.PI]
}

// ── Quality badge ─────────────────────────────────────────────────────────────
const QUALITY_DOT = { stable: '●', provisional: '◉', none: '○' }
const QUALITY_CHIP_CLASS = { stable: styles.radarChipStable, provisional: styles.radarChipProvisional, none: styles.radarChipNone }

// =============================================================================
export default function Stage3LocalisationPage({ snapshot }) {
  const mapDivRef = useRef(null)
  const mapRef = useRef(null)
  const baseTileRef = useRef(null)
  const layerGroupRef = useRef(null)

  // UI state
  const [targetIcao, setTargetIcao] = useState(null)
  const [filter, setFilter] = useState('')
  const [activeIids, setActiveIids] = useState(new Set())     // selected radar IIDs
  const [showRays, setShowRays] = useState(true)
  const [showSeeds, setShowSeeds] = useState(true)
  const [showCep, setShowCep] = useState(true)
  const [showRejected, setShowRejected] = useState(false)
  const [basemap, setBasemap] = useState('dark')

  // Data from backend
  const [status, setStatus] = useState(null)
  const [radars, setRadars] = useState([])
  const [evidence, setEvidence] = useState(null)
  const [actionFeedback, setActionFeedback] = useState('')

  // ── Map initialisation ─────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapDivRef.current || mapRef.current) return

    const map = L.map(mapDivRef.current, {
      center: [51.5, -0.1],
      zoom: 7,
      zoomControl: true,
      attributionControl: true,
    })

    const bm = BASEMAPS.dark
    baseTileRef.current = L.tileLayer(bm.url, {
      attribution: bm.attribution,
      subdomains: bm.subdomains || 'abc',
      maxZoom: bm.maxZoom,
    }).addTo(map)

    layerGroupRef.current = L.layerGroup().addTo(map)
    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  // ── Basemap switch ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!mapRef.current || !baseTileRef.current) return
    const bm = BASEMAPS[basemap] || BASEMAPS.dark
    baseTileRef.current.setUrl(bm.url)
  }, [basemap])

  // ── Status polling (every 5 s) ─────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/aircraft/status`)
        if (r.ok && !cancelled) setStatus(await r.json())
      } catch (_) {}
    }
    poll()
    const id = setInterval(poll, 5000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  // ── Radar list polling (every 15 s) ───────────────────────────────────────
  useEffect(() => {
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/aircraft/radars`)
        if (r.ok && !cancelled) {
          const data = await r.json()
          const list = data.radars || []
          setRadars(list)
          // Auto-select all eligible radars on first load
          setActiveIids(prev => {
            if (prev.size === 0) return new Set(list.filter(r => r.stage3_eligible).map(r => r.iid))
            return prev
          })
        }
      } catch (_) {}
    }
    poll()
    const id = setInterval(poll, 15000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  // ── Evidence polling for selected target (every 2 s) ──────────────────────
  useEffect(() => {
    if (!targetIcao) { setEvidence(null); return }
    let cancelled = false

    async function poll() {
      try {
        const iidsParam = activeIids.size > 0 ? `?iids=${[...activeIids].join(',')}` : ''
        const r = await fetch(`${API_BASE}/api/radar/aircraft/tracks/${encodeURIComponent(targetIcao)}/evidence${iidsParam}`)
        if (r.ok && !cancelled) setEvidence(await r.json())
      } catch (_) {}
    }
    poll()
    const id = setInterval(poll, 2000)
    return () => { cancelled = true; clearInterval(id) }
  }, [targetIcao, activeIids])

  // ── Render evidence layers onto the map ───────────────────────────────────
  useEffect(() => {
    const group = layerGroupRef.current
    if (!group) return
    group.clearLayers()
    if (!evidence) return

    const layers = evidence.layers || []

    for (const layer of layers) {
      const { method, features } = layer
      if (!features || features.length === 0) continue

      for (const feat of features) {
        const props = feat.properties || {}
        const geom = feat.geometry

        // Client-side IID filter: skip this feature if its iid is not active
        if (props.iid !== undefined && activeIids.size > 0 && !activeIids.has(props.iid)) {
          // Still show radar origins so user can see all available radars
          if (method !== 'radar_origins') continue
        }

        if (method === 'radar_origins') {
          if (geom?.type === 'Point') {
            const [lon, lat] = geom.coordinates
            const eligible = activeIids.has(props.iid)
            const hasObs = props.has_obs
            L.circleMarker([lat, lon], {
              radius: eligible ? 7 : 4,
              color: eligible ? COLOUR.radarOrigin : COLOUR.radarOriginInact,
              fillColor: eligible ? COLOUR.radarOrigin : COLOUR.radarOriginInact,
              fillOpacity: eligible ? 0.25 : 0.1,
              weight: eligible ? 2 : 1,
            }).bindTooltip(`IID ${props.iid} (${props.quality || 'none'})${hasObs ? ' ✓' : ''}`, {
              permanent: false, direction: 'top',
            }).addTo(group)
          }
        }

        if (method === 'bearing_rays' && showRays) {
          if (geom?.type === 'LineString') {
            const coords = geom.coordinates  // [[lon,lat],[lon,lat]]
            const latlngs = coords.map(([lo, la]) => [la, lo])
            L.polyline(latlngs, {
              color: COLOUR.bearingRay,
              weight: 1.5,
              opacity: 0.7,
              dashArray: null,
            }).bindTooltip(
              `IID ${props.iid} · ${(props.bearing_obs_deg || 0).toFixed(1)}° ±${(props.bearing_sigma_deg || 0).toFixed(1)}°`,
              { permanent: false, direction: 'center' }
            ).addTo(group)
          }
        }

        if (method === 'rejected_rays' && showRejected) {
          if (geom?.type === 'LineString') {
            const coords = geom.coordinates
            const latlngs = coords.map(([lo, la]) => [la, lo])
            L.polyline(latlngs, {
              color: COLOUR.rejectedRay,
              weight: 1,
              opacity: 0.5,
              dashArray: '4 4',
            }).addTo(group)
          }
        }

        if (method === 'seed_intersections' && showSeeds) {
          if (geom?.type === 'Point') {
            const [lon, lat] = geom.coordinates
            L.circleMarker([lat, lon], {
              radius: 5,
              color: COLOUR.seedIntersection,
              fillColor: COLOUR.seedIntersection,
              fillOpacity: 0.5,
              weight: 1.5,
            }).bindTooltip('Seed', { permanent: false, direction: 'top' }).addTo(group)
          }
        }

        if (method === 'selected_fix') {
          if (geom?.type === 'Point') {
            const [lon, lat] = geom.coordinates
            const label = props.label || targetIcao
            const cep = props.cep_m != null ? ` CEP ${props.cep_m.toFixed(0)} m` : ''
            const geo = props.geometry_score != null ? ` geo=${props.geometry_score.toFixed(2)}` : ''
            const n = props.n_radars != null ? ` n=${props.n_radars}` : ''
            L.circleMarker([lat, lon], {
              radius: 9,
              color: COLOUR.selectedFix,
              fillColor: COLOUR.selectedFix,
              fillOpacity: 0.8,
              weight: 2,
            }).bindTooltip(`${label}${cep}${geo}${n}`, { permanent: true, direction: 'top', className: 'stage3-label' })
              .addTo(group)

            // Zoom to fix if map hasn't been zoomed yet by user
            if (mapRef.current) {
              const centre = mapRef.current.getCenter()
              const dist = mapRef.current.distance(centre, [lat, lon])
              if (dist > 500_000) {
                mapRef.current.setView([lat, lon], 8, { animate: true })
              }
            }
          }
        }

        if (method === 'fix_uncertainty' && showCep) {
          if (geom?.type === 'Point' && props.radius_km > 0) {
            const [lon, lat] = geom.coordinates
            const radiusM = props.radius_km * 1000
            const latlngs = cepCircleLatLngs(lat, lon, radiusM)
            L.polygon(latlngs, {
              color: COLOUR.cepBorder,
              fillColor: COLOUR.cepCircle,
              fillOpacity: 0.15,
              weight: 1,
              dashArray: '3 3',
            }).bindTooltip(props.label || 'CEP', { permanent: false, direction: 'top' }).addTo(group)
          }
        }
      }
    }
  }, [evidence, showRays, showSeeds, showCep, showRejected, activeIids, targetIcao])

  // ── Aircraft list from snapshot ────────────────────────────────────────────
  const aircraft = snapshot?.aircraft ?? []
  const filteredAircraft = filter
    ? aircraft.filter(ac =>
        ac.icao?.toLowerCase().includes(filter.toLowerCase()) ||
        (ac.callsign || '').toLowerCase().includes(filter.toLowerCase())
      )
    : aircraft

  // ── Radar chip toggle ─────────────────────────────────────────────────────
  const toggleIid = useCallback((iid) => {
    setActiveIids(prev => {
      const next = new Set(prev)
      if (next.has(iid)) next.delete(iid); else next.add(iid)
      return next
    })
  }, [])

  // ── Action helpers ─────────────────────────────────────────────────────────
  async function doAction(url, label) {
    setActionFeedback(`${label}…`)
    try {
      const r = await fetch(`${API_BASE}${url}`, { method: 'POST' })
      const data = await r.json()
      setActionFeedback(r.ok ? `${label} OK (${data.updated ?? data.n_fixes ?? 0})` : `${label} failed`)
    } catch (_) {
      setActionFeedback(`${label} error`)
    }
    setTimeout(() => setActionFeedback(''), 3000)
  }

  // ── Derived status values ─────────────────────────────────────────────────
  const enabled = status?.enabled ?? false
  const nCalibrated = status?.n_calibrated_radars ?? 0
  const nTracks = status?.n_active_tracks ?? 0
  const backlog = status?.queue_backlog ?? false

  // ============================================================================
  return (
    <div className={styles.page}>

      {/* ── Top toolbar ─────────────────────────────────────────────────── */}
      <div className={styles.toolbar}>
        <span className={styles.toolbarTitle}>Stage 3 · Localisation</span>

        {/* Basemap toggle */}
        <div className={styles.modeGroup}>
          {Object.entries(BASEMAPS).map(([key, bm]) => (
            <button key={key}
              className={basemap === key ? styles.btnActive : styles.btn}
              onClick={() => setBasemap(key)}
            >{key.charAt(0).toUpperCase() + key.slice(1)}</button>
          ))}
        </div>

        {/* View toggles */}
        <div className={styles.modeGroup}>
          <button className={showRays ? styles.btnActive : styles.btn} onClick={() => setShowRays(v => !v)}>Rays</button>
          <button className={showSeeds ? styles.btnActive : styles.btn} onClick={() => setShowSeeds(v => !v)}>Seeds</button>
          <button className={showCep ? styles.btnActive : styles.btn} onClick={() => setShowCep(v => !v)}>CEP</button>
          <button className={showRejected ? styles.btnActive : styles.btn} onClick={() => setShowRejected(v => !v)}>Rejected</button>
        </div>

        {/* Status strip */}
        <div className={styles.statusStrip}>
          {actionFeedback && <span style={{ color: '#d29922', fontSize: '0.72rem' }}>{actionFeedback}</span>}
          <span className={`${styles.statusBadge} ${enabled ? styles.enabled : styles.disabled}`}>
            {enabled ? '● enabled' : '○ disabled'}
          </span>
          <span className={styles.statusBadge}>{nCalibrated} calibrated</span>
          <span className={styles.statusBadge}>{nTracks} tracks</span>
          {backlog && <span className={`${styles.statusBadge} ${styles.backlog}`}>⚠ backlog</span>}
        </div>
      </div>

      <div className={styles.mapArea}>

        {/* ── Left drawer ─────────────────────────────────────────────────── */}
        <div className={styles.drawer}>

          {/* Aircraft picker */}
          <div className={styles.drawerSection} style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <div className={styles.drawerLabel}>Aircraft target</div>
            <input
              className={styles.filterInput}
              placeholder="ICAO / callsign…"
              value={filter}
              onChange={e => setFilter(e.target.value)}
            />
            <div className={styles.aircraftList}>
              {filteredAircraft.length === 0 && (
                <div className={styles.emptyList}>No aircraft</div>
              )}
              {filteredAircraft.map(ac => (
                <div
                  key={ac.icao}
                  className={ac.icao === targetIcao ? styles.aircraftRowSelected : styles.aircraftRow}
                  onClick={() => setTargetIcao(ac.icao === targetIcao ? null : ac.icao)}
                >
                  <span className={styles.aircraftIcao}>{ac.icao}</span>
                  <span className={styles.aircraftCallsign}>{ac.callsign || '—'}</span>
                  {ac.altitude != null && (
                    <span style={{ fontSize: '0.65rem', color: '#484f58' }}>{ac.altitude.toLocaleString()}'</span>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Radar chips */}
          <div className={styles.drawerSection}>
            <div className={styles.drawerLabel}>Radars ({radars.filter(r => r.stage3_eligible).length} eligible)</div>
            <div className={styles.radarChips}>
              {radars.length === 0 && <span className={styles.emptyList}>No resolved radars</span>}
              {radars.map(r => {
                const isActive = activeIids.has(r.iid)
                const qualClass = QUALITY_CHIP_CLASS[r.calibration_quality] || ''
                return (
                  <span
                    key={r.iid}
                    className={`${isActive ? styles.radarChipActive : styles.radarChip} ${qualClass}`}
                    onClick={() => toggleIid(r.iid)}
                    title={`IID ${r.iid} · σ=${r.bearing_sigma_deg?.toFixed(1) ?? '?'}° · ${r.n_calibration_samples ?? 0} samples`}
                  >
                    <span className={styles.qualityDot}>{QUALITY_DOT[r.calibration_quality] || '○'}</span>
                    IID {r.iid}
                  </span>
                )
              })}
            </div>
          </div>

          {/* Actions */}
          <div className={styles.drawerSection}>
            <div className={styles.drawerLabel}>Actions</div>
            <button className={styles.actionBtn} onClick={() => doAction('/api/radar/aircraft/calibration/run', 'Calibrate')}>
              Run calibration
            </button>
            <button className={styles.actionBtn} onClick={() => doAction(`/api/radar/aircraft/tracks/run${targetIcao ? `?icaos=${targetIcao}` : ''}`, 'Localise')}>
              {targetIcao ? `Localise ${targetIcao}` : 'Localise all'}
            </button>
            <button className={`${styles.actionBtn} ${styles.btnDanger}`}
              onClick={() => { if (confirm('Reset all runtime tracks?')) doAction('/api/radar/aircraft/tracks/reset', 'Reset') }}>
              Reset tracks
            </button>
          </div>

          {/* Evidence summary */}
          {targetIcao && evidence && (
            <div className={styles.drawerSection}>
              <div className={styles.drawerLabel}>Fix · {targetIcao}</div>
              <div style={{ fontSize: '0.72rem', color: '#8b949e', lineHeight: 1.6 }}>
                {evidence.available ? (
                  <>
                    <div>Observations: {evidence.n_observations}</div>
                    <div>Radars: {evidence.n_radars}</div>
                    {(() => {
                      const fixLayer = (evidence.layers || []).find(l => l.method === 'selected_fix')
                      const fix = fixLayer?.features?.[0]?.properties
                      if (!fix) return <div style={{ color: '#484f58' }}>No fix yet</div>
                      return <>
                        <div>CEP: {fix.cep_m != null ? `${fix.cep_m.toFixed(0)} m` : '—'}</div>
                        <div>Geo: {fix.geometry_score != null ? fix.geometry_score.toFixed(2) : '—'}</div>
                        <div>Status: {fix.solver_status || '—'}</div>
                      </>
                    })()}
                  </>
                ) : (
                  <span style={{ color: '#484f58' }}>{evidence.reason || 'no data'}</span>
                )}
              </div>
            </div>
          )}
        </div>

        {/* ── Map ───────────────────────────────────────────────────────── */}
        <div className={styles.mapWrap} ref={mapDivRef} />
      </div>

      {/* ── Legend ──────────────────────────────────────────────────────── */}
      <div className={styles.legend}>
        <div className={styles.legendItem}>
          <div className={styles.legendDot} style={{ background: COLOUR.radarOrigin }} />
          Radar origin
        </div>
        <div className={styles.legendItem}>
          <div className={styles.legendLine} style={{ background: COLOUR.bearingRay }} />
          Bearing ray
        </div>
        <div className={styles.legendItem}>
          <div className={styles.legendDot} style={{ background: COLOUR.seedIntersection }} />
          Seed intersection
        </div>
        <div className={styles.legendItem}>
          <div className={styles.legendDot} style={{ background: COLOUR.selectedFix }} />
          Selected fix
        </div>
        <div className={styles.legendItem}>
          <div className={styles.legendDot} style={{ background: COLOUR.cepBorder, opacity: 0.6 }} />
          CEP (50%)
        </div>
        {showRejected && (
          <div className={styles.legendItem}>
            <div className={styles.legendLine} style={{ background: COLOUR.rejectedRay }} />
            Rejected ray
          </div>
        )}
      </div>
    </div>
  )
}
