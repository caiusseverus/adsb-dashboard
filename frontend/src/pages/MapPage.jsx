import { useEffect, useRef, useState, useMemo } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { TYPE_GROUPS, TYPE_GROUP_OTHER_COLOR, typeGroupColor, buildNameColorMap, NAMED_PALETTE } from '../utils/typeGroups'
import styles from './MapPage.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

// ── Altitude colour gradient — matches polar/coverage charts ──────────────
const ALT_STOPS = [
  [0.00, [0x3f, 0xb9, 0x50]],  // #3fb950 green  (low)
  [0.33, [0xd2, 0x99, 0x22]],  // #d29922 gold   (mid)
  [0.67, [0x38, 0x8b, 0xfd]],  // #388bfd blue   (high)
  [1.00, [0xbc, 0x8c, 0xff]],  // #bc8cff purple (very high)
]
const ALT_MAX = 45000

function altColor(alt_ft) {
  if (!alt_ft) return '#8b949e'
  const t = Math.min(1, Math.max(0, alt_ft / ALT_MAX))
  for (let i = 1; i < ALT_STOPS.length; i++) {
    const [t0, c0] = ALT_STOPS[i - 1]
    const [t1, c1] = ALT_STOPS[i]
    if (t <= t1) {
      const f = (t - t0) / (t1 - t0)
      const r = Math.round(c0[0] + f * (c1[0] - c0[0]))
      const g = Math.round(c0[1] + f * (c1[1] - c0[1]))
      const b = Math.round(c0[2] + f * (c1[2] - c0[2]))
      return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`
    }
  }
  return '#bc8cff'
}

// Tags: matches SkyView exactly — civil = #3fb950 as requested
function tagColor(ac) {
  if (ac.squawk === '7700' || ac.squawk === '7600' || ac.squawk === '7500') return '#ff4444'
  if (ac.military)    return '#bc8cff'
  if (ac.mlat)        return '#388bfd'
  if (ac.interesting) return '#d29922'
  return '#3fb950'
}

// ── Aircraft marker icon ──────────────────────────────────────────────────
// Cache keyed by "<color>-<roundedHeading|circle>" — heading quantised to 5°
// so we create at most ~72 icons per colour rather than one per frame.
const _iconCache = new Map()
function makeIcon(color, heading) {
  const headingKey = heading != null ? Math.round(heading / 5) * 5 : 'circle'
  const cacheKey = `${color}-${headingKey}`
  if (_iconCache.has(cacheKey)) return _iconCache.get(cacheKey)

  const hasHeading = headingKey !== 'circle'
  const html = hasHeading
    ? `<svg viewBox="-7 -9 14 18" width="14" height="18" style="display:block;transform:rotate(${headingKey}deg)">
         <path d="M0,-8 L5,7 L0,3 L-5,7 Z" fill="${color}" stroke="#0b0c10" stroke-width="1" stroke-linejoin="round"/>
       </svg>`
    : `<svg viewBox="-5 -5 10 10" width="10" height="10" style="display:block">
         <circle cx="0" cy="0" r="4" fill="${color}" stroke="#0b0c10" stroke-width="1"/>
       </svg>`
  const icon = L.divIcon({
    html,
    className: '',
    iconSize:   hasHeading ? [14, 18] : [10, 10],
    iconAnchor: hasHeading ? [7, 9]   : [5, 5],
  })
  _iconCache.set(cacheKey, icon)
  return icon
}

// Receiver crosshair icon
const RECEIVER_ICON = L.divIcon({
  html: `<svg viewBox="-8 -8 16 16" width="16" height="16" style="display:block">
           <circle cx="0" cy="0" r="5" fill="none" stroke="#388bfd" stroke-width="2"/>
           <circle cx="0" cy="0" r="1.5" fill="#388bfd"/>
           <line x1="0" y1="-8" x2="0" y2="-6" stroke="#388bfd" stroke-width="1.5"/>
           <line x1="0" y1="6"  x2="0" y2="8"  stroke="#388bfd" stroke-width="1.5"/>
           <line x1="-8" y1="0" x2="-6" y2="0" stroke="#388bfd" stroke-width="1.5"/>
           <line x1="6"  y1="0" x2="8"  y2="0" stroke="#388bfd" stroke-width="1.5"/>
         </svg>`,
  className: '',
  iconSize: [16, 16],
  iconAnchor: [8, 8],
})

const COLOR_MODES = [
  { value: 'altitude', label: 'Altitude' },
  { value: 'type',     label: 'Type' },
  { value: 'operator', label: 'Operator' },
  { value: 'country',  label: 'Country' },
  { value: 'tags',     label: 'Tags' },
]

// Distinct colours for per-source MLAT dots — consistent by source name hash
const MLAT_SRC_PALETTE = ['#ff7b00', '#00d4ff', '#ff00cc', '#aaff00', '#ff3366', '#00ffaa', '#ffcc00', '#cc99ff']
function mlatSrcColor(src) {
  let h = 0
  for (const c of src) h = (h * 31 + c.charCodeAt(0)) & 0xffff
  return MLAT_SRC_PALETTE[h % MLAT_SRC_PALETTE.length]
}

// Residual thresholds → colour
function residualColor(nm) {
  if (nm < 0.3) return '#3fb950'   // good
  if (nm < 1.0) return '#e3b341'   // marginal
  return '#f85149'                  // poor
}

// Build two-line HTML label for permanent tooltip
function makeLabelContent(ac) {
  const id = ac.registration || ac.icao.toUpperCase()
  const row1 = [id, ac.type_code].filter(Boolean).join(' ')
  const row2 = [
    ac.airspeed_kts != null ? `${ac.airspeed_kts}kt` : null,
    ac.altitude != null     ? `${ac.altitude.toLocaleString()}ft` : null,
  ].filter(Boolean).join(' ')
  return row2
    ? `<span>${row1}</span><br><span class="ac-label-sub">${row2}</span>`
    : `<span>${row1}</span>`
}


export default function MapPage({ snapshot, onSelectIcao, receiverPos, selectedIcao }) {
  const mapRef            = useRef(null)
  const mountRef          = useRef(null)
  const markersRef        = useRef(new Map())   // icao → L.Marker
  const hiresTrailsRef    = useRef(new Map())   // icao → { line: L.Polyline }
  const hiresPollRef      = useRef(null)
  const labelsRef         = useRef(new Map())   // icao → last label string (for dirty-check)
  const fittedRef         = useRef(false)
  const initCenteredRef   = useRef(false)  // true if map was init'd at receiver pos
  const receiverMarkerRef = useRef(null)
  const residualLayerRef  = useRef([])          // L.CircleMarker[] for residual overlay
  const residualTimerRef  = useRef(null)
  // MLAT source dots: accumulated per aircraft until it leaves sight
  const mlatDotsRef       = useRef(new Map())   // icao → Map<source, L.CircleMarker[]>
  const mlatSeenRef       = useRef(new Map())   // icao → Map<source, Set<"lat,lon">>
  const mlatPollRef       = useRef(null)
  const selectedTrailRef  = useRef([])    // L.Polyline[] — per-segment gradient for selected aircraft

  const [colorMode,       setColorMode]       = useState('altitude')
  const [acCount,         setAcCount]         = useState(0)
  const [showResiduals,   setShowResiduals]   = useState(false)
  const [showMlatSources, setShowMlatSources] = useState(false)
  const [showTrails,      setShowTrails]      = useState(false)
  const [showLabels,      setShowLabels]      = useState(false)

  // ── Init Leaflet (once on mount) ────────────────────────────────────────
  useEffect(() => {
    if (mapRef.current) return
    // Use receiver position if already known (from App-level status fetch) so the
    // map opens centred correctly without a later re-zoom
    const initCenter = receiverPos ?? [51.5, -0.1]
    const initZoom   = receiverPos ? 9 : 8
    if (receiverPos) initCenteredRef.current = true
    const map = L.map(mountRef.current, { center: initCenter, zoom: initZoom, zoomControl: true })
    // Base layers — CartoDB Dark Matter (default) + OpenTopoMap terrain option
    const darkLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors ' +
        '&copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 19,
    })
    const topoLayer = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors ' +
        '&copy; <a href="https://opentopomap.org">OpenTopoMap</a>',
      subdomains: 'abc',
      maxZoom: 17,
    })
    darkLayer.addTo(map)
    L.control.layers({ 'Dark': darkLayer, 'Terrain': topoLayer }, {}, { position: 'topleft' }).addTo(map)
    mapRef.current = map
    return () => {
      map.remove()
      mapRef.current = null
      markersRef.current.clear()
      hiresTrailsRef.current.clear()
      labelsRef.current.clear()
      fittedRef.current = false
      initCenteredRef.current = false
      residualLayerRef.current = []
      clearInterval(residualTimerRef.current)
      mlatDotsRef.current.clear()
      mlatSeenRef.current.clear()
      clearInterval(mlatPollRef.current)
      clearInterval(hiresPollRef.current)
    }
  }, [])

  // ── Place receiver marker (uses prop if available, else fetches status) ──
  useEffect(() => {
    const placeMarker = (lat, lon) => {
      const map = mapRef.current
      if (!map) return
      if (receiverMarkerRef.current) receiverMarkerRef.current.remove()
      receiverMarkerRef.current = L.marker([lat, lon], { icon: RECEIVER_ICON, zIndexOffset: 2000 })
        .bindTooltip('Receiver', { direction: 'top' })
        .addTo(map)
      fittedRef.current = true  // don't auto-fit to aircraft if receiver is known
      // Re-center only if the map was initialised at the fallback coords (receiverPos
      // was null at mount time). If it was already centred at init, leave the user's
      // current viewport alone. initCenteredRef avoids a stale-closure read of the prop.
      if (!initCenteredRef.current) {
        map.setView([lat, lon], 9)
        initCenteredRef.current = true
      }
    }

    if (receiverPos) {
      placeMarker(receiverPos[0], receiverPos[1])
    } else {
      fetch(`${API_BASE}/api/status`)
        .then(r => r.ok ? r.json() : null)
        .then(d => {
          const lat = d?.config?.receiver_lat
          const lon = d?.config?.receiver_lon
          if (lat != null && lon != null) placeMarker(lat, lon)
        })
        .catch(() => {})
    }
  }, [receiverPos])

  // ── Hi-res trails: poll /api/tracks when enabled ──────────────────────
  useEffect(() => {
    const clearTrails = () => {
      hiresTrailsRef.current.forEach(({ line }) => line.remove())
      hiresTrailsRef.current.clear()
    }

    if (!showTrails) {
      clearTrails()
      clearInterval(hiresPollRef.current)
      return
    }

    const drawTrails = (data) => {
      const map = mapRef.current
      if (!map) return

      const incoming = new Set(Object.keys(data))

      // Remove trails for aircraft no longer in track data
      for (const [icao, entry] of hiresTrailsRef.current) {
        if (!incoming.has(icao)) {
          entry.line.remove()
          hiresTrailsRef.current.delete(icao)
        }
      }

      for (const [icao, points] of Object.entries(data)) {
        const pts = points.filter(p => p.lat != null && p.lon != null).map(p => [p.lat, p.lon])
        if (pts.length < 2) continue
        // Color trail by the most recent point's altitude
        const lastAlt = points[points.length - 1]?.altitude_ft
        const color = altColor(lastAlt)
        const existing = hiresTrailsRef.current.get(icao)
        if (existing) {
          existing.line.setLatLngs(pts)
          // Re-color if altitude band changed
          if (existing.color !== color) {
            existing.line.setStyle({ color })
            existing.color = color
          }
        } else {
          const line = L.polyline(pts, {
            color, weight: 1.5, opacity: 0.55, smoothFactor: 1,
            lineCap: 'round', lineJoin: 'round',
          }).addTo(map)
          hiresTrailsRef.current.set(icao, { line, color })
        }
      }
    }

    const poll = () => {
      fetch(`${API_BASE}/api/tracks`)
        .then(r => r.ok ? r.json() : {})
        .then(drawTrails)
        .catch(() => {})
    }

    poll()
    hiresPollRef.current = setInterval(poll, 5000)
    return () => {
      clearInterval(hiresPollRef.current)
      clearTrails()
    }
  }, [showTrails])

  // ── Selected aircraft: per-segment gradient trail ─────────────────────────
  // Renders N-1 two-point polylines for the selected aircraft, each coloured by
  // the segment's altitude. Capped at 200 segments (downsamples older points).
  useEffect(() => {
    const clearSelected = () => {
      selectedTrailRef.current.forEach(seg => seg.remove())
      selectedTrailRef.current = []
    }
    clearSelected()
    if (!selectedIcao || !mapRef.current) return

    fetch(`${API_BASE}/api/tracks`)
      .then(r => r.ok ? r.json() : {})
      .then(data => {
        const map = mapRef.current
        if (!map) return
        const points = (data[selectedIcao] || []).filter(p => p.lat != null && p.lon != null)
        if (points.length < 2) return
        // Downsample to ≤200 segments
        const MAX_SEGS = 200
        const sampled = points.length > MAX_SEGS + 1
          ? [points[0], ...points.slice(-(MAX_SEGS))]
          : points
        const segments = []
        for (let i = 0; i < sampled.length - 1; i++) {
          const a = sampled[i], b = sampled[i + 1]
          const color = altColor(b.altitude_ft)
          const seg = L.polyline([[a.lat, a.lon], [b.lat, b.lon]], {
            color, weight: 2.5, opacity: 0.85,
            lineCap: 'round', lineJoin: 'round',
          }).addTo(map)
          segments.push(seg)
        }
        selectedTrailRef.current = segments
      })
      .catch(() => {})

    return clearSelected
  }, [selectedIcao])

  // ── MLAT source dots: poll bulk fixes endpoint, accumulate dots ───────────
  useEffect(() => {
    const clearAllDots = () => {
      mlatDotsRef.current.forEach(srcMap =>
        srcMap.forEach(dots => dots.forEach(d => d.marker.remove()))
      )
      mlatDotsRef.current.clear()
      mlatSeenRef.current.clear()
    }

    if (!showMlatSources) {
      clearAllDots()
      clearInterval(mlatPollRef.current)
      return
    }

    const poll = () => {
      fetch(`${API_BASE}/api/mlat/fixes`)
        .then(r => r.ok ? r.json() : {})
        .then(data => {
          const map = mapRef.current
          if (!map) return
          for (const [icao, sources] of Object.entries(data)) {
            for (const [src, pts] of Object.entries(sources)) {
              if (!Array.isArray(pts)) continue
              if (!mlatDotsRef.current.has(icao)) mlatDotsRef.current.set(icao, new Map())
              if (!mlatSeenRef.current.has(icao))  mlatSeenRef.current.set(icao, new Map())
              const dotsMap = mlatDotsRef.current.get(icao)
              const seenMap = mlatSeenRef.current.get(icao)
              if (!dotsMap.has(src)) dotsMap.set(src, [])
              if (!seenMap.has(src)) seenMap.set(src, new Set())
              const dots = dotsMap.get(src)
              const seen = seenMap.get(src)
              const color = mlatSrcColor(src)
              const MLAT_DOT_CAP = 200  // max markers per source per aircraft
              for (const [lat, lon] of pts) {
                const key = `${lat},${lon}`
                if (!seen.has(key)) {
                  // Ring-buffer: evict oldest marker when cap reached
                  if (dots.length >= MLAT_DOT_CAP) {
                    const evicted = dots.shift()
                    seen.delete(evicted.key)
                    evicted.marker.remove()
                  }
                  seen.add(key)
                  dots.push({
                    key,
                    marker: L.circleMarker([lat, lon], {
                      radius: 3, color, fillColor: color, fillOpacity: 0.85,
                      weight: 0,
                    })
                      .bindTooltip(src, { direction: 'top' })
                      .addTo(map),
                  })
                }
              }
            }
          }
        })
        .catch(() => {})
    }

    poll()
    mlatPollRef.current = setInterval(poll, 2000)
    return () => clearInterval(mlatPollRef.current)
  }, [showMlatSources])

  // ── Residual overlay: poll /api/mlat/residuals when enabled ──────────────
  useEffect(() => {
    const clear = () => {
      residualLayerRef.current.forEach(m => m.remove())
      residualLayerRef.current = []
    }

    if (!showResiduals) {
      clear()
      clearInterval(residualTimerRef.current)
      return
    }

    const refresh = () => {
      fetch(`${API_BASE}/api/mlat/residuals`)
        .then(r => r.ok ? r.json() : [])
        .then(data => {
          if (!mapRef.current) return
          clear()
          data.forEach(pt => {
            const color = residualColor(pt.avg_residual_nm)
            const m = L.circleMarker([pt.lat, pt.lon], {
              radius: 6, color, fillColor: color, fillOpacity: 0.55,
              weight: 1, opacity: 0.8,
            })
              .bindTooltip(
                `${pt.icao.toUpperCase()}<br>${pt.avg_residual_nm.toFixed(2)} nm<br>${pt.sources.join(', ')}`,
                { direction: 'top' }
              )
              .addTo(mapRef.current)
            residualLayerRef.current.push(m)
          })
        })
        .catch(() => {})
    }

    refresh()
    residualTimerRef.current = setInterval(refresh, 5000)
    return () => clearInterval(residualTimerRef.current)
  }, [showResiduals])

  // ── Compute colour function and legend from current mode + snapshot ───
  const { colorFn, legendItems } = useMemo(() => {
    const aircraft = snapshot?.aircraft ?? []
    let colorFn, legendItems

    if (colorMode === 'altitude') {
      colorFn = ac => altColor(ac.altitude)
      legendItems = [
        { label: '< 10k ft', color: '#3fb950' },
        { label: '< 25k ft', color: '#d29922' },
        { label: '< 38k ft', color: '#388bfd' },
        { label: '> 38k ft', color: '#bc8cff' },
        { label: 'Unknown',  color: '#8b949e' },
      ]
    } else if (colorMode === 'type') {
      colorFn = ac => typeGroupColor(ac.type_code, ac.type_category)
      legendItems = TYPE_GROUPS.map(g => ({ label: g.label, color: g.color }))
        .concat([{ label: 'Other', color: TYPE_GROUP_OTHER_COLOR }])
    } else if (colorMode === 'operator') {
      const { map: opMap, top } = buildNameColorMap(aircraft, 'operator')
      colorFn = ac => opMap[ac.operator] ?? TYPE_GROUP_OTHER_COLOR
      legendItems = top.concat([{ name: 'Other', color: TYPE_GROUP_OTHER_COLOR }])
        .map(({ name, color }) => ({ label: name, color }))
    } else if (colorMode === 'country') {
      const { map: cMap, top } = buildNameColorMap(aircraft, 'country')
      colorFn = ac => cMap[ac.country] ?? TYPE_GROUP_OTHER_COLOR
      legendItems = top.concat([{ name: 'Other', color: TYPE_GROUP_OTHER_COLOR }])
        .map(({ name, color }) => ({ label: name, color }))
    } else {  // tags
      colorFn = tagColor
      legendItems = [
        { label: 'Emergency', color: '#ff4444' },
        { label: 'Military',  color: '#bc8cff' },
        { label: 'MLAT',      color: '#388bfd' },
        { label: 'Interesting', color: '#d29922' },
        { label: 'Civil',     color: '#3fb950' },
      ]
    }
    return { colorFn, legendItems }
  }, [colorMode, snapshot?.aircraft])

  // ── Update markers and labels on each snapshot tick ───────────────────
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    // When MLAT-sources mode is active, only show MLAT aircraft
    const allAircraft = (snapshot?.aircraft ?? []).filter(ac => ac.lat != null && ac.lon != null)
    const aircraft    = showMlatSources ? allAircraft.filter(ac => ac.mlat) : allAircraft
    const icaoSet     = new Set(aircraft.map(ac => ac.icao))

    // ── Remove stale markers, labels, and MLAT dots ───────────────────
    for (const [icao, marker] of markersRef.current) {
      if (!icaoSet.has(icao)) {
        marker.remove()
        markersRef.current.delete(icao)
        labelsRef.current.delete(icao)
        // Clear accumulated MLAT dots when aircraft leaves sight
        if (mlatDotsRef.current.has(icao)) {
          mlatDotsRef.current.get(icao).forEach(dots => dots.forEach(d => d.marker.remove()))
          mlatDotsRef.current.delete(icao)
          mlatSeenRef.current.delete(icao)
        }
      }
    }

    // ── Add/update markers ─────────────────────────────────────────────
    for (const ac of aircraft) {
      const color = colorFn(ac)
      const icon  = makeIcon(color, ac.heading_deg)
      if (markersRef.current.has(ac.icao)) {
        const marker = markersRef.current.get(ac.icao)
        marker.setLatLng([ac.lat, ac.lon])
        marker.setIcon(icon)
      } else {
        const label = [ac.callsign, ac.type_code, ac.operator].filter(Boolean).join(' · ') || ac.icao
        const marker = L.marker([ac.lat, ac.lon], { icon })
          .bindTooltip(label, { direction: 'top', offset: [0, -10] })
          .on('click', () => onSelectIcao?.(ac.icao))
          .addTo(map)
        markersRef.current.set(ac.icao, marker)
      }
    }

    // ── Permanent aircraft labels ──────────────────────────────────────
    if (showLabels) {
      for (const ac of aircraft) {
        const marker = markersRef.current.get(ac.icao)
        if (!marker) continue
        const content = makeLabelContent(ac)
        const prev = labelsRef.current.get(ac.icao)
        if (prev !== content) {
          // Unbind hover tooltip and replace with permanent label
          marker.unbindTooltip()
          marker.bindTooltip(content, {
            permanent: true,
            direction: 'right',
            offset: [8, 0],
            className: 'ac-label',
          })
          labelsRef.current.set(ac.icao, content)
        }
      }
    } else if (labelsRef.current.size > 0) {
      // Labels were just turned off — restore hover tooltips
      for (const [icao] of labelsRef.current) {
        const marker = markersRef.current.get(icao)
        if (!marker) continue
        const ac = aircraft.find(a => a.icao === icao)
        if (!ac) continue
        const label = [ac.callsign, ac.type_code, ac.operator].filter(Boolean).join(' · ') || ac.icao
        marker.unbindTooltip()
        marker.bindTooltip(label, { direction: 'top', offset: [0, -10] })
      }
      labelsRef.current.clear()
    }

    setAcCount(aircraft.length)

    // Auto-fit to first batch of aircraft if no receiver coords known
    if (!fittedRef.current && aircraft.length > 0) {
      const bounds = L.latLngBounds(aircraft.map(ac => [ac.lat, ac.lon]))
      map.fitBounds(bounds.pad(0.1))
      fittedRef.current = true
    }
  }, [snapshot?.aircraft, colorFn, onSelectIcao, showMlatSources, showLabels])

  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        <span className={styles.count}>{acCount} aircraft</span>
        <div className={styles.modeGroup}>
          {COLOR_MODES.map(m => (
            <button
              key={m.value}
              className={colorMode === m.value ? styles.btnActive : styles.btn}
              onClick={() => setColorMode(m.value)}
            >{m.label}</button>
          ))}
        </div>
        <div className={styles.modeGroup}>
          <button
            className={showTrails ? styles.btnActive : styles.btn}
            onClick={() => setShowTrails(v => !v)}
            title="Show 30-minute hi-res track trails"
          >Trails</button>
          <button
            className={showLabels ? styles.btnActive : styles.btn}
            onClick={() => setShowLabels(v => !v)}
            title="Show registration, type, speed and altitude labels"
          >Labels</button>
        </div>
        <button
          className={showMlatSources ? styles.btnActive : styles.btn}
          onClick={() => setShowMlatSources(v => !v)}
          title="Show only MLAT aircraft with per-source position dots"
        >MLAT sources</button>
        <button
          className={showResiduals ? styles.btnActive : styles.btn}
          onClick={() => setShowResiduals(v => !v)}
          title="Show MLAT cross-source residual quality overlay"
        >MLAT residuals</button>
      </div>

      <div className={styles.mapWrap} ref={mountRef} />

      <div className={styles.legend}>
        {legendItems.map(({ label, color }) => (
          <span key={label} className={styles.legendItem}>
            <svg width="10" height="10" viewBox="-5 -5 10 10" style={{ flexShrink: 0 }}>
              <circle cx="0" cy="0" r="4" fill={color} />
            </svg>
            {label}
          </span>
        ))}
        <span className={styles.legendSep}>▲ = heading known  ● = heading unknown</span>
      </div>
    </div>
  )
}
