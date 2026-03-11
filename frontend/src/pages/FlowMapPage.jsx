import { useEffect, useRef, useState, useCallback } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import styles from './FlowMapPage.module.css'

// Fix Leaflet default icon path broken by bundlers
delete L.Icon.Default.prototype._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl:       'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl:     'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
})

const DAY_OPTIONS = [1, 3, 7, 14, 30]
const DEFAULT_DAYS = 7

// Map a normalised value 0–1 onto a traffic colour (dark blue → cyan → yellow → red)
function trafficColor(t) {
  // stops: 0=navy, 0.3=blue, 0.6=cyan, 0.8=yellow, 1=red
  const stops = [
    [0.0,  [  8,  40, 100]],
    [0.25, [ 30, 100, 220]],
    [0.50, [ 30, 200, 200]],
    [0.75, [230, 210,  30]],
    [1.0,  [220,  30,  30]],
  ]
  for (let i = 1; i < stops.length; i++) {
    const [t0, c0] = stops[i - 1]
    const [t1, c1] = stops[i]
    if (t <= t1) {
      const f = (t - t0) / (t1 - t0)
      const r = Math.round(c0[0] + f * (c1[0] - c0[0]))
      const g = Math.round(c0[1] + f * (c1[1] - c0[1]))
      const b = Math.round(c0[2] + f * (c1[2] - c0[2]))
      return `rgb(${r},${g},${b})`
    }
  }
  return 'rgb(220,30,30)'
}

export default function FlowMapPage() {
  const mapRef      = useRef(null)   // leaflet Map instance
  const mountRef    = useRef(null)   // DOM div
  const layerRef    = useRef(null)   // current cell layer group
  const markerRef   = useRef(null)   // receiver marker

  const [days,      setDays]     = useState(DEFAULT_DAYS)
  const [loading,   setLoading]  = useState(true)
  const [error,     setError]    = useState(null)
  const [cellCount, setCellCount] = useState(0)
  const [maxCount,  setMaxCount]  = useState(0)

  // ── Initialise Leaflet map (once) ─────────────────────────────────────
  useEffect(() => {
    if (mapRef.current) return   // already mounted

    const map = L.map(mountRef.current, {
      center:    [51.5, -0.1],  // default; will be recentred once data loads
      zoom:      8,
      zoomControl: true,
    })

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 18,
    }).addTo(map)

    mapRef.current = map
    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  // ── Fetch and render cells when days changes ──────────────────────────
  const fetchAndRender = useCallback(() => {
    const map = mapRef.current
    if (!map) return

    setLoading(true)
    setError(null)

    fetch(`/api/coverage/flow?days=${days}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json() })
      .then(data => {
        if (data.error) throw new Error(data.error)

        // Remove previous cell layer
        if (layerRef.current) {
          layerRef.current.remove()
          layerRef.current = null
        }
        if (markerRef.current) {
          markerRef.current.remove()
          markerRef.current = null
        }

        const { cells, max_count, grid_deg, receiver_lat, receiver_lon } = data
        setMaxCount(max_count)
        setCellCount(cells.length)

        // Centre map on receiver if it hasn't been panned yet
        if (receiver_lat != null && receiver_lon != null) {
          if (!map._flowCentred) {
            map.setView([receiver_lat, receiver_lon], 8)
            map._flowCentred = true
          }

          // Receiver marker (small circle)
          markerRef.current = L.circleMarker([receiver_lat, receiver_lon], {
            radius: 6, color: '#388bfd', fillColor: '#388bfd',
            fillOpacity: 1, weight: 2,
          }).bindTooltip('Receiver').addTo(map)
        }

        if (!cells.length) { setLoading(false); return }

        // Use log scale so low-density corridors are still visible
        const logMax = Math.log1p(max_count)
        const g = grid_deg

        // Build cells as canvas-renderer rectangles for performance
        const renderer = L.canvas()
        const layer = L.layerGroup()
        for (const [lat, lon, count] of cells) {
          const t = Math.log1p(count) / logMax
          const opacity = 0.15 + t * 0.65   // 0.15–0.80
          L.rectangle(
            [[lat, lon], [lat + g, lon + g]],
            {
              renderer,
              stroke: false,
              fillColor:   trafficColor(t),
              fillOpacity: opacity,
            }
          ).addTo(layer)
        }
        layer.addTo(map)
        layerRef.current = layer
        setLoading(false)
      })
      .catch(e => {
        setLoading(false)
        setError(e.message)
      })
  }, [days])

  // Re-fetch when days changes (also fires on initial mount after map init)
  useEffect(() => {
    // Small delay to ensure map is initialised
    const id = setTimeout(fetchAndRender, 50)
    return () => clearTimeout(id)
  }, [fetchAndRender])

  return (
    <div className={styles.page}>
      <div className={styles.toolbar}>
        <h2 className={styles.title}>Traffic Flow</h2>

        <div className={styles.controls}>
          {DAY_OPTIONS.map(d => (
            <button
              key={d}
              className={days === d ? styles.btnActive : styles.btn}
              onClick={() => setDays(d)}
            >{d}d</button>
          ))}
        </div>

        {!loading && !error && (
          <span className={styles.meta}>
            {cellCount.toLocaleString()} cells · peak {maxCount.toLocaleString()} samples
          </span>
        )}
        {loading && <span className={styles.meta}>Loading…</span>}
        {error   && <span className={styles.metaError}>Error: {error}</span>}
      </div>

      <div className={styles.mapWrap} ref={mountRef} />

      <div className={styles.legend}>
        <span className={styles.legendLabel}>Low</span>
        <div className={styles.gradBar} />
        <span className={styles.legendLabel}>High</span>
        <span className={styles.legendNote}>· 0.05° grid (~3 nm) · log scale · © OpenStreetMap contributors</span>
      </div>
    </div>
  )
}
