import { Fragment, useState, useEffect, useCallback, useRef } from 'react'
import L from 'leaflet'
import styles from './AircraftDetailPanel.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const EMERGENCY_SQUAWKS = {
  '7700': 'General emergency',
  '7600': 'Radio failure',
  '7500': 'Hijack',
}

function fmtDate(unix) {
  if (!unix) return '—'
  return new Date(unix * 1000).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' })
}

function fmtAlt(alt) {
  if (alt == null) return null
  return alt.toLocaleString() + ' ft'
}

function fmtDuration(startTs, endTs) {
  if (!startTs || !endTs) return null
  const secs = endTs - startTs
  if (secs < 60) return `${secs}s`
  const mins = Math.floor(secs / 60)
  if (mins < 60) return `${mins}m`
  return `${Math.floor(mins / 60)}h ${mins % 60}m`
}

function Field({ label, value, href }) {
  if (value == null || value === '') return null
  return (
    <div className={styles.field}>
      <span className={styles.fieldLabel}>{label}</span>
      {href
        ? <a className={styles.fieldValue} href={href} target="_blank" rel="noopener noreferrer">{value}</a>
        : <span className={styles.fieldValue}>{value}</span>
      }
    </div>
  )
}

// Reconstruct lat/lon track from visit on a Leaflet mini-map
function MiniMap({ icao, visitId }) {
  const containerRef = useRef(null)
  const mapRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return

    // Init map once
    if (!mapRef.current) {
      mapRef.current = L.map(containerRef.current, {
        zoomControl: true,
        attributionControl: false,
        scrollWheelZoom: false,
      })
      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 18,
      }).addTo(mapRef.current)
    }

    const map = mapRef.current
    // Clear previous layers
    map.eachLayer(l => { if (l instanceof L.Polyline || l instanceof L.CircleMarker) map.removeLayer(l) })

    fetch(`${API_BASE}/api/aircraft/${icao}/visits/${visitId}/track`)
      .then(r => r.ok ? r.json() : [])
      .then(pts => {
        if (!pts.length) return
        const latlngs = pts.map(p => [p.lat, p.lon])
        L.polyline(latlngs, { color: '#388bfd', weight: 2, opacity: 0.8 }).addTo(map)
        // Start/end markers
        L.circleMarker(latlngs[0], { radius: 5, color: '#3fb950', fillColor: '#3fb950', fillOpacity: 1, weight: 0 }).addTo(map)
        L.circleMarker(latlngs[latlngs.length - 1], { radius: 5, color: '#f85149', fillColor: '#f85149', fillOpacity: 1, weight: 0 }).addTo(map)
        map.fitBounds(L.polyline(latlngs).getBounds(), { padding: [16, 16] })
      })
      .catch(() => {})

    return () => {
      // Keep map instance alive across visitId changes — don't destroy
    }
  }, [icao, visitId])

  // Destroy on unmount
  useEffect(() => {
    return () => {
      if (mapRef.current) {
        mapRef.current.remove()
        mapRef.current = null
      }
    }
  }, [])

  return <div ref={containerRef} className={styles.miniMap} />
}

function RouteDisplay({ visits, visitId }) {
  if (!visits?.length) return null
  const v = visitId ? visits.find(x => x.id === visitId) : null
  const visit = v || visits[0]
  if (!visit?.origin_icao && !visit?.dest_icao) return null
  return (
    <div className={styles.routeInline}>
      <span className={styles.routeCode}>{visit.origin_icao ?? '?'}</span>
      <span className={styles.routeArrow}>→</span>
      <span className={styles.routeCode}>{visit.dest_icao ?? '?'}</span>
    </div>
  )
}

export default function AircraftDetailPanel({ icao, snapshot, onClose, onRefreshed }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [acasEvents, setAcasEvents] = useState([])
  const [watched, setWatched] = useState(false)
  const [visits, setVisits] = useState([])
  const [activeVisitId, setActiveVisitId] = useState(null)
  const [photo, setPhoto] = useState(undefined) // undefined=loading, null=none, obj=loaded

  const load = useCallback(() => {
    if (!icao) return
    setLoading(true)
    setError(false)
    fetch(`${API_BASE}/api/aircraft/${icao}`)
      .then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(d => { setData(d); setLoading(false) })
      .catch(() => { setError(true); setLoading(false) })
  }, [icao])

  const refresh = useCallback(() => {
    if (!icao || refreshing) return
    setRefreshing(true)
    fetch(`${API_BASE}/api/aircraft/${icao}/refresh`, { method: 'POST' })
      .then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(d => { setData(d); setRefreshing(false); onRefreshed?.() })
      .catch(() => setRefreshing(false))
  }, [icao, refreshing, onRefreshed])

  useEffect(() => { load() }, [load])

  useEffect(() => {
    if (!icao) return
    fetch(`${API_BASE}/api/aircraft/${icao}/visits`)
      .then(r => r.ok ? r.json() : [])
      .then(setVisits)
      .catch(() => setVisits([]))
  }, [icao])

  useEffect(() => {
    if (!icao) return
    fetch(`${API_BASE}/api/acas/aircraft/${icao}?limit=10`)
      .then(r => r.ok ? r.json() : [])
      .then(setAcasEvents)
      .catch(() => setAcasEvents([]))
  }, [icao])

  useEffect(() => {
    if (!icao) return
    fetch(`${API_BASE}/api/notify/watchlist/${icao}`)
      .then(r => r.ok ? r.json() : { watched: false })
      .then(d => setWatched(d.watched))
      .catch(() => {})
  }, [icao])

  useEffect(() => {
    if (!icao) return
    setPhoto(undefined)
    fetch(`https://api.planespotters.net/pub/photos/hex/${icao}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        const p = d?.photos?.[0]
        setPhoto(p ? { src: p.thumbnail_large?.src || p.thumbnail?.src, link: p.link, photographer: p.photographer } : null)
      })
      .catch(() => setPhoto(null))
  }, [icao])

  const toggleWatch = async () => {
    const method = watched ? 'DELETE' : 'POST'
    const r = await fetch(`${API_BASE}/api/notify/watchlist/${icao}`, {
      method,
      headers: method === 'POST' ? { 'Content-Type': 'application/json' } : {},
      body: method === 'POST' ? JSON.stringify({ max_range_nm: null }) : undefined,
    })
    if (r.ok) setWatched(w => !w)
  }

  // Close on Escape or backdrop click
  useEffect(() => {
    function onKey(e) { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  // Live data: prefer real-time snapshot
  const liveData = snapshot?.aircraft?.find(a => a.icao === icao) ?? data?.live

  const regStr = data?.registration || ''
  const typeStr = data?.type_code
    ? [data.type_code, data.type_full_name].filter(Boolean).join(' · ')
    : data?.type_desc || ''
  const opStr = formatOperator(data?.operator) || ''

  const handleVisitRowClick = (visitId) => {
    setActiveVisitId(prev => prev === visitId ? null : visitId)
  }

  return (
    <div className={styles.overlay} onMouseDown={e => { if (e.target === e.currentTarget) onClose() }}>
      <div className={styles.modal}>

        {/* Header */}
        <div className={styles.header}>
          <span className={styles.headerIcao}>{icao}</span>
          {data?.military && <span className={styles.milBadge}>MIL</span>}
          {regStr && <><span className={styles.headerSep}>·</span><span className={styles.headerReg}>{regStr}</span></>}
          {typeStr && <><span className={styles.headerSep}>·</span><span className={styles.headerType}>{typeStr}</span></>}
          {opStr && <><span className={styles.headerSep}>·</span><span className={styles.headerOperator}>{opStr}</span></>}
          <div className={styles.headerActions}>
            <button
              className={`${styles.watchBtn}${watched ? ' ' + styles.watching : ''}`}
              onClick={toggleWatch}
            >{watched ? '★ Watching' : '☆ Watch'}</button>
            <button className={styles.iconBtn} onClick={refresh} disabled={refreshing} title="Re-apply enrichment">
              {refreshing ? '…' : '↻'}
            </button>
            <button className={styles.closeBtn} onClick={onClose} aria-label="Close">✕</button>
          </div>
        </div>

        {loading && <div className={styles.loading}>Loading…</div>}
        {error && <div className={styles.errorMsg}>Failed to load aircraft data.</div>}

        {data && !loading && (
          <div className={styles.body}>

            {/* Photo */}
            {photo?.src && (
              <>
                <a href={photo.link} target="_blank" rel="noopener noreferrer">
                  <img src={photo.src} alt={`Aircraft ${icao}`} className={styles.photo} />
                </a>
                {photo.photographer && (
                  <div className={styles.photoCredit}>Photo: {photo.photographer}</div>
                )}
              </>
            )}

            {/* Two-column: Airframe | Live */}
            <div className={styles.twoCol}>
              {/* Left: airframe details */}
              <div className={styles.col}>
                <div className={styles.sectionTitle}>Airframe</div>
                <Field label="ICAO"        value={icao} />
                <Field label="Registration" value={data.registration} />
                <Field label="Operator"    value={opStr} />
                <Field label="Country"     value={data.country} />
                <Field label="Type code"   value={data.type_code} />
                <Field label="Full name"   value={data.type_full_name} />
                <Field label="Category"    value={data.type_category} />
                <Field label="WTC"         value={data.wtc} />
                <Field label="Details"     value={data.type_desc} />
                <Field label="Year"        value={data.year} />
                {data.history && (
                  <>
                    <Field label="First seen"
                      value={fmtDate(data.history.first_seen)} />
                    <Field label="Sessions"
                      value={data.history.sighting_count} />
                  </>
                )}
                {data.history && (
                  <div className={styles.flagsRow}>
                    {data.military             && <span className={`${styles.flag} ${styles.flagMil}`}>Military</span>}
                    {data.history.rare         && <span className={`${styles.flag} ${styles.flagRare}`}>Rare type</span>}
                    {data.history.first_seen_flag && <span className={`${styles.flag} ${styles.flagNew}`}>First sighting</span>}
                    {data.history.interesting  && <span className={`${styles.flag} ${styles.flagInt}`}>Interesting</span>}
                    {data.history.foreign_military && <span className={`${styles.flag} ${styles.flagMil}`}>Foreign mil</span>}
                  </div>
                )}
              </div>

              {/* Right: live state (if active) */}
              <div className={styles.col}>
                <div className={styles.sectionTitle}>Now {!liveData && <span style={{ color: '#484f58', fontWeight: 400 }}>· not in range</span>}</div>
                {liveData ? (
                  <>
                    <Field label="Callsign"   value={liveData.callsign} />
                    <Field label="Altitude"   value={fmtAlt(liveData.altitude)} />
                    <Field label="Sel. alt"   value={fmtAlt(liveData.selected_alt)} />
                    <Field label="Speed"      value={
                      liveData.airspeed_kts != null
                        ? `${liveData.airspeed_kts} kt${liveData.airspeed_type ? ` (${liveData.airspeed_type})` : ''}`
                        : null
                    } />
                    {liveData.mach != null && (
                      <Field label="Mach" value={`M${liveData.mach.toFixed(3)}`} />
                    )}
                    <Field label="Heading"    value={liveData.heading_deg != null ? `${liveData.heading_deg}°` : null} />
                    <Field label="Vert. rate" value={
                      liveData.vertical_rate_fpm != null
                        ? `${liveData.vertical_rate_fpm > 0 ? '+' : ''}${liveData.vertical_rate_fpm} fpm`
                        : null
                    } />
                    <Field label="Range"      value={liveData.range_nm != null ? `${liveData.range_nm} nm` : null} />
                    {liveData.lat != null && liveData.lon != null && (
                      <Field
                        label="Position"
                        value={`${liveData.lat.toFixed(4)}, ${liveData.lon.toFixed(4)}`}
                        href={`https://www.openstreetmap.org/?mlat=${liveData.lat}&mlon=${liveData.lon}&zoom=10`}
                      />
                    )}
                    <Field label="Squawk"     value={
                      liveData.squawk
                        ? `${liveData.squawk}${EMERGENCY_SQUAWKS[liveData.squawk] ? ` — ${EMERGENCY_SQUAWKS[liveData.squawk]}` : ''}`
                        : null
                    } />
                    <Field label="Signal"     value={liveData.signal != null ? `${Math.round((255 - liveData.signal) / 2.55)}%` : null} />
                    <Field label="Messages"   value={liveData.msg_count} />
                    <Field label="Last seen"  value={liveData.age != null ? `${liveData.age}s ago` : null} />

                    {/* Route from most-recent visit */}
                    {visits.length > 0 && (visits[0].origin_icao || visits[0].dest_icao) && (
                      <div style={{ marginTop: '0.5rem' }}>
                        <div className={styles.sectionTitle} style={{ marginBottom: '0.3rem' }}>Route</div>
                        <div className={styles.routeInline}>
                          <span className={styles.routeCode}>{visits[0].origin_icao ?? '?'}</span>
                          <span className={styles.routeArrow}>→</span>
                          <span className={styles.routeCode}>{visits[0].dest_icao ?? '?'}</span>
                        </div>
                      </div>
                    )}
                  </>
                ) : (
                  <div className={styles.noData} style={{ paddingTop: '0.5rem' }}>No live data</div>
                )}
              </div>
            </div>

            {/* Visits table */}
            {visits.length > 0 && (
              <div className={styles.visitsSection}>
                <div className={styles.sectionTitle}>Visit History ({visits.length})</div>
                <table className={styles.visitsTable}>
                  <thead>
                    <tr>
                      <th>Date</th>
                      <th>Callsign</th>
                      <th>Route</th>
                      <th>Max alt</th>
                      <th style={{ textAlign: 'right' }}>Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visits.map(v => (
                      <Fragment key={v.id}>
                        <tr
                          className={`${styles.visitRow}${activeVisitId === v.id ? ' ' + styles.active : ''}`}
                          onClick={() => handleVisitRowClick(v.id)}
                          title="Click to show track"
                        >
                          <td>{fmtDate(v.start_ts)}</td>
                          <td><span className={styles.visitCallsign}>{v.callsign || '—'}</span></td>
                          <td>
                            {(v.origin_icao || v.dest_icao)
                              ? <span className={styles.visitRoute}>{v.origin_icao ?? '?'} → {v.dest_icao ?? '?'}</span>
                              : <span className={styles.noData}>—</span>
                            }
                          </td>
                          <td className={styles.visitAlt}>{v.max_altitude ? (v.max_altitude.toLocaleString() + ' ft') : '—'}</td>
                          <td className={styles.visitDur}>{fmtDuration(v.start_ts, v.end_ts) ?? '—'}</td>
                        </tr>
                        {activeVisitId === v.id && (
                          <tr>
                            <td colSpan={5} style={{ padding: '0 0.5rem 0.5rem' }}>
                              <MiniMap icao={icao} visitId={v.id} />
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* ACAS events */}
            {acasEvents.length > 0 && (
              <div className={styles.acasSection}>
                <div className={styles.sectionTitle}>ACAS Events</div>
                {acasEvents.map(ev => (
                  <div key={ev.ts + ev.icao} className={styles.acasEvent}>
                    <span className={styles.acasEvTime}>{new Date(ev.ts * 1000).toLocaleString()}</span>
                    <span className={ev.ra_corrective ? styles.correctiveBadge : styles.preventiveBadge}>
                      {ev.ra_description}
                    </span>
                    {ev.threat_icao && (
                      <span className={styles.acasThreat}>
                        vs {ev.threat_reg || ev.threat_icao}
                        {ev.threat_type_code && ` (${ev.threat_type_code})`}
                      </span>
                    )}
                    {ev.altitude != null && (
                      <span className={styles.acasAlt}>{ev.altitude.toLocaleString()} ft</span>
                    )}
                    {ev.mte ? <span className={styles.mteBadge}>MTE</span> : null}
                  </div>
                ))}
              </div>
            )}

          </div>
        )}

      </div>
    </div>
  )
}
