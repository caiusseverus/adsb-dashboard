import { useState, useEffect, useCallback } from 'react'
import styles from './AircraftDetailPanel.module.css'
import { formatOperator } from '../utils/formatOperator'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const EMERGENCY_SQUAWKS = {
  '7700': 'General emergency',
  '7600': 'Radio failure',
  '7500': 'Hijack',
}

function fmtTs(unix) {
  if (!unix) return '—'
  return new Date(unix * 1000).toLocaleString()
}

function fmtAlt(alt) {
  if (alt == null) return '—'
  return alt.toLocaleString() + ' ft'
}

function Field({ label, value, className }) {
  if (value == null || value === '') return null
  return (
    <div className={styles.field}>
      <span className={styles.fieldLabel}>{label}</span>
      <span className={`${styles.fieldValue} ${className ?? ''}`}>{value}</span>
    </div>
  )
}

function Section({ title, children }) {
  return (
    <div className={styles.section}>
      <div className={styles.sectionTitle}>{title}</div>
      {children}
    </div>
  )
}

function PhotoSection({ icao }) {
  const [photo, setPhoto] = useState(undefined) // undefined=loading, null=none, obj=data
  useEffect(() => {
    fetch(`https://api.planespotters.net/pub/photos/hex/${icao}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        const p = d?.photos?.[0]
        setPhoto(p ? { src: p.thumbnail_large?.src || p.thumbnail?.src, link: p.link, photographer: p.photographer } : null)
      })
      .catch(() => setPhoto(null))
  }, [icao])

  if (photo === undefined) return (
    <Section title="Photo"><div className={styles.loading}>Loading…</div></Section>
  )
  if (!photo) return null

  return (
    <Section title="Photo">
      <a href={photo.link} target="_blank" rel="noopener noreferrer" className={styles.photoLink}>
        <img src={photo.src} alt={`Aircraft ${icao}`} className={styles.photo} />
      </a>
      {photo.photographer && (
        <div className={styles.photoCredit}>Photo: {photo.photographer}</div>
      )}
    </Section>
  )
}

function RouteSection({ icao, callsign }) {
  const [route, setRoute] = useState(undefined) // undefined=loading, null=none, obj=data
  const [error, setError] = useState(false)

  useEffect(() => {
    if (!callsign) { setRoute(null); return }
    setRoute(undefined)
    setError(false)
    fetch(`${API_BASE}/api/aircraft/${icao}/route?callsign=${encodeURIComponent(callsign)}`)
      .then(r => r.ok ? r.json() : null)
      .then(data => setRoute(data))
      .catch(() => { setError(true); setRoute(null) })
  }, [icao, callsign])

  if (!callsign) return null

  return (
    <Section title="Route">
      {route === undefined && <div className={styles.loading}>Loading route…</div>}
      {error && <div className={styles.noData}>Route lookup failed</div>}
      {route === null && !error && <div className={styles.noData}>No route data</div>}
      {route && (
        <div className={styles.route}>
          <div className={styles.routeRow}>
            <div className={styles.routeAirport}>
              <span className={styles.routeIcao}>{route.origin?.icao ?? '?'}</span>
              {route.origin?.info?.airport && (
                <span className={styles.routeName}>{route.origin.info.airport}</span>
              )}
              {route.origin?.info?.country_code && (
                <span className={styles.routeCountry}>{route.origin.info.country_code}</span>
              )}
            </div>
            <span className={styles.routeArrow}>→</span>
            <div className={styles.routeAirport}>
              <span className={styles.routeIcao}>{route.destination?.icao ?? '?'}</span>
              {route.destination?.info?.airport && (
                <span className={styles.routeName}>{route.destination.info.airport}</span>
              )}
              {route.destination?.info?.country_code && (
                <span className={styles.routeCountry}>{route.destination.info.country_code}</span>
              )}
            </div>
          </div>
          {route.flight && <Field label="Flight" value={route.flight} />}
        </div>
      )}
    </Section>
  )
}

export default function AircraftDetailPanel({ icao, snapshot, onClose, onRefreshed }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [acasEvents, setAcasEvents] = useState([])
  const [watched, setWatched] = useState(false)

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

  // Live section: prefer real-time snapshot over the one-shot API response.
  // Snapshot aircraft fields match the data.live schema directly.
  const liveData = snapshot?.aircraft?.find(a => a.icao === icao) ?? data?.live

  useEffect(() => {
    if (!icao) return
    fetch(`${API_BASE}/api/acas/aircraft/${icao}?limit=10`)
      .then(r => r.ok ? r.json() : [])
      .then(setAcasEvents)
      .catch(() => setAcasEvents([]))
  }, [icao])

  useEffect(() => {
    fetch(`${API_BASE}/api/notify/watchlist/${icao}`)
      .then(r => r.ok ? r.json() : { watched: false })
      .then(d => setWatched(d.watched))
      .catch(() => {})
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

  // Close on Escape
  useEffect(() => {
    function onKey(e) { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <aside className={styles.panel}>
        <div className={styles.panelHeader}>
          <div className={styles.panelTitle}>
            {icao}
            {data?.military && <span className={styles.milBadge}>MIL</span>}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <button
              onClick={toggleWatch}
              style={{
                background: watched ? '#388bfd22' : 'transparent',
                border: `1px solid ${watched ? '#388bfd' : '#30363d'}`,
                color: watched ? '#388bfd' : '#484f58',
                borderRadius: 4,
                padding: '0.15rem 0.6rem',
                fontSize: '0.75rem',
                cursor: 'pointer',
              }}
            >
              {watched ? '★ Watching' : '☆ Watch'}
            </button>
            <button
              className={styles.refreshBtn}
              onClick={refresh}
              disabled={refreshing}
              title="Re-apply enrichment from ADSBex and hexdb"
            >{refreshing ? '…' : '↻'}</button>
            <button className={styles.closeBtn} onClick={onClose} aria-label="Close">✕</button>
          </div>
        </div>

        {loading && <div className={styles.loading}>Loading…</div>}
        {error && <div className={styles.errorMsg}>Failed to load aircraft data.</div>}

        {data && !loading && (
          <div className={styles.body}>
            <PhotoSection icao={icao} />

            <Section title="Identity">
              <Field label="ICAO"         value={data.icao} />
              <Field label="Registration" value={data.registration} />
              <Field label="Operator"     value={formatOperator(data.operator)} />
              <Field label="Country"      value={data.country} />
              {data.military && <Field label="Military" value="Yes" />}
            </Section>

            <Section title="Type">
              <Field label="Type code"    value={data.type_code} />
              <Field label="Full name"    value={data.type_full_name} />
              <Field label="Category"     value={data.type_category} />
              <Field label="WTC"          value={data.wtc} />
              <Field label="Details"      value={data.type_desc} />
              <Field label="Year"         value={data.year} />
            </Section>

            {liveData && (
              <Section title="Live">
                <Field label="Callsign"  value={liveData.callsign} />
                <Field label="Altitude"  value={fmtAlt(liveData.altitude)} />
                <Field label="Sel. alt"  value={liveData.selected_alt != null ? fmtAlt(liveData.selected_alt) : null} />
                <Field label="Speed"     value={
                  liveData.airspeed_kts != null
                    ? `${liveData.airspeed_kts} kt${liveData.airspeed_type ? ` (${liveData.airspeed_type})` : ''}`
                    : null
                } />
                {liveData.mach != null && (
                  <Field label="Mach" value={`M${liveData.mach.toFixed(3)}`} />
                )}
                <Field label="Heading"   value={liveData.heading_deg != null ? `${liveData.heading_deg}°` : null} />
                <Field label="Vert. rate" value={
                  liveData.vertical_rate_fpm != null
                    ? `${liveData.vertical_rate_fpm > 0 ? '+' : ''}${liveData.vertical_rate_fpm} fpm`
                    : null
                } />
                <Field label="Range"     value={liveData.range_nm != null ? `${liveData.range_nm} nm` : null} />
                {liveData.lat != null && liveData.lon != null && (
                  <div className={styles.field}>
                    <span className={styles.fieldLabel}>Position</span>
                    <a
                      className={styles.fieldValue}
                      href={`https://www.openstreetmap.org/?mlat=${liveData.lat}&mlon=${liveData.lon}&zoom=10`}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {liveData.lat.toFixed(4)}, {liveData.lon.toFixed(4)}
                    </a>
                  </div>
                )}
                <Field label="Squawk"    value={
                  liveData.squawk
                    ? `${liveData.squawk}${EMERGENCY_SQUAWKS[liveData.squawk] ? ` — ${EMERGENCY_SQUAWKS[liveData.squawk]}` : ''}`
                    : null
                } />
                <Field label="Signal"    value={liveData.signal != null ? `${Math.round((255 - liveData.signal) / 2.55)}%` : null} />
                <Field label="Messages"  value={liveData.msg_count} />
                <Field label="Last seen" value={liveData.age != null ? `${liveData.age}s ago` : null} />
              </Section>
            )}

            {liveData?.callsign && (
              <RouteSection icao={icao} callsign={liveData.callsign} />
            )}

            {data.history && (
              <Section title="History">
                <Field label="First seen"     value={fmtTs(data.history.first_seen)} />
                <Field label="Last seen"      value={fmtTs(data.history.last_seen)} />
                <Field label="Sessions"       value={data.history.sighting_count} />
                {data.history.foreign_military && <Field label="Flag" value="Foreign military" />}
                {data.history.interesting     && <Field label="Flag" value="Interesting" />}
                {data.history.rare            && <Field label="Flag" value="Rare" />}
                {data.history.first_seen_flag && <Field label="Flag" value="First sighting" />}
              </Section>
            )}

            {acasEvents.length > 0 && (
              <Section title="ACAS Events">
                {acasEvents.map(ev => (
                  <div key={ev.ts + ev.icao} className={styles.acasEvent}>
                    <span className={styles.acasEvTime}>{fmtTs(ev.ts)}</span>
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
                      <span className={styles.acasAlt}>{fmtAlt(ev.altitude)}</span>
                    )}
                    {ev.mte ? <span className={styles.mteBadge}>MTE</span> : null}
                  </div>
                ))}
              </Section>
            )}

          </div>
        )}
      </aside>
  )
}
