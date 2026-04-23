/**
 * RadarPage — Passive radar characterisation and positioning.
 *
 * Stage 1 is visual-first:
 *  - select an IID from the summary table
 *  - inspect all ICAOs on that IID against the detected cycle
 *  - use the later sections as secondary context
 */

import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import styles from './RadarPage.module.css'
import { useTimingEventStream } from '../hooks/useTimingEventStream'
import { useTimingEventBuffer } from '../hooks/useTimingEventBuffer'
import {
  MESSAGE_FIELD_RENDER_HOLDBACK_US,
  selectMessageFieldEvents,
} from '../utils/messageField'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const RADAR_SYNC_WS_BASE = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}`
  : 'ws://localhost:8000'
const BURST_SYNC_POLL_MS = 1500
const BURST_SYNC_ALIGNMENT_WINDOW_S = 90
const BURST_SYNC_DF11_ON_TIME_THRESHOLD_DEG = 6
const POSITION_VERIFICATION_ON_TIME_THRESHOLD_DEG = 6
const RADAR_FIELD_PERSISTENCE_US = 3_000_000
const RADAR_FIELD_BUFFER_MAX = 60_000
const RADAR_FIELD_AXIS_HEADROOM = 1.08
const RADAR_FIELD_AXIS_SHRINK_HOLD_MS = 45_000
const RADAR_FIELD_AXIS_SHRINK_TIME_CONSTANT_MS = 12_000
const BURST_SYNC_VIEW_MODE_RESIDUALS = 'burst_sync_residuals'
const BURST_SYNC_VIEW_MODE_LEGACY = 'legacy_live_df_alignment'
function getRadarPageMetricsStore() {
  if (typeof window === 'undefined') return null
  if (!window.__RADAR_PAGE_REQUEST_METRICS__) {
    window.__RADAR_PAGE_REQUEST_METRICS__ = {
      requests: {},
      streams: {},
      recent: [],
    }
  }
  return window.__RADAR_PAGE_REQUEST_METRICS__
}

function recordRadarPageRequest(endpoint, trigger, status, startedAt, meta = {}) {
  const store = getRadarPageMetricsStore()
  if (!store || !endpoint) return
  const bucket = store.requests[endpoint] ?? {
    started: 0,
    completed: 0,
    aborted: 0,
    failed: 0,
    inflight: 0,
    maxInflight: 0,
    cached: 0,
    recomputed: 0,
    triggers: {},
    lastStatus: null,
    lastDurationMs: null,
    lastMeta: null,
  }
  if (status === 'start') {
    bucket.started += 1
    bucket.inflight += 1
    bucket.maxInflight = Math.max(bucket.maxInflight, bucket.inflight)
  } else {
    bucket.inflight = Math.max(0, bucket.inflight - 1)
    if (status === 'completed') bucket.completed += 1
    else if (status === 'aborted') bucket.aborted += 1
    else bucket.failed += 1
  }
  if (trigger) {
    bucket.triggers[trigger] = (bucket.triggers[trigger] ?? 0) + 1
  }
  if (meta.cached === true) bucket.cached += 1
  if (meta.cached === false) bucket.recomputed += 1
  bucket.lastStatus = status
  bucket.lastDurationMs = startedAt != null ? Math.round(performance.now() - startedAt) : null
  bucket.lastMeta = meta
  store.requests[endpoint] = bucket
  store.recent.push({
    kind: 'request',
    endpoint,
    trigger,
    status,
    durationMs: bucket.lastDurationMs,
    at: Date.now(),
    ...meta,
  })
  if (store.recent.length > 100) {
    store.recent.splice(0, store.recent.length - 100)
  }
}

function recordRadarPageStream(endpoint, event, meta = {}) {
  const store = getRadarPageMetricsStore()
  if (!store || !endpoint) return
  const bucket = store.streams[endpoint] ?? {
    opens: 0,
    closes: 0,
    messages: 0,
    heartbeats: 0,
    lastEvent: null,
    lastMeta: null,
  }
  if (event === 'open') bucket.opens += 1
  else if (event === 'close') bucket.closes += 1
  else if (event === 'message') bucket.messages += 1
  else if (event === 'heartbeat') bucket.heartbeats += 1
  bucket.lastEvent = event
  bucket.lastMeta = meta
  store.streams[endpoint] = bucket
}

async function trackedRadarFetchJson(url, { endpoint, trigger, signal, ...options } = {}) {
  const startedAt = performance.now()
  recordRadarPageRequest(endpoint, trigger, 'start', startedAt)
  try {
    const response = await fetch(url, { ...options, signal })
    if (!response.ok) {
      recordRadarPageRequest(endpoint, trigger, 'failed', startedAt, { httpStatus: response.status })
      return null
    }
    const payload = await response.json()
    recordRadarPageRequest(endpoint, trigger, 'completed', startedAt, {
      cached: payload?.transport?.cached,
    })
    return payload
  } catch (error) {
    const aborted = signal?.aborted || error?.name === 'AbortError'
    recordRadarPageRequest(endpoint, trigger, aborted ? 'aborted' : 'failed', startedAt)
    return null
  }
}

function statusColor(status) {
  switch (status) {
    case 'SINGLE_RADAR': return '#3fb950'
    case 'LIKELY_SINGLE': return '#d29922'
    case 'MULTI_RADAR': return '#ff7b72'
    case 'CHECK_MULTI': return '#d29922'
    default: return '#484f58'
  }
}

function readinessColor(readiness) {
  switch (readiness) {
    case 'ESTABLISHED': return '#3fb950'
    case 'PROVISIONAL': return '#d29922'
    case 'TENTATIVE': return '#db6d28'
    default: return '#484f58'
  }
}

function qualityColor(pct) {
  if (pct >= 80) return '#3fb950'
  if (pct >= 50) return '#d29922'
  return '#ff7b72'
}

const ROLLING_HISTORY_MAX = 300

function useRadarLiveStream() {
  const [iidMap, setIidMap] = useState({})
  const rollingHistoryRef = useRef({})

  useEffect(() => {
    let ws = null
    let closed = false
    let retryTimeout = null

    function connect() {
      if (closed) return
      ws = new WebSocket(`${RADAR_SYNC_WS_BASE}/ws/radar/live`)

      ws.onmessage = event => {
        try {
          const msg = JSON.parse(event.data)
          if (msg.type !== 'radar_live') return
          const ts = msg.server_ts
          for (const entry of msg.iids) {
            const prev = rollingHistoryRef.current[entry.iid] ?? { period: [], jitter: [], residual: [], cep: [] }
            const push = (arr, v) => v != null ? [...arr.slice(-(ROLLING_HISTORY_MAX - 1)), { ts, v }] : arr
            rollingHistoryRef.current[entry.iid] = {
              period:   push(prev.period,   entry.period_s),
              jitter:   push(prev.jitter,   entry.sync?.sync_jitter_deg),
              residual: push(prev.residual, entry.sync?.residual_ema_deg),
              cep:      push(prev.cep,      entry.localiser?.cep_m),
            }
          }
          setIidMap(prev => {
            const next = { ...prev }
            for (const entry of msg.iids) next[entry.iid] = entry
            return next
          })
        } catch {}
      }

      ws.onclose = () => {
        if (!closed) retryTimeout = setTimeout(connect, 3000)
      }
    }

    connect()
    return () => {
      closed = true
      clearTimeout(retryTimeout)
      ws?.close()
    }
  }, [])

  const rows = useMemo(
    () => Object.values(iidMap).sort((a, b) => a.iid - b.iid).map(entry => {
      const loc = entry.localiser ?? {}
      const syncCoherent = entry.sync?.fit_support_count ?? null
      const syncResidual = entry.sync?.fit_reject_count ?? null
      const syncTotal = syncCoherent != null && syncResidual != null ? syncCoherent + syncResidual : null
      return {
        iid: entry.iid,
        status: entry.status,
        period_s: entry.period_s,
        period_std_s: entry.period_std_s,
        rpm: entry.rpm,
        last_updated: entry.last_updated,
        ref_icao: entry.ref_icao,
        fm_cep_m: loc.source !== 'manual' ? loc.cep_m : null,
        fm_lat: loc.source !== 'manual' ? loc.lat : null,
        display_lat: loc.lat,
        display_lon: loc.lon,
        display_source: loc.source,
        manual_lat: loc.source === 'manual' ? loc.lat : null,
        manual_lon: loc.source === 'manual' ? loc.lon : null,
        manual_note: entry.manual_note,
        unresolvable_reason: entry.unresolvable_reason,
        sync: entry.sync,
        quality_pct: syncTotal != null && syncTotal > 0 ? Math.round(100 * syncCoherent / syncTotal) : null,
        quality_coherent: syncCoherent,
        quality_residual: syncResidual,
        quality_low_sample: syncTotal != null && syncTotal < 3,
      }
    }),
    [iidMap],
  )

  return { rows, iidMap, rollingHistory: rollingHistoryRef }
}

function smoothAxisMax(nextTarget, axisState, nowMs) {
  const safeTarget = Math.max(1, nextTarget)
  if (!axisState || !Number.isFinite(axisState.value) || axisState.value <= 0) {
    return { value: safeTarget, belowSinceMs: null, lastSampleMs: nowMs }
  }

  const previousValue = axisState.value
  const lastSampleMs = Number.isFinite(axisState.lastSampleMs) ? axisState.lastSampleMs : nowMs

  if (safeTarget >= previousValue) {
    return { value: safeTarget, belowSinceMs: null, lastSampleMs: nowMs }
  }

  const belowSinceMs = Number.isFinite(axisState.belowSinceMs) ? axisState.belowSinceMs : nowMs
  if (nowMs - belowSinceMs < RADAR_FIELD_AXIS_SHRINK_HOLD_MS) {
    return { value: previousValue, belowSinceMs, lastSampleMs: nowMs }
  }

  const deltaMs = Math.max(0, nowMs - lastSampleMs)
  const shrinkFraction = 1 - Math.exp(-deltaMs / RADAR_FIELD_AXIS_SHRINK_TIME_CONSTANT_MS)
  const nextValue = previousValue - (previousValue - safeTarget) * shrinkFraction
  return { value: nextValue, belowSinceMs, lastSampleMs: nowMs }
}

function useReceiverPosition() {
  const [data, setData] = useState(null)

  useEffect(() => {
    const controller = new AbortController()
    async function fetchPosition() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/status`, {
        endpoint: 'status',
        trigger: 'page_load_or_poll',
        signal: controller.signal,
      })
      const lat = d?.config?.receiver_lat
      const lon = d?.config?.receiver_lon
      if (!controller.signal.aborted && lat != null && lon != null) {
        setData({ lat, lon })
      }
    }
    fetchPosition()
    const id = setInterval(fetchPosition, 60_000)
    return () => {
      controller.abort()
      clearInterval(id)
    }
  }, [])

  return data
}

/** Shared hook: fetch pipeline health for an IID, polling every 10s. */
function usePipelineHealth(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    async function poll() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/pipeline-health`, {
        endpoint: 'iid_pipeline_health',
        trigger: 'iid_change_or_poll',
        signal: controller.signal,
      })
      if (!controller.signal.aborted && d) setData(d)
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { controller.abort(); clearInterval(id) }
  }, [iid])

  return data
}

/** Shared hook: fetch reference aircraft info for an IID, polling every 10s. */
function useReferenceAircraft(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    async function poll() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/reference-aircraft`, {
        endpoint: 'iid_reference_aircraft',
        trigger: 'iid_change_or_poll',
        signal: controller.signal,
      })
      if (!controller.signal.aborted && d) setData(d)
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { controller.abort(); clearInterval(id) }
  }, [iid])

  return data
}

/** Shared hook: fetch sweep frames for an IID, polling every 10s. */
function useSweepFrames(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    async function poll() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/sweep-frames`, {
        endpoint: 'iid_sweep_frames',
        trigger: 'iid_change_or_poll',
        signal: controller.signal,
      })
      if (!controller.signal.aborted && d) setData(d)
    }
    poll()
    const id = setInterval(poll, 2_000)
    return () => { controller.abort(); clearInterval(id) }
  }, [iid])

  return data
}

function useFrameFmGeometry(iid, frameIndex, direction = 'cw') {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null || frameIndex == null) { setData(null); return }
    const controller = new AbortController()
    setLoading(true)
    async function fetchGeometry() {
      const dir = direction === 'ccw' ? -1 : 1
      const d = await trackedRadarFetchJson(
        `${API_BASE}/api/radar/iids/${iid}/sweep-frames/${frameIndex}/fm-geometry?direction=${dir}`,
        {
          endpoint: 'iid_sweep_frame_fm_geometry',
          trigger: 'frame_geometry_explicit_load_or_change',
          signal: controller.signal,
        },
      )
      if (!controller.signal.aborted && d) setData(d)
      if (!controller.signal.aborted) setLoading(false)
    }
    fetchGeometry()
    return () => { controller.abort() }
  }, [iid, frameIndex, direction])

  return { data, loading }
}

/** Shared hook: fetch airport hypothesis for an IID, polling every 60s.
 *  Accepts an optional `refreshKey` to force an immediate re-fetch. */
function useAirportHypothesis(iid, refreshKey) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/airport-hypothesis`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    poll()
    const id = setInterval(poll, 60_000)  // every 60s — FM pass runs every 10 min
    return () => { cancelled = true; clearInterval(id) }
  }, [iid, refreshKey])

  return { data, loading }
}

/** Shared hook: fetch FM convergence history for an IID, polling every 60s. */
function useFmConvergence(iid) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/fm-convergence`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    poll()
    const id = setInterval(poll, 60_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid])

  return { data, loading }
}

/** Shared hook: fetch current FM location for an IID, polling every 60s. */
function useFmLocation(iid) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    setLoading(true)
    async function poll() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/fm-location`, {
        endpoint: 'iid_fm_location',
        trigger: 'iid_change_or_poll',
        signal: controller.signal,
      })
      if (!controller.signal.aborted && d) setData(d)
      if (!controller.signal.aborted) setLoading(false)
    }
    poll()
    const id = setInterval(poll, 60_000)
    return () => { controller.abort(); clearInterval(id) }
  }, [iid])

  return { data, loading }
}

function useFmDiagnostics(iid, refreshKey = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    setLoading(true)
    async function poll() {
      const d = await trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/fm-diagnostics`, {
        endpoint: 'iid_fm_diagnostics',
        trigger: refreshKey ? 'control_change_or_poll' : 'iid_change_or_poll',
        signal: controller.signal,
      })
      if (!controller.signal.aborted && d) setData(d)
      if (!controller.signal.aborted) setLoading(false)
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { controller.abort(); clearInterval(id) }
  }, [iid, refreshKey])

  return { data, loading }
}

function useTdoaDiagnostics(iid, refreshKey = 0) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/tdoa-diagnostics`, {
          signal: controller.signal,
        })
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 60_000)
    return () => { cancelled = true; controller.abort(); clearInterval(id) }
  }, [iid, refreshKey])

  return data
}

async function resetIid(iid) {
  const response = await fetch(`${API_BASE}/api/radar/iids/${iid}/reset`, {
    method: 'POST',
  })
  if (!response.ok) {
    throw new Error(`Failed to reset IID ${iid}`)
  }
  return response.json()
}

async function resetAllRadarLearning() {
  const response = await fetch(`${API_BASE}/api/radar/reset`, {
    method: 'POST',
  })
  if (!response.ok) {
    throw new Error('Failed to purge radar learning')
  }
  return response.json()
}

function healthIcon(status) {
  switch (status) {
    case 'working': return <span style={{ color: '#3fb950' }}>✓</span>
    case 'accumulating': return <span style={{ color: '#d29922' }}>⏳</span>
    case 'failed': return <span style={{ color: '#ff7b72' }}>✗</span>
    default: return <span style={{ color: '#484f58' }}>—</span>
  }
}

function sourceColor(source) {
  switch (source) {
    case 'manual': return '#58a6ff'
    case 'combined': return '#3fb950'
    case 'fm': return '#d29922'
    case 'frame_accumulation': return '#3fb950'
    case 'coincident_illumination': return '#bc8cff'
    case 'tdoa': return '#ff7b72'
    default: return '#8b949e'
  }
}

/** 5-stage pipeline health bar */
function PipelineHealthBar({ iid, healthData = null }) {
  const health = healthData ?? usePipelineHealth(iid)

  if (!health?.stages) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Pipeline Health</div>
          </div>
        </div>
        <div className={styles.empty}>Select an IID to view pipeline health</div>
      </section>
    )
  }

  const stages = ['period', 'reference', 'frames', 'scoring', 'optimisation']
  const labels = ['① Period', '② Ref A/C', '③ Frames', '④ Scoring', '⑤ Optimise']

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Pipeline Health</div>
        </div>
      </div>
      <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
        {stages.map((key, idx) => {
          const stage = health.stages[key]
          return (
            <div
              key={key}
              title={stage.detail}
              style={{
                flex: 1,
                minWidth: '140px',
                background: '#0d1117',
                border: '1px solid #21262d',
                borderRadius: '6px',
                padding: '0.6rem 0.8rem',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.3rem' }}>
                {healthIcon(stage.status)}
                <span style={{ fontSize: '0.78rem', color: '#c9d1d9', fontWeight: 500 }}>
                  {labels[idx]}
                </span>
              </div>
              <div style={{ fontSize: '0.72rem', color: '#8b949e' }}>
                {stage.detail || ''}
              </div>
            </div>
          )
        })}
      </div>
    </section>
  )
}

/** Aggregated pipeline health for all IIDs — polled every 15s. */
function useAllPipelineHealth() {
  const [data, setData] = useState(null)

  useEffect(() => {
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/pipeline-health`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 15_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  return data
}

function stageIcon(status) {
  switch (status) {
    case 'working': return <span style={{ color: '#3fb950', fontSize: '0.72rem' }}>✓</span>
    case 'accumulating': return <span style={{ color: '#d29922', fontSize: '0.72rem' }}>⏳</span>
    case 'failed': return <span style={{ color: '#ff7b72', fontSize: '0.72rem' }}>✗</span>
    default: return <span style={{ color: '#484f58', fontSize: '0.72rem' }}>—</span>
  }
}

/** Reference aircraft panel with challenger table */
function ReferenceAircraftPanel({ iid, referenceData = null }) {
  const refInfo = referenceData ?? useReferenceAircraft(iid)

  if (!refInfo || refInfo.status === 'NO_MODEL' || refInfo.status === 'NOT_INITIALISED') {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Reference Aircraft</div>
            <div className={styles.sectionLead}>No model for this IID yet</div>
          </div>
        </div>
      </section>
    )
  }

  if (refInfo.status === 'NOT_SELECTED') {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Reference Aircraft</div>
            <div className={styles.sectionLead}>
              Accumulating — need ≥4 bursts per aircraft to select reference
            </div>
          </div>
        </div>
      </section>
    )
  }

  const challengers = refInfo.challengers || []

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Reference Aircraft</div>
          <div className={styles.sectionLead}>
            Current: <strong>{refInfo.ref_icao}</strong>
            {refInfo.ref_score != null && ` (score=${refInfo.ref_score.toFixed(4)})`}
            {' · '}Stable since sweep #{refInfo.ref_since_sweep}
          </div>
        </div>
      </div>
      {challengers.length > 0 && (
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>ICAO</th>
                <th>Score</th>
                <th>Ratio</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {challengers.map(c => (
                <tr key={c.icao} style={{ background: c.status === 'current' ? '#388bfd12' : 'transparent' }}>
                  <td className={styles.monoCell}>
                    {c.icao}
                    {c.status === 'current' && <span style={{ color: '#58a6ff', marginLeft: 6 }}>◀</span>}
                  </td>
                  <td className={styles.monoCell}>{c.score.toFixed(4)}</td>
                  <td className={styles.monoCell}>{c.ratio_to_best.toFixed(2)}x</td>
                  <td>
                    {c.status === 'better_switch' && <span style={{ color: '#3fb950' }}>Better — would switch</span>}
                    {c.status === 'close_skip' && <span style={{ color: '#d29922' }}>Close — stay</span>}
                    {c.status === 'too_far' && <span style={{ color: '#484f58' }}>Too far</span>}
                    {c.status === 'current' && <span style={{ color: '#58a6ff' }}>Current</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function googleMapsUrl(lat, lon) {
  return `https://www.google.com/maps?q=${lat},${lon}&t=k`
}

function formatMethodName(source) {
  switch (source) {
    case 'forward_model': return 'Forward Model'
    case 'fm': return 'Estimated'
    case 'frame_accumulation': return 'Frame Buffer'
    case 'coincident_illumination': return 'Coincident Rays'
    case 'combined': return 'Combined'
    case 'manual': return 'Manual'
    default: return source
  }
}

function IIDTable({ rows, selectedIid, onSelect, onResetAll, resettingAll }) {
  return (
    <section className={styles.card} data-panel="iid-selector">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>IID Selector</div>
          <div className={styles.sectionLead}>
            Click a row to inspect.
          </div>
        </div>
        <div className={styles.metricRow}>
          <span className={styles.cardSub}>{rows.length} IID{rows.length !== 1 ? 's' : ''}</span>
          <button
            type="button"
            className={styles.resetButton}
            onClick={onResetAll}
            disabled={resettingAll}
          >
            {resettingAll ? 'Purging…' : 'Purge Radar Learning'}
          </button>
        </div>
      </div>
      {rows.length === 0 ? (
        <div className={styles.empty}>Accumulating DF11 data…</div>
      ) : (
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>IID</th>
                <th>Location</th>
                <th title="Proportion of burst observations classified as inlier (coherent with the sync model) vs rejected. Low-data entries (fewer than 3 observations) are dimmed. Tooltip shows raw counts.">Quality</th>
                <th>Period</th>
                <th title="Period standard deviation">Period σ</th>
                <th>RPM</th>
                <th title="FM position uncertainty (CEP)">Uncertainty</th>
                <th title="Distance from FM estimate to manual reference">Error</th>
                <th>Name</th>
              </tr>
            </thead>
            <tbody>
              {rows.map(row => {
                const isSelected = selectedIid === row.iid
                const cepKm = row.fm_cep_m != null ? row.fm_cep_m / 1000 : null
                const errorKm = row.manual_reference_error_m != null ? row.manual_reference_error_m / 1000 : null
                const name = row.manual_note || (row.unresolvable_reason ? `Unresolvable: ${row.unresolvable_reason}` : null)
                const hasManual = row.manual_lat != null
                const hasEstimate = row.fm_lat != null || row.display_lat != null
                const locLabel = hasManual ? 'Confirmed' : hasEstimate ? 'Estimated' : 'Not located'
                const locColor = hasManual ? '#3fb950' : hasEstimate ? '#58a6ff' : '#484f58'
                return (
                  <tr
                    key={row.iid}
                    className={isSelected ? styles.rowSelected : styles.row}
                    onClick={() => onSelect(row.iid)}
                  >
                    <td className={styles.monoCell}>{row.iid}</td>
                    <td>
                      <span
                        className={styles.statusBadge}
                        style={{ background: `${locColor}22`, color: locColor, borderColor: `${locColor}55` }}
                      >
                        {locLabel}
                      </span>
                    </td>
                    <td>
                      {row.quality_pct != null ? (
                        <span
                          className={styles.statusBadge}
                          title={`coherent ${row.quality_coherent ?? 0} / residual ${row.quality_residual ?? 0}`}
                          style={{
                            background: row.quality_low_sample ? '#48485822' : `${qualityColor(row.quality_pct)}22`,
                            color: row.quality_low_sample ? '#8b949e' : qualityColor(row.quality_pct),
                            borderColor: row.quality_low_sample ? '#48485855' : `${qualityColor(row.quality_pct)}55`,
                          }}
                        >
                          {row.quality_pct}%{row.quality_low_sample ? '?' : ''}
                        </span>
                      ) : (
                        <span className={styles.statusBadge} style={{ background: '#48485822', color: '#484f58', borderColor: '#48485855' }}>
                          —
                        </span>
                      )}
                    </td>
                    <td className={styles.monoCell}>
                      {row.period_s != null ? `${row.period_s.toFixed(3)}s` : '—'}
                    </td>
                    <td className={styles.monoCell}>
                      {row.period_std_s != null ? `±${(row.period_std_s * 1000).toFixed(1)}ms` : '—'}
                    </td>
                    <td className={styles.monoCell}>
                      {row.rpm != null ? row.rpm.toFixed(2) : '—'}
                    </td>
                    <td className={styles.monoCell}>
                      {cepKm != null ? (
                        <span style={{ color: cepKm < 5 ? '#3fb950' : cepKm < 20 ? '#d29922' : '#ff7b72' }}>
                          {cepKm < 1 ? `${Math.round(row.fm_cep_m)} m` : `${cepKm.toFixed(1)} km`}
                        </span>
                      ) : '—'}
                    </td>
                    <td className={styles.monoCell}>
                      {errorKm != null ? (
                        <span style={{ color: errorKm < 2 ? '#3fb950' : errorKm < 10 ? '#d29922' : '#ff7b72' }}>
                          {errorKm.toFixed(1)} km
                        </span>
                      ) : '—'}
                    </td>
                    <td className={styles.monoCell} style={{ color: row.unresolvable_reason ? '#ff7b72' : '#c9d1d9' }}>
                      {name ?? '—'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function LocalisationControlPanel({ iid, onChanged, controlData = null, feedStatus = null }) {
  const data = controlData
  const loading = iid != null && data == null
  const [manualLat, setManualLat] = useState('')
  const [manualLon, setManualLon] = useState('')
  const [manualNote, setManualNote] = useState('')
  const [unresolvableReason, setUnresolvableReason] = useState('')
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    setManualLat(data?.manual_lat != null ? String(data.manual_lat) : '')
    setManualLon(data?.manual_lon != null ? String(data.manual_lon) : '')
    setManualNote(data?.manual_note ?? '')
    setUnresolvableReason(data?.unresolvable_reason ?? '')
  }, [data?.manual_lat, data?.manual_lon, data?.manual_note, data?.unresolvable_reason])

  async function call(path, options = {}) {
    if (iid == null) return
    setBusy(true)
    try {
      await fetch(`${API_BASE}/api/radar/iids/${iid}${path}`, options)
      onChanged?.()
    } catch {}
    setBusy(false)
  }

  async function saveManual() {
    const lat = Number(manualLat)
    const lon = Number(manualLon)
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return
    await call('/manual-position', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lat, lon, note: manualNote || null }),
    })
  }

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Localisation Control</div>
          </div>
        </div>
        <div className={styles.empty}>Select an IID to manage localisation state.</div>
      </section>
    )
  }

  return (
    <section className={styles.card} data-panel="localisation-control">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Localisation Control</div>
          <div className={styles.sectionLead}>
            Lock a known site position, return an IID to automatic solving, or mark it unresolvable so background solvers stop spending CPU on it.
          </div>
        </div>
        <div className={styles.metricRow}>
          <span className={styles.metricPill}>
            Mode <span className={styles.metricValue}>{data?.resolution_mode ?? (loading ? 'loading' : 'auto')}</span>
          </span>
          <span className={styles.metricPill}>
            Feed <span className={styles.metricValue}>{feedStatus?.mode ?? 'idle'}</span>
          </span>
          <span className={styles.metricPill} title="Sections changed in the latest selected-IID pushed snapshot">
            Changed <span className={styles.metricValue}>
              {feedStatus?.changedSections?.length ? feedStatus.changedSections.join(',') : '—'}
            </span>
          </span>
          <span className={styles.metricPill}>
            Display <span className={styles.metricValue}>{data?.display_source ?? '—'}</span>
          </span>
          {data?.manual_reference_error_m != null && (
            <span className={styles.metricPill}>
              Model Error vs Manual <span className={styles.metricValue}>{formatDistance(data.manual_reference_error_m)}</span>
            </span>
          )}
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: '0.75rem', marginBottom: '0.9rem' }}>
        <label>
          <div className={styles.cardSub} style={{ marginBottom: '0.25rem' }}>Manual Latitude</div>
          <input value={manualLat} onChange={e => setManualLat(e.target.value)} className={styles.controlInput} />
        </label>
        <label>
          <div className={styles.cardSub} style={{ marginBottom: '0.25rem' }}>Manual Longitude</div>
          <input value={manualLon} onChange={e => setManualLon(e.target.value)} className={styles.controlInput} />
        </label>
        <label style={{ gridColumn: '1 / -1' }}>
          <div className={styles.cardSub} style={{ marginBottom: '0.25rem' }}>Name</div>
          <input value={manualNote} onChange={e => setManualNote(e.target.value)} className={styles.controlInput} />
        </label>
      </div>
      <div className={styles.metricRow} style={{ marginBottom: '0.9rem' }}>
        <button type="button" className={styles.actionButton} onClick={saveManual} disabled={busy}>Save Manual Position</button>
        <button type="button" className={styles.actionButton} onClick={() => call('/lock-position', { method: 'POST' })} disabled={busy}>Lock Position</button>
        <button type="button" className={styles.actionButton} onClick={() => call('/unlock-position', { method: 'POST' })} disabled={busy}>Return To Auto</button>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr auto auto', gap: '0.75rem', alignItems: 'end' }}>
        <label>
          <div className={styles.cardSub} style={{ marginBottom: '0.25rem' }}>Unresolvable Reason</div>
          <input value={unresolvableReason} onChange={e => setUnresolvableReason(e.target.value)} className={styles.controlInput} />
        </label>
        <button
          type="button"
          className={styles.actionButton}
          disabled={busy}
          onClick={() => call('/mark-unresolvable', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason: unresolvableReason || null }),
          })}
        >
          Mark Unresolvable
        </button>
        <button type="button" className={styles.actionButton} onClick={() => call('/clear-unresolvable', { method: 'POST' })} disabled={busy}>Clear</button>
      </div>
    </section>
  )
}

function SolutionComparisonPanel({ iid, comparisonData = null }) {
  const comparison = comparisonData

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div><div className={styles.cardTitle}>Position Sources</div></div>
        </div>
        <div className={styles.empty}>Select an IID to view position sources.</div>
      </section>
    )
  }

  const allMethods = comparison?.methods ?? []
  const methods = allMethods.filter(m =>
    m.source === 'manual' || m.source === 'fm' || m.source === 'frame_accumulation'
  )
  const selected = comparison?.selected ?? null

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Position Sources</div>
          <div className={styles.sectionLead}>
            Manual reference and forward-model derived positions. The active display position is highlighted.
          </div>
        </div>
        {selected && selected.source !== 'none' && (
          <span className={styles.metricPill}>
            Active <span className={styles.metricValue} style={{ color: sourceColor(selected.source) }}>{formatMethodName(selected.source)}</span>
          </span>
        )}
      </div>
      {methods.length === 0 ? (
        <div className={styles.empty}>No position sources available yet.</div>
      ) : (
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Source</th>
                <th>Status</th>
                <th>Latitude</th>
                <th>Longitude</th>
                <th title="Circular Error Probable — radius containing 50% of estimates">Uncertainty</th>
                <th>Updated</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {methods.map(method => (
                <tr key={method.source} style={{ background: selected?.source === method.source ? '#388bfd12' : 'transparent' }}>
                  <td className={styles.monoCell} style={{ color: sourceColor(method.source) }}>{formatMethodName(method.source)}</td>
                  <td className={styles.monoCell}>{method.status ?? '—'}</td>
                  <td className={styles.monoCell}>{method.lat != null ? method.lat.toFixed(4) : '—'}</td>
                  <td className={styles.monoCell}>{method.lon != null ? method.lon.toFixed(4) : '—'}</td>
                  <td className={styles.monoCell}>{formatUncertainty(method.cep_m)}</td>
                  <td className={styles.monoCell}>{method.updated_ts ? new Date(method.updated_ts * 1000).toLocaleTimeString() : '—'}</td>
                  <td className={styles.monoCell}>
                    {method.lat != null && method.lon != null ? (
                      <a
                        href={googleMapsUrl(method.lat, method.lon)}
                        target="_blank"
                        rel="noopener noreferrer"
                        style={{ color: sourceColor(method.source), textDecoration: 'none' }}
                        title="Open in Google Maps satellite view"
                      >
                        ↗ Map
                      </a>
                    ) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function TdoaDiagnosticsPanel({ iid, refreshKey = 0 }) {
  const data = useTdoaDiagnostics(iid, refreshKey)

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div><div className={styles.cardTitle}>TDOA Solver</div></div>
        </div>
        <div className={styles.empty}>Select an IID to inspect TDOA readiness and the last manual solve attempt.</div>
      </section>
    )
  }

  const sweepDiag = data?.sweep_diagnostics ?? {}
  const lastRun = data?.last_run
  const statusColorValue = data?.status === 'ready'
    ? '#3fb950'
    : data?.status === 'poor_geometry'
      ? '#ff7b72'
      : '#d29922'

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>TDOA Solver</div>
          <div className={styles.sectionLead}>
            Monitor calibration-pair readiness, per-sweep solve viability, and the outcome of the last manual TDOA run.
          </div>
        </div>
        {data?.status && (
          <span className={styles.metricPill}>
            Status <span className={styles.metricValue} style={{ color: statusColorValue }}>{data.status}</span>
          </span>
        )}
      </div>
      {!data?.available ? (
        <div className={styles.empty}>No TDOA diagnostics available yet.</div>
      ) : (
        <>
          <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
            <span className={styles.metricPill}>Pairs <span className={styles.metricValue}>{data.total_pairs ?? 0}</span></span>
            <span className={styles.metricPill}>Consistent <span className={styles.metricValue}>{data.consistent_pairs ?? 0}</span></span>
            <span className={styles.metricPill}>Selected <span className={styles.metricValue}>{data.selected_pairs ?? 0}</span></span>
            <span className={styles.metricPill}>Sweeps <span className={styles.metricValue}>{data.sweep_groups ?? 0}</span></span>
            <span className={styles.metricPill}>Solved Sweeps <span className={styles.metricValue}>{sweepDiag.solved ?? 0}</span></span>
            <span className={styles.metricPill}>Az Spread <span className={styles.metricValue}>{data.selected_pair_azimuth_spread_deg != null ? `${data.selected_pair_azimuth_spread_deg.toFixed(1)}°` : '—'}</span></span>
          </div>
          <div className={styles.metricRow} style={{ marginBottom: '0.9rem' }}>
            <span className={styles.metricPill}>
              Current Gate <span className={styles.metricValue}>{data.blocker ?? 'Ready to solve'}</span>
            </span>
            {data.stored_solution && (
              <span className={styles.metricPill}>
                Stored TDOA <span className={styles.metricValue}>{data.stored_solution.lat.toFixed(4)}, {data.stored_solution.lon.toFixed(4)} / {formatUncertainty(data.stored_solution.cep_m)}</span>
              </span>
            )}
          </div>
          <div className={styles.tableWrap} style={{ marginBottom: '0.9rem' }}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Meaning</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className={styles.monoCell}>mean_pairs_in_sweep</td>
                  <td className={styles.monoCell}>{data.mean_pairs_in_sweep != null ? data.mean_pairs_in_sweep.toFixed(1) : '—'}</td>
                  <td className={styles.monoCell}>Average calibration-pair count per detected sweep group.</td>
                </tr>
                <tr>
                  <td className={styles.monoCell}>max_pairs_in_sweep</td>
                  <td className={styles.monoCell}>{data.max_pairs_in_sweep ?? '—'}</td>
                  <td className={styles.monoCell}>Best single sweep group seen so far.</td>
                </tr>
                <tr>
                  <td className={styles.monoCell}>too_few_aircraft</td>
                  <td className={styles.monoCell}>{sweepDiag.too_few_aircraft ?? 0}</td>
                  <td className={styles.monoCell}>Sweep groups that lacked enough connected aircraft to solve.</td>
                </tr>
                <tr>
                  <td className={styles.monoCell}>poor_geometry</td>
                  <td className={styles.monoCell}>{sweepDiag.poor_geometry ?? 0}</td>
                  <td className={styles.monoCell}>Sweep groups whose aircraft azimuth spread was too narrow.</td>
                </tr>
                <tr>
                  <td className={styles.monoCell}>residual_too_large</td>
                  <td className={styles.monoCell}>{sweepDiag.residual_too_large ?? 0}</td>
                  <td className={styles.monoCell}>Sweeps that solved numerically but failed residual/uncertainty quality gates.</td>
                </tr>
                <tr>
                  <td className={styles.monoCell}>insufficient_connected_component</td>
                  <td className={styles.monoCell}>{sweepDiag.insufficient_connected_component ?? 0}</td>
                  <td className={styles.monoCell}>Pairs existed, but the aircraft graph was too fragmented to reconstruct one sweep.</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div className={styles.cardHeader} style={{ marginBottom: '0.35rem' }}>
            <div><div className={styles.cardTitle}>Last Manual Run</div></div>
          </div>
          {lastRun ? (
            <div className={styles.metricRow}>
              <span className={styles.metricPill}>Result <span className={styles.metricValue} style={{ color: lastRun.success ? '#3fb950' : '#ff7b72' }}>{lastRun.success ? 'success' : 'failed'}</span></span>
              <span className={styles.metricPill}>Method <span className={styles.metricValue}>{lastRun.method ?? '—'}</span></span>
              <span className={styles.metricPill}>Elapsed <span className={styles.metricValue}>{lastRun.elapsed_ms != null ? `${lastRun.elapsed_ms.toFixed(0)} ms` : '—'}</span></span>
              <span className={styles.metricPill}>Pairs <span className={styles.metricValue}>{lastRun.pair_count ?? '—'}</span></span>
              {lastRun.n_sweeps != null && <span className={styles.metricPill}>Good Sweeps <span className={styles.metricValue}>{lastRun.n_sweeps}</span></span>}
              {lastRun.reason && <span className={styles.metricPill}>Reason <span className={styles.metricValue}>{lastRun.reason}</span></span>}
              {lastRun.fallback_from && <span className={styles.metricPill}>Sweep Failure <span className={styles.metricValue}>{lastRun.fallback_from}</span></span>}
            </div>
          ) : (
            <div className={styles.empty}>No manual TDOA run recorded yet.</div>
          )}
        </>
      )}
    </section>
  )
}

function collectFrameGeometryPoints(data) {
  const points = []
  ;(data?.layers ?? []).forEach(layer => {
    ;(layer.features ?? []).forEach(feature => {
      const geometry = feature.geometry
      if (!geometry) return
      if (geometry.type === 'Point') {
        const [lon, lat] = geometry.coordinates
        points.push({ lat, lon })
      } else if (geometry.type === 'LineString') {
        geometry.coordinates.forEach(([lon, lat]) => points.push({ lat, lon }))
      }
    })
  })
  return points
}

function kmPerLonDegree(lat) {
  return Math.max(111.32 * Math.cos(lat * Math.PI / 180), 1e-6)
}

function flatDistKm(lat1, lon1, lat2, lon2) {
  const dlat = (lat2 - lat1) * 111.32
  const dlon = (lon2 - lon1) * kmPerLonDegree((lat1 + lat2) / 2)
  return Math.hypot(dlat, dlon)
}

function niceRingDistances(maxKm) {
  if (maxKm <= 0) return []
  const tiers = [
    [0.5, 1, 2], [1, 2, 5], [2, 5, 10], [5, 10, 20],
    [10, 25, 50], [25, 50, 100], [50, 100, 200], [100, 200, 500],
  ]
  for (const tier of tiers) {
    if (tier[2] >= maxKm) return tier
  }
  return [100, 200, 500]
}

function collectSolutionPoints(controlData, fmEstimate) {
  const points = []
  if (controlData?.display_lat != null && controlData?.display_lon != null) {
    points.push({ lat: controlData.display_lat, lon: controlData.display_lon })
    if (controlData.display_cep_m != null) {
      const cepKm = controlData.display_cep_m / 1000
      points.push({ lat: controlData.display_lat + cepKm / 111.32, lon: controlData.display_lon })
      points.push({ lat: controlData.display_lat - cepKm / 111.32, lon: controlData.display_lon })
      points.push({ lat: controlData.display_lat, lon: controlData.display_lon + cepKm / kmPerLonDegree(controlData.display_lat) })
      points.push({ lat: controlData.display_lat, lon: controlData.display_lon - cepKm / kmPerLonDegree(controlData.display_lat) })
    }
  }
  if (fmEstimate?.lat != null && fmEstimate?.lon != null) {
    points.push({ lat: fmEstimate.lat, lon: fmEstimate.lon })
    if (fmEstimate.cep_m != null) {
      const cepKm = fmEstimate.cep_m / 1000
      points.push({ lat: fmEstimate.lat + cepKm / 111.32, lon: fmEstimate.lon })
      points.push({ lat: fmEstimate.lat - cepKm / 111.32, lon: fmEstimate.lon })
    }
  }
  return points
}

function formatUncertainty(cepM) {
  if (cepM == null) return '—'
  if (cepM < 1000) return `${Math.round(cepM)} m`
  return `${(cepM / 1000).toFixed(1)} km`
}

function formatDistance(distanceM) {
  if (distanceM == null) return '—'
  if (distanceM < 1000) return `${Math.round(distanceM)} m`
  return `${(distanceM / 1000).toFixed(1)} km`
}

function EvidenceMapPanel({ iid, controlData = null, evidenceData = null, evidenceRevision = 0 }) {
  const [outlierResult, setOutlierResult] = useState(null)  // null | {outlierSet, n_outliers, n_inliers, n_total, rejection_counts}
  const [analyseRunning, setAnalyseRunning] = useState(false)
  const [deleteRunning, setDeleteRunning] = useState(false)

  useEffect(() => {
    setOutlierResult(null)
  }, [evidenceRevision])

  useEffect(() => {
    setOutlierResult(null)
    setAnalyseRunning(false)
    setDeleteRunning(false)
  }, [iid])

  async function handleDeletePoint(frameIndex) {
    if (frameIndex == null) return
    try {
      await fetch(`${API_BASE}/api/radar/iids/${iid}/frame-positions/${frameIndex}`, { method: 'DELETE' })
    } catch {}
  }

  async function handleAnalyseOutliers() {
    setAnalyseRunning(true)
    try {
      const res = await fetch(`${API_BASE}/api/radar/iids/${iid}/frame-positions/filter-analysis`)
      const d = await res.json()
      setOutlierResult({
        outlierSet: new Set(d.outlier_sweep_start_us),
        n_outliers: d.n_outliers,
        n_inliers: d.n_inliers,
        n_total: d.n_total,
        rejection_counts: d.rejection_counts,
      })
    } catch {}
    setAnalyseRunning(false)
  }

  async function handleDeleteOutliers() {
    if (!outlierResult?.outlierSet?.size) return
    setDeleteRunning(true)
    try {
      await fetch(`${API_BASE}/api/radar/iids/${iid}/frame-positions/bulk-delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sweep_start_us: [...outlierResult.outlierSet] }),
      })
      setOutlierResult(null)
    } catch {}
    setDeleteRunning(false)
  }

  const loading = iid != null && evidenceData == null
  const scatterPoints = Array.isArray(evidenceData?.frame_positions)
    ? evidenceData.frame_positions
        .filter(point => point?.lat != null && point?.lon != null)
        .map(point => ({
          lat: point.lat,
          lon: point.lon,
          frameIndex: point.frame_index,
          sweepStartUs: point.sweep_start_us,
        }))
    : []
  const fmEstimate = evidenceData?.fm_estimate ?? null
  const manualEstimate = evidenceData?.manual_position ?? (
    controlData?.manual_lat != null && controlData?.manual_lon != null
      ? { lat: controlData.manual_lat, lon: controlData.manual_lon }
      : null
  )

  const ringAnchor = (() => {
    if (controlData?.display_lat != null) return { lat: controlData.display_lat, lon: controlData.display_lon }
    return fmEstimate?.lat != null ? { lat: fmEstimate.lat, lon: fmEstimate.lon } : null
  })()

  const maxDistKm = ringAnchor && scatterPoints.length > 0
    ? Math.max(...scatterPoints.map(point => flatDistKm(ringAnchor.lat, ringAnchor.lon, point.lat, point.lon)))
    : 0
  const ringDistances = niceRingDistances(maxDistKm)
  const solutionPoints = collectSolutionPoints(controlData, fmEstimate)
  const innerRing = ringAnchor && ringDistances.length > 0 ? ringDistances[0] : 0
  const ringContextPoints = ringAnchor && innerRing > 0 ? [
    { lat: ringAnchor.lat + innerRing / 111.32, lon: ringAnchor.lon },
    { lat: ringAnchor.lat - innerRing / 111.32, lon: ringAnchor.lon },
    { lat: ringAnchor.lat, lon: ringAnchor.lon + innerRing / kmPerLonDegree(ringAnchor.lat) },
    { lat: ringAnchor.lat, lon: ringAnchor.lon - innerRing / kmPerLonDegree(ringAnchor.lat) },
  ] : []

  const width = 600
  const height = 600
  const selectedEstimate = controlData?.display_lat != null && controlData?.display_lon != null
    ? { source: controlData.display_source, lat: controlData.display_lat, lon: controlData.display_lon, cep_m: controlData.display_cep_m }
    : null
  const frameCount = Number(evidenceData?.frame_position_count ?? scatterPoints.length)

  const manualPoint = manualEstimate ? [{ lat: manualEstimate.lat, lon: manualEstimate.lon }] : []
  const points = [...scatterPoints, ...solutionPoints, ...ringContextPoints, ...manualPoint]

  const bounds = useMemo(() => {
    if (points.length === 0) return null
    const lats = points.map(p => p.lat)
    const lons = points.map(p => p.lon)
    const minLat = Math.min(...lats)
    const maxLat = Math.max(...lats)
    const minLon = Math.min(...lons)
    const maxLon = Math.max(...lons)
    const latPad = Math.max(0.02, (maxLat - minLat) * 0.15)
    const lonPad = Math.max(0.02, (maxLon - minLon) * 0.15)
    return {
      minLat: minLat - latPad,
      maxLat: maxLat + latPad,
      minLon: minLon - lonPad,
      maxLon: maxLon + lonPad,
    }
  }, [points])

  function project(lat, lon, w, h) {
    if (!bounds) return { x: w / 2, y: h / 2 }
    const x = ((lon - bounds.minLon) / Math.max(1e-6, bounds.maxLon - bounds.minLon)) * w
    const y = h - ((lat - bounds.minLat) / Math.max(1e-6, bounds.maxLat - bounds.minLat)) * h
    return { x, y }
  }

  return (
    <section className={styles.card} data-panel="evidence-map">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Position Accumulation Map</div>
          <div className={styles.sectionLead}>
            Each dot is an independent position estimate from one sweep frame. The scatter cloud converges on the radar position as more frames accumulate.
          </div>
        </div>
      </div>
      <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
        <span className={styles.metricPill}>
          Frames <span className={styles.metricValue}>{frameCount > 0 ? frameCount : (loading ? '…' : '0')}</span>
        </span>
        <span className={styles.metricPill}>
          Uncertainty <span className={styles.metricValue}>{formatUncertainty(selectedEstimate?.cep_m)}</span>
        </span>
        {fmEstimate && (
          <a
            href={googleMapsUrl(fmEstimate.lat, fmEstimate.lon)}
            target="_blank"
            rel="noopener noreferrer"
            className={styles.metricPill}
            style={{ textDecoration: 'none' }}
            title="Open in Google Maps satellite view"
          >
            Estimate <span className={styles.metricValue}>{fmEstimate.lat.toFixed(4)}, {fmEstimate.lon.toFixed(4)} ↗</span>
          </a>
        )}
        {manualEstimate && (
          <a
            href={googleMapsUrl(manualEstimate.lat, manualEstimate.lon)}
            target="_blank"
            rel="noopener noreferrer"
            className={styles.metricPill}
            style={{ textDecoration: 'none' }}
            title="Open manual position in Google Maps"
          >
            Manual <span className={styles.metricValue}>{manualEstimate.lat.toFixed(4)}, {manualEstimate.lon.toFixed(4)} ↗</span>
          </a>
        )}
        {manualEstimate && fmEstimate && (() => {
          const errKm = flatDistKm(fmEstimate.lat, fmEstimate.lon, manualEstimate.lat, manualEstimate.lon)
          const errColor = errKm < 2 ? '#3fb950' : errKm < 10 ? '#d29922' : '#ff7b72'
          return (
            <span className={styles.metricPill}>
              Error <span className={styles.metricValue} style={{ color: errColor }}>{errKm.toFixed(1)} km</span>
            </span>
          )
        })()}
      </div>
      <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
        <button
          type="button"
          className={styles.actionButton}
          onClick={handleAnalyseOutliers}
          disabled={analyseRunning || loading}
        >
          {analyseRunning ? 'Analysing…' : 'Analyse Outliers'}
        </button>
        {outlierResult && outlierResult.n_inliers >= 2 && outlierResult.n_outliers > 0 && (
          <button
            type="button"
            className={styles.resetButton}
            onClick={handleDeleteOutliers}
            disabled={deleteRunning}
          >
            {deleteRunning ? 'Deleting…' : `Delete ${outlierResult.n_outliers} Outliers`}
          </button>
        )}
        {outlierResult && (
          <button
            type="button"
            className={styles.actionButton}
            onClick={() => setOutlierResult(null)}
          >
            Clear
          </button>
        )}
        {outlierResult && outlierResult.n_inliers >= 2 && (
          <span className={styles.metricPill}>
            Inliers <span className={styles.metricValue} style={{ color: '#3fb950' }}>{outlierResult.n_inliers}</span>
          </span>
        )}
        {outlierResult && outlierResult.n_inliers >= 2 && outlierResult.n_outliers > 0 && (
          <span className={styles.metricPill}>
            Outliers <span className={styles.metricValue} style={{ color: '#ff7b72' }}>{outlierResult.n_outliers}</span>
          </span>
        )}
        {outlierResult && outlierResult.rejection_counts && (
          <span className={styles.metricPill} title="Frames rejected per filter stage">
            Stage 0 <span className={styles.metricValue}>{outlierResult.rejection_counts.stage0 ?? 0}</span>
            {' · '}Stage 1 <span className={styles.metricValue}>{outlierResult.rejection_counts.stage1 ?? 0}</span>
            {' · '}Stage 2 <span className={styles.metricValue}>{outlierResult.rejection_counts.stage2 ?? 0}</span>
          </span>
        )}
      </div>
      {outlierResult && outlierResult.n_inliers < 2 && (
        <div className={styles.sectionLead} style={{ color: '#d29922', marginBottom: '0.75rem' }}>
          Filter could not find a reliable cluster. Stage 0 rejected {outlierResult.rejection_counts?.stage0 ?? '?'} of {outlierResult.n_total} frames.
        </div>
      )}
      {loading || !bounds ? (
        <div className={styles.empty}>{loading ? 'Loading…' : 'No frame estimates yet.'}</div>
      ) : (
        <div className={styles.alignmentWrap} style={{ position: 'relative', overflow: 'hidden', maxWidth: '850px', margin: '0 auto' }}>
          <svg width="100%" viewBox={`0 0 ${width} ${height}`} style={{ display: 'block', aspectRatio: '1 / 1' }}>
            <rect x="0" y="0" width={width} height={height} fill="#0d131a" />

            {ringAnchor && ringDistances.map(ringKm => {
              const c = project(ringAnchor.lat, ringAnchor.lon, width, height)
              const edgeY = project(ringAnchor.lat + ringKm / 111.32, ringAnchor.lon, width, height)
              const r = Math.abs(edgeY.y - c.y)
              if (r < 2) return null
              return (
                <g key={`ring-${ringKm}`}>
                  <circle cx={c.x} cy={c.y} r={r} fill="none" stroke="#2d3340" strokeWidth="1" strokeDasharray="3 4" />
                  <text x={c.x + 3} y={c.y - r - 3} fill="#404858" fontSize="10" fontFamily="SFMono-Regular, Consolas, monospace">{ringKm} km</text>
                </g>
              )
            })}

            {scatterPoints.map(point => {
              const p = project(point.lat, point.lon, width, height)
              const isOutlier = outlierResult != null && outlierResult.outlierSet.has(point.sweepStartUs)
              const dotColor = isOutlier ? '#ff7b72' : '#58a6ff'
              const dotOpacity = isOutlier ? 0.75 : (outlierResult != null ? 0.2 : 0.3)
              const dotR = isOutlier ? 3 : 2
              return (
                <g
                  key={`${point.frameIndex}-${point.sweepStartUs}-${point.lat}-${point.lon}`}
                  onClick={() => handleDeletePoint(point.frameIndex)}
                  style={{ cursor: 'pointer' }}
                >
                  <circle cx={p.x} cy={p.y} r="7" fill="transparent" />
                  <circle cx={p.x} cy={p.y} r={dotR} fill={dotColor} opacity={dotOpacity} />
                </g>
              )
            })}

            {fmEstimate && (() => {
              const p = project(fmEstimate.lat, fmEstimate.lon, width, height)
              return (
                <g>
                  <circle cx={p.x} cy={p.y} r="5" fill="#58a6ff" />
                  <text x={p.x + 7} y={p.y - 7} fill="#58a6ff" fontSize="11" fontWeight="600">Estimate</text>
                </g>
              )
            })()}

            {fmEstimate && manualEstimate && (() => {
              const pFm = project(fmEstimate.lat, fmEstimate.lon, width, height)
              const pMan = project(manualEstimate.lat, manualEstimate.lon, width, height)
              const mx = (pFm.x + pMan.x) / 2
              const my = (pFm.y + pMan.y) / 2
              const errKm = flatDistKm(fmEstimate.lat, fmEstimate.lon, manualEstimate.lat, manualEstimate.lon)
              const errLabel = errKm < 1 ? `${Math.round(errKm * 1000)} m` : `${errKm.toFixed(1)} km`
              const dx = pMan.x - pFm.x
              const dy = pMan.y - pFm.y
              const len = Math.hypot(dx, dy) || 1
              const ox = -dy / len * 12
              const oy = dx / len * 12
              return (
                <g>
                  <line x1={pFm.x} y1={pFm.y} x2={pMan.x} y2={pMan.y} stroke="#8b949e" strokeWidth="1" strokeDasharray="5 4" opacity="0.7" />
                  <text
                    x={mx + ox}
                    y={my + oy}
                    fill="#8b949e"
                    fontSize="10"
                    fontFamily="SFMono-Regular, Consolas, monospace"
                    textAnchor="middle"
                    dominantBaseline="middle"
                  >
                    {errLabel}
                  </text>
                </g>
              )
            })()}

            {manualEstimate && (() => {
              const p = project(manualEstimate.lat, manualEstimate.lon, width, height)
              return (
                <g>
                  <circle cx={p.x} cy={p.y} r="5" fill="#d29922" />
                  <text x={p.x + 7} y={p.y - 7} fill="#d29922" fontSize="11" fontWeight="600">Manual</text>
                </g>
              )
            })()}
          </svg>
          <div className={styles.evidenceLegend}>
            <div className={styles.evidenceLegendItem}>
              <span style={{ width: '0.55rem', height: '0.55rem', borderRadius: '999px', background: '#58a6ff', display: 'inline-block', flexShrink: 0 }} />
              FM estimate
            </div>
            {manualEstimate && (
              <div className={styles.evidenceLegendItem}>
                <span style={{ width: '0.55rem', height: '0.55rem', borderRadius: '999px', background: '#d29922', display: 'inline-block', flexShrink: 0 }} />
                Manual position
              </div>
            )}
            <div className={styles.evidenceLegendItem}>
              <span style={{ width: '0.55rem', height: '0.55rem', borderRadius: '999px', background: '#58a6ff', opacity: 0.35, display: 'inline-block', flexShrink: 0 }} />
              Per-frame solution
            </div>
          </div>
        </div>
      )}
    </section>
  )
}

function burstSyncClassColor(classification) {
  switch (classification) {
    case 'inlier': return '#3fb950'
    case 'soft': return '#d29922'
    case 'rejected': return '#ff7b72'
    default: return '#8b949e'
  }
}

function wrapSignedResidualDeg(observedDeg, predictedDeg) {
  return ((observedDeg - predictedDeg + 540) % 360) - 180
}

function classifyTimingResidual(residualDeg, thresholdDeg) {
  if (!Number.isFinite(residualDeg)) return 'unknown'
  if (residualDeg > thresholdDeg) return 'early'
  if (residualDeg < -thresholdDeg) return 'late'
  return 'on_time'
}

function timingClassColor(timingClass) {
  switch (timingClass) {
    case 'early': return '#58a6ff'
    case 'on_time': return '#3fb950'
    case 'late': return '#ff7b72'
    default: return '#8b949e'
  }
}

function useSelectedIidPageState(iid, windowS, debugLimit = 120) {
  const [snapshot, setSnapshot] = useState(null)
  const [status, setStatus] = useState({
    connected: false,
    mode: 'idle',
    lastUpdateAt: null,
    sequence: null,
    fallback: false,
    changedSections: [],
  })
  const retryRef = useRef(null)
  const fallbackRef = useRef(null)
  const lastUpdateRef = useRef(0)
  const revisionsRef = useRef(null)

  useEffect(() => {
    if (iid == null) {
      setSnapshot(null)
      setStatus({
        connected: false,
        mode: 'idle',
        lastUpdateAt: null,
        sequence: null,
        fallback: false,
        changedSections: [],
      })
      revisionsRef.current = null
      return
    }

    let closed = false
    let ws = null
    revisionsRef.current = null

    const getChangedSections = revisions => {
      const next = revisions && typeof revisions === 'object' ? revisions : {}
      const prev = revisionsRef.current
      revisionsRef.current = next
      if (!prev) return Object.keys(next)
      return Object.entries(next)
        .filter(([section, revision]) => prev?.[section] !== revision)
        .map(([section]) => section)
    }

    const ingestSnapshot = payload => {
      if (!payload || payload.type !== 'radar_selected_iid_state') return
      const changedSections = getChangedSections(payload.revisions)
      recordRadarPageStream('ws_selected_iid_state', 'message', {
        iid,
        sequence: payload.sequence ?? null,
        changedSections,
        revisions: payload.revisions ?? null,
      })
      lastUpdateRef.current = performance.now()
      startTransition(() => setSnapshot(payload))
      setStatus({
        connected: true,
        mode: 'stream',
        lastUpdateAt: Date.now(),
        sequence: payload.sequence ?? null,
        fallback: false,
        changedSections,
      })
    }

    const pollFallback = () => {
      if (closed) return
      trackedRadarFetchJson(`${API_BASE}/api/radar/iids/${iid}/state?window_s=${windowS}&debug_limit=${debugLimit}`, {
        endpoint: 'iid_selected_state',
        trigger: 'selected_state_stream_fallback',
      })
        .then(payload => {
          if (closed || !payload || payload.type !== 'radar_selected_iid_state') return
          const changedSections = getChangedSections(payload.revisions)
          lastUpdateRef.current = performance.now()
          startTransition(() => setSnapshot(payload))
          setStatus({
            connected: false,
            mode: 'fallback',
            lastUpdateAt: Date.now(),
            sequence: payload.sequence ?? null,
            fallback: true,
            changedSections,
          })
        })
        .catch(() => {
          if (!closed) {
            setStatus(prev => ({ ...prev, connected: false, mode: 'disconnected', fallback: true }))
          }
        })
    }

    const connect = () => {
      if (closed) return
      ws = new WebSocket(`${RADAR_SYNC_WS_BASE}/ws/radar/iids/${iid}/state`)
      ws.onopen = () => {
        recordRadarPageStream('ws_selected_iid_state', 'open', { iid })
        try {
          ws.send(JSON.stringify({ window_s: windowS, debug_limit: debugLimit }))
        } catch {}
        setStatus(prev => ({ ...prev, connected: true, mode: 'connecting', fallback: false }))
      }
      ws.onmessage = event => {
        try {
          const payload = JSON.parse(event.data)
          if (payload.type === 'radar_selected_iid_state_heartbeat') {
            recordRadarPageStream('ws_selected_iid_state', 'heartbeat', { iid, sequence: payload.sequence ?? null })
            lastUpdateRef.current = performance.now()
            setStatus(prev => ({
              ...prev,
              connected: true,
              mode: 'stream',
              sequence: payload.sequence ?? prev.sequence,
              fallback: false,
              changedSections: [],
            }))
            return
          }
          ingestSnapshot(payload)
        } catch {}
      }
      ws.onclose = () => {
        recordRadarPageStream('ws_selected_iid_state', 'close', { iid })
        if (closed) return
        setStatus(prev => ({ ...prev, connected: false, mode: 'reconnecting' }))
        retryRef.current = setTimeout(connect, 1000)
      }
      ws.onerror = () => ws.close()
    }

    pollFallback()
    connect()
    fallbackRef.current = setInterval(() => {
      if (closed) return
      if (performance.now() - lastUpdateRef.current > 2500) {
        pollFallback()
      }
    }, 1000)

    return () => {
      closed = true
      clearTimeout(retryRef.current)
      clearInterval(fallbackRef.current)
      ws?.close()
    }
  }, [debugLimit, iid, windowS])

  return { snapshot, status }
}

const legendDotStyle = {
  width: '0.55rem',
  height: '0.55rem',
  borderRadius: '999px',
  display: 'inline-block',
}

function PhaseAnchorPanel({ syncState, observations, candidates }) {
  if (!syncState) return null
  const anchorIcao = syncState.phase_anchor_icao
  const candidateRows = Array.isArray(candidates) && candidates.length > 0
    ? candidates
    : (Array.isArray(syncState.phase_anchor_candidates) ? syncState.phase_anchor_candidates : [])
  const anchorObs = (observations || []).filter(obs => obs?.phase_anchor_contributor)
  const impliedRows = (observations || [])
    .filter(obs => Number.isFinite(Number(obs?.implied_phase_offset_deg)))
    .slice(-160)
  const scatterW = 520
  const scatterH = 132
  const times = impliedRows.map(obs => Number(obs.beam_center_us ?? obs.raw_arrival_us)).filter(Number.isFinite)
  const tMin = times.length ? Math.min(...times) : 0
  const tMax = times.length ? Math.max(...times) : 1
  const tSpan = Math.max(1, tMax - tMin)
  const sinceTs = Number(syncState.phase_anchor_since_ts)
  const sinceAgeS = Number.isFinite(sinceTs) ? (Date.now() / 1000 - sinceTs) : null
  const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.72rem' }
  const thStyle = { textAlign: 'right', color: '#8b949e', fontWeight: 500, padding: '2px 5px', borderBottom: '1px solid #21262d' }
  const tdRight = { textAlign: 'right', padding: '2px 5px', fontFamily: 'SFMono-Regular, Consolas, monospace', borderTop: '1px solid #21262d' }
  const tdLeft = { textAlign: 'left', padding: '2px 5px', borderTop: '1px solid #21262d' }

  return (
    <div style={{ padding: '6px 8px', marginBottom: '0.5rem', border: '1px solid #30363d', borderRadius: '4px', background: '#0b0f14' }}>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: '8px', marginBottom: '0.35rem' }}>
        <div>
          <div style={{ color: '#c9d1d9', fontWeight: 600 }}>Phase Anchor</div>
          <div style={{ color: '#8b949e', fontSize: '0.72rem' }}>Absolute phase is anchored from one aircraft; the rest validate and nudge.</div>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'flex-end', gap: '4px', fontSize: '0.72rem' }}>
          <span className={styles.metricPill}>Anchor <span className={styles.metricValue}>{anchorIcao || '—'}</span></span>
          <span className={styles.metricPill}>Status <span className={styles.metricValue}>{syncState.phase_anchor_status || '—'}</span></span>
          <span className={styles.metricPill}>Score <span className={styles.metricValue}>{fmtNumber(syncState.phase_anchor_score, 1)}</span></span>
          <span className={styles.metricPill}>Spread <span className={styles.metricValue}>{fmtNumber(syncState.phase_anchor_spread_deg, 2, '°')}</span></span>
          <span className={styles.metricPill}>Obs <span className={styles.metricValue}>{syncState.phase_anchor_obs_count ?? anchorObs.length ?? '—'}</span></span>
          <span className={styles.metricPill}>Since <span className={styles.metricValue}>{Number.isFinite(sinceAgeS) ? `${Math.max(0, sinceAgeS).toFixed(0)}s` : '—'}</span></span>
          <span className={styles.metricPill}>Validation <span className={styles.metricValue}>{syncState.phase_validation_status || '—'}</span></span>
          <span className={styles.metricPill}>Agree/reject <span className={styles.metricValue}>{syncState.phase_validation_contributors ?? 0}/{syncState.phase_validation_reject_count ?? 0}</span></span>
          <span className={styles.metricPill}>Median Δ <span className={styles.metricValue}>{fmtNumber(syncState.phase_validation_median_error_deg, 2, '°')}</span></span>
        </div>
      </div>

      {syncState.phase_anchor_replacement_reason && (
        <div style={{ color: '#8b949e', fontSize: '0.72rem', marginBottom: '0.35rem' }}>
          Replacement reason <span className={styles.metricValue}>{syncState.phase_anchor_replacement_reason}</span>
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '10px' }}>
        <div style={{ overflow: 'auto', border: '1px solid #30363d', background: '#0f141b', minWidth: 0 }}>
          <div style={{ color: '#8b949e', fontSize: '0.72rem', padding: '4px 6px' }}>Anchor candidates</div>
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: 'left' }}>ICAO</th>
                <th style={thStyle}>score</th>
                <th style={thStyle}>spread</th>
                <th style={thStyle}>count</th>
                <th style={thStyle}>fit</th>
                <th style={{ ...thStyle, textAlign: 'left' }}>status</th>
              </tr>
            </thead>
            <tbody>
              {candidateRows.slice(0, 12).map(row => (
                <tr key={row.icao} style={{ background: row.icao === anchorIcao ? '#388bfd18' : 'transparent' }}>
                  <td style={{ ...tdLeft, fontFamily: 'SFMono-Regular, Consolas, monospace' }}>{row.icao}</td>
                  <td style={tdRight}>{fmtNumber(row.score, 1)}</td>
                  <td style={tdRight}>{fmtNumber(row.spread_deg, 2, '°')}</td>
                  <td style={tdRight}>{row.obs_count ?? '—'}</td>
                  <td style={tdRight}>{fmtNumber(Number(row.fit_eligible_fraction) * 100, 0, '%')}</td>
                  <td style={tdLeft}>{row.status === 'rejected' ? (row.reject_reasons?.join(', ') || 'rejected') : (row.warning_reasons?.length ? `candidate: ${row.warning_reasons.join(', ')}` : (row.status || 'candidate'))}</td>
                </tr>
              ))}
              {candidateRows.length === 0 && (
                <tr><td colSpan={6} style={{ ...tdLeft, color: '#8b949e' }}>No candidate ranking yet.</td></tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={{ minWidth: 0 }}>
          <div style={{ color: '#8b949e', fontSize: '0.72rem', marginBottom: '2px' }}>Implied phase offset by observation</div>
          <svg width="100%" height={scatterH} viewBox={`0 0 ${scatterW} ${scatterH}`} style={{ background: '#0f141b', border: '1px solid #30363d', display: 'block' }}>
            {[0, 90, 180, 270, 360].map(v => {
              const y = scatterH - (v / 360) * scatterH
              return <line key={v} x1={0} y1={y} x2={scatterW} y2={y} stroke={v === 0 || v === 360 ? '#30363d' : '#21262d'} />
            })}
            {impliedRows.map((obs, idx) => {
              const t = Number(obs.beam_center_us ?? obs.raw_arrival_us)
              const off = Number(obs.implied_phase_offset_deg)
              const x = Number.isFinite(t) ? ((t - tMin) / tSpan) * scatterW : 0
              const y = scatterH - (((off % 360) + 360) % 360 / 360) * scatterH
              const isAnchor = obs.icao === anchorIcao
              const rejected = Boolean(obs.phase_anchor_reject_reason)
              return (
                <circle
                  key={`${obs.icao}-${t}-${idx}`}
                  cx={x}
                  cy={y}
                  r={isAnchor ? 2.6 : 1.8}
                  fill={isAnchor ? '#ffd166' : rejected ? '#ff7b72' : '#58a6ff'}
                  fillOpacity={isAnchor ? 0.95 : 0.65}
                >
                  <title>{`${obs.icao || '—'} implied ${off.toFixed(2)}°${obs.anchor_relative_phase_error_deg != null ? ` | anchor Δ ${Number(obs.anchor_relative_phase_error_deg).toFixed(2)}°` : ''}`}</title>
                </circle>
              )
            })}
            <text x={2} y={10} fontSize="9" fill="#8b949e">360°</text>
            <text x={2} y={scatterH - 2} fontSize="9" fill="#8b949e">0°</text>
          </svg>
        </div>

        <div style={{ overflow: 'auto', border: '1px solid #30363d', background: '#0f141b', minWidth: 0 }}>
          <div style={{ color: '#8b949e', fontSize: '0.72rem', padding: '4px 6px' }}>Per-aircraft implied offsets</div>
          <table style={tableStyle}>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: 'left' }}>ICAO</th>
                <th style={thStyle}>latest offset</th>
                <th style={thStyle}>anchor Δ</th>
                <th style={{ ...thStyle, textAlign: 'left' }}>role</th>
              </tr>
            </thead>
            <tbody>
              {impliedRows.slice(-20).reverse().map((obs, idx) => (
                <tr key={`${obs.icao}-${obs.beam_center_us}-${idx}`}>
                  <td style={{ ...tdLeft, fontFamily: 'SFMono-Regular, Consolas, monospace' }}>{obs.icao}</td>
                  <td style={tdRight}>{fmtNumber(obs.implied_phase_offset_deg, 2, '°')}</td>
                  <td style={tdRight}>{fmtNumber(obs.anchor_relative_phase_error_deg, 2, '°')}</td>
                  <td style={tdLeft}>{obs.phase_anchor_contributor ? 'anchor' : (obs.phase_anchor_reject_reason || 'validator')}</td>
                </tr>
              ))}
              {impliedRows.length === 0 && (
                <tr><td colSpan={4} style={{ ...tdLeft, color: '#8b949e' }}>No implied-offset observations yet.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function fmtNumber(value, digits = 2, suffix = '') {
  const n = Number(value)
  return Number.isFinite(n) ? `${n.toFixed(digits)}${suffix}` : '-'
}

function RotationAlignmentPanel({
  iid,
  selectedRow,
  selectedIcao,
  onSelectIcao,
  rows,
  onSelectIid,
  syncSnapshot,
  syncFeedStatus,
  timingPacket,
}) {
  const [alignmentMode, setAlignmentMode] = useState(BURST_SYNC_VIEW_MODE_RESIDUALS)
  const [resetting, setResetting] = useState(false)
  const [legacyTimeline, setLegacyTimeline] = useState(null)
  const [legacyLoading, setLegacyLoading] = useState(false)
  const [refOverride, setRefOverride] = useState(null)
  const [refOverrideSent, setRefOverrideSent] = useState(false)
  const timelineCacheRef = useRef(new Map())
  const streamStatus = syncFeedStatus ?? {
    connected: false,
    mode: 'idle',
    lastUpdateAt: null,
    sequence: null,
    fallback: false,
  }
  const timingView = useTimingEventBuffer(
    timingPacket,
    BURST_SYNC_ALIGNMENT_WINDOW_S * 1_000_000,
    RADAR_FIELD_BUFFER_MAX,
  )

  useEffect(() => {
    if (iid == null) {
      setLegacyTimeline(null)
      setLegacyLoading(false)
      setRefOverride(null)
      setRefOverrideSent(false)
    }
  }, [iid])

  useEffect(() => {
    // Only poll the legacy timeline when the legacy alignment view is active.
    // This endpoint is expensive on the backend and the default view
    // (burst-sync residuals) does not use it.
    if (iid == null || alignmentMode !== BURST_SYNC_VIEW_MODE_LEGACY) {
      setLegacyLoading(false)
      return
    }
    let cancelled = false
    const cachedTimeline = timelineCacheRef.current.get(iid)
    setLegacyTimeline(cachedTimeline ?? null)
    setLegacyLoading(true)

    async function pollTimeline() {
      const payload = await trackedRadarFetchJson(
        `${API_BASE}/api/radar/iids/${iid}/timeline?window_s=${BURST_SYNC_ALIGNMENT_WINDOW_S}`,
        {
          endpoint: 'iid_timeline',
          trigger: 'legacy_alignment_mode_poll',
        },
      )
      if (!cancelled && payload) {
        timelineCacheRef.current.set(iid, payload)
        startTransition(() => setLegacyTimeline(payload))
      }
      if (!cancelled) {
        if (!cancelled) setLegacyLoading(false)
      }
    }

    pollTimeline()
    const intervalId = setInterval(pollTimeline, BURST_SYNC_POLL_MS)
    return () => {
      cancelled = true
      clearInterval(intervalId)
    }
  }, [iid, alignmentMode])

  const burstTimeline = syncSnapshot
  const rotation = syncSnapshot?.rotation ?? null
  const observations = Array.isArray(burstTimeline?.observations) ? burstTimeline.observations : []
  const alignmentStatus = burstTimeline?.alignment_status ?? null
  const legacyIcaosRaw = Array.isArray(legacyTimeline?.icaos) ? legacyTimeline.icaos : []
  const syncState = burstTimeline?.sync_state ?? null
  const loading = legacyLoading || streamStatus.mode === 'connecting' || streamStatus.mode === 'reconnecting'
  const periodS = syncState?.period_s ?? rotation?.period_s ?? selectedRow?.period_s ?? null
  const legacyPeriodS = legacyTimeline?.dominant_period_s ?? rotation?.period_s ?? selectedRow?.period_s ?? null
  const legacyPeriodUs = legacyPeriodS != null ? legacyPeriodS * 1_000_000 : null
  const latestBurstUs = observations.reduce((max, obs) => {
    const value = Number(obs?.beam_center_us ?? 0)
    return Number.isFinite(value) ? Math.max(max, value) : max
  }, 0)
  const timingNowUs = Number(timingView?.nowUs ?? 0)
  const windowSpanUs = Math.max(
    10 * 1_000_000,
    Number(burstTimeline?.window_s ?? BURST_SYNC_ALIGNMENT_WINDOW_S) * 1_000_000,
  )
  const windowEndUs = Math.max(timingNowUs, latestBurstUs)
  const windowStartUs = Math.max(0, windowEndUs - windowSpanUs)
  // DF11 residual dots for the burst-sync chart are now backend-derived.
  // The backend computes them via _build_df11_residual_observations() using the
  // same predict_sync_observation() call and the same sync snapshot as burst
  // observations, so both chart layers are timing-consistent and directly comparable.
  // Frontend residual math (predictBearingFromSyncModel / wrapSignedResidualDeg) is
  // no longer used for this chart — residual_deg and timing_class come from the backend.
  const df11ResidualDots = useMemo(() => {
    const backendDots = Array.isArray(burstTimeline?.df11_residual_observations)
      ? burstTimeline.df11_residual_observations
      : []
    if (!selectedIcao) return backendDots
    return backendDots.filter(dot => dot.icao === selectedIcao)
  }, [burstTimeline?.df11_residual_observations, selectedIcao])
  const filteredObservations = observations.filter(obs => {
    const sampleUs = Number(obs?.beam_center_us ?? 0)
    if (!Number.isFinite(sampleUs) || sampleUs < windowStartUs || sampleUs > windowEndUs) return false
    if (selectedIcao && obs?.icao !== selectedIcao) return false
    return true
  })
  const inlierCount = filteredObservations.filter(obs => obs.classification === 'inlier').length
  const softCount = filteredObservations.filter(obs => obs.classification === 'soft').length
  const rejectedCount = filteredObservations.filter(obs => obs.classification === 'rejected').length
  const syncDrivingCount = filteredObservations.filter(obs => obs?.sync_update_eligible !== false).length
  const nonSyncDrivingCount = filteredObservations.length - syncDrivingCount
  const dfEarlyCount = df11ResidualDots.filter(dot => dot.timing_class === 'early').length
  const dfOnTimeCount = df11ResidualDots.filter(dot => dot.timing_class === 'on_time').length
  const dfLateCount = df11ResidualDots.filter(dot => dot.timing_class === 'late').length
  const sortedLegacyIcaos = useMemo(() => [...legacyIcaosRaw].sort((a, b) => {
    if ((b.arrivals_us?.length ?? 0) !== (a.arrivals_us?.length ?? 0)) {
      return (b.arrivals_us?.length ?? 0) - (a.arrivals_us?.length ?? 0)
    }
    return a.icao.localeCompare(b.icao)
  }), [legacyIcaosRaw])
  const sortedIcaos = useMemo(() => {
    const counts = new Map()
    for (const obs of observations) {
      if (!obs?.icao) continue
      counts.set(obs.icao, (counts.get(obs.icao) ?? 0) + 1)
    }
    if (counts.size === 0) {
      for (const entry of sortedLegacyIcaos) {
        if (!entry?.icao) continue
        counts.set(entry.icao, Number(entry.arrivals_us?.length ?? 0))
      }
    }
    return [...counts.entries()]
      .map(([icao, count]) => ({ icao, count }))
      .sort((a, b) => (b.count - a.count) || a.icao.localeCompare(b.icao))
  }, [observations, sortedLegacyIcaos])

  async function handleReset() {
    if (iid == null || resetting) return
    setResetting(true)
    try {
      await resetIid(iid)
      setLegacyTimeline(null)
      timelineCacheRef.current.delete(iid)
      onSelectIcao(null)
      setRefOverride(null)
      setRefOverrideSent(false)
    } catch {
    } finally {
      setResetting(false)
    }
  }

  async function handleSetRefOverride(icao) {
    if (iid == null) return
    setRefOverride(icao)
    setRefOverrideSent(true)
    try {
      await fetch(`${API_BASE}/api/radar/iids/${iid}/set-reference-aircraft`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ icao: icao || null }),
      })
    } catch {}
  }

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Burst Sync Alignment</div>
            <div className={styles.sectionLead}>
              Select an IID to inspect burst-centre residuals against the maintained sync model.
            </div>
          </div>
        </div>
        <div className={styles.empty}>Choose an IID from the selector above.</div>
      </section>
    )
  }

  const chartW = 980
  const residualChartH = 340
  const padL = 58
  const padR = 26
  const padT = 24
  const padB = 40
  const chartH = residualChartH
  const plotW = chartW - padL - padR
  const plotH = chartH - padT - padB
  const spanUs = Math.max(1, windowEndUs - windowStartUs)
  const maxAbsResidualBursts = filteredObservations.reduce((max, obs) => {
    const value = Math.abs(Number(obs?.residual_deg ?? 0))
    return Number.isFinite(value) ? Math.max(max, value) : max
  }, 0)
  const maxAbsResidualDf11 = df11ResidualDots.reduce((max, dot) => {
    const value = Math.abs(Number(dot?.residual_deg ?? 0))
    return Number.isFinite(value) ? Math.max(max, value) : max
  }, 0)
  const jitterDeg = Number(syncState?.sync_jitter_deg)
  const yAbs = Math.min(
    180,
    Math.max(45, Math.max(maxAbsResidualBursts, maxAbsResidualDf11) * 1.25, Number.isFinite(jitterDeg) ? jitterDeg * 4 : 0),
  )
  const phaseResidualRows = filteredObservations
    .map(obs => ({
      key: `${obs?.icao ?? 'unknown'}-${Number(obs?.beam_center_us ?? obs?.raw_arrival_us ?? 0)}-${Number(obs?.residual_deg ?? 0).toFixed(3)}`,
      phaseDeg: Number(obs?.bearing_deg),
      residualDeg: Number(obs?.residual_deg),
      residualCorrectedDeg: Number(obs?.residual_corrected_deg ?? obs?.residual_deg),
      classification: obs?.classification,
      icao: obs?.icao,
      bearingDeg: Number(obs?.bearing_deg),
      rangeNm: Number(obs?.range_nm),
    }))
    .filter(row => Number.isFinite(row.phaseDeg) && Number.isFinite(row.residualDeg))
    .sort((a, b) => (a.phaseDeg - b.phaseDeg) || a.key.localeCompare(b.key))
  const rangeResidualRows = phaseResidualRows
    .filter(row => Number.isFinite(row.rangeNm) && Number.isFinite(row.residualCorrectedDeg))
    .sort((a, b) => (a.rangeNm - b.rangeNm) || a.key.localeCompare(b.key))
  const foldedPhaseCurve = (() => {
    const bins = Array.from({ length: 24 }, (_, idx) => ({
      phaseCenterDeg: (idx + 0.5) * (360 / 24),
      values: [],
    }))
    for (const row of phaseResidualRows) {
      const phaseDeg = ((row.phaseDeg % 360) + 360) % 360
      const index = Math.min(bins.length - 1, Math.floor((phaseDeg / 360) * bins.length))
      bins[index].values.push(row.residualDeg)
    }
    return bins
      .filter(bin => bin.values.length > 0)
      .map(bin => {
        const sorted = bin.values.slice().sort((a, b) => a - b)
        const mid = Math.floor(sorted.length / 2)
        const median = sorted.length % 2 === 1
          ? sorted[mid]
          : (sorted[mid - 1] + sorted[mid]) / 2
        return { phaseCenterDeg: bin.phaseCenterDeg, residualDeg: median }
      })
  })()
  const legacyVisibleSpanUs = legacyPeriodUs != null
    ? Math.max(60 * 1_000_000, legacyPeriodUs * 6)
    : null
  const shownPeriodS = alignmentMode === BURST_SYNC_VIEW_MODE_LEGACY ? legacyPeriodS : periodS
  const phaseChartH = 220
  const auxChartH = 220
  const phasePadL = 58
  const phasePadR = 26
  const phasePadT = 24
  const phasePadB = 40
  const phasePlotW = chartW - phasePadL - phasePadR
  const phasePlotH = phaseChartH - phasePadT - phasePadB
  const maxRangeNm = rangeResidualRows.length > 0
    ? Math.max(10, ...rangeResidualRows.map(row => row.rangeNm))
    : 10

  function sampleUsToX(sampleUs) {
    return padL + ((sampleUs - windowStartUs) / spanUs) * plotW
  }

  function obsToX(obs) {
    return sampleUsToX(Number(obs.beam_center_us ?? windowStartUs))
  }

  function residualToY(residualDeg) {
    return padT + ((yAbs - residualDeg) / (2 * yAbs)) * plotH
  }

  function phaseToX(phaseDeg) {
    const normalized = ((Number(phaseDeg) % 360) + 360) % 360
    return phasePadL + (normalized / 360) * phasePlotW
  }

  function phaseResidualToY(residualDeg) {
    return phasePadT + ((yAbs - residualDeg) / (2 * yAbs)) * phasePlotH
  }

  function rangeToX(rangeNm) {
    return phasePadL + (Math.max(0, Number(rangeNm)) / Math.max(1, maxRangeNm)) * phasePlotW
  }

  function legacyClassColor(classification, selected) {
    switch (classification) {
      case 'primary': return '#58a6ff'
      case 'primary_harmonic': return '#3fb950'
      case 'residual': return '#ff7b72'
      default: return selected ? '#ffd166' : '#8b949e'
    }
  }
  const legacyDisplayIcaos = selectedIcao
    ? sortedLegacyIcaos.filter(entry => entry.icao === selectedIcao)
    : sortedLegacyIcaos
  const syncFeedAgeS = streamStatus.lastUpdateAt != null
    ? Math.max(0, (Date.now() - streamStatus.lastUpdateAt) / 1000)
    : null

  return (
    <section className={styles.card} data-panel="rotation-alignment">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Burst Sync Alignment</div>
          <div className={styles.sectionLead}>
            {alignmentMode === BURST_SYNC_VIEW_MODE_RESIDUALS
              ? 'Both layers are backend-derived and directly comparable: burst-centre residuals (circles) and DF11 arrival residuals (dots) use the same authoritative sync model and predictor.'
              : 'Legacy view: per-aircraft live DF alignment across the rolling window for broad multi-aircraft timing context.'}
          </div>
        </div>
        <div className={styles.metricRow}>
          <span className={styles.metricPill}>
            IID{' '}
            {rows && rows.length > 1 ? (
              <select
                value={iid ?? ''}
                onChange={e => onSelectIid?.(e.target.value || null)}
                style={{
                  background: 'transparent',
                  border: '1px solid #30363d',
                  borderRadius: '3px',
                  color: '#c9d1d9',
                  fontSize: '0.78rem',
                  marginLeft: '4px',
                  padding: '1px 4px',
                }}
              >
                {rows.map(r => (
                  <option key={r.iid} value={r.iid}>{r.iid}</option>
                ))}
              </select>
            ) : (
              <span className={styles.metricValue}>{iid}</span>
            )}
          </span>
          <span className={styles.metricPill}>
            View
            <select
              value={alignmentMode}
              onChange={e => setAlignmentMode(e.target.value)}
              style={{
                background: 'transparent',
                border: '1px solid #30363d',
                borderRadius: '3px',
                color: '#c9d1d9',
                fontSize: '0.72rem',
                marginLeft: '4px',
                padding: '1px 3px',
              }}
            >
              <option value={BURST_SYNC_VIEW_MODE_RESIDUALS}>Burst Sync Residuals</option>
              <option value={BURST_SYNC_VIEW_MODE_LEGACY}>Live DF Alignment (Legacy)</option>
            </select>
          </span>
          <span className={styles.metricPill}>
            Status <span className={styles.metricValue}>{rotation?.status ?? '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Readiness <span className={styles.metricValue}>{rotation?.primary_readiness ?? '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Period <span className={styles.metricValue}>{shownPeriodS != null ? `${shownPeriodS.toFixed(4)}s` : '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Jitter <span className={styles.metricValue}>
              {syncState?.sync_jitter_deg != null ? `±${syncState.sync_jitter_deg.toFixed(1)}°` : '—'}
            </span>
          </span>
          {alignmentMode === BURST_SYNC_VIEW_MODE_RESIDUALS ? (
            <>
              <span className={styles.metricPill}>
                Burst obs <span className={styles.metricValue}>{filteredObservations.length}</span>
              </span>
              <span className={styles.metricPill}>
                DF11 residual dots <span className={styles.metricValue}>{df11ResidualDots.length}</span>
              </span>
            </>
          ) : (
            <span className={styles.metricPill}>
              ICAOs <span className={styles.metricValue}>{sortedLegacyIcaos.length}</span>
            </span>
          )}
          <span className={styles.metricPill}>
            Sync feed <span className={styles.metricValue}>
              {streamStatus.fallback ? 'fallback' : streamStatus.connected ? 'stream' : (streamStatus.mode || 'idle')}
            </span>
          </span>
          <span className={styles.metricPill}>
            Last sync <span className={styles.metricValue}>{syncFeedAgeS != null ? `${syncFeedAgeS.toFixed(1)}s` : '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Seq <span className={styles.metricValue}>{streamStatus.sequence ?? '—'}</span>
          </span>
          <span className={styles.metricPill} title="Set reference aircraft for next FM run">
            Ref A/C
            <select
              value={refOverride ?? ''}
              onChange={e => handleSetRefOverride(e.target.value || null)}
              style={{
                background: refOverride ? '#d2992222' : 'transparent',
                border: '1px solid #30363d',
                borderRadius: '3px',
                color: '#c9d1d9',
                fontSize: '0.72rem',
                marginLeft: '4px',
                padding: '1px 3px',
              }}
            >
              <option value="">auto</option>
              {sortedIcaos.map(entry => (
                <option key={entry.icao} value={entry.icao}>{entry.icao}</option>
              ))}
            </select>
            {refOverrideSent && refOverride && <span style={{ color: '#d29922', marginLeft: '3px' }}>⚑</span>}
          </span>
          {alignmentMode === BURST_SYNC_VIEW_MODE_RESIDUALS ? (
            <>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#3fb950' }} />Burst inlier</span>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#d29922' }} />Burst soft</span>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#ff7b72' }} />Burst rejected</span>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#58a6ff' }} />DF11 early</span>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#3fb950' }} />DF11 on time</span>
              <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#ff7b72' }} />DF11 late</span>
            </>
          ) : (
            <>
              <span className={styles.legendChip}><span className={styles.legendDotPrimary} />Primary</span>
              <span className={styles.legendChip}><span className={styles.legendDotHarmonic} />Primary harmonic</span>
              <span className={styles.legendChip}><span className={styles.legendDotResidual} />Residual</span>
            </>
          )}
          <button
            type="button"
            className={styles.resetButton}
            onClick={handleReset}
            disabled={resetting}
          >
            {resetting ? 'Resetting…' : 'Reset IID'}
          </button>
        </div>
      </div>

      <PhaseAnchorPanel
        syncState={syncState}
        observations={filteredObservations}
        candidates={burstTimeline?.phase_anchor_candidates}
      />

      {alignmentMode === BURST_SYNC_VIEW_MODE_RESIDUALS ? (
        <>
          {filteredObservations.length === 0 && df11ResidualDots.length === 0 ? (
            <div className={styles.empty}>
              {alignmentStatus?.detail
                ?? (syncState
                  ? 'No burst-sync or DF11 residual data yet for this IID.'
                  : 'Waiting for a maintained sync model before backend DF11 residual dots can be computed.')}
            </div>
          ) : (
            <>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginBottom: '0.7rem' }}>
              <button
                type="button"
                className={styles.metricPill}
                onClick={() => onSelectIcao(null)}
                style={{
                  cursor: 'pointer',
                  background: selectedIcao == null ? '#388bfd22' : '#0f141b',
                  color: selectedIcao == null ? '#d2e4ff' : '#8b949e',
                }}
              >
                All ICAOs ({observations.length})
              </button>
              {sortedIcaos.slice(0, 12).map(entry => (
                <button
                  key={entry.icao}
                  type="button"
                  className={styles.metricPill}
                  onClick={() => onSelectIcao(selectedIcao === entry.icao ? null : entry.icao)}
                  style={{
                    cursor: 'pointer',
                    background: selectedIcao === entry.icao ? '#388bfd22' : '#0f141b',
                    color: selectedIcao === entry.icao ? '#d2e4ff' : '#8b949e',
                  }}
                >
                  <span style={{ fontFamily: 'SFMono-Regular, Consolas, monospace' }}>{entry.icao}</span>
                  <span className={styles.metricValue}>{entry.count}</span>
                </button>
              ))}
              </div>
              <div className={styles.alignmentWrap}>
                <svg
                  width={chartW}
                  height={chartH}
                  viewBox={`0 0 ${chartW} ${chartH}`}
                  className={styles.alignmentSvg}
                >
                  {[1, 0.5, 0, -0.5, -1].map(f => {
                    const residual = yAbs * f
                    const y = residualToY(residual)
                    return (
                      <g key={f}>
                        <line
                          x1={padL}
                          y1={y}
                          x2={chartW - padR}
                          y2={y}
                          stroke={residual === 0 ? '#58a6ff88' : '#30363d'}
                          strokeDasharray={residual === 0 ? '0' : '4 4'}
                        />
                        <text x={padL - 8} y={y + 4} textAnchor="end" className={styles.axisLabel}>
                          {residual.toFixed(0)}°
                        </text>
                      </g>
                    )
                  })}
                  {Number.isFinite(jitterDeg) && jitterDeg > 0 && (
                    <rect
                      x={padL}
                      y={residualToY(jitterDeg)}
                      width={plotW}
                      height={Math.max(1, residualToY(-jitterDeg) - residualToY(jitterDeg))}
                      fill="rgba(63, 185, 80, 0.08)"
                    />
                  )}
                  {Array.from({ length: 7 }, (_, idx) => {
                    const x = padL + (idx / 6) * plotW
                    const relS = (spanUs * idx / 6) / 1_000_000
                    return (
                      <g key={idx}>
                        <line
                          x1={x}
                          y1={padT}
                          x2={x}
                          y2={chartH - padB}
                          stroke="#21262d"
                          strokeDasharray="3 5"
                        />
                        <text x={x} y={chartH - 10} textAnchor="middle" className={styles.axisLabel}>
                          +{relS.toFixed(1)}s
                        </text>
                      </g>
                    )
                  })}
                  <text x={padL + plotW / 2} y={18} textAnchor="middle" className={styles.axisLabel}>
                    Burst-centre residuals and DF11 arrival residuals — both backend-derived, directly comparable
                  </text>
                  <text x={padL + plotW / 2} y={chartH - 4} textAnchor="middle" className={styles.axisLabel}>
                    Elapsed seconds across rolling window
                  </text>
                  {(() => {
                    // Backend-derived DF11 residual dots — timing_class and residual_deg
                    // come from predict_sync_observation() on the backend, same path as burst obs.
                    const stride = Math.max(1, Math.ceil(df11ResidualDots.length / 2400))
                    return df11ResidualDots
                      .filter((_, idx) => idx % stride === 0)
                      .map((dot, idx) => (
                        <circle
                          key={`df11-dot-${dot.icao ?? ''}-${dot.arrival_beast_us ?? idx}`}
                          cx={sampleUsToX(Number(dot.arrival_beast_us ?? windowStartUs))}
                          cy={residualToY(Number(dot.residual_deg ?? 0))}
                          r={1.9}
                          fill={timingClassColor(dot.timing_class)}
                          opacity={0.62}
                        >
                          <title>
                            {`${dot.icao ?? 'DF11'} ${(dot.timing_class ?? '').replace('_', ' ')} residual ${Number(dot.residual_deg ?? 0).toFixed(2)}° (backend)`}
                          </title>
                        </circle>
                      ))
                  })()}
                  {filteredObservations.map(obs => {
                    const x = obsToX(obs)
                    const y = residualToY(Number(obs.residual_deg ?? 0))
                    const weight = Number(obs.weight ?? 0)
                    const syncEligible = obs?.sync_update_eligible !== false
                    const radiusBase = syncEligible ? 2.7 : 2.1
                    const radius = Math.max(radiusBase, Math.min(5.4, radiusBase + weight * 2.2))
                    return (
                      <circle
                        key={`${obs.icao}-${obs.beam_center_us}-${obs.residual_deg}-${syncEligible ? 'sync' : 'vis'}`}
                        cx={x}
                        cy={y}
                        r={radius}
                        fill={burstSyncClassColor(obs.classification)}
                        opacity={syncEligible ? 0.9 : 0.6}
                        stroke={selectedIcao === obs.icao ? '#ffd166' : syncEligible ? 'none' : '#30363d'}
                        strokeWidth={selectedIcao === obs.icao ? 1.2 : syncEligible ? 0 : 0.8}
                      >
                        <title>
                          {`${obs.icao} residual ${Number(obs.residual_deg ?? 0).toFixed(2)}° | predicted ${Number(obs.predicted_deg ?? 0).toFixed(1)}° | replies ${obs.n_replies ?? 0}${syncEligible ? '' : ' | non-sync-driving'}`}
                        </title>
                      </circle>
                    )
                  })}
                </svg>
              </div>
              <div style={{ fontSize: '0.74rem', color: '#8b949e', marginTop: '0.5rem', textAlign: 'center' }}>
                Burst inlier {inlierCount} · Soft {softCount} · Rejected {rejectedCount}
                {' · '}Sync-driving bursts {syncDrivingCount}
                {nonSyncDrivingCount > 0 ? ` · Non-sync-driving bursts ${nonSyncDrivingCount}` : ''}
                {' · '}DF11 early {dfEarlyCount} · on time {dfOnTimeCount} · late {dfLateCount}
                {syncState?.sync_jitter_deg != null ? ` · Sync jitter ±${syncState.sync_jitter_deg.toFixed(1)}°` : ''}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '12px', marginTop: '0.8rem' }}>
                <div className={styles.alignmentWrap}>
                  <svg
                    width="100%"
                    height={phaseChartH}
                    viewBox={`0 0 ${chartW} ${phaseChartH}`}
                    className={styles.alignmentSvg}
                  >
                    {[1, 0.5, 0, -0.5, -1].map(f => {
                      const residual = yAbs * f
                      const y = phaseResidualToY(residual)
                      return (
                        <g key={`phase-y-${f}`}>
                          <line
                            x1={phasePadL}
                            y1={y}
                            x2={chartW - phasePadR}
                            y2={y}
                            stroke={residual === 0 ? '#58a6ff88' : '#30363d'}
                            strokeDasharray={residual === 0 ? '0' : '4 4'}
                          />
                          <text x={phasePadL - 8} y={y + 4} textAnchor="end" className={styles.axisLabel}>
                            {residual.toFixed(0)}°
                          </text>
                        </g>
                      )
                    })}
                    {[0, 90, 180, 270, 360].map(phaseDeg => {
                      const x = phasePadL + (phaseDeg / 360) * phasePlotW
                      return (
                        <g key={`phase-x-${phaseDeg}`}>
                          <line
                            x1={x}
                            y1={phasePadT}
                            x2={x}
                            y2={phaseChartH - phasePadB}
                            stroke={phaseDeg === 0 || phaseDeg === 360 ? '#58a6ff66' : '#21262d'}
                            strokeDasharray={phaseDeg === 0 || phaseDeg === 360 ? '0' : '3 5'}
                          />
                          <text x={x} y={phaseChartH - 10} textAnchor="middle" className={styles.axisLabel}>
                            {phaseDeg}°
                          </text>
                        </g>
                      )
                    })}
                    <text x={phasePadL + phasePlotW / 2} y={18} textAnchor="middle" className={styles.axisLabel}>
                      Folded residual vs bearing
                    </text>
                    <text x={phasePadL + phasePlotW / 2} y={phaseChartH - 4} textAnchor="middle" className={styles.axisLabel}>
                      Static 0-360° rotation domain
                    </text>
                    {phaseResidualRows.map(row => (
                      <circle
                        key={row.key}
                        cx={phaseToX(row.phaseDeg)}
                        cy={phaseResidualToY(row.residualDeg)}
                        r={2.2}
                        fill={burstSyncClassColor(row.classification)}
                        opacity={0.78}
                      >
                        <title>{`${row.icao ?? '—'} bearing ${row.phaseDeg.toFixed(1)}° residual ${row.residualDeg.toFixed(2)}°`}</title>
                      </circle>
                    ))}
                    {foldedPhaseCurve.length > 1 && (
                      <polyline
                        fill="none"
                        stroke="#ffd166"
                        strokeWidth={1.9}
                        points={foldedPhaseCurve
                          .map(bin => `${phaseToX(bin.phaseCenterDeg).toFixed(1)},${phaseResidualToY(bin.residualDeg).toFixed(1)}`)
                          .join(' ')}
                      />
                    )}
                  </svg>
                </div>
                <div className={styles.alignmentWrap}>
                  <svg
                    width="100%"
                    height={auxChartH}
                    viewBox={`0 0 ${chartW} ${auxChartH}`}
                    className={styles.alignmentSvg}
                  >
                    {[1, 0.5, 0, -0.5, -1].map(f => {
                      const residual = yAbs * f
                      const y = phaseResidualToY(residual)
                      return (
                        <g key={`range-y-${f}`}>
                          <line
                            x1={phasePadL}
                            y1={y}
                            x2={chartW - phasePadR}
                            y2={y}
                            stroke={residual === 0 ? '#58a6ff88' : '#30363d'}
                            strokeDasharray={residual === 0 ? '0' : '4 4'}
                          />
                          <text x={phasePadL - 8} y={y + 4} textAnchor="end" className={styles.axisLabel}>
                            {residual.toFixed(0)}°
                          </text>
                        </g>
                      )
                    })}
                    {[0, 0.25, 0.5, 0.75, 1].map(f => {
                      const rangeNm = maxRangeNm * f
                      const x = rangeToX(rangeNm)
                      return (
                        <g key={`range-x-${f}`}>
                          <line
                            x1={x}
                            y1={phasePadT}
                            x2={x}
                            y2={auxChartH - phasePadB}
                            stroke={f === 0 ? '#58a6ff66' : '#21262d'}
                            strokeDasharray={f === 0 ? '0' : '3 5'}
                          />
                          <text x={x} y={auxChartH - 10} textAnchor="middle" className={styles.axisLabel}>
                            {rangeNm.toFixed(0)} NM
                          </text>
                        </g>
                      )
                    })}
                    <text x={phasePadL + phasePlotW / 2} y={18} textAnchor="middle" className={styles.axisLabel}>
                      Residual vs range
                    </text>
                    <text x={phasePadL + phasePlotW / 2} y={auxChartH - 4} textAnchor="middle" className={styles.axisLabel}>
                      Corrected residual by aircraft range
                    </text>
                    {rangeResidualRows.map(row => (
                      <circle
                        key={`range-${row.key}`}
                        cx={rangeToX(row.rangeNm)}
                        cy={phaseResidualToY(row.residualCorrectedDeg)}
                        r={2.2}
                        fill={burstSyncClassColor(row.classification)}
                        opacity={0.78}
                      >
                        <title>{`${row.icao ?? '—'} range ${row.rangeNm.toFixed(1)} NM residual ${row.residualCorrectedDeg.toFixed(2)}°`}</title>
                      </circle>
                    ))}
                  </svg>
                </div>
              </div>
            </>
          )}
        </>
      ) : (
        <>
          {legacyPeriodUs == null || legacyPeriodUs <= 0 || legacyVisibleSpanUs == null || sortedLegacyIcaos.length === 0 ? (
            <div className={styles.empty}>No legacy live DF alignment data yet for this IID.</div>
          ) : (
            <>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginBottom: '0.7rem' }}>
                <button
                  type="button"
                  className={styles.metricPill}
                  onClick={() => onSelectIcao(null)}
                  style={{
                    cursor: 'pointer',
                    background: selectedIcao == null ? '#388bfd22' : '#0f141b',
                    color: selectedIcao == null ? '#d2e4ff' : '#8b949e',
                  }}
                >
                  All ICAOs ({sortedLegacyIcaos.length})
                </button>
                {sortedLegacyIcaos.slice(0, 12).map(entry => (
                  <button
                    key={entry.icao}
                    type="button"
                    className={styles.metricPill}
                    onClick={() => onSelectIcao(selectedIcao === entry.icao ? null : entry.icao)}
                    style={{
                      cursor: 'pointer',
                      background: selectedIcao === entry.icao ? '#388bfd22' : '#0f141b',
                      color: selectedIcao === entry.icao ? '#d2e4ff' : '#8b949e',
                    }}
                  >
                    <span style={{ fontFamily: 'SFMono-Regular, Consolas, monospace' }}>{entry.icao}</span>
                    <span className={styles.metricValue}>{entry.arrivals_us?.length ?? 0}</span>
                  </button>
                ))}
              </div>
              <div className={styles.alignmentWrap}>
                <svg
                  width={chartW}
                  height={padT + padB + Math.max(legacyDisplayIcaos.length, 1) * 18}
                  viewBox={`0 0 ${chartW} ${padT + padB + Math.max(legacyDisplayIcaos.length, 1) * 18}`}
                  className={styles.alignmentSvg}
                >
                  {Array.from({ length: Math.max(1, Math.ceil(legacyVisibleSpanUs / legacyPeriodUs)) + 1 }, (_, idx) => {
                    const relUs = idx * legacyPeriodUs
                    const x = padL + (relUs / legacyVisibleSpanUs) * plotW
                    return (
                      <g key={idx}>
                        <line
                          x1={x}
                          y1={padT - 10}
                          x2={x}
                          y2={padT + Math.max(legacyDisplayIcaos.length, 1) * 18}
                          stroke={idx === 0 ? '#58a6ff66' : '#30363d'}
                          strokeDasharray={idx === 0 ? '0' : '4 4'}
                        />
                        <text x={x} y={18} textAnchor="middle" className={styles.axisLabel}>
                          {(idx * legacyPeriodS).toFixed(1)}s
                        </text>
                      </g>
                    )
                  })}
                  <text
                    x={padL + plotW / 2}
                    y={padT + Math.max(legacyDisplayIcaos.length, 1) * 18 + 18}
                    textAnchor="middle"
                    className={styles.axisLabel}
                  >
                    Elapsed time since each ICAO&apos;s first observed reply
                  </text>
                  {legacyDisplayIcaos.map((entry, rowIdx) => {
                    const rowH = 18
                    const y = padT + rowIdx * rowH + rowH / 2
                    const arrivals = Array.isArray(entry.arrivals_us) ? entry.arrivals_us : []
                    const familySeries = Array.isArray(entry.family_series) && entry.family_series.length > 0
                      ? entry.family_series
                      : [{
                          family: entry.classification ?? 'unclassified',
                          arrivals_us: arrivals,
                          multiplier: entry.multiplier,
                        }]
                    const isSelected = selectedIcao === entry.icao
                    return (
                      <g key={entry.icao}>
                        <rect
                          x={padL}
                          y={padT + rowIdx * rowH + 1}
                          width={plotW}
                          height={rowH - 2}
                          fill={isSelected ? '#388bfd12' : rowIdx % 2 === 0 ? '#0f1720' : '#111820'}
                          rx={3}
                        />
                        <text
                          x={padL - 10}
                          y={y + 4}
                          textAnchor="end"
                          className={styles.alignmentLabel}
                          fill={isSelected ? '#ffd166' : '#c9d1d9'}
                          onClick={() => onSelectIcao(entry.icao === selectedIcao ? null : entry.icao)}
                        >
                          {entry.icao}  {arrivals.length}
                        </text>
                        {familySeries.map((series, seriesIdx) => {
                          const seriesArrivals = Array.isArray(series.arrivals_us) ? series.arrivals_us : []
                          const firstArrival = seriesArrivals.length > 0 ? seriesArrivals[0] : null
                          const relArrivals = firstArrival == null
                            ? []
                            : seriesArrivals
                                .map(arrival => arrival - firstArrival)
                                .filter(relUs => relUs >= 0 && relUs <= legacyVisibleSpanUs)
                          const pointColor = legacyClassColor(series.family, isSelected)
                          const yOffset = series.family.startsWith('primary') ? -2 : 2
                          return relArrivals.map((relUs, idx) => (
                            <circle
                              key={`${entry.icao}-${series.family}-${seriesIdx}-${idx}-${relUs}`}
                              cx={padL + (relUs / legacyVisibleSpanUs) * plotW}
                              cy={y + yOffset}
                              r={isSelected ? 4 : 3}
                              fill={pointColor}
                              opacity={0.9}
                              stroke={isSelected ? '#ffd166' : 'none'}
                              strokeWidth={isSelected ? 1.25 : 0}
                            />
                          ))
                        })}
                      </g>
                    )
                  })}
                </svg>
              </div>
              <div style={{ fontSize: '0.74rem', color: '#8b949e', marginTop: '0.5rem', textAlign: 'center' }}>
                Legacy per-aircraft alignment keeps the broad operational timing picture across all observed ICAOs.
              </div>
            </>
          )}
        </>
      )}
    </section>
  )
}

// Bearing from (lat1,lon1) to (lat2,lon2) in degrees (0=N, CW)
function bearing(lat1, lon1, lat2, lon2) {
  const φ1 = lat1 * Math.PI / 180, φ2 = lat2 * Math.PI / 180
  const Δλ = (lon2 - lon1) * Math.PI / 180
  const x = Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ)
  const y = Math.sin(Δλ) * Math.cos(φ2)
  return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360
}

function haversineNm(lat1, lon1, lat2, lon2) {
  const R_M = 6371000
  const φ1 = lat1 * Math.PI / 180
  const φ2 = lat2 * Math.PI / 180
  const Δφ = (lat2 - lat1) * Math.PI / 180
  const Δλ = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(Δφ / 2) ** 2 + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) ** 2
  const d = 2 * R_M * Math.asin(Math.sqrt(a))
  return d / 1852
}

const US_PER_NM_LIGHT = (1852 / 299792458) * 1_000_000

function computePropagationDelayUs(rangeNm) {
  if (!Number.isFinite(rangeNm) || rangeNm <= 0) return 0
  return rangeNm * US_PER_NM_LIGHT
}

function waveformBinIndex(phaseDeg, nBins) {
  const phase = ((phaseDeg % 360) + 360) % 360
  const width = 360 / nBins
  let idx = Math.floor(phase / width)
  if (idx < 0) idx = 0
  if (idx >= nBins) idx = nBins - 1
  return idx
}

function applyPhaseWaveformCorrection(waveformBins, phaseDeg, applied) {
  if (!applied || !Array.isArray(waveformBins) || waveformBins.length === 0) return 0
  const nBins = waveformBins.length
  const idx = waveformBinIndex(phaseDeg, nBins)
  const width = 360 / nBins
  const centre = (idx + 0.5) * width
  let delta = (((phaseDeg % 360) + 360) % 360) - centre
  if (delta > width) delta -= 360
  else if (delta < -width) delta += 360
  const other = delta >= 0 ? (idx + 1) % nBins : (idx - 1 + nBins) % nBins
  const frac = Math.abs(delta) / width
  const a = Number(waveformBins[idx]?.correction_deg ?? 0)
  const b = Number(waveformBins[other]?.correction_deg ?? 0)
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0
  return ((1 - frac) * a) + (frac * b)
}

function predictBearingFromSyncModel(syncState, arrivalUs, { rangeNm = null, waveformBins = null } = {}) {
  if (!syncState) return null
  const periodUs = Number(syncState.period_s) * 1_000_000
  const phaseEpochUs = Number(syncState.phase_epoch_us)
  const phaseOffsetDeg = Number(syncState.phase_offset_deg)
  if (!Number.isFinite(periodUs) || periodUs <= 0 || !Number.isFinite(phaseEpochUs) || !Number.isFinite(phaseOffsetDeg)) {
    return null
  }
  const propDelayEnabled = Boolean(syncState.prop_delay_enabled)
  const effectiveUs = propDelayEnabled ? (arrivalUs - computePropagationDelayUs(rangeNm)) : arrivalUs
  const phaseInRotDeg = ((((effectiveUs - phaseEpochUs) / periodUs) * 360) % 360 + 360) % 360
  let predictedDeg = (phaseInRotDeg + phaseOffsetDeg) % 360
  if (Boolean(syncState.waveform_enabled) && Boolean(syncState.waveform_applied)) {
    const corr = applyPhaseWaveformCorrection(waveformBins, phaseInRotDeg, true)
    predictedDeg = (predictedDeg - corr + 360) % 360
  }
  return { predictedDeg, phaseInRotDeg }
}

function beamResidualAtTimestampDeg(bearingDeg, sampleUs, beamAnchor, periodUs) {
  if (
    beamAnchor == null
    || periodUs == null
    || !Number.isFinite(bearingDeg)
    || !Number.isFinite(sampleUs)
  ) {
    return null
  }
  const beamAtSample = (
    (beamAnchor.ref_bearing_deg + ((sampleUs - beamAnchor.ref_arrival_us) / periodUs) * 360) % 360 + 360
  ) % 360
  return wrapSignedResidualDeg(bearingDeg, beamAtSample)
}

const SYNC_STALE_US = 15_000_000  // 15 s without DF11 updates → re-evaluate anchor

function ReceiverCentredRadarField({
  iid,
  selectedRow,
  syncSnapshot,
  frameData,
  fmLocationData,
  refInfo,
  receiverPos,
  timingPacket,
}) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const rangeAxisMaxRef = useRef(null)
  const displayNowUsRef = useRef(0)
  const displayNowWallRef = useRef(performance.now())
  const beamAnchorRef = useRef(null)
  const lastDrawMsRef = useRef(0)
  const canvasSizeRef = useRef({ width: 0, height: 0 })
  const lastSeenUsRef = useRef({})   // icao → latest DF11 arrival_us seen
  const [syncOverrideIcao, setSyncOverrideIcao] = useState(null)
  const [displaySyncIcao, setDisplaySyncIcao] = useState(null)

  const burstTimeline = syncSnapshot
  const timingView = useTimingEventBuffer(timingPacket, RADAR_FIELD_PERSISTENCE_US, RADAR_FIELD_BUFFER_MAX)

  useEffect(() => {
    const packetNowUs = Number(timingView?.nowUs ?? 0)
    const wallNowMs = performance.now()
    const projectedDisplayNowUs = displayNowUsRef.current > 0
      ? displayNowUsRef.current + Math.max(0, wallNowMs - displayNowWallRef.current) * 1000
      : packetNowUs
    displayNowUsRef.current = Math.max(projectedDisplayNowUs, packetNowUs)
    displayNowWallRef.current = wallNowMs
  }, [timingView?.nowUs])

  useEffect(() => {
    rangeAxisMaxRef.current = null
  }, [iid])

  const frames = frameData?.frames ?? []
  const preferredRefIcao = refInfo?.ref_icao ?? null
  const effectivePrefIcao = syncOverrideIcao ?? preferredRefIcao
  const radarFieldEvents = useMemo(() => {
    return selectMessageFieldEvents({
      events: timingView?.events ?? [],
      renderNowUs: Number(timingView?.nowUs ?? 0),
      persistenceUs: RADAR_FIELD_PERSISTENCE_US + MESSAGE_FIELD_RENDER_HOLDBACK_US,
      trafficFilter: 'df_surveillance',
      iidFilter: iid == null ? 'all' : iid,
    }).filter(ev => Number.isFinite(ev.bearing_deg) && Number.isFinite(ev.range_nm))
  }, [iid, timingView?.events, timingView?.nowUs])
  const latestFrame = [...frames].reverse().find(f => {
    if (!(f.quality === 'good' || f.quality === 'marginal')) return false
    if (effectivePrefIcao != null) return f.ref_icao === effectivePrefIcao
    const currentRefIcao = beamAnchorRef.current?.ref_icao
    return currentRefIcao ? f.ref_icao === currentRefIcao : true
  }) ?? [...frames].reverse().find(f => f.quality === 'good' || f.quality === 'marginal')
  const syncState = burstTimeline?.sync_state ?? null
  const syncWaveformBins = Array.isArray(burstTimeline?.waveform_bins) ? burstTimeline.waveform_bins : []
  const hasAuthoritativeSync = (
    Boolean(syncState?.usable)
    && Number.isFinite(Number(syncState?.period_s))
    && Number(syncState?.period_s) > 0
    && Number.isFinite(Number(syncState?.phase_epoch_us))
    && Number.isFinite(Number(syncState?.phase_offset_deg))
  )
  const period_s = hasAuthoritativeSync
    ? Number(syncState.period_s)
    : (beamAnchorRef.current?.period_s ?? latestFrame?.period_s ?? selectedRow?.period_s ?? null)
  const fmPos = fmLocationData?.status === 'LOCALISED' ? fmLocationData : null

  useEffect(() => {
    for (const ev of radarFieldEvents) {
      if (ev.df !== 11 || ev.iid !== iid) continue
      const prev = lastSeenUsRef.current[ev.icao] ?? 0
      if (ev.arrival_us > prev) lastSeenUsRef.current[ev.icao] = ev.arrival_us
    }
  }, [radarFieldEvents, iid])

  useEffect(() => {
    if (hasAuthoritativeSync) return
    const anchor = beamAnchorRef.current
    if (!anchor) return
    const nowUs = Number(timingView?.nowUs ?? 0)
    if (nowUs === 0) return
    const lastSeen = lastSeenUsRef.current[anchor.ref_icao] ?? 0
    if (lastSeen === 0 || nowUs - lastSeen <= SYNC_STALE_US) return
    const candidates = [preferredRefIcao, ...frames.map(f => f.ref_icao)].filter(Boolean)
    const newIcao = candidates.find(icao => (lastSeenUsRef.current[icao] ?? 0) > nowUs - SYNC_STALE_US) ?? null
    if (newIcao === anchor.ref_icao) return
    beamAnchorRef.current = null
    setSyncOverrideIcao(newIcao !== preferredRefIcao ? newIcao : null)
    setDisplaySyncIcao(null)
  }, [hasAuthoritativeSync, timingView?.nowUs, preferredRefIcao, frames])

  useEffect(() => {
    if (hasAuthoritativeSync) {
      beamAnchorRef.current = null
      setDisplaySyncIcao(null)
      return
    }
    if (!fmPos || !latestFrame) {
      beamAnchorRef.current = null
      return
    }
    const nextAnchor = {
      frame_index: latestFrame.frame_index,
      ref_icao: latestFrame.ref_icao,
      ref_arrival_us: latestFrame.ref_arrival_us,
      ref_bearing_deg: bearing(fmPos.lat, fmPos.lon, latestFrame.ref_lat, latestFrame.ref_lon),
      period_s: latestFrame.period_s ?? selectedRow?.period_s ?? null,
    }
    const current = beamAnchorRef.current
    if (
      current == null
      || nextAnchor.ref_arrival_us > current.ref_arrival_us
      || nextAnchor.ref_icao !== current.ref_icao
      || !Number.isFinite(current.period_s)
    ) {
      beamAnchorRef.current = nextAnchor
      setDisplaySyncIcao(nextAnchor.ref_icao)
    }
  }, [
    hasAuthoritativeSync,
    fmPos?.lat,
    fmPos?.lon,
    latestFrame?.frame_index,
    latestFrame?.period_s,
    latestFrame?.ref_icao,
    latestFrame?.ref_arrival_us,
    latestFrame?.ref_lat,
    latestFrame?.ref_lon,
    selectedRow?.period_s,
  ])

  function handleResetSync() {
    beamAnchorRef.current = null
    setDisplaySyncIcao(null)
    const nowUs = Number(timingView?.nowUs ?? 0)
    if (nowUs > 0) {
      const candidates = [preferredRefIcao, ...frames.map(f => f.ref_icao)].filter(Boolean)
      const newIcao = candidates.find(icao => (lastSeenUsRef.current[icao] ?? 0) > nowUs - SYNC_STALE_US) ?? null
      setSyncOverrideIcao(newIcao !== preferredRefIcao ? newIcao : null)
    } else {
      setSyncOverrideIcao(null)
    }
  }

  useEffect(() => {
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas || document.hidden) return
      const frameNowMs = performance.now()
      if ((frameNowMs - lastDrawMsRef.current) < 50) return
      lastDrawMsRef.current = frameNowMs

      const width = canvas.offsetWidth || 720
      const height = canvas.offsetHeight || 720
      if (canvasSizeRef.current.width !== width || canvasSizeRef.current.height !== height) {
        canvas.width = width
        canvas.height = height
        canvasSizeRef.current = { width, height }
      }

      const ctx = canvas.getContext('2d')
      const interpUs = Math.max(0, frameNowMs - displayNowWallRef.current) * 1000
      const liveClockUs = Math.max(0, displayNowUsRef.current + interpUs)
      const renderNowUs = Math.max(0, liveClockUs - MESSAGE_FIELD_RENDER_HOLDBACK_US)
      const rawCutoffUs = Math.max(0, renderNowUs - RADAR_FIELD_PERSISTENCE_US)
      const rawEvents = []
      for (let index = radarFieldEvents.length - 1; index >= 0; index -= 1) {
        const ev = radarFieldEvents[index]
        if (ev.arrival_us > renderNowUs) continue
        if (ev.arrival_us < rawCutoffUs) break
        rawEvents.push(ev)
      }

      ctx.fillStyle = '#0b0c10'
      ctx.fillRect(0, 0, width, height)

      const left = 18
      const top = 18
      const right = 18
      const bottom = 28
      const plotW = Math.max(80, width - left - right)
      const plotH = Math.max(80, height - top - bottom)
      const cx = left + plotW / 2
      const cy = top + plotH / 2
      const radius = Math.min(plotW / 2 - 8, plotH / 2 - 8)

      ctx.fillStyle = '#8b949e'
      ctx.font = '10px monospace'

      const rawMaxRange = rawEvents.reduce((max, ev) => Math.max(max, ev.range_nm ?? 0), 0)
      const targetRangeMax = Math.max(50, Math.max(rawMaxRange, 1) * RADAR_FIELD_AXIS_HEADROOM)
      const nextRangeState = smoothAxisMax(targetRangeMax, rangeAxisMaxRef.current, frameNowMs)
      rangeAxisMaxRef.current = nextRangeState
      const maxRangeNm = nextRangeState.value

      ctx.strokeStyle = '#21262d'
      ctx.lineWidth = 1
      for (let ring = 0.25; ring <= 1.0; ring += 0.25) {
        ctx.beginPath()
        ctx.arc(cx, cy, radius * ring, 0, Math.PI * 2)
        ctx.stroke()
        const ringRange = maxRangeNm * ring
        ctx.textAlign = 'left'
        ctx.textBaseline = 'middle'
        ctx.fillText(`${ringRange.toFixed(ringRange >= 10 ? 0 : 1)} nm`, cx + 8, cy - radius * ring)
      }
      for (const deg of [0, 45, 90, 135, 180, 225, 270, 315]) {
        const rad = (deg - 90) * Math.PI / 180
        ctx.beginPath()
        ctx.moveTo(cx, cy)
        ctx.lineTo(cx + Math.cos(rad) * radius, cy + Math.sin(rad) * radius)
        ctx.stroke()
      }

      let beamOrigin = { x: cx, y: cy }
      let beamSweepDeg = null
      const beamAnchor = beamAnchorRef.current
      const periodUs = beamAnchor?.period_s != null && beamAnchor.period_s > 0
        ? beamAnchor.period_s * 1_000_000
        : null
      const hasRadarOrigin = (
        fmPos
        && receiverPos
        && Number.isFinite(receiverPos.lat)
        && Number.isFinite(receiverPos.lon)
      )
      if (hasRadarOrigin) {
        const radarBearingDeg = bearing(receiverPos.lat, receiverPos.lon, fmPos.lat, fmPos.lon)
        const radarRangeNm = haversineNm(receiverPos.lat, receiverPos.lon, fmPos.lat, fmPos.lon)
        const radarTheta = (radarBearingDeg - 90) * Math.PI / 180
        const radarRadius = Math.min(radius, (radarRangeNm / Math.max(1e-6, maxRangeNm)) * radius)
        beamOrigin = {
          x: cx + Math.cos(radarTheta) * radarRadius,
          y: cy + Math.sin(radarTheta) * radarRadius,
        }
        if (hasAuthoritativeSync) {
          const prediction = predictBearingFromSyncModel(syncState, renderNowUs, {
            waveformBins: syncWaveformBins,
          })
          if (prediction) {
            beamSweepDeg = prediction.predictedDeg
          }
        } else if (beamAnchor?.ref_bearing_deg != null && beamAnchor?.ref_arrival_us != null && periodUs != null && renderNowUs > 0) {
          const sweepTurns = ((renderNowUs - beamAnchor.ref_arrival_us) / periodUs) % 1
          const normalizedTurns = (sweepTurns + 1) % 1
          beamSweepDeg = (beamAnchor.ref_bearing_deg + normalizedTurns * 360.0) % 360.0
        }
      }

      if (beamSweepDeg != null) {
        const sweepRad = (beamSweepDeg - 90) * Math.PI / 180
        const beamLength = Math.max(width, height) * 1.8
        const trailRad = 4 * Math.PI / 180
        ctx.beginPath()
        ctx.moveTo(beamOrigin.x, beamOrigin.y)
        ctx.lineTo(
          beamOrigin.x + Math.cos(sweepRad - trailRad) * beamLength,
          beamOrigin.y + Math.sin(sweepRad - trailRad) * beamLength,
        )
        ctx.lineTo(
          beamOrigin.x + Math.cos(sweepRad + trailRad) * beamLength,
          beamOrigin.y + Math.sin(sweepRad + trailRad) * beamLength,
        )
        ctx.closePath()
        ctx.fillStyle = 'rgba(63, 185, 80, 0.10)'
        ctx.fill()

        ctx.beginPath()
        ctx.moveTo(beamOrigin.x, beamOrigin.y)
        ctx.lineTo(
          beamOrigin.x + Math.cos(sweepRad) * beamLength,
          beamOrigin.y + Math.sin(sweepRad) * beamLength,
        )
        ctx.strokeStyle = 'rgba(63, 185, 80, 0.72)'
        ctx.lineWidth = 1.5
        ctx.stroke()
      }

      for (const ev of rawEvents) {
        const r = (ev.range_nm / Math.max(1e-6, maxRangeNm)) * radius
        const theta = (ev.bearing_deg - 90) * Math.PI / 180
        const x = cx + Math.cos(theta) * r
        const y = cy + Math.sin(theta) * r
        const ageRatio = (renderNowUs - ev.arrival_us) / Math.max(1, RADAR_FIELD_PERSISTENCE_US)
        const alpha = 0.18 + (1 - Math.min(1, ageRatio)) * 0.8
        const timingResidual = hasAuthoritativeSync
          ? (() => {
            const prediction = predictBearingFromSyncModel(syncState, ev.arrival_us, {
              rangeNm: Number(ev?.range_nm),
              waveformBins: syncWaveformBins,
            })
            return prediction ? wrapSignedResidualDeg(ev.bearing_deg, prediction.predictedDeg) : null
          })()
          : beamResidualAtTimestampDeg(ev.bearing_deg, ev.arrival_us, beamAnchor, periodUs)
        const timingClass = classifyTimingResidual(timingResidual, POSITION_VERIFICATION_ON_TIME_THRESHOLD_DEG)
        ctx.fillStyle = timingClassColor(timingClass)
        ctx.globalAlpha = alpha
        const size = ev.msg_len >= 14 ? 3.5 : 2.5
        ctx.fillRect(x - size / 2, y - size / 2, size, size)
        ctx.globalAlpha = 1
      }

      ctx.beginPath()
      ctx.arc(cx, cy, 4.5, 0, Math.PI * 2)
      ctx.fillStyle = '#58a6ff'
      ctx.fill()

      if (hasRadarOrigin) {
        ctx.beginPath()
        ctx.arc(beamOrigin.x, beamOrigin.y, 4.5, 0, Math.PI * 2)
        ctx.fillStyle = '#d29922'
        ctx.fill()
      }

      ctx.textAlign = 'left'
      ctx.textBaseline = 'alphabetic'
      ctx.fillStyle = '#8b949e'
      ctx.fillText('Receiver-centred DF11 field', left, height - 12)
      ctx.textAlign = 'right'
      ctx.fillText(`${rawEvents.length} raw DF11`, width - 12, height - 12)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current)
    }
  }, [
    hasAuthoritativeSync,
    fmPos?.lat,
    fmPos?.lon,
    iid,
    latestFrame?.frame_index,
    period_s,
    preferredRefIcao,
    radarFieldEvents,
    receiverPos?.lat,
    receiverPos?.lon,
    syncState,
    syncWaveformBins,
  ])

  const syncLabel = hasAuthoritativeSync
    ? (
      <span style={{ color: '#3fb950', marginLeft: '0.6rem', fontSize: '0.82rem' }}>
        sync: refined model
      </span>
    )
    : displaySyncIcao
    ? (
      <span style={{ fontFamily: 'SFMono-Regular, Consolas, monospace', color: '#58a6ff', marginLeft: '0.6rem', fontSize: '0.82rem' }}>
        sync: {displaySyncIcao}
      </span>
    )
    : null
  const rawDf11Count = radarFieldEvents.length

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>
            Position Verification{syncLabel}
          </div>
          <div className={styles.sectionLead}>
            Live DF11 arrivals are the primary beam-versus-reply check on this receiver-centred field.
            {' '}Blue = early, green = on time, red = late.
          </div>
        </div>
        <div className={styles.metricRow}>
          <span className={styles.metricPill}>
            Raw DF11 <span className={styles.metricValue}>{rawDf11Count}</span>
          </span>
          <span className={styles.metricPill}>
            Period <span className={styles.metricValue}>{period_s != null ? `${period_s.toFixed(4)}s` : '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Refresh <span className={styles.metricValue}>{timingView?.nowUs ? 'live stream' : 'connecting'}</span>
          </span>
          <span className={styles.metricPill}>
            Predictor <span className={styles.metricValue}>{hasAuthoritativeSync ? 'refined sync' : 'frame anchor'}</span>
          </span>
          <button
            type="button"
            className={styles.actionButton}
            onClick={handleResetSync}
            title="Re-assess the fallback frame-anchor synchronisation"
          >
            Reset Sync
          </button>
        </div>
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', padding: '0.5rem 0' }}>
        <canvas
          ref={canvasRef}
          width={720}
          height={720}
          style={{ borderRadius: '4px', background: '#0b0c10', width: '100%', maxWidth: '720px', aspectRatio: '1 / 1' }}
        />
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', gap: '0.45rem', flexWrap: 'wrap', marginTop: '0.25rem' }}>
        <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#58a6ff' }} />Early</span>
        <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#3fb950' }} />On time</span>
        <span className={styles.legendChip}><span style={{ ...legendDotStyle, background: '#ff7b72' }} />Late</span>
      </div>
      <div style={{ fontSize: '0.72rem', color: '#8b949e', textAlign: 'center', marginTop: '0.25rem' }}>
        Receiver shown in blue. {fmPos ? 'Solved radar shown in amber.' : 'Run FM to project the radar beam origin.'}
        {' '}Use live DF11 point motion against the beam sweep as the primary sync check.
      </div>
    </section>
  )
}

function FMDataFunnel({ iid }) {
  const [funnel, setFunnel] = useState(null)

  useEffect(() => {
    if (iid == null) { setFunnel(null); return }
    const controller = new AbortController()
    let cancelled = false
    fetch(`${API_BASE}/api/radar/iids/${iid}/fm-diagnostics`, { signal: controller.signal })
      .then(r => r.json())
      .then(d => { if (!cancelled) setFunnel(d) })
      .catch(() => {})
    return () => { cancelled = true; controller.abort() }
  }, [iid])

  if (!funnel) return <div className={styles.empty}>Loading diagnostics…</div>
  if (!funnel.available) return <div className={styles.empty}>{funnel.reason}</div>

  const f = funnel.data_funnel || {}
  const fm = funnel.forward_model || {}

  return (
    <div style={{ padding: '0.5rem 0' }}>
      <div style={{ fontSize: '0.78rem', color: '#8b949e', marginBottom: '0.5rem' }}>
        Data funnel — where are observations being lost?
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginBottom: '0.5rem' }}>
        <span className={styles.metricPill}>
          Sweeps: <span className={styles.metricValue}>{f.sweeps_in_history ?? '—'}</span>
        </span>
        <span className={styles.metricPill}>
          Max a/c/sweep: <span className={styles.metricValue}>{f.max_aircraft_per_sweep ?? '—'}</span>
        </span>
        <span className={styles.metricPill}>
          Frames built: <span className={styles.metricValue}>{f.sweep_frames_built ?? 0}</span>
        </span>
        <span className={styles.metricPill}>
          Good: <span className={styles.metricValue} style={{ color: (f.good_frames || 0) > 0 ? '#3fb950' : '#ff7b72' }}>
            {f.good_frames ?? 0}
          </span>
        </span>
        <span className={styles.metricPill}>
          Marginal: <span className={styles.metricValue}>{f.marginal_frames ?? 0}</span>
        </span>
        <span className={styles.metricPill}>
          Observations: <span className={styles.metricValue}>{f.total_observations ?? 0}</span>
        </span>
        <span className={styles.metricPill}>
          Unique a/c: <span className={styles.metricValue}>{f.unique_aircraft ?? 0}</span>
        </span>
      </div>
      {f.bottleneck != null && (
        <div style={{ fontSize: '0.82rem', color: '#ff7b72', marginBottom: '0.4rem' }}>
          ⚠ {f.bottleneck}
        </div>
      )}
      {fm.has_position && (
        <div style={{ fontSize: '0.82rem', color: '#3fb950', marginTop: '0.4rem' }}>
          ✓ FM position exists: ({fm.fm_lat?.toFixed(4)}, {fm.fm_lon?.toFixed(4)}) uncertainty {fm.fm_cep_m ? `${Math.round(fm.fm_cep_m)}m` : '?'}
          {fm.n_observations ? ` from ${fm.n_observations} obs` : ''}
        </div>
      )}
    </div>
  )
}

function FMStatusPanel({ iid, panelData = null, loading = false }) {

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div><div className={styles.cardTitle}>Frame Accumulation</div></div>
        </div>
        <div className={styles.empty}>Select an IID to inspect frame accumulation status.</div>
      </section>
    )
  }

  if (!panelData && loading) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div><div className={styles.cardTitle}>Frame Accumulation</div></div>
        </div>
        <div className={styles.empty}>Loading…</div>
      </section>
    )
  }

  if (!panelData?.available) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div><div className={styles.cardTitle}>Frame Accumulation</div></div>
        </div>
        <div className={styles.empty}>{panelData?.reason ?? 'No data available.'}</div>
      </section>
    )
  }

  const rotation = panelData.rotation_model ?? {}
  const funnel = panelData.data_funnel ?? {}
  const fm = panelData.forward_model ?? {}
  const acc = panelData.frame_accumulation ?? {}
  const lastRun = fm.last_run ?? null
  const blocker = funnel.bottleneck
    || (rotation.multi_radar_flag ? 'Multi-radar IID is excluded from FM.' : null)
    || (rotation.period_s == null ? 'Waiting for a stable radar period.' : null)

  const centroid = acc.centroid ?? null
  const nEstimates = acc.n_estimates ?? 0
  const errorM = acc.error_vs_manual_m ?? null

  return (
    <section className={styles.card} data-panel="fm-status">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Frame Accumulation</div>
          <div className={styles.sectionLead}>
            Each sweep frame is solved independently. Estimates accumulate in a rolling buffer; a weighted centroid is used as the FM position once 20+ estimates are available. The FM pipeline runs automatically every 30 seconds.
          </div>
        </div>
      </div>

      <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
        <span className={styles.metricPill}>
          Estimates in buffer <span className={styles.metricValue} style={{ color: nEstimates >= 20 ? '#3fb950' : nEstimates > 0 ? '#d29922' : '#8b949e' }}>{nEstimates}</span>
        </span>
        <span className={styles.metricPill}>
          Sweep frames <span className={styles.metricValue}>{(funnel.good_frames ?? 0) + (funnel.marginal_frames ?? 0)}</span>
        </span>
        <span className={styles.metricPill}>
          Unique A/C <span className={styles.metricValue}>{funnel.unique_aircraft ?? 0}</span>
        </span>
        {errorM != null && (
          <span className={styles.metricPill}>
            Error vs manual <span className={styles.metricValue} style={{ color: errorM < 2000 ? '#3fb950' : errorM < 10000 ? '#d29922' : '#ff7b72' }}>{(errorM / 1000).toFixed(1)} km</span>
          </span>
        )}
      </div>

      {blocker && (
        <div style={{ fontSize: '0.82rem', color: '#ff7b72', marginBottom: '0.6rem' }}>
          Blocker: {blocker}
        </div>
      )}

      {centroid ? (
        <div style={{ fontSize: '0.82rem', color: '#3fb950', marginBottom: '0.6rem' }}>
          Centroid ({nEstimates} estimates): {centroid.lat.toFixed(4)}, {centroid.lon.toFixed(4)}
          {' / scatter '}{centroid.cep_km != null ? `${centroid.cep_km.toFixed(1)} km` : '—'}
          {fm.fm_source === 'frame_accumulation' ? ' — active position' : ''}
        </div>
      ) : (
        <div style={{ fontSize: '0.82rem', color: '#8b949e', marginBottom: '0.6rem' }}>
          {nEstimates > 0 ? `${nEstimates} estimate${nEstimates !== 1 ? 's' : ''} in buffer — need 20 for centroid` : 'No frame estimates yet.'}
        </div>
      )}

      {lastRun && (
        <div style={{ borderTop: '1px solid #21262d', paddingTop: '0.75rem', marginTop: '0.75rem' }}>
          <div className={styles.metricRow}>
            <span className={styles.metricPill}>
              Last pipeline run <span className={styles.metricValue}>{lastRun.ts ? new Date(lastRun.ts * 1000).toLocaleTimeString() : '—'}</span>
            </span>
            <span className={styles.metricPill}>
              Elapsed <span className={styles.metricValue}>{lastRun.elapsed_ms != null ? `${Math.round(lastRun.elapsed_ms)} ms` : '—'}</span>
            </span>
            <span className={styles.metricPill}>
              Stored <span className={styles.metricValue}>{lastRun.stored == null ? '—' : lastRun.stored ? 'yes' : 'no'}</span>
            </span>
          </div>
          {lastRun.reason && (
            <div style={{ fontSize: '0.82rem', color: '#ff7b72', marginTop: '0.4rem' }}>
              {lastRun.reason}
            </div>
          )}
        </div>
      )}

    </section>
  )
}

function AirportHypothesisTable({ iid }) {
  const { data, loading } = useAirportHypothesis(iid, 0)

  if (loading) return <div className={styles.empty}>Loading hypothesis…</div>
  if (!data) return <div className={styles.empty}>Select an IID</div>
  if (data.status === 'NOT_SCORED_YET') {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Airport Hypothesis</div>
            <div className={styles.sectionLead}>
              Forward model has not yet scored airports for this IID.
              {data.diagnostics && (
                <>
                  {' '}Rotation: {data.diagnostics.rotation_status || 'unknown'}.
                  {data.diagnostics.multi_radar_flag && ' Multi-radar IID — excluded from FM.'}
                </>
              )}
            </div>
          </div>
        </div>
        <FMDataFunnel iid={iid} />
      </section>
    )
  }
  if (data.status === 'NOT_AVAILABLE') {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Airport Hypothesis</div>
          </div>
        </div>
        <div className={styles.empty}>{data.reason}</div>
      </section>
    )
  }

  const candidates = data.candidates ?? []
  const topScore = candidates[0]?.score ?? 0
  const secondScore = candidates[1]?.score ?? Infinity

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Airport Hypothesis</div>
          <div className={styles.sectionLead}>
            Ranked airports by forward model score (lower = better fit).
            {topScore > 0 && secondScore > 0 && topScore / secondScore < 0.33
              ? ' Clear winner detected.'
              : topScore > 0 && secondScore > 0 && topScore / secondScore < 1.5
              ? ' Ambiguous — multiple candidates.'
              : ''}
          </div>
        </div>
        <span className={styles.cardSub}>{candidates.length} airports scored</span>
      </div>
      {candidates.length === 0 ? (
        <div className={styles.empty}>No airports scored</div>
      ) : (
        <div className={styles.tableWrap}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Rank</th>
                <th>Airport</th>
                <th>Name</th>
                <th>Score</th>
                <th>σ (ms)</th>
                <th>Aircraft</th>
                <th>Directional Signal</th>
              </tr>
            </thead>
            <tbody>
              {candidates.slice(0, 20).map((c, i) => {
                const isClearWinner = i === 0 && secondScore > 0 && topScore / secondScore < 0.33
                const isAmbiguous = i < 2 && secondScore > 0 && topScore / secondScore >= 0.33 && topScore / secondScore < 1.5
                const ds = c.directional_signal
                return (
                  <tr
                    key={c.airport_icao}
                    style={{
                      background: isClearWinner ? '#388bfd12' : isAmbiguous ? '#d2992212' : 'transparent',
                    }}
                  >
                    <td className={styles.monoCell}>{i + 1}</td>
                    <td className={styles.monoCell}>
                      {c.airport_icao}
                      {isClearWinner && <span style={{ color: '#3fb950', marginLeft: 6 }}>✓</span>}
                      {isAmbiguous && <span style={{ color: '#d29922', marginLeft: 6 }}>≈</span>}
                    </td>
                    <td>{c.airport_name || '—'}</td>
                    <td className={styles.monoCell}>{c.score != null ? c.score.toFixed(1) : '—'}</td>
                    <td className={styles.monoCell}>
                      {c.residual_sigma_deg != null
                        ? `${c.residual_sigma_deg.toFixed(2)}°`
                        : c.residual_sigma_ms != null
                          ? `${c.residual_sigma_ms.toFixed(1)}ms`
                          : '—'}
                    </td>
                    <td className={styles.monoCell}>{c.n_aircraft ?? '—'}</td>
                    <td className={styles.monoCell}>
                      {ds ? (
                        <span title={`${ds.displacement_km} km toward ${ds.direction_deg}°`}>
                          ↗ {ds.amplitude_deg != null ? `${ds.amplitude_deg.toFixed(1)}°` : `${ds.amplitude_ms?.toFixed(1)}ms`}
                        </span>
                      ) : '—'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      <FMDataFunnel iid={iid} />
    </section>
  )
}

/** SweepFrame table — per-frame breakdown. Always visible to show live accumulation status. */
// Stable ICAO→colour mapping so the same aircraft keeps the same colour across frames
const STRIP_COLOURS = [
  '#58a6ff', '#3fb950', '#d29922', '#bc8cff', '#ff7b72',
  '#79c0ff', '#56d364', '#e3b341', '#d2a8ff', '#ffa198',
]
function icaoColour(icao, refIcao) {
  if (icao === refIcao) return '#58a6ff'
  let h = 0
  for (let i = 0; i < icao.length; i++) h = (h * 31 + icao.charCodeAt(i)) & 0xffff
  return STRIP_COLOURS[1 + (h % (STRIP_COLOURS.length - 1))]
}

/**
 * Convert a flat-projection circle (as computed by the Python backend) to a
 * polygon latlng array so it renders correctly on Leaflet.
 *
 * The backend uses the equirectangular projection:
 *   x = lon * kmPerDegLon,  y = lat * kmPerDegLat
 * All circle geometry (center, radius) lives in that space. Leaflet's L.circle
 * draws a geodesic circle, which diverges noticeably for large radii.  By
 * regenerating the vertices in the same flat space and converting back to
 * lat/lon we get a polygon that correctly passes through the chord endpoints.
 */
function flatProjectionCircleLatLngs(centerLat, centerLon, radiusKm, kmPerDegLat, kmPerDegLon, n = 72) {
  const cx = centerLon * kmPerDegLon
  const cy = centerLat * kmPerDegLat
  const pts = []
  for (let i = 0; i < n; i++) {
    const angle = (i / n) * 2 * Math.PI
    pts.push([
      (cy + radiusKm * Math.sin(angle)) / kmPerDegLat,
      (cx + radiusKm * Math.cos(angle)) / kmPerDegLon,
    ])
  }
  pts.push(pts[0]) // close polygon
  return pts
}

function pairKey(pair) {
  return `${pair?.icao_a ?? ''}:${pair?.icao_b ?? ''}:${pair?.circle_index ?? ''}`
}

function pairLabel(pair) {
  return `${pair?.icao_a ?? '-'} <-> ${pair?.icao_b ?? '-'}`
}

const FRAME_GEOMETRY_BASEMAPS = {
  dark: {
    label: 'Dark',
    url: 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  },
  light: {
    label: 'Light',
    url: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  },
  voyager: {
    label: 'Street',
    url: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
  },
  osm: {
    label: 'OSM',
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  },
}

function FrameGeometryDiagnostics({ iid, frameIndex }) {
  const [direction, setDirection] = useState('cw')
  const [basemapKey, setBasemapKey] = useState('dark')
  const [activePairs, setActivePairs] = useState(() => new Set())
  const { data, loading } = useFrameFmGeometry(iid, frameIndex, direction)
  // When the automatic solver rejects a frame, admitted_pair_circles may be empty.
  // Fall back to scored_pair_circles (includes rejected pairs with exclusion_reason)
  // so the user can still inspect the pair geometry for rejected frames.
  const admittedPairs = (data?.admitted_pair_circles ?? data?.observations ?? [])
  const scoredPairs = data?.scored_pair_circles ?? []
  const displayPairs = admittedPairs.length > 0 ? admittedPairs : scoredPairs
  const pairSummary = data?.pair_circle_summary ?? {}
  const candidateClusters = data?.candidate_clusters ?? []

  // Manual preview state
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewResult, setPreviewResult] = useState(null)

  // Leaflet map refs
  const leafletMapRef = useRef(null)
  const tileLayerRef = useRef(null)
  const geomLayerRef = useRef(null)
  const [mapReady, setMapReady] = useState(false)

  // ── Default pair selection helper (reused on data change and reset-to-auto) ──
  const defaultActivePairSet = useCallback((pairs) => {
    const selected = pairs.filter(pair => pair.inlier).map(pairKey)
    return new Set(selected.length > 0 ? selected : pairs.slice(0, 3).map(pairKey))
  }, [])

  // Reset active pair filters when frame/direction/data changes
  useEffect(() => {
    setActivePairs(defaultActivePairSet(displayPairs))
  }, [iid, frameIndex, direction, displayPairs, defaultActivePairSet])

  // ── Effective geometry: preview overrides automatic when active ──
  const hasPreview = previewResult != null && previewResult.available
  const effectiveGeometry = hasPreview ? previewResult : data
  const effectiveAdmittedPairs = hasPreview
    ? (previewResult.admitted_pair_circles ?? previewResult.observations ?? [])
    : admittedPairs
  const effectiveScoredPairs = hasPreview
    ? (previewResult.scored_pair_circles ?? [])
    : scoredPairs
  const effectiveDisplayPairs = effectiveAdmittedPairs.length > 0 ? effectiveAdmittedPairs : effectiveScoredPairs
  const effectivePairSummary = hasPreview
    ? (previewResult.pair_circle_summary ?? {})
    : pairSummary
  const effectiveCandidateClusters = hasPreview
    ? (previewResult.candidate_clusters ?? [])
    : candidateClusters

  // Callback ref: initialise Leaflet when the container div mounts
  const mapDivRef = useCallback(node => {
    if (node && !leafletMapRef.current) {
      const map = L.map(node, { center: [51.5, 0], zoom: 9, zoomControl: true })
      geomLayerRef.current = L.layerGroup().addTo(map)
      leafletMapRef.current = map
      setMapReady(true)
    } else if (!node && leafletMapRef.current) {
      leafletMapRef.current.remove()
      leafletMapRef.current = null
      tileLayerRef.current = null
      geomLayerRef.current = null
      setMapReady(false)
    }
  }, [])

  useEffect(() => {
    const map = leafletMapRef.current
    if (!map || !mapReady) return
    const basemap = FRAME_GEOMETRY_BASEMAPS[basemapKey] ?? FRAME_GEOMETRY_BASEMAPS.dark
    if (tileLayerRef.current) {
      tileLayerRef.current.remove()
    }
    const tileOptions = {
      attribution: basemap.attribution,
      maxZoom: basemap.maxZoom,
    }
    if (basemap.subdomains) {
      tileOptions.subdomains = basemap.subdomains
    }
    tileLayerRef.current = L.tileLayer(basemap.url, tileOptions).addTo(map)
  }, [basemapKey, mapReady])

  // Fit bounds when effective geometry or map readiness changes
  useEffect(() => {
    const map = leafletMapRef.current
    if (!map || !effectiveGeometry?.available) return
    const pts = collectFrameGeometryPoints(effectiveGeometry)
    // Include circle polygon extents using the same flat projection the backend uses
    const pointLayer = effectiveGeometry.layers?.find(l => l.geometry_type === 'point')
    const pointLats = (pointLayer?.features ?? []).map(f => f.geometry.coordinates[1])
    if (pointLats.length > 0) {
      const meanLat = pointLats.reduce((a, b) => a + b, 0) / pointLats.length
      const kmPerDegLat = 111.32
      const kmPerDegLon = Math.max(111.32 * Math.cos(meanLat * Math.PI / 180), 1e-6)
      ;(effectiveGeometry.layers ?? []).forEach(layer => {
        if (layer.geometry_type !== 'circle') return
        ;(layer.features ?? []).forEach(feature => {
          const [lon, lat] = feature.geometry.center
          const rKm = feature.geometry.radius_km ?? 0
          // Sample 8 points around the flat-projection circle for bounds
          for (let i = 0; i < 8; i++) {
            const angle = (i / 8) * 2 * Math.PI
            pts.push({
              lat: (lat * kmPerDegLat + rKm * Math.sin(angle)) / kmPerDegLat,
              lon: (lon * kmPerDegLon + rKm * Math.cos(angle)) / kmPerDegLon,
            })
          }
        })
      })
    }
    if (!pts.length) return
    try {
      map.invalidateSize()
      map.fitBounds(L.latLngBounds(pts.map(p => [p.lat, p.lon])), { padding: [30, 30], maxZoom: 12 })
    } catch (_) {}
  }, [effectiveGeometry, mapReady])

  // Redraw geometry layers when effective geometry, active pairs, or map readiness changes
  useEffect(() => {
    const map = leafletMapRef.current
    const lg = geomLayerRef.current
    if (!map || !lg) return
    lg.clearLayers()
    if (!effectiveGeometry?.available) return

    const pointLayer = effectiveGeometry.layers?.find(l => l.geometry_type === 'point')
    const lineLayer = effectiveGeometry.layers?.find(l => l.geometry_type === 'line')
    const circleLayer = effectiveGeometry.layers?.find(l => l.geometry_type === 'circle')

    // Flat projection parameters — must match the backend's to_xy/from_xy exactly
    const pointLats = (pointLayer?.features ?? []).map(f => f.geometry.coordinates[1])
    const meanLat = pointLats.length > 0
      ? pointLats.reduce((a, b) => a + b, 0) / pointLats.length
      : 51.5
    const KM_PER_DEG_LAT = 111.32
    const kmPerDegLon = Math.max(111.32 * Math.cos(meanLat * Math.PI / 180), 1e-6)

    // FM ranging circles — drawn as flat-projection polygons so they correctly
    // pass through the chord endpoints (matching the backend's geometry)
    ;(circleLayer?.features ?? []).forEach(feature => {
      const props = feature.properties ?? {}
      const isFrameCep = props.circle_type === 'frame_cep'
      const key = `${props.icao_a ?? ''}:${props.icao_b ?? ''}:${props.circle_index ?? ''}`
      if (!isFrameCep && !activePairs.has(key)) return
      const [lon, lat] = feature.geometry.center
      const radiusKm = feature.geometry.radius_km ?? 0
      const latlngs = flatProjectionCircleLatLngs(lat, lon, radiusKm, KM_PER_DEG_LAT, kmPerDegLon)
      L.polygon(latlngs, {
        color: isFrameCep ? '#ff7b72' : props.inlier ? '#3fb950' : '#d29922',
        weight: isFrameCep ? 2.6 : props.inlier ? 2.4 : 1.7,
        fill: false,
        opacity: isFrameCep ? 0.95 : props.inlier ? 0.95 : 0.65,
        dashArray: isFrameCep ? '8 6' : props.inlier ? null : '6 5',
      }).addTo(lg)
    })

    // Chord lines (baselines) for admitted aircraft pairs — kept dim and dotted
    // so they are visually distinct from the inscribed circles
    ;(lineLayer?.features ?? []).forEach(feature => {
      const props = feature.properties ?? {}
      const key = `${props.icao_a ?? ''}:${props.icao_b ?? ''}:${props.circle_index ?? ''}`
      if (!activePairs.has(key)) return
      const latlngs = feature.geometry.coordinates.map(([lon, lat]) => [lat, lon])
      L.polyline(latlngs, {
        color: '#8b949e',
        weight: 1.0,
        opacity: props.inlier ? 0.5 : 0.3,
        dashArray: '2 5',
      }).addTo(lg)
    })

    // Aircraft position points
    ;(pointLayer?.features ?? []).forEach(feature => {
      const props = feature.properties ?? {}
      const [lon, lat] = feature.geometry.coordinates
      const isRef = props.source_role === 'sweep_reference'
      const isFrameEstimate = props.role === 'frame_estimate'
      const isActive = effectiveAdmittedPairs.some(pair => (
        activePairs.has(pairKey(pair)) && (pair.icao_a === props.icao || pair.icao_b === props.icao)
      ))
      const fillColor = isFrameEstimate ? '#ff7b72' : isRef ? '#58a6ff' : isActive ? '#ffffff' : props.selected ? '#3fb950' : '#d29922'
      const marker = L.circleMarker([lat, lon], {
        radius: isFrameEstimate ? 8 : isRef ? 7 : isActive ? 6 : 4,
        color: (isFrameEstimate || isRef || isActive) ? '#ffffff' : 'transparent',
        weight: (isFrameEstimate || isRef || isActive) ? 1.2 : 0,
        fillColor,
        fillOpacity: props.drop_reason ? 0.65 : 1.0,
      }).addTo(lg)
      const label = isFrameEstimate
        ? `Frame estimate${props.cep_km != null ? ` / CEP ${props.cep_km.toFixed(1)} km` : ''}`
        : props.icao
      if (label) {
        marker.bindTooltip(label, {
          permanent: true,
          direction: 'right',
          offset: [4, 0],
          className: 'frame-geom-icao-label',
        })
      }
    })
  }, [effectiveGeometry, effectiveAdmittedPairs, activePairs, mapReady])

  function togglePair(pair) {
    const key = pairKey(pair)
    setActivePairs(current => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  async function handlePreviewSolve() {
    const selectedIndices = [...activePairs]
      .map(key => {
        // pairKey returns `${icao_a}:${icao_b}:${circle_index}`
        const parts = key.split(':')
        const idx = parts[parts.length - 1]
        return idx !== '' ? parseInt(idx, 10) : null
      })
      .filter(n => n !== null && !isNaN(n))
    if (selectedIndices.length === 0) return

    setPreviewLoading(true)
    setPreviewResult(null)
    try {
      const dir = direction === 'ccw' ? -1 : 1
      const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/sweep-frames/${frameIndex}/fm-geometry/manual-preview`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ direction: dir, selected_circle_indices: selectedIndices }),
      })
      if (r.ok) {
        const d = await r.json()
        setPreviewResult(d)
      }
    } catch {
      // ignore cancelled requests
    } finally {
      setPreviewLoading(false)
    }
  }

  function handleResetToAuto() {
    // Clear preview result AND restore automatic default pair selection
    setPreviewResult(null)
    setActivePairs(defaultActivePairSet(admittedPairs.length > 0 ? admittedPairs : scoredPairs))
  }

  if (frameIndex == null) {
    return <div className={styles.empty}>Click a sweep frame to inspect its FM geometry.</div>
  }
  if (loading && !data) {
    return <div className={styles.empty}>Loading frame geometry...</div>
  }
  if (!data?.available) {
    return <div className={styles.empty}>{data?.reason ?? 'No frame geometry available.'}</div>
  }

  const enabledPairs = activePairs
  const selection = data.selection_diagnostics ?? {}

  return (
    <div style={{ borderTop: '1px solid #21262d', marginTop: '1rem', paddingTop: '0.85rem' }}>
      <div className={styles.cardHeader} style={{ marginBottom: '0.75rem' }}>
        <div>
          <div className={styles.cardTitle}>
            Frame FM Geometry{hasPreview ? ' — Manual Preview' : ''}
          </div>
          <div className={styles.sectionLead}>
            Frame #{data.frame_index + 1} · aircraft-pair circle diagnostics
            {effectiveGeometry.frame_cep_km != null && (
              <> · CEP{' '}
              <span style={{
                color: effectiveGeometry.frame_cep_km > 10 ? '#f85149' : effectiveGeometry.frame_cep_km > 5 ? '#d29922' : '#3fb950',
                fontWeight: 600,
              }}>
                {effectiveGeometry.frame_cep_km.toFixed(1)} km
              </span>
              </>
            )}
            {effectiveGeometry.frame_n_inlier_pair_circles != null && <> · {effectiveGeometry.frame_n_inlier_pair_circles} inlier pair circles</>}
            {' '}· click a pair row to highlight its admitted circle.
          </div>
          {/* Automatic solver status — always shown from automatic data */}
          {data.frame_solve_reason && (
            <div style={{ marginTop: '0.35rem' }}>
              <span className={styles.metricPill} style={{
                borderColor: data.frame_cep_km != null ? '#3fb950' : '#d29922',
                borderWidth: '1px',
                borderStyle: 'solid',
              }}>
                Automatic solver:{' '}
                <span style={{ color: data.frame_cep_km != null ? '#3fb950' : '#d29922', fontWeight: 600 }}>
                  {data.frame_cep_km != null ? 'accepted' : `rejected — ${data.frame_solve_reason}`}
                </span>
              </span>
            </div>
          )}
        </div>
        <div className={styles.metricRow}>
          <label className={styles.legendChip}>
            Map
            <select
              value={basemapKey}
              onChange={event => setBasemapKey(event.target.value)}
              style={{ marginLeft: '0.35rem' }}
            >
              {Object.entries(FRAME_GEOMETRY_BASEMAPS).map(([key, basemap]) => (
                <option key={key} value={key}>{basemap.label}</option>
              ))}
            </select>
          </label>
          <label className={styles.legendChip}>
            <input type="radio" checked={direction === 'cw'} onChange={() => setDirection('cw')} />
            CW
          </label>
          <label className={styles.legendChip}>
            <input type="radio" checked={direction === 'ccw'} onChange={() => setDirection('ccw')} />
            CCW
          </label>
          {/* Manual preview controls */}
          <button
            type="button"
            className={styles.actionButton}
            onClick={handlePreviewSolve}
            disabled={previewLoading || activePairs.size < 2}
            title="Preview solve from selected pairs only"
          >
            {previewLoading ? 'Previewing…' : 'Preview Solve'}
          </button>
          {previewResult && (
            <button
              type="button"
              className={styles.actionButton}
              onClick={handleResetToAuto}
            >
              Reset To Auto
            </button>
          )}
        </div>
      </div>

      <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
        <span className={styles.metricPill}>Enabled Pairs <span className={styles.metricValue}>{enabledPairs.size}</span></span>
        <span className={styles.metricPill}>Raw Pairs <span className={styles.metricValue}>{effectivePairSummary.total_raw_pair_circles ?? selection.total_raw_pair_circles ?? 0}</span></span>
        <span className={styles.metricPill}>Scored Pairs <span className={styles.metricValue}>{effectivePairSummary.total_scored_pair_circles ?? selection.total_scored_pair_circles ?? effectiveScoredPairs.length}</span></span>
        <span className={styles.metricPill}>Admitted Pairs <span className={styles.metricValue}>{effectivePairSummary.total_admitted_pair_circles ?? selection.total_admitted_pair_circles ?? effectiveAdmittedPairs.length}</span></span>
        <span className={styles.metricPill}>Inlier Pairs <span className={styles.metricValue}>{effectivePairSummary.total_inlier_pair_circles ?? selection.total_inlier_pair_circles ?? 0}</span></span>
        <span className={styles.metricPill}>With Frame Ref <span className={styles.metricValue}>{effectivePairSummary.admitted_pairs_containing_reference ?? selection.admitted_pairs_containing_reference ?? 0}</span></span>
        <span className={styles.metricPill}>Without Frame Ref <span className={styles.metricValue}>{effectivePairSummary.admitted_pairs_not_containing_reference ?? selection.admitted_pairs_not_containing_reference ?? 0}</span></span>
      </div>

      {/* Frame CEP from intersection solve — use effective geometry */}
      {effectiveGeometry.frame_cep_km != null && (
        <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
          <span className={styles.metricPill}>
            Frame CEP{' '}
            <span
              className={styles.metricValue}
              style={{
                color: effectiveGeometry.frame_cep_km > 10 ? '#f85149' : effectiveGeometry.frame_cep_km > 5 ? '#d29922' : '#3fb950',
              }}
            >
              {effectiveGeometry.frame_cep_km.toFixed(1)} km
            </span>
          </span>
          {effectiveGeometry.frame_n_arcs != null && (
            <span className={styles.metricPill}>Inlier Pair Circles <span className={styles.metricValue}>{effectiveGeometry.frame_n_inlier_pair_circles ?? effectiveGeometry.frame_n_arcs}</span></span>
          )}
        </div>
      )}
      {effectiveGeometry.frame_cep_km == null && (
        <div className={styles.metricRow} style={{ marginBottom: '0.75rem' }}>
          <span className={styles.metricPill}>Frame CEP <span className={styles.metricValue} style={{ color: '#d29922' }}>{effectiveGeometry.frame_solve_reason ?? 'no solve'}</span></span>
        </div>
      )}

      <div
        ref={mapDivRef}
        style={{ width: '100%', height: '600px', borderRadius: '4px', overflow: 'hidden', border: '1px solid #21262d' }}
      />

      <div className={styles.tableWrap} style={{ marginTop: '0.75rem' }}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Aircraft Pair</th><th>Show</th><th>Inlier</th><th>Circle Score</th>
              <th>Intrinsic</th><th>Prior</th><th>Baseline</th><th>Delta Phi</th>
              <th>Sigma</th><th>Residual</th><th>Replies</th><th>Position</th>
              <th>Exclusion</th>
            </tr>
          </thead>
          <tbody>
            {effectiveDisplayPairs.map(pair => (
              <tr
                key={pairKey(pair)}
                onClick={() => togglePair(pair)}
                style={{
                  cursor: 'pointer',
                  background: enabledPairs.has(pairKey(pair)) ? '#388bfd12' : 'transparent',
                }}
              >
                <td className={styles.monoCell}>{pairLabel(pair)}</td>
                <td>
                  <input
                    type="checkbox"
                    checked={enabledPairs.has(pairKey(pair))}
                    onChange={() => togglePair(pair)}
                    onClick={event => event.stopPropagation()}
                  />
                </td>
                <td className={styles.monoCell} style={{ color: pair.inlier ? '#3fb950' : '#d29922' }}>{pair.inlier ? 'yes' : 'no'}</td>
                <td className={styles.monoCell}>{pair.circle_score != null ? pair.circle_score.toFixed(3) : '-'}</td>
                <td className={styles.monoCell}>{pair.intrinsic_weight != null ? pair.intrinsic_weight.toFixed(3) : '-'}</td>
                <td className={styles.monoCell}>{pair.prior_weight != null ? pair.prior_weight.toFixed(3) : '-'}</td>
                <td className={styles.monoCell}>{pair.pair_baseline_m != null ? `${(pair.pair_baseline_m / 1000).toFixed(1)} km` : '-'}</td>
                <td className={styles.monoCell}>{pair.delta_phi_deg != null ? `${pair.delta_phi_deg.toFixed(1)} deg` : '-'}</td>
                <td className={styles.monoCell}>{pair.sigma_band_metres != null ? `${pair.sigma_band_metres.toFixed(0)} m` : '-'}</td>
                <td className={styles.monoCell}>{pair.normalized_residual != null ? pair.normalized_residual.toFixed(2) : '-'}</td>
                <td className={styles.monoCell}>{pair.n_replies_a ?? '-'} / {pair.n_replies_b ?? '-'}</td>
                <td>{pair.interpolated_a || pair.interpolated_b ? 'interpolated' : 'direct'}</td>
                <td className={styles.monoCell} style={{ color: pair.exclusion_reason ? '#f85149' : '#8b949e' }}>
                  {pair.exclusion_reason ?? '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Candidate clusters — shown even when the automatic solver rejected the frame */}
      {effectiveCandidateClusters.length > 0 && (
        <div style={{ marginTop: '1rem' }}>
          <h4 style={{ margin: '0 0 0.5rem', fontSize: '0.9rem', color: '#8b949e' }}>
            Candidate Clusters ({effectiveCandidateClusters.length})
          </h4>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Rank</th><th>Lat</th><th>Lon</th><th>Support</th>
                  <th>RMS (deg)</th><th>Members</th><th>RMS (km)</th>
                  <th>Quality Score</th><th>Condition #</th>
                  <th>Dominance</th><th>Status</th>
                </tr>
              </thead>
              <tbody>
                {effectiveCandidateClusters.map(cluster => (
                  <tr key={cluster.cluster_rank}>
                    <td className={styles.monoCell}>{cluster.cluster_rank}</td>
                    <td className={styles.monoCell}>{cluster.lat != null ? cluster.lat.toFixed(4) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.lon != null ? cluster.lon.toFixed(4) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.support_score != null ? cluster.support_score.toFixed(3) : cluster.cluster_support_score != null ? cluster.cluster_support_score.toFixed(3) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.pairwise_weighted_rms_deg != null ? cluster.pairwise_weighted_rms_deg.toFixed(1) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.member_count}</td>
                    <td className={styles.monoCell}>{cluster.rms_km != null ? cluster.rms_km.toFixed(1) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.cluster_quality_score != null ? cluster.cluster_quality_score.toFixed(3) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.cluster_condition_number != null ? cluster.cluster_condition_number.toFixed(1) : '-'}</td>
                    <td className={styles.monoCell}>{cluster.quality_dominance_ratio != null ? cluster.quality_dominance_ratio.toFixed(2) : cluster.support_dominance_ratio != null ? cluster.support_dominance_ratio.toFixed(2) : '-'}</td>
                    <td>
                      {cluster.is_selected_best && !hasPreview ? (
                        <span style={{ color: '#3fb950', fontWeight: 600 }}>best (auto)</span>
                      ) : hasPreview ? (
                        <span style={{ color: '#d29922' }}>preview</span>
                      ) : data.frame_solve_reason ? (
                        <span style={{ color: '#d29922' }}>auto-rejected</span>
                      ) : (
                        <span style={{ color: '#8b949e' }}>alternative</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Manual preview result summary — still shown for metadata, but map/table already use preview */}
      {previewResult && (
        <div style={{ marginTop: '1rem', padding: '0.75rem', border: '1px solid #d29922', borderRadius: '6px', background: '#d2992210' }}>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: '0.5rem' }}>
            <h4 style={{ margin: 0, fontSize: '0.9rem', color: '#d29922' }}>
              Manual Preview Metadata
            </h4>
            {previewResult.automatic_acceptance_status === 'would_be_rejected_ambiguous' && (
              <span className={styles.metricPill} style={{ marginLeft: '0.5rem', borderColor: '#d29922', borderWidth: '1px', borderStyle: 'solid', fontSize: '0.75rem' }}>
                Would be rejected — ambiguous
              </span>
            )}
            {previewResult.automatic_acceptance_status === 'would_be_rejected_other' && (
              <span className={styles.metricPill} style={{ marginLeft: '0.5rem', borderColor: '#f85149', borderWidth: '1px', borderStyle: 'solid', fontSize: '0.75rem' }}>
                Would be rejected — quality gates
              </span>
            )}
            {previewResult.automatic_acceptance_status === 'accepted' && (
              <span className={styles.metricPill} style={{ marginLeft: '0.5rem', borderColor: '#3fb950', borderWidth: '1px', borderStyle: 'solid', fontSize: '0.75rem' }}>
                Would be accepted automatically
              </span>
            )}
          </div>
          <div className={styles.metricRow}>
            {previewResult.frame_lat != null && (
              <span className={styles.metricPill}>Preview Lat <span className={styles.metricValue}>{previewResult.frame_lat.toFixed(4)}</span></span>
            )}
            {previewResult.frame_lon != null && (
              <span className={styles.metricPill}>Preview Lon <span className={styles.metricValue}>{previewResult.frame_lon.toFixed(4)}</span></span>
            )}
            {previewResult.frame_cep_km != null && (
              <span className={styles.metricPill}>Preview CEP <span className={styles.metricValue}>{previewResult.frame_cep_km.toFixed(1)} km</span></span>
            )}
            {previewResult.frame_n_arcs != null && (
              <span className={styles.metricPill}>Inlier Pairs <span className={styles.metricValue}>{previewResult.frame_n_arcs}</span></span>
            )}
            {previewResult.frame_solve_reason && (
              <span className={styles.metricPill}>Reason <span className={styles.metricValue} style={{ color: '#d29922' }}>{previewResult.frame_solve_reason}</span></span>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

function SweepFrameStrips({ iid, selectedFrame, onSelectFrame, frameData }) {
  const DISPLAY_COUNT = 15
  const [geometryEnabled, setGeometryEnabled] = useState(false)

  const allFrames = frameData?.frames ?? []
  const qualFrames = allFrames.filter(f => f.quality === 'good' || f.quality === 'marginal')
  // Show last DISPLAY_COUNT quality frames, most recent at top
  const displayFrames = qualFrames.slice(-DISPLAY_COUNT).reverse()
  const effectiveFrameIndex = selectedFrame ?? null
  const nGood = allFrames.filter(f => f.quality === 'good').length
  const nMarginal = allFrames.filter(f => f.quality === 'marginal').length

  const detailFrame = effectiveFrameIndex != null
    ? allFrames.find(f => f.frame_index === effectiveFrameIndex)
    : null
  const geometryFrameIndex = geometryEnabled && detailFrame ? detailFrame.frame_index : null

  useEffect(() => {
    setGeometryEnabled(false)
  }, [iid])

  if (iid == null || allFrames.length === 0) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Sweep Frames</div>
            <div className={styles.sectionLead}>
              {iid == null ? 'Select an IID.' : 'Accumulating sweep data…'}
            </div>
          </div>
        </div>
        <div className={styles.empty}>
          {iid != null ? 'Waiting for sweep data…' : 'No IID selected.'}
        </div>
      </section>
    )
  }

  return (
    <section className={styles.card} data-panel="sweep-frames">
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Sweep Frames</div>
          <div className={styles.sectionLead}>
            {allFrames.length} frames · {nGood} good · {nMarginal} marginal
            {' · '}Last {displayFrames.length} shown · ref at 0°
          </div>
        </div>
      </div>

      {/* Phase strip diagram — each row is one sweep */}
      <div style={{ overflowX: 'auto' }}>
        {/* Scale header */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: '3.5rem 1fr',
          gap: '0 0.5rem',
          marginBottom: '0.25rem',
          paddingRight: '0.5rem',
        }}>
          <div />
          <div style={{ position: 'relative', height: '1rem' }}>
            {[0, 90, 180, 270, 360].map(deg => (
              <span key={deg} style={{
                position: 'absolute',
                left: `${(deg / 360) * 100}%`,
                transform: deg === 360 ? 'translateX(-100%)' : deg > 0 ? 'translateX(-50%)' : 'none',
                fontSize: '0.65rem',
                color: '#484f58',
              }}>{deg}°</span>
            ))}
          </div>
        </div>

        {displayFrames.map(frame => {
          const isSelected = effectiveFrameIndex === frame.frame_index
          const qualColor = frame.quality === 'good' ? '#3fb950' : '#d29922'
          return (
            <div
              key={frame.frame_index}
              onClick={() => onSelectFrame?.(isSelected ? null : frame.frame_index)}
              style={{
                display: 'grid',
                gridTemplateColumns: '3.5rem 1fr',
                gap: '0 0.5rem',
                marginBottom: '3px',
                cursor: 'pointer',
                paddingRight: '0.5rem',
              }}
            >
              {/* Frame label */}
              <div style={{
                fontSize: '0.65rem',
                color: isSelected ? '#c9d1d9' : '#8b949e',
                textAlign: 'right',
                lineHeight: '14px',
                paddingTop: '1px',
                borderRight: `2px solid ${isSelected ? qualColor : qualColor + '44'}`,
                paddingRight: '4px',
                whiteSpace: 'nowrap',
              }}>
                #{frame.frame_index + 1}
              </div>

              {/* Phase strip */}
              <div style={{
                position: 'relative',
                height: '14px',
                background: isSelected ? '#1a2332' : '#0d1117',
                border: `1px solid ${isSelected ? qualColor + '88' : '#21262d'}`,
                borderRadius: '2px',
              }}>
                {/* Subtle 90° grid lines */}
                {[90, 180, 270].map(g => (
                  <div key={g} style={{
                    position: 'absolute',
                    left: `${(g / 360) * 100}%`,
                    top: 0, bottom: 0,
                    width: '1px',
                    background: '#21262d',
                  }} />
                ))}

                {/* Reference aircraft at 0° */}
                <div
                  title={`${frame.ref_icao} (ref) 0°`}
                  style={{
                    position: 'absolute',
                    left: '0%',
                    top: '1px', bottom: '1px',
                    width: '3px',
                    background: '#58a6ff',
                    borderRadius: '1px',
                    transform: 'translateX(0)',
                  }}
                />

                {/* Observations */}
                {frame.observations.map(obs => {
                  if (obs.observed_phase_deg == null) return null
                  const pct = (obs.observed_phase_deg / 360) * 100
                  return (
                    <div
                      key={obs.icao}
                      title={`${obs.icao} ${obs.observed_phase_deg.toFixed(1)}°${obs.interpolated ? ' (interp)' : ''}`}
                      style={{
                        position: 'absolute',
                        left: `${pct}%`,
                        top: '1px', bottom: '1px',
                        width: obs.interpolated ? '2px' : '3px',
                        background: icaoColour(obs.icao, frame.ref_icao),
                        opacity: obs.interpolated ? 0.55 : 1,
                        borderRadius: '1px',
                        transform: 'translateX(-50%)',
                      }}
                    />
                  )
                })}
              </div>
            </div>
          )
        })}
      </div>

      {/* Detail table for selected frame */}
      {detailFrame && (
        <div style={{ marginTop: '1rem', borderTop: '1px solid #21262d', paddingTop: '0.75rem' }}>
          <div style={{ fontSize: '0.78rem', color: '#8b949e', marginBottom: '0.5rem' }}>
            Frame #{detailFrame.frame_index + 1} · ref {detailFrame.ref_icao}
            {' · '}{detailFrame.n_aircraft} aircraft
            {' · '}<span style={{ color: detailFrame.quality === 'good' ? '#3fb950' : '#d29922' }}>
              {detailFrame.quality}
            </span>
          </div>
          <div className={styles.tableWrap}>
            <table className={styles.table}>
              <thead>
                <tr><th>ICAO</th><th>Phase</th><th>Lat</th><th>Lon</th><th>Quality</th></tr>
              </thead>
              <tbody>
                <tr style={{ background: '#388bfd12' }}>
                  <td className={styles.monoCell} style={{ color: '#58a6ff' }}>{detailFrame.ref_icao}</td>
                  <td className={styles.monoCell}>0.0° (ref)</td>
                  <td className={styles.monoCell}>{detailFrame.ref_lat.toFixed(4)}</td>
                  <td className={styles.monoCell}>{detailFrame.ref_lon.toFixed(4)}</td>
                  <td><span style={{ color: '#58a6ff' }}>reference</span></td>
                </tr>
                {detailFrame.observations.map(obs => (
                  <tr key={obs.icao}>
                    <td className={styles.monoCell} style={{ color: icaoColour(obs.icao, detailFrame.ref_icao) }}>
                      {obs.icao}
                    </td>
                    <td className={styles.monoCell}>
                      {obs.observed_phase_deg != null ? `${obs.observed_phase_deg.toFixed(1)}°` : '—'}
                    </td>
                    <td className={styles.monoCell}>{obs.lat?.toFixed(4) ?? '—'}</td>
                    <td className={styles.monoCell}>{obs.lon?.toFixed(4) ?? '—'}</td>
                    <td>
                      {obs.interpolated
                        ? <span style={{ color: '#d29922' }}>interpolated</span>
                        : <span style={{ color: '#3fb950' }}>direct</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ marginTop: '0.75rem', display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
            <button
              type="button"
              className={styles.actionButton}
              onClick={() => setGeometryEnabled(enabled => !enabled)}
            >
              {geometryEnabled ? 'Hide FM geometry diagnostics' : 'Load FM geometry diagnostics'}
            </button>
            <span style={{ fontSize: '0.74rem', color: '#8b949e' }}>
              Heavy pair-circle diagnostics are on-demand so normal live browsing does not recompute geometry.
            </span>
          </div>
        </div>
      )}
      {geometryEnabled ? (
        <FrameGeometryDiagnostics iid={iid} frameIndex={geometryFrameIndex} />
      ) : (
        <div className={styles.empty} style={{ marginTop: '1rem' }}>
          Select a frame, then load FM geometry diagnostics explicitly.
        </div>
      )}
    </section>
  )
}

function ConvergencePlot({ iid }) {
  const { data, loading } = useFmConvergence(iid)

  if (loading) return <div className={styles.empty}>Loading…</div>
  if (!data || !data.history?.length) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Position Convergence</div>
          </div>
        </div>
        <div className={styles.empty}>No convergence data yet</div>
      </section>
    )
  }

  const history = data.history
  const latest = history[history.length - 1]
  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Position Convergence</div>
          <div className={styles.sectionLead}>
            {history.length} estimates. Latest: ({latest.lat.toFixed(4)}, {latest.lon.toFixed(4)})
            Uncertainty {latest.cep_m ? `${Math.round(latest.cep_m)} m` : '?'}
            via {latest.source || '?'}.
          </div>
        </div>
        <span className={styles.cardSub}>{history.length} points</span>
      </div>
      <div className={styles.tableWrap}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>#</th>
              <th>Time</th>
              <th>Latitude</th>
              <th>Longitude</th>
              <th>Uncertainty (m)</th>
              <th>Source</th>
              <th>Obs</th>
            </tr>
          </thead>
          <tbody>
            {history.map((entry, i) => (
              <tr key={i}>
                <td className={styles.monoCell}>{i + 1}</td>
                <td className={styles.monoCell}>
                  {entry.ts ? new Date(entry.ts * 1000).toLocaleTimeString() : '—'}
                </td>
                <td className={styles.monoCell}>{entry.lat.toFixed(4)}</td>
                <td className={styles.monoCell}>{entry.lon.toFixed(4)}</td>
                <td className={styles.monoCell}>{entry.cep_m ? Math.round(entry.cep_m) : '—'}</td>
                <td className={styles.monoCell}>{entry.source || '—'}</td>
                <td className={styles.monoCell}>{entry.n_observations ?? '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}

export default function RadarPage() {
  const { rows, iidMap, rollingHistory } = useRadarLiveStream()
  const [selectedIid, setSelectedIid] = useState(null)
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [selectedFrame, setSelectedFrame] = useState(null)
  const [resettingAll, setResettingAll] = useState(false)
  const selectedRow = rows.find(row => row.iid === selectedIid) ?? null
  const receiverPosition = useReceiverPosition()
  const sharedTimingPacket = useTimingEventStream({
    enabled: selectedIid != null,
    iid: selectedIid,
    df11Only: true,
    debugLabel: 'radar_page_df11',
  })
  const { snapshot: selectedIidState, status: selectedIidStateStatus } = useSelectedIidPageState(
    selectedIid,
    BURST_SYNC_ALIGNMENT_WINDOW_S,
  )
  const sharedSweepFrames = selectedIidState?.frames ?? null
  const sharedReferenceAircraft = selectedIidState?.reference ?? null
  const sharedFmLocation = selectedIidState?.fm?.location ?? null
  const sharedControl = selectedIidState?.control ?? null
  const sharedSolution = selectedIidState?.solution ?? null
  const sharedFmSummary = selectedIidState?.fm ?? null
  const sharedEvidence = selectedIidState?.evidence ?? null
  const syncSnapshot = selectedIidState?.sync ?? null
  const syncFeedStatus = selectedIidStateStatus

  useEffect(() => {
    if (rows.length === 0) return
    if (selectedIid == null || !rows.some(row => row.iid === selectedIid)) {
      setSelectedIid(rows[0].iid)
      setSelectedIcao(null)
      setSelectedFrame(null)
    }
  }, [rows, selectedIid])

  function handleSelectIid(iid) {
    setSelectedIid(iid)
    setSelectedIcao(null)
    setSelectedFrame(null)
  }

  async function handleResetAll() {
    if (resettingAll) return
    const confirmed = window.confirm(
      'Purge all radar learning?\n\nThis deletes:\n• IID rotation models\n• Co-sweep calibration pairs\n• Accumulated frame positions (FM solver input)\n\nEvery IID will relearn from scratch.',
    )
    if (!confirmed) return

    setResettingAll(true)
    try {
      await resetAllRadarLearning()
      setSelectedIid(null)
      setSelectedIcao(null)
    } catch {}
    setResettingAll(false)
  }

return (
  <main className={styles.main}>
    <div className={styles.stack}>
      <div data-slot="iid-selector">
        <IIDTable
          rows={rows}
          selectedIid={selectedIid}
          onSelect={handleSelectIid}
          onResetAll={handleResetAll}
          resettingAll={resettingAll}
        />
      </div>

      <div data-slot="localisation-control">
        <LocalisationControlPanel
          iid={selectedIid}
          controlData={sharedControl}
          feedStatus={selectedIidStateStatus}
        />
      </div>

      <div data-slot="position-sources">
        <SolutionComparisonPanel
          iid={selectedIid}
          comparisonData={sharedSolution}
        />
      </div>

      <div data-slot="sweep-frames">
        <SweepFrameStrips
          iid={selectedIid}
          selectedFrame={selectedFrame}
          onSelectFrame={setSelectedFrame}
          frameData={sharedSweepFrames}
        />
      </div>

      {selectedIid != null && (
        <div data-slot="fm-status">
          <FMStatusPanel
            iid={selectedIid}
            panelData={sharedFmSummary}
            loading={selectedIid != null && sharedFmSummary == null}
          />
        </div>
      )}

      {selectedIid != null && (
        <div data-slot="evidence-map">
          <EvidenceMapPanel
            iid={selectedIid}
            controlData={sharedControl}
            evidenceData={sharedEvidence}
            evidenceRevision={selectedIidState?.revisions?.evidence ?? 0}
          />
        </div>
      )}

      {selectedIid != null && (
        <div data-slot="rotation-alignment">
          <RotationAlignmentPanel
            iid={selectedIid}
            selectedRow={selectedRow}
            selectedIcao={selectedIcao}
            onSelectIcao={setSelectedIcao}
            rows={rows}
            onSelectIid={handleSelectIid}
            syncSnapshot={syncSnapshot}
            syncFeedStatus={syncFeedStatus}
            timingPacket={sharedTimingPacket}
          />
        </div>
      )}

      <div data-slot="position-verification">
        <ReceiverCentredRadarField
          iid={selectedIid}
          selectedRow={selectedRow}
          syncSnapshot={syncSnapshot}
          frameData={sharedSweepFrames}
          fmLocationData={sharedFmLocation}
          refInfo={sharedReferenceAircraft}
          receiverPos={receiverPosition}
          timingPacket={sharedTimingPacket}
        />
      </div>
    </div>
  </main>
)
}
