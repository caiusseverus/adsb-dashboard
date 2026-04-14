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
  messageFieldPointColour,
  selectMessageFieldEvents,
} from '../utils/messageField'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const RADAR_WS_BASE = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}`
  : 'ws://localhost:8000'
const RADAR_FIELD_PERSISTENCE_US = 3_000_000
const RADAR_FIELD_BUFFER_MAX = 60_000
const RADAR_FIELD_AXIS_HEADROOM = 1.08
const RADAR_FIELD_AXIS_SHRINK_HOLD_MS = 45_000
const RADAR_FIELD_AXIS_SHRINK_TIME_CONSTANT_MS = 12_000
const EVIDENCE_METHODS = [
  'forward_model',
  'coincident_illumination',
]

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

function useRotationRows() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [rows, setRows] = useState([])

  useEffect(() => {
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/rotation`)
        if (!r.ok) return
        const data = await r.json()
        if (!cancelled) setRows(Array.isArray(data) ? data : [])
      } catch {}
    }
    poll()
    const id = setInterval(poll, 5000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [refreshKey])

  return { rows, refresh: () => setRefreshKey(v => v + 1) }
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
    let cancelled = false
    async function fetchPosition() {
      try {
        const r = await fetch(`${API_BASE}/api/status`)
        if (!r.ok) return
        const d = await r.json()
        const lat = d?.config?.receiver_lat
        const lon = d?.config?.receiver_lon
        if (!cancelled && lat != null && lon != null) {
          setData({ lat, lon })
        }
      } catch {}
    }
    fetchPosition()
    const id = setInterval(fetchPosition, 60_000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  return data
}

function LazyMountSection({ children, placeholder = 'Loading panel…', minHeight = 220, rootMargin = '600px 0px' }) {
  const hostRef = useRef(null)
  const [visible, setVisible] = useState(false)

  useEffect(() => {
    if (visible) return
    const node = hostRef.current
    if (!node) return
    if (typeof IntersectionObserver !== 'function') {
      setVisible(true)
      return
    }
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting || entry.intersectionRatio > 0)) {
        setVisible(true)
      }
    }, { rootMargin })
    observer.observe(node)
    return () => observer.disconnect()
  }, [rootMargin, visible])

  if (visible) return children

  return (
    <section ref={hostRef} className={styles.card} style={{ minHeight }}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Deferred Panel</div>
        </div>
      </div>
      <div className={styles.empty}>{placeholder}</div>
    </section>
  )
}

/** Shared hook: fetch pipeline health for an IID, polling every 10s. */
function usePipelineHealth(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/pipeline-health`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid])

  return data
}

/** Shared hook: fetch reference aircraft info for an IID, polling every 10s. */
function useReferenceAircraft(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/reference-aircraft`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid])

  return data
}

/** Shared hook: fetch sweep frames for an IID, polling every 10s. */
function useSweepFrames(iid) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/sweep-frames`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 2_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid])

  return data
}

function useFrameFmGeometry(iid, frameIndex, direction = 'cw') {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null || frameIndex == null) { setData(null); return }
    const controller = new AbortController()
    let cancelled = false
    setLoading(true)
    async function fetchGeometry() {
      try {
        const dir = direction === 'ccw' ? -1 : 1
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/sweep-frames/${frameIndex}/fm-geometry?direction=${dir}`, {
          signal: controller.signal,
        })
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    fetchGeometry()
    return () => { cancelled = true; controller.abort() }
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
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/fm-location`)
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

function useFmDiagnostics(iid, refreshKey = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    const controller = new AbortController()
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/fm-diagnostics`, {
          signal: controller.signal,
        })
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { cancelled = true; controller.abort(); clearInterval(id) }
  }, [iid, refreshKey])

  return { data, loading }
}

function useIidControl(iid, refreshKey = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/control`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid, refreshKey])

  return { data, loading }
}

function useSolutionComparison(iid, refreshKey = 0) {
  const [data, setData] = useState(null)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/solution-comparison`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
    }
    poll()
    const id = setInterval(poll, 10_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid, refreshKey])

  return data
}

function useEvidence(iid, refreshKey = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (iid == null) { setData(null); return }
    let cancelled = false
    setLoading(true)
    async function poll() {
      try {
        const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/evidence`)
        if (!r.ok) return
        const d = await r.json()
        if (!cancelled) setData(d)
      } catch {}
      finally { if (!cancelled) setLoading(false) }
    }
    poll()
    const id = setInterval(poll, 20_000)
    return () => { cancelled = true; clearInterval(id) }
  }, [iid, refreshKey])

  return { data, loading }
}

function useEvidenceMethods(iid, methods, refreshKey = 0) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const methodKey = methods.join(',')

  useEffect(() => {
    if (iid == null || methods.length === 0) {
      setData(null)
      setLoading(false)
      return
    }

    const controller = new AbortController()
    setLoading(true)

    async function fetchEvidence() {
      try {
        const results = await Promise.all(methods.map(async method => {
          const r = await fetch(`${API_BASE}/api/radar/iids/${iid}/evidence/${method}`, {
            signal: controller.signal,
          })
          if (!r.ok) return null
          return r.json()
        }))
        if (controller.signal.aborted) return
        const evidenceMethods = results.filter(Boolean)
        const display = evidenceMethods.find(method => method?.display_position)?.display_position ?? null
        startTransition(() => {
          setData({
            iid,
            available: true,
            display_source: display?.source ?? 'none',
            display_lat: display?.lat ?? null,
            display_lon: display?.lon ?? null,
            display_cep_m: display?.cep_m ?? null,
            methods: evidenceMethods,
          })
        })
      } catch (err) {
        if (!controller.signal.aborted) {
          setData(null)
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false)
      }
    }

    fetchEvidence()
    return () => controller.abort()
  }, [iid, methodKey, refreshKey])

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
    case 'coincident_illumination': return '#bc8cff'
    case 'tdoa': return '#ff7b72'
    default: return '#8b949e'
  }
}

/** 5-stage pipeline health bar */
function PipelineHealthBar({ iid }) {
  const health = usePipelineHealth(iid)

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
function ReferenceAircraftPanel({ iid }) {
  const refInfo = useReferenceAircraft(iid)

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
    case 'coincident_illumination': return 'Coincident Rays'
    case 'combined': return 'Combined'
    case 'manual': return 'Manual'
    default: return source
  }
}

function IIDTable({ rows, selectedIid, onSelect, onResetAll, resettingAll }) {
  return (
    <section className={styles.card}>
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
                <th title="SINGLE_RADAR = consistent single source; LIKELY_SINGLE = tentatively single; MULTI_RADAR = multiple overlapping radars detected; CHECK_MULTI = possible multi-radar">Rotation</th>
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
                      <span
                        className={styles.statusBadge}
                        style={{
                          background: `${statusColor(row.status)}22`,
                          color: statusColor(row.status),
                          borderColor: `${statusColor(row.status)}55`,
                        }}
                      >
                        {row.status ?? '—'}
                      </span>
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

function LocalisationControlPanel({ iid, onChanged }) {
  const [refreshKey, setRefreshKey] = useState(0)
  const { data, loading } = useIidControl(iid, refreshKey)
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
      setRefreshKey(v => v + 1)
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
    <section className={styles.card}>
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

function SolutionComparisonPanel({ iid, refreshKey = 0 }) {
  const comparison = useSolutionComparison(iid, refreshKey)

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

function collectEvidencePoints(methods) {
  const points = []
  methods.forEach(method => {
    ;(method.layers ?? []).forEach(layer => {
      ;(layer.features ?? []).forEach(feature => {
        const geometry = feature.geometry
        if (!geometry) return
        if (geometry.type === 'Point') {
          const [lon, lat] = geometry.coordinates
          points.push({ lat, lon, role: feature.properties?.role })
        } else if (geometry.type === 'LineString') {
          geometry.coordinates.forEach(([lon, lat]) => points.push({ lat, lon }))
        } else if (geometry.type === 'Circle') {
          const [lon, lat] = geometry.center
          const radiusKm = geometry.radius_km ?? 0
          const latDeg = radiusKm / 111.32
          const lonDeg = radiusKm / Math.max(111.32 * Math.cos(lat * Math.PI / 180), 1e-6)
          points.push({ lat: lat - latDeg, lon: lon - lonDeg })
          points.push({ lat: lat + latDeg, lon: lon + lonDeg })
        }
      })
    })
  })
  return points
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

function collectSolutionPoints(controlData, methods) {
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
  methods.forEach(method => {
    const estimate = method?.layers?.find(layer => layer.active_estimate)?.active_estimate
    if (estimate?.lat != null && estimate?.lon != null) {
      points.push({ lat: estimate.lat, lon: estimate.lon })
      if (estimate.cep_m != null) {
        const cepKm = estimate.cep_m / 1000
        points.push({ lat: estimate.lat + cepKm / 111.32, lon: estimate.lon })
        points.push({ lat: estimate.lat - cepKm / 111.32, lon: estimate.lon })
      }
    }
  })
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

function EvidenceMapPanel({ iid, refreshKey = 0 }) {
  const [autoKey, setAutoKey] = useState(0)
  const [deleteKey, setDeleteKey] = useState(0)
  const [outlierResult, setOutlierResult] = useState(null)  // null | {outlierSet, n_outliers, n_inliers, n_total, rejection_counts}
  const [analyseRunning, setAnalyseRunning] = useState(false)
  const [deleteRunning, setDeleteRunning] = useState(false)

  useEffect(() => {
    const id = setInterval(() => setAutoKey(k => k + 1), 30_000)
    return () => clearInterval(id)
  }, [])

  // Clear analysis whenever data refreshes (auto or after delete)
  useEffect(() => { setOutlierResult(null) }, [autoKey])

  async function handleDeletePoint(frameIndex) {
    if (frameIndex == null) return
    try {
      await fetch(`${API_BASE}/api/radar/iids/${iid}/frame-positions/${frameIndex}`, { method: 'DELETE' })
      setDeleteKey(k => k + 1)
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
      setDeleteKey(k => k + 1)
    } catch {}
    setDeleteRunning(false)
  }

  const { data, loading } = useEvidenceMethods(iid, ['forward_model'], refreshKey + autoKey + deleteKey)
  const loadedMethods = data?.methods ?? []
  const fmMethod = loadedMethods.find(m => m.method === 'forward_model') ?? { method: 'forward_model', available: false, layers: [] }
  const activeMethods = fmMethod.available ? [fmMethod] : []

  const scatterPoints = collectEvidencePoints(activeMethods).filter(p => p.role === 'frame_position_estimate')

  // Ring anchor: prefer the display (manual or fm) position, fall back to FM layer estimate
  const ringAnchor = (() => {
    if (data?.display_lat != null) return { lat: data.display_lat, lon: data.display_lon }
    const est = fmMethod.layers?.find(l => l.active_estimate)?.active_estimate
    return est?.lat != null ? { lat: est.lat, lon: est.lon } : null
  })()

  // Dynamic ring distances based on actual scatter extent
  const maxDistKm = ringAnchor && scatterPoints.length > 0
    ? Math.max(...scatterPoints.map(p => flatDistKm(ringAnchor.lat, ringAnchor.lon, p.lat, p.lon)))
    : 0
  const ringDistances = niceRingDistances(maxDistKm)

  // Bounds: scatter + anchor + inner ring context (so innermost ring is visible)
  const solutionPoints = collectSolutionPoints(data, activeMethods)
  const innerRing = ringAnchor && ringDistances.length > 0 ? ringDistances[0] : 0
  const ringContextPoints = ringAnchor && innerRing > 0 ? [
    { lat: ringAnchor.lat + innerRing / 111.32, lon: ringAnchor.lon },
    { lat: ringAnchor.lat - innerRing / 111.32, lon: ringAnchor.lon },
    { lat: ringAnchor.lat, lon: ringAnchor.lon + innerRing / kmPerLonDegree(ringAnchor.lat) },
    { lat: ringAnchor.lat, lon: ringAnchor.lon - innerRing / kmPerLonDegree(ringAnchor.lat) },
  ] : []

  const width = 600
  const height = 600
  const selectedEstimate = data?.display_lat != null && data?.display_lon != null
    ? { source: data.display_source, lat: data.display_lat, lon: data.display_lon, cep_m: data.display_cep_m }
    : null
  const frameCount = fmMethod.layers?.find(l => l.label === 'Per-Frame Estimates')?.source_count ?? 0
  const fmEstimate = fmMethod.layers?.find(l => l.active_estimate)?.active_estimate ?? null
  const manualEstimate = data?.manual_lat != null ? { lat: data.manual_lat, lon: data.manual_lon } : null

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
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Position Accumulation Map</div>
          <div className={styles.sectionLead}>
            Each dot is an independent position estimate from one sweep frame. The scatter cloud converges on the radar position as more frames accumulate. Refreshes every 30 s.
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
          Filter could not find a reliable cluster — data may not have converged yet, or all frames are scattered.
          Stage 0 rejected {outlierResult.rejection_counts?.stage0 ?? '?'} of {outlierResult.n_total} frames.
        </div>
      )}
      {loading || !bounds ? (
        <div className={styles.empty}>{loading ? 'Loading…' : 'No frame estimates yet.'}</div>
      ) : (
        <div className={styles.alignmentWrap} style={{ position: 'relative', overflow: 'hidden', maxWidth: '850px', margin: '0 auto' }}>
          <svg width="100%" viewBox={`0 0 ${width} ${height}`} style={{ display: 'block', aspectRatio: '1 / 1' }}>
            <rect x="0" y="0" width={width} height={height} fill="#0d131a" />

            {/* Range rings sized to the actual scatter extent — circles using lat-axis px/km scale */}
            {ringAnchor && ringDistances.map(ringKm => {
              const c = project(ringAnchor.lat, ringAnchor.lon, width, height)
              const edgeY = project(ringAnchor.lat + ringKm / 111.32, ringAnchor.lon, width, height)
              const r = Math.abs(edgeY.y - c.y)
              if (r < 2) return null
              return (
                <g key={`ring-${ringKm}`}>
                  <circle cx={c.x} cy={c.y} r={r}
                    fill="none" stroke="#2d3340" strokeWidth="1" strokeDasharray="3 4" />
                  <text x={c.x + 3} y={c.y - r - 3} fill="#404858" fontSize="10"
                    fontFamily="SFMono-Regular, Consolas, monospace">{ringKm} km</text>
                </g>
              )
            })}

            {/* Per-frame position estimate dots — outliers highlighted red when analysis is active */}
            {activeMethods.flatMap(method =>
              (method.layers ?? []).flatMap((layer, li) =>
                (layer.features ?? []).map((feature, fi) => {
                  const geometry = feature.geometry
                  if (!geometry || geometry.type !== 'Point') return null
                  if (feature.properties?.role !== 'frame_position_estimate') return null
                  const [lon, lat] = geometry.coordinates
                  const p = project(lat, lon, width, height)
                  const frameIndex = feature.properties.frame_index
                  const sus = feature.properties.sweep_start_us
                  const isOutlier = outlierResult != null && outlierResult.outlierSet.has(sus)
                  const dotColor = isOutlier ? '#ff7b72' : '#58a6ff'
                  const dotOpacity = isOutlier ? 0.75 : (outlierResult != null ? 0.2 : 0.3)
                  const dotR = isOutlier ? 3 : 2
                  return (
                    <g
                      key={`${li}-${fi}`}
                      onClick={() => handleDeletePoint(frameIndex)}
                      style={{ cursor: 'pointer' }}
                    >
                      <circle cx={p.x} cy={p.y} r="7" fill="transparent" />
                      <circle cx={p.x} cy={p.y} r={dotR} fill={dotColor} opacity={dotOpacity} />
                    </g>
                  )
                })
              )
            )}

            {/* FM estimate — blue dot */}
            {fmEstimate && (() => {
              const p = project(fmEstimate.lat, fmEstimate.lon, width, height)
              return (
                <g>
                  <circle cx={p.x} cy={p.y} r="5" fill="#58a6ff" />
                  <text x={p.x + 7} y={p.y - 7} fill="#58a6ff" fontSize="11" fontWeight="600">Estimate</text>
                </g>
              )
            })()}

            {/* Dashed line from FM estimate to manual position, with error label */}
            {fmEstimate && manualEstimate && (() => {
              const pFm = project(fmEstimate.lat, fmEstimate.lon, width, height)
              const pMan = project(manualEstimate.lat, manualEstimate.lon, width, height)
              const mx = (pFm.x + pMan.x) / 2
              const my = (pFm.y + pMan.y) / 2
              const errKm = flatDistKm(fmEstimate.lat, fmEstimate.lon, manualEstimate.lat, manualEstimate.lon)
              const errLabel = errKm < 1 ? `${Math.round(errKm * 1000)} m` : `${errKm.toFixed(1)} km`
              // Perpendicular offset for the label so it doesn't sit on the line
              const dx = pMan.x - pFm.x, dy = pMan.y - pFm.y
              const len = Math.hypot(dx, dy) || 1
              const ox = -dy / len * 12, oy = dx / len * 12
              return (
                <g>
                  <line x1={pFm.x} y1={pFm.y} x2={pMan.x} y2={pMan.y}
                    stroke="#8b949e" strokeWidth="1" strokeDasharray="5 4" opacity="0.7" />
                  <text x={mx + ox} y={my + oy} fill="#8b949e" fontSize="10"
                    fontFamily="SFMono-Regular, Consolas, monospace"
                    textAnchor="middle" dominantBaseline="middle">{errLabel}</text>
                </g>
              )
            })()}

            {/* Manual reference — amber dot */}
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

function RotationAlignmentPanel({ iid, selectedRow, selectedIcao, onSelectIcao, rows, onSelectIid }) {
  const [timeline, setTimeline] = useState(null)
  const [rotation, setRotation] = useState(null)
  const [resetting, setResetting] = useState(false)
  const [loading, setLoading] = useState(false)
  const [refOverride, setRefOverride] = useState(null)
  const [refOverrideSent, setRefOverrideSent] = useState(false)
  const timelineCacheRef = useRef(new Map())
  const rotationCacheRef = useRef(new Map())

  useEffect(() => {
    if (iid == null) {
      setTimeline(null)
      setRotation(null)
      setLoading(false)
      setRefOverride(null)
      setRefOverrideSent(false)
      return
    }

    let cancelled = false
    let ws
    let retryTimer = null
    let fallbackTimer = null
    let lastMessageWall = 0
    const cachedTimeline = timelineCacheRef.current.get(iid)
    const cachedRotation = rotationCacheRef.current.get(iid)

    setTimeline(cachedTimeline ?? null)
    setRotation(cachedRotation ?? null)
    setLoading(true)

    async function fetchTimeline() {
      try {
        const timelineResp = await fetch(`${API_BASE}/api/radar/iids/${iid}/timeline?window_s=90`)
        if (!timelineResp.ok) return
        const timelineData = await timelineResp.json()
        if (cancelled) return
        timelineCacheRef.current.set(iid, timelineData)
        startTransition(() => {
          setTimeline(timelineData)
        })
      } catch {}
    }

    async function fetchRotation() {
      try {
        const rotationResp = await fetch(`${API_BASE}/api/radar/iids/${iid}/rotation`)
        if (!rotationResp.ok) return
        const rotationData = await rotationResp.json()
        if (cancelled) return
        rotationCacheRef.current.set(iid, rotationData)
        startTransition(() => {
          setRotation(rotationData)
        })
      } catch {}
    }

    async function refreshOnce() {
      await Promise.allSettled([fetchTimeline(), fetchRotation()])
      if (!cancelled) setLoading(false)
    }

    function connect() {
      if (cancelled) return
      ws = new WebSocket(`${RADAR_WS_BASE}/ws/radar/iids/${iid}`)

      ws.onopen = () => {
        try {
          ws.send(JSON.stringify({ window_s: 90 }))
        } catch {}
        lastMessageWall = performance.now()
      }

      ws.onmessage = event => {
        lastMessageWall = performance.now()
        try {
          const payload = JSON.parse(event.data)
          const timelineData = payload?.timeline ?? null
          const rotationData = payload?.rotation ?? null
          if (timelineData) {
            timelineCacheRef.current.set(iid, timelineData)
            startTransition(() => {
              setTimeline(timelineData)
            })
          }
          if (rotationData) {
            rotationCacheRef.current.set(iid, rotationData)
            startTransition(() => {
              setRotation(rotationData)
            })
          }
          if (!cancelled) setLoading(false)
        } catch {}
      }

      ws.onclose = () => {
        if (cancelled) return
        retryTimer = setTimeout(connect, 2000)
      }

      ws.onerror = () => {
        // Don't close — let onclose handle reconnection
      }
    }

    refreshOnce()
    connect()
    fallbackTimer = setInterval(() => {
      if (cancelled) return
      if (performance.now() - lastMessageWall > 1500) {
        refreshOnce()
      }
    }, 1000)

    return () => {
      cancelled = true
      clearTimeout(retryTimer)
      clearInterval(fallbackTimer)
      ws?.close()
    }
  }, [iid])

  const periodS = timeline?.dominant_period_s ?? rotation?.period_s ?? null
  const icaos = Array.isArray(timeline?.icaos) ? timeline.icaos : []
  const periodUs = periodS != null ? periodS * 1_000_000 : null
  const sortedIcaos = useMemo(() => [...icaos].sort((a, b) => {
    if ((b.arrivals_us?.length ?? 0) !== (a.arrivals_us?.length ?? 0)) {
      return (b.arrivals_us?.length ?? 0) - (a.arrivals_us?.length ?? 0)
    }
    return a.icao.localeCompare(b.icao)
  }), [icaos])

  if (iid == null) {
    return (
      <section className={styles.card}>
        <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Cycle Alignment</div>
            <div className={styles.sectionLead}>
              Select an IID to compare ICAO reply timings against the detected cycle.
            </div>
          </div>
        </div>
        <div className={styles.empty}>Choose an IID from the selector above.</div>
      </section>
    )
  }

  const chartW = 980
  const rowH = 18
  const padL = 120
  const padR = 24
  const padT = 44
  const padB = 26
  const plotW = chartW - padL - padR
  const chartH = padT + padB + Math.max(sortedIcaos.length, 1) * rowH
  const minWindowUs = 60 * 1_000_000
  const visibleSpanUs = Math.max(minWindowUs, periodUs * 6)
  const gridCount = Math.max(1, Math.ceil(visibleSpanUs / periodUs))

  function relUsToX(relUs) {
    return padL + (relUs / visibleSpanUs) * plotW
  }

  function classColor(classification, selected) {
    switch (classification) {
      case 'primary': return '#58a6ff'
      case 'primary_harmonic': return '#3fb950'
      case 'residual': return '#ff7b72'
      default: return selected ? '#ffd166' : '#8b949e'
    }
  }

  async function handleReset() {
    if (iid == null || resetting) return
    setResetting(true)
    try {
      await resetIid(iid)
      setTimeline(null)
      setRotation(null)
      timelineCacheRef.current.delete(iid)
      rotationCacheRef.current.delete(iid)
      onSelectIcao(null)
      setRefOverride(null)
      setRefOverrideSent(false)
    } catch {}
    setResetting(false)
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

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
          <div>
            <div className={styles.cardTitle}>Cycle Alignment</div>
            <div className={styles.sectionLead}>
            Each row starts at that ICAO&apos;s first observed reply. Use readiness to judge whether the displayed primary period is established or still provisional under sparse traffic.
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
            Status <span className={styles.metricValue}>{rotation?.status ?? '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Readiness <span className={styles.metricValue}>{rotation?.primary_readiness ?? '—'}</span>
          </span>
          <span className={styles.metricPill}>
            Period <span className={styles.metricValue}>{periodS != null ? `${periodS.toFixed(4)}s` : '—'}</span>
          </span>
          {(selectedRow?.period_std_s != null || rotation?.period_std_s != null) && (
            <span className={styles.metricPill} title="Standard deviation of the detected period across sweeps">
              Period σ <span className={styles.metricValue}>
                ±{((selectedRow?.period_std_s ?? rotation?.period_std_s) * 1000).toFixed(1)}ms
              </span>
            </span>
          )}
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
          <span className={styles.metricPill}>
            Refresh <span className={styles.metricValue}>{loading ? 'updating' : 'live'}</span>
          </span>
          <span className={styles.legendChip}><span className={styles.legendDotPrimary} />Primary</span>
          <span className={styles.legendChip}><span className={styles.legendDotHarmonic} />Primary harmonic</span>
          <span className={styles.legendChip}><span className={styles.legendDotResidual} />Residual</span>
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

      {periodUs == null || sortedIcaos.length === 0 ? (
        <div className={styles.empty}>No cycle-alignment data yet for this IID.</div>
      ) : (
        <div className={styles.alignmentWrap}>
          <svg
            width={chartW}
            height={chartH}
            viewBox={`0 0 ${chartW} ${chartH}`}
            className={styles.alignmentSvg}
          >
            {Array.from({ length: gridCount + 1 }, (_, idx) => {
              const relUs = idx * periodUs
              const x = relUsToX(relUs)
              return (
                <g key={idx}>
                  <line
                    x1={x}
                    y1={padT - 10}
                    x2={x}
                    y2={chartH - padB}
                    stroke={idx === 0 ? '#58a6ff66' : '#30363d'}
                    strokeDasharray={idx === 0 ? '0' : '4 4'}
                  />
                  <text x={x} y={18} textAnchor="middle" className={styles.axisLabel}>
                    {(idx * periodS).toFixed(1)}s
                  </text>
                </g>
              )
            })}

            <text x={padL + plotW / 2} y={chartH - 6} textAnchor="middle" className={styles.axisLabel}>
              Elapsed time since each ICAO&apos;s first observed reply
            </text>

            {sortedIcaos.map((entry, rowIdx) => {
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
                          .filter(relUs => relUs >= 0 && relUs <= visibleSpanUs)
                    const pointColor = classColor(series.family, isSelected)
                    const yOffset = series.family.startsWith('primary') ? -2 : 2

                    return relArrivals.map((relUs, idx) => (
                      <circle
                        key={`${entry.icao}-${series.family}-${seriesIdx}-${idx}-${relUs}`}
                        cx={relUsToX(relUs)}
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

/**
 * Colour a radar-field dot by how early (blue) or late (red) the message
 * arrived relative to the sweep beam passing that bearing.
 *   delta > 0  →  dot bearing is ahead of beam at arrival time  →  early  →  blue
 *   delta < 0  →  beam had already passed the dot bearing       →  late   →  red
 */
function sweepTimingColour(ev, beamAnchor, periodUs) {
  if (beamAnchor == null || periodUs == null || ev.bearing_deg == null) return '#8b949e'
  const beamAtArrival = (
    (beamAnchor.ref_bearing_deg + ((ev.arrival_us - beamAnchor.ref_arrival_us) / periodUs) * 360) % 360 + 360
  ) % 360
  // Normalise to (−180, +180]: positive = dot ahead of beam = early
  const delta = ((ev.bearing_deg - beamAtArrival + 540) % 360) - 180
  const maxDeg = 30  // ±30° maps to full saturation
  const t = Math.max(-1, Math.min(1, delta / maxDeg))
  const abs_t = Math.abs(t)
  const hue = t >= 0 ? 213 : 3
  const sat = Math.round(abs_t * 88)
  const light = Math.round(55 + (1 - abs_t) * 10)
  return `hsl(${hue},${sat}%,${light}%)`
}

const SYNC_STALE_US = 15_000_000  // 15 s without a DF11 → aircraft gone

function ReceiverCentredRadarField({ iid, selectedRow }) {
  const canvasRef = useRef(null)
  const rafRef = useRef(null)
  const rangeAxisMaxRef = useRef(null)
  const displayNowUsRef = useRef(0)
  const displayNowWallRef = useRef(performance.now())
  const beamAnchorRef = useRef(null)
  const lastDrawMsRef = useRef(0)
  const canvasSizeRef = useRef({ width: 0, height: 0 })
  const lastSeenUsRef = useRef({})   // icao → latest DF11 arrival_us seen
  const [liveEnabled] = useState(true)
  const [syncOverrideIcao, setSyncOverrideIcao] = useState(null)
  const [displaySyncIcao, setDisplaySyncIcao] = useState(null)

  const frameData = useSweepFrames(iid)
  const { data: fmLocationData } = useFmLocation(iid)
  const refInfo = useReferenceAircraft(iid)
  const receiverPos = useReceiverPosition()
  const timingPacket = useTimingEventStream({ enabled: liveEnabled, iid, df11Only: true })
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
    if (!liveEnabled) return []
    return selectMessageFieldEvents({
      events: timingView?.events ?? [],
      renderNowUs: Number(timingView?.nowUs ?? 0),
      persistenceUs: RADAR_FIELD_PERSISTENCE_US + MESSAGE_FIELD_RENDER_HOLDBACK_US,
      trafficFilter: 'df_surveillance',
      iidFilter: iid == null ? 'all' : iid,
    }).filter(ev => Number.isFinite(ev.bearing_deg) && Number.isFinite(ev.range_nm))
  }, [iid, liveEnabled, timingView?.events, timingView?.nowUs])
  const latestFrame = [...frames].reverse().find(f => {
    if (!(f.quality === 'good' || f.quality === 'marginal')) return false
    if (effectivePrefIcao != null) return f.ref_icao === effectivePrefIcao
    const currentRefIcao = beamAnchorRef.current?.ref_icao
    return currentRefIcao ? f.ref_icao === currentRefIcao : true
  }) ?? [...frames].reverse().find(f => f.quality === 'good' || f.quality === 'marginal')
  const period_s = beamAnchorRef.current?.period_s ?? latestFrame?.period_s ?? selectedRow?.period_s ?? null
  const fmPos = fmLocationData?.status === 'LOCALISED' ? fmLocationData : null

  // Track the most recent DF11 arrival_us per ICAO across the live event buffer.
  useEffect(() => {
    for (const ev of radarFieldEvents) {
      if (ev.df !== 11 || ev.iid !== iid) continue
      const prev = lastSeenUsRef.current[ev.icao] ?? 0
      if (ev.arrival_us > prev) lastSeenUsRef.current[ev.icao] = ev.arrival_us
    }
  }, [radarFieldEvents, iid])

  // Auto re-sync: when the current anchor ICAO hasn't been seen for SYNC_STALE_US, switch.
  useEffect(() => {
    const anchor = beamAnchorRef.current
    if (!anchor) return
    const nowUs = Number(timingView?.nowUs ?? 0)
    if (nowUs === 0) return
    const lastSeen = lastSeenUsRef.current[anchor.ref_icao] ?? 0
    if (lastSeen === 0 || nowUs - lastSeen <= SYNC_STALE_US) return
    // Find the best currently-visible replacement.
    const candidates = [preferredRefIcao, ...frames.map(f => f.ref_icao)].filter(Boolean)
    const newIcao = candidates.find(icao => (lastSeenUsRef.current[icao] ?? 0) > nowUs - SYNC_STALE_US) ?? null
    if (newIcao === anchor.ref_icao) return
    beamAnchorRef.current = null
    setSyncOverrideIcao(newIcao !== preferredRefIcao ? newIcao : null)
    setDisplaySyncIcao(null)
  }, [timingView?.nowUs, preferredRefIcao, frames])

  useEffect(() => {
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
    // Immediately pick the best visible ICAO rather than waiting for auto-detection.
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
    if (!liveEnabled) return
    const draw = () => {
      rafRef.current = requestAnimationFrame(draw)
      const canvas = canvasRef.current
      if (!canvas) return
      if (document.hidden) return
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
      const cutoffUs = Math.max(0, renderNowUs - RADAR_FIELD_PERSISTENCE_US)
      const events = []
      for (let index = radarFieldEvents.length - 1; index >= 0; index -= 1) {
        const ev = radarFieldEvents[index]
        if (ev.arrival_us > renderNowUs) continue
        if (ev.arrival_us < cutoffUs) break
        events.push(ev)
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

      const targetRangeMax = Math.max(50, Math.max(1, ...events.map(ev => ev.range_nm ?? 0)) * RADAR_FIELD_AXIS_HEADROOM)
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
      if (fmPos && receiverPos && Number.isFinite(receiverPos.lat) && Number.isFinite(receiverPos.lon)) {
        const radarBearingDeg = bearing(receiverPos.lat, receiverPos.lon, fmPos.lat, fmPos.lon)
        const radarRangeNm = haversineNm(receiverPos.lat, receiverPos.lon, fmPos.lat, fmPos.lon)
        const radarTheta = (radarBearingDeg - 90) * Math.PI / 180
        const radarRadius = Math.min(radius, (radarRangeNm / Math.max(1e-6, maxRangeNm)) * radius)
        beamOrigin = {
          x: cx + Math.cos(radarTheta) * radarRadius,
          y: cy + Math.sin(radarTheta) * radarRadius,
        }

        if (beamAnchor?.ref_bearing_deg != null && beamAnchor?.ref_arrival_us != null && periodUs != null && renderNowUs > 0) {
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

      for (const ev of events) {
        const r = (ev.range_nm / Math.max(1e-6, maxRangeNm)) * radius
        const theta = (ev.bearing_deg - 90) * Math.PI / 180
        const x = cx + Math.cos(theta) * r
        const y = cy + Math.sin(theta) * r
        const ageRatio = (renderNowUs - ev.arrival_us) / Math.max(1, RADAR_FIELD_PERSISTENCE_US)
        const alpha = 0.18 + (1 - Math.min(1, ageRatio)) * 0.8
        ctx.fillStyle = sweepTimingColour(ev, beamAnchor, periodUs)
        ctx.globalAlpha = alpha
        const size = ev.msg_len >= 14 ? 3.5 : 2.5
        ctx.fillRect(x - size / 2, y - size / 2, size, size)
        ctx.globalAlpha = 1
      }

      ctx.beginPath()
      ctx.arc(cx, cy, 4.5, 0, Math.PI * 2)
      ctx.fillStyle = '#58a6ff'
      ctx.fill()

      if (fmPos && receiverPos) {
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
      ctx.fillText(`${events.length} points`, width - 12, height - 12)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current)
    }
  }, [
    fmPos?.lat,
    fmPos?.lon,
    iid,
    latestFrame?.frame_index,
    period_s,
    preferredRefIcao,
    receiverPos?.lat,
    receiverPos?.lon,
    radarFieldEvents,
  ])

  const syncLabel = displaySyncIcao
    ? <span style={{ fontFamily: 'SFMono-Regular, Consolas, monospace', color: '#58a6ff', marginLeft: '0.6rem', fontSize: '0.82rem' }}>
        sync: {displaySyncIcao}
      </span>
    : null

  return (
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>
            Position Verification{syncLabel}
          </div>
          <div className={styles.sectionLead}>
            DF11 arrivals for the selected IID on a receiver-centred polar bearing/range field, with about 3 s persistence.
            {' '}The beam is projected from the solved radar position into that field.
          </div>
        </div>
        <button
          className={styles.btn}
          onClick={handleResetSync}
          title="Re-assess which aircraft to use for beam synchronisation"
        >
          Reset Sync
        </button>
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', padding: '0.5rem 0' }}>
        <canvas
          ref={canvasRef}
          width={720}
          height={720}
          style={{ borderRadius: '4px', background: '#0b0c10', width: '100%', maxWidth: '720px', aspectRatio: '1 / 1' }}
        />
      </div>
      <div style={{ fontSize: '0.72rem', color: '#8b949e', textAlign: 'center', marginTop: '0.25rem' }}>
        Receiver shown in blue. {fmPos ? 'Solved radar shown in amber.' : 'Run FM to project the radar beam origin.'}
        {' '}Beam rotation uses the latest good sweep frame and measured period when available.
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

function FMStatusPanel({ iid, refreshKey = 0 }) {
  const { data: panelData, loading } = useFmDiagnostics(iid, refreshKey)

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
    <section className={styles.card}>
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
    tileLayerRef.current = L.tileLayer(basemap.url, {
      attribution: basemap.attribution,
      subdomains: basemap.subdomains,
      maxZoom: basemap.maxZoom,
    }).addTo(map)
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

function SweepFrameStrips({ iid, selectedFrame, onSelectFrame }) {
  const frameData = useSweepFrames(iid)
  const DISPLAY_COUNT = 15

  const allFrames = frameData?.frames ?? []
  const qualFrames = allFrames.filter(f => f.quality === 'good' || f.quality === 'marginal')
  // Show last DISPLAY_COUNT quality frames, most recent at top
  const displayFrames = qualFrames.slice(-DISPLAY_COUNT).reverse()
  const nGood = allFrames.filter(f => f.quality === 'good').length
  const nMarginal = allFrames.filter(f => f.quality === 'marginal').length

  // Selected frame detail
  const detailFrame = selectedFrame != null
    ? allFrames.find(f => f.frame_index === selectedFrame)
    : null

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
    <section className={styles.card}>
      <div className={styles.cardHeader}>
        <div>
          <div className={styles.cardTitle}>Sweep Frames</div>
          <div className={styles.sectionLead}>
            {allFrames.length} frames · {nGood} good · {nMarginal} marginal
            {' · '}Last {displayFrames.length} shown · ref at 0°, click for detail
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
          const isSelected = selectedFrame === frame.frame_index
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
        </div>
      )}
      <FrameGeometryDiagnostics iid={iid} frameIndex={selectedFrame} />
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
  const { rows, refresh: refreshRows } = useRotationRows()
  const [selectedIid, setSelectedIid] = useState(null)
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [selectedFrame, setSelectedFrame] = useState(null)
  const [resettingAll, setResettingAll] = useState(false)
  const [controlRefreshKey, setControlRefreshKey] = useState(0)
  const selectedRow = rows.find(row => row.iid === selectedIid) ?? null

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
      refreshRows()
      setControlRefreshKey(v => v + 1)
    } catch {}
    setResettingAll(false)
  }

  function handleChanged() {
    refreshRows()
    setControlRefreshKey(v => v + 1)
  }

  return (
    <main className={styles.main}>
      <div className={styles.stack}>
        <IIDTable
          rows={rows}
          selectedIid={selectedIid}
          onSelect={handleSelectIid}
          onResetAll={handleResetAll}
          resettingAll={resettingAll}
        />
        <LocalisationControlPanel
          iid={selectedIid}
          onChanged={handleChanged}
        />
        <SolutionComparisonPanel iid={selectedIid} refreshKey={controlRefreshKey} />
        <LazyMountSection placeholder="Loading sweep frames when visible…" minHeight={320}>
          <SweepFrameStrips iid={selectedIid} selectedFrame={selectedFrame} onSelectFrame={setSelectedFrame} />
        </LazyMountSection>
        <FMStatusPanel iid={selectedIid} refreshKey={controlRefreshKey} onChanged={handleChanged} />
        <LazyMountSection placeholder="Loading evidence map when visible…" minHeight={420}>
          <EvidenceMapPanel iid={selectedIid} refreshKey={controlRefreshKey} />
        </LazyMountSection>
        <RotationAlignmentPanel
          iid={selectedIid}
          selectedRow={selectedRow}
          selectedIcao={selectedIcao}
          onSelectIcao={setSelectedIcao}
          rows={rows}
          onSelectIid={handleSelectIid}
        />
        <LazyMountSection placeholder="Loading position verification when visible…" minHeight={760}>
          <ReceiverCentredRadarField iid={selectedIid} selectedRow={selectedRow} />
        </LazyMountSection>
      </div>
    </main>
  )
}
