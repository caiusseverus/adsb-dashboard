/**
 * BenchmarkPanel.jsx
 *
 * Pipeline benchmark panel for the Status page.
 * Calls GET /api/debug/benchmark to run/fetch results and
 * GET /api/debug/benchmark/status to poll while a run is in progress.
 *
 * Usage in StatusPage.jsx:
 *   import BenchmarkPanel from './BenchmarkPanel'
 *   // add <BenchmarkPanel /> anywhere inside the StatusPage return
 */

import { useState, useEffect, useRef, useCallback } from 'react'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

// ─── Colours ─────────────────────────────────────────────────────────────────

const VERDICT = {
  PASS:     { bg: 'rgba(63,185,80,0.12)',  fg: '#3fb950', border: 'rgba(63,185,80,0.3)'  },
  MARGINAL: { bg: 'rgba(210,153,34,0.12)', fg: '#d29922', border: 'rgba(210,153,34,0.3)' },
  FAIL:     { bg: 'rgba(248,81,73,0.12)',  fg: '#f85149', border: 'rgba(248,81,73,0.3)'  },
}

// ─── Stage definitions ────────────────────────────────────────────────────────

const STAGES = [
  { key: 'stage_beast_parse',   label: 'Beast parse',         desc: 'TCP bytes → message dict',                           target: 40  },
  { key: 'stage_decode_warm',   label: 'Decode (warm)',        desc: 'process_message() — known aircraft, no I/O',         target: 400 },
  { key: 'stage_new_aircraft',  label: 'New aircraft (cold)',  desc: 'process_message() — first frame, full enrichment',   target: 400 },
  { key: 'stage_get_snapshot',  label: 'get_snapshot()',       desc: 'Build broadcast dict under lock (1×/sec)',           target: 500 },
  { key: 'stage_json_serial',   label: 'JSON serialise',       desc: 'orjson/json snapshot → bytes (1×/sec)',              target: 200 },
  { key: 'stage_full_pipeline', label: '★ Full pipeline',      desc: 'Beast parse + decode end-to-end — the key figure',   target: 400 },
]

// ─── Latency bar ─────────────────────────────────────────────────────────────

function LatencyBar({ p50, p95, p99, target }) {
  const maxVal  = Math.max(p99 * 1.1, target * 1.5, 1)
  const pct     = v => Math.min(100, (v / maxVal) * 100)

  return (
    <div style={{ position: 'relative', height: 22, background: '#0b0c10', borderRadius: 3, overflow: 'hidden', marginTop: 5 }}>
      <div style={{ position: 'absolute', left: 0, top: 0, height: '100%', width: `${pct(p99)}%`, background: 'rgba(248,81,73,0.15)' }} />
      <div style={{ position: 'absolute', left: 0, top: 0, height: '100%', width: `${pct(p95)}%`, background: 'rgba(210,153,34,0.22)' }} />
      <div style={{ position: 'absolute', left: 0, top: 0, height: '100%', width: `${pct(p50)}%`, background: 'rgba(56,139,253,0.38)' }} />
      {/* target threshold line */}
      <div style={{ position: 'absolute', left: `${pct(target)}%`, top: 0, width: 1, height: '100%', background: '#3fb950', opacity: 0.75 }} />
      <div style={{ position: 'absolute', left: 6, top: '50%', transform: 'translateY(-50%)', fontSize: '0.7rem', color: '#388bfd', fontVariantNumeric: 'tabular-nums' }}>
        p50 {p50}µs
      </div>
      <div style={{ position: 'absolute', right: 6, top: '50%', transform: 'translateY(-50%)', fontSize: '0.7rem', color: '#6e7681', fontVariantNumeric: 'tabular-nums' }}>
        p99 {p99}µs
      </div>
    </div>
  )
}

// ─── Single stage row ─────────────────────────────────────────────────────────

function StageRow({ stage, data }) {
  if (!data) return null
  const pass      = data.p95_us <= stage.target
  const rateColor = data.max_sustained_rate >= 3500 ? '#3fb950' : data.max_sustained_rate >= 2000 ? '#d29922' : '#f85149'

  return (
    <div style={{ padding: '0.7rem 0', borderBottom: '1px solid #21262d' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: '1rem', flexWrap: 'wrap' }}>
        <div>
          <span style={{ color: '#c9d1d9', fontSize: '0.87rem', fontWeight: 500 }}>{stage.label}</span>
          <span style={{ color: '#484f58', fontSize: '0.74rem', marginLeft: '0.55rem' }}>{stage.desc}</span>
        </div>
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', flexShrink: 0 }}>
          <span style={{ fontSize: '0.74rem', color: pass ? '#3fb950' : '#f85149', fontWeight: 600 }}>
            p95: {data.p95_us}µs {pass ? '✓' : `✗ >target ${stage.target}µs`}
          </span>
          <span style={{ fontSize: '0.74rem', color: rateColor, fontVariantNumeric: 'tabular-nums', fontWeight: 500 }}>
            ≤{data.max_sustained_rate.toLocaleString()} msg/s
          </span>
        </div>
      </div>
      <LatencyBar p50={data.p50_us} p95={data.p95_us} p99={data.p99_us} target={stage.target} />
      <div style={{ display: 'flex', gap: '1.2rem', marginTop: 4, flexWrap: 'wrap' }}>
        {[['mean', data.mean_us], ['p50', data.p50_us], ['p95', data.p95_us], ['p99', data.p99_us], ['max', data.max_us]].map(([lbl, val]) => (
          <span key={lbl} style={{ fontSize: '0.7rem', color: '#484f58', fontVariantNumeric: 'tabular-nums' }}>
            <span style={{ color: '#6e7681' }}>{lbl} </span>{val}µs
          </span>
        ))}
        <span style={{ fontSize: '0.7rem', color: '#484f58', marginLeft: 'auto', fontVariantNumeric: 'tabular-nums' }}>
          {data.samples.toLocaleString()} samples
        </span>
      </div>
    </div>
  )
}

// ─── Environment row ──────────────────────────────────────────────────────────

function EnvRow({ label, value, ok, warn }) {
  const color = ok ? '#3fb950' : warn ? '#f85149' : '#c9d1d9'
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '0.3rem 0', borderBottom: '1px solid #1c2128', fontSize: '0.81rem' }}>
      <span style={{ color: '#8b949e' }}>{label}</span>
      <span style={{ color, fontVariantNumeric: 'tabular-nums' }}>{String(value)}</span>
    </div>
  )
}

// ─── Animated spinner ─────────────────────────────────────────────────────────

function Spinner() {
  return (
    <span style={{ display: 'inline-block', width: 12, height: 12, border: '2px solid #30363d', borderTop: '2px solid #388bfd', borderRadius: '50%', animation: 'spin 0.7s linear infinite', marginRight: 6 }} />
  )
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function BenchmarkPanel() {
  const [result,   setResult]   = useState(null)
  const [running,  setRunning]  = useState(false)
  const [error,    setError]    = useState(null)
  const [elapsed,  setElapsed]  = useState(0)
  const pollRef   = useRef(null)
  const timerRef  = useRef(null)
  const startedAt = useRef(null)

  // On mount, fetch cached result / status without triggering a run
  useEffect(() => {
    fetch(`${API_BASE}/api/debug/benchmark/status`)
      .then(r => r.ok ? r.json() : null)
      .then(s => {
        if (!s) return
        if (s.running) {
          startPolling()
        } else if (s.has_result) {
          // Load the cached result immediately
          fetch(`${API_BASE}/api/debug/benchmark`)
            .then(r => r.ok ? r.json() : null)
            .then(d => { if (d) setResult(d) })
            .catch(() => {})
        }
      })
      .catch(() => {})
    return stopPolling
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const startPolling = useCallback(() => {
    stopPolling()
    startedAt.current = Date.now()
    setRunning(true)
    setElapsed(0)

    // Elapsed-time ticker
    timerRef.current = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startedAt.current) / 1000))
    }, 500)

    // Poll status every 800ms; fetch result when done
    pollRef.current = setInterval(async () => {
      try {
        const s = await fetch(`${API_BASE}/api/debug/benchmark/status`).then(r => r.json())
        if (!s.running) {
          stopPolling()
          setRunning(false)
          // Fetch the completed result
          const d = await fetch(`${API_BASE}/api/debug/benchmark`).then(r => r.json())
          setResult(d)
        }
      } catch {
        stopPolling()
        setRunning(false)
        setError('Lost connection while benchmark was running')
      }
    }, 800)
  }, [])

  const stopPolling = useCallback(() => {
    clearInterval(pollRef.current)
    clearInterval(timerRef.current)
  }, [])

  const handleRun = useCallback(async (fresh = true) => {
    setError(null)
    try {
      // Fire the request — it will block on the server side while the decoder
      // is paused and the benchmark runs (~5–30s).  We poll status separately
      // so the UI stays responsive and shows progress.
      fetch(`${API_BASE}/api/debug/benchmark?fresh=${fresh}&n=5000`)
        .then(r => {
          if (r.status === 409) throw new Error('Benchmark already running — wait and retry')
          if (!r.ok) throw new Error(`HTTP ${r.status}`)
          return r.json()
        })
        .then(d => {
          setResult(d)
          setRunning(false)
          stopPolling()
        })
        .catch(e => {
          setError(String(e))
          setRunning(false)
          stopPolling()
        })

      // Give the server ~300ms to accept the request and flip the running flag,
      // then start polling so the UI reflects the running state immediately.
      setTimeout(startPolling, 300)
    } catch (e) {
      setError(String(e))
    }
  }, [startPolling, stopPolling])

  const verdict      = result?.verdict
  const verdictStyle = verdict ? VERDICT[verdict] : null

  return (
    <div style={{ background: '#161b22', border: '1px solid #21262d', borderRadius: 8, overflow: 'hidden', marginBottom: '1.25rem' }}>
      {/* keyframe for spinner — injected once */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>

      {/* ── Header ── */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.75rem 1.25rem', borderBottom: '1px solid #21262d', flexWrap: 'wrap', gap: '0.5rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
          <span style={{ color: '#c9d1d9', fontWeight: 600, fontSize: '0.95rem' }}>Pipeline Benchmark</span>
          {running && (
            <span style={{ display: 'flex', alignItems: 'center', fontSize: '0.78rem', color: '#388bfd' }}>
              <Spinner />decoder paused · {elapsed}s elapsed
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          {result && !running && (
            <span style={{ fontSize: '0.71rem', color: '#484f58' }}>
              {new Date(result.timestamp * 1000).toLocaleTimeString()} · {result.total_bench_time_s}s · {result.n_msgs.toLocaleString()} iters
            </span>
          )}
          <button
            onClick={() => handleRun(true)}
            disabled={running}
            style={btnStyle(true, running)}
          >
            {running ? 'Running…' : result ? 'Run again' : 'Run benchmark'}
          </button>
        </div>
      </div>

      {/* ── Warning banner: decoder is paused ── */}
      {running && (
        <div style={{ background: 'rgba(210,153,34,0.08)', borderBottom: '1px solid rgba(210,153,34,0.2)', padding: '0.5rem 1.25rem', fontSize: '0.81rem', color: '#d29922' }}>
          ⚠ Live decoding is paused. Aircraft data will not update until the benchmark completes.
        </div>
      )}

      {/* ── Error ── */}
      {error && (
        <div style={{ padding: '0.75rem 1.25rem', color: '#f85149', fontSize: '0.84rem' }}>{error}</div>
      )}

      {/* ── Empty state ── */}
      {!result && !running && !error && (
        <div style={{ padding: '2rem 1.25rem', textAlign: 'center', color: '#484f58', fontSize: '0.85rem', lineHeight: 1.7 }}>
          Click <strong style={{ color: '#8b949e' }}>Run benchmark</strong> to measure pipeline throughput across all stages.<br />
          <span style={{ fontSize: '0.78rem' }}>
            The live Beast decoder will be paused for the duration (~5–20s) so measurements are not contaminated by real traffic.
          </span>
        </div>
      )}

      {/* ── Progress placeholder while running ── */}
      {running && !result && (
        <div style={{ padding: '2rem 1.25rem', textAlign: 'center', color: '#484f58', fontSize: '0.85rem', lineHeight: 1.8 }}>
          Running 5,000-message benchmark…<br />
          <span style={{ fontSize: '0.78rem' }}>Beast parse → warm decode → cold decode → snapshot → JSON → full pipeline</span>
        </div>
      )}

      {/* ── Results ── */}
      {result && (
        <div style={{ padding: '0 1.25rem' }}>

          {/* Verdict */}
          <div style={{
            background: verdictStyle.bg,
            border: `1px solid ${verdictStyle.border}`,
            borderRadius: 6,
            padding: '0.55rem 1rem',
            margin: '1rem 0',
            display: 'flex', alignItems: 'center', gap: '0.75rem',
          }}>
            <span style={{ color: verdictStyle.fg, fontWeight: 700, fontSize: '0.92rem' }}>{verdict}</span>
            <span style={{ color: verdictStyle.fg, fontSize: '0.83rem', opacity: 0.9 }}>{result.verdict_detail}</span>
          </div>

          {/* Decoder pause notice */}
          {result.decoder_paused_during_run === false && (
            <div style={{ background: 'rgba(210,153,34,0.08)', border: '1px solid rgba(210,153,34,0.2)', borderRadius: 6, padding: '0.45rem 0.9rem', marginBottom: '0.75rem', fontSize: '0.78rem', color: '#d29922' }}>
              ⚠ This result was captured without pausing the decoder — timings may be inflated by GIL contention from live traffic.
            </div>
          )}

          {/* Environment */}
          <div style={{ color: '#484f58', fontSize: '0.7rem', textTransform: 'uppercase', letterSpacing: '0.06em', padding: '0.5rem 0 0.2rem' }}>Environment</div>
          <EnvRow label="Python"         value={result.python_version} />
          <EnvRow label="orjson"         value={result.orjson ? `✓ ${result.orjson_version}` : '✗ not installed'} ok={result.orjson} warn={!result.orjson} />
          <EnvRow label="pyModeS"        value={result.pymodes_version} />
          <EnvRow label="pyModeS Cython" value={result.pymodes_cython ? '✓ C extension loaded' : '✗ pure Python — install pyModeS[cython]'} ok={result.pymodes_cython} warn={!result.pymodes_cython} />
          <EnvRow label="Warm aircraft"  value={result.warm_aircraft_count} />

          {/* Stages */}
          <div style={{ color: '#484f58', fontSize: '0.7rem', textTransform: 'uppercase', letterSpacing: '0.06em', padding: '0.9rem 0 0' }}>
            Stage breakdown — target: sustain ≥3,500 msgs/s (≤286µs p95 end-to-end)
          </div>
          {STAGES.map(s => <StageRow key={s.key} stage={s} data={result[s.key]} />)}

          {/* Legend */}
          <div style={{ display: 'flex', gap: '1.1rem', padding: '0.6rem 0 0.8rem', fontSize: '0.7rem', color: '#484f58', flexWrap: 'wrap' }}>
            {[['rgba(56,139,253,0.38)', 'p50'], ['rgba(210,153,34,0.22)', 'p95'], ['rgba(248,81,73,0.15)', 'p99']].map(([bg, lbl]) => (
              <span key={lbl} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                <span style={{ display: 'inline-block', width: 10, height: 10, background: bg, borderRadius: 2 }} />{lbl}
              </span>
            ))}
            <span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <span style={{ display: 'inline-block', width: 1, height: 10, background: '#3fb950' }} />target threshold
            </span>
            <span style={{ marginLeft: 'auto' }}>All times in µs. max sustained rate = 1 ÷ p95.</span>
          </div>
        </div>
      )}
    </div>
  )
}

function btnStyle(primary = false, disabled = false) {
  return {
    background:    primary ? (disabled ? '#21262d' : '#1f6feb') : '#21262d',
    border:        primary ? '1px solid #388bfd' : '1px solid #30363d',
    borderRadius:  6,
    color:         '#e6edf3',
    fontSize:      '0.8rem',
    padding:       '0.3rem 0.9rem',
    cursor:        disabled ? 'default' : 'pointer',
    opacity:       disabled ? 0.55 : 1,
    fontWeight:    primary ? 500 : 400,
  }
}
