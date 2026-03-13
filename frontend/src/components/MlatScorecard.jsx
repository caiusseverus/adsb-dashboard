import styles from './MlatScorecard.module.css'

/**
 * Aggregate per-aircraft mlat_sources data into per-network totals.
 * Returns an array of { source, fixes, spikes, spikeRate, medianResidual, quality }
 * sorted by quality descending.
 */
function aggregateSources(aircraft) {
  const totals = {}   // source → { fixes, spikes, residuals[], qualities[] }

  for (const ac of aircraft) {
    const srcs = ac.mlat_sources
    const quals = ac.mlat_quality
    if (!srcs) continue
    for (const [src, stats] of Object.entries(srcs)) {
      if (!totals[src]) totals[src] = { fixes: 0, spikes: 0, residuals: [], qualities: [] }
      totals[src].fixes  += stats.fixes  ?? 0
      totals[src].spikes += stats.spikes ?? 0
      if (stats.median_residual != null) totals[src].residuals.push(stats.median_residual)
      if (quals?.[src] != null)          totals[src].qualities.push(quals[src])
    }
  }

  return Object.entries(totals)
    .map(([source, d]) => {
      const attempted = d.fixes + d.spikes
      const spikeRate = attempted > 0 ? d.spikes / attempted : 0
      const sorted    = [...d.residuals].sort((a, b) => a - b)
      const medianRes = sorted.length > 0 ? sorted[Math.floor(sorted.length / 2)] : null
      const quality   = d.qualities.length > 0
        ? d.qualities.reduce((s, v) => s + v, 0) / d.qualities.length
        : null
      return { source, fixes: d.fixes, spikes: d.spikes, spikeRate, medianRes, quality }
    })
    .filter(d => d.fixes + d.spikes > 0)
    .sort((a, b) => (b.quality ?? 0) - (a.quality ?? 0))
}

function qualityColor(q) {
  if (q == null) return '#8b949e'
  if (q >= 0.9)  return '#3fb950'   // green
  if (q >= 0.7)  return '#e3b341'   // amber
  return '#f85149'                   // red
}

function qualityBar(q) {
  const pct = q != null ? Math.round(q * 100) : 0
  const color = qualityColor(q)
  return (
    <div className={styles.barTrack}>
      <div className={styles.barFill} style={{ width: `${pct}%`, background: color }} />
      <span className={styles.barLabel} style={{ color }}>{q != null ? `${pct}%` : '—'}</span>
    </div>
  )
}

export default function MlatScorecard({ aircraft }) {
  if (!aircraft?.length) return null

  const rows = aggregateSources(aircraft)
  if (rows.length === 0) return null

  return (
    <div className={styles.card}>
      <div className={styles.heading}>MLAT Network Quality</div>
      <table className={styles.table}>
        <thead>
          <tr>
            <th>Network</th>
            <th className={styles.right}>Fixes</th>
            <th className={styles.right}>Spike&nbsp;rate</th>
            <th className={styles.right}>Median&nbsp;residual</th>
            <th className={styles.quality}>Quality</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(row => (
            <tr key={row.source}>
              <td className={styles.name}>{row.source}</td>
              <td className={styles.right}>{row.fixes.toLocaleString()}</td>
              <td className={styles.right} style={{ color: row.spikeRate > 0.05 ? '#f85149' : '#8b949e' }}>
                {(row.spikeRate * 100).toFixed(1)}%
                {row.spikes > 0 && <span className={styles.spikeCount}> ({row.spikes})</span>}
              </td>
              <td className={styles.right} style={{ color: row.medianRes == null ? '#8b949e' : row.medianRes > 1 ? '#e3b341' : '#3fb950' }}>
                {row.medianRes != null ? `${row.medianRes.toFixed(2)} nm` : '—'}
              </td>
              <td className={styles.quality}>{qualityBar(row.quality)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
