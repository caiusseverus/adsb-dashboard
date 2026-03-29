// Shared display-formatting utilities used across multiple components.

/**
 * Continuous HSL gradient for heatmap cells.
 * Colour scale: purple (low) → blue → yellow → green (high).
 * Matches the scatter-plot palette used throughout the dashboard.
 */
export function cellColor(value, maxVal) {
  if (!value) return '#21262d'
  const t = Math.max(0, Math.min(1, value / maxVal))
  let h
  if (t < 0.444) {
    h = 280 - (t / 0.444) * 70           // purple → blue (280→210)
  } else if (t < 0.778) {
    h = 210 - ((t - 0.444) / 0.334) * 150 // blue → yellow (210→60)
  } else {
    h = 60 + ((t - 0.778) / 0.222) * 60   // yellow → green (60→120)
  }
  return `hsl(${Math.round(h)},80%,55%)`
}

/** Format a Unix timestamp as a locale date+time string, or '—' if absent. */
export function fmtTs(unix) {
  if (!unix) return '—'
  return new Date(unix * 1000).toLocaleString()
}

/** Format an altitude in feet with thousands separator, or '—' if absent. */
export function fmtAlt(alt) {
  if (alt == null) return '—'
  return alt.toLocaleString() + ' ft'
}
