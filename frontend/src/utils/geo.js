/**
 * Shared geographic utility functions.
 */

const R_NM = 3440.065 // Earth radius in nautical miles

/**
 * Great-circle distance in nautical miles. Returns null if any argument is null/undefined.
 */
export function haversineNm(lat1, lon1, lat2, lon2) {
  if ([lat1, lon1, lat2, lon2].some(v => v == null)) return null
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2
  return R_NM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

/**
 * True bearing in degrees (0=N, 90=E) from (lat1,lon1) to (lat2,lon2).
 */
export function bearingDeg(lat1, lon1, lat2, lon2) {
  const dLon = (lon2 - lon1) * Math.PI / 180
  const lat1r = lat1 * Math.PI / 180
  const lat2r = lat2 * Math.PI / 180
  const x = Math.sin(dLon) * Math.cos(lat2r)
  const y = Math.cos(lat1r) * Math.sin(lat2r) - Math.sin(lat1r) * Math.cos(lat2r) * Math.cos(dLon)
  return (Math.atan2(x, y) * 180 / Math.PI + 360) % 360
}
