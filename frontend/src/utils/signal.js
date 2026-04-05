const MIN_SIGNAL_DBFS = -127.5
const DEFAULT_BUCKET_FLOOR_DBFS = -48

export function clampSignalDbfs(value) {
  if (value == null) return null
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  return Math.max(MIN_SIGNAL_DBFS, Math.min(0, Math.round(numeric * 10) / 10))
}

export function formatSignalDbfs(value) {
  const dbfs = clampSignalDbfs(value)
  return dbfs != null ? `${dbfs.toFixed(1)} dBFS` : '—'
}

export function signalStrengthPercent(value) {
  const dbfs = clampSignalDbfs(value)
  if (dbfs == null) return null
  return Math.max(0, Math.min(100, Math.round(((dbfs - MIN_SIGNAL_DBFS) / -MIN_SIGNAL_DBFS) * 100)))
}

export function signalColour(value) {
  const pct = signalStrengthPercent(value)
  if (pct == null) return '#484f58'
  if (pct > 66) return '#3fb950'
  if (pct > 33) return '#d29922'
  return '#f85149'
}

export function signalBucketIndex(value, bucketCount, floorDbfs = DEFAULT_BUCKET_FLOOR_DBFS) {
  const dbfs = clampSignalDbfs(value)
  if (dbfs == null) return 0
  const weakestDbfs = Math.min(-1, Number(floorDbfs) || DEFAULT_BUCKET_FLOOR_DBFS)
  const magnitude = Math.min(-weakestDbfs, Math.max(0, -dbfs))
  return Math.max(
    0,
    Math.min(bucketCount - 1, Math.floor((magnitude / -weakestDbfs) * bucketCount)),
  )
}
