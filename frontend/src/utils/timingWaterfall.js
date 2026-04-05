// Pure aggregation helpers for the Timing page message waterfall.
export const WATERFALL_DF_BUCKETS = [
  { key: '17', label: '17' },
  { key: '18', label: '18' },
  { key: '11', label: '11' },
  { key: '20', label: '20' },
  { key: '21', label: '21' },
  { key: '4', label: '4' },
  { key: '5', label: '5' },
  { key: '16', label: '16' },
  { key: '0', label: '0' },
  { key: 'other', label: 'oth' },
]

export function timingDfBucket(df) {
  if (df === 17 || df === 18 || df === 11 || df === 20 || df === 21 || df === 4 || df === 5 || df === 16 || df === 0) {
    return `${df}`
  }
  return 'other'
}

export function buildDfWaterfallRows({
  events,
  renderNowUs,
  visibleWindowUs,
  sliceUs,
  buckets = WATERFALL_DF_BUCKETS,
  displayLagSlices = 1,
}) {
  const safeSliceUs = Math.max(1, Number(sliceUs) || 1)
  const safeWindowUs = Math.max(safeSliceUs, Number(visibleWindowUs) || safeSliceUs)
  const rowCount = Math.max(1, Math.floor(safeWindowUs / safeSliceUs))
  const currentSliceIndex = Math.floor(Math.max(0, Number(renderNowUs) || 0) / safeSliceUs)
  const newestVisibleSliceIndex = currentSliceIndex - 1 - Math.max(0, displayLagSlices)

  if (newestVisibleSliceIndex < 0) {
    return {
      rows: [],
      rowCount,
      newestVisibleSliceIndex,
      oldestVisibleSliceIndex: newestVisibleSliceIndex - rowCount + 1,
    }
  }

  const oldestVisibleSliceIndex = newestVisibleSliceIndex - rowCount + 1
  const bucketIndexByKey = new Map(buckets.map((bucket, index) => [bucket.key, index]))
  const countsBySlice = new Map()

  for (const ev of events ?? []) {
    const arrivalUs = Number(ev?.arrival_us ?? 0)
    if (!Number.isFinite(arrivalUs) || arrivalUs > renderNowUs) continue
    const sliceIndex = Math.floor(arrivalUs / safeSliceUs)
    if (sliceIndex < oldestVisibleSliceIndex || sliceIndex > newestVisibleSliceIndex) continue
    const bucketIndex = bucketIndexByKey.get(timingDfBucket(Number(ev?.df ?? 0))) ?? (buckets.length - 1)
    let counts = countsBySlice.get(sliceIndex)
    if (!counts) {
      counts = new Array(buckets.length).fill(0)
      countsBySlice.set(sliceIndex, counts)
    }
    counts[bucketIndex] += 1
  }

  const rows = []
  for (let sliceIndex = newestVisibleSliceIndex; sliceIndex >= oldestVisibleSliceIndex; sliceIndex -= 1) {
    rows.push({
      sliceIndex,
      counts: countsBySlice.get(sliceIndex) ?? new Array(buckets.length).fill(0),
    })
  }

  return {
    rows,
    rowCount,
    newestVisibleSliceIndex,
    oldestVisibleSliceIndex,
  }
}
