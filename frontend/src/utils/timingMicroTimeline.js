export const MICRO_TIMELINE_ROW_LIMIT = 12
export const MICRO_TIMELINE_REFRESH_US = 1_500_000
export const MICRO_TIMELINE_GRACE_US = 4_000_000
export const MICRO_TIMELINE_MAX_GRACE_ROWS = 3

export function getMicroTimelineRankingWindowUs(visibleWindowUs) {
  return Math.max(8_000_000, visibleWindowUs * 2)
}

function compareAircraft(a, b, lastSeenByIcao) {
  if (b.count !== a.count) return b.count - a.count
  const bLastSeen = lastSeenByIcao.get(b.icao) ?? 0
  const aLastSeen = lastSeenByIcao.get(a.icao) ?? 0
  if (bLastSeen !== aLastSeen) return bLastSeen - aLastSeen
  return a.icao.localeCompare(b.icao)
}

function dominantSourceClass(sourceCounts) {
  let winner = 0
  let best = -1
  for (const [sourceClass, count] of sourceCounts.entries()) {
    if (count > best || (count === best && sourceClass > winner)) {
      winner = sourceClass
      best = count
    }
  }
  return winner
}

export function buildAirspaceMicroTimelineRows({
  events,
  renderNowUs,
  visibleWindowUs,
  previousState = null,
  rowLimit = MICRO_TIMELINE_ROW_LIMIT,
  refreshUs = MICRO_TIMELINE_REFRESH_US,
  rankingWindowUs = getMicroTimelineRankingWindowUs(visibleWindowUs),
  membershipGraceUs = MICRO_TIMELINE_GRACE_US,
  maxGraceRows = MICRO_TIMELINE_MAX_GRACE_ROWS,
}) {
  const visibleCutoffUs = Math.max(0, renderNowUs - visibleWindowUs)
  const rankingCutoffUs = Math.max(0, renderNowUs - rankingWindowUs)
  const visibleByIcao = new Map()
  const rankingCounts = new Map()
  const lastSeenByIcao = new Map()
  const sourceCountsByIcao = new Map()

  for (const ev of events ?? []) {
    const arrivalUs = Number(ev?.arrival_us ?? 0)
    if (!Number.isFinite(arrivalUs) || arrivalUs > renderNowUs) continue
    const icao = `${ev?.icao ?? ''}`.trim().toUpperCase()
    if (!icao) continue
    if (arrivalUs >= rankingCutoffUs) {
      rankingCounts.set(icao, (rankingCounts.get(icao) ?? 0) + 1)
      lastSeenByIcao.set(icao, Math.max(lastSeenByIcao.get(icao) ?? 0, arrivalUs))
    }
    if (arrivalUs < visibleCutoffUs) continue
    let row = visibleByIcao.get(icao)
    if (!row) {
      row = { icao, events: [], visibleCount: 0, rankingCount: 0, lastSeenUs: arrivalUs, dominantSourceClass: 0 }
      visibleByIcao.set(icao, row)
    }
    row.events.push(ev)
    row.visibleCount += 1
    row.lastSeenUs = arrivalUs
    let sourceCounts = sourceCountsByIcao.get(icao)
    if (!sourceCounts) {
      sourceCounts = new Map()
      sourceCountsByIcao.set(icao, sourceCounts)
    }
    const sourceClass = Number.isFinite(Number(ev?.source_class)) ? Number(ev.source_class) : 0
    sourceCounts.set(sourceClass, (sourceCounts.get(sourceClass) ?? 0) + 1)
  }

  const rankedIds = [...rankingCounts.entries()]
    .map(([icao, count]) => ({ icao, count }))
    .sort((a, b) => compareAircraft(a, b, lastSeenByIcao))
    .map(entry => entry.icao)

  const previousRows = previousState?.rows ?? []
  const previousGraceByIcao = previousState?.graceByIcao ?? {}
  let rows = previousRows
  let graceByIcao = previousGraceByIcao
  let lastRefreshUs = previousState?.lastRefreshUs ?? 0

  if (!previousState || renderNowUs - lastRefreshUs >= refreshUs) {
    const desiredIds = rankedIds.slice(0, rowLimit)
    const desiredSet = new Set(desiredIds)
    const stickyRows = []
    const graceRows = []
    const nextGraceByIcao = {}

    for (const icao of previousRows) {
      if (desiredSet.has(icao)) {
        stickyRows.push(icao)
        continue
      }
      const lastSeenUs = lastSeenByIcao.get(icao) ?? 0
      if (lastSeenUs < visibleCutoffUs) continue
      const keepUntilUs = Math.max(previousGraceByIcao[icao] ?? 0, renderNowUs + membershipGraceUs)
      if (keepUntilUs <= renderNowUs || graceRows.length >= maxGraceRows) continue
      graceRows.push(icao)
      nextGraceByIcao[icao] = keepUntilUs
    }

    rows = [...stickyRows, ...graceRows]
    for (const icao of desiredIds) {
      if (rows.includes(icao)) continue
      rows.push(icao)
      if (rows.length >= rowLimit) break
    }
    graceByIcao = nextGraceByIcao
    lastRefreshUs = renderNowUs
  }

  const rowData = rows
    .map(icao => {
      const visible = visibleByIcao.get(icao)
      const sourceCounts = sourceCountsByIcao.get(icao) ?? new Map()
      return {
        icao,
        events: visible?.events ?? [],
        visibleCount: visible?.visibleCount ?? 0,
        rankingCount: rankingCounts.get(icao) ?? 0,
        lastSeenUs: visible?.lastSeenUs ?? (lastSeenByIcao.get(icao) ?? 0),
        dominantSourceClass: dominantSourceClass(sourceCounts),
      }
    })
    .filter(row => row.visibleCount > 0 || row.rankingCount > 0)

  return {
    rows: rowData,
    state: { rows, graceByIcao, lastRefreshUs },
    meta: {
      rankingWindowUs,
      visibleCutoffUs,
      rankingCutoffUs,
      rankedIds,
    },
  }
}
