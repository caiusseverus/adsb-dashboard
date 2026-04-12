export const MESSAGE_FIELD_RENDER_HOLDBACK_US = 650_000
export const MESSAGE_FIELD_SIGNAL_MIN_DBFS = -45

const TRAFFIC_FILTER_DF_MAP = {
  df11: [11],
  df_surveillance: [11, 4, 5],
  adsb: [17],
  tisb: [18],
  commb: [20, 21],
  surveillance: [4, 5],
  acas: [0, 16],
}
const KNOWN_FILTER_DFS = new Set(Object.values(TRAFFIC_FILTER_DF_MAP).flat())

function matchesTrafficFilter(df, trafficFilter) {
  if (trafficFilter === 'all') return true
  if (trafficFilter === 'other') {
    return !KNOWN_FILTER_DFS.has(df)
  }
  const allowed = TRAFFIC_FILTER_DF_MAP[trafficFilter]
  return Array.isArray(allowed) ? allowed.includes(df) : true
}

function lowerBoundArrival(events, targetArrivalUs) {
  let low = 0
  let high = events.length
  while (low < high) {
    const mid = (low + high) >> 1
    if ((events[mid]?.arrival_us ?? 0) < targetArrivalUs) low = mid + 1
    else high = mid
  }
  return low
}

export function selectMessageFieldEvents({
  events,
  renderNowUs,
  persistenceUs,
  trafficFilter = 'all',
  iidFilter = 'all',
}) {
  const source = events ?? []
  if (!source.length) return []

  const cutoffUs = Math.max(0, renderNowUs - persistenceUs)
  const startIndex = lowerBoundArrival(source, cutoffUs)
  const selected = []

  for (let index = startIndex; index < source.length; index += 1) {
    const ev = source[index]
    if (ev.arrival_us < cutoffUs || ev.arrival_us > renderNowUs) continue
    if (!matchesTrafficFilter(ev.df, trafficFilter)) continue
    if (iidFilter === 'exclude-zero' && ev.iid === 0) continue
    if (iidFilter !== 'all' && iidFilter !== 'exclude-zero' && ev.iid !== iidFilter) continue
    selected.push(ev)
  }

  return selected
}

export function buildActiveIids(events) {
  const values = new Set()
  for (const ev of events ?? []) {
    if (Number.isInteger(ev.iid)) values.add(ev.iid)
  }
  return [...values].sort((a, b) => a - b)
}

function clampMessageFieldSignal(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return null
  return Math.max(MESSAGE_FIELD_SIGNAL_MIN_DBFS, Math.min(0, numeric))
}

const IID_COLOURS = ['#58a6ff', '#3fb950', '#d29922', '#f78166', '#bc8cff', '#39c5bb', '#ff7b72', '#a5d6ff']
const DF_COLOURS = {
  17: '#388bfd',
  18: '#57a6ff',
  11: '#3fb950',
  4: '#d29922',
  5: '#e3b341',
  20: '#bc8cff',
  21: '#d2a8ff',
  0: '#f85149',
  16: '#ff6b6b',
  24: '#6e7681',
}
const SOURCE_COLOURS = {
  6: '#58a6ff',
  5: '#3fb950',
  4: '#d29922',
  3: '#f78166',
  0: '#6e7681',
}

export function messageFieldPointColour(ev, colourMode) {
  if (colourMode === 'df') {
    return DF_COLOURS[ev.df] ?? '#6e7681'
  }
  if (colourMode === 'source') {
    return SOURCE_COLOURS[ev.source_class] ?? SOURCE_COLOURS[0]
  }
  if (colourMode === 'iid') {
    if (!Number.isInteger(ev.iid)) return '#6e7681'
    return IID_COLOURS[Math.abs(ev.iid) % IID_COLOURS.length]
  }
  const signalDbfs = clampMessageFieldSignal(ev.signal_dbfs)
  if (signalDbfs == null) return '#6e7681'
  const normalized = (signalDbfs - MESSAGE_FIELD_SIGNAL_MIN_DBFS) / Math.abs(MESSAGE_FIELD_SIGNAL_MIN_DBFS)
  const hue = 8 + normalized * 120
  const lightness = 48 + normalized * 16
  return `hsl(${hue}, 85%, ${lightness}%)`
}
