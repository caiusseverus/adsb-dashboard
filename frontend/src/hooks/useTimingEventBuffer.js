import { useEffect, useRef, useState } from 'react'

const TIMING_BUFFER_MAX = 60_000

function normalizeTimingEvent(ev) {
  const [
    seq,
    arrival_us,
    df,
    msg_len,
    signal_dbfs,
    source_class,
    icao,
    bearing_deg,
    range_nm,
    iid,
  ] = ev

  const numericBearing = Number(bearing_deg)
  const numericRange = Number(range_nm)
  const numericIid = Number(iid)

  return {
    seq,
    arrival_us,
    df,
    msg_len,
    signal_dbfs,
    source_class,
    icao: `${icao ?? ''}`.toUpperCase(),
    bearing_deg: Number.isFinite(numericBearing) ? numericBearing : null,
    range_nm: Number.isFinite(numericRange) ? numericRange : null,
    iid: Number.isInteger(numericIid) ? numericIid : null,
  }
}

export function useTimingEventBuffer(timingPacket, windowUs, maxEvents = TIMING_BUFFER_MAX) {
  const [view, setView] = useState({ nowUs: 0, events: [] })
  const bufRef = useRef([])

  useEffect(() => {
    if (!timingPacket) return
    const nowUs = Number(timingPacket.now_us ?? 0)
    const cutoffUs = Math.max(0, nowUs - windowUs - 1_000_000)
    const newEvents = (timingPacket.events ?? []).map(normalizeTimingEvent)
    const mergedMap = new Map()

    for (const ev of bufRef.current) {
      if (ev.arrival_us >= cutoffUs) mergedMap.set(ev.seq, ev)
    }
    for (const ev of newEvents) {
      if (ev.arrival_us >= cutoffUs) mergedMap.set(ev.seq, ev)
    }

    const merged = [...mergedMap.values()].sort((a, b) => a.seq - b.seq)
    if (merged.length > maxEvents) merged.splice(0, merged.length - maxEvents)
    bufRef.current = merged
    setView({ nowUs, events: merged })
  }, [maxEvents, timingPacket, windowUs])

  return view
}
