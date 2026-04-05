import { useCallback, useEffect, useRef, useState } from 'react'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const TIMING_WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/timing`
  : 'ws://localhost:8000/ws/timing'

const TIMING_POLL_FALLBACK_MS = 1000

export function useTimingEventStream() {
  const [packet, setPacket] = useState(null)
  const retryRef = useRef(null)
  const pollRef = useRef(null)
  const lastEventWallRef = useRef(0)
  const sinceSeqRef = useRef(0)
  const activeRef = useRef(true)

  useEffect(() => {
    const onVisibility = () => { activeRef.current = !document.hidden }
    document.addEventListener('visibilitychange', onVisibility)
    return () => document.removeEventListener('visibilitychange', onVisibility)
  }, [])

  const ingestPacket = useCallback((nextPacket) => {
    const events = nextPacket?.events ?? []
    if (events.length) {
      const lastSeq = Number(events[events.length - 1]?.[0] ?? 0)
      sinceSeqRef.current = Math.max(sinceSeqRef.current, lastSeq)
      lastEventWallRef.current = performance.now()
    }
    setPacket(nextPacket)
  }, [])

  useEffect(() => {
    let ws
    let closed = false

    const pollOnce = () => {
      if (closed || !activeRef.current) return
      fetch(`${API_BASE}/api/timing/events?since_seq=${sinceSeqRef.current}`)
        .then(r => r.ok ? r.json() : null)
        .then(payload => { if (payload) ingestPacket(payload) })
        .catch(() => {})
    }

    const connect = () => {
      if (closed) return
      ws = new WebSocket(TIMING_WS_URL)

      ws.onmessage = event => {
        try {
          ingestPacket(JSON.parse(event.data))
        } catch {
          // ignore malformed frames
        }
      }

      ws.onclose = () => {
        if (closed) return
        retryRef.current = setTimeout(connect, 1000)
      }

      ws.onerror = () => ws.close()
    }

    pollOnce()
    pollRef.current = setInterval(() => {
      if (closed) return
      if (performance.now() - lastEventWallRef.current > 1500) {
        pollOnce()
      }
    }, TIMING_POLL_FALLBACK_MS)

    connect()
    return () => {
      closed = true
      clearTimeout(retryRef.current)
      clearInterval(pollRef.current)
      ws?.close()
    }
  }, [ingestPacket])

  return packet
}
