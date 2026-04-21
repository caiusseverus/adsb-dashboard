import { useCallback, useEffect, useRef, useState } from 'react'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'
const TIMING_WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws/timing`
  : 'ws://localhost:8000/ws/timing'

const TIMING_POLL_FALLBACK_MS = 1000

export function useTimingEventStream(options = {}) {
  const { enabled = true, iid = null, df11Only = false, debugLabel = null } = options
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
    if (!enabled) {
      setPacket(null)
      return
    }
    sinceSeqRef.current = 0
    let ws
    let closed = false
    const params = new URLSearchParams()
    if (iid != null) params.set('iid', String(iid))
    if (df11Only) params.set('df11_only', '1')
    const query = params.toString()
    const wsUrl = query ? `${TIMING_WS_URL}?${query}` : TIMING_WS_URL
    const apiUrl = `${API_BASE}/api/timing/events${query ? `?${query}&` : '?'}since_seq=`

    const pollOnce = () => {
      if (closed || !activeRef.current) return
      fetch(`${apiUrl}${sinceSeqRef.current}`)
        .then(r => r.ok ? r.json() : null)
        .then(payload => { if (payload) ingestPacket(payload) })
        .catch(() => {})
    }

    const connect = () => {
      if (closed) return
      ws = new WebSocket(wsUrl)
      if (typeof window !== 'undefined' && debugLabel) {
        const store = window.__RADAR_PAGE_REQUEST_METRICS__
        if (store) {
          const key = `timing_ws:${debugLabel}`
          const bucket = store.streams?.[key] ?? { opens: 0, closes: 0, messages: 0, heartbeats: 0, lastEvent: null, lastMeta: null }
          bucket.opens += 1
          bucket.lastEvent = 'open'
          bucket.lastMeta = { iid, df11Only }
          store.streams[key] = bucket
        }
      }

      ws.onmessage = event => {
        try {
          if (typeof window !== 'undefined' && debugLabel) {
            const store = window.__RADAR_PAGE_REQUEST_METRICS__
            if (store) {
              const key = `timing_ws:${debugLabel}`
              const bucket = store.streams?.[key] ?? { opens: 0, closes: 0, messages: 0, heartbeats: 0, lastEvent: null, lastMeta: null }
              bucket.messages += 1
              bucket.lastEvent = 'message'
              store.streams[key] = bucket
            }
          }
          ingestPacket(JSON.parse(event.data))
        } catch {
          // ignore malformed frames
        }
      }

      ws.onclose = () => {
        if (typeof window !== 'undefined' && debugLabel) {
          const store = window.__RADAR_PAGE_REQUEST_METRICS__
          if (store) {
            const key = `timing_ws:${debugLabel}`
            const bucket = store.streams?.[key] ?? { opens: 0, closes: 0, messages: 0, heartbeats: 0, lastEvent: null, lastMeta: null }
            bucket.closes += 1
            bucket.lastEvent = 'close'
            store.streams[key] = bucket
          }
        }
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
  }, [df11Only, enabled, iid, ingestPacket])

  return packet
}
