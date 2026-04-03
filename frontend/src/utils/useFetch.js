import { useState, useEffect, useCallback, useRef } from 'react'

/**
 * Minimal fetch hook. Re-fetches whenever `url` changes.
 * Returns { data, loading } where data is the parsed JSON or null on error.
 * Cancels any in-flight request when `url` changes or the component unmounts.
 */
export function useFetch(url) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    const controller = new AbortController()
    setLoading(true)
    setData(null)
    fetch(url, { signal: controller.signal })
      .then(r => r.ok ? r.json() : null)
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { if (e.name !== 'AbortError') setLoading(false) })
    return () => controller.abort()
  }, [url])
  return { data, loading }
}

/**
 * Like useFetch but exposes a `reload` callback for imperative refetch after mutations.
 * `fallback` is the initial data value (default null).
 * Cancels any in-flight request on each reload or unmount.
 */
export function useReloadableFetch(url, fallback = null) {
  const [data, setData] = useState(fallback)
  const [loading, setLoading] = useState(true)
  const controllerRef = useRef(null)
  const reload = useCallback(() => {
    controllerRef.current?.abort()
    const controller = new AbortController()
    controllerRef.current = controller
    setLoading(true)
    fetch(url, { signal: controller.signal })
      .then(r => r.ok ? r.json() : fallback)
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { if (e.name !== 'AbortError') setLoading(false) })
  }, [url]) // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => {
    reload()
    return () => controllerRef.current?.abort()
  }, [reload])
  return { data, loading, reload }
}
