import { useState, useEffect, useCallback } from 'react'

/**
 * Minimal fetch hook. Re-fetches whenever `url` changes.
 * Returns { data, loading } where data is the parsed JSON or null on error.
 */
export function useFetch(url) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  useEffect(() => {
    setLoading(true)
    setData(null)
    fetch(url)
      .then(r => r.ok ? r.json() : null)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [url])
  return { data, loading }
}

/**
 * Like useFetch but exposes a `reload` callback for imperative refetch after mutations.
 * `fallback` is the initial data value (default null).
 */
export function useReloadableFetch(url, fallback = null) {
  const [data, setData] = useState(fallback)
  const [loading, setLoading] = useState(true)
  const reload = useCallback(() => {
    setLoading(true)
    fetch(url)
      .then(r => r.ok ? r.json() : fallback)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [url]) // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(reload, [reload])
  return { data, loading, reload }
}
