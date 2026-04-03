import { useState, useEffect, useCallback, useRef } from 'react'

/**
 * useFetchState — fetch hook with explicit status machine.
 *
 * Returns { status, data, retry } where status is one of:
 *   'idle'    — not yet fetched (url is null/undefined)
 *   'loading' — fetch in progress
 *   'empty'   — fetch succeeded but returned no data (empty array / null body)
 *   'error'   — fetch failed (network error or non-ok response)
 *   'ok'      — fetch succeeded and data is non-empty
 *
 * @param {string|null} url - Fetch URL. Set to null/undefined to stay idle.
 * @param {object} opts
 * @param {function} [opts.isEmpty] - Called with parsed JSON; return true if
 *   data should be treated as empty. Default: checks for null, empty array/object.
 */
export function useFetchState(url, { isEmpty } = {}) {
  const [status, setStatus] = useState(url ? 'loading' : 'idle')
  const [data,   setData]   = useState(null)
  const retryCountRef = useRef(0)

  const doFetch = useCallback(() => {
    if (!url) { setStatus('idle'); setData(null); return }
    setStatus('loading')
    fetch(url)
      .then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(d => {
        const empty = isEmpty
          ? isEmpty(d)
          : d == null || (Array.isArray(d) && d.length === 0)
            || (typeof d === 'object' && Object.keys(d).length === 0)
        setData(d)
        setStatus(empty ? 'empty' : 'ok')
      })
      .catch(() => setStatus('error'))
  }, [url, isEmpty])

  useEffect(() => {
    retryCountRef.current = 0
    doFetch()
  }, [doFetch])

  return { status, data, retry: doFetch }
}
