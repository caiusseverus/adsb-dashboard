import styles from './LoadingState.module.css'

/**
 * LoadingState — small status panel for async overlays.
 *
 * Props:
 *   status   — 'idle' | 'loading' | 'empty' | 'error' | 'ok'
 *   onRetry  — optional retry callback (shown on error)
 *   emptyMsg — optional message for the empty state
 */
export function LoadingState({ status, onRetry, emptyMsg = 'No data available' }) {
  if (status === 'loading') {
    return (
      <div className={styles.wrap}>
        <span className={styles.spinner} />
        <span className={styles.msg}>Loading…</span>
      </div>
    )
  }
  if (status === 'empty') {
    return (
      <div className={styles.wrap}>
        <span className={styles.msg} style={{ color: '#484f58' }}>{emptyMsg}</span>
      </div>
    )
  }
  if (status === 'error') {
    return (
      <div className={styles.wrap}>
        <span className={styles.msg} style={{ color: '#f85149' }}>Failed to load</span>
        {onRetry && (
          <button className={styles.retryBtn} onClick={onRetry}>Retry</button>
        )}
      </div>
    )
  }
  return null
}
