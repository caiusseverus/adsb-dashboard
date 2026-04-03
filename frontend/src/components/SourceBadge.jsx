import styles from './SourceBadge.module.css'

/**
 * Determines the position source type for an aircraft.
 * Returns 'MLAT' | 'ADS-B' | 'EST'
 */
export function posSourceType(ac) {
  if (!ac) return null
  if (ac.mlat) return 'MLAT'
  if (ac.pos_confident) return 'ADS-B'
  return 'EST'
}

/**
 * Badge showing how an aircraft's position was determined.
 * type: 'ADS-B' | 'MLAT' | 'EST'
 */
export function SourceBadge({ type, className }) {
  if (!type) return null
  const key = type === 'ADS-B' ? 'adsb' : type.toLowerCase()
  return (
    <span className={`${styles.badge} ${styles[key]} ${className ?? ''}`}>
      {type}
    </span>
  )
}
