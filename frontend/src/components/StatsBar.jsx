import styles from './StatsBar.module.css'

export default function StatsBar({ snapshot }) {
  const mlatEnabled = (snapshot.mlat_total ?? 0) > 0
  return (
    <div className={styles.bar}>
      <StatCard label="Live Aircraft"    value={snapshot.aircraft_count}          accent="blue"   />
      <StatCard label="Live Military"    value={snapshot.live_military ?? 0}      accent="purple" />
      <StatCard label="Messages / sec"   value={snapshot.msg_per_sec}             accent="green"  />
      {mlatEnabled && (
        <StatCard label="MLAT Aircraft"  value={snapshot.mlat_aircraft_count ?? 0} accent="blue" />
      )}
      {mlatEnabled && (
        <StatCard label="MLAT / sec"     value={snapshot.mlat_per_sec ?? 0}       accent="green" />
      )}
      <StatCard label="Aircraft Today"   value={snapshot.unique_today ?? '—'}     accent="blue"   />
      <StatCard label="Military Today"   value={snapshot.unique_today_military ?? '—'} accent="purple" />
      <StatCard label="Total Messages"   value={(snapshot.total_messages ?? 0).toLocaleString()} accent="green"  />
    </div>
  )
}

function StatCard({ label, value, accent }) {
  return (
    <div className={`${styles.card} ${styles[accent]}`}>
      <div className={styles.value}>{value}</div>
      <div className={styles.label}>{label}</div>
    </div>
  )
}
