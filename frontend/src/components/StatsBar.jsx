import styles from './StatsBar.module.css'

export default function StatsBar({ snapshot }) {
  return (
    <div className={styles.bar}>
      <StatCard label="Live Aircraft"    value={snapshot.aircraft_count}          accent="blue"   />
      <StatCard label="Live Military"    value={snapshot.live_military ?? 0}      accent="purple" />
      <StatCard label="Messages / sec"   value={snapshot.msg_per_sec}             accent="green"  />
      <StatCard label="Aircraft Today"   value={snapshot.unique_today ?? '—'}     accent="blue"   />
      <StatCard label="Military Today"   value={snapshot.unique_today_military ?? '—'} accent="purple" />
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
