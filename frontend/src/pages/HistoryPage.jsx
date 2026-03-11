import HourlyHeatmap from '../components/HourlyHeatmap'
import CalendarHeatmap from '../components/CalendarHeatmap'
import TrendChart from '../components/TrendChart'
import AltHeatmap from '../components/AltHeatmap'
import styles from './HistoryPage.module.css'

export default function HistoryPage({ snapshot }) {
  return (
    <main className={styles.main}>
      <HourlyHeatmap />
      <AltHeatmap />
      <CalendarHeatmap />
      <TrendChart />
    </main>
  )
}
