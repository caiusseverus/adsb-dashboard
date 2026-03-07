import HourlyHeatmap from '../components/HourlyHeatmap'
import CalendarHeatmap from '../components/CalendarHeatmap'
import TrendChart from '../components/TrendChart'
import NotableSightings from '../components/NotableSightings'
import styles from './HistoryPage.module.css'

export default function HistoryPage({ onSelectIcao, snapshot, notableRefreshKey }) {
  return (
    <main className={styles.main}>
      <HourlyHeatmap />
      <TrendChart />
      <CalendarHeatmap />
      <NotableSightings onSelectIcao={onSelectIcao} refreshKey={notableRefreshKey} />
    </main>
  )
}
