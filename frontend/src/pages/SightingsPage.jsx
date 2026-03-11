import NotableSightings from '../components/NotableSightings'
import styles from './HistoryPage.module.css'

export default function SightingsPage({ onSelectIcao, notableRefreshKey }) {
  return (
    <main className={styles.main}>
      <NotableSightings onSelectIcao={onSelectIcao} refreshKey={notableRefreshKey} />
    </main>
  )
}
