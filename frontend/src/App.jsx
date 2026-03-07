import { useState, useEffect, useRef, useCallback } from 'react'
import StatsBar from './components/StatsBar'
import MessageRateChart from './components/MessageRateChart'
import AircraftTable from './components/AircraftTable'
import AircraftDetailPanel from './components/AircraftDetailPanel'
import HistoryPage from './pages/HistoryPage'
import ReceiverPage from './pages/ReceiverPage'
import FleetPage from './pages/FleetPage'
import EventsPage from './pages/EventsPage'
import styles from './App.module.css'

const WS_URL = import.meta.env.PROD
  ? `ws://${window.location.host}/ws`
  : 'ws://localhost:8000/ws'

export default function App() {
  const [snapshot, setSnapshot] = useState(null)
  const [connected, setConnected] = useState(false)
  const [tab, setTab] = useState('live')
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [notableRefreshKey, setNotableRefreshKey] = useState(0)
  const wsRef = useRef(null)
  const retryRef = useRef(null)

  const connect = useCallback(() => {
    const ws = new WebSocket(WS_URL)
    wsRef.current = ws

    ws.onopen = () => {
      setConnected(true)
      clearTimeout(retryRef.current)
    }
    ws.onmessage = (e) => {
      setSnapshot(JSON.parse(e.data))
    }
    ws.onclose = () => {
      setConnected(false)
      retryRef.current = setTimeout(connect, 3000)
    }
    ws.onerror = () => ws.close()
  }, [])

  useEffect(() => {
    connect()
    return () => {
      wsRef.current?.close()
      clearTimeout(retryRef.current)
    }
  }, [connect])

  return (
    <div className={styles.layout}>
      <header className={styles.header}>
        <div className={styles.title}>
          <span className={styles.titleIcon}>📡</span>
          ADS-B Dashboard
        </div>
        <nav className={styles.tabs}>
          <button
            className={tab === 'live' ? styles.tabActive : styles.tab}
            onClick={() => setTab('live')}
          >Live</button>
          <button
            className={tab === 'history' ? styles.tabActive : styles.tab}
            onClick={() => setTab('history')}
          >History</button>
          <button
            className={tab === 'receiver' ? styles.tabActive : styles.tab}
            onClick={() => setTab('receiver')}
          >Receiver</button>
          <button
            className={tab === 'fleet' ? styles.tabActive : styles.tab}
            onClick={() => setTab('fleet')}
          >Fleet</button>
          <button
            className={tab === 'events' ? styles.tabActive : styles.tab}
            onClick={() => setTab('events')}
          >Events</button>
        </nav>
        <span className={connected ? styles.live : styles.offline}>
          {connected ? '● Live' : '○ Reconnecting…'}
        </span>
      </header>

      {tab === 'live' && (
        <main className={styles.main}>
          {snapshot ? (
            <>
              <StatsBar snapshot={snapshot} />
              <MessageRateChart data={snapshot.rate_history} />
              <AircraftTable aircraft={snapshot.aircraft} onSelectIcao={setSelectedIcao} queueSize={snapshot.hexdb_queue_size ?? 0} />
            </>
          ) : (
            <div className={styles.waiting}>
              {connected ? 'Waiting for data…' : 'Connecting to backend…'}
            </div>
          )}
        </main>
      )}

      {tab === 'history' && <HistoryPage onSelectIcao={setSelectedIcao} snapshot={snapshot} notableRefreshKey={notableRefreshKey} />}
      {tab === 'receiver' && <ReceiverPage snapshot={snapshot} />}
      {tab === 'fleet' && <FleetPage onSelectIcao={setSelectedIcao} />}
      {tab === 'events' && <EventsPage onSelectIcao={setSelectedIcao} />}

      {selectedIcao && (
        <AircraftDetailPanel
          icao={selectedIcao}
          onClose={() => setSelectedIcao(null)}
          onRefreshed={() => setNotableRefreshKey(k => k + 1)}
        />
      )}
    </div>
  )
}
