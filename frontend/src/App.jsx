import { useState, useEffect, useRef, useCallback } from 'react'
import StatsBar from './components/StatsBar'
import MlatScorecard from './components/MlatScorecard'
import MessageRateChart from './components/MessageRateChart'
import AircraftTable from './components/AircraftTable'
import AircraftDetailPanel from './components/AircraftDetailPanel'
import HistoryPage from './pages/HistoryPage'
import ReceiverPage from './pages/ReceiverPage'
import FleetPage from './pages/FleetPage'
import CoveragePage from './pages/CoveragePage'
import MapPage from './pages/MapPage'
import FlowMapPage from './pages/FlowMapPage'
import EventsPage from './pages/EventsPage'
import SightingsPage from './pages/SightingsPage'
import StatusPage from './pages/StatusPage'
import SettingsPage from './pages/SettingsPage'
import SkyView from './pages/SkyView'
import PositionQualityPage from './pages/PositionQualityPage'
import styles from './App.module.css'

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws`
  : 'ws://localhost:8000/ws'

export default function App() {
  const [snapshot, setSnapshot] = useState(null)
  const [connected, setConnected] = useState(false)
  const [tab, setTab] = useState('live')
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [notableRefreshKey, setNotableRefreshKey] = useState(0)
  const [receiverPos, setReceiverPos] = useState(null)
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
      try {
        setSnapshot(JSON.parse(e.data))
      } catch {
        // ignore malformed frames
      }
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

  // Fetch receiver position once at app start so MapPage can set its initial
  // view synchronously rather than re-zooming after an async status fetch
  useEffect(() => {
    fetch(`${API_BASE}/api/status`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        const lat = d?.config?.receiver_lat
        const lon = d?.config?.receiver_lon
        if (lat != null && lon != null) setReceiverPos([lat, lon])
      })
      .catch(() => {})
  }, [])

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
            className={tab === 'sky' ? styles.tabActive : styles.tab}
            onClick={() => setTab('sky')}
          >Sky</button>
          <button
            className={tab === 'map' ? styles.tabActive : styles.tab}
            onClick={() => setTab('map')}
          >Map</button>
          <button
            className={tab === 'history' ? styles.tabActive : styles.tab}
            onClick={() => setTab('history')}
          >History</button>
          <button
            className={tab === 'receiver' ? styles.tabActive : styles.tab}
            onClick={() => setTab('receiver')}
          >Receiver</button>
          <button
            className={tab === 'coverage' ? styles.tabActive : styles.tab}
            onClick={() => setTab('coverage')}
          >Coverage</button>
          <button
            className={tab === 'flow' ? styles.tabActive : styles.tab}
            onClick={() => setTab('flow')}
          >Flow</button>
          <button
            className={tab === 'fleet' ? styles.tabActive : styles.tab}
            onClick={() => setTab('fleet')}
          >Fleet</button>
          <button
            className={tab === 'sightings' ? styles.tabActive : styles.tab}
            onClick={() => setTab('sightings')}
          >Sightings</button>
          <button
            className={tab === 'events' ? styles.tabActive : styles.tab}
            onClick={() => setTab('events')}
          >Events</button>
          <button
            className={tab === 'positionqa' ? styles.tabActive : styles.tab}
            onClick={() => setTab('positionqa')}
          >Position QA</button>
          <button
            className={tab === 'status' ? styles.tabActive : styles.tab}
            onClick={() => setTab('status')}
          >Status</button>
          <button
            className={tab === 'settings' ? styles.tabActive : styles.tab}
            onClick={() => setTab('settings')}
          >Settings</button>
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
              {(snapshot.mlat_total ?? 0) > 0 && (
                <MlatScorecard aircraft={snapshot.aircraft} />
              )}
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

      {tab === 'map' && <MapPage snapshot={snapshot} onSelectIcao={setSelectedIcao} selectedIcao={selectedIcao} receiverPos={receiverPos} />}
      {tab === 'history' && <HistoryPage snapshot={snapshot} />}
      {tab === 'sightings' && <SightingsPage onSelectIcao={setSelectedIcao} notableRefreshKey={notableRefreshKey} />}
      {tab === 'receiver' && <ReceiverPage snapshot={snapshot} />}
      {tab === 'coverage' && <CoveragePage aircraft={snapshot.aircraft ?? []} />}
      {tab === 'flow' && <FlowMapPage />}
      {tab === 'fleet' && <FleetPage onSelectIcao={setSelectedIcao} />}
      {tab === 'events' && <EventsPage onSelectIcao={setSelectedIcao} />}
      {tab === 'sky' && <SkyView snapshot={snapshot} onSelectIcao={setSelectedIcao} />}
      {tab === 'positionqa' && <PositionQualityPage />}
      {tab === 'status' && <StatusPage />}
      {tab === 'settings' && <SettingsPage />}

      {selectedIcao && (
        <AircraftDetailPanel
          icao={selectedIcao}
          snapshot={snapshot}
          onClose={() => setSelectedIcao(null)}
          onRefreshed={() => setNotableRefreshKey(k => k + 1)}
        />
      )}
    </div>
  )
}
