import { useState, useEffect, useRef, useCallback, lazy, Suspense } from 'react'
import StatsBar from './components/StatsBar'
import MlatScorecard from './components/MlatScorecard'
import MessageRateChart from './components/MessageRateChart'
import AircraftTable from './components/AircraftTable'
import AircraftDetailPanel from './components/AircraftDetailPanel'
import styles from './App.module.css'

// Page modules are lazy-loaded so heavy dependencies (Leaflet, Three.js,
// Recharts) are only fetched when the user first visits that tab.
const HistoryPage        = lazy(() => import('./pages/HistoryPage'))
const ReceiverPage       = lazy(() => import('./pages/ReceiverPage'))
const FleetPage          = lazy(() => import('./pages/FleetPage'))
const CoveragePage       = lazy(() => import('./pages/CoveragePage'))
const MapPage            = lazy(() => import('./pages/MapPage'))
const FlowMapPage        = lazy(() => import('./pages/FlowMapPage'))
const EventsPage         = lazy(() => import('./pages/EventsPage'))
const SightingsPage      = lazy(() => import('./pages/SightingsPage'))
const StatusPage         = lazy(() => import('./pages/StatusPage'))
const SettingsPage       = lazy(() => import('./pages/SettingsPage'))
const SkyView            = lazy(() => import('./pages/SkyView'))
const PositionQualityPage = lazy(() => import('./pages/PositionQualityPage'))

const API_BASE = import.meta.env.PROD ? '' : 'http://localhost:8000'

const WS_URL = import.meta.env.PROD
  ? `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/ws`
  : 'ws://localhost:8000/ws'

export default function App() {
  const [snapshot, setSnapshot] = useState(null)
  const [connected, setConnected] = useState(false)
  const [tab, setTab] = useState('live')
  const [selectedIcao, setSelectedIcao] = useState(null)
  const [selectedVisitTs, setSelectedVisitTs] = useState(null)
  const [coverageIcao, setCoverageIcao] = useState('')
  const [notableRefreshKey, setNotableRefreshKey] = useState(0)
  const [receiverPos, setReceiverPos] = useState(null)
  const [debugMode, setDebugMode] = useState(false)
  const wsRef = useRef(null)
  const retryRef = useRef(null)
  const closingRef = useRef(false)

  function handleOpenCoverage(icao) {
    setCoverageIcao(icao)
    setTab('coverage')
    setSelectedIcao(null)
    setSelectedVisitTs(null)
  }

  // Accept either a plain icao string or { icao, visitTs } from pages that
  // want to pre-select a specific visit in the detail panel.
  function handleSelectIcao(value) {
    if (value && typeof value === 'object') {
      setSelectedIcao(value.icao)
      setSelectedVisitTs(value.visitTs ?? null)
    } else {
      setSelectedIcao(value)
      setSelectedVisitTs(null)
    }
  }

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
      // Skip reconnect if this close was triggered by our own cleanup
      if (!closingRef.current && wsRef.current === ws) {
        retryRef.current = setTimeout(connect, 3000)
      }
    }
    ws.onerror = () => ws.close()
  }, [])

  useEffect(() => {
    closingRef.current = false
    connect()
    return () => {
      closingRef.current = true
      clearTimeout(retryRef.current)
      wsRef.current?.close()
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
        if (d?.config?.debug) setDebugMode(true)
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
          {debugMode && (
            <button
              className={tab === 'positionqa' ? styles.tabActive : styles.tab}
              onClick={() => setTab('positionqa')}
            >Position QA</button>
          )}
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
              <AircraftTable aircraft={snapshot.aircraft} onSelectIcao={handleSelectIcao} queueSize={snapshot.hexdb_queue_size ?? 0} />
            </>
          ) : (
            <div className={styles.waiting}>
              {connected ? 'Waiting for data…' : 'Connecting to backend…'}
            </div>
          )}
        </main>
      )}

      <Suspense fallback={null}>
        {tab === 'map' && <MapPage snapshot={snapshot} onSelectIcao={handleSelectIcao} selectedIcao={selectedIcao} receiverPos={receiverPos} />}
        {tab === 'history' && <HistoryPage snapshot={snapshot} />}
        {tab === 'sightings' && <SightingsPage onSelectIcao={handleSelectIcao} notableRefreshKey={notableRefreshKey} />}
        {tab === 'receiver' && <ReceiverPage snapshot={snapshot} />}
        {tab === 'coverage' && <CoveragePage aircraft={snapshot?.aircraft ?? []} initialIcao={coverageIcao} />}
        {tab === 'flow' && <FlowMapPage />}
        {tab === 'fleet' && <FleetPage onSelectIcao={handleSelectIcao} />}
        {tab === 'events' && <EventsPage onSelectIcao={handleSelectIcao} />}
        {tab === 'sky' && <SkyView snapshot={snapshot} onSelectIcao={handleSelectIcao} />}
        {tab === 'positionqa' && debugMode && <PositionQualityPage />}
        {tab === 'status' && <StatusPage />}
        {tab === 'settings' && <SettingsPage />}
      </Suspense>

      {selectedIcao && (
        <AircraftDetailPanel
          icao={selectedIcao}
          initialVisitTs={selectedVisitTs}
          snapshot={snapshot}
          onClose={() => { setSelectedIcao(null); setSelectedVisitTs(null) }}
          onRefreshed={() => setNotableRefreshKey(k => k + 1)}
          onOpenCoverage={handleOpenCoverage}
        />
      )}
    </div>
  )
}
