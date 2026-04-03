# Polish & Features Implementation Summary

**Plan source:** `.docs/development/polish-and-features-proposal.md`  
**Completed:** 2026-04-03  
**Commits:** `19be10a` → `194038e` on `dev`

All 17 items from the plan were implemented. Notes below describe what was actually built, including any divergences from the original spec.

---

## Phase 0 — Shared Infrastructure

### 0A — Shared aircraft filter hook
`frontend/src/hooks/useAircraftFilter.js`

Extracted filter state (military, MLAT, interesting, type_category, type_code) from per-page copies in `AircraftTable`, `MapPage`, `CoveragePage`, and `SkyView` into a single `useAircraftFilter` hook with a companion `aircraftPassesFilter` predicate. All pages now share identical filter behaviour with no duplication.

### 0B — Loading / empty / error state pattern
`frontend/src/utils/useFetchState.js`, `frontend/src/components/LoadingState.jsx`

`useFetchState(url)` returns `{ status, data, retry }` with an explicit `idle | loading | empty | error | ok` state machine. `<LoadingState status={...} onRetry={...} />` renders the appropriate spinner, empty message, or error+retry UI. Items 6 and 12 were implemented before 0B landed and used their own inline states; these utilities are the canonical pattern for new overlays.

---

## Phase 1 — Trivial Fixes

### 1 — Total messages compact notation
`StatsBar.jsx`: replaced `toLocaleString()` with `Intl.NumberFormat(undefined, {notation:'compact', maximumFractionDigits:2})`. Displays `1.2M` instead of `1,234,567`.

### 5 — Sky View dot size reduction
`SkyView.jsx`: dot radius reduced from 4 → 3 px base, with signal-strength scaling (stronger signal → slightly larger dot up to 4 px).

### 8a — Msgs/sec axis fix
`ReceiverPage.jsx`: per-minute message counts from the history endpoint were being displayed raw on the "msg/sec" axis. Now divided by 60 client-side before plotting. Axis label updated to `msg/sec`.

---

## Phase 2 — Data Correctness

### 14 — Position leak fix
`aircraft_state.py`: added `_pos_source` field (`global_cpr | local_cpr | mlat`) to the `Aircraft` dataclass. Applied the 1500 kt speed gate to **global** CPR fixes (previously only local fixes were gated). Added a freshness guard in the coverage sample writer (`main.py`): samples are skipped if `time() - ac.last_pos_ts > POS_FRESH_S`. Per-aircraft position rejection counters exposed via the Status / debug tab.

### 10 — Signal distribution investigation
Investigated the apparent discrepancy between the dashboard's signal distribution and graphs1090. Conclusion: not a bug. `minute_stats.signal_avg` is a per-minute per-aircraft average; graphs1090 plots per-message peak RSSI. The sampling scopes are fundamentally different. No code change needed.

---

## Phase 3 — Quick-Win Functional Improvements

### 8b — Colour-by-age toggle on receiver scatter
`ReceiverPage.jsx`: added a Signal / Age toggle to the range scatter plot. Age mode colours points by how many days ago the sighting occurred (today=green, yesterday=blue, week=amber, month=purple, older=grey). Selection persisted in `localStorage`.

### 9 — Horizon envelope on range/altitude scatter
`ReceiverPage.jsx`: computed a radio horizon reference curve (`alt_ft = (range_nm / 1.23)²`) and rendered it as a `<Line>` series (not `<ReferenceLine>`, which can only draw straight lines). Labelled "radio horizon". Aircraft above the line are expected (visible); below are anomalies.

### 11 — Coverage trail length slider
`CoveragePage.jsx`: reduced default `MAX_TRAIL_PTS` from 600 to 180 (3 min). Added a 1 / 3 / 5 / 10 min dropdown. Selection persisted in `localStorage`.

### 12 — Hi-res trail loading fix
`CoveragePage.jsx`: confirmed the correct endpoint (`/api/coverage/timelapse_hires`). Added a loading state. If the response returns zero trail points for aircraft visible in the live snapshot, the fetch retries once after 5 seconds (root cause: buffer is sparse at startup). Fixed an off-by-one in the backend age filter.

### 13c — Terrain tile base layer (Map page)
`MapPage.jsx`: added a terrain tile style as an alternative base layer in the existing layer-toggle control. Uses the OpenTopoMap tile set. The Coverage page was not touched (it is Three.js, not Leaflet).

---

## Phase 4 — Medium UI Improvements

### 2A — Airport names in detail panel route header
`AircraftDetailPanel.jsx`: the `/api/aircraft/{icao}/route` endpoint already returned full airport data (`Airport`, `City`, `IATA`, `ICAO`). Wired it into the route display: now shows `EGLL (Heathrow) → KJFK (Kennedy)`.

### 2B — Airport names in visit history
`backend/aircraft.py`: the `aircraft_visits` endpoint now concurrently resolves airport names from hexdb.io for all unique origin/dest ICAOs in the visit list via `asyncio.gather`. Results are cached in a module-level dict (500-entry cap) since airport names are stable.  
`AircraftDetailPanel.jsx`: the visit history Route column now renders `EGLL (Heathrow) → KJFK (JFK)` instead of bare ICAO codes. Falls back gracefully when no name is available.

### 3 — Sky View filter alignment
`SkyView.jsx`: added Type Group and Type Code dropdowns using the shared `useAircraftFilter` hook from 0A. Client-side filtering only; no backend change. Filters now match the Coverage page behaviour exactly.

### 7b — Per-segment altitude-colour trail on map
`MapPage.jsx`: when an aircraft is selected, its track is fetched from `/api/aircraft/{icao}/track`, split into two-point `L.polyline` segments, and each segment is coloured by altitude using `altColor(altitude_ft)`. Capped at 200 segments by downsampling older points. All-aircraft gradient trails were evaluated and deferred (too expensive for the full fleet).

---

## Phase 5 — Larger Features

### 7a — Map side drawer
`AircraftDetailPanel.jsx`, `App.jsx`: added a `mode` prop (`'modal' | 'drawer'`). Drawer mode: `position:fixed; right:0; top:0; height:100%; width:380px`; the map container gains `padding-right: 380px` so the map content is not obscured. On marker click the drawer opens and the trail is force-enabled. On narrow viewports the drawer falls back to modal overlay.

### 6 — Sky View history heatmap
`backend/history.py`: new endpoint `GET /api/history/skyview?hours=N` bins `coverage_samples` into 2°az × 1°el cells, returning cell density and average signal. Elevation angle derived as `arctan(alt_ft / (range_nm × 6076))`.  
`SkyView.jsx`: added a Live / History mode toggle on the horizon chart. In History mode the heatmap overlay shows where aircraft have historically been seen, coloured by density or signal strength.

### 4 — Terrain pseudo-3D (Sky View)
`backend/terrain.py`: new endpoint `GET /api/terrain/ranges` computes cumulative-maximum elevation angles for four fixed range bands ([25, 50, 75, 100] nm) in a single radial pass per azimuth. Cached to disk. Because each profile is a cumulative max, hidden-surface removal is implicit — closer terrain always dominates farther terrain.  
`SkyView.jsx`: the horizon chart now draws four filled areas back-to-front. Darkest fill = terrain within 25 nm; lightest = terrain within 100 nm. The ridge outline is retained on the closest (25 nm) layer. The status bar shows `· terrain (25/50/75/100 nm)` when profiles are loaded.

---

## Phase 6 — New Features

### Source / confidence badge system
`frontend/src/components/SourceBadge.jsx`, `SourceBadge.module.css`:  
`<SourceBadge type="ADS-B|MLAT|EST" />` with matching colours (green, blue, grey). `posSourceType(ac)` utility derives the type from `ac.mlat` and `ac.pos_confident`.  
- `AircraftTable.jsx`: EST badge added to ICAO cell for aircraft with no position fix (new visible information; MLAT badge was already present).  
- `AircraftDetailPanel.jsx`: source badge shown inline next to the "Now" live section header (`Now ADS-B`, `Now MLAT`, `Now EST`).

### 15 — DF11 interrogator codes (IID)
`backend/native/decode_api.h`, `decode_api.c`: added `int iid` field to `decode_result_t`; populated from `mm.IID` for DF11 messages. Library rebuilt.  
`decode_cffi.py`: cffi struct and output dict updated.  
`aircraft_state.py`: `_iid_events` deque (maxlen 50k) records `(ts, iid)` pairs; `get_iid_counts(window_s)` reads it lock-free.  
`backend/interrogators.py`: `GET /api/interrogators?window_s=N` returns active IID codes and counts.  
`ReceiverPage.jsx`: `InterrogatorCodes` card with a horizontal bar chart. IID 0 (no specific interrogator) shown in green; IID 1–127 in blue. Window selector: 1 min – 1 hr.

---

## Phase 7 — Advanced Diagnostics

### 17 — Interrogator timing lanes
`aircraft_state.py`: `get_iid_timeline(window_s)` groups `_iid_events` into per-IID timestamp lists.  
`backend/interrogators.py`: `GET /api/interrogators/timeline?window_s=N` endpoint.  
`ReceiverPage.jsx`: `InterrogatorTimeline` canvas panel. One row per active IID; tick marks at individual DF11 arrival times; 1-second auto-refresh. Regular tick spacing reveals the SSR rotation period (~4 s for most ground stations). Window selector: 5 s – 60 s.

### 16 — Message timing scroll plot
`aircraft_state.py`: `_timing_events` deque (maxlen 20k) records `(wall_ts, df)` for every decoded message in the native path. `get_timing_events(since_ts)` returns only events newer than the caller's cursor.  
`backend/timing.py`: `GET /api/timing/events?since_ts=<float>` endpoint with a 5000-event hard cap per response.  
`ReceiverPage.jsx`: `MessageTimingPlot` — `requestAnimationFrame`-animated canvas with one horizontal lane per active DF type (ordered by message count). Polls every 200 ms using `since_ts` for incremental delivery. Page Visibility API pauses both polling and animation when the tab is hidden, preventing wasted CPU at high message rates. 1-second vertical grid lines; DF colours consistent with `LiveDFBreakdown`.

---

## Files Changed (key)

| Layer | Files |
|-------|-------|
| Native C | `decode_api.h`, `decode_api.c` (IID field) |
| Backend — core | `aircraft_state.py` (pos_source, timing buffer, IID events) |
| Backend — API | `aircraft.py` (airport name resolution), `terrain.py` (range profiles), `interrogators.py` (new), `timing.py` (new), `history.py` (skyview endpoint), `main.py` (router wiring) |
| Frontend — hooks | `useAircraftFilter.js` (new), `useFetchState.js` (new) |
| Frontend — components | `SourceBadge.jsx` (new), `LoadingState.jsx` (new), `AircraftDetailPanel.jsx`, `AircraftTable.jsx`, `StatsBar.jsx` |
| Frontend — pages | `ReceiverPage.jsx`, `SkyView.jsx`, `MapPage.jsx`, `CoveragePage.jsx`, `App.jsx` |
