# Polish & Features Implementation Plan

Source: `.docs/development/polish-and-features-proposal.md` + addendum (2026-04-03).
Addendum corrections are applied inline — see the "Corrections" section below for key facts.

---

## Key Corrections (from addendum — must read before implementing any item)

- **Item 1**: Use `Intl.NumberFormat(undefined, {notation:'compact', maximumFractionDigits:2})` — not a hand-rolled formatter.
- **Item 2**: `/api/aircraft/{icao}/route` already returns full airport data; detail panel just isn't consuming it. Visit history names are a separate scope (needs persisted metadata).
- **Item 9**: Aircraft **above** the horizon curve are expected/visible; those **below** are the anomalies. `<ReferenceLine>` can't render a curve — needs a computed `<Line>` data series. Receiver elevation is NOT in config; use sea-level reference only.
- **Item 12**: Endpoint is `/api/coverage/timelapse_hires`, not `/api/hires/tracks`. Root cause: buffer empty/sparse at startup, no retry path.
- **Item 13**: Coverage page is Three.js, not Leaflet. Terrain tile base layer belongs to the **Map page**, not Coverage.
- **Item 14**: Position Quality page already exists — build on it, don't start from scratch.
- **Item 15**: `backend/native/mode_s.c` already tracks `IID` for DF11 — use the native path, do not add parallel Python bit extraction.

---

## Phase 0 — Shared Infrastructure (prerequisites)

- [x] **0A — Shared aircraft filter hook** (`useAircraftFilter` or filter context)
  - Audit current filter logic in: `AircraftTable.jsx`, `MapPage.jsx`, `CoveragePage.jsx`, `SkyView.jsx`
  - Extract shared filter state (military / MLAT / interesting / type_category / type_code) into a reusable hook
  - Replace per-page copies with the shared hook
  - *Prerequisite for items 3, 6, 7a*
  - Effort: ~3h

- [ ] **0B — Loading / empty / error state pattern for historical overlays**
  - Define shared state shape: `{ status: 'idle'|'loading'|'empty'|'error', data, retry }`
  - Apply to: hi-res trail fetch, timelapse fetch, coverage history, route history
  - Add a small reusable loading/empty UI component
  - *Prerequisite for items 6, 12*
  - Effort: ~2h

---

## Phase 1 — Trivial Fixes (ship first, zero risk)

- [x] **1 — Total messages compact notation** (`StatsBar.jsx:18`)
  - Replace `toLocaleString()` with `Intl.NumberFormat(undefined, {notation:'compact', maximumFractionDigits:2}).format(n)`
  - Apply to total messages card only
  - Effort: ~15 min

- [x] **5 — Sky View dot size reduction** (`SkyView.jsx` render loop)
  - Reduce dot radius constant by ~30% (e.g. 4 → 3 px)
  - Optionally scale by signal strength (stronger → slightly larger)
  - Effort: ~20 min

- [x] **8a — Msgs/sec axis fix** (`ReceiverPage.jsx:79`)
  - Divide `msgs` by 60 client-side; update axis label to "msg/sec"
  - No backend change
  - Effort: ~10 min

---

## Phase 2 — Data Correctness (before visual polish)

- [x] **14 — Position leak investigation + fix**
  - Start from existing `PositionQualityPage` — review what it already exposes
  - Add `_pos_source` enum (`GLOBAL_CPR | LOCAL_CPR | MLAT`) to `Aircraft` dataclass in `aircraft_state.py`
  - Expose per-aircraft position-rejection counter in Status/debug tab
  - Apply speed gate (> 1500 kt) to **global** CPR fixes (currently only applied to local)
  - Add freshness guard in coverage writer (`main.py`): skip sample if `time() - ac.last_pos_ts > POS_FRESH_S`
  - Effort: ~3–4h

- [x] **10 — Signal distribution investigation** — RESOLVED: apples-to-oranges, no fix needed
  - `ac.signal` stored as raw Beast byte (0-255); conversion `-(raw/2)` applied correctly in both Beast decode path and readsb JSON path
  - `signal_heatmap` uses coverage_samples (per-aircraft-per-minute) → distinct aircraft counts per signal bucket — fundamentally different from graphs1090 per-message peaks
  - `minute_stats.signal_avg` is a per-minute average, not per-message peak distribution
  - Conclusion: discrepancy vs graphs1090 is inherent in the sampling scope, not a bug

---

## Phase 3 — Quick-Win Functional Improvements

- [x] **8b — Colour-by-age toggle on receiver scatter** (`ReceiverPage.jsx`)
  - `ts` field already returned by scatter endpoint (confirmed in `db.py`) — frontend change only
  - Add colour-mode toggle: Signal / Age
  - Age bucket colours: Today=#3fb950, Yesterday=#388bfd, Week=#d29922, Month=#bc8cff, Older=#6e7681
  - Persist selection in `localStorage`
  - Effort: ~2h

- [x] **9 — Horizon envelope on range/altitude scatter** (`ReceiverPage.jsx:250-279`)
  - Compute series: `{range_nm, alt_ft}` where `alt_ft = (range_nm / 1.23)²` (radio horizon)
  - Render as a `<Line>` overlay series (NOT `<ReferenceLine>` — that's for straight lines only)
  - Label: "radio horizon"
  - Do NOT use receiver elevation (not in config) — use sea-level reference
  - Correct axis labels: aircraft below the line are anomalies, above are expected
  - Effort: ~1.5h

- [x] **11 — Coverage trail length slider** (`CoveragePage.jsx:17`)
  - Reduce `MAX_TRAIL_PTS` default: 600 → 180 (3 min)
  - Add dropdown control: 1 / 3 / 5 / 10 min
  - Keep live-trail, hi-res backfill depth, and timelapse history as separate controls (separate user needs)
  - Persist in `localStorage`
  - Effort: ~1h

- [x] **12 — Hi-res trail loading fix** (`CoveragePage.jsx:149-189`)
  - Confirm endpoint is `/api/coverage/timelapse_hires`
  - Add loading state (via Phase 0B pattern)
  - If response has 0 trail points for aircraft visible in live snapshot → retry once after 5s
  - Check age-filter off-by-one on backend
  - Effort: ~1.5h

- [x] **13c — Terrain tile base layer (Map page only)**
  - Add terrain tile style as alternative base layer on **2D MapPage** layer toggle
  - Use existing layer-toggle mechanism (not Coverage page — that's Three.js)
  - Effort: ~30 min

---

## Phase 4 — Medium UI Improvements

- [x] **2A — Airport names in detail panel route header** (`AircraftDetailPanel.jsx:340-349`)
  - `/api/aircraft/{icao}/route` already returns `Airport`, `City`, `IATA`, `ICAO` fields — just wire it in
  - Inline format: `EGLL (Heathrow) → KJFK (Kennedy)`
  - Effort: ~1h

- [ ] **2B — Airport names in visit history** (separate from 2A)
  - Options: (a) on-demand hexdb lookup per visit with module-level cache, or (b) persist airport names to SQLite at route-write time
  - Decide scope: option (b) is more robust for large histories
  - Effort: ~3h

- [x] **3 — Sky View filter alignment with Coverage page** (`SkyView.jsx`)
  - Implement **after Phase 0A** (shared filter hook) — do not copy Coverage filter logic by hand
  - Add Type Group dropdown and Type Code dropdown using shared hook
  - Client-side filtering against live aircraft array — no backend change
  - Effort: ~2h (with shared hook); ~2.5h (without)

- [x] **7b — Per-segment trail colour on map** (`MapPage.jsx:245-247`)
  - Implement for **selected aircraft only** first (not all-aircraft — too expensive)
  - On selection: fetch track from `/api/aircraft/{icao}/track`; split into two-point `L.polyline` segments; colour each by `altColor(altitude_ft)`
  - Cap at 200 segments by downsampling older points
  - After validation: evaluate all-aircraft gradient trails
  - Effort: ~2.5h

---

## Phase 5 — Larger Features

- [x] **7a — Map side panel / drawer** (`App.jsx`, `AircraftDetailPanel`)
  - `AircraftDetailPanel` is a centred modal owned at app level in `App.jsx` — this is an app-wide UX change
  - Add `mode` prop to `AircraftDetailPanel`: `'modal' | 'drawer'`
  - Drawer: `position:fixed; right:0; top:0; height:100%; width:380px`; map content adjusts via `padding-right` or CSS grid
  - On marker click: force-enable trail, show callsign label at marker, open drawer
  - Handle mobile/narrow viewports: drawer should collapse or overlay on small screens
  - Effort: ~4–5h

- [ ] **6 — Sky View historical dots** (`SkyView.jsx`)
  - Implement **after Phase 0A + 0B**
  - Needs server-side aggregation: backend bins `coverage_samples` into 2°az × 1°el cells, returns density + avg signal per bin
  - New backend endpoint: `/api/skyview/history?hours=N`
  - Frontend: heatmap overlay mode toggle (Live / History); derive elevation angle from `arctan(alt_ft / (range_nm × 6076))`
  - Effort: ~3h backend + ~3h frontend

- [ ] **4 — Terrain pseudo-3D (Sky View)** (`terrain.py`, `SkyView.jsx`)
  - Option A (range rings) — correct choice for ADS-B use
  - Backend: extend `/api/terrain/horizon` to accept fixed range set [25, 50, 75, 100 nm]; return array of elevation profiles; apply per-azimuth hidden-surface removal (max of nearer bands)
  - Frontend: layered filled areas with progressive shading
  - Fix range set to avoid cache-key explosion
  - Effort: ~5h backend + ~3h frontend

---

## Phase 6 — New Features

- [ ] **Source / confidence badge system**
  - `pos_confident`, `mlat`, `has_adsb` already tracked in backend
  - Add `<SourceBadge type="ADS-B|MLAT|EST" />` component
  - Apply to: live table, map markers (subtle), detail panel
  - No backend change needed
  - Effort: ~2h
  - *Implement after Item 14 (position correctness)*

- [ ] **15 — DF11 II/SI interrogator codes**
  - Start from `backend/native/mode_s.c` which already tracks `IID` for DF11 — expose via existing decode path (do NOT add parallel Python extraction)
  - Maintain rolling 60s counter `{ii_code: count}` in `aircraft_state.py`
  - Expose via `/api/interrogators` endpoint
  - New sub-tab on Receiver page: active codes table + bar chart of activity last 10 min
  - Clarify scope: specifically DF11 IID, not full II/SI/selective-call abstraction
  - Effort: ~4h backend + ~3h frontend

---

## Phase 7 — Advanced / Deferred

- [ ] **16 — Message timing scroll plot** (advanced observability)
  - New ring buffer in `beast_client.py`/`aircraft_state.py`: last 5s of `(timestamp_us, df_type, msg_len)` tuples
  - New SSE or WebSocket sub-channel at ~10 Hz
  - Canvas-based frontend (not SVG): scrolling X=time, Y=DF type lanes, coloured bars
  - Performance risk at 2000 msg/s — requires `requestAnimationFrame` + batching; only render when tab visible
  - Effort: ~4h backend + ~6h frontend
  - *Treat as advanced diagnostics milestone, not polish pass*

- [ ] **17 — Interrogator timing lanes** (builds on item 15)
  - Per-interrogator timestamp history: `/api/interrogators/timeline`
  - Canvas-based: per-row time lane, tick marks at arrival time, rotation period visible as regular spacing
  - Click row to highlight aircraft on map
  - Effort: ~3h backend + ~5h frontend
  - *Implement after item 15*

---

## Review / Status

_To be filled in as phases complete._
