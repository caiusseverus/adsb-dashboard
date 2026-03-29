# ADS-B Dashboard — Quality Audit

**Date:** 2026-03-28
**Scope:** Full codebase (`backend/`, `frontend/src/`, config, Docker)
**Method:** Static analysis via complete file read; no runtime testing

---

## Fix Progress

| ID | Description | Commit | Status |
|----|-------------|--------|--------|
| MiniMap | `invalidateSize()` after init and before fitBounds | `ededf70` | ✅ Fixed |
| B-H1 | CORS `allow_methods` missing PUT | `ededf70` | ✅ Fixed |
| F-H3 | StatsBar grid `auto-fill` | `ededf70` | ✅ Fixed |
| B-H3 | `_adsbx_conn` RLock | `3a3990b` | ✅ Fixed |
| F-M18 | `Math.min(...spread)` → reduce | `3a3990b` | ✅ Fixed |
| F-H2 | MapPage stale `receiverPos` closure | `fe42c9a` | ✅ Fixed |
| F-M2 | AircraftTable filter empty state | `fe42c9a` | ✅ Fixed |
| X-M1 / B-M2 / F-M11 | Consolidate `haversine`/`bearing` into `utils_geo.py` + `geo.js` | — | ✅ Fixed |
| F-H1 | CoveragePage module-level `_altScale`/`_curveMode` → `useRef` + param threading | — | ✅ Fixed |
| F-H4 | BenchmarkPanel stale `startPolling` closure; reorder hooks + fix deps | — | ✅ Fixed |
| B-H2 | No auth on any endpoint — document LAN-only assumption in `main.py` | — | ✅ Fixed |
| B-M10 | `global _route_queue_drops` moved to function top in `_push_updates` | — | ✅ Fixed |
| F-M1 | Dead expression `{filter === 'all' && ... && null}` removed | — | ✅ Fixed |
| F-M7 | AltHeatmap fetch callback used stale `ceilFt` — fixed via `extendedRef` | — | ✅ Fixed |
| F-M5 | `ALL_DAYS`/`DAY_LABELS` stale after midnight — moved into component with midnight timer | — | ✅ Fixed |
| F-M4 | `new Date()` allocated per cell in CalendarHeatmap `buildWeeks` — hoisted before loop | — | ✅ Fixed |
| F-M15 | PositionQualityPage off-palette colors `#22c55e`/`#38bdf8` → `#3fb950`/`#388bfd` | — | ✅ Fixed |
| F-M12 | `milParam` local var in `TopOperators` shadowed outer function — renamed to `milFilter` | — | ✅ Fixed |
| F-M17 | FlowMapPage `delete L.Icon.Default.prototype._getIconUrl` — dead code (no markers created); removed | — | ✅ Fixed |
| F-M13 | `EMERGENCY_SQUAWKS` duplicated in AircraftTable/AircraftDetailPanel/SkyView — extracted to `utils/squawks.js` | — | ✅ Fixed |
| F-M8  | MlatScorecard `aggregateSources` called in render body — wrapped in `useMemo` | — | ✅ Fixed |
| F-M16 | PositionQualityPage polled at 1 Hz unconditionally — paused via Page Visibility API | — | ✅ Fixed |

---

## Summary

| Severity | Backend | Frontend | Cross-cutting | Total | Fixed |
|----------|---------|----------|---------------|-------|-------|
| Critical | 0       | 0        | 0             | **0** | —     |
| High     | 3       | 4        | 0             | **7** | 7     |
| Medium   | 10      | 19       | 2             | **31**| 14    |
| Low      | 10      | 8        | 1             | **19**| 0     |
| **Total**| **23**  | **31**   | **3**         | **57**| **21** |

---

## Backend Issues

### High

#### ~~B-H1 — CORS `allow_methods` missing PUT~~ ✅ Fixed in `ededf70`
- **Severity:** High
- **Type:** Bug
- **File:** `backend/main.py:843`
- **Description:** `allow_methods=["GET", "POST", "DELETE"]` omits `PUT`. The cast router (`backend/cast_api.py:61`, `:108`) defines `PUT /api/cast/config` and `PUT /api/cast/rules/{rule_id}`. Cross-origin preflight requests for PUT will receive 405 Method Not Allowed.
- **Impact:** Cast config and rule updates are broken when the frontend is served from a different origin (Vite dev proxy sidesteps this, but any non-proxied CORS client will fail).

#### B-H2 — No authentication on any endpoint
- **Severity:** High
- **Type:** Security
- **File:** `backend/main.py` (all routers)
- **Description:** Every endpoint — including destructive ones (`POST /api/debug/aircraft/{icao}/override`, `DELETE /api/cast/rules/{rule_id}`, `POST /api/history/visits/cleanup`, `POST /api/debug/benchmark`) — is publicly accessible with zero authentication or authorization.
- **Impact:** Anyone with network access can modify aircraft records, delete data, trigger benchmarks that pause the decoder, or change notification/cast settings. Acceptable on a private LAN; dangerous if exposed to the internet.

#### ~~B-H3 — `EnrichmentDB._adsbx_conn` not protected by a lock~~ ✅ Fixed in `3a3990b`
- **Severity:** High
- **Type:** Bug / Race Condition
- **File:** `backend/enrichment.py:315, :563–743`
- **Description:** `_adsbx_conn` is a single `sqlite3.Connection` shared across all callers. Methods like `get_adsbx()`, `_import_adsbx_csv()`, and `_import_adsbx_legacy_cache()` can be called from multiple `asyncio.to_thread` workers concurrently with no mutex.
- **Impact:** `sqlite3.OperationalError` or database corruption under concurrent enrichment lookups.

### Medium

#### B-M1 — Duplicate ghost filter logic
- **Severity:** Medium
- **Type:** Duplication
- **File:** `backend/main.py:260–293`
- **Description:** `_credible_aircraft(ac)` operates on `Aircraft` dataclass objects; `_ghost_credible(ac: dict)` does the same logic on dict snapshots. Both perform identical enrichment DB lookups.
- **Impact:** Maintenance burden; divergent bug fixes if one is updated without the other.

#### B-M2 — `haversine_nm` / `bearing_deg` duplicated across 3 files
- **Severity:** Medium
- **Type:** Duplication
- **File:** `backend/aircraft_state.py:403,:411`, `backend/coverage.py:35,:44`, `backend/position_quality.py:29`
- **Description:** Identical implementations of `_haversine_nm()` and `_bearing_deg()` in three separate files.
- **Impact:** Triple maintenance; any fix must be applied in all three places.

#### B-M3 — Cast table schema duplicated in db.py
- **Severity:** Medium
- **Type:** Dead Code / Duplication
- **File:** `backend/db.py:426–436, :457–468`
- **Description:** Cast tables (`cast_config`, `cast_rules`) are created in the main schema init block and again in a separate migration block with identical `CREATE TABLE IF NOT EXISTS`. The migration block is redundant for new databases.
- **Impact:** No functional bug (IF NOT EXISTS is idempotent), but confusing.

#### B-M4 — Unbounded `_route_cache` in aircraft.py
- **Severity:** Medium
- **Type:** Performance / Resource Leak
- **File:** `backend/aircraft.py:27`
- **Description:** `_route_cache: dict[str, tuple[float, dict | None]] = {}` grows without bound. Entries are added but never evicted.
- **Impact:** Slow memory leak proportional to unique callsigns seen over months.

#### B-M5 — Unbounded `_cooldown` dict in cast.py
- **Severity:** Medium
- **Type:** Performance / Resource Leak
- **File:** `backend/cast.py:64`
- **Description:** `_cooldown: dict[str, float] = {}` accumulates ICAO entries that are never purged.
- **Impact:** Minor memory leak; grows with every unique ICAO that triggers a cast evaluation.

#### B-M6 — mlat.py accesses private `_state._lock` and `_state._aircraft`
- **Severity:** Medium
- **Type:** Inconsistency / Encapsulation Violation
- **File:** `backend/mlat.py:28–29, :53–54, :76–77`
- **Description:** The mlat router directly acquires `_state._lock` and iterates `_state._aircraft` instead of using public methods on `AircraftState`. No other router does this.
- **Impact:** Fragile coupling; breaks silently if `AircraftState` internals change.

#### B-M7 — `_route_cache` serves stale entries indefinitely on fetch failure
- **Severity:** Medium
- **Type:** Bug (minor)
- **File:** `backend/aircraft.py:304–311`
- **Description:** On failed route lookup, the old cache entry remains with its original timestamp, never refreshing. Stale route data is served indefinitely.
- **Impact:** Low severity since routes rarely change mid-flight, but incorrect after the TTL window.

#### B-M8 — f-string interpolation for SQL table/column names in db.py
- **Severity:** Medium
- **Type:** Security (Defence-in-depth)
- **File:** `backend/db.py:2695, :2699`
- **Description:** `f"SELECT COUNT(*) FROM {tbl}"` uses f-string interpolation. Values come from a hardcoded list (safe today), but there is no whitelist assertion at the db.py layer.
- **Impact:** No exploitable vulnerability today; would become an injection vector if callers ever pass user input.

#### B-M9 — `update_aircraft_field` f-string column name with distant validation
- **Severity:** Medium
- **Type:** Security (Defence-in-depth)
- **File:** `backend/db.py:2136`, `backend/debug.py:209`
- **Description:** `f"UPDATE aircraft_registry SET {field} = ?"` interpolates `field`. Validation exists in `debug.py` via `OVERRIDEABLE_FIELDS` whitelist, but not enforced at the db.py layer.
- **Impact:** SQL injection vector if any future caller bypasses the whitelist.

#### B-M10 — `global _route_queue_drops` declared inside nested scope
- **Severity:** Medium
- **Type:** Code Quality
- **File:** `backend/main.py:452`
- **Description:** `global _route_queue_drops` is declared inside an `if expired:` block rather than at function top. Syntactically valid but easy to miss.
- **Impact:** Readability only.

### Low

#### B-L1 — `_fetch()` retries on all exceptions including permanent failures
- **Severity:** Low
- **Type:** Performance
- **File:** `backend/enrichment.py:281`
- **Description:** Retry loop retries 3 times on any exception, including 404/403 that will never succeed.
- **Impact:** Wasted time (up to 45s) on permanent failures.

#### B-L2 — No size limit on initial WebSocket snapshot
- **Severity:** Low
- **Type:** Performance
- **File:** `backend/main.py:872`
- **Description:** Full aircraft snapshot sent on WebSocket connect. With many tracked aircraft, this can be large.
- **Impact:** Slow initial connect on constrained links.

#### B-L3 — Beast client buffer can grow unbounded on malformed streams
- **Severity:** Low
- **Type:** Performance / Resource
- **File:** `backend/beast_client.py`
- **Description:** Internal `bytearray` buffer grows with no cap on persistent framing errors.
- **Impact:** Theoretical memory issue on extremely malformed streams.

#### B-L4 — Terrain tile cache is unbounded
- **Severity:** Low
- **Type:** Performance / Resource
- **File:** `backend/terrain.py`
- **Description:** SRTM HGT tiles cached in memory with no eviction (~2.4 MB each).
- **Impact:** Memory growth on systems with diverse geographic coverage.

#### B-L5 — `position_quality.py` HTTP fetch has no explicit timeout
- **Severity:** Low
- **Type:** Performance
- **File:** `backend/position_quality.py`
- **Description:** `urllib.request.urlopen` for readsb JSON with no timeout. Blocks for OS TCP default (~120s+) if readsb is unreachable.
- **Impact:** Endpoint hangs for a long time when readsb is down.

#### B-L6 — `cast.py` photo fetch has no size limit
- **Severity:** Low
- **Type:** Security / Resource
- **File:** `backend/cast.py`
- **Description:** `_fetch_photo()` downloads aircraft photos from external URLs without checking Content-Length or capping read size.
- **Impact:** Memory exhaustion if a fetched image is extremely large. Unlikely with planespotters.net.

#### B-L7 — `benchmark.py` pauses live decoder with no auth gate
- **Severity:** Low
- **Type:** Operational Risk
- **File:** `backend/benchmark.py`
- **Description:** `POST /api/debug/benchmark` pauses the decoder thread. Combined with B-H2 (no auth), anyone can trigger this.
- **Impact:** Denial-of-service for live tracking during benchmark runs.

#### B-L8 — `health.py` type hint is imprecise
- **Severity:** Low
- **Type:** Code Quality
- **File:** `backend/health.py:91`
- **Description:** `register_context(msg_queue, clients: list)` — `msg_queue` has no type annotation; `clients` typed as `list` but receives a `dict[WebSocket, Queue]`.
- **Impact:** Misleading type hints; no runtime effect.

#### B-L9 — Notify/backup endpoint has no rate limit
- **Severity:** Low
- **Type:** Security / Resource
- **File:** `backend/notify_settings.py`
- **Description:** Backup endpoint can be called repeatedly with no throttle.
- **Impact:** Excessive disk I/O under intentional abuse.

#### B-L10 — `readsb_ingest.py` polling interval not configurable
- **Severity:** Low
- **Type:** Code Quality
- **File:** `backend/readsb_ingest.py`
- **Description:** Polling sleep interval is hardcoded rather than using a config variable.
- **Impact:** Inflexible for users wanting faster/slower polling.

✅ **Backend — 23 issues found**

---

## Frontend Issues

### High

#### F-H1 — Module-level mutable state in CoveragePage
- **Severity:** High
- **Type:** Bug / Architecture
- **File:** `frontend/src/pages/CoveragePage.jsx:11–12`
- **Description:** `_altScale` and `_curveMode` are module-level `let` variables mutated by UI handlers and read by builder functions. If React StrictMode double-mounts or a second instance is created, these silently corrupt.
- **Impact:** Incorrect 3D scene rendering if single-instance assumption is violated.

#### ~~F-H2 — `useEffect` closes over stale `receiverPos` in MapPage~~ ✅ Fixed in `fe42c9a`
- **Severity:** High
- **Type:** Bug
- **File:** `frontend/src/pages/MapPage.jsx:~140`
- **Description:** Leaflet map init `useEffect` has `[]` deps but reads `receiverPos` from closure. If `receiverPos` arrives after first render (async from App.jsx), the map uses `null` fallback and never re-centers.
- **Impact:** Map may not center on receiver position if the status API is slow on first load.

#### ~~F-H3 — StatsBar grid column mismatch~~ ✅ Fixed in `ededf70`
- **Severity:** High
- **Type:** Bug
- **File:** `frontend/src/components/StatsBar.jsx` + `StatsBar.module.css`
- **Description:** CSS grid is hardcoded to 4 columns but the component renders 5–8 stat cards depending on MLAT availability.
- **Impact:** Visual layout breakage when MLAT cards are shown; extra cards wrap unpredictably.

#### F-H4 — BenchmarkPanel stale closure on re-mount
- **Severity:** High
- **Type:** Bug
- **File:** `frontend/src/pages/BenchmarkPanel.jsx:129–146`
- **Description:** Mount `useEffect` (line 129) calls `startPolling()` but has `[]` deps with eslint-disable suppressing the missing dependency warning. `startPolling` is a `useCallback` defined later. On re-mount, the closure may reference a stale `startPolling`.
- **Impact:** Polling may not restart correctly after component re-mount.

### Medium

#### F-M1 — Dead expression in AircraftTable
- **Severity:** Medium
- **Type:** Dead Code
- **File:** `frontend/src/components/AircraftTable.jsx:124`
- **Description:** `{filter === 'all' && aircraft.length !== filtered.length && null}` always evaluates to `null`.
- **Impact:** Dead code; confusing to maintainers.

#### ~~F-M2 — Wrong empty-state check in AircraftTable~~ ✅ Fixed in `fe42c9a`
- **Severity:** Medium
- **Type:** Bug
- **File:** `frontend/src/components/AircraftTable.jsx:~211`
- **Description:** Checks `aircraft.length === 0` for the empty message but should check `filtered.length === 0`. When all aircraft are filtered out, no empty message appears.
- **Impact:** User sees empty table with no explanation when filter excludes all aircraft.

#### F-M3 — SVG gradient ID collision risk
- **Severity:** Medium
- **Type:** Bug
- **File:** `frontend/src/components/MessageRateChart.jsx` (ID `bandGrad`), `TrendChart.jsx` (ID `totalGrad`)
- **Description:** Recharts gradient `<defs>` use hardcoded IDs. Multiple instances on one page would produce ID collisions.
- **Impact:** Visual corruption if multi-instanced. Low risk today since each appears once.

#### F-M4 — `new Date()` per cell in CalendarHeatmap
- **Severity:** Medium
- **Type:** Performance
- **File:** `frontend/src/components/CalendarHeatmap.jsx:~44`
- **Description:** `buildWeeks` creates `new Date()` inside the loop body for `future: cur > new Date()`, allocating per cell per render.
- **Impact:** Minor GC pressure; should hoist `now` before the loop.

#### F-M5 — `ALL_DAYS` / `DAY_LABELS` stale after midnight
- **Severity:** Medium
- **Type:** Bug
- **File:** `frontend/src/components/HourlyHeatmap.jsx:53–64`
- **Description:** Computed at module load time. If the dashboard runs overnight without refresh, day labels become stale.
- **Impact:** Heatmap shows wrong day labels after midnight.

#### F-M6 — `cellColor` function duplicated across 3 heatmap components
- **Severity:** Medium
- **Type:** Duplication
- **File:** `HourlyHeatmap.jsx`, `DFHeatmap.jsx`, `CalendarHeatmap.jsx`
- **Description:** Identical heatmap coloring logic copy-pasted. `buildDayList`, `formatBucket`, `ALL_DAYS`, and `DAY_LABELS` also duplicated between HourlyHeatmap and DFHeatmap.
- **Impact:** Maintenance burden; color logic can drift.

#### F-M7 — `ceilFt` dependency suppressed in AltHeatmap
- **Severity:** Medium
- **Type:** Bug
- **File:** `frontend/src/components/AltHeatmap.jsx:158–159`
- **Description:** Fetch `useEffect` has eslint-disable suppressing `ceilFt` dependency. Altitude ceiling change does not re-fetch data.
- **Impact:** Heatmap shows data for wrong altitude range after ceiling change.

#### F-M8 — MlatScorecard recomputes aggregation every render
- **Severity:** Medium
- **Type:** Performance
- **File:** `frontend/src/components/MlatScorecard.jsx`
- **Description:** `aggregateSources(aircraft)` called in render body with no `useMemo`. `aircraft` is a new array from every WebSocket snapshot (1 Hz).
- **Impact:** Unnecessary CPU work every second.

#### F-M9 — `makeIcon` creates new `L.divIcon` per aircraft per snapshot
- **Severity:** Medium
- **Type:** Performance
- **File:** `frontend/src/pages/MapPage.jsx`
- **Description:** Each marker update creates a new Leaflet `divIcon`. With 100+ aircraft at 1 Hz, this is hundreds of short-lived DOM objects per second.
- **Impact:** GC pressure and potential jank on low-power devices.

#### F-M10 — Duplicated `useFetch`, `Card`, `Empty` across pages
- **Severity:** Medium
- **Type:** Duplication
- **File:** `frontend/src/pages/ReceiverPage.jsx`, `FleetPage.jsx`
- **Description:** Both pages define identical `useFetch` custom hooks, `Card` wrapper components, and `Empty` placeholders.
- **Impact:** Fixes in one copy won't propagate to the other.

#### F-M11 — `haversineNm` duplicated across 3 frontend files
- **Severity:** Medium
- **Type:** Duplication
- **File:** `CoveragePage.jsx:36`, `MapPage.jsx`, `ReceiverPage.jsx`
- **Description:** Haversine function copy-pasted. CoveragePage re-declares `const R_NM = 3440.065` inside function body, shadowing module-level constant.
- **Impact:** Shadowed constant could diverge; triple maintenance burden.

#### F-M12 — Variable shadowing in FleetPage `TopOperators`
- **Severity:** Medium
- **Type:** Bug-prone
- **File:** `frontend/src/pages/FleetPage.jsx:~203`
- **Description:** `TopOperators` component shadows outer-scope `milParam` with a local `const milParam`. Outer function changes would be silently masked.
- **Impact:** Could mask bugs if outer function signature changes.

#### F-M13 — `EMERGENCY_SQUAWKS` duplicated
- **Severity:** Medium
- **Type:** Duplication
- **File:** `AircraftTable.jsx`, `EventsPage.jsx`
- **Description:** Emergency squawk code-to-label mapping defined independently in both files.
- **Impact:** Both files must be updated if squawk labels change.

#### F-M14 — Formatting functions (`fmtTs`, `fmtAlt`) duplicated
- **Severity:** Medium
- **Type:** Duplication
- **File:** `EventsPage.jsx`, `AircraftDetailPanel.jsx`, and others
- **Description:** Timestamp and altitude formatting reimplemented in multiple components.
- **Impact:** Inconsistent formatting if implementations drift.

#### F-M15 — `PositionQualityPage` uses off-palette colors
- **Severity:** Medium
- **Type:** Inconsistency
- **File:** `frontend/src/pages/PositionQualityPage.jsx`
- **Description:** Uses `#22c55e` and `#38bdf8` (Tailwind defaults) instead of project palette (`#3fb950`, `#388bfd`).
- **Impact:** Visual inconsistency with the rest of the dashboard.

#### F-M16 — `PositionQualityPage` polls at 1-second interval unconditionally
- **Severity:** Medium
- **Type:** Performance
- **File:** `frontend/src/pages/PositionQualityPage.jsx`
- **Description:** `setInterval` at 1000ms polls the API continuously while the tab is active, even if the user isn't looking.
- **Impact:** Unnecessary network and server load.

#### F-M17 — FlowMapPage monkey-patches `L.Icon.Default.prototype`
- **Severity:** Medium
- **Type:** Bug-prone
- **File:** `frontend/src/pages/FlowMapPage.jsx`
- **Description:** `delete L.Icon.Default.prototype._getIconUrl` at module level permanently modifies the Leaflet prototype globally.
- **Impact:** Could cause unexpected icon behavior in MapPage or AircraftDetailPanel MiniMap.

#### ~~F-M18 — Timelapse `Math.min(...array)` stack overflow risk~~ ✅ Fixed in `3a3990b`
- **Severity:** Medium
- **Type:** Bug
- **File:** `frontend/src/pages/CoveragePage.jsx:1210–1211`
- **Description:** `Math.min(...data.tracks.map(...))` spreads entire tracks array as arguments. V8 has a ~65k argument limit. Large timelapse datasets will throw `RangeError`.
- **Impact:** Crash on very large timelapse datasets.

#### F-M19 — `altColor` allocates new `THREE.Color` per point
- **Severity:** Medium
- **Type:** Performance
- **File:** `frontend/src/pages/CoveragePage.jsx:101–109`
- **Description:** `c0.clone().lerp(c1, ...)` allocates a new `THREE.Color` per point. For 100k+ points, this creates 100k+ temporary objects.
- **Impact:** GC pressure during scene builds.

### Low

#### F-L1 — `SightingsPage` imports `HistoryPage.module.css`
- **Severity:** Low
- **Type:** Inconsistency
- **File:** `frontend/src/pages/SightingsPage.jsx:2`
- **Description:** Uses another page's stylesheet, creating an implicit cross-page dependency.
- **Impact:** HistoryPage style changes could inadvertently affect SightingsPage.

#### F-L2 — `HistoryPage` accepts unused `snapshot` prop
- **Severity:** Low
- **Type:** Dead Code
- **File:** `frontend/src/App.jsx:174` → `frontend/src/pages/HistoryPage.jsx`
- **Description:** App passes `snapshot={snapshot}` to `<HistoryPage>` but the function signature ignores it.
- **Impact:** Unnecessary prop passing; no functional issue.

#### F-L3 — BenchmarkPanel injects global `<style>` for keyframe
- **Severity:** Low
- **Type:** Inconsistency
- **File:** `frontend/src/pages/BenchmarkPanel.jsx:224`
- **Description:** `<style>{@keyframes spin {...}}</style>` injected inside component return. Could conflict with identical `@keyframes spin` in `CoveragePage.module.css`.
- **Impact:** Low risk due to CSS specificity, but pollutes global scope.

#### F-L4 — BenchmarkPanel uses inline styles throughout
- **Severity:** Low
- **Type:** Inconsistency
- **File:** `frontend/src/pages/BenchmarkPanel.jsx`
- **Description:** Every element uses `style={{...}}` objects instead of CSS Modules. The only component that deviates from project convention.
- **Impact:** Inline style objects recreated on every render; style maintenance inconsistency.

#### F-L5 — No `ErrorBoundary` around lazy-loaded pages
- **Severity:** Low
- **Type:** Missing Error Handling
- **File:** `frontend/src/App.jsx:172`
- **Description:** `<Suspense fallback={null}>` wraps lazy pages but there is no `<ErrorBoundary>`. If a chunk fails to load, React throws an unhandled error.
- **Impact:** Full app crash on chunk load failure; no recovery path.

#### F-L6 — WebSocket reconnect has no exponential backoff
- **Severity:** Low
- **Type:** Performance
- **File:** `frontend/src/App.jsx:58`
- **Description:** Fixed 3-second retry on `ws.onclose`. During extended outages, this generates constant reconnection attempts.
- **Impact:** Unnecessary traffic during extended outages; benign for typical use.

#### F-L7 — `formatOperator.js` ACRONYMS set is incomplete
- **Severity:** Low
- **Type:** Inconsistency
- **File:** `frontend/src/utils/formatOperator.js`
- **Description:** The ACRONYMS set preserves casing for known acronyms, but the list is necessarily incomplete.
- **Impact:** Minor display issues for some operator names.

#### F-L8 — `terrain.js` creates geometry without checking WebGL limits
- **Severity:** Low
- **Type:** Bug-prone
- **File:** `frontend/src/utils/terrain.js`
- **Description:** Terrain mesh built from grid data without checking WebGL max vertex count. A large grid could exceed device limits.
- **Impact:** Silent failure or crash on devices with lower WebGL limits.

✅ **Frontend — 31 issues found**

---

## Cross-Cutting Issues

### Medium

#### X-M1 — `haversine_nm` / `bearing_deg` duplicated across backend AND frontend
- **Severity:** Medium
- **Type:** Duplication
- **File:** Backend (3 files) + Frontend (3 files) = 6 independent implementations
- **Description:** The haversine distance and bearing calculations are implemented independently in `aircraft_state.py`, `coverage.py`, `position_quality.py`, `CoveragePage.jsx`, `MapPage.jsx`, and `ReceiverPage.jsx`. Backend duplication should be a shared `utils.py` function; frontend should be a shared `utils/geo.js`.
- **Impact:** Six places to fix if the formula has a bug; high drift risk.

#### X-M2 — No linting or formatting tooling configured
- **Severity:** Medium
- **Type:** Code Quality
- **File:** `backend/pyproject.toml`, `frontend/package.json`
- **Description:** No linter (ruff, eslint), formatter (black, prettier), or type checker (mypy, tsc) is configured. The project relies entirely on manual code review.
- **Impact:** Inconsistencies (naming, style) accumulate over time; no automated enforcement.

### Low

#### X-L1 — Dockerfile pins uv via `COPY --from=ghcr.io/astral-sh/uv:latest`
- **Severity:** Low
- **Type:** Inconsistency
- **File:** `Dockerfile:20`
- **Description:** Comment says "pinned for reproducibility" but the tag is `:latest`, not a version pin.
- **Impact:** Builds may get different uv versions; could break reproducibility.

✅ **Cross-cutting — 3 issues found**

---

## Top Priorities (recommended fix order)

1. ✅ **B-H1** — Add `PUT` to CORS `allow_methods` (`ededf70`)
2. ✅ **B-H3** — Add a lock around `_adsbx_conn` (`3a3990b`)
3. ✅ **F-H3** — Fix StatsBar grid to accommodate variable card count (`ededf70`)
4. ✅ **F-M18** — Replace `Math.min(...spread)` with reduce loop (`3a3990b`)
5. ✅ **F-H2** — Fix MapPage `receiverPos` stale closure (`fe42c9a`)
6. ✅ **F-M2** — AircraftTable filter empty state (`fe42c9a`)
7. ✅ **MiniMap** — `invalidateSize()` on init and before fitBounds (`ededf70`)
8. **X-M1 / B-M2 / F-M11** — Consolidate `haversine`/`bearing` into shared utils (6 dupes)
9. **B-H2** — Document the "LAN-only" security assumption; optionally add auth
10. **F-H1** — Refactor CoveragePage module-level mutable state into React state/ref
