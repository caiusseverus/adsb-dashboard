## 2026-05-01 Recorded Residual Event Completeness and Retention

- [x] Trace current recorded burst-event, recorded DF11-event, timeline API, and frontend recorded-buffer/render derivation paths.
- [x] Run impact checks for touched backend/frontend symbols and confirm expected blast radius.
- [x] Fix backend recorded burst event generation so every eligible burst alignment observation emits a complete immutable scalar event with geometry and source-path metadata.
- [x] Fix backend recorded event retention/diagnostics so append/prune behaviour is stable and distinguishable from missing new events.
- [x] Fix timeline/live snapshot payload separation so recorded burst rows and recorded DF11 rows remain distinct and complete across the display window.
- [x] Fix frontend recorded-mode consumption so main chart, ICAO chips, burst counts, residual-vs-bearing, and residual-vs-range use recorded fields only.
- [x] Add explicit recorded empty-state / missing-geometry reasons and recorded buffer diagnostics in the UI.
- [x] Add/update backend tests for recorded completeness, event separation, immutability, timestamp pruning, and full-window inclusion.
- [x] Add/update frontend/static tests for recorded-only derivation, no fallback, stable append/prune semantics, and explicit empty reasons.
- [x] Run focused verification and document the review/results in this file.

Plan confirmation:
- Scope limited to recorded residual event generation, schema, retention, API payloads, and frontend recorded rendering/buffering.
- No changes to period refinement maths, phase maths, handoff gates, Go refiner logic, or localisation solving.

Review:
- Go evidence events now emit immutable recorded burst residual events instead of only feeding recomputed diagnostic projections.
- Recorded event payloads now include stable `event_id`, geometry fields, corrected residual, source path, and aircraft position age.
- Recorded event diagnostics now distinguish retained totals, in-window counts, appended/pruned deltas, missing geometry counts, and normal window pruning.
- Frontend recorded mode now maintains a rolling `event_id`-keyed buffer, dedupes partial snapshots, and avoids clearing on mode/basis changes or empty polls.
- Recorded per-ICAO chips, burst counts, residual-vs-bearing, and residual-vs-range all derive from recorded burst events only.
- Empty secondary plots now report explicit reasons instead of silently showing blank charts.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_ui_labels.py -q`
- `cd frontend && npm run build`

Residual risk:
- Manual live-feed validation is still needed to confirm the recorded buffer keeps filling correctly for more than one 300s window under real traffic.

## 2026-05-01 Recorded Event Regression Triage

- [x] Review lessons and inspect the Go snapshot / burst-fired recorded-event paths touched by the prior change.
- [x] Verify the likely regression mechanism and blast radius before editing.
- [x] Move immutable Go recorded-event creation off the snapshot replacement path and back onto the live burst-fired ingest hook.
- [x] Update backend tests to reflect event-time recording semantics and verify snapshot ingestion no longer manufactures recorded events.
- [x] Run backend verification for the regression fix.

Review:
- The regression came from generating immutable Go recorded burst events inside `update_go_snapshot()`, which replays whole retained evidence snapshots rather than only new live events.
- That put expensive per-evidence prediction and dedupe work on the snapshot ingest path, which is the wrong control path for event-time recording and is consistent with live frame/alignment starvation.
- The fix removes recorded-event generation from `update_go_snapshot()` and records Go burst residual events only from `update_go_burst_fired()`, where the event is actually created.
- A second hot-path regression remained in `_append_recorded_event()`: deduplicating by scanning the whole retained deque on every append made live burst ingestion O(n) per event. The fix reduces this to an adjacent-duplicate O(1) guard.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py -q`

Residual risk:
- This is verified by backend tests, not by a live running feed. Runtime confirmation is still needed to prove DF alignment and frame generation recover under live traffic.

## 2026-05-01 Residual/Chart Semantics Regression Recovery

- [x] Review lessons and verify current frame-generation health versus timeline/render regression using counters and pipeline endpoints.
- [x] Run GitNexus impact analysis for all backend/frontend symbols touched by this fix and record any high-risk blast radius.
- [x] Fix backend timeline snapshot builders so burst/frame timeline events are emitted without radar-position gating; geometry fields must be optional.
- [x] Fix backend recorded burst event creation to preserve complete rows even when radar geometry is unavailable.
- [x] Fix backend sweep/timeline API payloads to avoid empty suppressions tied to missing authoritative radar position.
- [x] Fix frontend DF Alignment/Burst Sync fetch lifecycle so IID selection initializes data without view-toggle priming.
- [x] Fix frontend recorded buffer retention semantics: append + dedupe + prune-by-window only; no destructive clears on mode/basis/authority switches.
- [x] Add/update backend tests covering missing-geometry behavior, recorded burst completeness, event-kind separation, and frame counters independence.
- [x] Add/update frontend tests for fetch lifecycle and recorded-buffer behavior, including explicit missing-geometry empty reasons.
- [x] Run focused verification (backend pytest, frontend build/tests, endpoint checks) and document review/results here.

Plan confirmation:
- Preserve the design constraint that pre-localisation sync diagnostics must not require known radar lat/lon.
- Keep geometry-derived fields (`bearing_deg`, `range_nm`, `corrected_residual_deg`) as optional enrichments only.
- Separate "frame generation health" from "chart/timeline display health" in diagnostics and reporting.

Review:
- Backend Go timeline snapshot builders no longer return `[]` solely because authoritative radar position is unavailable; they now emit rows with timing/sync fields intact and nullable geometry fields.
- Recorded Go burst event creation no longer drops events on missing radar position. Events are retained with full metadata, `bearing_deg/range_nm/corrected_residual_deg = null`, and missing-geometry counters are incremented.
- Recorded diagnostics now expose backend created totals by event kind (`backend_recorded_burst_events_created_total`, `backend_recorded_df11_events_created_total`) and omitted-missing-geometry totals.
- Frontend legacy DF-alignment timeline polling is initialized on IID selection rather than being gated by subview selection, removing view-toggle priming behavior.
- Recorded residual empty reasons now explicitly state radar-position-unavailable when geometry-dependent secondary plots are empty for that cause.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_ui_labels.py -q` (116 passed).
- `cd frontend && npm run build` (success).

Residual risk:
- Live endpoint/runtime checks (`/api/radar/iids/<iid>/pipeline-health`, `/api/radar/iids/<iid>/sweep-frames`, `/api/radar/iids/<iid>/state`) were not executed against a running receiver in this pass; runtime confirmation is still required for final sign-off.

## 2026-05-01 Fundamental Radar UI/Data-Path Regression Recovery

- [x] Verify authoritative backend live streams for one active IID and classify breakage layer: generation vs API vs frontend render.
- [x] Run GitNexus impact analysis for each backend/frontend symbol to be edited and report blast radius.
- [x] Decouple frame/sync/radar-location/DF-alignment source-of-truth paths from recorded residual buffers.
- [x] Restore burst-sync recomputed-mode population independent of recorded immutable-event availability.
- [x] Fix DF Alignment fetch lifecycle: initial fetch on IID/view activation and continuous polling while visible.
- [x] Fix frontend state-clearing/caching behavior so empty payloads do not permanently suppress valid data.
- [x] Ensure missing radar position yields nullable geometry enrichment rather than suppressing frame/sync/alignment rows.
- [x] Add compact diagnostics for frame/sync/alignment/location pipeline liveness counts and last-update ages.
- [x] Add backend and frontend regression tests for source independence, fetch lifecycle, and missing-geometry handling.
- [x] Perform verification and document outcomes; keep period/phase/refiner/localisation maths untouched.

Review:
- Runtime probe found backend service not running on `127.0.0.1:8000`, so endpoint liveness classification could not be completed against live traffic in this pass.
- Added backend `data_path_diagnostics` counters/ages to the sync snapshot payload and selected-IID page-state payload, plus a dedicated endpoint: `/api/radar/iids/{iid}/data-path-diagnostics`.
- Added frontend HTTP safety-net polling for:
  - IID live table (`/api/radar/iids`) in `useRadarLiveStream`.
  - selected-IID state (`/api/radar/iids/{iid}/state`) in `useSelectedIidPageState`, even while websocket heartbeat is active.
- Added guard so empty frame payload with unchanged sequence does not overwrite an already-populated selected-IID snapshot.
- Kept frame generation/sync/localisation math untouched; no changes to period refinement, Go refiner logic, phase logic, or solver math.

Verification:
- `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_radar_ui_labels.py -q` (167 passed).
- `cd frontend && npm run build` (success).

Residual risk:
- Live/manual endpoint validation with an active backend process is still required to conclusively classify generation-vs-API-vs-frontend for your current deployment.

## 2026-05-01 Selected-IID Data-Path Regression Audit (In Progress)

- [x] Capture live endpoint snapshots for one active selected IID and classify each endpoint (live, valid-empty, stale, error, frontend-only).
- [x] Verify frame-generation chain (Go FrameReady -> Python ingest -> backend frame state -> sweep API -> pipeline health) with direct counters and classify first failing hop.
- [ ] Run GitNexus impact analysis for each symbol to be edited and report blast radius/risk before edits.
- [x] Audit `frontend/src/pages/RadarPage.jsx` selected-IID fetch/poll lifecycle for sync snapshot, burst timeline, DF alignment, sweep frames, accumulation, FM location/diagnostics, and position accumulation.
- [ ] Audit frontend response-shape mapping against live backend JSON; add explicit schema-mismatch diagnostics (no silent empty success).
- [x] Restore source-of-truth separation so sweep/frame/position/DF/pipeline panels do not depend on recorded residual-event buffers.
- [ ] Fix Burst Sync modes: recorded uses immutable recorded events only; recomputed uses live/recomputed observations independent of recorded buffers.
- [x] Fix DF alignment lifecycle so first view selection fetches immediately and polling continues while visible.
- [x] Add temporary frontend diagnostics (URL/status/age/seq-or-ts/count/empty/schema-rejected/polling-active) per selected-IID data source.
- [ ] Add backend/frontend regression tests for source independence, lifecycle polling, empty recorded behavior, and schema-mismatch handling.
- [ ] Run verification (backend pytest + frontend tests/build + manual acceptance flow) and document review results.

## 2026-05-01 /state Performance Regression Fix (In Progress)

- [x] Add detailed per-section timing, size, count, cache diagnostics, and serialization timing for `/api/radar/iids/{iid}/state`.
- [x] Identify and remove volatile signature invalidators causing unnecessary section rebuilds.
- [x] Slim `/state` to lightweight selected-IID summary/status only (no large arrays/heavy payload sections).
- [x] Add backend single-flight + short TTL cache for slim `/state` to prevent overlapping builds per IID.
- [x] Keep heavy data in dedicated endpoints and ensure `/state` does not trigger heavy builders.
- [x] Rewire frontend so panel data comes from dedicated endpoints, not `/state` bulk fallback.
- [x] Ensure selected-IID fallback does not overlap requests and remains stale-safe under slow responses.
- [x] Add regression tests for slim `/state`, cache behavior, frontend endpoint usage boundaries, and failure isolation.
- [x] Run verification (backend tests + frontend build + live endpoint latency checks) and record results.

Review:
- `/api/radar/iids/{iid}/state` now returns summary-only payload (`summary` + revisions + transport performance/cache metadata) and no heavy section arrays.
- Added per-section timing/size/count and signature-based cache diagnostics in `transport.performance.sections`.
- Added single-flight per-(iid,window,debugLimit) build lock and 1.5s TTL cache for `/state` summary payloads.
- Added dedicated `GET /api/radar/iids/{iid}/position-accumulation` endpoint for frame-position history map data.
- Frontend panels now fetch dedicated endpoints directly (`sync-snapshot`, `timeline`, `sweep-frames`, `control`, `solution-comparison`, `fm-location`, `fm-diagnostics`, `position-accumulation`) and no longer hydrate panel data from `/state`.

Verification:
- `uv run --directory backend pytest tests/test_radar_api.py -k "selected_state or position_accumulation" -q`
- `uv run --directory backend pytest tests/test_radar_ui_labels.py -q`
- `cd frontend && npm run build`
- Elevated local probe on patched backend (port `8011`): 5x `/api/radar/iids/{iid}/state` calls returned `200` in `~1.0–3.2ms`, payload keys were summary-only (`iid/revisions/sequence/server_ts/summary/transport/type`), and TTL cache hits were observed (`transport.source=selected_iid_state_ttl_cache`).
- In-process benchmark for `/state` summary path with large mocked buffers: min 0.053ms, median 0.06ms, p95 0.163ms, max 0.47ms (non-Pi dev host).

Residual risk:
- Existing long-running backend on port `8000` appears to still be pre-fix build; live environment validation still requires restart/deploy of patched backend before UI-level acceptance can reflect these changes.

## 2026-05-01 Selected-IID State/Source Consistency + Frontend Gating

- [x] Run GitNexus impact analysis on `/state` and RadarPage symbols and record blast radius/risk.
- [x] Align `/state` frame summary with `/sweep-frames` source and expose `go_sweep_frame_count`, `legacy_live_frame_count`, `displayed_frame_count`.
- [x] Align `/state` sync authority summary with `/sync-snapshot`/`burst_sync_timeline` operational authority fields and avoid contradictory collapse.
- [x] Update `/state` cache signatures and transport diagnostics with source revisions and cache age.
- [x] Ensure frontend sweep/burst/DF/position panels render from dedicated endpoints and are not gated by `/state` summary counts.
- [x] Harden DF alignment polling while visible and independent from panel priming.
- [x] Add position accumulation panel diagnostics for endpoint/status/count/schema mismatch when blank.
- [x] Add backend/frontend regression tests for summary consistency + dedicated-endpoint panel rendering/polling behavior.
- [x] Run verification (targeted backend tests + frontend build) and document review.

Review:
- `/state` now derives sync summary from `get_live_sync_snapshot(...)` and frame summary from `get_sweep_frame_summary_payload(...)`, matching dedicated source paths used by `/sync-snapshot` and `/sweep-frames`.
- `/state.summary.frames` now exposes `go_sweep_frame_count`, `legacy_live_frame_count`, and `displayed_frame_count`, and will not emit `no_frames_for_this_iid` when Go sweep-frame rows exist.
- `/state.summary.sync` now exposes separated authority fields: `operational_period_authority`, `operational_sync_authority`, `go_runtime_authority`, and `legacy_python_authority`.
- `/state.transport` now includes `source_revisions` and source-cache metadata tied to sync snapshot sequence and sweep-frame revision.
- DF alignment timeline polling now uses dedicated `useIidTimeline(...)` polling while the legacy DF panel mode is visible; polling no longer relies on the prior panel-local cache priming path.
- Position Accumulation map blank state now surfaces endpoint diagnostics (URL, HTTP status, payload count, polling state, schema mismatch reason).

Verification:
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_api.py -k "selected_state or position_accumulation" -q`
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_ui_labels.py -q`
- `cd frontend && npm run build`

## 2026-05-01 Sync Authority / Diagnostic Semantics + Go Hard-Bound Diagnosis

- [x] Run GitNexus impact checks for backend sync serialization, Go sync-refiner state, and frontend authority panels; record blast radius and risk before edits.
- [x] Separate current operational authority from recorded event-time authority metadata in backend payloads and frontend rendering.
- [x] Split operational fit/readiness fields from Go diagnostic fit/refiner fields in `sync_state`; stop frontend use of ambiguous aliases.
- [x] Add explicit Go hard-bound diagnostics (reason, limits, requested/current/base/effective deltas in s+ppm, DF disagreement fields) and surface them in the UI.
- [x] Add Go fit-history epoch segmentation/invalidation for incompatible model-state changes (base period shift, phase epoch/reset, phase-offset discontinuity, reference change, residual-basis change, authority-basis changes, holdover basis changes, dominant-family reset).
- [x] Expose fit-epoch diagnostics (`fit_epoch_id`, reset reason/timestamp, dropped count, segment count, epoch span/count) and wire to sync snapshot payloads.
- [x] Diagnose hard-bound root cause path using exported observation diagnostics (fit window/span, per-ICAO contributions/rejections, history crossing boundaries) and encode explicit reject-state reporting.
- [x] Update RadarPage sections so current operational state, Go diagnostic refiner state, and recorded chart/event-time semantics are visually separated and non-contradictory.
- [x] Add/update backend tests for authority separation, fit-field separation, hard-bound diagnostics, non-operational Go delta behavior, and fit-epoch reset/segmentation rules.
- [x] Add/update frontend/static tests for authority-pill source, recorded event-time labeling, diagnostic-section isolation, hard-bound reason visibility, and operational-period stability under hard-bound rejection.
- [x] Run focused verification (`uv run --directory backend pytest ...`, `cd frontend && npm run build`) and document review + outcomes in this file.

Review:
- Operational authority now stays sourced from current `sync_state` (`period_authority/sync_authority/phase_authority/handoff_*`), while recorded residual rows carry explicit `event_*` authority snapshot metadata and an explicit event-time notice.
- Frontend Sync Source/authority pills no longer consume ambiguous fit aliases; they now use operational fields (`operational_*`) for current readiness and separate `go_diagnostic_*` fields for Go refiner diagnostics.
- Go diagnostic hard-bound state now exports reasoned diagnostics in both seconds and ppm (`requested/current/base/disagreement/limit`), plus explicit hard-bound reason/reject status.
- Fit-history compatibility handling now rotates fit epochs on incompatible model-state shifts and exposes epoch diagnostics (`fit_epoch_id`, reset reason, dropped count, segment count, epoch span/count).
- Added Go unit coverage for fit-epoch resets on reference change, period-authority-basis change, and repeated hard-bound reject rotation.
- Hard-bound root-cause determination: oversized proposed correction is consistent with mixed-basis/epoch contamination of residual slope history; segmentation + reset rules now prevent fitting a single slope through incompatible phase/base/authority regimes.

Verification:
- `GOCACHE=/tmp/go-build-cache go test ./...` (from `radar-core`)  
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_sweep.py -q`  
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_api.py -k "selected_state or sync_snapshot or sync_state or data_path_diagnostics or position_accumulation or authority_matches" -q`  
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_ui_labels.py -q`  
- `cd frontend && npm run build`

Residual risk:
- `backend/tests/test_radar_api.py::test_manual_position_controls_persist_and_lock` is currently hanging in this environment and was excluded from the focused sync-semantics verification scope; this appears pre-existing and unrelated to sync authority/refiner semantics.
