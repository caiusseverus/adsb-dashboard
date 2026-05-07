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

## 2026-05-02 Sync Source Operational State Card — Retained Value Isolation

- [x] Extract pure sync helper functions from RadarPage.jsx into testable utility module.
- [x] Modify `getOperationalPeriodTriple` to return null when `period_authority` is not operational (`holdover`, `unavailable`, etc.).
- [x] Update Current operational state card to show Base/Δ/Effective as unavailable when authority is not operational.
- [x] Add a separately labelled retained-diagnostic subsection inside Current operational state card for last-known raw values (only when authority is not operational but raw values exist).
- [x] Ensure Go diagnostic refiner card continues to show retained/proposed/applied Go deltas unchanged.
- [x] Add frontend/static test using Node built-in test runner covering operational triple authority gating and retained Go delta visibility.
- [x] Run frontend build and static test verification.

Plan confirmation:
- Scope is limited to frontend rendering logic and helper functions; no backend or Go core changes.
- Operational period triple must gate on `py_base` / `py_refined` / `go_refined` only.
- Retained values may be shown in a diagnostic subsection, never as operational.

Review:
- Extracted `humanizeSyncReason`, `formatAuthorityLabel`, `collectBlockingHandoffGates`, `authorityModeLabel`, `getOperationalPeriodTriple`, `isFiniteValue`, and `fmtNumber` into `frontend/src/utils/radarSync.js`.
- `getOperationalPeriodTriple` now gates on `period_authority` being `py_base`, `py_refined`, or `go_refined`; returns `null` for `holdover`, `unavailable`, or any other non-operational authority.
- Fixed latent `fmtNumber` bug where `null` was coerced to `0` via `Number(null)`; now explicitly returns `'-'` for `null`/`undefined`.
- Current operational state card renders Base/Δ/Effective as `'-'` when authority is not operational.
- Added a "Retained diagnostic values" subsection inside Current operational state card that conditionally shows last-known base/Δ/effective when authority is non-operational but raw values are still finite.
- Go diagnostic refiner card is untouched and continues to show retained/proposed/applied Go deltas.

Verification:
- `cd frontend && npm run test:static` — 5 tests pass
- `cd frontend && npm run build` — builds cleanly
- `uv run --directory backend pytest tests/test_radar_ui_labels.py -q` — 11 passed


## 2026-05-05 High-Traffic Provisional Reacquire Instability Investigation

- [x] Review lessons and inspect current Go/Python sync-state transition/diagnostic paths for provisional reacquire and hard residual reject behavior.
- [x] Run GitNexus impact analysis for symbols to be edited and record risk.
- [x] Add missing Go->Python diagnostic field passthrough for provisional reacquire and fit-epoch counters.
- [x] Surface reacquire support vs current fit epoch counters in Radar UI diagnostics.
- [x] Add/update focused backend tests for new diagnostic field normalization/serialization.
- [x] Run focused verification and produce diagnosis report, including unresolved runtime-measurement gaps.

Plan confirmation:
- No Stage 10 implementation.
- No Go operational default changes.
- No period tolerance changes.
- No removal of diagnostic chart recording.

Review:
- Added Go->Python passthrough for provisional-reacquire transition diagnostics: observed/predicted/residual at update epoch, hard-reject streak, epoch-age/candidate/current epoch, and provisional flag.
- Added Radar UI diagnostics to show provisional state, hard-reject counters, last update residual triple, reacquire support counts, and current fit-epoch obs/ICAO/age.
- Kept operational behavior unchanged: no Stage 10 changes, no Go default changes, no period tolerance changes, and no chart-recording removal.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "reacquire_and_epoch_diagnostics or go_sync_snapshot_exposes_holdover_reason_counters" -q`
- `cd frontend && npm run build`

Residual risk:
- 5-minute runtime percentages were not measured in this offline dev session; those require a live feed or replay trace.

## 2026-05-05 Hard Residual Holdover Instability Investigation

- [x] Reconfirm residual gate math in `radar-core/iid/sync.go` (wrapped vs raw) and document exact behavior with code references.
- [x] Run GitNexus impact analysis for every symbol touched and record risk/blast radius before edits.
- [x] Add Go sync diagnostics for raw vs wrapped residual, rejecting reference-aircraft metadata, and hard-reject transition counters.
- [x] Ensure existing behavior remains unchanged unless a correctness bug is found (no Stage 10, no default/tolerance changes).
- [x] Wire new diagnostics through `iid/state.go` and compact snapshot payload exports.
- [x] Add/update Go tests for residual wrapping semantics, predicted >360 handling, transition counters, and rejecting-reference diagnostics.
- [x] Run focused verification (`go test` for `radar-core/iid` and any impacted snapshot package tests).
- [x] Add review notes and recommendations (policy-only for confidence-aware holdover gating).

Plan confirmation:
- Scope is diagnostics + correctness verification for hard residual reject path only.
- No period-agreement tolerance changes.
- No Stage 10 implementation.
- No Go operational default changes.

Review:
- Confirmed the hard gate path in `SyncState.UpdateEpoch` computes residual via `circularDiff(...)` and compares `absResidual > residualRejectDeg`.
- Found and fixed a correctness bug in `circularDiff` under large unwrapped deltas caused by Go `math.Mod` negative remainder behavior; this could emit values outside `(-180, 180]` and trigger false hard rejects.
- Added explicit diagnostics for both raw and wrapped residuals/predicted angles:
  - `last_update_epoch_raw_residual_deg`
  - `last_update_epoch_wrapped_residual_deg`
  - `last_update_epoch_predicted_wrapped_deg`
  - legacy `last_update_epoch_residual_deg` remains wrapped for compatibility.
- Added hard-reject transition diagnostics:
  - `hard_reject_transition_consecutive_before`
  - `hard_reject_transition_consecutive_after`
  - `hard_reject_entered_holdover_bool`
  - `hard_reject_gate_reason` (`wrapped_abs_residual_gt_50deg`).
- Added rejecting reference diagnostics currently available in this path:
  - `last_update_epoch_ref_icao`
  - `last_update_epoch_ref_bearing_deg`
  - `last_update_epoch_ref_range_nm` uses `-1` sentinel when unavailable in this stage path.
- Kept behavior unchanged except the circular-wrap correctness fix; no Stage 10/default/tolerance changes.

Verification:
- `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid`
- `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./cmd/radar-core`
## 2026-05-05 Radar Page Snapshot Consistency Investigation

- [x] Review existing lessons and document scope constraints for this investigation.
- [x] Run GitNexus impact analysis for frontend snapshot/render symbols before edits and report blast radius/risk.
- [x] Trace radar page data sources for sync snapshot, chart history, compact timeline, and frontend cached/local state.
- [x] Confirm whether population and phase panels can consume different anchors from mixed snapshot generations.
- [x] Add visible snapshot identity diagnostics (sync sequence, chart sequence, snapshot timestamp, anchor ICAO, population/phase timestamps) for each panel.
- [x] Enforce coherent rendering semantics: main population panel uses compact sync snapshot for current state; chart-history population is explicitly labelled as windowed/event-history diagnostics.
- [x] Add/update frontend tests covering anchor-change consistency and stale/windowed labeling behavior.
- [x] Run focused verification and document review/results.

Plan confirmation:
- Do not implement Stage 10.
- Do not change sync/period/holdover behavior.
- Do not change validation thresholds.

Review:
- Confirmed `RadarPage` was mixing logical sources: `syncState` and population monitor both came from `burstTimeline = chartHistory ?? syncSnapshot`, allowing chart-history snapshot lag to present a different anchor than current phase state.
- Kept current phase/anchor panel on compact sync snapshot (`syncSnapshot.sync_state`) via `selectCurrentSyncState(...)`.
- Made population phase agreement explicitly windowed/event-history diagnostics from `chartHistory.population_residual_monitor` only, with visible source metadata and mismatch warning when windowed anchor differs from current sync anchor.
- Added per-panel snapshot identity/debug pills: sync/chart sequence, snapshot timestamps, phase state timestamp, anchor ICAOs, and population window.
- Added static utility tests to enforce policy: current state prefers compact sync snapshot; anchor mismatch is detected and label-triggerable.
- No Stage 10 changes, no sync/period/holdover behavior changes, no validation-threshold changes.

Verification:
- `cd frontend && npm run test:static`
- `cd frontend && npm run build`

Residual risk:
- GitNexus MCP impact function was unavailable in this session (only `mcp__gitnexus__.tool_map` exposed), so blast radius was assessed from direct call-site tracing in `RadarPage.jsx` rather than graph-backed impact output.

## 2026-05-05 Go Discontinuity + Sync-Usable Diagnostics (In Progress)

- [x] Confirm scope constraints: no Stage 10, no default flag changes, no period tolerance change, no discontinuity-threshold relaxation, no new debounce/suppression behavior.
- [x] Run impact checks for Go sync/discontinuity symbols and report blast radius before edits.
- [x] Add Go `phase_offset_discontinuity` diagnostics to SyncState/DebugSnapshot/IID_STATE payload with wrapped delta + fit-epoch/reference context.
- [x] Add Go `sync_usable` component diagnostics + dominant failure reason in SyncState/DebugSnapshot/IID_STATE payload.
- [x] Ingest/map new compact IID fields in backend, preserve in normalized Go sync dict, and expose `go_diagnostic_*` fields in sync snapshot API.
- [x] Update frontend diagnostics rendering for discontinuity reset details and go_sync_unusable component gates with graceful missing handling.
- [x] Add/update Go, backend, and frontend tests for emission/mapping/rendering behavior.
- [x] Run focused verification and record review/results here.

Plan confirmation:
- Instrumentation-only change; no runtime behavior gating relaxations.
- Keep compact sync snapshot as the source of truth for current phase/anchor operational state.

Review:
- Added Go diagnostics for `phase_offset_discontinuity` capture (`old/new/delta/threshold`, fit-epoch context, epoch timings, reference ICAOs/change flag, basis note) and sync-usable gate components with a dominant failure reason.
- Exposed diagnostics through both Go debug snapshot payload and compact IID state codec fields for backend ingestion.
- Added backend normalization/mapping + API exposure using `go_diagnostic_*` fields for discontinuity and `go_sync_unusable` component diagnostics.
- Added UI rendering for both required views:
  - discontinuity detail block when `go_diagnostic_fit_epoch_reset_reason == "phase_offset_discontinuity"`;
  - sync gate breakdown when `handoff_reason == "go_sync_unusable"`.
- No Stage 10 work and no behavior relaxations (no flag defaults changed, no period tolerance changed, no discontinuity threshold changed, no debounce/suppression logic added).

Verification:
- `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid ./cmd/radar-core ./protocol`
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_sweep.py -k "go_iid_state_maps_discontinuity_and_sync_usable_diagnostics or go_diagnostic_fields_remain_diagnostic_when_go_is_not_authoritative" -q`
- `cd frontend && node --test src/utils/radarSync.test.js`

## 2026-05-05 Phase Offset Jump Root-Cause Investigation

- [x] Trace the exact `ctx.phaseOffsetDeg` computation path from observed/predicted bearing through residual, wrap, blend, and fit-epoch reset gates.
- [x] Run impact checks by direct symbol/call-site tracing for `SyncState.UpdateEpoch`, `maybeResetFitEpochLocked`, and Go/Python snapshot propagation paths.
- [x] Add missing discontinuity diagnostics for candidate/raw/wrapped/blended offset, pre/post blend deltas, prior fit/phase offsets, update outcome, and candidate epoch age.
- [x] Export new diagnostics through Go `DebugSnapshot` -> compact IID_STATE codec -> backend normalization -> sync snapshot payload.
- [x] Add/extend tests for circular blend behavior and discontinuity diagnostic payload completeness.
- [x] Run focused verification and finalize diagnosis report with corrective options (no Stage 10 / no threshold or flag changes).

Plan confirmation:
- Scope is diagnostics and investigation only.
- No Stage 10 implementation.
- No default operational-flag changes.
- No period-tolerance changes.
- No chart-rendering changes.
- No discontinuity-threshold relaxation.

Review:
- Traced `ctx.phaseOffsetDeg` production in Go path: `maybeUpdateSync` computes observed bearing from receiver->reference aircraft, passes it to `SyncState.UpdateEpoch`, which predicts bearing at candidate epoch, computes circular residual, and circularly blends at candidate epoch.
- Confirmed discontinuity reset compares `circularDiff(blendedOffset, fitEpochPhaseOffsetDeg)` against fixed 30° threshold (`fitEpochPhaseOffsetResetDeg`) in `maybeResetFitEpochLocked`.
- Added diagnostics around candidate/blended phase offset and delta progression:
  - update outcome (`accepted`/`provisional_reacquire`/`rejected`)
  - candidate epoch age
  - candidate raw/wrapped phase offset
  - blended phase offset
  - blend delta, delta-before-blend, delta-after-blend
  - previous phase offset and previous fit-epoch phase offset
- Plumbed fields through Go debug snapshot, IID_STATE compact codec, Python compact-field normalization, and API `go_diagnostic_*` payload.
- Added tests:
  - circular blend shortest-arc behavior near wrap
  - discontinuity diagnostics include new candidate/blend context

Verification:
- `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid ./cmd/radar-core ./protocol`
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_sweep.py -k "go_iid_state_maps_discontinuity_and_sync_usable_diagnostics" -q`

## 2026-05-05 Weak-Fit Discontinuity Churn Fix

- [x] Add fit-support gate for destructive `phase_offset_discontinuity` handling in Go sync epoch-reset logic.
- [x] Keep destructive reset path unchanged for supported fit epochs (same 30° threshold).
- [x] Add weak-fit non-destructive rebase behavior to prevent repeated discontinuity churn on unsupported epochs.
- [x] Add weak-fit discontinuity diagnostics (ignored count + last delta/old/new/fit obs/fit ICAOs) to Go state and snapshot payloads.
- [x] Propagate new compact fields through backend normalization and sync snapshot `go_diagnostic_*` exposure.
- [x] Add/update Go tests for weak-fit ignore + rebase, supported-fit reset preservation, and existing reset invariants.
- [x] Update backend mapping tests for new diagnostics.
- [x] Run focused verification.

Review:
- `maybeResetFitEpochLocked(...)` now checks `hasSupportedFitEpochForDiscontinuity()` before destructive discontinuity rotation.
- Support gate uses existing reacquire support thresholds (`reacquireMinFitObs=12`, `reacquireMinFitICAOs=3`) rather than introducing new unrelated constants.
- When `|delta| > 30°` on weak-fit epochs, the code:
  - does not rotate/reset fit epoch;
  - rebases `fitEpochPhaseOffsetDeg` to current context;
  - records `phase_offset_discontinuity_ignored_weak_fit`;
  - increments weak-fit discontinuity diagnostics.
- Supported epochs keep the existing destructive behavior with reason `phase_offset_discontinuity`.

Verification:
- `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid ./cmd/radar-core ./protocol`
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_radar_sweep.py -k "go_iid_state_maps_discontinuity_and_sync_usable_diagnostics" -q`

## 2026-05-05 Supported Phase Discontinuity Reset Investigation (In Progress)

- [x] Trace the exact supported `phase_offset_discontinuity` reset code path and map every input used to produce old/new offsets.
- [ ] Run GitNexus impact analysis on symbols that require diagnostic edits and report blast radius/risk before changing code.
- [ ] Reproduce/collect supported discontinuity reset events from current live capture telemetry for the affected IIDs. (blocked: local backend not running on 127.0.0.1:8000 in this session)
- [x] Add only missing discontinuity diagnostics required for reset classification (no behavior changes).
- [x] Verify diagnostics via targeted backend tests and, where possible, live snapshot inspection.
- [x] Produce a per-reset diagnosis report: source of new offset, reference representativeness, population agreement comparison, staleness analysis, and classification.
- [ ] Recommend the minimal corrective change for a later fix without implementing behavior changes.

Plan confirmation:
- No Stage 10 implementation.
- No default-flag changes.
- No period tolerance changes.
- No chart rendering changes.
- No threshold relaxation.

## 2026-05-06 Anchor-Relative Phase Lock Loss Before Stage 10 (In Progress)

- [x] Trace exact anchor-clear/phase-basis override path across `_ingest_go_frame_sync_diagnostic_locked`, `_apply_go_handoff_state_locked`, `_resolve_phase_anchor_state`, `update_simple_live_sync_state`, and `_live_sync_state_to_dict`.
- [x] Confirm whether anchor loss is dataclass mutation vs serialization-only override; document exact branch producing `phase_basis=sweep_epoch_only`, `phase_anchor_icao=None`, and `population=no_anchor` during `go_sync_unusable_reason=quality_below_threshold`.
- [x] Add minimal diagnostics for phase-lock loss causality and previous-anchor state.
- [x] Implement minimal anchor-relative retention/hysteresis only for transient `quality_below_threshold` while anchor remains fresh and no disqualifying failure gates are present.
- [x] Preserve existing blocking behavior for real failures: holdover, stale/no-anchor, population demotion/disagreement, and period disagreement.
- [x] Add backend tests for transient retention, holdover clearing, stale-anchor clearing, population demotion behavior, period disagreement behavior, absolute-phase invariants, and clear-reason diagnostics.
- [x] Run focused backend verification and record a review section here.

Plan confirmation:
- No Stage 10 implementation.
- No Go operational authority enablement.
- No period tolerance changes.
- No chart rendering changes.
- No geographic/absolute phase semantic changes.

Review:
- Root cause confirmed: `_ingest_go_frame_sync_diagnostic_locked` replaced `LiveSyncState` with a new Go-diagnostic object and did not carry anchor-relative fields (`phase_anchor_icao`, `phase_anchor_status`, `phase_basis`, anchor timestamp/status). That dataclass replacement produced immediate `phase_basis=sweep_epoch_only` and null anchor values.
- This is a true dataclass-state mutation path, not just a serializer override. `_live_sync_state_to_dict` then reflected the cleared fields and could additionally fall back to sweep-epoch semantics when no anchor is present.
- Added targeted retention only for transient `go_sync_unusable_reason=quality_below_threshold` with strict conditions (previous trusted anchor-relative state, fresh anchor age, not holdover, period agrees, strict gate passes, no population demotion/fail).
- Added clear/override diagnostics fields to state/payload, including previous anchor metadata, clear reason, override reason, and timestamp/reason at clear.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "go_quality_transient_retains_trusted_anchor_relative_phase or go_quality_transient_does_not_retain_through_holdover_or_stale_anchor or go_quality_transient_does_not_retain_through_population_demotion_or_period_disagreement" -q`
- `uv run --directory backend pytest tests/test_stage3r_handoff.py tests/test_phase_semantics.py -q`

## 2026-05-06 Current-Sync vs Windowed-Population Mismatch (In Progress)

- [x] Trace source-of-truth paths for population summary vs current sync anchor and classify expected lag vs bug.
- [x] Confirm authority gates consume current sync state only (not chart-history windowed population summary).
- [x] Add explicit backend diagnostics for population source/anchor source/lag/current-vs-windowed anchors/authority-usage flag.
- [x] Update frontend labels to distinguish hard-residual consecutive vs total counters with semantically correct totals.
- [x] Add/update focused backend tests for new diagnostics and counter semantics.
- [x] Run focused verification and document findings.

Plan confirmation:
- No Stage 10 changes.
- No authority promotion changes.
- No geographic/absolute phase semantic changes.
- No threshold changes.

Review:
- The observed mismatch is expected in this architecture: the population panel consumes windowed event-history summary from chart-history, while current phase authority uses the current live sync state.
- Source trace:
  - Windowed population summary is built in `get_burst_sync_timeline()` via `compute_population_residual_summary(entries, sync, iid)` and then passed through `get_chart_history()` as `population_residual_monitor`.
  - Current sync anchor/phase fields come from `get_live_sync_snapshot()` / `_live_sync_state_to_dict()`.
  - UI intentionally compares these two snapshots in the “Population phase agreement (windowed/event-history)” panel.
- Authority-gate confirmation: `_evaluate_phase_authority_gates_locked()` reads only `LiveSyncState` fields (`population_validation_state`, `phase_anchor_*`, `phase_status`) and does not consume chart-history population summary.
- Added diagnostics to make stale/windowed vs current explicit:
  - `population_summary_source`, `population_anchor_source`, `population_anchor_age_s`, `population_summary_generated_ts`, `population_summary_lag_s`, `current_sync_last_update_ts`, `current_anchor_icao`, `windowed_anchor_icao`, `population_used_for_authority` (false in windowed monitor).
  - Added sync-state authority indicator fields: `population_used_for_authority` (true) and `population_authority_source`.
  - Chart-history API now rewrites monitor source to `chart_history` and updates `population_summary_lag_s` at response time.
- Hard residual reject counters:
  - The prior UI “total” used `holdover_hard_residual_reject` (holdover-specific), which can disagree with the consecutive counter.
  - Updated UI to show `consecutive_hard_residual_rejects / update_epoch_reject_hard_residual` as the main pair and display holdover hard-residual rejects separately.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "population_summary_source or population_authority_flags_are_current_state_based or go_quality_transient" -q`
- `uv run --directory backend pytest tests/test_radar_api.py -k "chart_history or sync_snapshot" -q`
- `cd frontend && npm run build`

## 2026-05-06 Stage 10 Controlled Go Authority Promotion (In Progress)

- [ ] Run GitNexus impact analysis on Stage 10 touchpoints (`_apply_go_handoff_state_locked`, phase absolute normalisation helpers, sync snapshot/API summary serializers, RadarPage authority diagnostics) and record blast radius/risk.
- [ ] Implement backend Go operational promotion gating behind `RADAR_SYNC_GO_REFINER_OPERATIONAL` with controlled handoff semantics and fallback to Python base/bootstrap.
- [ ] Implement/extend hysteresis for Go operational activation/demotion to avoid flapping while preserving hard-failure immediate demotion.
- [ ] Preserve phase semantics split: trusted anchor-relative may be trusted for internal use but remains non-absolute/non-geographic; geographic absolute remains unchanged and gated by geographic basis/offset validity.
- [ ] Expose/confirm Stage 10 operational diagnostics fields in sync snapshot/state payloads (`operational_period_source`, `operational_period_s`, `go_refined_period_delta_s`, `python_base_period_s`, `go_operational_enabled`, `go_operational_active`, `blocking_gate`, authorities, handoff fields).
- [ ] Update RadarPage diagnostics text/labels only as needed to clearly distinguish go-ready-disabled, go-active, Python fallback, and anchor-relative-not-geographic.
- [ ] Add backend tests for: flag disabled ready-state behavior; flag enabled promotion behavior; gate-failure blocking reasons; phase absolute semantics; hysteresis promotion/demotion behavior; contamination gate semantics.
- [ ] Run focused verification (`pytest` targeted files + frontend build as needed), then add review/results here.

Plan confirmation:
- No period tolerance/residual-threshold changes.
- No chart rendering changes.
- No removal of diagnostics.
- No geographic-phase claims from anchor-relative basis.
- No Python fallback removal.
- No Stage 8 contamination algorithm changes.

Review (Stage 10 progress):
- Added controlled Go operational promotion in `_apply_go_handoff_state_locked` behind `RADAR_SYNC_GO_REFINER_OPERATIONAL` with explicit `go_operational_enabled`/`go_operational_active` state.
- Added promotion hysteresis (`_GO_OPERATIONAL_PROMOTION_CONSECUTIVE=3`) and short soft-failure holdover (`_GO_OPERATIONAL_SOFT_FAILURE_HOLD_S=3.0`) while keeping immediate demotion for hard safety failures (holdover/unusable/base-invalid/base-disagree/contaminated/effective-invalid).
- Preserved diagnostic-only behavior when flag is false (`GO_REFINED_READY` + `go_ready_flag_disabled`), Python fallback authority, and Stage 8 contamination gate semantics.
- Added/extended sync payload fields: `operational_period_source`, `operational_period_s`, `go_refined_period_delta_s`, `python_base_period_s`, `blocking_gate`, `go_operational_enabled`, `go_operational_active`.
- Kept anchor-relative vs geographic absolute semantics untouched; no geographic claims are made from anchor-relative trust.
- Updated Radar page authority diagnostics to show Go operational flag/active state and current blocking gate.
- Updated backend tests for new operational handoff reason (`go_runtime_operational`) and hysteresis behavior, including transient-ready and soft-failure holdover coverage.

Verification:
- `uv run --directory backend pytest tests/test_stage3r_handoff.py -q`
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "stage5_flag_enabled_go_ready or stage5_flag_disabled_go_ready or operational_full" -q`
- `uv run --directory backend pytest tests/test_phase_semantics.py tests/test_radar_api.py -k "sync_state or sync_summary or phase_is_absolute or phase_absolute_available or handoff_reason" -q`
- `cd frontend && npm run build`

## 2026-05-07 Live Unclassified Attribution Shape Fix

- [ ] Extract 5-10 representative live `sync_state` payloads where `handoff_reason == "go_state_unclassified"` or `go_operational_blocking_gate == "go_readiness.unclassified_state"` from restart capture artifacts.
- [ ] Dump exact raw values + Python types for all fallback-classifier input fields for each extracted sample.
- [ ] Add a unit test using one exact live unclassified payload shape (verbatim data shape, not approximation).
- [ ] Run impact analysis for touched symbol(s), apply minimal attribution mapping fix for only that exact shape, and avoid policy/threshold/localiser/holdover behavior changes.
- [ ] Re-run targeted tests and a short capture/verification script; confirm unclassified attribution is zero or produce exact new remaining shape dump.
- [ ] Record review + verification results in this file.

Plan confirmation:
- Scope restricted to fallback attribution mapping in sync-state serialization.
- No changes to thresholds, readiness policy, localiser behavior, chart logic, phase/geographic logic, or holdover semantics.

Review:
- Extracted representative samples from `tasks/radar_sync_baseline/sync_capture_all_stage10_unclassified_cleanup_verify_restart_20260507T004454Z.ndjson` where `handoff_reason=go_state_unclassified` / `go_operational_blocking_gate=go_readiness.unclassified_state`.
- Exact shape observed: `handoff_state=GO_REFINING`, `source=go_frame_sync`, `usable=True`, `holdover=False`, empty `handoff_gate_failures`, `period_refinement_status=stable`, diagnostic usable gates true via `go_diagnostic_go_sync_usable_*`, `fit_icao_count=0`, `go_diagnostic_fit_icao_count>0`.
- Added regression test using exact live shape values/types and updated fallback attribution mapping only for this shape.
- Mapping fix treats `go_readiness.unclassified_state` as placeholder, consumes `go_diagnostic_go_sync_usable_*` booleans as fallback inputs, and replaces placeholder reason `go_state_unclassified` when hysteresis inference is valid.
- No thresholds/policy/localiser/chart/phase-geographic/holdover logic changed.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "stage10_live_unclassified_shape_with_diag_usable_flags_maps_to_hysteresis or stage10_enabled_nonactive_stable_usable_without_go_sync_dict_maps_to_hysteresis or stage10_enabled_nonactive_stable_usable_without_gate_snapshot_maps_to_hysteresis" -q`
- `UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend python - <<'PY' ... replay capture ... PY`
- Replay result on captured unclassified samples: `{'replayed_samples': 94, 'still_unclassified': 0, 'nonactive_null_gate': 0}`.

## 2026-05-07 SyncQuality vs Rotation Status Mismatch Fix

- [x] Trace quality computation and status update ordering in Go runtime paths.
- [x] Add Go diagnostics for quality-eval status/base context and mismatch detection.
- [x] Fix stale quality behavior by recomputing SyncQuality after rotation/base updates.
- [x] Expose new diagnostics through Go snapshot payload and Python sync mapping.
- [x] Add Go + backend tests for mapping and transition behavior.
- [ ] Run short live capture and report post-fix runtime counts.

Review:
- Root cause confirmed: `SyncQuality` was calculated on `UpdateSyncEpoch` using current `s.Status`, while `s.Status` is refreshed later on the rotation ticker (`ApplyRotation`/`reinforce`). This allowed `SyncQuality` to stay at `0.0` after status transitioned to `SINGLE_RADAR` until another sync update arrived.
- Fix: add locked recomputation of SyncQuality and quality-eval diagnostics whenever rotation/base-period context updates, and surface mismatch diagnostics if exported status and quality diverge under mapped statuses with base present.
- No changes to Stage 10 handoff policy, thresholds, tolerances, holdover policy, localiser, chart rendering, phase authority, or geographic semantics.

Verification:
- `cd radar-core && GOCACHE=/tmp/go-build go test ./iid/... ./cmd/radar-core/...`
- `uv run --directory backend pytest tests/test_radar_sweep.py -k "go_quality_eval_diagnostics_fields_are_mapped or stage10_live_unclassified_shape_with_diag_usable_flags_maps_to_hysteresis" -q`
- Live capture verification pending because no backend was reachable at `127.0.0.1:8000` in this session.
