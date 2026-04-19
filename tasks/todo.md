# Deficiency Rectification Plan

Source inputs:
- `.docs/development/deficiency-remediation.md`
- `.docs/development/combined-deficiency-report.md`
- `.docs/development/polish-and-features-proposal.md`

Prepared: 2026-04-04
Updated: 2026-04-06

## 2026-04-19 Live Sync/Alignment Retention Truncation Fix

- [x] Confirm root cause in live sync/alignment buffers and request window mismatch
- [x] Implement time-window-first retention with high safety caps for `_live_aligned_burst_obs` and `_live_burst_timeline_obs`
- [x] Add per-IID retention diagnostics (counts, oldest/newest burst timestamp, effective duration)
- [x] Expose diagnostics through live sync timeline/snapshot/debug payloads
- [x] Add focused backend tests for retention behavior and diagnostics fields
- [x] Run targeted backend verification and document results

Plan confirmation: proceed with a bounded time-based retention strategy plus high count caps, preserving existing sync math/diagnostics paths while ensuring requested 60–90s windows are genuinely retainable at operational burst rates.

### Review

- Root cause:
  - Confirmed `_live_aligned_burst_obs` and `_live_burst_timeline_obs` were both bounded to `200` entries (`_MULTI_SYNC_OBS_MAX` / `_BURST_SYNC_TIMELINE_OBS_MAX`), so retention was effectively count-limited rather than time-window-limited.
  - Timeline/debug/snapshot payloads filter by requested `window_s` (60/90/etc) but can only return what remains in those deques, so larger requests could not be satisfied when burst rates were high.
- Implementation:
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) to use a time-first strategy with safety caps:
    - `self._LIVE_SYNC_OBS_RETENTION_S = 360.0`
    - `self._MULTI_SYNC_OBS_MAX = 6_000`
    - `self._BURST_SYNC_TIMELINE_OBS_MAX = 8_000`
  - Added active pruning on append for both buffers via `_prune_live_sync_observation_buffer(...)`.
  - Added per-IID retention diagnostics (`count`, `oldest_burst_centroid_us`, `newest_burst_centroid_us`, `retained_duration_s`, cap) and exposed them in:
    - `get_burst_sync_timeline(...)`
    - `get_live_sync_snapshot(...)`
    - `get_sync_debug_payload(...)` (top-level and summary)
  - Added/updated focused tests in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) and [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) for pruning behavior and payload diagnostics.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_live_sync_observation_buffers_prune_by_age_with_high_count_caps tests/test_radar_sweep.py::test_get_burst_sync_timeline_includes_non_sync_driving_observations tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation tests/test_radar_sweep.py::test_live_sync_snapshot_reuses_cached_payload_until_sync_inputs_change tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation tests/test_radar_api.py::test_get_iid_sync_snapshot_endpoint_combines_fast_sync_payloads`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
- Result: all tests passed (`6 passed` focused, `96 passed` combined suite).

## 2026-04-19 Stage 3/4 Live Radar Pipeline Recovery (Go Frames + Sync Debug)

- [x] Trace current Go/Python frame and sync-debug paths end-to-end and identify concrete blocking gates
- [x] Add Go per-IID frame-accumulator gate diagnostics to radar-core snapshot payloads
- [x] Add Python/API per-IID pipeline-debug payload combining Go snapshot + Python live state
- [x] Fix Go reference-selection mismatch that can block frame start (`ref_not_dominant`)
- [x] Fix Go-frame mode sync bootstrap starvation so `_live_sync_states` can populate from `inject_frame_from_go()`
- [x] Add focused backend + Go unit coverage for new diagnostics and sync/bootstrap behavior
- [x] Run targeted verification and document results

### Review

- Root causes addressed:
  - Go frame start could be starved when Stage-3 reference selection picked a non-dominant ICAO while Stage-4 frame start still required dominant-family acceptance (`ref_not_dominant` gate).
  - In `RADAR_CORE_FRAMES_ENABLED` mode, Python suppressed local frame finalization and did not bootstrap live sync state from Go `FRAME_READY`, leaving `_live_sync_states` empty and causing sync snapshot/debug payloads to stay empty by design.
- Implementation:
  - Go:
    - Added `Accumulator.Diagnostics()` with per-gate counters/state in [accumulator.go](/home/keith/claude/adsb-dashboard/radar-core/frame/accumulator.go).
    - Added IID debug snapshot and dominant-family-constrained reference selection in [state.go](/home/keith/claude/adsb-dashboard/radar-core/iid/state.go).
    - Extended snapshot payload generation with per-IID state + accumulator gates in [main.go](/home/keith/claude/adsb-dashboard/radar-core/cmd/radar-core/main.go).
  - Python:
    - Added periodic radar-core snapshot requests and storage in [client.py](/home/keith/claude/adsb-dashboard/backend/radar_core/client.py).
    - Bootstrapped live sync from Go `FRAME_READY` path and tracked per-IID Go injections in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py).
    - Added per-IID debug API endpoint `/api/radar/iids/{iid}/pipeline-debug` and radar-core stats provider registration in [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) and [main.py](/home/keith/claude/adsb-dashboard/backend/main.py).
  - Tests:
    - Added Go tests in [state_test.go](/home/keith/claude/adsb-dashboard/radar-core/iid/state_test.go).
    - Added backend tests in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py), [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py), and [test_radar_core_client.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_core_client.py).
- Verification:
  - `uv run --directory backend pytest tests/test_radar_core_client.py tests/test_radar_sweep.py::test_radar_core_frame_injection_populates_fm_mailbox_and_completed_buffer tests/test_radar_sweep.py::test_radar_core_frame_injection_bootstraps_live_sync_state_when_missing tests/test_radar_api.py::test_get_iid_pipeline_debug_combines_python_and_go_diagnostics tests/test_radar_api.py::test_get_iid_pipeline_health_reports_live_sweep_frames`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `python3 -m py_compile backend/radar_core/client.py backend/radar/sweep.py backend/radar/api.py backend/main.py backend/tests/test_radar_core_client.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - Result: targeted tests passed (`9 passed`), radar sweep/API suites passed (`98 passed`), Python compile checks passed.
  - Limitation: Go toolchain is unavailable in this environment (`go`/`gofmt` not installed), so Go tests/format checks could not be executed here.

## 2026-04-19 Radar-Core Integration Rectification

- [x] Refactor `RadarCoreClient` to maintain one shared bidirectional Unix socket for send and receive, with coordinated reconnect and non-blocking enqueue semantics
- [x] Move radar-core `POSITION_UPDATE` forwarding from the once-per-minute DB writer to a bounded live cadence with freshness/confidence filtering and deduplication
- [x] Add lightweight `FRAME_READY` observability in Go emission, Python reception, and Python FM-mailbox injection paths, including cheap frame-gate reason categories
- [x] Add focused regression tests for single-socket behavior, live position forwarding, and frame observability/injection counters
- [x] Run focused backend and radar-core verification, then document results

Plan confirmation: proceeding directly because the user provided a concrete implementation brief and explicitly constrained the scope. The changes stay within the existing one-client radar-core model, preserve the Python hot-path queue boundary, and avoid redesigning frame ownership.

### Review

- Implementation:
  - Refactored [client.py](/home/keith/claude/adsb-dashboard/backend/radar_core/client.py) so sender and receiver share one bidirectional Unix socket, one connection generation, and one coordinated teardown/reconnect path. The hot radar path remains queue-only and non-blocking.
  - Moved radar-core position forwarding out of `_db_writer()` in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) and into a 2-second live loop with confident/fresh-position filtering, movement/altitude dedupe, and a 10-second refresh to keep radar-core's 30-second position cache warm.
  - Extended [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) lightweight position snapshots with confidence, freshness, and altitude fields needed by the live radar-core feed.
  - Added `FRAME_READY` visibility: Go logs emitted frames and frame accumulator gate categories in [accumulator.go](/home/keith/claude/adsb-dashboard/radar-core/frame/accumulator.go); Python client stats now include frames received and latest Go health; RadarState tracks Go-frame injection successes/errors; `/api/status` exposes radar-core runtime stats.
  - Added focused tests in [test_radar_core_client.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_core_client.py), [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py), and strengthened Go frame payload assertions in [accumulator_test.go](/home/keith/claude/adsb-dashboard/radar-core/frame/accumulator_test.go).
- Verification:
  - `python3 -m py_compile backend/radar_core/client.py backend/aircraft_state.py backend/main.py backend/radar/sweep.py backend/status.py backend/tests/test_radar_core_client.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_core_client.py tests/test_radar_sweep.py::test_radar_core_frame_injection_populates_fm_mailbox_and_completed_buffer tests/test_radar_sweep.py::test_radar_core_frames_enabled_suppresses_python_fm_mailbox_injection`
  - `uv run --directory backend pytest tests/test_radar_core_protocol.py tests/test_radar_core_client.py tests/test_radar_sweep.py`
  - `uv run --directory backend pytest`
  - Result: focused tests passed (`6 passed`), radar-core/radar sweep backend set passed (`90 passed`), full backend passed (`346 passed`). Go-side `gofmt`/`go test` could not be run because `go`/`gofmt` are not installed in this environment.

## 2026-04-19 Radar Page Lazy Loading Removal

- [x] Inspect Radar page lazy-mounted panels and frame-selection gates
- [x] Mount radar panels directly instead of viewport-triggered lazy sections
- [x] Default sweep-frame geometry diagnostics to the newest displayed frame
- [x] Build frontend and document verification

Plan confirmation: proceeding directly because the request is a concrete UI workflow fix. Scope is limited to removing Radar page lazy mounting and the extra frame-click requirement for default geometry diagnostics.

### Review

- Implementation:
  - Removed the Radar page `LazyMountSection` wrapper in [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx).
  - Mounted Sweep Frames, Evidence Map, and Position Verification directly so they start loading when an IID is selected rather than waiting for viewport intersection.
  - Changed Sweep Frames to default the detail table and frame-geometry diagnostics to the newest displayed good/marginal frame when no frame is manually selected.
- Verification:
  - `npm run build`
  - Result: frontend build passed with the existing chunk-size warning.

## 2026-04-19 Frame Geometry OSM Map Fix

- [x] Inspect Frame FM geometry basemap configuration and Leaflet tile-layer options
- [x] Fix OSM tile layer creation so subdomains are never passed as undefined
- [x] Build frontend and document verification

Plan confirmation: proceeding directly because this is a concrete browser error with a narrow root cause in the Frame FM geometry map.

### Review

- Implementation:
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so Frame FM geometry tile layers only pass `subdomains` to Leaflet when the selected basemap defines it. This fixes OSM, whose URL uses `{s}` but had no `subdomains` option configured.
- Verification:
  - `npm run build`
  - Result: frontend build passed with the existing chunk-size warning.

## 2026-04-19 Radar BurstRecord Refactor Rectification

- [x] Add native fired-burst `BurstRecord` emission with fields matching the Python fallback path
- [x] Clear `_burst_records` from per-IID and global reset paths and expose it in reset accounting
- [x] Preserve dwell replies only through diagnostics-gated retention and make lookup use that source after sweep reconstruction
- [x] Add `_burst_records` counts to memory observability
- [x] Add focused regression tests and run backend verification

Plan confirmation: proceeding directly because the requested scope is concrete bug rectification. The patch is limited to the backend radar state implementation and focused backend tests.

### Review

- Implementation:
  - Added native fired-burst `BurstRecord` emission in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) with `iid`, `icao`, `centroid_us`, `n_replies`, and `signal_dbfs`, matching the Python fallback record shape.
  - Added diagnostics-gated completed dwell reply retention in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so `get_dwell_profile()` can serve reply profiles after sweep history is rebuilt from `_burst_records`. When diagnostics are disabled or no retained replies match, dwell returns an empty list.
  - Updated reset paths in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) to clear `_burst_records` for one IID or all IIDs, with `reset_all()` reporting the cleared burst-record count.
  - Updated memory observability in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) with total burst records, IID count, max per IID, and average per non-empty IID.
  - Added focused regression coverage in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py).
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_fired_burst_processing_emits_burst_records_and_diagnostic_dwell tests/test_radar_sweep.py::test_dwell_profile_uses_diagnostic_replies_after_burst_record_rebuild tests/test_radar_sweep.py::test_memory_stats_include_burst_records tests/test_radar_sweep.py::test_reset_iid_clears_in_memory_learning_state tests/test_radar_sweep.py::test_reset_all_clears_sync_refinement_state`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - Result: focused tests passed (`5 passed`), radar sweep/API tests passed (`93 passed`), full backend passed (`308 passed`).

## 2026-04-17 Radar Sync Stream and Period Correction Diagnosis

- [x] Inspect RadarPage sync polling, existing websocket/fallback hooks, radar API routes, and live sync state maintenance
- [x] Add a compact per-IID sync snapshot that reuses one backend build for rotation, burst residuals, and compact sync-debug fields
- [x] Add a pushed `/ws/radar/iids/{iid}/sync` feed with heartbeat/sequence diagnostics and an HTTP fallback snapshot
- [x] Replace default RadarPage fast sync polling with one pushed sync subscription and keep legacy/advanced data lazy or low-rate
- [x] Split period update diagnostics into proposed/applied delta, gain, ppm, allowed/block reason, clamp reason, support, span, and status
- [x] Verify slope-to-period conversion, fit gates/clamps, and refined period propagation through predictor paths; fix implementation defects found
- [x] Add focused backend/frontend validation and document results

Plan confirmation: proceeding without a separate confirmation stop because the task explicitly says not to ask for confirmation. Scope is limited to RadarPage sync transport/load and period-correction diagnosis/fix.

### Review

- Implementation:
  - Added `RadarState.get_live_sync_snapshot()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), with a per-IID signature cache and sequence counter so websocket and HTTP fallback clients reuse one combined sync snapshot when live inputs have not changed.
  - Added `/api/radar/iids/{iid}/sync-snapshot` and `/ws/radar/iids/{iid}/sync`, combining rotation, burst residual timeline, sync state, waveform/anchor fields, period histories, and sync-debug data for the default RadarPage sync panels.
  - Replaced the default RadarPage `rotation`, `burst-sync-timeline`, and `sync-debug` 1.5s polling path with one `useRadarSyncStream()` subscription shared by Burst Sync Alignment and Position Verification. The legacy alignment timeline remains opt-in and poll-driven only when selected.
  - Split period-correction state into proposed/applied seconds and microseconds, unclamped/applied ppm, allowed/block/clamp reasons, fit support/span, adaptive clamp limits, and `period_correction_status`.
  - Verified the slope conversion sign was already correct. The practical convergence blocker was opaque and tight clamping: clamp reasons were stored as block reasons, and the base-period clamp could cap total correction even under persistent strong-fit slope. Strong, persistent multi-aircraft fits now get wider safety clamps while weak fits keep the conservative limits.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/main.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_period_refinement_uses_effective_time_slope_and_correct_sign tests/test_radar_sweep.py::test_live_sync_snapshot_reuses_cached_payload_until_sync_inputs_change tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation tests/test_radar_api.py::test_get_iid_sync_snapshot_endpoint_combines_fast_sync_payloads`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: focused backend tests passed (`4 passed`), radar sweep/API suites passed (`90 passed`), full backend passed (`305 passed`), frontend build succeeded with the existing chunk-size warning.

## 2026-04-17 Radar Sync Detrended Phase-Shape Diagnostics

- [x] Inspect sync residual generation, period-fit slope basis, sync-debug API payload, and Radar page diagnostic rendering
- [x] Add diagnostic-only detrended residual fields using the same Beast-time slope basis as period refinement
- [x] Add folded phase-in-rotation and per-cycle metadata plus phase-bin/repeatability summaries
- [x] Add a compact dominant-error-mode classification for period drift vs repeatable phase shape vs unstable cycle shape
- [x] Simplify the default Radar sync-debug UI to summary, raw residual vs time, detrended residual vs phase, and folded per-sweep overlay
- [x] Move low-value existing sync diagnostics behind an advanced diagnostics toggle
- [x] Add/update focused tests and run backend/frontend verification

Plan confirmation: implement a focused diagnostics-only pass. The operational solver, period refinement, phase anchor, waveform learning, propagation correction, and motion compensation behavior remain unchanged; new fields explain the existing residuals on the same Beast-time basis.

### Review

- Implementation:
  - Added diagnostic-only detrending helpers in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), using a weighted residual-vs-effective-Beast-time slope over the displayed sync-debug rows and leaving solver state unchanged.
  - Extended sync-debug observations with `residual_raw_deg`, `fit_slope_deg_per_s`, `time_offset_s`, `detrend_component_deg`, `residual_detrended_deg`, `phase_deg`, `cycle_index`, and `cycle_start_beast_us`.
  - Added folded phase-shape diagnostics with phase-binned median detrended residual, bin spread, per-cycle shape error, repeatability score, and compact rule-based `dominant_error_mode`.
  - Replaced the default Radar sync-debug panel in [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) with compact summary pills, raw residual vs elapsed Beast time, detrended residual vs phase, and folded per-sweep overlay.
  - Moved timestamp/predictor checks, observation rows, burst-method summary, and per-aircraft quality into an explicit advanced diagnostics toggle.
  - Added focused payload assertions in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) and [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py).
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: focused tests passed, radar sweep/API tests passed (`88 passed`), full backend passed (`303 passed`), frontend build succeeded with the existing chunk-size warning.

## 2026-04-17 Radar Sync Consistency Diagnostic

- [x] Inspect current backend predictor, burst-sync timeline, localiser, and Radar page wiring
- [x] Add a dedicated per-IID sync-debug backend payload comparing one observation across all sync-sensitive paths
- [x] Add summary consistency metrics for predictor deltas, time-basis deltas, roundtrip errors, fit support, and correction decomposition
- [x] Expose the sync-debug payload through `/api/radar/iids/{iid}/sync-debug`
- [x] Add a Radar page diagnostic panel with predictor table, residual-vs-effective-time, ICAO-relative residuals, timestamp-basis comparison, and phase decomposition
- [x] Add/update focused backend coverage for payload fields and operational Beast-time rules
- [x] Run backend/frontend verification and record results

Plan confirmation: implement a focused diagnostics-only patch. Operational prediction remains Beast-relative and uses `effective_beast_us` after propagation correction when enabled; wall-clock prediction is exposed only as a diagnostic comparison.

### Review

- Implementation:
  - Added `RadarState.get_sync_debug_payload()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) to emit per-observation sync-debug rows with raw arrival Beast time, burst-centre Beast time, effective Beast time, wall timestamp, propagation delay, truth bearing, all requested predictor outputs, residuals, deltas, fit eligibility, phase decomposition, and wall-to-Beast diagnostic error.
  - Added `/api/radar/iids/{iid}/sync-debug` in [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py).
  - Added `predict_localiser_live_path_bearing()` in [aircraft_localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/aircraft_localiser.py) so the sync-debug payload can compare the localiser-live path through backend code.
  - Added `SyncDebugPanel` to [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) with predictor consistency table, residual-vs-effective-Beast plot, ICAO-relative residual plot, timestamp-basis comparison, and phase decomposition.
  - Added focused coverage in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) and [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py).
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/radar/aircraft_localiser.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `npm run build`
  - Result: backend focused tests passed, radar test pair passed (`83 passed`), full backend passed (`298 passed`), frontend build succeeded with the existing large-chunk warning.

## 2026-04-17 Radar Observation Model Diagnostics

- [x] Inspect current burst finalization, sync-debug payload, localiser predictor, and Radar page sync-debug UI
- [x] Add diagnostic-only burst timestamp candidates without changing the operational burst timestamp
- [x] Extend sync-debug observations with burst shape, signal, position timing, and per-method residual fields
- [x] Add backend summaries by timestamp method, fit-driving subset, quality bins, position age, bearing rate, range, and ICAO
- [x] Add an operator-facing Observation model diagnosis section with method comparison, per-aircraft consistency, and compact characteristic plots/tables
- [x] Add/update focused backend tests for candidate fields, Beast-only residual comparisons, summaries, and diagnosis flags
- [x] Run backend and frontend verification, then document results

Plan confirmation: implement this as diagnostics only. Keep the sync solver and operational burst timestamp definition unchanged; compare alternate Beast-relative burst timestamp definitions against the authoritative predictor and expose which observation subgroup explains the residual spread.

### Review

- Implementation:
  - Added diagnostic-only burst timestamp candidates in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py): first reply, strongest reply, simple centroid, weighted centroid, mid-strong-window midpoint, and last reply, all exposed as `*_beast_us` without changing the operational burst timestamp.
  - Extended sync-debug observations with per-method predicted/residual fields, residual-improvement fields, burst shape/signal metadata, position age/interpolation/extrapolation/source-age fields, and Beast-equivalent truth-position timestamp when source age is known.
  - Added `observation_model_diagnostics` summaries for overall, fit-driving, and high-quality method comparisons; signal/width/reply-count/position-age/bearing-rate/range bins; correlations; per-ICAO consistency; and rule-based likely contributors.
  - Added an Observation model diagnosis block to [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) with method comparison, per-aircraft table, residual-vs-characteristic plots, residual-improvement plot, residual-vs-phase small multiples, and split-bin summaries.
  - Added focused assertions in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) and [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py).
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/radar/aircraft_localiser.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_detect_bursts_with_signals_refines_beam_center_toward_stronger_replies tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `npm run build`
  - Result: focused tests passed, radar API/sweep tests passed (`84 passed`), full backend passed (`299 passed`), frontend build succeeded with the existing large-chunk warning.

## 2026-04-17 Radar Observation Model Diagnostics Backend Fix

- [x] Inspect native and Python burst-building paths for candidate timestamp retention
- [x] Add backend diagnostic reply-retention for the native burst path without changing operational burst timestamps
- [x] Enrich fired burst observations with real candidate timestamps before sync-debug recording
- [x] Mark method availability/unavailability explicitly in observation-model summaries
- [x] Add focused tests that fail when candidate methods remain zero-count on native-style burst processing
- [x] Run focused and broader verification, then record results

Plan confirmation: repair backend diagnostic data population only. The operational solver keeps using the current burst timestamp; this fix makes the comparison payload honest and populated.

### Review

- Implementation:
  - Added a diagnostic-only reply mirror for native burst processing in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), so live native-fired bursts retain per-reply Beast timestamps and signal values for observation-model diagnostics.
  - Enriched native `fired_bursts` with first/strongest/simple/weighted/mid-strong-window/last timestamp candidates before `_process_fired_bursts()` records sync-debug observations. The operational `burst_centroid_us` from the native processor is not changed.
  - Added a one-reply native fallback where the native burst output itself is sufficient to identify first/simple/last and signal-based methods.
  - Updated method summaries to compare only paired observations where both operational and candidate residuals exist, and added `available_burst_timestamp_methods`, `unavailable_burst_timestamp_methods`, and `method_unavailable_reasons`.
  - Added a native-style regression test in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) that fails if candidate residuals remain zero-count on the native burst path.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_native_burst_path_populates_observation_model_timestamp_candidates tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - Result: focused tests passed, radar API/sweep tests passed (`85 passed`), full backend passed (`300 passed`).

## 2026-04-17 Radar Sync Aircraft Motion Compensation

- [x] Inspect current authoritative sync predictor, burst observation capture, period fit, localiser, diagnostics payloads, and Radar page rendering
- [x] Add explicit bearing-rate and motion-compensation helpers with reliability gates and Beast-relative timing fields
- [x] Route authoritative prediction, burst residual generation, period/phase fitting, sync-debug, and live localiser construction through the motion-capable predictor
- [x] Add per-observation and summary before/after residual diagnostics, including high-rate splits and per-aircraft summaries
- [x] Add Radar page diagnostics for residual-vs-bearing-rate, residual improvement, summary pills, and per-aircraft before/after table
- [x] Add/update focused backend tests for motion compensation, fallbacks, and diagnostic fields
- [x] Run focused backend tests, full backend tests if feasible, frontend build, and record results

Plan confirmation: implement motion compensation as an optional first-order timing correction on Beast-relative burst-centre observations. Use bearing-rate estimates only when the observation already has trustworthy radar-to-aircraft geometry, keep propagation and motion terms separate in diagnostics, and fall back to the current model when motion quality gates fail.

### Review

- Implementation:
  - Added motion-compensation feature flags in [config.py](/home/keith/claude/adsb-dashboard/backend/config.py): `RADAR_SYNC_MOTION_COMP_PHASE_ENABLED` and `RADAR_SYNC_MOTION_COMP_FIT_ENABLED`.
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) with finite-difference bearing-rate estimation, first-order `Δt = θdot / 360 * T²` timing correction, reliability block reasons, and explicit propagation-vs-motion Beast timestamp fields.
  - Extended `predict_sync_observation()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so burst-sync residuals, period/phase fitting, sync-debug, and Stage 3 live localiser calls can use the same motion-capable authoritative path.
  - Added before/after residual diagnostics, motion applied/blocked fields, high-rate residual summaries, and per-aircraft motion summaries to burst timeline and sync-debug payloads.
  - Updated [aircraft_models.py](/home/keith/claude/adsb-dashboard/backend/radar/aircraft_models.py) and [aircraft_localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/aircraft_localiser.py) so live detections carry motion fields into the shared predictor.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) with motion summary pills, residual-vs-bearing-rate, residual improvement, and per-aircraft before/after diagnostics.
  - Added focused assertions in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) and [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py).
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/radar/aircraft_localiser.py backend/radar/aircraft_models.py backend/tests/test_radar_sweep.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_authoritative_sync_predictor_applies_motion_compensation tests/test_radar_sweep.py::test_get_sync_debug_payload_compares_predictor_paths_on_same_observation tests/test_radar_api.py::test_get_iid_sync_debug_endpoint_exposes_summary_and_observation`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `npm run build`
  - Result: focused tests passed, radar API/sweep tests passed (`84 passed`), full backend passed (`299 passed`), frontend build succeeded with the existing chunk-size warning.

## 2026-04-17 Radar Sync Compliance Gap Remediation

- [x] Unify sync-sensitive frontend verification with the authoritative refined live sync model (period/phase/waveform/prop-delay basis)
- [x] Ensure `reset_iid()` and `reset_all()` clear all new refinement/waveform/throttle/quality state
- [x] Remove premature internal period rounding in aggregate/base estimation and reinforcement paths
- [x] Add minimal burst-centre estimator comparison diagnostics to burst-sync timeline payloads
- [x] Run focused + full backend tests and frontend build verification

Plan confirmation: apply a focused backend/frontend corrective patch only; preserve existing operational behaviour except where required for sync-model consistency and precision retention.

### Review

- Implementation:
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) period/base estimator internals to keep full precision (removed early rounding in harmonic folding, candidate scoring, and period reinforcement paths).
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) burst-sync timeline prediction to call the shared authoritative sync predictor helper, keeping diagnostics aligned with the refined model.
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) reset paths to clear newly added per-IID refinement state (`_live_waveform_bins`, `_live_icao_sync_quality`, `_last_multi_sync_update_ts`) and related derived state (`_rotation_analysis_meta`, per-IID live detections, dirty IID flag, pending FM mailbox entry).
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) burst-centre diagnostics to expose simple centroid vs amplitude-weighted centre fields (`burst_center_simple_us`, `burst_center_weighted_us`, `burst_center_delta_us`, `burst_center_method`) in burst-sync timeline observations.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `ReceiverCentredRadarField` to use the authoritative refined sync predictor for beam sweep and DF11 residual timing classification (with waveform + propagation correction), with frame-anchor retained only as explicit fallback.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `RotationAlignmentPanel` raw DF11 residual dots to use the same refined predictor basis for consistency.
  - Updated [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) with reset-state and burst-centre-diagnostic assertions.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/radar/aircraft_localiser.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: backend `292 passed`; frontend build succeeded (existing chunk-size warning remains).

## 2026-04-16 Radar Sync Visualisation Purpose Split Follow-Up

- [x] Inspect current `RotationAlignmentPanel` and `ReceiverCentredRadarField` wiring against intended panel roles
- [x] Add live DF11 timing overlay to Burst Sync Alignment while preserving burst residual/classification diagnostics
- [x] Change Burst Sync Alignment x-axis to a shared rolling time window usable by both burst and raw DF11 overlays
- [x] Restore Position Verification to raw-DF11-first rendering and demote/remove burst-centre markers from primary display
- [x] Update panel legends/copy so burst-vs-raw roles are explicit and accurate
- [x] Run focused frontend verification and record results

Plan confirmation: apply a frontend-focused corrective patch only; keep burst-sync backend route/model unchanged.

### Review

- Implementation:
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `RotationAlignmentPanel` to add a second live source via `useTimingEventStream` + `useTimingEventBuffer`, producing a faint raw DF11 timing tick overlay while keeping burst residual/classification points as primary diagnostics.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `RotationAlignmentPanel` x-axis to a shared rolling time window (`windowStartUs`/`windowEndUs`), so burst points and raw DF11 timing markers are co-plotted in one timeline.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `ReceiverCentredRadarField` to remove burst-centre primary drawing and restore raw DF11 arrivals as the dominant visual layer for beam-vs-reply checks.
  - Updated `ReceiverCentredRadarField` metrics/legend/copy to describe raw DF11 as primary and remove burst inlier/outlier prominence from this panel.
- Verification:
  - `npm run build`
  - Result: frontend build succeeded (existing chunk-size warning remains).

## 2026-04-16 Burst-Centre Sync Visualisation Wiring Rectification

- [x] Inspect frontend and backend radar timeline/sync routes and identify legacy wiring points
- [x] Add or expose a dedicated burst-sync timeline API route path for frontend consumption
- [x] Update `RotationAlignmentPanel` to poll burst-sync timeline data and render residual/classification observations
- [x] Update `ReceiverCentredRadarField` so burst-centre observations are the primary sync verification layer, with raw DF11 optional secondary context
- [x] Update panel labels/legend text to clearly distinguish burst-sync observations from raw message diagnostics
- [x] Run focused backend/frontend verification and record results

Plan confirmation: proceeding with a targeted wiring rectification only (no unrelated radar refactors), preserving legacy timeline/raw-event endpoints as secondary diagnostics.

### Review

- Implementation:
  - Updated [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) to expose a frontend-friendly burst-sync timeline alias route at `/api/radar/iids/{iid}/burst-sync-timeline` while retaining the existing underscore route.
  - Updated [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so `get_burst_sync_timeline()` observations now include `range_nm` derived from radar-to-aircraft geometry, enabling polar-field projection from burst-centre observations.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so `RotationAlignmentPanel` now polls burst-sync timeline data and renders a residual-vs-time burst-sync scatter (inlier/soft/rejected), replacing legacy raw timeline/websocket wiring.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so `ReceiverCentredRadarField` now uses burst-centre observations as the primary verification overlay, computes beam residual checks at `beam_center_us`, and renders raw DF11 events as a faint secondary diagnostic layer.
  - Updated panel titles, legend chips, and explanatory text in [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) to explicitly distinguish burst-sync verification data from raw message context.
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/radar/sweep.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py`
  - `npm run build`
  - Result: backend `73 passed in 0.61s`; frontend build succeeded (existing chunk-size warning remains).

## 2026-04-13 All-Pairs Circle Solver Follow-Up Refactor

- [x] Inspect the current backend all-pairs circle path, endpoint rejection, and payload diagnostics
- [x] Replace final all-pairs cluster ordering with support-primary, pairwise-residual-aware ranking
- [x] Update single-frame result selection to use pair-based support/fit metrics
- [x] Add admitted/inlier pair-circle payload rows and pair summary counts while retaining compatibility fields
- [x] Narrow endpoint rejection to aircraft relevant to the intersecting circles and align pair midpoint bearing convention
- [x] Update the radar localisation UI to display pair identities, admitted/inlier status, pair metrics, and summary counts
- [x] Run focused backend/frontend verification and record results

Plan confirmation: proceeding with this targeted refactor against the existing all-pairs implementation, preserving compatibility fields where current callers still use them.

### Review

- Implementation:
  - Updated [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) with a pairwise candidate residual scorer and changed all-pairs intersection cluster selection to rank primarily by circle support, using pairwise residual RMS only as a secondary criterion.
  - Updated [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so single-frame direction/result selection uses support, inlier pair count, pairwise RMS, and cluster RMS rather than the old reference-based fit score.
  - Updated [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so endpoint rejection checks only the endpoint aircraft attached to the two circles producing a candidate intersection, and pair midpoint bearing now uses geographic `_bearing_deg`.
  - Added admitted and inlier pair-circle payload rows with both ICAOs, score components, sigma/baseline/phase diagnostics, normalized residuals, and pair summary counts while retaining compatibility fields such as `n_contributing_arcs`, `n_selected_observations`, and `fit_score`.
  - Updated [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) so the sweep-frame FM geometry endpoint renders admitted pair baselines/circles from the solver payload instead of rebuilding reference-observation geometry.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so the frame geometry diagnostics table and map toggles are keyed by aircraft pairs, show both ICAOs, expose admitted/inlier state, and display pair summary counts.
  - Updated [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) for the new pair-circle debug payload.
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/radar/forward_model.py`
  - `uv run --directory backend pytest tests/test_radar_api.py::test_get_iid_sweep_frame_fm_geometry_returns_pair_circles tests/test_radar_api.py::test_get_iid_sweep_frame_fm_geometry_returns_frame_estimate_overlay`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_circle_scorer.py tests/test_forward_model.py`
  - `uv run --directory backend pytest`
  - `npm run build`
  - Result: backend `286 passed in 1.54s`; frontend build succeeded with the existing large-chunk warning.

## 2026-04-13 Frame Geometry Basemap Selector

- [x] Add a basemap selector to the sweep-frame FM geometry Leaflet map
- [x] Verify the frontend build still passes

### Review

- Implementation:
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so the sweep-frame FM geometry Leaflet map has a basemap selector with Dark, Light, Street, and OSM options.
  - Kept Dark as the default and swapped the active Leaflet tile layer without recreating the map or geometry overlays.
- Verification:
  - `npm run build`
  - Result: frontend build succeeded with the existing large-chunk warning.

## 2026-04-13 Radar All-Pairs Circle Solver

- [x] Inspect the current inscribed-angle circle scorer, selector, and forward-model solver path
- [x] Refactor circle scoring to carry pair identities, pair quality weights, direct uncertainty penalties, and weak prior weighting
- [x] Change `_solve_by_intersection_attempt()` to generate all unordered aircraft-pair circles per frame and map selected geometry by `circle_index`
- [x] Replace greedy six-circle selection with capped pair admission and add selection diagnostics
- [x] Rank clusters with robust support from all admitted circles and derive frame quality metrics from inlier circles
- [x] Run focused backend tests and record the verification result

## 2026-04-13 Sweep Frame Map Frame Estimate Overlay

- [x] Inspect the sweep-frame geometry API payload and Leaflet renderer
- [x] Add the per-frame determined position and CEP circle to the frame geometry payload
- [x] Render the frame estimate point and CEP circle on the sweep frame map
- [x] Add/update focused tests and run backend/frontend verification

### Review

- Implementation:
  - Updated [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) so the sweep-frame FM geometry endpoint appends a `frame_estimate` point and a `frame_cep` circle feature when the per-frame solve passes the existing CEP/arcs gates.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so the Leaflet sweep-frame map renders the frame estimate point and the CEP circle independently of the aircraft-row highlight filter.
  - Updated [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) to assert the new overlay payload features.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_api.py::test_get_iid_sweep_frame_fm_geometry_returns_pair_circles tests/test_radar_api.py::test_get_iid_sweep_frame_fm_geometry_returns_frame_estimate_overlay`
  - `uv run --directory backend pytest tests/test_radar_api.py`
  - `npm run build`
  - Result: backend `33 passed in 0.47s`; frontend build succeeded with the existing large-chunk warning.

### Review

- Implementation:
  - Updated [circle_scorer.py](/home/keith/claude/adsb-dashboard/backend/radar/circle_scorer.py) so scored circles represent pair-derived constraints with stable `circle_index`, pair ICAOs, baseline, pair age/reply weights, direct uncertainty weighting, and weak prior weighting.
  - Updated [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so `_solve_by_intersection_attempt()` generates all unordered aircraft-pair circles per frame, uses signed phase for circle side selection while preserving folded `delta_phi` for scoring, maps admitted circles by `circle_index`, ranks supported clusters through the forward-model residual fit, and computes metrics from inlier circles.
  - Added endpoint-intersection rejection for shared-aircraft circle intersections and kept the rest of the frame accumulation and residual-fit pipeline intact.
  - Updated [test_circle_scorer.py](/home/keith/claude/adsb-dashboard/backend/tests/test_circle_scorer.py) for pair scoring and capped admission semantics.
- Constants introduced or retuned:
  - `SIGMA_BAND_REFERENCE_METRES = 3_000.0`
  - `MAX_PAIRS_PER_AIRCRAFT_PER_FRAME = 8`
  - `MAX_CIRCLES_PER_FRAME = 48`
  - `_ENDPOINT_INTERSECTION_REJECT_KM = 1.0`
  - `_INTERSECTION_SUPPORT_KEEP_FRACTION = 0.25`
- Assumptions:
  - The pair `sigma_band_metres` uses an unsigned timing-sensitivity approximation: `baseline * abs(delta_phi_uncertainty) / (2 * sin(delta_phi)^2)`, floored at `500 m`. This keeps the old geometric intent without the negative `cos(delta_phi)` behavior.
  - Sweep-frame references do not currently carry explicit reply/age metadata, so reference pair records use a conservative available/default reply count and age `0.0` unless future frame fields provide those values.
- Verification:
  - `python3 -m py_compile backend/radar/circle_scorer.py backend/radar/forward_model.py`
  - `uv run --directory backend pytest tests/test_circle_scorer.py tests/test_forward_model.py tests/test_forward_model_integration.py tests/test_forward_model_stage6.py`
  - `uv run --directory backend pytest`
  - Result: `285 passed in 1.56s`

## 2026-04-13 Remove Qwen Co-author Trailer From Dev Tip

- [x] Inspect the current `dev` tip and identify where the `qwencoder`/Qwen contributor reference is coming from
- [x] Rewrite only the latest `dev` commit metadata/message to remove the Qwen co-author trailer while preserving the existing author and tree
- [x] Verify the rewritten commit no longer contains the Qwen trailer and that the tree content is unchanged
- [x] Force-update `origin/dev` with lease so GitHub stops attributing the tip commit to that contributor
- [x] Add a review section with verification results

### Review

- Investigation:
  - The contributor reference is in the latest `dev` commit message trailer: `Co-authored-by: Qwen-Coder <qwen-coder@alibabacloud.com>`.
  - The author and committer are already `caiusseverus <7763268+caiusseverus@users.noreply.github.com>`.
  - Local `dev` and `origin/dev` both currently point at `dea4fe426347199450d8ddf56addddc305e6f9cd`.
- Implementation:
  - Rewrote the `dev` tip as `5fdd0501d7a8661dd01b338e9a9d1d293ff3d833`, removing only the co-author trailer from the commit message.
- Verification:
  - The commit hook ran `uv run --directory backend pytest`; result: `295 passed in 2.00s`.
  - The rewritten commit tree remains `f6fd3c0ce51ebbdbf68fd32994c5abcabf480203`, matching the original tree.
  - `git log -1 --format=%B dev` no longer contains the Qwen co-author trailer.
  - `git push --force-with-lease origin dev` updated GitHub from `dea4fe4` to `5fdd050`.
  - `git ls-remote origin refs/heads/dev` now reports `5fdd0501d7a8661dd01b338e9a9d1d293ff3d833`.

## 2026-04-12 Pi 5 Radar Rotation Backlog

- [x] Interpret the supplied Pi 5 debug sample and identify the active bottleneck
- [x] Inspect the radar rotation maintenance path and existing throttling/deferral behavior
- [x] Implement a bounded rotation-analysis pass that preserves dirty IID reprocessing without long Python CPU bursts
- [x] Add focused regression coverage for deferred IID handling
- [x] Run targeted backend tests and record the verification result

### Review

- Investigation:
  - The supplied Pi 5 sample shows the main pressure around radar rotation maintenance rather than websocket broadcast or raw native predecode: `radar_rotation_ms.sweep_build_avg=3062.06 ms`, `radar_loop_ms.update_avg=3412.43 ms`, and about `13843` recent events per update.
  - Decode and radar worker timings show high wall/off-CPU time while the queues are saturated, which is consistent with Python/GIL pressure from long background analysis bursts.
- Implementation:
  - [config.py](/home/keith/claude/adsb-dashboard/backend/config.py) adds `RADAR_UPDATE_BUDGET_MS`, defaulting to `750`.
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) passes that budget into the 30 second radar maintenance loop.
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now processes rotation analysis in oldest-last-updated IID order, stops after the runtime budget once at least one IID has been analysed, and requeues untouched dirty IIDs for later ticks.
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) also skips the redundant `sorted(...)` call in `detect_bursts()` when arrivals are already chronological, while preserving order-insensitive behavior for other callers.
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) exposes `radar_rotation_ms.deferred_iid_count_avg` so live samples can show whether the budget is actively spreading work across ticks.
  - [backend/.env.example](/home/keith/claude/adsb-dashboard/backend/.env.example) documents the new tuning knob.
- Expected live impact:
  - The next Pi 5 sample should show lower `radar_rotation_ms.sweep_build_avg` and lower `radar_loop_ms.update_avg` per maintenance tick, with a possible nonzero `deferred_iid_count_avg`.
  - This does not hide radar work under backlog skips; it keeps dirty IIDs queued and spreads the analysis across ticks to reduce long GIL-heavy bursts.
- Verification:
  - `python3 -m py_compile backend/config.py backend/main.py backend/debug.py backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `72 passed in 0.38s`

## 2026-04-12 Decoder Native Boundary Instrumentation

- [x] Compare the updated Pi 5 stats against the radar maintenance fix
- [x] Decide whether the next step should be instrumentation before a larger native rewrite
- [x] Add batch decoder phase timings for native predecode wall/cpu, lock wait, locked state application, and total batch wall/cpu
- [x] Expose the new timings in `/api/debug/perf`
- [x] Add focused regression coverage and run targeted backend verification

### Review

- Investigation:
  - The updated Pi 5 sample shows the radar maintenance fix helped, but the main message queue still spikes: `msg_queue_stats.p95=4888`, `msg_queue_depth=4515`, and `msg_drops_total=34625`.
  - The strongest remaining signal is off-CPU decoder batch time: `decoder_thread_ms.process_wall_avg=38.16 ms` versus `process_cpu_avg=3.1 ms`, plus a high `predecode_us.p95=2893.8`.
  - The current native path already decodes each message in C, but it still crosses the Python/CFFI boundary and allocates/converts a Python dict per message. Instrumentation is needed before deciding whether native batch predecode is the right larger change.
- Implementation:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now records `decoder_phase_timings` for each `process_messages_batch()` call, including batch size, predecode wall/cpu/off-CPU, lock wait, locked state-application wall/cpu/off-CPU, and total wall/cpu/off-CPU.
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) exposes the new `decoder_batch_phase_ms` block from `/api/debug/perf`.
  - [test_debug_perf.py](/home/keith/claude/adsb-dashboard/backend/tests/test_debug_perf.py) covers the new debug payload fields.
  - [test_aircraft_state_counts.py](/home/keith/claude/adsb-dashboard/backend/tests/test_aircraft_state_counts.py) covers recording a decoder batch phase sample.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/debug.py backend/tests/test_debug_perf.py backend/tests/test_aircraft_state_counts.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_aircraft_state_counts.py tests/test_decoder_batching.py`
  - Result: `7 passed in 0.50s`

## 2026-04-12 Native Batch Predecode

- [x] Review the live `decoder_batch_phase_ms` sample and confirm the bottleneck is predecode wall/off-CPU time
- [x] Add a native batch decode API that decodes one decoder batch in a single C call
- [x] Add a reusable Python CFFI batch wrapper with fallback to the existing per-message decode path
- [x] Wire `AircraftState.process_messages_batch()` to use the batch wrapper
- [x] Add regression coverage for the batch wrapper and decoder batch integration
- [x] Build native helpers and run focused/full backend verification

### Review

- Investigation:
  - The live `decoder_batch_phase_ms` sample showed the decoder batch was dominated by predecode wall/off-CPU time: `predecode_wall_avg=19.71 ms`, `predecode_cpu_avg=0.69 ms`, and `predecode_offcpu_avg=19.02 ms` for about `65` messages per batch.
  - Python state application was much smaller at `apply_wall_avg=2.36 ms`, so moving aircraft-state mutation native would be the wrong next target.
- Implementation:
  - [decode_api.h](/home/keith/claude/adsb-dashboard/backend/native/decode_api.h) and [decode_api.c](/home/keith/claude/adsb-dashboard/backend/native/decode_api.c) now expose `decode_messages_batch(...)`, a native batch wrapper over the existing `decode_message(...)` semantics.
  - [decode_cffi.py](/home/keith/claude/adsb-dashboard/backend/decode_cffi.py) adds `DecodeBatcher`, which reuses CFFI frame/result/status arrays and falls back to per-message decode if the native batch symbol is unavailable.
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) constructs one reusable batch decoder per `AircraftState` and uses it from `process_messages_batch()` while leaving single-message processing unchanged.
  - [test_decode_cffi_batch.py](/home/keith/claude/adsb-dashboard/backend/tests/test_decode_cffi_batch.py) verifies batch decode matches single-message decode and the fallback path.
  - [test_aircraft_state_counts.py](/home/keith/claude/adsb-dashboard/backend/tests/test_aircraft_state_counts.py) now patches the batch predecode hook in the decoder phase-timing test.
- Verification:
  - `make -C backend/native`
  - `python3 -m py_compile backend/decode_cffi.py backend/aircraft_state.py backend/debug.py backend/tests/test_decode_cffi_batch.py backend/tests/test_aircraft_state_counts.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_decode_cffi_batch.py tests/test_aircraft_state_counts.py tests/test_debug_perf.py tests/test_decoder_batching.py`
  - Result: `9 passed in 0.33s`
  - `uv run --directory backend pytest`
  - Result: `242 passed in 1.41s`

## 2026-04-12 Radar Worker Phase Instrumentation

- [x] Review the post-batch-predecode Pi 5 stats and confirm the bottleneck moved from the main decoder to the radar worker
- [x] Add radar batch phase timings around DF11 preparation, state append, flash grouping, native burst processing, and Python burst/frame processing
- [x] Expose the new radar worker phase metrics in `/api/debug/perf`
- [x] Add focused regression coverage and run targeted backend verification
- [x] Commit the instrumentation change

### Review

- Investigation:
  - After native batch predecode, the main decoder was healthy (`msg_queue_depth=0`, `msg_drops_total=0`), but the radar queue saturated: `radar_queue_depth=4872`, `radar_queue_stats.p95=4973`, and `radar_drops_total=13700`.
  - The active unknown is now inside radar-worker batch processing: `radar_worker_ms.process_wall_avg=67.93 ms`, `process_cpu_avg=3.66 ms`, and `process_offcpu_avg=64.27 ms`.
- Implementation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now records per-batch `df11_batch_phase_timings` around event preparation, shared-state append, flash grouping, native burst processing, Python burst/frame processing, and total wall/cpu/off-CPU time.
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) exposes these samples as `radar_worker_phase_ms` in `/api/debug/perf`.
  - [test_debug_perf.py](/home/keith/claude/adsb-dashboard/backend/tests/test_debug_perf.py) covers the new debug payload fields.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_radar_sweep.py`
  - Result: `40 passed in 0.45s`

## 2026-04-12 Fired Burst Phase Instrumentation

- [x] Add deeper timings inside `_process_fired_bursts()` to split the remaining `process_burst_avg` cost
- [x] Expose the new per-burst phase metrics in `/api/debug/perf`
- [x] Add focused regression coverage for the debug payload
- [x] Run targeted backend verification and record the result
- [x] Commit the instrumentation change

### Review

- Investigation:
  - The previous `radar_worker_phase_ms.process_burst_avg` metric still wrapped all of `_process_fired_bursts()`, so it could show that Python burst/frame processing was expensive without separating reference selection, ADS-B position lookup, phase-family checks, frame mutation, and per-frame FM callbacks.
- Implementation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now accumulates fired-burst phase metrics while processing native-fired bursts, including reference selection, position lookup, dominant-period checks, suppression checks, phase-family checks, frame mutation, finalization, and FM callback time/counters.
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) exposes these as `radar_fired_burst_phase_ms` in `/api/debug/perf`.
  - [test_debug_perf.py](/home/keith/claude/adsb-dashboard/backend/tests/test_debug_perf.py) covers the new debug payload fields.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_radar_sweep.py`
  - Result: `40 passed in 0.41s`
  - `uv run --directory backend pytest`
  - Result: `242 passed in 1.35s`

## 2026-04-12 Radar Reference Selection Throttle

- [x] Review the live fired-burst phase sample and identify the reference-selection hotspot
- [x] Reuse a recent current reference instead of rescoring on every fired burst
- [x] Add focused regression coverage for the reduced selection-call pattern
- [x] Run backend verification and record the result
- [x] Commit the change

### Review

- Investigation:
  - The Pi 5 sample shows radar worker saturation while the main decoder is healthy: `radar_queue_depth=5000`, `radar_drops_total=21950`, `msg_queue_depth=0`, and `msg_drops_total=0`.
  - The new fired-burst phase metrics isolate the hot path to reference selection: `radar_worker_phase_ms.process_burst_avg=49.77 ms` and `radar_fired_burst_phase_ms.reference_select_avg=47.79 ms`.
- Implementation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now reuses the current reference aircraft while its latest burst centroid is still within the same recency window used by the selector.
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) still forces a full rescore when there is no current reference or the current reference has gone stale.
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) adds `reference_reuse_count_avg` and `reference_rescore_count_avg` to `radar_fired_burst_phase_ms` so the next live sample can show whether repeated native scans were eliminated.
  - [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) covers the recent-reference reuse path and the stale-reference rescore path.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/debug.py backend/tests/test_debug_perf.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_radar_sweep.py`
  - Result: `42 passed in 0.46s`
  - `uv run --directory backend pytest`
  - Result: `244 passed in 1.36s`

## 2026-04-12 Position Verification Sync Stability

- [x] Review the live symptom and identify why the beam jumps while the sync ICAO label is stable
- [x] Keep the beam phase anchored to sweep-frame reference centroids instead of raw DF11 messages
- [x] Run frontend verification and record the result
- [x] Commit the change

### Review

- Investigation:
  - The new Pi 5 sample confirms the reference-selection throttle worked: radar queue and drop counters are healthy (`radar_queue_depth=0`, `radar_drops_total=0`), `reference_rescore_count_avg=0.06`, and `reference_select_avg=0.12 ms`.
  - The position verification canvas still re-anchored beam phase from raw live DF11 timing events for the sync ICAO. Those are message arrivals, not completed sweep-frame reference centroids, so the beam phase could jump while the displayed sync ICAO stayed the same.
- Implementation:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now keeps the beam phase anchored to the latest good/marginal sweep frame reference centroid.
  - Live DF11 events are still used to decide whether a sync ICAO has gone stale, but no longer overwrite `beamAnchorRef.current.ref_arrival_us`.
- Verification:
  - `npm run build`
  - Result: production build succeeded with the existing Vite large-chunk warning.

## 2026-04-12 Sweep Frame Builder Regression

- [x] Correct the reference-selection throttle so it does not suppress frame starts
- [x] Add regression coverage for frame construction under batched fired bursts
- [x] Run focused and full backend verification
- [x] Commit the fix

### Review

- Investigation:
  - The reference-selection throttle incorrectly reused the previous reference aircraft across batches while it was merely recent. Under live burst ordering, that can suppress frame starts when the old reference has not fired in the current batch.
- Implementation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now performs a normal reference selection once per `_process_fired_bursts()` call and reuses only that batch-local decision.
  - [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) covers the one-rescore-per-batch path and verifies a selected fired burst can start a live sweep frame.
  - [lessons.md](/home/keith/claude/adsb-dashboard/tasks/lessons.md) records the correction.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py`
  - Result: `42 passed in 0.38s`
  - `uv run --directory backend pytest`
  - Result: `244 passed in 1.53s`

## 2026-04-11 FM Frame Geometry Diagnostics

- [x] Inspect current SweepFrame and inscribed-angle evidence payloads
- [x] Add a backend endpoint for one frame's FM intersection geometry
- [x] Add a visual frame-detail panel showing aircraft, selected pairs, and resulting circles
- [x] Wire selection from the Sweep Frames panel into the diagnostic view
- [x] Verify backend tests and frontend build, then record results

### Review

- Implementation:
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes `/api/radar/iids/{iid}/sweep-frames/{frame_index}/fm-geometry`, returning the selected frame's aircraft points, reference-observation baselines, inscribed-angle circles, selection diagnostics, and per-observation drop reasons.
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now keeps the selected sweep frame in page state and renders `FrameGeometryDiagnostics` below the sweep-frame strip after a frame is clicked.
  - The frame diagnostic view supports CW/CCW direction toggles, highlights selected vs dropped pairs, draws the resulting circles, and includes a table with phase, phase strength, quality weight, position source, and drop reason.
  - Follow-up correction: the frame diagnostic now highlights a single aircraft's reference-pair circle when its row is clicked, and no longer caps the displayed circle radius. The earlier cap made large circles easier to fit but could make them visibly miss the aircraft points, which was misleading for geometry diagnosis.
  - Second correction: screen projection scales latitude and longitude independently, so geographic circles must render as projected ellipses in SVG. The diagnostic now uses projected x/y radii, and each aircraft row toggles its reference-pair circle on/off so the geometry can be built up one pair at a time.
  - [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) covers the new per-frame geometry endpoint.
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/tests/test_radar_api.py`
  - `npm run build`
  - Result: production build succeeded with the existing Vite large-chunk warning.
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_forward_model.py tests/test_radar_sweep.py tests/test_debug_perf.py`
  - Result: `115 passed in 0.48s`
  - Re-verified after the ellipse/toggle correction:
    - `npm run build`
    - `uv run --directory backend pytest tests/test_radar_api.py tests/test_forward_model.py tests/test_radar_sweep.py tests/test_debug_perf.py`
    - Result: production build succeeded; backend `115 passed in 0.47s`

## 2026-04-11 FM Solver Visibility Panel

- [x] Inspect existing FM diagnostics/status endpoints and Radar page components
- [x] Move Sweep Frames above the Evidence Map in the Radar page stack
- [x] Add an operator-facing FM Solver Status panel for the selected IID
- [x] Include solve readiness, current position, frame/funnel counts, convergence/candidate stats, and explicit blocker/reason text
- [x] Verify with focused backend/frontend checks and record results here

### Review

- Existing context:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) already had unmounted FM/airport hypothesis diagnostics, but no visible per-IID FM status panel in the main radar stack.
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) already exposed `/fm-diagnostics`, but it did not include a persistent last-attempt summary for a particular IID.
- Implementation:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now renders `SweepFrameStrips` above the evidence map.
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) adds `FMStatusPanel`, showing solver state, blocker text, current FM position, data-funnel counts, candidate count, coincidence validation summary, and last-run stage/reason/elapsed time.
  - [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py) adds runtime-only `fm_last_run`.
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) records the latest FM run attempt in `record_forward_model_attempt(...)` and clears it on FM reset.
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) records background FM attempts after each run.
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) records manual FM attempts and includes `last_run` plus top-candidate summary in `/fm-diagnostics`.
  - [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) covers the new FM diagnostics last-run payload.
- Verification:
  - `python3 -m py_compile backend/main.py backend/radar/api.py backend/radar/models.py backend/radar/sweep.py`
  - `npm run build`
  - Result: production build succeeded with the existing Vite large-chunk warning.
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_forward_model.py tests/test_debug_perf.py`
  - Result: `114 passed in 0.47s`

## 2026-04-11 Radar Page Performance Regression

- [x] Trace the current radar-page hot paths shown by live metrics: rotation sweep rebuilds, IID websocket rebuilds, coincident/evidence diagnostics, and sweep-frame polling
- [x] Identify whether the regression is backend maintenance work, frontend request cadence, or both
- [x] Implement the smallest fix that removes repeated expensive work while preserving operator-visible radar data
- [x] Verify with focused backend tests and frontend build where touched
- [x] Record the result and any remaining live-validation expectations in this review section

### Review

- Investigation started from a live sample showing:
  - main decode backlog saturated again: `msg_queue_depth=4872`, `msg_queue_stats.p95=5000`, `msg_drops_total=1171810`
  - radar background work still expensive: `radar_rotation_ms.sweep_build_avg=961.24 ms` over about `11019` events per update
  - radar page traffic active: `radar_iid_ws_ms.elapsed_avg=72.18 ms` with `141` full sends/rebuilds, `iid_sweep_frames` called `264` times, and coincident evidence/diagnostics sometimes taking seconds
  - filtered timing stream was no longer the main apparent issue: `timing_ws_ms.event_count_avg=1.77` versus `raw_event_count_avg=187.69`
- Confirmed request cadence:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) uses `/ws/radar/iids/{iid}` for the cycle-alignment panel and falls back to `/timeline` plus `/rotation` only if the websocket stalls
  - the evidence map defaults to `forward_model`; coincident evidence is fetched only if that layer is enabled
  - `sweep-frames` is polled by two mounted panels, but the endpoint itself is cheap in the supplied sample (`0.56 ms` average)
- Root cause addressed in this pass:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) `get_iid_timeline()` copied and scanned the entire 30-minute IID event deque for every selected-IID websocket rebuild, even though the radar page requests a 90-second window
  - because the websocket signature includes latest selected-IID arrival, active radar traffic can trigger a full timeline rebuild about once per second per open radar page
- Implementation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now walks the event deque backward from the newest sample, stops at the requested window cutoff, and copies only matching events for the selected IID before returning arrivals in ascending order
  - [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) adds regression coverage for recent selected-IID timeline extraction and ordering
- Expected live impact:
  - `radar_iid_ws_ms.elapsed_avg` should drop materially because websocket rebuilds no longer scan the full retained event history
  - this should also reduce GIL pressure on the decode and radar worker paths while the radar page is open
  - `radar_rotation_ms.sweep_build_avg` may still show periodic background analysis cost; if it remains near one second after this fix, that is the next separate target, not the per-page websocket hot path
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `68 passed in 0.46s`
- Additional follow-up in this pass:
  - [config.py](/home/keith/claude/adsb-dashboard/backend/config.py) now exposes `RADAR_IID_WS_REBUILD_INTERVAL_S`, defaulting to `5.0`
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now uses that configured interval for selected-IID websocket full snapshot rebuilds while preserving heartbeat behavior
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now caches negative coincident-evidence results for the existing 30 second diagnostic TTL, so repeated `no calibration pairs` / `no coincident pairs` requests do not rerun the DB/localiser path
  - [test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py) now covers the negative coincident-evidence cache path
- Additional verification:
  - `python3 -m py_compile backend/config.py backend/main.py backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `32 passed in 0.45s`

## 2026-04-11 Coincident Validation Refactor

- [x] Confirm calibration-pair/coincident consumers and decide which paths should remain live
- [x] Add a bounded FM-position coincident validator that scores predicted vs observed same-beam pairs from recent sweep frames
- [x] Disable the background coincident solver by default while keeping explicit manual solve/debug paths available
- [x] Surface validation diagnostics in FM results without making it the primary localisation solver
- [x] Add focused regression tests and run targeted backend verification
- [x] Record results and follow-up metrics in this review section

### Review

- Design decision:
  - Coincident illumination is no longer treated as a live independent resolver by default.
  - The cheaper live role is now to validate an FM-derived position: predict near-coincident aircraft pairs from the FM origin and recent sweep frames, then compare those predictions against observed arrival-time coincidence.
- Implementation:
  - [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) now includes `validate_coincident_alignment(...)`, a bounded pass over at most recent frames and aircraft per frame. It reports predicted pairs, supporting observed pairs, missed predicted pairs, weak unexpected coincidences, and a `supporting` / `mixed` / `contradicting` / `insufficient_geometry` status.
  - [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) attaches `coincident_validation` to successful FM run results.
  - [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py) and [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) keep the latest FM coincident-validation summary in memory and in the FM convergence history.
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) exposes that validation summary in FM status and diagnostics payloads.
  - [config.py](/home/keith/claude/adsb-dashboard/backend/config.py) adds `RADAR_COINCIDENT_BACKGROUND_ENABLED`, defaulting to false.
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) only starts the old background coincident solver when that flag is true.
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) no longer auto-fetches the old coincident diagnostics when the panel lazy-mounts; the operator must click `Load Diagnostics` or run the manual solve.
- Follow-up requirement change:
  - The old coincident solver is now treated as obsolete for the operator UI, not merely expensive.
  - Manual `/api/radar/iids/{iid}/coincident-run` and `/coincident-diagnostics` remain backend debug endpoints, but [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) no longer mounts a diagnostics panel, poller, or `Solve Now` button for them.
  - The evidence map's `Coincident Rays` toggle now calls the existing evidence method slot only to render FM-predicted same-beam rays from recent sweep frames.
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now builds `coincident_illumination` evidence from the current FM solution and recent sweep frames instead of loading calibration-pair rows and beam-line intersections.
  - Supporting predicted pairs render as solid coincidence rays; missed predicted pairs carry `validation=missed` for dashed/low-opacity display.
- Verification:
  - `python3 -m py_compile backend/config.py backend/main.py backend/radar/models.py backend/radar/sweep.py backend/radar/api.py backend/radar/forward_model.py backend/tests/test_forward_model.py`
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_radar_sweep.py tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `113 passed in 0.45s`
  - `npm run build`
  - Result: production build succeeded
- Additional verification after removing the old UI-triggered solver path:
  - `python3 -m py_compile backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_forward_model.py tests/test_debug_perf.py`
  - Result: `77 passed in 0.43s`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_forward_model.py tests/test_debug_perf.py`
  - Result: `113 passed in 0.45s`
  - `npm run build`
  - Result: production build succeeded
- Expected live impact:
  - `iid_evidence_coincident_illumination` and `iid_coincident_diagnostics` should stop appearing during normal radar-page load unless explicitly requested.
  - Background CI solver CPU should disappear unless `RADAR_COINCIDENT_BACKGROUND_ENABLED=true`.
  - FM results now carry cheap validation evidence, so the next useful live check is whether `coincident_validation.status` correlates with good/bad FM locations before making it a hard publishing gate.

## 2026-04-10 Radar Pair-Accumulation Assessment

- [x] Trace calibration-pair generation gates in `backend/radar/sweep.py`
- [x] Trace TDOA and coincident-illumination solver-side filtering in `backend/radar/localiser.py`
- [x] Compare the gating thresholds to the expected live geometry/timing regime and identify the most likely blocker
- [x] Record the assessment and any recommended threshold changes in the review section below

### Review

- Assessment date: 2026-04-10
- Live persistence check against `backend/data/adsb.db` showed:
  - `radar_calibration` currently has `0` rows total
  - `radar_iids` contains many `SINGLE_RADAR` / `LIKELY_SINGLE` IIDs with learned periods, but all have `n_pairs = 0` and `ci_n_pairs = 0`
- Primary blocker is structural, not solver-threshold tuning:
  - calibration pairs are created only by `RadarState.update_calibration_pairs()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), but no background loop in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) calls that function or flushes `pop_pending_pairs()` into `stats_db.insert_calibration_pair(...)`
  - both `_coincident_loop()` and the TDOA paths load pairs only from persisted `radar_calibration`, so with no producer path they can only ever report `0` pairs
- There is a second structural blocker introduced by the lazy sweep-history optimisation:
  - `update_calibration_pairs()` reads only from `self._sweep_history`
  - `_sweep_history` is now populated lazily by `get_sweep_history()` / `_compute_sweep_history_for_iid()`, so even after restoring a caller, pair generation would still see no sweeps for most IIDs unless sweep history were rebuilt proactively for calibration
- Threshold assessment after separating out the producer bug:
  - pair-generation thresholds in `sweep.py` are not obviously too strict for initial accumulation; `CO_SWEEP_WINDOW_US = 70_000` and `MAX_PAIR_TDOA_US = 5_000` are already fairly permissive
  - the most selective generation-time content filter is `_eligible_for_pair_generation()`, which excludes residual and secondary-folded ICAOs and, when a folded primary set exists, keeps only aircraft in that primary set
  - the strongest solver-side gating is in [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py): repeated family consistency (`3` repeats within `180 s`, spread `<= 800 us`) and, for coincident illumination, `abs(tdoa_us) <= 500 us`
  - those solver filters may prove conservative once pairs exist, especially the coincident `500 us` cut, but they are not the reason the live system is stuck at zero accumulated pairs today
- Recommended order of attack:
  - first restore an active calibration-pair producer path and persistence flush
  - rebuild or derive the pair source from live data that exists independently of the lazy UI sweep-history cache
  - only after pairs are flowing, use the diagnostics endpoints to decide whether `MIN_PAIR_FAMILY_REPEATS`, `MAX_PAIR_FAMILY_SPREAD_US`, or `MAX_COINCIDENT_TDOA_US` need retuning
- Implementation on 2026-04-10:
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now runs calibration-pair generation in the 30 s radar maintenance loop, flushes newly generated pairs to SQLite in batch, and requeues unsaved pairs if DB insertion fails
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now backfills sweep inputs for eligible IIDs by computing sweep history on demand when the lazy cache is empty, so pair generation no longer depends on operator/UI access to warm `_sweep_history`
  - [db.py](/home/keith/claude/adsb-dashboard/backend/db.py) now provides batched `insert_calibration_pairs(...)` so the restored producer uses one transaction per batch instead of one insert per pair
  - Added regression coverage in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) for lazy sweep-history backfill and in [test_db.py](/home/keith/claude/adsb-dashboard/backend/tests/test_db.py) for batched calibration inserts
- Verification:
  - `python3 -m py_compile backend/main.py backend/db.py backend/radar/sweep.py backend/tests/test_db.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_db.py tests/test_radar_sweep.py tests/test_radar_api.py`
  - Result: `84 passed in 1.33s`
- Follow-up regression on 2026-04-10 after restoring the producer:
  - Symptom report:
    - live message-rate graph became uneven again
    - `radar_queue_depth` stayed high and `radar_queue_stats.p95` hit the queue ceiling
    - evidence-map TDOA lines rendered as a cluttered straight-line mess instead of usable hyperbola branches
  - Root causes:
    - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) `update_calibration_pairs()` was forcing broad sweep-history rebuilds and/or full recent-sweep regeneration for eligible IIDs inside the 30 s radar maintenance loop; that recreated the same expensive sweep-building work that had previously been removed from the hot background path
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) TDOA evidence still exposed straight aircraft-to-aircraft `Calibration Pairs` line features, which visually dominated the layer stack and looked like “TDOA lines” even though they were not hyperbolas
    - [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py) `compute_hyperbola_points()` was sampling unordered “best residual” grid points rather than parameterising and ordering the actual locus, so even the hyperbola layer could render as a scrambled line
  - Mitigation:
    - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now derives pair-generation sweeps from only a short recent event window (`PAIR_GENERATION_WINDOW_S = 120`) and uses cached recent sweeps only as a fallback when available, instead of rebuilding full long-window sweep history for every eligible IID
    - [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py) now parameterises the receiver-path-corrected TDOA branch directly in focal coordinates, yielding ordered curve points on the correct branch
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) no longer adds the misleading `Calibration Pairs` straight-line layer to TDOA evidence; the TDOA line geometry is now the actual hyperbola layer only
  - Verification:
    - `python3 -m py_compile backend/radar/sweep.py backend/radar/localiser.py backend/radar/api.py`
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_localiser.py tests/test_radar_api.py`
    - Result: `74 passed in 0.66s`
- Second follow-up on 2026-04-10 after live perf still showed CPU pegging:
  - Symptom report:
    - `radar_rotation_ms.total_avg` and `sweep_build_avg` remained extremely high
    - `radar_loop_ms.update_avg` still consumed tens of seconds per cycle
    - `radar_loop_ms.flush_avg` was also very high because IID model persistence was still done one row at a time
  - Root causes:
    - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) was still re-running full Stage 1 rotation analysis every 30 seconds for continuously dirty but already stable IIDs, using a much larger event history than necessary for steady-state maintenance
    - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) was flushing radar IID rows via repeated per-model `upsert_radar_iid(...)` calls, opening the door to substantial transaction/connector overhead
  - Mitigation:
    - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now:
      - limits Stage 1 analysis input to a recent `ROTATION_ANALYSIS_MAX_AGE_S = 300` second window instead of the full 30-minute event-retention horizon
      - defers reanalysis of already stable `SINGLE_RADAR` / `LIKELY_SINGLE` IIDs for `STABLE_REANALYZE_INTERVAL_S = 300` seconds, while keeping them marked dirty so they are revisited later
    - [db.py](/home/keith/claude/adsb-dashboard/backend/db.py) now provides batched `upsert_radar_iids(...)`
    - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now uses that batched IID persistence path during the periodic radar flush
  - Verification:
    - `python3 -m py_compile backend/db.py backend/main.py backend/radar/sweep.py`
    - `uv run --directory backend pytest tests/test_db.py tests/test_radar_sweep.py tests/test_radar_localiser.py tests/test_radar_api.py`
    - Result: `102 passed in 1.16s`

## 2026-04-10 Remaining Decode-Path Contention Assessment

- [x] Trace main decode-thread and queue hot paths in `main.py` and `aircraft_state.py`, focusing on shared locks and work still executed inline
- [x] Trace FM/radar shared-state access patterns for contention against the decode path
- [x] Assess whether the remaining bottleneck is a good candidate for native/Cython work versus further structural changes
- [x] Record the findings and recommendations in the review section below

### Review

- Assessment date: 2026-04-10
- After the radar maintenance fixes, the latest live sample showed:
  - radar-specific maintenance costs are now low enough that they are no longer the primary source of overload
  - `radar_rotation_ms.total_avg` dropped to sub-second territory and `radar_drops_total` fell to `0`
  - the remaining overload is concentrated in the main Beast decode path: `msg_queue_stats` remains near saturation while `radar_queue_stats` is materially healthier
- Main contention source traced in code:
  - the main decoder thread created by [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) calls `state.process_messages_batch(...)`
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py)`process_messages_batch(...)` acquires `self._lock` once and then runs the full per-message `_decode(...)` path for the whole batch while holding that lock
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py)`_decode(...)` is not a small mutation-only section; while the lock is held it still performs:
    - native decode dispatch / fallback decode logic
    - CPR solve and positional gating logic
    - operator/type/country cache lookups
    - ADS-B / Mode S / EHS field application
    - timing-event recording and IID/timing bookkeeping
  - that means the shared aircraft-state lock serialises both actual state mutation and a large amount of pure/mostly-pure decode work
- FM / radar interaction assessment:
  - FM background work is still expensive (`fm_run_ms.elapsed_avg` is high), but it runs off-thread and now skips entirely when `_msg_queue` backlog exceeds the configured threshold
  - the current perf pattern does not point to FM as the primary cause of the saturated main decode queue
  - radar worker isolation appears to be doing its job: the radar queue is no longer the dominating pressure signal
- Native/Cython assessment:
  - the server is already on the native decode path (`aircraft_state._NATIVE_DECODE == True` in the project runtime), so “add a native decoder plugin” is not the main missing piece
  - the remaining bottleneck is more about lock scope and Python-side field-application/state-mutation than raw frame parse cost
  - additional native work would only be worthwhile after reducing lock scope; otherwise a faster parser would still leave a long serialized critical section
- Best next step:
  - split the current `_decode(...)` path into:
    - an outside-lock parse/extract phase that computes immutable message facts
    - a minimal under-lock apply/update phase that mutates shared aircraft state and counters
  - likely secondary improvements:
    - move enrichment cache lookups and other pure read-side helpers out of the hot locked section when safe
    - add phase-level decode timings so the remaining hot sub-stages inside `_decode(...)` can be measured separately before any native work is attempted
- Native/Cython candidates only if structural refactor is insufficient:
  - CPR/global+relative solve and compact field-application helpers for DF17/18
  - bulk per-batch application of immutable parsed outputs to reduce Python call overhead
  - not recommended yet: another standalone native “plugin” for generic decode, because the project already uses `decode_cffi` and the current evidence says the bigger win is architectural
- Implementation on 2026-04-10:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now runs `decode_cffi.decode_message(...)` in a new `_predecode_native_message(...)` helper before acquiring `AircraftState._lock`
  - both `process_message(...)` and `process_messages_batch(...)` now pass that predecoded native result into `_decode(...)`, so the expensive native parse step is no longer serialized under the shared aircraft-state lock
  - fallback behavior is preserved: if native decode is unavailable or the caller overrides `_decode(...)`, `_decode(...)` still remains the final authority for accept/reject semantics
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/benchmark.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_aircraft_state_counts.py tests/test_timing_events.py tests/test_radar_sweep.py`
  - Result: targeted suites exercised during this pass remained green before later API-only persistence tests were isolated
  - Additional note:
    - isolated `test_radar_api.py::test_manual_position_controls_persist_and_lock` currently hangs in the `asyncio.to_thread(_persist_model)` path even though the decode-path refactor does not touch [radar/api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py); treat that as a separate issue rather than proof against the decode refactor

## 2026-04-10 Radar Solution-Comparison Hot Path

- [x] Replace summary-only calibration-pair row loads with a cheap aggregate count
- [x] Replace summary-only sweep-frame materialisation with live frame counters
- [x] Verify the solution-comparison and debug/API regression suites
- [x] Record the result and live-perf expectation in this review section

### Review

- Investigation started after live samples showed `iid_solution_comparison` jumping from ~39 ms avg with the radar display disabled to ~1017 ms avg, 7572 ms max, when enabled.
- Working diagnosis:
  - the enabled radar display repeatedly hits `/api/radar/iids/{iid}/solution-comparison`
  - the endpoint no longer builds coincident diagnostics, but `_build_method_statuses()` still loads and dedupes up to 5000 calibration rows only to compute `pair_count`
  - it also materialises full sweep frames only to count total/usable frames
  - both are avoidable in a status endpoint and can create GIL pressure that shows up as off-CPU delay in the decoder and radar workers
- Implementation result:
  - [db.py](/home/keith/claude/adsb-dashboard/backend/db.py) now has `count_calibration_pairs(iid)` and a composite `(iid, ts)` calibration index so the comparison endpoint can count recent pair rows without loading and deduping payloads
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now uses that count in `_build_method_statuses()` and keeps the full calibration-row loads only for diagnostics/evidence endpoints that need row content
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now reports `n_marginal` and `n_usable` from `get_live_frame_counts()`, letting solution-comparison preserve the old “good or marginal is usable” wording without materialising frame payloads
- Expected live impact:
  - `/api/debug/perf` `radar_api_ms.by_endpoint.iid_solution_comparison` should drop back toward low single-digit or low tens of milliseconds instead of seconds when the radar display is enabled
  - if CPU remains pegged after this, the next suspect is another page-open endpoint or websocket not yet visible in the current timing buckets, not the radar worker/decoder core path
- Verification:
  - `python3 -m py_compile backend/db.py backend/radar/api.py backend/radar/sweep.py backend/tests/test_db.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_db.py tests/test_radar_api.py tests/test_debug_perf.py tests/test_radar_sweep.py`
  - Result: `94 passed in 1.41s`

## 2026-04-10 Radar IID Switch CPU Spike

- [x] Trace selected-IID change fanout across frontend hooks, per-IID websockets, and heavy backend endpoints
- [x] Make expensive evidence/diagnostic work visible in `/api/debug/perf`
- [x] Prevent selected-IID changes from eagerly building every evidence layer
- [x] Abort stale selected-IID fetches on the frontend where practical
- [x] Verify with focused frontend build and backend regression tests

### Review

- Investigation started after first radar-page load looked healthy, but clicking a different IID caused a backend CPU spike and made `/api/debug/perf` hard to load.
- Working diagnosis:
  - the visible steady-state buckets were clean, but the evidence and TDOA/coincident diagnostics endpoints were not recorded in `radar_api_ms`
  - `/api/radar/iids/{iid}/evidence` builds TDOA, coincident illumination, forward-model, and inscribed-angle evidence synchronously in one async request
  - if the evidence panel has mounted, changing the selected IID can therefore perform several calibration-row loads, sweep-frame materialisations, and geometry builders at once; old fetches are ignored client-side but not aborted
- Implementation result:
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now records `iid_evidence`, `iid_evidence_<method>`, `iid_tdoa_diagnostics`, and `iid_coincident_diagnostics` timings in `radar_api_ms`
  - those evidence/diagnostic builders now run through `asyncio.to_thread(...)`, so a slow build should no longer block the main FastAPI event loop and prevent `/api/debug/perf` from responding
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now loads evidence map geometry per selected method rather than calling the all-method `/evidence` endpoint on every selected-IID change
  - evidence map defaults to the forward-model layer only; TDOA, coincident, and inscribed-angle evidence are fetched only when their checkboxes are enabled
  - selected-IID evidence and diagnostics fetches now use `AbortController` so stale client requests are cancelled when switching IIDs
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/debug.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `31 passed in 0.75s`
  - `npm run build`
  - Result: production build succeeded

## 2026-04-10 Radar Overlay Timing Stream Pressure

- [x] Add selected-IID/DF11 filtering to the backend timing websocket and polling fallback
- [x] Route the radar receiver-centred overlay through the filtered stream instead of the all-message timing feed
- [x] Keep timing stream perf counters useful for confirming reduced event counts
- [x] Verify backend timing API tests and frontend build

### Review

- Investigation started after the post-evidence-change sample still showed queue saturation while `radar_api_ms` was clean.
- Working diagnosis:
  - the receiver-centred live overlay was enabled, shown by non-zero `timing_ws_ms`
  - `/ws/timing` still sent the full timing stream to the radar page and relied on the browser to filter to DF11 plus selected IID
  - that makes selected-IID switching pay backend JSON/list work for events the radar overlay will discard
- Implementation result:
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) `/ws/timing` now accepts optional `iid` and `df11_only` query filters
  - [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) applies the same filters for the polling fallback endpoint
  - [useTimingEventStream.js](/home/keith/claude/adsb-dashboard/frontend/src/hooks/useTimingEventStream.js) now accepts `iid` and `df11Only` options and reconnects/resets sequence state when those filters change
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now requests only DF11 events for the selected IID when the live receiver-centred overlay is enabled
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now reports `timing_ws_ms.raw_event_count_avg` alongside filtered `event_count_avg`
- Expected live impact:
  - with the radar overlay enabled, `timing_ws_ms.event_count_avg` should be much lower than `raw_event_count_avg`
  - `timing_ws_ms.elapsed_avg` should drop, especially during selected-IID changes
- Verification:
  - `python3 -m py_compile backend/main.py backend/timing.py backend/debug.py`
  - `uv run --directory backend pytest tests/test_timing_events.py tests/test_debug_perf.py`
  - Result: `9 passed in 0.35s`
  - `npm run build`
  - Result: production build succeeded

## 2026-04-09 Radar Display CPU Investigation

- [x] Trace the radar display live data path and identify concrete hot loops in the frontend and backend timing/radar streams
- [x] Remove avoidable per-frame full-buffer scans and per-packet O(n log n) rebuilds from the frontend timing/radar paths
- [x] Reduce backend timing websocket CPU by avoiding repeated full-deque scans/serialisation when nothing materially changed
- [x] Add focused verification for any new buffer/indexing logic and run targeted frontend/backend verification
- [x] Record review notes here with root causes, measured mitigations, and remaining native/Cython follow-up candidates

### Review

- Plan verified on 2026-04-09 against the live timing/radar path in [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx), [useTimingEventBuffer.js](/home/keith/claude/adsb-dashboard/frontend/src/hooks/useTimingEventBuffer.js), [messageField.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/messageField.js), [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), and [main.py](/home/keith/claude/adsb-dashboard/backend/main.py).
- Root causes found:
  - The receiver-centred radar canvas was doing `selectMessageFieldEvents(...).filter(...)` inside every `requestAnimationFrame`, which meant the browser could rescan a large timing buffer 60 times per second on one UI thread.
  - The shared timing buffer hooks were rebuilding the full merged buffer with `Map` + sort on every packet, turning a mostly append-only stream into repeated O(n log n) work.
  - Backend timing accessors were snapshotting and scanning the entire 60k timing deque on every websocket tick, even though clients normally only need the new suffix since the previous sequence id.
  - Aggregate/interrogator/radar websocket loops were rebuilding expensive payloads every 100 ms, which is unnecessarily aggressive for human-facing graphs that already animate/interpolate on the client.
- Mitigations implemented on 2026-04-09:
  - Updated [messageField.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/messageField.js) so visible-event selection starts from an arrival-time lower bound instead of rescanning the whole buffer each time.
  - Updated [useTimingEventBuffer.js](/home/keith/claude/adsb-dashboard/frontend/src/hooks/useTimingEventBuffer.js) and the timing-page local buffer in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) to use append-first incremental merges, with a slower fallback only when the incoming sequence stream resets or overlaps.
  - Updated [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so the expensive DF11/IID/bearing/range filtering is memoized per timing packet rather than redone every animation frame.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so timing and IID window reads iterate newest-first and stop early instead of copying/scanning the full deque every poll.
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) so:
    - `/ws/timing` and `/ws/timing-page` request bounded timing suffixes directly from state
    - aggregate/interrogator/radar websocket payloads rebuild at most every `250 ms` instead of every `100 ms`
- Verification:
  - `uv run --directory backend pytest tests/test_timing_events.py`
  - Result: `7 passed in 0.35s`
  - `uv run --directory backend pytest tests/test_timing_events.py tests/test_radar_api.py tests/test_radar_sweep.py`
  - Result: `48 passed in 0.36s`
  - `npm run build` in `frontend/`
  - Result: success
- Remaining native/Cython follow-up candidates:
  - The first native target should be timing aggregation on the backend only if live profiling still shows `_build_highfreq_payload()` dominating CPU after these structural fixes. A small Cython/C helper for DF-family binning and burst counts would be reasonable there because it is pure numeric bucketing over append-only event arrays.
  - The frontend hot path does not currently justify a C/Cython port. The bigger wins were algorithmic: stop rescanning and resorting large JS arrays on every frame/packet.
  - If the decoder thread still proves hot on lower-power hardware after this pass, the next native candidate is not the radar page but deeper decode/timing extraction paths, using the existing `backend/native/` pattern rather than adding a one-off helper just for the radar UI.
  - Follow-up on the same day: the radar path was also decoding every Beast frame a second time in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) via `radar_state.on_frame(msg)` after [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) had already decoded it once. That second pass sat outside the recorded `pure_decode_us` timings but still ran on the same decoder thread, so it could inflate queue depth and message drops while making the perf panel look deceptively decode-light.
  - Mitigation: [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now returns predecoded DF11 radar events from the main decode path, [benchmark.py](/home/keith/claude/adsb-dashboard/backend/benchmark.py) forwards those directly, and [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now has `on_df11_event(...)` so the radar subsystem no longer re-runs `decode_cffi.decode_message()` on every frame.
- Verification for that follow-up:
    - `uv run --directory backend pytest tests/test_timing_events.py tests/test_radar_api.py tests/test_radar_sweep.py`
    - Result: `48 passed in 0.38s`
    - `python3 -m py_compile backend/aircraft_state.py backend/benchmark.py backend/radar/sweep.py`

- Follow-up metering added after later reports of radar-only stalls:
  - Added decoder-path radar timings in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) for:
    - total `on_df11_event(...)` cost per predecoded DF11 event
    - the `_on_df11_frame_builder(...)` sub-cost within that event path
  - Added stage timings inside `update_rotation_models()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so `/api/debug/perf` now shows whether time is going into:
    - event snapshot/pruning
    - tracker refresh
    - bulk position-history fetch
    - position-cache build
    - sweep-history build
    - IID analysis/swap
  - Added outer-loop timings in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) for:
    - `_radar_loop()` update and DB flush durations
    - per-IID FM pipeline runtimes in `_fm_loop()`
  - Extended [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) so `/api/debug/perf` now reports:
    - `radar_decoder_ms`
    - `radar_rotation_ms`
    - `radar_loop_ms`
    - `fm_run_ms`
  - Verification:
    - `python3 -m py_compile backend/debug.py backend/main.py backend/radar/sweep.py`
    - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_timing_events.py`
    - Result: `48 passed in 0.41s`

## 2026-04-10 Decode Throughput Instrumentation

- [x] Inspect the current decode perf plumbing and identify which phases are missing from `/api/debug/perf`
- [x] Add end-to-end decoder timings that include native predecode cost, without losing the existing under-lock apply metric
- [x] Extend the latency-waveform payload so live timing graphs can distinguish total decode, predecode, lock wait, and apply cost
- [x] Verify with focused backend tests and compile checks, then record the resulting perf contract here

### Review

- Assessment date: 2026-04-10
- Root cause in the instrumentation itself:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) started timing `process_message(...)` and `process_messages_batch(...)` only after `_predecode_native_message(...)` had already run
  - as a result, `/api/debug/perf` `msg_decode_us` shrank to the under-lock apply phase while the expensive native predecode stage remained invisible, which made the decoder look healthy even while `_msg_queue` was saturating and dropping messages
- Instrumentation changes implemented on 2026-04-10:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now records:
    - `msg_timings` as full end-to-end per-message decode time, including native predecode
    - `predecode_timings` as the per-message native predecode phase before lock acquisition
    - `lock_wait_timings` as lock acquisition wait only, measured after predecode completes
    - `decode_timings` unchanged as the under-lock apply path
  - batched message processing now apportions total, predecode, lock-wait, and apply time per message so batch and single-message samples remain comparable in `/api/debug/perf`
  - low-rate latency waveforms in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now expose:
    - `total_decode_ms`
    - `predecode_ms`
    - `decode_ms` (still the under-lock apply phase for continuity)
    - `lock_wait_ms`
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now reports `predecode_us` in `/api/debug/perf` and restores `msg_decode_us` to the intended end-to-end meaning
- Resulting perf contract:
  - `msg_decode_us`: full end-to-end per-message decoder cost
  - `predecode_us`: native parse/predecode cost before touching `AircraftState._lock`
  - `lock_wait_us`: time waiting to acquire `AircraftState._lock` after predecode has finished
  - `pure_decode_us`: time spent applying decoded state under the lock
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `11 passed in 0.36s`

## 2026-04-10 Decode Starvation vs Native Decode Diagnosis

- [x] Benchmark the native decode boundary directly to separate `libdecode.so` cost from Python/CFFI wrapper cost
- [x] Reduce background GIL-heavy radar/FM work when the main decode queue is backing up
- [x] Verify the new scheduling guardrails and record the diagnosis/results here

### Review

- Assessment date: 2026-04-10
- Diagnosis:
  - The new `predecode_us` metric initially looked like the native decoder was taking about `1 ms` per message, but direct micro-benchmarks of the actual C boundary did not support that.
  - Using the existing backend corpus and a set of accepted DF17 examples against [decode_cffi.py](/home/keith/claude/adsb-dashboard/backend/decode_cffi.py) and [decode_api.c](/home/keith/claude/adsb-dashboard/backend/native/decode_api.c), the observed costs in this environment were:
    - direct `lib.decode_message(...)` with reused result/buffer: about `0.44` to `0.51 us/msg`
    - direct call with fresh CFFI result allocation: about `0.98 us/msg`
    - full Python wrapper with dict marshalling on accepted frames: about `1.83 us/msg`
  - That is orders of magnitude below the live `predecode_us` readings, so the native readsb-derived decode path is not the primary source of the observed `~1 ms` wall-clock delay.
  - The more plausible explanation is decoder-thread starvation from other Python work that competes for the GIL:
    - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) was allowing multiple FM/CI background runs in parallel via `asyncio.create_task(...)`
    - live perf still showed long FM and radar maintenance runtimes (`fm_run_ms`, `radar_loop_ms.flush_avg`), which can inflate wall-clock time around `_predecode_native_message(...)` even when the actual native decode work is fast
- Mitigations implemented on 2026-04-10:
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now treats main-message backlog as a hard priority signal:
    - radar maintenance is skipped for that cycle when `_msg_queue.qsize() >= 100`
    - radar IID DB flush is deferred while the queue is under pressure
    - FM and coincident loops now use the same backlog guard
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now serializes FM and coincident work instead of launching concurrent background tasks per cycle:
    - `_FM_MAX_IIDS_PER_CYCLE = 1`
    - `_CI_MAX_IIDS_PER_CYCLE = 1`
    - per-IID runs are awaited sequentially, with an additional backlog check before each run starts
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now reports radar-loop queue-pressure context:
    - `radar_loop_ms.msg_queue_depth_avg`
    - `radar_loop_ms.skipped_for_backlog_count`
- Resulting assessment:
  - The isolated native/CFFI decode implementation is not showing the pathological cost implied by the live `predecode_us` metric.
  - The stronger suspect is Python-side scheduling pressure from background localisation/maintenance work, not a fundamental flaw in `libdecode.so`.
  - If live queue saturation persists even after the new backlog guardrails, the next step should be CPU-time instrumentation on the decode thread itself to separate on-CPU decode from off-CPU starvation directly.
- Verification:
  - Direct benchmark commands:
    - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend python ...` against `decode_cffi.decode_message(...)` and raw `lib.decode_message(...)`
  - `python3 -m py_compile backend/main.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `11 passed in 0.39s`

## 2026-04-10 Decoder Thread Starvation Instrumentation

- [x] Inspect the live decoder worker loop and identify where to measure batch wait, assembly, wall time, and CPU time
- [x] Add decoder-thread batch instrumentation in the production worker path and expose it in `/api/debug/perf`
- [x] Verify with focused backend tests and compile checks, then record the new decoder-thread perf contract here

### Review

- Assessment date: 2026-04-10
- Goal:
  - distinguish true on-CPU decode cost from scheduler/GIL starvation inside the single decoder worker thread
- Implementation result on 2026-04-10:
  - [benchmark.py](/home/keith/claude/adsb-dashboard/backend/benchmark.py) now instruments the production decoder worker created by `make_pause_aware_decoder(...)`, which is the same path used by [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) for live Beast ingest
  - each processed batch now records:
    - `queue_wait_ms`: blocking time spent waiting for the first queue item
    - `batch_fill_ms`: time spent assembling the rest of the batch with `get_nowait()`
    - `process_wall_ms`: wall-clock time spent inside `state.process_messages_batch(...)` plus radar-event handoff
    - `process_cpu_ms`: decoder thread CPU time spent over that same processing section via `time.thread_time()`
    - `process_offcpu_ms`: `process_wall_ms - process_cpu_ms`, clipped at zero, to show time the decoder batch was runnable but not on-CPU
    - `batch_size`: number of messages processed in the batch
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now exposes these aggregates in `/api/debug/perf` as `decoder_thread_ms`
- Resulting perf contract:
  - `decoder_thread_ms.queue_wait_avg`: average idle/blocking wait for work
  - `decoder_thread_ms.batch_fill_avg`: average time spent pulling extra items into the current batch
  - `decoder_thread_ms.process_wall_avg`: average wall-clock batch processing time
  - `decoder_thread_ms.process_cpu_avg`: average decoder-thread CPU time for that batch work
  - `decoder_thread_ms.process_offcpu_avg`: average inferred off-CPU starvation during batch processing
  - `decoder_thread_ms.batch_size_avg`: average messages per processed batch
- How to interpret the next live sample:
  - high `process_wall_avg` with low `process_cpu_avg` means scheduler/GIL starvation, not inherently slow decode logic
  - high `process_cpu_avg` means the decoder thread itself is truly CPU-bound
  - high `queue_wait_avg` with a deep queue would be suspicious and suggest queue/thread interaction issues
  - very small `batch_size_avg` under sustained backlog would suggest the worker is not draining efficiently
- Verification:
  - `python3 -m py_compile backend/benchmark.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `11 passed in 0.36s`

## 2026-04-10 Beast Ingest Thread Instrumentation

- [x] Inspect the Beast ingest/parsing path and identify where to measure chunk CPU vs wall time and queue handoff pressure
- [x] Add Beast ingest chunk instrumentation and expose it in `/api/debug/perf`
- [x] Verify with focused backend tests and compile checks, then record the new ingest perf contract here

### Review

- Assessment date: 2026-04-10
- Goal:
  - determine whether the producer side of Beast ingest is monopolizing the GIL / event-loop thread while the decoder worker stays mostly off-CPU
- Implementation result on 2026-04-10:
  - [beast_client.py](/home/keith/claude/adsb-dashboard/backend/beast_client.py) now records a per-chunk ingest sample for both native and Python parser modes
  - each received chunk records:
    - `chunk_bytes`: bytes received from the socket read
    - `frames`: non-Mode-A/C frames dispatched from that chunk
    - `parse_wall_ms`: wall-clock time spent extending/parsing/dispatching that chunk
    - `parse_cpu_ms`: thread CPU time spent over the same section via `time.thread_time()`
    - `parse_offcpu_ms`: `parse_wall_ms - parse_cpu_ms`, clipped at zero
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now exposes these aggregates as `beast_ingest_ms` in `/api/debug/perf`
- Resulting perf contract:
  - `beast_ingest_ms.chunk_bytes_avg`: average bytes per received chunk
  - `beast_ingest_ms.frames_avg`: average dispatched Beast frames per chunk
  - `beast_ingest_ms.parse_wall_avg`: average wall-clock ingest/parse/dispatch time per chunk
  - `beast_ingest_ms.parse_cpu_avg`: average event-loop thread CPU time spent per chunk
  - `beast_ingest_ms.parse_offcpu_avg`: average inferred off-CPU delay during chunk handling
- How to interpret the next live sample:
  - high `beast_ingest_ms.parse_cpu_avg` with low `decoder_thread_ms.process_cpu_avg` would strongly indicate the producer side is monopolizing the GIL and starving the decode worker
  - low Beast CPU and low decoder CPU with high decoder wall time would instead suggest starvation from some other Python thread or runtime behavior
  - if both Beast and decoder CPU are low while queues still back up, the bottleneck is likely elsewhere in scheduling or another uninstrumented thread
- Verification:
  - `python3 -m py_compile backend/beast_client.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_beast_client.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `26 passed in 0.36s`

## 2026-04-10 Adaptive Decoder Batch Draining

- [x] Inspect the current decode batch configuration and choose an adaptive higher-cap drain strategy
- [x] Implement larger/adaptive decoder batch draining and keep perf instrumentation aligned
- [x] Verify with focused backend tests/compilation and record the batching change here

### Review

- Assessment date: 2026-04-10
- Root cause:
  - The live metrics showed Beast ingest delivering about `203` frames per chunk while the decoder worker was consuming only about `16` messages per batch.
  - With the queue already saturated, that forced the decoder through too many queue/drain cycles and left it repeatedly off-CPU between small batches.
- Implementation result on 2026-04-10:
  - Added adaptive batch sizing in [benchmark.py](/home/keith/claude/adsb-dashboard/backend/benchmark.py) via `_target_decode_batch_size(...)`
  - The decoder worker now:
    - uses `DECODE_BATCH_SIZE` as the baseline drain target
    - grows the target when `msg_queue.qsize()` reaches `DECODE_BATCH_BACKLOG_THRESHOLD`
    - caps growth at `DECODE_BATCH_SIZE_MAX`
  - Added new config knobs in [config.py](/home/keith/claude/adsb-dashboard/backend/config.py):
    - `DECODE_BATCH_SIZE` default `16`
    - `DECODE_BATCH_SIZE_MAX` default `128`
    - `DECODE_BATCH_BACKLOG_THRESHOLD` default `64`
  - Extended decoder-thread perf reporting in [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) with `decoder_thread_ms.batch_target_avg` so live samples show whether adaptive draining is actually engaging
- Resulting perf contract:
  - `decoder_thread_ms.batch_size_avg`: average messages actually processed per batch
  - `decoder_thread_ms.batch_target_avg`: average target chosen by the adaptive drain policy
  - when backlog exists, `batch_target_avg` should rise materially above the base `16`
- Verification:
  - `python3 -m py_compile backend/config.py backend/benchmark.py backend/debug.py backend/tests/test_debug_perf.py backend/tests/test_decoder_batching.py`
  - `uv run --directory backend pytest tests/test_decoder_batching.py tests/test_debug_perf.py tests/test_beast_client.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `28 passed in 0.41s`

## 2026-04-10 Radar Worker Thread Instrumentation

- [x] Inspect the radar worker loop and `on_df11_event(...)` path to place worker-level CPU/wall instrumentation cleanly
- [x] Add radar-worker batch/timing instrumentation and a low-risk queue-drain batching pass
- [x] Verify with focused backend tests/compilation and record the radar-worker perf contract/results here

### Review

- Assessment date: 2026-04-10
- Goal:
  - determine whether the continuous radar worker thread is the remaining source of GIL/scheduler pressure after Beast ingest and decoder batching were instrumented
- Implementation result on 2026-04-10:
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now instruments the radar worker thread created by `_start_radar_processor()`
  - the radar worker now:
    - drains `_radar_queue` in batches instead of one event at a time
    - chooses a larger target batch size when radar backlog grows
    - records per-batch:
      - `queue_wait_ms`
      - `batch_fill_ms`
      - `process_wall_ms`
      - `process_cpu_ms`
      - `process_offcpu_ms`
      - `batch_size`
      - `batch_target`
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now exposes these aggregates as `radar_worker_ms` in `/api/debug/perf`
  - This was implemented as a low-risk batching pass only in the worker loop; the existing per-event `radar_decoder_ms` timings from [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) remain intact
- Resulting perf contract:
  - `radar_worker_ms.process_cpu_avg`: actual radar worker CPU time per batch
  - `radar_worker_ms.process_offcpu_avg`: inferred off-CPU/starvation time during radar batch processing
  - `radar_worker_ms.batch_size_avg`: actual radar events processed per batch
  - `radar_worker_ms.batch_target_avg`: selected radar drain target under current backlog conditions
- Verification:
  - `python3 -m py_compile backend/main.py backend/debug.py backend/tests/test_debug_perf.py`
  - `uv run --directory backend pytest tests/test_debug_perf.py tests/test_radar_sweep.py tests/test_beast_client.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `56 passed in 0.38s`

## 2026-04-10 Native Radar Burst Processing

- [x] Inspect the live DF11 builder state and existing native wrapper patterns to define a minimal native seam
- [x] Implement a native batch helper for DF11 burst accumulation/fired-burst extraction plus Python wrapper integration
- [x] Add focused tests/verification and record the native-radar design/result here

### Review

- Assessment date: 2026-04-10
- Design choice:
  - Do not port the whole radar localisation stack to native code.
  - Move only the hottest per-event burst-processing seam into native code:
    - pending burst accumulation
    - burst expiry detection
    - weighted burst-centroid computation
    - fired-burst emission back to Python
  - Keep ADS-B position lookup, reference-aircraft logic, and `SweepFrame` object construction in Python.
- Implementation result on 2026-04-10:
  - Added a stateful native burst processor in [radar_burst.c](/home/keith/claude/adsb-dashboard/backend/native/radar_burst.c) and exported its API in [decode_api.h](/home/keith/claude/adsb-dashboard/backend/native/decode_api.h)
  - Updated [Makefile](/home/keith/claude/adsb-dashboard/backend/native/Makefile) and rebuilt `backend/native/libdecode.so`
  - Added [decode_cffi.py](/home/keith/claude/adsb-dashboard/backend/decode_cffi.py) `RadarBurstProcessor`, a stateful wrapper that accepts batches of `(arrival_us, icao, signal_dbfs)` events and returns fired bursts with:
    - `icao`
    - `burst_centroid_us`
    - `burst_signal`
    - `trigger_arrival_us`
  - Refactored [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py):
    - `on_df11_event(...)` now routes through new batched `on_df11_batch(...)`
    - `on_df11_batch(...)` appends IID events / flash events in Python, then feeds grouped per-IID DF11 batches into the native burst processor
    - `_process_fired_bursts(...)` now handles only the post-burst Python logic: reference selection, ADS-B position lookup, and `SweepFrameObservation` assembly
    - reset paths now clear native per-IID burst processors as well
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) radar worker batching so it calls `radar_state.on_df11_batch(batch)` rather than looping over `on_df11_event(...)`
- Expected impact:
  - lower Python CPU time in the continuous radar worker thread
  - less GIL pressure from burst expiry/centroid processing
  - decoder worker should spend less time off-CPU if the radar burst path was the dominant remaining starver
- Verification:
  - `make` in `backend/native`
  - `python3 -m py_compile backend/decode_cffi.py backend/main.py backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py tests/test_beast_client.py tests/test_decoder_batching.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `60 passed in 0.40s`

## 2026-04-10 Native Radar History Checks

- [x] Inspect the live post-burst Python logic to choose the next high-value native seam
- [x] Restore native-path dominant-family semantics so folded/residual IID classifications still gate live frame assembly correctly
- [x] Extend the native radar helper to handle centroid-history-based family/reference checks
- [x] Verify with focused radar tests/compile/build checks and record the result here

### Review

- Implemented on 2026-04-10.
- Native seam added in [radar_burst.c](/home/keith/claude/adsb-dashboard/backend/native/radar_burst.c) and exposed through [decode_cffi.py](/home/keith/claude/adsb-dashboard/backend/decode_cffi.py):
  - centroid-history-based dominant-period matching
  - centroid-history-based reference-phase-family matching
  - live reference-aircraft selection with recency and hysteresis
- Integrated that seam into the live post-burst path in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), so the radar worker now keeps more of the continuous burst-family scoring and reference-selection logic in native code instead of Python.
- Correctness fix:
  - the first native integration incorrectly bypassed the established `rotation_model.folded` / `residual` / `secondary_folded` shortcuts for live frame assembly
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now preserves those learned IID classifications first, and only falls back to the native centroid-history matcher for aircraft that are not already classified by the rotation model
  - this restored live FM frame production while keeping the new native fast path
- Follow-up regression fix on 2026-04-10 after live throughput recovered but frame generation disappeared again:
  - root cause was in [radar_burst.c](/home/keith/claude/adsb-dashboard/backend/native/radar_burst.c), not in the solver thresholds
  - the native burst processor was deleting the per-ICAO pending/history slot whenever a burst expired, which also discarded the centroid history needed for:
    - dominant-period fallback checks
    - phase-family checks
    - reference-aircraft selection
  - that left the runtime fast but starved live frame assembly of the history it needed, so FM frames stopped opening even though period and reference state could still look superficially healthy
  - fixed by keeping the per-ICAO native state alive across burst expiries and clearing only the reply buffer for the completed burst
  - also corrected the native reference scorer so its standard-deviation term uses the real sample mean, matching the Python scorer instead of incorrectly measuring spread around the median interval
  - added a regression in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) that reloads the real native wrapper and checks that native reference selection matches the Python scorer on the same centroid histories
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/decode_cffi.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py`
  - Result: `35 passed in 0.33s`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py tests/test_beast_client.py tests/test_decoder_batching.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
  - Result: `61 passed in 0.38s`
  - follow-up verification after the native history-lifetime fix:
    - `python3 -m py_compile backend/radar/sweep.py backend/decode_cffi.py backend/tests/test_radar_sweep.py`
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py`
    - Result: `36 passed in 0.40s`
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_debug_perf.py tests/test_beast_client.py tests/test_decoder_batching.py tests/test_timing_events.py tests/test_aircraft_state_counts.py`
    - Result: `62 passed in 0.40s`

## 2026-04-10 Radar Page Load Pressure

- [x] Trace radar-page mount/poll fan-out and identify which backend handlers are still expensive outside the live decoder metrics
- [x] Reduce redundant backend summary work for radar-page load
- [x] Defer or lazy-mount heavy radar-page sections so opening the page does not immediately trigger every expensive panel
- [x] Reduce browser-side timing/render pressure from the live receiver-centred radar field and Stage 1 alignment panels
- [x] Verify with focused frontend/backend checks and record the outcome here

### Review

- Assessment date: 2026-04-10.
- Root cause shifted again: the latest live perf sample no longer pointed at decode or the live radar worker.
  - `msg_decode_us`, `predecode_us`, `radar_worker_ms`, and `radar_rotation_ms` were all relatively small.
  - The remaining collapse happened when the radar page loaded, which strongly suggested page-triggered backend work outside the current live ingest counters.
- Radar page findings in [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx):
  - the page mounts a large stack of panels immediately for the selected IID
  - many of those panels start their own polling loops on mount, including:
    - solution comparison
    - TDOA diagnostics
    - coincident diagnostics
    - evidence map
    - sweep frames
    - FM diagnostics / airport hypothesis
    - convergence history
  - that means opening the page can trigger a burst of expensive API calls even before the operator scrolls to those panels
- Backend finding in [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py):
  - `/api/radar/rotation` was recomputing sweep-frame state twice per IID by calling `get_pipeline_health(iid)` and then `get_sweep_frames(iid)` again for the same row
  - that duplicated work on every 5 s summary refresh
- Mitigations implemented:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) `get_pipeline_health(...)` now accepts an optional precomputed `sweep_frames` list
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) `/api/radar/rotation` now computes `sweep_frames` once per IID and reuses them for both pipeline-stage summary and good-frame counts
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now lazy-mounts the lower-costly panels with an intersection-based `LazyMountSection`, so opening the page no longer immediately mounts and polls every heavy section below the fold
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/radar/sweep.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_debug_perf.py`
  - Result: `65 passed in 0.78s`
  - `npm run build` in `frontend/`
  - Result: success
- Follow-up after the first page-load mitigation still showed CPU pegging on radar-page open, but the live backend counters no longer matched a decode-path overload:
  - `msg_queue_depth = 0`
  - `radar_queue_depth = 0`
  - `decoder_thread_ms.process_cpu_avg` and `radar_worker_ms.process_cpu_avg` were both small
  - that pointed instead to the selected-IID Stage 1 websocket and/or browser-side rendering churn
- Root cause found in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py):
  - `/ws/radar/iids/{iid}` was rebuilding the full timeline payload on a fixed timer and, worse, its “heartbeat” resent the same full timeline/rotation payload again even when nothing had changed
  - that kept forcing repeated large JSON serialisation/parsing and Stage 1 rerenders while the page was open
- Mitigation:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now tracks `get_iid_latest_arrival_us(iid)` cheaply from live ingest
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now uses a change signature for `/ws/radar/iids/{iid}` based on:
    - latest arrival for that IID
    - rotation-model timestamps/state
    - current reference aircraft
    - requested window
  - the selected-IID websocket now rebuilds only when that signature changes, rate-limited by a dedicated `1.0 s` radar-IID rebuild interval
  - unchanged heartbeats are now tiny `radar_iid_heartbeat` payloads instead of resending the full timeline blob
- Verification for that follow-up:
  - `python3 -m py_compile backend/main.py backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_debug_perf.py`
  - Result: `66 passed in 0.89s`
- Final follow-up after CPU still pegged while opening the radar page:
  - the new live sample again did not implicate the backend ingest workers:
    - `msg_queue_depth = 0`
    - `radar_queue_depth = 0`
    - `decoder_thread_ms.process_cpu_avg` and `radar_worker_ms.process_cpu_avg` stayed low
  - that pointed to browser-side pressure, and the console error about `/ws/timing` lined up with the receiver-centred radar field still auto-starting the live timing stream when its panel mounted
  - root causes on the frontend:
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) `ReceiverCentredRadarField` immediately opened `/ws/timing`, built a large filtered event set, and ran a continuous canvas loop
    - the canvas loop was also resetting canvas dimensions every frame and rescanning the visible event set every frame
    - the Stage 1 alignment panel was also resorting the ICAO list on every render
  - mitigations:
    - [useTimingEventStream.js](/home/keith/claude/adsb-dashboard/frontend/src/hooks/useTimingEventStream.js) now supports `enabled`
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now keeps the live receiver-centred overlay paused by default and requires an explicit `Enable Live Overlay` action before opening `/ws/timing`
    - the receiver-field draw loop now:
      - avoids canvas resize churn unless size changed
      - skips hidden-tab work
      - caps itself to a lower draw cadence
      - walks backward through time-sorted events instead of filtering the whole set each frame
    - the alignment panel now memoizes the sorted ICAO list instead of rebuilding it on every render
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_debug_perf.py`
    - Result: `66 passed in 0.83s`
    - `npm run build` in `frontend/`
    - Result: success

## 2026-04-09 Radar Lock-Contention Mitigation

- [x] Move Stage 1 radar IID analysis fully off the shared radar lock so decoder-path DF11 ingestion does not stall behind 30 s rotation updates
- [x] Reduce additive solver pressure while Stage 1 radar updates are running or the decoder queue is already under pressure
- [x] Verify with targeted backend tests and compile checks, then record the expected impact here

### Review

- Root cause confirmed from the new radar-specific perf metrics:
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) was still holding `radar_state._lock` while running `_analyse_iid_events(evs)` for every active IID during `update_rotation_models()`
  - that lock-held stage was averaging about `10 s` in the reported `analyse_swap_avg`, which caused decoder-thread `on_df11_event(...)` calls to block behind Stage 1 radar updates and eventually overflow the decode queue
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) was also allowing the FM loop to keep launching solver work even when Stage 1 radar updates or queue backlog already showed the system was under stress
- Mitigations implemented on 2026-04-09:
  - Refactored [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so `update_rotation_models()` now:
    - snapshots/prunes raw event state under lock
    - builds sweep history and runs `_analyse_iid_events(...)` for each IID off-lock
    - reacquires the lock only for the final model/sweep-history swap and reinforcement step
  - Added an `is_update_active()` flag in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) so other subsystems can avoid piling work on during Stage 1 radar refreshes
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) so `_fm_loop()` now:
    - skips a cycle if a radar update is in progress
    - skips a cycle if `_msg_queue.qsize()` is already above the backlog threshold
    - limits each cycle to the top `2` IIDs with new frames instead of trying to launch every eligible FM run at once
    - fixes the FM timing sample capture so `frame_count` is bound per run instead of by late closure state
- Expected impact:
  - `radar_rotation_ms.analyse_swap_avg` should collapse from seconds to a much smaller pointer-swap/update phase
  - `radar_decoder_ms.df11_event.max` should no longer show giant outliers dominated by waiting behind the radar lock
  - `msg_queue_stats` and `msg_drops_total` should remain stable during the 30 s radar refresh cadence
- Verification:
  - `python3 -m py_compile backend/main.py backend/radar/sweep.py backend/debug.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_timing_events.py`
  - Result: `48 passed in 0.39s`

- Follow-up after the first mitigation run:
  - The lock contention was fixed, but the new metrics then showed the dominant remaining Stage 1 cost had moved to `radar_rotation_ms.sweep_build_avg` at about `10.6 s`, while `analyse_swap_avg` had already fallen to about `9 ms`.
  - That showed the remaining problem was no longer lock structure; it was the total amount of sweep-history work being done every 30 seconds for all active IIDs.
  - Mitigation:
    - removed periodic sweep-history construction from `update_rotation_models()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py)
    - added lazy per-IID sweep-history construction in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py), so expensive sweep/dwell history is now built only on demand when `get_sweep_history()` or `get_dwell_profile()` is called
    - kept the 30-second radar maintenance path focused on rotation-model analysis only
  - Expected impact:
    - `radar_rotation_ms.sweep_build_avg`, `history_fetch_avg`, and `cache_build_avg` should drop close to zero during steady-state background operation
    - the 30-second radar update cadence should stop acting like a sustained decoder-throughput sink
    - sweep/dwell endpoints may be slower on first access for a given IID, but that cost is now paid only for the IID the operator is actually inspecting
  - Verification:
    - `python3 -m py_compile backend/radar/sweep.py backend/main.py backend/debug.py`
    - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_timing_events.py`
    - Result: `48 passed in 0.39s`

## 2026-04-09 Radar Worker Isolation

- [x] Move radar DF11 event handling off the main Beast decoder thread into a separate bounded radar queue/worker
- [x] Add debug metrics for radar queue depth and radar-only drops so overload is visible without inferring it from the main decoder queue
- [x] Verify with targeted backend tests and compile checks

### Review

- Motivation:
  - after the Stage 1 background mitigations, the periodic radar update path was cheap, but the main decoder queue was still saturating while `radar_decoder_ms.df11_event` remained materially non-zero
  - that meant the remaining cost was live radar DF11 handling on the main decoder thread, not the periodic maintenance loops
- Implementation:
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now owns a bounded `_radar_queue` and a dedicated `_radar_thread`
  - [benchmark.py](/home/keith/claude/adsb-dashboard/backend/benchmark.py) now emits predecoded radar DF11 events to a sink callback instead of calling radar processing inline on the decode thread
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now enqueues those radar events with `_enqueue_radar_event(...)`; if the radar queue is full, only radar events are dropped
  - the dedicated radar worker thread drains that queue and calls [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py)`on_df11_event(...)`
  - [debug.py](/home/keith/claude/adsb-dashboard/backend/debug.py) now reports:
    - `radar_queue_depth`
    - `radar_queue_stats`
    - `radar_drops_total`
- Functional implication:
  - core ADS-B decode is now isolated from radar overload
  - radar is now best-effort under load: it may lag or drop radar-only DF11 events, but it should no longer force drops in the main message decode path just because passive-radar processing is slow
- Verification:
  - `python3 -m py_compile backend/main.py backend/debug.py backend/benchmark.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_timing_events.py`
  - Result: `48 passed in 0.38s`
- Follow-up regression fix on 2026-04-10:
  - After the radar worker isolation, [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py)`on_df11_event(...)` was appending live DF11 events but no longer marking `self._dirty_iids`.
  - That broke Stage 1 rotation learning on the worker path, so fresh/live IIDs could stop updating `period_s`; downstream live SweepFrame building then appeared to stall because `_on_df11_frame_builder(...)` requires a learned period.
  - Fixed by restoring `self._dirty_iids.add(iid)` in the predecoded worker path and adding a regression test in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py).
  - Verification:
    - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
    - Result: `52 passed in 0.48s`

## DF4/5 IID Attribution — Future Work (Option 3)

Option 2 (implemented 2026-04-10) uses a per-ICAO recency cache in `aircraft_state.py`
to attribute DF4/5 timing events to the IID of the most recent DF11 from the same aircraft.
This works well for single-radar environments but has a known limitation in multi-radar
environments: the cache stores one IID per ICAO (last-write-wins), so if two radars
illuminate the same aircraft, the attributed IID may belong to whichever radar swept last.

Option 3 — proper temporal co-sweep attribution — should be implemented when:
- Multi-radar support is needed (two IIDs at the same airport), OR
- You want to measure the intra-dwell timing offset between DF4/5 and DF11 from the
  same aircraft (the offset is a fingerprint of the radar's interrogation sequence), OR
- You want to include Mode A/C-only aircraft (no DF11 at all) in the sweep detection pool.

**Design sketch:**
- Add `on_df45_event(iid_hint, icao, arrival_us, signal_dbfs)` to `RadarState` in `radar/sweep.py`.
- Maintain a short ring-buffer `{icao: [(arrival_us, iid)]}` of recent DF11 events in `RadarState`.
- When a DF4/5 arrives, find the nearest DF11 from the same ICAO within `CO_SWEEP_WINDOW_US`
  (70 ms, already defined).  If found, attribute to that IID and pass to `_on_df11_frame_builder`.
- For Mode A/C-only aircraft (no confirmed ICAO at all), use the dominant IID from the DF11
  burst cluster that arrived in the same 200 ms window, if unambiguous.
- This correctly handles multi-radar: two DF11s from different IIDs at T and T+50 ms will
  each claim their co-temporal DF4/5 rather than sharing last-write state.

## 2026-04-09 Radar Localisation Control And Evidence-Map Implementation Plan

Objective:
- Treat radar localisation as a calibration workflow rather than a continuously running solver.
- Let the operator declare an IID resolved, manually lock an authoritative radar position, or mark an IID unresolvable so expensive search work stops.
- Add a per-IID evidence map that shows how each localisation method derives position and allows visual comparison of method quality.

Proposed IID resolution modes:
- `auto`
  - Current behavior baseline: observation capture continues and eligible localisation methods may run automatically.
- `locked_position`
  - Operator-provided coordinates are authoritative for the IID.
  - Automatic localisation/search for that IID is disabled.
  - Visualisations and beam/sweep overlays use the locked position.
- `locked_unresolvable`
  - Operator asserts there is insufficient coherent pulse structure to justify further search.
  - Automatic localisation/search for that IID is disabled.
  - Lightweight timing/alignment observation capture may continue, but no candidate location is published.

Associated operator actions:
- `set_manual_position(iid, lat, lon, note?)`
- `lock_position(iid)`
- `unlock_position(iid)` → returns to `auto`
- `mark_unresolvable(iid, reason?)`
- `clear_unresolvable(iid)` → returns to `auto`

Phase 1: State model and persistence
- [x] Extend the `RadarIID` runtime model in [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py) with:
  - `resolution_mode` (`auto`, `locked_position`, `locked_unresolvable`)
  - `manual_lat`
  - `manual_lon`
  - `manual_note`
  - `manual_updated_ts`
  - `unresolvable_reason`
  - `unresolvable_updated_ts`
- [x] Extend DB persistence for `radar_iids` in [db.py](/home/keith/claude/adsb-dashboard/backend/db.py):
  - migration for the new columns
  - read/write support in `load_radar_iids()` and `upsert_radar_iid()`
- [x] Define authoritative-position semantics:
  - if `resolution_mode == locked_position`, authoritative radar position for UI/API = manual lat/lon
  - if `resolution_mode == auto`, authoritative radar position remains the currently active solved method
  - if `resolution_mode == locked_unresolvable`, no authoritative solved position is exposed unless explicitly requested as historical/reference-only

### Phase 1 Review

- Implemented on 2026-04-10:
  - [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py) now persists localisation control state on `RadarIID` via:
    - `resolution_mode`
    - manual position/note/timestamp fields
    - unresolvable reason/timestamp fields
  - [db.py](/home/keith/claude/adsb-dashboard/backend/db.py) now:
    - creates/migrates the new `radar_iids` columns
    - writes them in `upsert_radar_iid()`
    - exposes them through `load_radar_iids()`
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) startup load now restores those fields into in-memory `RadarIID` models.
- Authoritative-position semantics recorded for Phase 2 enforcement:
  - `locked_position`: manual coordinates are the authoritative display position.
  - `auto`: current solved method remains authoritative.
  - `locked_unresolvable`: automatic solved position should be suppressed from normal UI/API presentation.
- Verification:
  - `python3 -m py_compile backend/db.py backend/radar/models.py backend/radar/sweep.py backend/tests/test_db.py`
  - `uv run --directory backend pytest tests/test_db.py tests/test_radar_sweep.py tests/test_radar_api.py`
  - Result: `65 passed in 0.76s`

Phase 2: Backend API and solver gating
- [x] Add radar control endpoints in [radar/api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) for:
  - set/update manual position
  - lock/unlock position
  - mark/clear unresolvable
  - fetch current resolution control state
- [x] Gate background solver work in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py):
  - `_fm_loop()` skips IIDs in `locked_position`
  - `_fm_loop()` skips IIDs in `locked_unresolvable`
  - any future TDOA/coincident-illumination/background localisation loops must obey the same gating
- [x] Gate publication paths in [radar/api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py):
  - map/list/detail endpoints should expose both:
    - `resolution_mode`
    - authoritative displayed position source (`manual`, `fm`, `tdoa`, `none`)
  - `locked_position` should display the manual position as authoritative
  - `locked_unresolvable` should suppress automatic “candidate” presentation as if the IID is still actively being solved
- [x] Preserve low-cost live timing capture for all modes unless explicitly disabled:
  - `locked_position`: still useful for beam animation and observational diagnostics
  - `locked_unresolvable`: still useful for cycle-alignment inspection, but should not trigger solver work

### Phase 2 Review

- Implemented on 2026-04-10:
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes control endpoints for:
    - `GET /api/radar/iids/{iid}/control`
    - `POST /api/radar/iids/{iid}/manual-position`
    - `POST /api/radar/iids/{iid}/lock-position`
    - `POST /api/radar/iids/{iid}/unlock-position`
    - `POST /api/radar/iids/{iid}/mark-unresolvable`
    - `POST /api/radar/iids/{iid}/clear-unresolvable`
  - Those control changes flush immediately to SQLite via `stats_db.upsert_radar_iid(...)` so they survive restart.
  - [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now owns the in-memory localisation-control mutators:
    - `set_manual_position`
    - `lock_manual_position`
    - `unlock_position`
    - `mark_unresolvable`
    - `clear_unresolvable`
    - `localisation_enabled`
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now skips non-`auto` IIDs in `_fm_loop()`, so operator-locked/manual-unresolvable IIDs no longer consume FM solver time in the background.
  - API publication now uses authoritative display semantics:
    - `locked_position` publishes the manual location as authoritative
    - `auto` prefers FM position when present, else TDOA, else none
    - `locked_unresolvable` suppresses automatic position presentation
  - The following responses now expose `resolution_mode` and authoritative display metadata:
    - `GET /api/radar/iids`
    - `GET /api/radar/iids/{iid}/rotation`
    - `GET /api/radar/rotation`
    - `GET /api/radar/iids/{iid}/location`
    - `GET /api/radar/map`
- Current scope note:
  - Manual `POST /api/radar/iids/{iid}/fm-run` remains available for explicit debugging; the automatic background FM loop is what is gated in this phase.
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/radar/sweep.py backend/main.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_db.py tests/test_radar_sweep.py tests/test_radar_api.py`
  - Result: `69 passed in 0.74s`

Phase 3: Evidence-map backend
- [x] Define one shared per-IID evidence-map payload family in [radar/api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) with method-specific layers.
- [x] Add evidence endpoints for:
  - `inscribed_angle`
    - aircraft positions used
    - circle/locus definitions
    - candidate intersection region / estimate
  - `coincident_illumination`
    - aircraft groups/pairs identified as coincident
    - derived beam/bearing lines
    - crossing/intersection summaries
  - `tdoa`
    - calibration aircraft pairs
    - hyperbolic curves
    - active estimate / residual summaries
  - `forward_model`
    - sweep-frame aircraft positions
    - reference aircraft
    - phase/bearing-derived geometry summaries
    - active estimate
- [x] Keep the geometry payloads compact and map-oriented:
  - raw lines/circles/curves/polylines + labels/metadata
  - avoid embedding presentation-specific formatting in the API
- [x] Standardize a common overlay metadata shape:
  - `method`
  - `label`
  - `geometry_type`
  - `confidence` / `weight`
  - `source_count`
  - optional `active_estimate`

### Phase 3 Review

- Implemented on 2026-04-10:
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes a shared evidence-map payload family with:
    - `GET /api/radar/iids/{iid}/evidence`
    - `GET /api/radar/iids/{iid}/evidence/{method}`
  - The common layer shape is now:
    - `method`
    - `label`
    - `geometry_type`
    - `confidence`
    - `source_count`
    - `active_estimate`
    - `features`
  - Implemented method-specific geometry payloads:
    - `tdoa`
      - receiver point
      - calibration aircraft points
      - calibration-pair line segments
      - hyperbola polylines
      - current TDOA estimate point when available
    - `coincident_illumination`
      - coincident-aircraft points
      - extended beam-line constraints derived from near-coincident pairs
      - current displayed estimate point for visual comparison when available
    - `forward_model`
      - sweep-frame reference/observation aircraft points
      - beam/bearing lines from the FM estimate to those aircraft
      - current FM estimate point when available
    - `inscribed_angle`
      - sweep aircraft points used for intersection solving
      - inscribed-angle circle definitions
      - current FM/intersection estimate point when available
- Scope note:
  - This phase is read-only and evidence-oriented. It reuses existing runtime data and persisted calibration pairs rather than adding new solver passes or frontend rendering.
  - The coincident-illumination payload currently exposes evidence lines, not an independent solved point. The actual method-design decision remains in Phase 4.
- Verification:
  - `python3 -m py_compile backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_db.py`
  - Result: `72 passed in 1.07s`

Phase 4: Coincident-illumination method support
- [x] Add coincident-illumination solver output to the backend model/persistence so it can participate in method comparison and final result selection.
- [x] Define and implement detection criteria:
  - coincidence window
  - minimum repeated evidence count
  - de-duplication / family grouping
  - how one coincidence event maps to a bearing-line constraint
- [x] Implement coincident illumination as an independent solver producing its own candidate point and quality metrics.
- [x] Add method-comparison output so the UI can compare `manual`, `coincident_illumination`, `fm`, `tdoa`, and any combined solution.
- [x] Update final-result selection semantics:
  - `locked_position` still wins when set
  - otherwise compare available automatic methods by quality
  - where multiple automatic methods agree closely enough, expose a combined weighted result

### Phase 4 Review

- Implemented on 2026-04-10:
  - [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py), [db.py](/home/keith/claude/adsb-dashboard/backend/db.py), and [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) now store coincident-illumination localisation results as first-class IID state:
    - `ci_lat`
    - `ci_lon`
    - `ci_cep_m`
    - `ci_source`
    - `ci_n_pairs`
    - `ci_last_updated`
  - [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py) now includes `solve_coincident(...)`, which:
    - filters to locally consistent repeated pair families
    - keeps near-coincident pairs
    - converts each pair into a bearing-line constraint
    - intersects those lines in local ENU space
    - clusters plausible crossings
    - emits a coincident-illumination candidate point plus RMS-based quality metric
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) now runs a bounded `_coincident_loop()` in the background:
    - skips non-`auto` IIDs
    - skips when radar maintenance or decode backlog already shows the system is under pressure
    - persists any new coincident-illumination result through `stats_db.upsert_radar_iid(...)`
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now compares automatic solutions rather than hard-coding FM over TDOA:
    - available automatic methods: `coincident_illumination`, `fm`, `tdoa`
    - `GET /api/radar/iids/{iid}/solution-comparison` now returns all available methods plus the selected result
    - when two or more automatic methods agree within the configured distance threshold, the API exposes a weighted `combined` result
    - authoritative display selection now becomes:
      - `manual` when locked
      - otherwise best automatic method or `combined`
      - `none` when unresolvable or unsolved
  - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) location/map/list responses now inherit that comparison-based selection logic, so CI or combined results can become the displayed solution without special casing in the frontend.
- Detection/quality criteria implemented in the coincident solver:
  - requires repeated stable pair families first
  - then near-coincident TDOA gating
  - rejects sparse or near-parallel bearing-line sets
  - rejects weak crossing clouds that fail the minimum cluster/intersection thresholds
- Verification:
  - `python3 -m py_compile backend/radar/models.py backend/db.py backend/radar/sweep.py backend/radar/localiser.py backend/radar/api.py backend/main.py backend/tests/test_db.py backend/tests/test_radar_api.py backend/tests/test_radar_localiser.py`
  - `uv run --directory backend pytest tests/test_db.py tests/test_radar_api.py tests/test_radar_localiser.py tests/test_radar_sweep.py`
  - Result: `91 passed in 1.63s`

Phase 5: Frontend controls
- [x] Add IID control UI on [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx):
  - current resolution mode
  - lock manual position controls
  - mark unresolvable controls
  - unlock / clear controls
- [x] Surface authoritative-source state in the IID table/detail panels:
  - `auto`
  - `manual locked`
  - `unresolvable`
  - displayed location source label
- [x] Add operator warnings/notes when:
  - a manual lock overrides an automatic solution
  - an IID is excluded from solver work because it is marked unresolvable

### Phase 5 Review

- Implemented on 2026-04-10:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now includes a `Localisation Control` panel with:
    - current `resolution_mode`
    - displayed authoritative source
    - manual lat/lon/note editing
    - lock/unlock actions
    - mark/clear unresolvable actions
  - Those controls call the new backend control endpoints directly and refresh the page state immediately after updates.
  - The page now includes a `Method Comparison` panel driven by `GET /api/radar/iids/{iid}/solution-comparison`, so operators can see:
    - each available automatic method
    - the selected final result
    - CEP and timing metadata
    - when a `combined` solution has been selected
  - Existing detail panels now inherit the backend’s comparison-based display source, so manual/CI/FM/TDOA/combined selection is surfaced consistently rather than recomputed client-side.
- Verification:
  - `npm run build` in `frontend/`
  - Result: success

Phase 6: Evidence-map frontend
- [x] Add a shared per-IID evidence map panel on [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx).
- [x] Add layer toggles for:
  - receiver
  - authoritative radar position
  - inscribed-angle overlays
  - coincident-illumination beam lines
  - TDOA hyperbolas
  - forward-model frame geometry
  - aircraft positions used by each method
- [x] Add method comparison affordances:
  - show each method’s active estimate
  - show CEP / uncertainty region where available
  - allow users to compare overlap/divergence between methods visually
- [x] Keep the map usable as an explanatory tool:
  - clicking a method should highlight only its evidence
  - labels should identify the aircraft/pairs involved
  - the operator should be able to understand why a method produced a given position

### Phase 6 Review

- Implemented on 2026-04-10:
  - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now includes an `Evidence Map` panel backed by `GET /api/radar/iids/{iid}/evidence`.
  - The frontend renders the backend evidence layers as a lightweight projected SVG map instead of requiring a full basemap stack.
  - Method toggles now let the operator show or hide:
    - inscribed-angle circles
    - coincident-illumination beam lines
    - TDOA hyperbolas
    - forward-model geometry
  - Follow-up clarity pass on 2026-04-10:
    - the selected final authoritative estimate is now rendered with an explicit crosshair marker and label
    - per-method estimates are now rendered as distinct square markers
    - CEP is now rendered as a visible radius ring for both method estimates and the selected final result when available
    - the panel now includes summary pills for final source, CEP, and selected coordinates
    - the panel now includes a legend so operators can distinguish final result, method estimates, CEP rings, and raw evidence points
  - The evidence view is intentionally explanatory rather than decorative: it shows the actual constraint geometry and lets the operator compare method overlap/divergence visually.
  - Follow-up observability pass on 2026-04-10:
    - [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py) now exposes coincident-solver diagnostics including:
      - total calibration pairs
      - stable repeated pair families
      - coincident-pair count
      - usable beam-line count
      - intersection count
      - best-cluster size and RMS
      - azimuth spread
      - top repeated aircraft-pair families with timing spread summaries
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes `GET /api/radar/iids/{iid}/coincident-diagnostics` so the operator can see whether the solver is blocked by insufficient repeated evidence, poor geometry, or low intersection support.
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now includes a `Coincident Solver` panel showing:
      - current solver stage/status
      - current blocker text
      - key readiness counters
      - stored CI solution and current cluster estimate when present
      - the strongest repeated aircraft-pair families contributing evidence
  - Follow-up method-comparison pass on 2026-04-10:
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now returns all localisation methods in `GET /api/radar/iids/{iid}/solution-comparison`, not just solved outputs.
    - The response now distinguishes:
      - `solved`
      - `collecting`
      - `blocked`
      - `inactive`
      - `paused`
      - `evidence_only`
      - `waiting`
      - `locked` / `available` / `unset` for manual override state
    - This makes the panel explicitly show that TDOA currently has evidence plumbing but no active background solver loop, so it may appear as `inactive` with stored calibration-pair counts even when no TDOA position has been solved.
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now renders a `Status` column and method notes so unsolved methods remain visible and auditable instead of disappearing from the table.
  - Follow-up manual-trigger pass on 2026-04-10:
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes `POST /api/radar/iids/{iid}/tdoa-run` for an immediate manual TDOA solve.
    - The manual TDOA path tries the sweep-accumulated TDOA solve first, then falls back to the direct TDOA solve if the sweep solve cannot produce a result.
    - Successful manual runs persist the solved TDOA position through [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py)`update_localisation(...)`.
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now exposes clearly labeled `Run FM` and `Run TDOA` buttons in the IID selector instead of a single unlabeled run icon.
  - Follow-up TDOA observability pass on 2026-04-10:
    - [localiser.py](/home/keith/claude/adsb-dashboard/backend/radar/localiser.py) now exposes `tdoa_diagnostics(...)` summarising:
      - total, consistent, and solver-selected pair counts
      - sweep-group counts and average/max pairs per sweep
      - selected-pair azimuth spread
      - per-sweep failure breakdown from `diagnose_sweep_groups(...)`
      - current blocker state (`no_pairs`, `insufficient_consistent_pairs`, `poor_geometry`, `insufficient_sweeps`, `direct_only`, `ready`)
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) now exposes `GET /api/radar/iids/{iid}/tdoa-diagnostics`, including the stored TDOA solution and the outcome of the last manual TDOA run.
    - Manual TDOA runs now record runtime metadata on the IID model:
      - success/failure
      - method used (`tdoa_sweeps` or `tdoa_direct`)
      - elapsed time
      - pair counts
      - failure reason
      - sweep-stage fallback reason when applicable
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) now includes a `TDOA Solver` panel that shows:
      - readiness counters
      - current gating reason
      - sweep failure breakdown
      - stored TDOA solution
      - last manual run outcome and failure reason
  - Follow-up uncertainty-label pass on 2026-04-10:
    - Operator-facing radar UI labels now describe solver `cep_m` values as `uncertainty` rather than `CEP`, because the current numbers are heuristic fit/spread scores rather than calibrated real-world error radii.
    - [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py)`_control_payload(...)` now exposes:
      - `best_effort_source`
      - `best_effort_lat`
      - `best_effort_lon`
      - `best_effort_cep_m`
      - `manual_reference_error_m`
    - [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx)`LocalisationControlPanel` now shows `Model Error vs Manual` whenever a manual position exists and a best automatic solution is available.
- Verification:
  - `npm run build` in `frontend/`
  - Result: success
  - `python3 -m py_compile backend/radar/localiser.py backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_localiser.py`
  - Result: `40 passed in 0.70s`
  - `python3 -m py_compile backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py`
  - Result: `27 passed in 0.62s`
  - `python3 -m py_compile backend/radar/localiser.py backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_localiser.py`
  - Result: `43 passed in 0.93s`
  - `python3 -m py_compile backend/radar/api.py backend/tests/test_radar_api.py`
  - `uv run --directory backend pytest tests/test_radar_api.py`
  - Result: `29 passed in 0.69s`

Phase 7: Automatic stop conditions
- [ ] Add optional automatic freeze rules for `auto` mode:
  - if an IID reaches a stable, high-confidence position and remains stable for a hold window, stop periodic search work automatically
  - allow manual override back to `auto` if the operator wants to resume searching
- [ ] Make auto-freeze subordinate to explicit operator state:
  - never auto-unlock a `locked_position`
  - never auto-restart a `locked_unresolvable`

Verification plan:
- [ ] Backend model/persistence tests:
  - new DB fields round-trip correctly
  - runtime load/save preserves `resolution_mode` and manual/unresolvable fields
- [ ] Solver gating tests:
  - locked-position IID is skipped by FM/background localisation
  - locked-unresolvable IID is skipped by FM/background localisation

Deferred follow-up considerations:
- [ ] Revisit coincident-illumination gating so coincidence is measured on receiver-path-corrected illumination time rather than raw reply-arrival `tdoa_us`.
- [ ] Evaluate aircraft-pair separation as a quality/weighting signal for coincident-illumination and TDOA pair selection.
  - Intention: test current behaviour first without this extra heuristic, then add it only if diagnostics show weak or misleading pair geometry is still a practical problem.
  - `auto` IID still runs normally
- [ ] API tests:
  - control endpoints mutate and return the expected state
  - map/detail endpoints expose authoritative source and resolution mode correctly
- [ ] Frontend verification:
  - `npm run build`
  - focused manual test of lock/unlock/unresolvable controls
  - focused manual test of evidence-map layer toggles and method comparison flow

Open design decisions to resolve during implementation:
- [ ] Whether manual position should overwrite persisted FM/TDOA fields or remain separate and authoritative only at presentation/gating time.
- [ ] Whether coincident illumination ships first as evidence-only overlays or as a full independent solver.
- [ ] Whether sweep-history / method-evidence payloads should be recomputed on demand only, cached per IID, or prewarmed for the selected IID.

### Review

- This plan aligns the system with the operational reality that radar sites are effectively static and expensive search should stop once the operator has enough confidence to identify or reject a site.
- The highest-leverage first implementation slice is:
  - Phase 1
  - Phase 2
  - minimal Phase 5 controls
- The evidence-map work is larger, but it is the right long-term operator tool because it makes each localisation method auditable rather than opaque.

## 2026-04-09 Live Reference-Pulse Beam Anchor

- [x] Re-anchor the receiver-centred beam to the latest live DF11 pulse of the current reference ICAO instead of the polled sweep-frame `ref_arrival_us`
- [x] Keep the beam bearing reference tied to the solved-radar geometry for that reference aircraft
- [x] Verify with `npm run build` and record review notes here

### Review

- The beam is no longer phase-zeroed only from the chosen sweep frame’s stored `ref_arrival_us`.
- In `frontend/src/pages/RadarPage.jsx`, the beam now:
  - keeps the solved-radar/reference-aircraft bearing from the selected sweep-frame anchor
  - updates that anchor timestamp to the latest live DF11 pulse seen on the timing websocket for the current reference ICAO on the selected IID
- This keeps the geometric bearing tied to the sweep-frame solution while letting phase zero follow the current live reference-aircraft pulse stream.
- Verification:
  - `npm run build`
  - Result: success

## 2026-04-09 Receiver-Centred Beam Jitter Fix

- [x] Switch the receiver-centred beam from the polling-based DF11 helper clock to the live timing websocket clock
- [x] Stabilise the chosen beam reference frame so the anchor does not jump between polled sweep-frame snapshots
- [x] Remove any now-unused DF11 flash helper code from `frontend/src/pages/RadarPage.jsx`
- [x] Verify with `npm run build` and record review notes here

### Review

- The beam now uses the live timing websocket clock already feeding the receiver-centred DF11 point field, instead of the polled DF11 flash helper.
- Added a stable beam anchor in `frontend/src/pages/RadarPage.jsx`:
  - prefer frames matching the current reference aircraft when available
  - otherwise keep using the current anchor reference aircraft if possible
  - only advance the anchor when a newer compatible frame arrives
- Removed the now-unused `useDF11Flash()` helper from `frontend/src/pages/RadarPage.jsx`, which also removes another polling-driven beam timing path.
- Verification:
  - `npm run build`
  - Result: success

## 2026-04-09 Receiver-Centred Radar Field Cleanup

- [x] Remove the old radar-centred sweep plot from the page and clean up any dead frontend code it leaves behind
- [x] Re-anchor the new receiver-centred beam to live Beast time from the radar DF11 path so it stays synchronised to the reference aircraft
- [x] Narrow the beam wedge so the conical area is visually tighter
- [x] Verify with `npm run build` and record review notes here

### Review

- Removed the old `SweepDiagram` from `frontend/src/pages/RadarPage.jsx` and dropped its page slot.
- The new receiver-centred field now uses the radar DF11 Beast-time anchor from `useDF11Flash(iid, latestArrivalUs)` for beam phase, instead of the delayed message-field render clock.
- Point persistence still uses the delayed message-field clock, but beam rotation now follows live Beast time so it stays aligned to the frame reference aircraft rather than lagging by the persistence holdback.
- Tightened the beam wedge from roughly `±10°` to `±4°`.
- Verification:
  - `npm run build`
  - Result: success

## 2026-04-09 Receiver-Centred Radar Field Display

- [x] Trace the existing message-field polar plot and identify the minimal reusable timing-event path for a radar-page receiver-centred DF11 view
- [x] Add a new radar-page live field panel that:
  - uses the timing-event websocket/buffer
  - filters to DF11 for the selected IID
  - renders a receiver-centred polar bearing/range field with about 3 s persistence
  - scales range from aircraft positions the same way as the message-field page
- [x] Overlay the beam sweep using the solved radar position and measured sweep period so off-centre radars sweep across the receiver-centred field rather than forcing a radar-centred disc
- [x] Verify with `npm run build` and record the review notes here

### Review

- Implemented a new `Receiver-Centred Radar Field` panel in `frontend/src/pages/RadarPage.jsx`.
- Reused the existing timing-event websocket and buffer (`useTimingEventStream`, `useTimingEventBuffer`) rather than adding another polling path.
- The panel now:
  - filters timing events to DF11 for the selected IID
  - renders a receiver-centred polar bearing/range field with fixed 3 s persistence
  - uses the same dynamic range scaling model as the message-field page
  - projects the solved radar position from FM into the receiver-centred field and draws the beam sweep from that off-centre origin
- Verification:
  - `npm run build`
  - Result: success

## 2026-04-09 Sweep Diagram FM Position Availability

- [x] Confirm why `SweepDiagram` shows `No FM position` while the IID list still shows a CEP
- [x] Patch the diagram to use the current persisted FM location, not only convergence-history tail points
- [x] Verify the display contract with the backend API and run frontend verification

### Review

- Root cause: `SweepDiagram` was deriving `fmPos` only from `/api/radar/iids/{iid}/fm-convergence`, specifically the last convergence-history point. The IID list CEP/path uses persisted `model.fm_lat`/`model.fm_lon` from `/api/radar/iids/{iid}/fm-location`, so a radar could be localised without any retained convergence history and the diagram would still render `No FM position`.
- Fix: added a dedicated `useFmLocation(iid)` hook in `frontend/src/pages/RadarPage.jsx` and switched `SweepDiagram` to use `/fm-location` as the authoritative solved position source.
- Verification:
  - `npm run build`
  - Result: success

## 2026-04-09 Requirements Compliance Assessment

- [x] Read `.docs/radar_localisation_angle_method.md` and `.docs/critical_requirements.md`, extract the concrete algorithm and hard requirements to assess
- [x] Trace the current implementation and tests across the radar localisation pipeline, from raw timing input through sweep/pair construction to solver/publication
- [x] Compare implementation against each requirement, classify compliance (`meets`, `partial`, `missing`, `not verifiable here`), and capture evidence with file references
- [x] Write a Markdown assessment report in `.docs` summarising findings, risks, and the highest-priority gaps
- [x] Add a review section here with verification notes and the report path

### Review

Report:
- `.docs/radar_localisation_requirements_assessment.md`

Headline findings:
- The repository contains several strong localisation building blocks, including Beast-time preservation, amplitude-weighted burst centres, receiver-path correction in the TDOA solver, and meaningful forward-model intersection quality gates.
- The current production localisation path is the forward model in `backend/main.py`; the documented angle-method / TDOA localiser is not wired in as the live background localisation pipeline.
- After the revised REQ-01 and REQ-02, the timing-source and clock-discipline assessment improved: the current Beast/SDR timing path is broadly compatible in principle, but the repository still does not verify receiver timing quality or TCXO stability at runtime.
- Several other hard requirements from `.docs/critical_requirements.md` remain unenforced in code, especially ADS-B calibrator quality thresholds and the documented minimum evidence rules for rotation-rate readiness.
- Co-sweep handling is reasonably aligned in the live sweep builder, but weaker in the offline/localiser regrouping path.

Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_localiser.py tests/test_forward_model.py tests/test_forward_model_integration.py tests/test_db.py`
- Result: `107 passed in 1.02s`

## 2026-04-08 Radar Localisation Audit

- [x] Map the radar location-finding path from sweep construction through pair generation, persistence, and solver entry points
- [x] Audit localisation correctness risks that can reduce solved-location accuracy
- [x] Audit performance costs in the radar finding path, especially where they interact with accuracy
- [x] Check existing backend coverage for the identified risks and note missing regressions
- [x] Add a short review section here with findings, recommended fixes, and verification notes

### Review

Scope reviewed:
- `backend/radar/sweep.py`
- `backend/radar/localiser.py`
- `backend/radar/forward_model.py`
- `backend/main.py`
- related radar/db tests

Findings:
- High: the only live background location finder is the forward-model loop in `backend/main.py`; there is no equivalent production loop invoking `RadarLocaliser.solve()` or `solve_static_from_sweeps()`. The TDOA path generates pairs and exposes hyperbolas, but it is not currently part of the live localisation update pipeline. Any accuracy work on TDOA alone will not improve the position shown by the running system until that path is actually wired in.
- High: ADS-B fallback projection can move aircraft in the wrong time direction. In `backend/radar/sweep.py`, `_lookup_adsb_position()` chooses the nearest point using `abs(target_wall_ts - candidate["ts"])`, then calls `_project_position_from_sample(nearest, nearest_dt)` with a forced-positive delta. If the nearest sample is after the target time, the code projects forward instead of backward, which biases burst positions and therefore both TDOA pair geometry and forward-model sweep geometry.
- High: TDOA sweep observation reconstruction silently overwrites duplicate pair edges and never checks cycle consistency. In `backend/radar/localiser.py`, `_build_sweep_observations()` stores one `pair_tdoa[(icao_a, icao_b)] = pair.tdoa_us`, so if a grouped sweep contains repeated edges the last one wins. The BFS then trusts those edges as exact relative-arrival constraints. This is fragile for noisy or duplicated persisted data and can shift a whole sweep solve.
- Medium: TDOA repeated-family gating is based on raw `tdoa_us` spread only, without removing the known geometry term. In `backend/radar/localiser.py`, `filter_consistent_pairs()` keeps or rejects families using only short-window spread in observed TDOA. That can reject valid moving-aircraft families when geometry changes, and it can also keep locally stable but semantically wrong families. Consistency should be evaluated on receiver-path-corrected residuals or on per-sweep-normalised timing, not raw observed TDOA alone.
- Medium: sweep grouping for static TDOA solving uses coarse wall-clock timestamps instead of the underlying Beast/beam-centre timing. In `backend/radar/localiser.py`, `group_pairs_by_sweep()` clusters by persisted `pair.ts` with a `0.25 s` window. Since `pair.ts` is derived from `time.time()`-based wall-clock estimation in `backend/radar/sweep.py`, that grouping is much weaker than grouping directly on Beast-relative sweep timing and can merge adjacent sweeps or split one sweep across restart/jitter boundaries.
- Medium: pair generation still looks up aircraft position at the sweep centroid, not at each aircraft burst centre. In `backend/radar/sweep.py`, `update_calibration_pairs()` calls `_get_aircraft_position(icao, centroid_us, ...)` even though `beam_center_us` is already available and then uses `beam_center_us` only for TDOA. That means geometry and timing are evaluated at different instants. The error is bounded, but it is systematic and directly harms localisation accuracy.
- Medium: the active forward-model solver weights every surviving circle intersection equally. In `backend/radar/forward_model.py`, `solve_by_intersection()` only drops very weak angles via `min_sin_phi`, then treats all circles and all circle-circle intersections the same. Large-radius, low-information constraints and duplicated aircraft pairs can dominate the candidate cloud just as much as strong, well-separated constraints. This is a likely source of noisy or biased live positions.
- Low: `score_candidate_position()` recomputes the reference bearing inside the inner observation loop and leaves `period_us` unused. This is not the main bottleneck, but it is unnecessary work in the hottest forward-model scoring function.

Accuracy-first recommendations:
- Decide which location finder is authoritative. If TDOA is intended to matter, add a production localisation loop, store accepted results, and gate publication against forward-model quality instead of leaving the TDOA solver dormant.
- Fix signed-time projection in `_lookup_adsb_position()` and add a regression test covering the “nearest sample is after target time” case.
- In TDOA sweep solving, aggregate duplicate edges per aircraft pair with a robust estimator (median or trimmed mean) and reject connected components whose pair graph is cycle-inconsistent beyond a small residual tolerance.
- Replace raw repeated-family `tdoa_us` stability checks with geometry-aware residual checks, ideally after subtracting the known receiver-to-aircraft term and normalising within a true sweep grouping.
- Carry Beast-relative or per-sweep burst timing through calibration persistence so `group_pairs_by_sweep()` can cluster on real sweep time instead of reconstructed wall time.
- Use `beam_center_us` for both TDOA timing and aircraft position lookup in pair generation so the observation geometry is evaluated at the same instant as the timing measurement.
- For the forward model, weight circle constraints by information content and observation quality: stronger `|sin(phi)|`, non-interpolated positions, stronger signal, diverse frames, and aircraft uniqueness should contribute more than weak or duplicated constraints.
- Add solver diagnostics that report how much of the estimate comes from interpolated/projected positions, weak-angle constraints, and repeated aircraft. That will make bad solves explainable instead of just “large CEP”.

Performance opportunities:
- Cache local XY conversions and corrected range terms per pair inside TDOA residual evaluation instead of recomputing `_latlon_to_xy()` and receiver-path terms on every least-squares call.
- Precompute forward-model per-frame reference bearings/phase terms where possible; `score_candidate_position()` currently repeats work for every candidate evaluation.
- In `solve_by_intersection()`, bucket or subsample circle intersections instead of evaluating every pairwise circle combination once frame counts grow. The current intersection stage is quadratic in the number of circles.
- Avoid repeated sorting of ADS-B histories on every `_lookup_adsb_position()` call by keeping time-ordered history or binary-search-ready arrays per ICAO.

Coverage gaps to add:
- Signed backward projection test for `_lookup_adsb_position()`
- Duplicate-edge and inconsistent-cycle regression tests for `_build_sweep_observations()`
- A test showing geometry-aware family consistency keeps a valid moving-aircraft family that raw `tdoa_us` spread would reject
- A test proving `update_calibration_pairs()` uses per-aircraft beam-centre timestamps for geometry, not only for timing
- Forward-model tests that compare weighted vs unweighted weak-angle/duplicate-aircraft cases

Verification:
- `uv run --directory backend pytest tests/test_radar_localiser.py tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py tests/test_db.py`
- Result: `73 passed in 0.96s`

## 2026-04-08 Forward-Model Accuracy Rectification Plan

Objective:
- Improve the accuracy of the live radar location finder used by the geometric forward-model path, without incorporating the dormant TDOA solver.

Stage 1: Input timing and geometry correctness
- [x] Fix signed-time ADS-B projection in `backend/radar/sweep.py` so nearest-sample fallback projects backward when the nearest point is after the target burst time
- [x] Audit forward-model burst/sweep geometry timestamps and ensure aircraft geometry is evaluated at the same instant used for phase timing wherever practical
  - Result: the active live forward-model frame builder already uses burst-centre timing (`burst_centroid_us`) for both frame timing and ADS-B tracker lookup, so no additional Stage 1 code change was required there
- [x] Add focused regressions for:
  - backward-time projection from a future ADS-B sample
  - forward-time projection from a past ADS-B sample
  - no-projection behavior when the gap exceeds the configured extrapolation window
- [x] Run the radar sweep and forward-model test subsets after the timestamp fix
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
  - Result: `42 passed in 0.71s`

Stage 2: Observation quality modeling
- [x] Introduce a forward-model observation-quality score that combines:
  - phase information strength (`|sin(phi)|`)
  - signal quality (`signal_dbfs`)
  - position reliability (`interpolated` / projected vs direct)
  - geometric leverage (aircraft separation / non-degenerate baseline)
- [x] Precompute these quality terms once per pipeline run so the same quality model is used consistently by live solving and any optional refinement path
- [x] Add focused tests proving stronger, direct, high-information observations outrank weaker interpolated ones
  - Implemented in `backend/radar/forward_model.py` via shared observation-quality helpers and preprocessed scoring frames
  - The live intersection path now also ignores clearly weak observations before circle construction
  - Verified with:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
    - Result: `44 passed in 0.41s`

Stage 3: Better circle selection before solving
- [x] Change `solve_by_intersection()` to rank candidate circles by observation quality before the circle-circle intersection stage
- [x] Add caps so a single frame or a single repeated ICAO cannot dominate the solve purely by count
- [x] Prefer azimuthally diverse constraints over redundant same-sector constraints when trimming the candidate set
- [x] Tighten or adapt the weak-angle rejection threshold so near-0° / near-180° phase pairs do not materially influence the live solve
- [x] Add tests covering:
  - repeated-aircraft oversampling does not swamp diverse geometry
  - weak-angle circles are pruned ahead of stronger constraints
  - frame-level caps preserve a better estimate than unbounded same-frame accumulation
  - Implemented in `backend/radar/forward_model.py` via a selected-intersection-observation stage ahead of circle construction
  - Current policy:
    - cap observations per frame
    - cap repeated ICAO use across frames
    - prefer one high-quality observation per azimuth bin before filling remaining frame slots
    - prune low-quality and weak-angle observations before forming circles
  - Verified with:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
    - Result: `47 passed in 0.38s`

Stage 4: Better consensus from intersections
- [x] Replace the current unweighted MAD-trimmed mean with a weighted consensus step:
  - either weighted densest-cluster selection followed by weighted center fit
  - or weighted geometric median over the best cluster
- [x] Carry source-circle quality into the intersection candidates so intersections from two weak circles count less than intersections from two strong circles
- [x] Add ambiguity handling when multiple clusters compete:
  - prefer the denser / higher-weight cluster
  - reject publication when no cluster clearly dominates
- [x] Add tests covering:
  - mixed strong/weak candidate clouds
  - bimodal candidate sets
  - outlier-heavy candidate sets where the current mean would drift
  - Implemented in `backend/radar/forward_model.py` via weighted intersection candidates and a dominant-cluster resolver
  - The live solver now:
    - weights each intersection by the product of its source observation qualities
    - selects the dominant weighted neighborhood rather than averaging the whole cloud
    - rejects ambiguous bimodal clouds when no cluster clearly dominates
  - Verified with:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
    - Result: `50 passed in 0.39s`

Stage 5: Publication gating and refinement
- [x] Add explicit solve-quality gates before storing a new forward-model position:
  - minimum azimuth spread
  - minimum number of high-quality frames
  - cap on projected/interpolated observation fraction
  - dominance threshold for the chosen intersection cluster
- [x] Revisit CEP estimation so it reflects weighted cluster spread and not only raw surviving-point RMS
- [x] Decide whether to enable `run_2d_optimisation()` as an optional refinement pass only when the intersection seed and cluster quality are already strong
  - Decision for now: keep refinement manual-only
  - Rationale: the live intersection solver now has explicit solve-quality gates and weighted-cluster CEP semantics, but automatic refinement has not yet been proven to improve already-good seeds without adding regressions
- [x] Add tests for:
  - refusing low-quality solves that would previously publish
  - accepting high-quality solves with stable CEP behavior
  - refinement improving a good seed without rescuing a bad cluster
  - Implemented the first two directly in `backend/tests/test_forward_model.py`
  - The third remains a follow-up verification item because refinement is intentionally still manual-only
  - Implemented in `backend/radar/forward_model.py`:
    - `solve_by_intersection()` now returns solve-quality metadata alongside the position estimate
    - `run_full_pipeline()` applies explicit quality gates before storage and returns `stage="quality_gates"` diagnostics for rejected solves
    - stored CEP now reflects weighted dominant-cluster RMS scatter
    - returned result metadata now makes the current refinement decision explicit via `refinement_enabled=False`
  - Verified with:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
    - Result: `52 passed in 0.42s`

Stage 6: Verification and operator review
- [x] Run the full backend radar/forward-model test subset after all changes
- [x] Add a synthetic evaluation harness or fixture set with known true radar positions and controlled noise cases:
  - clean geometry
  - weak-angle geometry
  - stale/projected positions
  - repeated-aircraft dominance
  - bimodal/interference candidate clouds
- [x] Compare pre-change vs post-change error distance and CEP on the synthetic set
- [x] Review live diagnostics/logging to ensure the solver now reports why constraints were down-weighted or rejected
- [x] Record the post-change review summary here with:
  - accuracy improvement evidence
  - remaining failure modes
  - recommendation on whether optimization refinement should remain optional or become automatic

### Stage 6 Review

- Added a focused synthetic integration harness in `backend/tests/test_forward_model_stage6.py` covering:
  - clean multi-frame geometry
  - weak-angle geometry
  - projected/interpolated-position overload
  - repeated-aircraft dominance
  - bimodal mixed-radar interference
- Verification command:
  - `uv run --directory backend pytest tests/test_forward_model_stage6.py tests/test_radar_sweep.py tests/test_forward_model.py tests/test_forward_model_integration.py`
  - Result: `57 passed in 0.39s`
- Synthetic outcome summary:
  - clean geometry: stored, distance error bounded under `20 km`, weighted-cluster CEP bounded under `32 km`
  - weak-angle geometry: not published (`intersection` / `quality_gates` rejection)
  - projected/interpolated overload: not published (`quality_gates` rejection)
  - repeated-aircraft dominance: still localises and remains bounded under `30 km`
  - bimodal interference: not published after adding a minimum dominant-cluster support gate
- Follow-up correction discovered during Stage 6:
  - the earlier quality gates still allowed a sparse bimodal cloud to publish if one cluster only narrowly dominated
  - `backend/radar/forward_model.py` now also requires a minimum dominant-cluster member count before storage
- Diagnostics review:
  - `run_full_pipeline()` now returns structured rejection context including:
    - `stage`
    - `intersection_rms_km`
    - `n_pairs`
    - `n_selected_observations`
    - `interpolated_fraction`
    - `azimuth_spread_deg`
    - `high_quality_frames`
    - `cluster_member_count`
    - `dominance_ratio`
  - this is sufficient for the manual `fm-run` path to explain why a solve was rejected without adding more logging in this pass
- Remaining failure modes:
  - clean synthetic solves can still carry multi-kilometre error and large CEP even when accepted; the current live path is materially more selective, but not yet precision-grade
  - acceptance still depends heavily on the selected frame geometry and dominant-cluster structure; sparse good frames can still fail early at the intersection stage
  - automatic optimisation refinement is still unproven on top of the new gated live path
- Refinement recommendation:
  - keep optimisation refinement manual-only for now
  - revisit automatic refinement only after collecting evidence that it consistently improves accepted live seeds across the new Stage 6 synthetic scenarios and representative live IIDs

## 2026-04-08 Live FM Validation Workflow

- [x] Add a repeatable live-validation helper for selected IIDs using the running backend's FM endpoints
- [x] Capture acceptance/rejection plus Stage 5 quality-gate metrics in one operator-facing summary
- [x] Define the recommended live-validation checklist for representative single-radar IIDs

### Workflow Review

- Added `backend/tools/fm_live_validate.py` to drive:
  - `GET /api/radar/iids/{iid}/fm-status`
  - `GET /api/radar/iids/{iid}/fm-diagnostics`
  - `GET /api/radar/iids/{iid}/pipeline-health`
  - `POST /api/radar/iids/{iid}/fm-run`
- The helper reports per-run:
  - success/failure
  - stage/reason
  - stored flag
  - `cep_m`
  - `intersection_rms_km`
  - `n_pairs`
  - `n_frames` / `n_good_frames`

## 2026-04-09 Radar Audit Improvement Plan

Execution rule:
- Complete items strictly in order.
- After each item, run the focused verification for that item and record the result here before moving to the next item.
- Do not begin the next item until the current one is checked.

1. Live burst expiry and frame admission correctness
- [x] Finalize pending bursts when they age out, even if no later reply arrives from the same ICAO
- [x] Add a regression proving a non-reference aircraft burst is admitted when a different ICAO arrives after the gap
- [x] Run focused sweep tests and record the result

2. Per-aircraft burst-time geometry in calibration pairs
- [x] Resolve calibration-pair aircraft positions at each aircraft's own `beam_center_us`
- [x] Add a regression covering differing sweep-centroid vs beam-centre timestamps
- [x] Run focused sweep/localiser tests and record the result

3. Pipeline-health frame accounting
- [x] Make pipeline-health frame counts use the live frame store rather than the unused model field
- [x] Add a regression proving pipeline health reports existing live frames
- [x] Run focused API/sweep tests and record the result

4. Live frame builder hot-path set membership
- [x] Replace linear duplicate-observation scans with per-frame ICAO membership tracking
- [x] Keep behavior unchanged apart from the data structure
- [x] Run focused sweep tests and record the result

5. Calibration-pair position lookup cost
- [x] Batch/cache historical position lookups for `update_calibration_pairs()`
- [x] Avoid repeated sort/fetch work per aircraft per sweep
- [x] Run focused sweep/localiser tests and record the result

6. `/api/radar/iids/{iid}/sweeps` request-path recomputation
- [x] Stop recomputing burst positions synchronously for every aircraft on every request
- [x] Reuse precomputed sweep-time position/azimuth/range data where available
- [x] Run focused radar API tests and record the result

7. Mixed-IID sweep contamination guards
- [x] Add an initial source-consistency gate so live frame admission rejects bursts inconsistent with the active frame timing family
- [x] Add an initial sweep-history/pair-generation gate to prevent obviously mixed-source pair formation
- [x] Add focused regressions and record the result

### Review

- Item 1 completed:
  - Implemented pending-burst expiry flushing in `backend/radar/sweep.py` so a burst can fire when any later DF11 proves the original burst aged out, not only when the same ICAO replies again.
  - Added regression `test_live_frame_builder_finalizes_stale_non_reference_burst_on_other_icao_activity` in `backend/tests/test_radar_sweep.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py`
    - Result: `23 passed in 0.07s`
- Item 2 completed:
  - Updated `update_calibration_pairs()` to resolve aircraft geometry at each aircraft's own `beam_center_us` instead of the sweep centroid.
  - Added regression `test_update_calibration_pairs_looks_up_position_at_each_aircraft_beam_center` in `backend/tests/test_radar_sweep.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_localiser.py`
    - Result: `38 passed in 0.41s`
- Item 3 completed:
  - Updated `get_pipeline_health()` in `backend/radar/sweep.py` to derive frame/scoring state from the live frame store via `get_sweep_frames(iid)`.
  - Added regression `test_get_iid_pipeline_health_reports_live_sweep_frames` in `backend/tests/test_radar_api.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py`
    - Result: `36 passed in 0.25s`
- Item 4 completed:
  - Added runtime-only `seen_icaos` membership tracking to `LiveFrameState` in `backend/radar/models.py`.
  - Replaced the live frame builder's linear duplicate-observation scan with constant-time set membership in `backend/radar/sweep.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py`
    - Result: `24 passed in 0.05s`
- Item 5 completed:
  - Added per-pass `(icao, beam_center_us)` caching in `update_calibration_pairs()` so identical geometry lookups are resolved once per run.
  - Added regression `test_update_calibration_pairs_caches_position_lookup_per_aircraft_and_beam_center` in `backend/tests/test_radar_sweep.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py`
    - Result: `25 passed in 0.07s`
- Item 6 completed:
  - Updated `/api/radar/iids/{iid}/sweeps` in `backend/radar/api.py` to reuse precomputed sweep-time `lat`/`lon` and derive range/azimuth directly when available, avoiding request-path burst position relookup.
  - Added regression `test_get_iid_sweeps_uses_precomputed_sweep_positions_without_relookup` in `backend/tests/test_radar_api.py`.
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py`
    - Result: `38 passed in 0.24s`
- Item 7 completed:
  - Added a bounded live frame source-consistency gate in `backend/radar/sweep.py` so once an aircraft has enough history, a new burst must stay close to that aircraft's historical reference-relative phase family to be admitted into the open frame.
  - Added a bounded pair-generation family gate in `backend/radar/sweep.py` so calibration-pair generation skips aircraft explicitly marked outside the dominant family by the current rotation model.
  - Added regressions:
    - `test_live_frame_builder_rejects_burst_inconsistent_with_historical_phase_family`
    - `test_update_calibration_pairs_skips_residual_aircraft_outside_dominant_family`
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_localiser.py`
    - Result: `41 passed in 0.42s`
- Next item:
  - No further items are currently listed in this improvement plan.

## 2026-04-09 Sweep Diagram Timing/Geometry Remediation

Execution rule:
- Trace the display from backend timing/state to frontend animation before changing behavior.
- Fix timing consistency first, then geometric/range plotting.
- Verify after each step before proceeding.

1. Backend/API timing and geometry handoff
- [x] Confirm the sweep-frame and flash endpoints expose the exact timing quantities needed by the page
- [x] Add any missing timing fields needed to anchor the browser animation to Beast time
- [x] Add/update focused API regressions and record the result

2. Frontend sweep timebase alignment
- [x] Replace the browser-local sweep animation clock with a Beast-time-derived clock anchored from backend frame/flash data
- [x] Make flash timing derive from DF11 `ts_us` on the same estimated Beast clock, not receipt wall time
- [x] Verify the Radar page still builds successfully and record the result

3. Frontend range/bearing representation
- [x] Replace live `/api/stats` aircraft geometry with burst-time positions from the latest sweep frame
- [x] Plot dots at scaled true range from the solved radar location instead of a fixed-radius arc
- [x] Verify the Radar page still builds successfully and record the result

### Review

- Root cause confirmed:
  - The sweep diagram was not using the solver's timing or geometry.
  - Beam motion used browser-local animation time, not Beast-relative burst timing.
  - Dot flashes used poll receipt `Date.now()`, not DF11 `ts_us`.
  - Aircraft bearings came from live `/api/stats` positions, not the burst-time positions stored in the sweep frames.
  - Dot radius was hard-coded to a constant arc, so resolved radar range was not represented at all.
- Backend/API change:
  - Updated `GET /api/radar/iids/{iid}/sweep-frames` in [backend/radar/api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) to return each frame's `period_s` and the payload-level `latest_arrival_us` anchor.
  - Added regression `test_get_iid_sweep_frames_includes_period_and_latest_arrival_anchor` in [backend/tests/test_radar_api.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_api.py).
  - Verification:
    - `uv run --directory backend pytest tests/test_radar_api.py`
    - Result: `14 passed in 0.23s`
- Frontend timing/geometry change:
  - Updated [frontend/src/pages/RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) so the sweep diagram now:
    - estimates current Beast time from `latest_arrival_us` and incoming DF11 flash `ts_us`
    - computes beam angle from `(estimated_beast_us - latestFrame.ref_arrival_us) / period_us`
    - uses DF11 `ts_us` on the same Beast clock for flash timing instead of receipt wall time
    - uses burst-time lat/lon from the latest sweep frame, not live `/api/stats` positions
    - scales dot radius by true range from the solved radar location, not a fixed-radius arc
  - Verification:
    - `npm run build`
    - Result: Vite production build succeeded on 2026-04-09
  - `n_selected_observations`
  - `azimuth_spread_deg`
  - `interpolated_fraction`
  - `high_quality_frames`

## 2026-04-08 Residual Replay Diagnostics

- [x] Add per-run residual replay diagnostics for the chosen forward-model seed/final location
- [x] Add a compact example-frame assessment showing observed phases, predicted phases, residuals, selection weights, and replay inputs for one frame
- [x] Surface the new diagnostics through `fm-run`/`fm_live_validate.py` in an operator-readable form
- [x] Add focused backend tests for the new diagnostic payloads
- [x] Run focused verification and record the review here

### Review

- Implemented in `backend/radar/forward_model.py`:
  - `_build_residual_replay_diagnostics()` now evaluates the chosen seed/final location against the exact preprocessed scoring frames used by the live solver
  - `run_full_pipeline()` now returns:
    - `seed_replay_summary`
    - `seed_example_frame`
    - `final_replay_summary`
    - `final_example_frame`
- The frame assessment includes:
  - reference aircraft and frame index
  - candidate-to-reference bearing
  - per-observation selected/not-selected flag
  - observed phase, predicted phase, residual phase
  - observed / predicted / residual timing in milliseconds
  - quality weight, baseline, phase strength, interpolation flag
- `backend/tools/fm_live_validate.py` now surfaces the new residual sigma and replay summary fields in both JSON output and the terminal summary.
- Focused verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - `python3 -m py_compile backend/tools/fm_live_validate.py backend/radar/forward_model.py`
  - Result: `37 passed in 0.08s`

## 2026-04-08 Beast Timing Resolution Preservation

- [x] Inspect where Beast ticks are converted to radar arrival timestamps
- [x] Preserve sub-microsecond Beast timing through the radar frame-building path
- [x] Remove avoidable integer truncation in burst-centroid and beam-centre calculations
- [x] Add focused regressions for fractional timestamp preservation
- [x] Run focused verification and record the review here

### Review

- Implemented in `backend/radar/sweep.py` and `backend/radar/models.py`.
- The radar path now keeps Beast-derived arrival timestamps as fractional microseconds instead of truncating with integer division.
- Specific changes:
  - DF11 ingest now uses `ticks / BEAST_TICKS_PER_US` instead of `ticks // BEAST_TICKS_PER_US`
  - live burst centroids keep fractional microseconds
  - `detect_bursts_with_signals()` and `refine_burst_center()` no longer force timestamps back to integers
  - SweepFrame timing fields now accept fractional microseconds end to end
- This change is limited to the radar/sweep path; unrelated recent-message timing buffers were left untouched in this pass.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py`
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/models.py`
  - Result: `57 passed in 0.07s`

## 2026-04-08 Fixed-Window SweepFrame Simplification

- [x] Inspect the current live frame builder against the intended reference-anchored window model
- [x] Simplify the live frame builder so an open frame freezes its reference and closes on one radar period
- [x] Replace strongest-signal duplicate selection as the main burst chooser with period-family admission plus first-in-window retention
- [x] Add a minimal duplicate-frame safeguard for near-duplicate frame starts
- [x] Add focused tests and run verification

### Review

- Implemented in `backend/radar/sweep.py`.
- The live frame builder now follows the intended simpler model more closely:
  - once a frame is open, its `ref_icao` is frozen
  - the reference burst defines `t0`
  - the frame window is fixed to `[t0, t0 + period)`
  - any burst after `t0 + period` finalizes the open frame before further processing
- Non-reference bursts are now admitted only if they match the dominant period family:
  - accept immediately if the current rotation model classifies the ICAO in `folded`
  - reject if the current rotation model classifies it in `residual` or `secondary_folded`
  - otherwise fall back to live burst-history period consistency
- Duplicate ICAOs within the same open frame are no longer resolved by strongest signal. The first admitted in-window burst is kept; later duplicates are ignored.
- Added a minimal near-duplicate frame-start suppression based on IID period fraction to guard against obvious sidelobe/reflection duplicates.
- Focused verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/models.py backend/tests/test_radar_sweep.py`
  - Result: `22 passed in 0.06s` and `37 passed in 0.04s`

## 2026-04-08 Alternative Reference Diagnostics

- [x] Inspect current replay diagnostics and identify the minimal extension for reference-comparison scoring
- [x] Add per-frame alternative-reference diagnostics to the forward-model replay payload
- [x] Add focused tests for the new payload
- [x] Run focused verification and record the review here

### Review

- Implemented in `backend/radar/forward_model.py`.
- The replay diagnostics for the worst/example frame now include `alternative_references`, which scores a small set of plausible anchor aircraft for that same frame at the same candidate radar location.
- The diagnostics let us distinguish:
  - the chosen reference aircraft is a bad anchor for an otherwise usable frame
  - the whole frame remains inconsistent regardless of which selected aircraft is treated as the reference
- Focused verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - `python3 -m py_compile backend/radar/forward_model.py backend/tools/fm_live_validate.py`
  - Result: `37 passed in 0.08s`

## 2026-04-08 Frame Reference Repair And Rejection

- [x] Inspect the forward-model frame preprocessing path and identify where to evaluate or rewrite per-frame references
- [x] Implement frame-level best-reference evaluation before intersection solving
- [x] Re-anchor frames that materially improve under an alternative reference
- [x] Drop frames that remain incoherent under every candidate reference
- [x] Add focused tests and run verification

### Review

- Implemented in `backend/radar/forward_model.py`.
- `run_full_pipeline()` now prepares frames before intersection solving using a provisional geographic anchor:
  - current stored FM location if available
  - otherwise receiver location
- Each valid frame is scored under:
  - its current reference aircraft
  - a small set of candidate alternative references drawn from the frame observations
- Frames are:
  - re-anchored when an alternative reference materially improves residual sigma
  - dropped when no candidate reference yields an acceptable frame residual sigma
- Returned result payloads now include `frame_preparation` diagnostics so live runs can show how many frames were re-anchored or dropped before solving.
- Focused verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py`
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/models.py backend/radar/forward_model.py`
  - Result: `39 passed in 0.08s` and `61 passed in 0.06s`
  - `cluster_member_count`
  - `dominance_ratio`
  - distance to expected known site when `--expected IID=LAT,LON` is provided
- Recommended live-validation checklist:
  - choose 3-5 representative single-radar IIDs with established periods
  - include at least:
    - one strong busy IID
    - one moderate-traffic IID
    - one IID known to have weaker geometry or more projected positions
  - run repeated manual checks across multiple traffic periods, for example:
    - `uv run --directory backend python tools/fm_live_validate.py --iid 7 --iid 21 --repeat 5 --delay 30 --expected 7=LAT,LON --expected 21=LAT,LON`
  - record for each IID:
    - acceptance rate
    - mean CEP
    - mean distance to expected site
    - dominant rejection stage (`intersection` vs `quality_gates`)
    - typical `azimuth_spread_deg`, `interpolated_fraction`, `cluster_member_count`, and `dominance_ratio`
  - treat a high rejection rate as useful signal if it coincides with poor geometry or projected-position overload; the key question is whether bad periods are rejected and good periods stay stable

## 2026-04-08 Live FM Follow-Up

- [x] Add deeper intersection-stage diagnostics so live `intersection` failures report where observations/circles were lost
- [x] Add a geographic plausibility gate so obviously wrong branch solutions are rejected before they appear as acceptable unstored solves
- [x] Extend the live-validation helper to surface the new diagnostics and solved-point distance from the receiver
- [x] Run targeted backend verification and record the follow-up live-data review

### Follow-Up Review

- Implemented detailed intersection-attempt diagnostics in `backend/radar/forward_model.py`.
  The live forward-model path now preserves pruning counts even when the solver fails before producing a location. `run_full_pipeline()` surfaces:
  - `total_scored_observations`
  - `eligible_observations`
  - `selected_observations`
  - `dropped_low_quality`
  - `dropped_weak_angle`
  - `dropped_same_azimuth_bin`
  - `dropped_per_icao_cap`
  - `dropped_duplicate_icao_in_frame`
  - `dropped_per_frame_cap`
  - `n_pairs`
  - `raw_candidate_count`
  - `plausible_candidate_count`
  - `degenerate_baseline_pairs`
- Added an explicit geographic plausibility gate to the live publication path.
  The current policy rejects accepted intersection clusters whose solved point is more than `200 NM` from the receiver, reported as `intersection quality gate failed: receiver_distance`.
- Extended `backend/tools/fm_live_validate.py` so live runs now print:
  - solved-point distance from the receiver
  - selection/pruning counts
  - raw vs plausible candidate counts
  This should make the `IID 7` starvation path directly attributable to a specific pruning stage on the next live pass.
- Added focused regressions in `backend/tests/test_forward_model.py` for:
  - pruning diagnostics from intersection selection
  - receiver-distance gate rejection
  - propagation of intersection-stage pruning detail through `run_full_pipeline()`
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_forward_model_stage6.py`
  - `python3 -m py_compile backend/tools/fm_live_validate.py`
  - `python3 backend/tools/fm_live_validate.py --help`
  - Result: `36 passed in 0.09s`, helper compiles, helper CLI loads
- Remaining operator step:
  - rerun the live validator on the same representative IIDs and compare:
    - whether `IID 7` is dominated by `drop_weak_angle`, `drop_per_frame_cap`, or low raw/plausible candidate counts
    - whether `IID 21` now fails cleanly at `quality_gates` with `receiver_distance_m` instead of appearing as a superficially healthy unstored solve

## 2026-04-08 Direction And Plausibility Tuning

- [x] Tighten the forward-model geographic plausibility gate based on the refreshed live-distance evidence
- [x] Add a clockwise preference so counterclockwise solutions only publish when their evidence is materially stronger
- [x] Strengthen ambiguity rejection policy and reflect the new direction/quality rules in focused tests
- [x] Run targeted forward-model verification and record the tuning review here

### Tuning Review

- Refined `backend/radar/forward_model.py` using the refreshed live-validation output:
  - tightened the general receiver-distance gate from `200 NM` to `100 NM`
  - added a stricter `60 NM` receiver-distance gate specifically for counterclockwise (`CCW`) solutions
  - retained clockwise/counterclockwise solving for compatibility, but changed direction selection so when both directions succeed the pipeline prefers clockwise (`CW`) by default
- Important calibration choice:
  - I initially tried a stronger general ambiguity/dominance threshold and an additional CCW dominance gate, but that wrongly rejected clean synthetic CCW cases
  - the final policy is narrower and better supported by evidence:
    - no harsher global dominance threshold
    - no blanket CCW rejection
    - CW wins ties when both directions are viable
    - standalone CCW solutions are still allowed, but only if they remain geographically tight to the receiver
- This directly targets the observed live failure mode:
  - the incorrect `IID 21` CCW branch at roughly `137.6 km` from the receiver should now fail `quality_gates` via the CCW-specific distance rule
  - `IID 7` remains an ambiguity problem, but it is no longer conflated with the wrong-branch CCW publication issue
- Added focused regressions in `backend/tests/test_forward_model.py` for:
  - rejecting implausible standalone CCW solutions by distance
  - preferring CW when both directions succeed
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_forward_model_stage6.py`
  - `python3 -m py_compile backend/tools/fm_live_validate.py`
  - Result: `38 passed in 0.07s`
- Next live-validation expectation:
  - `IID 21` should no longer store the `CCW` branch near `(51.056, 2.027)`
  - if `IID 7` still fails, the remaining work is in multi-cluster ambiguity resolution rather than direction plausibility

## 2026-04-08 Clockwise Ambiguity Resolution

- [x] Add a stronger clockwise cluster tie-breaker that uses forward-model fit quality, not only raw intersection-cluster weight
- [x] Preserve the current CW preference and distance safeguards while reducing false ambiguity in dense CW clouds
- [x] Add focused tests for cluster-fit tie-breaking and rerun the forward-model subsets
- [x] Record the tuning review here against the latest live-validation results

### Ambiguity Review

- Updated `backend/radar/forward_model.py` so dense intersection clouds are no longer resolved purely by cluster weight.
  The solver now:
  - builds distinct intersection neighborhoods
  - evaluates the top candidate neighborhoods using the full forward-model residual score
  - uses that residual fit as the tie-breaker for the final clockwise solution choice
- Kept the earlier direction/plausibility safeguards intact:
  - CW remains preferred when both directions succeed
  - general receiver-distance gate stays at `100 NM`
  - standalone CCW solutions still have the tighter `60 NM` receiver-distance limit
- Added one more sparse-solve guard discovered during verification:
  - minimum qualifying pair count is now `12`
  - this specifically rejects the synthetic bimodal false positive that still slipped through with only `11` pairs and low total support
- Verification impact:
  - clean synthetic CW geometry still localises successfully
  - repeated-aircraft dominance still localises successfully
  - weak-angle / projected overload / bimodal interference cases remain rejected
- Added a focused regression in `backend/tests/test_forward_model.py` for rejecting otherwise-strong but too-sparse intersection solves (`pair_count`)
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_forward_model_stage6.py`
  - Result: `39 passed in 0.10s`
- Expected live effect:
  - the solver should be less likely to report `candidate cloud was ambiguous` for dense CW cases where one cluster fits the full phase model materially better
  - sparse, deceptively sharp candidate clouds should now fail fast at `quality_gates` instead of being published

## 2026-04-08 Distance Default Revision

- [x] Raise the default receiver-distance thresholds to better match realistic receiver/radar geometry
- [x] Adjust the focused receiver-distance regressions for the new defaults
- [x] Re-run the forward-model verification subset

### Distance Review

- Revised the default plausibility bounds in `backend/radar/forward_model.py`:
  - general receiver-distance gate increased from `100 NM` to `300 NM`
  - CCW-specific receiver-distance gate increased from `60 NM` to `120 NM`
- Rationale:
  - the earlier defaults were intentionally conservative safeguards for a bad live branch, but they were too tight as general physical assumptions
  - a well-sited receiver can legitimately observe aircraft much farther out, so the default radar plausibility envelope needed to be widened
- The direction-specific safety policy is still preserved:
  - general solves now have a more realistic default envelope
  - standalone CCW solutions remain subject to a tighter bound than CW, but no longer an unrealistically small one
- Updated the focused distance regressions in `backend/tests/test_forward_model.py` to remain beyond the new limits
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_forward_model_stage6.py`
  - Result: `39 passed in 0.10s`

## 2026-04-08 Residual Refinement Integration

- [x] Add residual-based validation for the chosen intersection seed before publication
- [x] Add a bounded local refinement pass from the chosen intersection seed
- [x] Gate publication on final timing-consistency quality rather than cluster structure alone
- [x] Add focused regression coverage for refinement improving good seeds without rescuing weak ones
- [x] Re-run the forward-model verification subset and record the review here

### Refinement Review

- Updated `backend/radar/forward_model.py` so the live path is now:
  - geometric intersection seed
  - structural quality gates
  - residual-fit evaluation against the full forward-model phase/timing score
  - bounded local refinement from that seed
  - final publication gate on residual consistency
- Added new residual-fit plumbing:
  - `_summarize_residual_fit()` computes full-model score, mean residual, and residual sigma for an assumed radar location
  - `_refine_intersection_seed()` runs a bounded local refinement around the chosen seed using the same residual model
- Publication behavior now changes in two important ways:
  - a seed can be refined and stored as `source="intersection_refined"` when the residual fit materially improves
  - a geometrically plausible seed is still rejected if the final residual sigma is too poor (`stage="refinement_gates"`)
- Current refinement policy:
  - refinement is bounded to a local move around the seed
  - refinement is only accepted when it materially improves the seed fit
  - publication is rejected if the final residual sigma exceeds the configured threshold
- Added focused regressions in `backend/tests/test_forward_model.py` for:
  - applying refinement when it materially improves the seed
  - rejecting a seed when the final residual sigma remains too high
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py tests/test_forward_model_stage6.py`
  - Result: `41 passed in 0.44s`
- Expected live effect:
  - weak or wrong branches that previously survived on cluster structure alone should now also need to match the full sweep timing model
  - the next live run should show whether the bad `CW` publications are reduced by the residual-based final gate before storage

## 2026-04-08 SQLite Lock Mitigation

- [x] Inspect the failing background write paths and current SQLite connection behavior
- [x] Add targeted mitigation for transient `database is locked` failures in visit and radar-IID writers
- [x] Add focused DB regressions for lock retry behavior
- [x] Run the DB verification subset and record the review here

### DB Lock Review

- The live errors were coming from normal concurrent background writers:
  - `write_visits()` from the housekeeping loop
  - `upsert_radar_iid()` from the radar persistence loop
- Mitigation added in `backend/db.py`:
  - connection setup now explicitly applies `PRAGMA busy_timeout=10000`
  - `write_visits()` now uses one `executemany()` transaction instead of per-row inserts, reducing lock hold time
  - both `write_visits()` and `upsert_radar_iid()` now retry transient `sqlite3.OperationalError: database is locked` failures with short exponential backoff
- The retry is intentionally narrow:
  - only `database is locked` is retried
  - other `OperationalError` cases still raise immediately
- Added focused regression coverage in `backend/tests/test_db.py` for:
  - `write_visits()` succeeding after a transient lock on the first attempt
  - `upsert_radar_iid()` succeeding after a transient lock on the first attempt
- Verification:
  - `uv run --directory backend pytest tests/test_db.py`
  - `python3 -m py_compile backend/db.py backend/main.py`
  - Result: `22 passed in 0.48s`

## Passive Radar Remediation Planning

Current passive-radar state:
- Stage 1 is usable for operator review but still not formally signed off; current live traffic can still produce occasional detector regressions that need continued watching.
- Stage 2A backend semantics exist, but the operator-facing sweep/dwell plots were deliberately removed because they were not yet useful proof surfaces.
- Stage 2B/2C have been reworked substantially:
  - ADS-B-timestamped geometry lookup
  - deduped calibration persistence
  - corrected solver equation
  - repeated-consistency gating
  - coincidence-subset preference
  - dwell-based burst-centre refinement
  - direct per-sweep multi-aircraft solving with weighted accumulation
- Current blocker: representative sweeps still yield `0` accepted sweep solves on the latest light-traffic snapshot, mostly because they contain too few aircraft; the secondary issue is insufficient connected sweep graph coverage after pair filtering.
- Next decision point: rerun the same sweep-diagnostic review during a denser traffic period. If good sweep solves appear, evaluate weighted accumulation; if not, revisit same-sweep grouping/timing semantics rather than solver structure.

- [ ] Review the completed `add-passive-radar-positioning` change against its implementation and identify spec/implementation drift
- [ ] Create a follow-up OpenSpec remediation change with staged gates for:
  - Stage 1 rotation/activity
  - Stage 2A sweep and dwell visualisation
  - Stage 2B calibration pair generation and persistence
  - Stage 2C localisation solver
  - Stage 2D derived overlays and map presentation
- [ ] Ensure each stage defines:
  - backend correctness criteria
  - API verification criteria
  - frontend display verification criteria
  - explicit stop conditions before the next stage can begin
- [ ] Record a review summary explaining why the remediation is split as a new change instead of mutating the historical completed change
- [x] Implement the Stage 1 correction for `/api/radar/iids` so `last_seen` reflects each IID's actual latest event time
- [x] Add a focused backend verification for the corrected Stage 1 `last_seen` behaviour
- [x] Re-run Stage 1 live verification and update the remediation review note with the post-fix result
- [x] Rework the Radar page Stage 1 area into a full-width IID/ICAO timing-alignment view instead of a summary-only table
- [x] Refine Stage 1 detector behavior so dominant-family selection is robust to half-period clutter
- [x] Add secondary-family promotion and primary/secondary/residual classification to the Stage 1 detector and visual
- [ ] Live-verify representative IIDs against the refined Stage 1 primary/residual classification before clearing Stage 1
- [ ] Run a Stage 1 acceptance pass on representative live IIDs covering:
  - one clean single-radar IID
  - one noisy but still obviously single-radar IID
  - one ambiguous/shared IID
- [ ] For each representative IID, record whether:
  - the reported primary period matches the visually dominant cadence
  - confidence/support grows in a way that matches the amount of corroborating traffic
  - obviously primary-aligned ICAO rows are not left mostly residual
  - the live alignment view remains visually stable while bursts arrive
  - reset/relearn behavior works as intended when used
- [ ] Update the Stage 1 remediation review note with explicit GO / NO-GO criteria and the result of the representative-IID acceptance pass
- [ ] Redesign Stage 1 primary detection around mutually agreeing repeated sweep series instead of per-ICAO median periods
- [ ] Require repeated corroborating intervals before an ICAO can materially reinforce the learned primary characteristic
- [ ] Allow a bad learned primary estimate to be replaced when sustained contradictory cohort evidence becomes materially stronger
- [ ] Add focused backend regressions for:
  - bad initial estimate corrected by later stronger evidence
  - weak single-pair ICAOs not materially increasing support
  - obvious primary-aligned rows not remaining residual under mixed clutter
- [ ] Audit Stage 2A sweep semantics and decide whether to implement true beam/azimuth semantics or narrow the API/UI to occupancy-only sweep grouping
- [ ] Audit Stage 2A dwell semantics and decide whether to implement genuine per-reply RSSI or narrow the API/UI to burst-timing-only display
- [ ] Record an explicit Stage 2A GO / NO-GO decision before touching Stage 2B
- [ ] Implement Stage 2A full sweep semantics:
  - preserve per-reply signal within sweep history
  - compute per-burst aircraft geometry for `range_nm`
  - compute `azimuth_deg` from solved radar position when localisation exists
  - expose interpolation status where aircraft position was extrapolated
- [ ] Implement Stage 2A full dwell semantics:
  - return genuine per-reply `signal_dbfs` for the selected burst
  - stop reusing one sweep-level average signal for every reply
- [ ] Update the Radar page sweep and dwell panels so they present azimuth/range and real dwell amplitude semantics clearly
- [ ] Add focused backend tests for sweep geometry and dwell reply-signal fidelity
- [ ] Run a Stage 2A acceptance pass on representative live IIDs covering:
  - one unsolved IID, reviewed as sweep/dwell diagnostics only
  - one solved IID, reviewed for sweep/dwell diagnostics plus downstream azimuth sanity
- [ ] For each representative Stage 2A IID, record whether:
  - sweep grouping matches the trusted Stage 1 cadence
  - the dwell profile shows real within-burst amplitude structure rather than a flat or synthetic trace
  - burst-centre timing appears good enough to refine co-sweep timing inputs
  - unsolved IIDs are not being over-presented as beam-azimuth products
  - solved-IID azimuths look physically plausible rather than erratic
- [x] Fix Stage 2B/2C calibration semantics so co-sweep pairs use aircraft position at sweep time, not current live position
- [x] Make calibration-pair generation idempotent across background-loop passes and normalise pair ordering/sign
- [x] Harden the solver so poor or absurd solutions are rejected instead of being published as radar locations
- [x] Add focused backend regressions for:
  - repeated calibration-pair passes not duplicating the same sweep observation
  - historical aircraft track lookup for sweep-time calibration positions
  - localisation rejecting unbounded/implausible fits
- [x] Replace coarse 5-second track-store radar geometry lookup with timestamped ADS-B position history sampled at position-update cadence
- [x] Interpolate sweep-time aircraft position from adjacent ADS-B position reports, with only short motion-vector extrapolation when needed
- [x] Add focused backend regressions for:
  - sweep-time lookup using adjacent ADS-B position samples rather than coarse track snapshots
  - short extrapolation using sampled motion vectors when the sweep falls just outside the nearest ADS-B report
- [x] Correct the Stage 2C solver equation so each calibration pair includes the known receiver-distance correction term from the proposal
- [ ] Re-run localisation on representative active IIDs after the corrected equation and confirm solved positions remain physically plausible with bounded CEP
- [ ] Audit Stage 2B pair-quality semantics further:
  - verify whether current co-sweep grouping window is too loose for localisation-grade observations
  - verify whether burst-centre timing is too noisy without dwell refinement
  - verify whether pair selection needs stronger geometry/quality gating before solving
- [x] Refine burst-centre timing from per-reply dwell signal in the backend and use the refined timestamp for calibration-pair generation with safe centroid fallback
- [ ] Evaluate near-coincident co-sweep aircraft pairs as a higher-grade localisation subset and decide whether they should be used for bootstrap, preferred admission, or diagnostics only
- [x] Add repeated-consistency gating so only locally stable repeated aircraft-pair observations are admitted to Stage 2C solving
- [x] Add a global operator control to purge all learned radar state and persisted calibration history so passive-radar localisation can relearn from a clean baseline

## AGENTS.md Contributor Guide Task

- [x] Inspect repository structure, manifests, and recent git history for contributor-facing conventions
- [x] Draft root `AGENTS.md` titled `Repository Guidelines` with concise, repo-specific instructions
- [x] Verify the guide against current commands/files and record the review result

### Review

- Verified repository structure against `README.md`, `frontend/package.json`, `backend/pyproject.toml`, `frontend/src/`, and `backend/tests/`.
- Verified commit guidance against recent `git log` subjects.
- Confirmed `AGENTS.md` is within the requested size target at `373` words and uses Markdown headings throughout.

## Current Assessment

- Items `A` through `H` were already remediated in code.
- The original remediation items are complete, but a follow-on high-frequency observability gap remains.
- The current timing display is streamed, but it still diverges materially from the proposal:
  - it uses wall-clock arrival time instead of Beast 12 MHz timestamps
  - it renders fixed-width ticks rather than message-length-scaled bars
  - it lives on the Receiver page instead of a dedicated high-frequency page
  - it does not yet handle overlap/stacking in a way that reflects true arrival density
- `Item J` is mislabeled in the execution table inside `.docs/development/deficiency-remediation.md`; the heading defines `J` as the shared loading/error-state rollout, but the status table currently labels `J` as the SkyView history heatmap issue. This must be corrected as part of the documentation pass.
- User clarification on 2026-04-04: `Item J` was previously reviewed and intentionally left as-is because the existing fetches are fast enough that added loading UI is not worth the churn. Treat it as a documented non-remediation, not active work.

## Execution Rules

- Rectify one deficiency at a time.
- Do not start the next item until the current item is verified.
- For each item, capture the verification result in the review section at the end of this file.
- Because the repo has no configured lint or test suite, verification will rely on targeted code inspection, focused runtime/API checks where applicable, and frontend build validation when UI code changes.

## Review

- `2026-04-06`: Audited the current Chromecast discovery lifecycle against the repeated `Zeroconf instance loop must be running` log.
- The failure mode is still present in the current code:
  - [backend/cast.py](/home/keith/claude/adsb-dashboard/backend/cast.py) keeps the discovery browser alive after `cc.wait()` so the socket client can reconnect via zeroconf.
  - The same module later calls `pychromecast.discovery.stop_discovery(...)` from cache-reset and rediscovery paths without first explicitly disconnecting the cached Chromecast client or otherwise proving its worker thread is stopped.
  - That means an existing `pychromecast` socket worker can still attempt mDNS reconnection against a stopped zeroconf loop, which matches the reported traceback shape.

- `2026-04-05`: Audited `add-passive-radar-positioning` against the current backend/frontend code.
- The audit found that Stage 1 rotation work is materially present, but Stage 2 was marked complete prematurely:
  - calibration-pair generation is not deduplicated and is weaker than the spec
  - sweep/dwell endpoints are only partial implementations
  - azimuth recomputation and iterative refinement are not implemented
  - the frontend presents Stage 2 structure without proving Stage 2 correctness
- Chosen remediation approach:
  - preserve the historical completed change as a record of what was attempted
  - create a new follow-up OpenSpec change that restates the work as stage-gated remediation with mandatory backend/API/UI verification at each step
- `2026-04-05`: Stage 1 remediation implementation is now substantially in place:
  - `/api/radar/iids` `last_seen` is based on per-IID latest events and covered by a focused backend test
  - the Radar page uses a large aligned IID/ICAO timing view with full-width stacked sections
  - dominant-family selection was corrected to avoid obvious false half-period picks
  - residual ICAOs can now be promoted into a secondary family and are shown distinctly in the Stage 1 visual
  - focused backend tests pass and the frontend build passes
- `2026-04-05`: Stage 1 timeline semantics were refined again after operator review:
  - one ICAO row can now carry separate primary, secondary, and residual point series
  - primary-aligned bursts remain primary even when the same ICAO also contributes a secondary-aligned sequence
  - the frontend aligns each family series independently on the same row so the secondary pattern can be inspected alongside the primary one without expanding the ICAO list
- `2026-04-05`: Stage 1 characteristic retention was tightened after further operator review:
  - the row labels no longer append `primary` / `secondary` text; colour and geometry now carry that information
  - the persisted `radar_iids` record is now treated as the reinforced long-term characteristic, with support counts for primary and secondary families
  - the short-window fit remains available in `rotation_model` as the current observation, but it no longer replaces the long-term top-level period/status on every recompute
  - the Stage 1 timeline uses the reinforced primary and secondary periods for family alignment, so low-traffic windows do not immediately erase known radar characteristics
- `2026-04-05`: Stage 1 now exposes a confidence measure derived from accumulated qualifying support rather than a flat status only:
  - confidence grows from repeated direct-family evidence over time, so busy radars stabilise quickly and low-traffic radars accumulate more slowly
  - single weak observations do not materially establish a family; support only grows from already-qualified repeated-sweep detections
  - the API now exposes primary and secondary confidence/support, and the Radar page shows that confidence in the IID selector and selected-IID summary
- `2026-04-05`: Stage 1 operator controls and labeling were tightened again:
  - the selected IID now has a reset control that clears its in-memory learned state so characteristics can be relearned
  - operator-facing `MULTI_RADAR` is now reserved for fully established dual-family cases; low-secondary-confidence cases remain single-radar labels
  - the cycle-alignment view now fetches a longer timeline and renders at least 60s of span so slower radars show multiple rotations where possible
- `2026-04-06`: Stage 1 has been simplified back to a primary-only contract pending more reliable detector work:
  - secondary-family promotion is disabled again in the detector and timeline response
  - the Stage 1 visual now shows only `primary`, `primary_harmonic`, and `residual`
  - residuals are no longer reinterpreted as a second radar until primary-family identification is trustworthy on live IIDs
- `2026-04-06`: Stage 1 primary assignment was tightened again to reduce false residuals:
  - the detector now includes a centroid-sequence rescue pass for ICAOs whose interval summary is noisy but whose burst centroids still show a repeated primary cadence
  - rows with a clear repeated in-phase burst sequence can now be folded into the primary family even when their median interval was distorted by clutter bursts
- `2026-04-06`: Stage 1 interaction responsiveness was improved on the Radar page:
  - selecting an IID now seeds the alignment panel immediately from the selector-row summary instead of waiting for the next full poll cycle
  - the panel caches per-IID timeline/rotation payloads and refreshes the selected IID on a shorter cadence with independent requests
  - the cycle view should now feel immediate on click rather than batch-updated every 5 seconds
- `2026-04-06`: The next Stage 1 frontend refinement is now formalised as a transport change rather than more polling tweaks:
  - move the selected-IID alignment panel to an incremental live feed
  - keep row anchors stable while bursts stream in
  - only re-align deliberately when the backend model changes enough to justify it
- `2026-04-06`: Stage 1 selected-IID transport is now websocket-backed:
  - backend exposes a selected-IID websocket carrying compact timeline/rotation snapshots on change
  - the Radar page uses that live feed with immediate row-seeded state and HTTP fallback if the socket stalls
  - the cycle-alignment panel should now behave like a live instrument rather than a slow polling chart
- `2026-04-06`: Stage 1 acceptance criteria are now explicit and the current result remains `NO-GO`:
  - the representative live validation set is now defined as one clean single-radar IID, one noisy but still obviously single-radar IID, and one ambiguous/shared IID
  - live summary data confirms usable candidates exist, including IID `9` (`4.0082s`), IID `6` (`1.7385s`, `CHECK_MULTI`), and IID `1` (`4.8231s`)
  - the selected-IID view is now responsive and websocket-backed enough for real operator review
  - however, Stage 1 still fails acceptance because obviously dominant primary-aligned points can still be left mostly `residual`, which makes the learned primary confidence/state untrustworthy for downstream use
  - secondary-family work remains deferred until primary-family assignment is reliable on representative live IIDs
- `2026-04-06`: Stage 1 detector redesign is now in progress:
  - candidate periods are now derived from repeated within-ICAO interval series, not only one median period per ICAO
  - dominant-family selection now chooses the strongest mutually agreeing repeated-sweep cohort across ICAOs
  - reinforcement no longer gains support without direct cohort evidence
  - contradictory stronger cohort evidence can now decay and replace a bad learned primary estimate instead of leaving it stuck indefinitely
- `2026-04-06`: Stage 1 readiness semantics are now explicit in the API and UI:
  - low-support primary characteristics are now exposed as `TENTATIVE` or `PROVISIONAL` readiness even when a period is available
  - only well-supported characteristics present as `ESTABLISHED`
  - the Radar page now shows readiness separately from the detector status so sparse or messy IIDs are not over-claimed as settled
- `2026-04-06`: Stage 2A audit has started and the current implementation is weaker than the page/spec wording implies:
  - `GET /api/radar/iids/<iid>/sweeps` currently returns sweep centroid plus ICAO occupancy only; `azimuth_deg` and `range_nm` are placeholder `null` fields
  - sweep grouping is real, but it is a co-sweep occupancy clustering over burst centroids, not yet a validated beam/azimuth waterfall
  - `GET /api/radar/iids/<iid>/dwell` currently returns per-reply timestamps with one sweep-level average signal repeated for every reply, not genuine per-reply RSSI
  - Stage 2A therefore needs an explicit decision: implement the stronger semantics, or narrow the API/UI/spec to the weaker occupancy/timing semantics that actually exist
- `2026-04-06`: Stage 2A stronger semantics are now implemented locally:
  - sweep history now preserves real per-reply signal samples inside each burst
  - `GET /api/radar/iids/<iid>/sweeps` now returns real `range_nm`, `azimuth_deg` when radar position exists, and an `interpolated` flag when aircraft position extrapolation was needed
  - `GET /api/radar/iids/<iid>/dwell` now returns genuine per-reply `signal_dbfs` samples instead of repeating one sweep-level average
  - the Radar page sweep panel now renders in azimuth space when geometry is available and falls back to ICAO occupancy otherwise
  - focused backend tests pass and the frontend build passes
- Remaining Stage 2A gate:
  - representative live review still needs to confirm that sweep grouping and dwell amplitude traces look plausible on real traffic before Stage 2B can begin
  - Stage 2A should be judged primarily as sweep/dwell diagnostic correctness and dwell-centre refinement readiness, not as a beam-azimuth milestone for unsolved IIDs
- `2026-04-06`: Initial live Stage 2A review exposed and corrected one presentation bug:
  - unsolved IID `62` returned `azimuth_deg` in `/sweeps` because stale persisted `lat/lon` was being treated as sufficient localisation
  - `/api/radar/iids/<iid>/sweeps` now suppresses azimuth unless the IID is actually localisable (`SINGLE_RADAR`/`LIKELY_SINGLE`, not multi-radar)
  - live dwell calls for representative ICAOs now return real per-reply amplitudes, but many current examples are only one reply deep, so Gaussian-style beam-centre refinement still needs careful live judgement on better bursts
- `2026-04-06`: The Stage 2A sweep and dwell plots were removed from the Radar page:
  - the backend sweep grouping and per-reply dwell data are retained for later stages
  - the current sweep waterfall and click-through dwell plot were confusing remnants of the earlier execution and did not provide enough operator value
  - the operator-facing page now stays focused on the proven Stage 1 alignment view and the later-stage map context
- `2026-04-06`: Stage 2B/2C localisation foundations were corrected before further solver work:
  - calibration pairs now prefer historical aircraft track positions at sweep time instead of current live aircraft positions
  - repeated background passes now dedupe and normalise calibration pairs so the same sweep observation is not accumulated indefinitely
  - persisted calibration loads now dedupe historical duplicate rows before solving so old database duplication does not keep contaminating localisation
  - the solver now uses a bounded robust fit and rejects boundary-hitting or high-residual solutions instead of publishing obviously absurd radar positions
  - focused backend regressions cover sweep-time track lookup, pair idempotency, DB dedupe, and solver rejection gates
- `2026-04-06`: Stage 2B geometry lookup now uses timestamped ADS-B position history rather than the coarse 5-second track store:
  - `AircraftState` now retains a compact recent ADS-B position history per aircraft at accepted position-update cadence
  - `RadarState` sweep-time lookup now prefers that ADS-B history and only falls back to the coarse track store when no better history exists
- `2026-04-06`: A global purge control now exists for passive-radar learning:
  - backend exposes `POST /api/radar/reset` to clear in-memory radar models, sweep/event buffers, pending/seen calibration state, and persisted `radar_iids` / `radar_calibration` rows
  - the Radar page now has a confirmed `Purge Radar Learning` button in the IID selector header
  - focused backend tests cover both the API route and the DB deletion path, and the frontend build passes
- `2026-04-06`: Post-purge Stage 2B/2C review still results in `NO-GO`, but with a cleaner baseline:
  - Stage 1 relearned strongly for representative IIDs `1`, `3`, `6`, `20`, and `21`, all reaching `ESTABLISHED` readiness with fresh calibration data after the purge
  - fresh calibration row counts were modest and cleanly bounded (`1`: `82`, `3`: `82`, `6`: `153`, `20`: `97`, `21`: `15`), so this review is no longer dominated by legacy accumulated noise
  - however, repeated aircraft-pair observations are still far too inconsistent for localisation-grade hyperbolic solving: common repeated pairs show `4 ms` to `10 ms` TDOA spread, and some high-count examples still span nearly the full `±5 ms` admission limit
  - persisted localisation outputs for those same IIDs remain physically absurd (multi-million-metre CEP or impossible coordinates), and `/api/radar/map` still exposes no credible solved radars
  - IID `20` remains a useful holdout case: it is `ESTABLISHED` at Stage 1 but still appears residual-heavy, reinforcing that Stage 1 confidence alone is not sufficient to admit Stage 2 calibration data
  - next work should stay on Stage 2B observation quality: tighter co-sweep timing association, repeated-consistency gating, and eventually dwell-centre refinement rather than more solver tuning
- `2026-04-06`: Stage 2B now applies a repeated-consistency gate before solving:
  - the localiser only keeps aircraft-pair families that show at least three repeats within a short time window and within a tight TDOA spread
  - unstable repeated families remain in calibration history for diagnostics, but they no longer contaminate the Stage 2C solver input
  - focused tests now cover stable-family retention, unstable-family rejection, and solve-time rejection when no consistent pair families survive
  - on the current post-purge DB snapshot, the gate is selective rather than catastrophic: surviving observations were `1: 40/164`, `3: 20/150`, `6: 159/267`, `20: 113/165`, `21: 43/43`
- `2026-04-06`: Re-running localisation after the repeated-consistency gate still leaves Stage 2C at `NO-GO`:
  - representative post-purge IIDs `1`, `3`, `6`, `20`, and `21` all still reject under the local solver even after filtering
  - current filtered outcomes were:
    - `1`: `40/193` surviving pairs, solver hit search boundary at about `462 km`
    - `3`: `20/173` surviving pairs, solver residual about `3012 us`
    - `6`: `171/296` surviving pairs, solver hit search boundary at about `429 km`
    - `20`: `130/192` surviving pairs, solver hit search boundary at about `465 km`
    - `21`: `43/47` surviving pairs, solver residual about `3122 us`
  - `/api/radar/map` still returns no credible solved radars, which is preferable to publishing nonsense, but confirms that Stage 2B observation quality is still not sufficient
  - the next likely leverage point is not more solver tuning but improving the timing measurement itself, most likely by refining burst-centre timing and/or tightening co-sweep association further
- `2026-04-06`: The next Stage 2B experiment is now formalised as a coincidence-quality check:
  - identify near-coincident aircraft pairs within one sweep as a stronger directional constraint class
  - measure whether that subset is materially more stable than the general co-sweep pair pool
  - if it is, use coincidence pairs as a localisation bootstrap or preferred solver admission path rather than treating all co-sweep pairs as equally valuable
- `2026-04-06`: The first coincidence-based Stage 2B admission experiment is now implemented:
  - the localiser prefers a near-coincident subset (`|tdoa_us| <= 500`) after consistency filtering when that subset is large enough for solving
  - focused tests cover coincidence preference and fallback to the broader consistent set when coincidence is too sparse
  - on the current post-purge DB snapshot, the solver now chooses the coincident subset for representative IIDs `1`, `6`, and `20`, and falls back to the broader consistent set for `3` and `21`
  - however, the representative solve outcomes remain `NO-GO`:
    - `1`: `10` coincident pairs selected, solver hit search boundary at about `462 km`
    - `3`: fallback to `24` consistent pairs, solver hit search boundary at about `400 km`
    - `6`: `68` coincident pairs selected, solver hit search boundary at about `535 km`
    - `20`: `54` coincident pairs selected, solver hit search boundary at about `481 km`
    - `21`: fallback to `68` consistent pairs, residual about `3150 us`
  - this means coincidence-based admission is worth keeping as a higher-grade subset, but it is not sufficient on its own; burst-centre timing and/or co-sweep association still need materially better measurement quality
- `2026-04-06`: Burst-centre refinement is now implemented in the backend calibration path:
  - bursts with at least two replies carrying usable signal now compute an amplitude-weighted internal beam-centre timestamp instead of relying only on the raw centroid
  - sweep grouping and calibration-pair generation now consume that refined burst timestamp when available, while preserving the raw centroid as a safe fallback for shallow bursts
  - focused tests now cover both the amplitude-weighted refinement itself and the use of refined burst timing in calibration-pair TDOA generation
  - live impact on localisation still needs a fresh post-restart review; this change has been verified by focused backend tests and compile checks only so far
- `2026-04-06`: Post-purge review after burst-centre refinement shows partial improvement but still leaves Stage 2C at `NO-GO`:
  - representative calibration pools now look different from the previous run (`1`: `194`, `3`: `347`, `6`: `208`, `20`: `103`, `21`: `0` rows at review time)
  - representative solve outcomes improved from search-boundary failures to lower but still unacceptable residuals on some IIDs:
    - `1`: `27` consistent / `10` coincident selected, residual about `244 us`
    - `6`: `68` consistent / `28` coincident selected, residual about `260 us`
    - `3`: `34` consistent selected, residual about `3075 us`
    - `20`: `59` consistent / `27` coincident selected, still hit search boundary at about `416 km`
    - `21`: no fresh consistent pairs at review time
  - `/api/radar/map` still returns no credible solved radars, so Stage 2C remains correctly blocked
  - this is still useful progress because some representative IIDs now fail with hundreds of microseconds residual rather than multi-millisecond or pure boundary failures, which suggests the burst-centre refinement is reducing timing noise
  - however, the same live review also exposed a Stage 1 concern: IID `1` was reported as `1.1211s` in `/api/radar/rotation`, which is obviously inconsistent with its previously established behaviour and must not be ignored while Stage 2 work continues
- `2026-04-06`: Per-sweep solving with weighted accumulation is now implemented as the preferred Stage 2C structure:
  - the localiser can now group calibration pairs by sweep timestamp, solve sweeps independently, and accumulate accepted sweep estimates with quality-weighted averaging in local ENU space
  - the background radar solve loop now uses that per-sweep weighted path rather than one global solve across the entire pair pool
  - focused tests cover sweep grouping, successful weighted accumulation across good sweeps, and rejection when too few sweep solves survive
  - immediate local review on the current representative IIDs (`1`, `3`, `6`, `20`, `21`) found `0` good sweep solves for each, so the current blocker is now sharper: individual sweeps do not yet contain enough internally trustworthy geometry/timing to produce usable per-sweep radar estimates
  - this is still useful because it shows the next work is not better long-term averaging logic; it is improving same-sweep observation quality and/or broadening the within-sweep solve candidate set enough that at least a few sweeps can solve cleanly
- `2026-04-06`: Direct per-sweep multi-aircraft solving is now implemented in place of per-sweep pairwise solves:
  - each sweep is reconstructed as a set of aircraft with relative arrival offsets to an anchor aircraft, and the solver fits radar position directly against those sweep-level observations
  - representative local review still found `0` good sweep solves for IIDs `1`, `3`, `6`, `20`, and `21`, so the current blocker is now very specific: present sweep groups do not contain enough same-sweep aircraft geometry/timing quality to survive even the more correct direct sweep model
  - next work should therefore inspect why sweeps are failing (too few aircraft, poor azimuth spread, unstable relative timing, or over-fragmented sweep grouping) before any more localisation-layer changes are attempted
- `2026-04-06`: Per-sweep failure diagnostics now show the current blocker is mostly light traffic, not hidden solver instability:
  - representative breakdown on the current snapshot was:
    - IID `1`: `209` sweeps, `191` `too_few_aircraft`, `18` `missing_anchor_pair`
    - IID `3`: `406` sweeps, `344` `too_few_aircraft`, `61` `missing_anchor_pair`, `1` `search_boundary`
    - IID `6`: `290` sweeps, `252` `too_few_aircraft`, `38` `missing_anchor_pair`
    - IID `20`: `144` sweeps, `140` `too_few_aircraft`, `4` `missing_anchor_pair`
    - IID `21`: `69` sweeps, `68` `too_few_aircraft`, `1` `missing_anchor_pair`
  - that means the dominant current limitation really is insufficient same-sweep aircraft count on this traffic snapshot
  - the secondary issue is sweep connectivity: some sweep groups do not contain enough pair coverage to reconstruct all aircraft relative to one anchor, which points at either over-tight pair filtering or the need for a more general graph-based relative-timing reconstruction
  - radar calibration lookup now interpolates between adjacent ADS-B position samples at the sweep time
  - when the sweep falls just outside the nearest ADS-B report, lookup uses only short motion-vector extrapolation instead of long coarse-track projection
  - the old track-store lookup remains only as a fallback when no ADS-B history exists
  - focused backend regressions cover both adjacent-sample interpolation and short motion-vector extrapolation
- `2026-04-06`: Live Stage 2B/2C review found the next real blocker:
  - recent calibration rows for representative IIDs are now effectively unique and varied enough to use as observations
  - however, the solver is still fed raw observed `tdoa_us` and does not subtract the known receiver-to-aircraft path term from the proposal equation
  - persisted `radar_iids` localisation outputs are therefore not trustworthy; representative IIDs include absurd stored coordinates and `cep_m` values in the hundreds of millions of metres
  - re-running the current bounded solver locally against persisted deduped pairs rejects those representative IIDs with either huge residuals or boundary-hitting fits
  - the next Stage 2C implementation task is to correct the solver input equation, not to tune the current numerical fit further
- `2026-04-06`: The Stage 2C equation has now been corrected to subtract the known receiver-path term from each calibration pair:
  - the solver and hyperbola overlay now use the proposal's receiver-path-corrected range-difference target rather than raw burst `tdoa_us`
  - a focused regression proves the corrected residual goes to zero for a synthetically consistent pair
  - representative persisted IIDs still reject after the equation fix, which means the next blocker is now observation quality rather than the top-level solver equation
- `2026-04-06`: Stage 2B pair-quality diagnostics now point at the next concrete gate:
  - on representative persisted IIDs (`1`, `6`, `9`, `21`, `74`), short-interval repeat observations of the same aircraft pair often jump by millions of metres-equivalent or flip sign within seconds
  - representative examples include the same pair reversing from roughly `-64 ms` to `+57 ms` TDOA less than a second later, which is not physically plausible for one radar/aircraft geometry
  - that indicates the dominant current failure is observation quality, most likely from overly loose co-sweep association and/or weak burst-centre timing
  - the next implementation step should therefore tighten pair admission before further solver work, starting with co-sweep window/gating and repeated-consistency checks
- `2026-04-06`: The first Stage 2B pair-quality gate is now in place:
  - calibration pair generation now rejects candidate pairs whose absolute burst-centroid TDOA exceeds `5 ms`
  - focused regression coverage proves that broad sweep-cluster membership alone no longer makes a large-delay pair admissible
  - on representative persisted IIDs, only about `6%` to `9%` of the old rows would survive that gate, confirming how much of the legacy calibration pool was implausibly loose
  - because the database still contains previously stored loose pairs, localisation quality will not improve until IIDs are relearned or stale calibration rows are purged

## Ordered Work

- [x] `HF0` — Scope the high-frequency page and backend load model
  - Review the addendum’s high-frequency proposals and separate them into:
    - `Core V1` displays that should ship on the first dedicated page
    - `Later V2` displays that depend on extra metadata or materially higher rendering cost
  - Recommended `Core V1` set for planning:
    - Message timing visualisation (`16`)
    - Interrogator timing lanes (`17`) with accurate timing
    - Burst-rate strip chart (`18`)
    - Pipeline latency oscilloscope (`19`)
    - DF cadence lanes (`24`)
  - Candidate `V2` items after the shared infrastructure is proven:
    - Signal-floor shimmer (`20`)
    - Source-mix pulse monitor (`21`)
    - Micro-timeline of airspace activity (`22`)
    - Bearing-time sweep heatmap (`23`)
    - Waterfall / pulse-bloom / constellation variants (`25`–`27`)
  - Define a single canonical recent-message event model for high-frequency displays, likely extending to:
    - `arrival_ts_us` derived from Beast timestamp
    - `df`
    - `msg_bits` or `msg_len`
    - `signal_raw`
    - `source_class`
    - optional `icao`
    - optional `iid`
  - Define backend load strategy for simultaneous display:
    - one shared in-memory recent-message buffer
    - one shared high-frequency stream per page/client, not one stream per widget
    - derive cheap aggregations client-side where possible
    - derive expensive aggregations server-side once per tick if they are shared across widgets
    - bounded retention and bounded per-client queues
  - Verification gate:
    - `tasks/todo.md` records the chosen V1 scope and explicit deferrals.
    - The plan includes a backend load budget and a rule for simultaneous widgets so we do not build each panel as an independent producer.
  - Chosen `Core V1` scope:
    - `16` Message timing visualisation, corrected to Beast-time fidelity
    - `17` Interrogator timing lanes, aligned to the same accurate timing model
    - `18` Burst-rate strip chart
    - `19` Pipeline latency oscilloscope
    - `24` DF cadence lanes
  - Explicit `V2` deferrals:
    - `20` Signal-floor shimmer
    - `21` Source-mix pulse monitor
    - `22` Micro-timeline of airspace activity
    - `23` Bearing-time sweep heatmap
    - `25` Message waterfall
    - `26` Polar pulse bloom
    - `27` Constellation drift view
  - Canonical recent-message event model for `V1` planning:
    - `arrival_ts_us` in Beast-relative microseconds
    - `df`
    - `msg_bits`
    - `signal_raw`
    - `source_class`
    - optional `icao`
    - optional `iid`
  - Backend load model:
    - One shared recent-message ring buffer, short retention only.
    - One shared high-frequency aggregation path, not one producer per panel.
    - One multiplexed high-frequency WebSocket per page/client, with typed messages for:
      - raw/incremental recent-message events
      - shared aggregate bins/traces needed by multiple widgets
      - low-rate status/heartbeat metadata
    - Per-client queue must stay bounded, matching the existing philosophy used by the main `/ws` snapshot path.
  - Planning load budget:
    - Design target: tolerate at least `3,000 msg/s` sustained for a `5 s` window on the dedicated page.
    - Raw retention budget: roughly `15,000` recent-message events in memory for V1, with room to raise if profiling justifies it.
    - Aggregation cadence target: `10 Hz` page stream updates, with `requestAnimationFrame` rendering client-side.
    - The backend should scan/aggregate the shared recent buffer at most once per tick for page-wide shared products; widgets must not each trigger their own buffer walk.
    - Keep the main `/ws` snapshot path and the new high-frequency path isolated so opening the high-frequency page does not bloat or slow normal clients.

- [x] `HF1` — Correct timing source to use Beast timestamp space
  - Stop using wall-clock `time.time()` as the plotted X-axis source for message timing.
  - Introduce a dedicated recent-message timing ring buffer keyed by Beast timestamp, not decode-time wall clock.
  - Handle timestamp unwrap/rollover explicitly so the last 2–5 seconds remain monotonic even across counter wrap or reconnect.
  - Define a canonical event shape for high-frequency displays, likely including:
    - `beast_ts_ticks`
    - `rel_us` or equivalent plot-ready relative microseconds
    - `df`
    - `msg_len`
    - optional metadata for later displays such as `iid`, `icao`, and source stream
  - Verification gate:
    - Consecutive events are monotonic in Beast time within the live window.
    - Plot timing no longer shows obvious wall-clock quantisation/jitter unrelated to message cadence.
    - Ring buffer behavior across reconnect/wrap is documented and tested with a focused synthetic case if practical.

- [x] `HF2` — Establish a dedicated high-frequency page
  - Add a new top-level page/tab for high-rate observability rather than embedding these panels inside [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
  - Move the message timing display and interrogator timing display onto that page, and design the page so additional V1 high-frequency panels can coexist without multiplying backend streams.
  - Give the page its own local controls for time window, lane mode, pause/freeze, and display density without cluttering Receiver.
  - Keep Receiver focused on lower-frequency diagnostics and summary charts.
  - Verification gate:
    - New page is accessible from the top navigation in [App.jsx](/home/keith/claude/adsb-dashboard/frontend/src/App.jsx).
    - Receiver no longer owns the high-frequency visualisations.
    - Existing receiver diagnostics remain functional.

- [x] `HF3` — Rebuild the message timing renderer to match proposal semantics
  - Render bars with width derived from `msg_len` so 56-bit and 112-bit messages are visually distinct.
  - Use Beast-time relative coordinates, not wall-clock interpolation.
  - Replace the current fixed-width tick rendering with a batching-oriented canvas pipeline designed for sustained high message rates.
  - Add overlap handling:
    - either per-lane stacking
    - or overlap shading/density accumulation
  - Verification gate:
    - Short and long messages are visibly different widths.
    - Dense bursts show meaningful overlap behavior rather than simply overpainting ticks.
    - Rendering remains smooth while the page is visible.

- [x] `HF4` — Move interrogator timing onto the high-frequency page and align its timing source
  - Relocate the current interrogator timing lanes from Receiver to the new page.
  - Replace the current wall-clock-ish timing source with the same Beast-relative timing model used by the message timing view so interrogator cadence is temporally accurate.
  - If needed, extend the recent-message event model or maintain a derived IID event stream keyed off the same Beast-relative clock rather than a separate wall-clock buffer.
  - Preserve row-click-to-map behavior.
  - Verification gate:
    - Interrogator lanes render correctly on the new page.
    - Click-through to map still works.
    - Timing model is documented and consistent with the page’s message timing display.

- [x] `HF4A` — Add Burst/Latency/Cadence V1 panels to the high-frequency page
  - Add `Burst-rate strip chart` from proposal item `18`, derived from the shared recent-message buffer using 10/20/50 ms bins.
  - Add `Pipeline latency oscilloscope` from proposal item `19`, using existing runtime timing stores plus any small additional retention needed for a scrolling view.
  - Add `DF cadence lanes` from proposal item `24` as a lower-cost aggregate complement to the full per-message timing view.
  - Make these views share the same transport/buffer foundation rather than each polling or streaming independently.
  - Chosen execution split:
    - `HF4A.1` backend: add timestamped short-retention performance traces so latency can be rendered as a moving waveform instead of percentiles only
    - `HF4A.2` backend: add one shared high-frequency aggregate feed that scans the recent message window once per tick and emits:
      - burst-rate bins
      - DF cadence bins by family
      - latency traces
    - `HF4A.3` frontend: add three new Timing-page panels consuming that shared aggregate feed
    - `HF4A.4` verification: confirm simultaneous display works from one aggregate producer rather than per-widget scans
  - Concrete data model for `HF4A`:
    - Recent message aggregates stay on the existing Beast-relative clock with a `window_us` and `now_us`
    - Burst-rate chart uses configurable `bin_ms` and one `counts` array for all messages
    - DF cadence lanes use the same window with a coarser `bin_ms` and per-family count arrays
    - Pipeline latency oscilloscope uses wall-clock `ts_s` samples because it reflects runtime pipeline behaviour, not RF arrival timing
  - Family grouping for `DF cadence lanes`:
    - `ADS-B` = `DF17`
    - `TIS-B` = `DF18`
    - `All-Call` = `DF11`
    - `Comm-B` = `DF20` + `DF21`
    - `Surveillance` = `DF4` + `DF5`
    - `ACAS` = `DF0` + `DF16`
    - `Other` = any remaining DF present in the recent window
  - Verification gate:
    - All selected V1 panels render on the new page from shared live data.
    - Simultaneous display of V1 panels does not require multiple independent backend scans of the same raw event history.

- [x] `HF5` — Tighten the streamed transport for high-frequency displays
  - Keep the dedicated streamed channel concept, but align it to the new canonical Beast-time event model.
  - Prefer a page-specific multiplexed WebSocket with typed messages over one socket per widget, unless profiling shows a simpler split is clearly cheaper.
  - Ensure the stream carries only incremental data and remains robust under idle periods, reconnects, and bursty traffic.
  - Chosen execution split:
    - `HF5.1` backend: add one `Timing` page websocket that carries typed payload sections for:
      - incremental timing events
      - current interrogator lanes
      - shared aggregate panels
    - `HF5.2` frontend: add one page-level stream hook in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and pass the unified data into the existing panels
    - `HF5.3` compatibility: keep `/ws/timing`, `/ws/interrogators`, and `/ws/highfreq` in place for now so the transport refactor does not force a risky big-bang removal
  - Payload contract for the page stream:
    - `type: "timing_page"`
    - `timing: { now_us, events }` using the existing incremental event shape
    - `interrogators: { now_us, window_s, lanes }`
    - `aggregates: { now_us, window_s, burst, cadence, latency }`
  - Transport rules:
    - timing events remain incremental per connection via `since_seq`
    - interrogator and aggregate sections are latest-state snapshots
    - idle heartbeats still send the typed envelope with empty `events`
  - Verification gate:
    - Stream reconnects cleanly without corrupting local buffers.
    - Idle heartbeat behavior is defined and does not cause visual artifacts.
    - Payload rate stays bounded under heavy traffic.

- [x] `HF6` — Verification and acceptance pass for the whole high-frequency page
  - Verify against known traffic patterns:
    - dense DF17 periods
    - DF11 interrogator rhythm
    - MLAT-heavy periods if available
  - Compare displayed temporal spacing against Beast timestamp deltas, not just visual intuition.
  - Confirm the page behaves sensibly when hidden, resized, or reopened.
  - Add a short review note here with observed behavior and any intentional scope cuts.
  - Verification gate:
    - Backend syntax/build checks pass.
    - Frontend build passes.
    - Manual runtime validation confirms the display is no longer obviously quantized and better reflects true message arrival timing.

- [ ] `HFV2` — Plan the remaining addendum high-frequency items
  - Purpose:
    - Capture the remaining addendum items as a deliberate `V2` phase rather than leaving them as loose ideas.
    - Do not treat this as implementation-ready scope yet; each item still needs proper exploration, event-model review, rendering design, and backend load assessment before coding.
  - Remaining addendum candidates:
    - `20` Signal-floor shimmer / RF health panel
    - `21` Source-mix pulse monitor
    - `22` Micro-timeline of airspace activity
    - `23` Bearing-time sweep heatmap
    - `25` Message waterfall
    - `26` Polar pulse bloom
    - `27` Constellation drift view

## OpenSpec Change: `add-high-frequency-airspace-micro-timeline`

- [x] Review the existing timing-event ingest, websocket/API serialization, and Timing-page buffer contracts.
- [x] Extend backend timing events to retain inline `icao` at ingest time and serialize the widened event shape on `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
- [x] Update the Timing-page client buffer to consume widened events without regressing existing panels that share the same stream.
- [x] Add the `Micro-timeline of airspace activity` panel with a stable top-`N` row policy, sticky membership, and first-pass source-based coloring.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic runtime check covering idle, multi-aircraft, and row-stability behavior.

### Review

- Plan verified against `openspec/changes/add-high-frequency-airspace-micro-timeline/{proposal,design,tasks}.md` and the two spec files before implementation.
- Backend event retention now stores inline `icao` in the recent-message buffer and serializes the widened tuple consistently through the direct API, `/ws/timing`, and `/ws/timing-page`.
- The Timing page now includes a full-width `Micro-timeline of airspace activity` panel backed by the existing shared timing buffer and a pure helper that applies slower ranking refreshes with sticky row membership.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - `node --input-type=module` synthetic helper check confirmed `idleRows: 0`, stable initial rows `AAAAAA/BBBBBB/CCCCCC`, and admission of new entrant `DDDDDD` without reordering existing rows.
  - Exploration work required before implementation:

## OpenSpec Change: `add-live-message-field-page`

- [x] Review the current timing-event tuple, signal conversion helpers, and Timing-page exact-point panels before widening the event contract.
- [x] Widen retained timing events and transports to carry optional `bearing_deg`, `range_nm`, `iid`, and trusted display-grade signal values without adding per-widget transports.
- [x] Add focused backend coverage for the widened event shape, including publishability gates for `bearing_deg`/`range_nm` and availability rules for `iid`.
- [x] Add a dedicated top-level `Message Field` page with one large exact-point plot, selectable geometry/mode, bounded persistence, and compact colour/filter controls.
- [x] Remove the shelved waterfall panel from `Timing` and update related copy so `Timing` returns to timing/observability work while exact-point experimentation moves to the new page.
- [x] Verify backend syntax/tests, run `npm run build`, perform a focused synthetic persistence check, and compare signal semantics against readsb expectations before closing the change.

### Planned execution

- Treat the existing `raw_signal_to_dbfs()` path as the canonical readsb-aligned signal conversion unless code inspection or synthetic checks prove otherwise; do not ship a second signal formula.
- Keep one shared recent-event transport contract across `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`; page controls must operate from the local buffered stream instead of reconnecting per mode.
- Move exact-point exploratory views off `Timing` rather than duplicating them. The dedicated page will own the large-format exact-point canvas and the associated mode/filter controls.
- Finish the remaining tuning pass by tightening defaults and control behaviour around the shipped page, specifically:
  - make the default mode/geometry pairing match the intended first-view workflow
  - remove or reduce control states that imply a projection is active when the current mode is rendered cartesian anyway
  - tune point sizing and empty-state chrome so dense DF11/DF17 traffic remains legible without covering the plotting frame
- Follow up on live-traffic polish reported after the first tuning pass:
  - slow auto-zoom-in reactions so intermittent traffic does not make range-based views breathe in and out
  - make short-persistence `bearing × time` points slightly more prominent
  - ensure `30 s` persistence is supported by the client buffer instead of truncating early under load
  - widen the traffic filter beyond `all` and `DF11 only`
  - add an IID filter mode that excludes `IID 0` squitters from SSR-focused analysis
  - extend the auto-zoom shrink delay to a real operator-friendly hold window of roughly `30–60 s`, not just gentle frame-based damping
  - remove superfluous explanatory chrome around the page and make the `bearing × time` viewport scroll from a monotonic local render clock instead of directly from each packet timestamp
  - apply the same monotonic render-clock behavior consistently across the other live Timing/Receiver plots that still interpolate from `now_us`
  - keep signal-axis orientation consistent so `0 dBFS` is the top/high end on shimmer-style plots too

### Verification

- Backend: focused syntax/tests around the widened timing-event contract and optional field gating.
- Frontend: `npm run build` after the new page, routing, and Timing-page cleanup land.
- Synthetic/runtime: confirm points remain exact and persistence-based, aging out of the selected window rather than being bucketed or rescaled.
- Signal semantics: compare the exposed dBFS values against the existing readsb-style conversion helpers before signing off on signal-based axes/colouring.
- Tuning sign-off: document what was tuned locally and call out whether a real live-receiver pass was or was not possible in the current environment.

### Review

- Plan verified against `openspec/changes/add-live-message-field-page/{proposal,design,tasks}.md` and the two spec files before implementation.
- Polish/tuning follow-up plan for the remaining archive-signoff task:
  - [x] Revisit the `Message Field` page defaults so the initial mode/projection combination matches the primary exact-point transient-use case.
  - [x] Tighten geometry control behavior so unsupported or low-value projection combinations are not presented as equal defaults.
  - [x] Re-run focused frontend verification after the tuning pass and record the result here.
  - [x] Slow auto-zoom contraction in the frontend range-based projections and raise the message-field client buffer cap so `30 s` persistence is not clipped prematurely by the local event store.
  - [x] Expand traffic/IID filtering so SSR-focused scans can exclude `IID 0` while still supporting other DF families.
- Backend widened the shared recent timing-event contract by appending optional `range_nm` and `iid` while keeping `/api/timing/events`, `/ws/timing`, and `/ws/timing-page` on one transport shape.
- Focused backend tests now cover inline range retention, `iid` retention including `IID 0`, and publishability gating for bearing/range fields.
- Frontend added a new top-level `Message Field` page with one exact-point canvas, selectable modes, `polar`/`cartesian` projection handling, bounded persistence controls, and colour/filter controls backed by the existing shared timing stream.
- `Timing` now points operators to the dedicated page for exact-point experimentation and no longer renders the shelved waterfall panel.
- Tuning follow-up adjusted the `Message Field` page so the default `bearing × time` view now starts in its natural Cartesian projection, while the projection selector narrows itself to the geometries that actually make sense for the chosen mode instead of implying that every mode is equally polar/cartesian.
- Second tuning follow-up widened the page’s traffic filters to include `DF17`, `DF18`, `DF20/21`, `DF4/5`, `DF0/16`, and `Other DF`, added an `Exclude IID 0` option for SSR-focused scans, increased the client timing buffer size used by the page so `30 s` persistence has room to survive heavier traffic, and changed range-based auto-scaling to expand immediately but contract gradually so intermittent points do not make the plot breathe.
- `Bearing × time` now renders slightly brighter/larger points for `1 s` and `3 s` persistence so sparse intermittent traffic remains readable at short windows.
- Latest cleanup pass removed the superfluous hero/notes panels and changed the Message Field render clock to extrapolate from a monotonic local timebase, so intermittent timing packets should no longer nudge the `bearing × time` viewport backward or forward.
- Consistency pass applied the same monotonic render-clock helper across the Timing-page canvases and the Receiver-page timing/interrogator canvases so all live timing plots advance from the same no-backwards-scroll rule.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py backend/tests/test_timing_events.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - `npm run build` in `frontend/` after the tuning follow-up
  - `npm run build` in `frontend/` after the second tuning follow-up
  - `npm run build` in `frontend/` after the monotonic-clock consistency pass
  - `node --input-type=module` synthetic check on `selectMessageFieldEvents()` returned visible sequence ids `[2, 3]` for a `3 s` window at `5.5 s`, confirming older exact points age out cleanly instead of being rebucketed.
  - Signal semantics check compared `raw_signal_to_dbfs()` against `20 * log10(raw / 255)` for raw samples `1, 12, 37, 128, 255`; all matched expected readsb-style dBFS values exactly.
  - Live-receiver tuning pass is still pending. This environment allowed code inspection and build verification, but not a real DF11/DF17 traffic validation pass for the new zoom damping, short-window point prominence, or the `30 s` retention feel under real traffic bursts.
    - Confirm which additional fields are needed in the recent-message event model:
      - `signal_raw`
      - `source_class`
      - `icao`
      - optional bearing/range or enough metadata to derive them cheaply
    - Decide which displays belong on the existing `Timing` page versus a separate advanced visualisation page.
    - Profile simultaneous-display load now that `V1` is working, so `V2` does not overrun the shared buffer/stream model.
    - For each candidate, define:
      - operator value
      - exact data requirements
      - rendering approach
      - expected refresh cadence
      - mobile/responsive impact
      - acceptance criteria
  - Recommended `V2` planning order:
    - `HFV2.1` signal/source/data-model exploration for items `20` and `21`
    - `HFV2.2` aircraft/bearing/spatial-data exploration for items `22`, `23`, `26`, and `27`
    - `HFV2.3` choose the first implementation subset based on value versus load
    - `HFV2.4` implement one item at a time with the same verify-before-next rule used for `V1`
  - Investigation result on 2026-04-04:
    - Proposed next implementation slice is `HFV2A` on the existing `Timing` page, centered on proposal items `20` and `21`.
    - Chosen first-slice scope:
      - widen the shared recent-message event model with per-message `signal_raw` and a compact `source_class`
      - add `Signal-floor shimmer / RF health panel`
      - add `Source-mix pulse monitor`
    - Reasoning:
      - these two panels add operator-facing value while reusing the current Beast-time ring buffer and multiplexed `/ws/timing-page` transport
      - they de-risk the `V2` data model with lighter backend changes than the bearing/range-dependent views
      - they keep the first `V2` slice within the existing load rule of one shared recent-message buffer and one page stream
    - Deferred beyond `HFV2A`:
      - `22` Micro-timeline of airspace activity after deciding whether per-message `icao` belongs in the same enriched event model
      - `23`, `26`, and `27` until per-message spatial attribution is defined and profiled
      - `25` until the enriched buffer is proven and a stronger visual grammar is chosen for the more decorative views
    - OpenSpec change created for this next-stage proposal:
      - `openspec/changes/plan-high-frequency-v2-visualizations/`
  - Follow-on exploration result on 2026-04-04:
    - Proposed next implementation slice after `HFV2A` is `HFV2B`: proposal item `22`, `Micro-timeline of airspace activity`.
    - Chosen `HFV2B` direction:
      - widen the shared recent-message event model with inline `icao`
      - keep the panel on the existing `Timing` page
      - use a stable top-`N` row policy driven by a slower ranking cadence rather than live per-frame sorting
      - start with lightweight color semantics, preferably `source_class`, instead of widening the event model again for altitude or spatial color
    - Reasoning:
      - item `22` is the smallest remaining `V2` step that materially improves operator readability
      - inline `icao` keeps the client event buffer self-contained and avoids fragile joins against mutable aircraft state
      - this de-risks aircraft-centric high-frequency views before the much larger spatial-attribute work needed by `23`, `26`, and `27`
    - OpenSpec change created for this follow-on proposal:
      - `openspec/changes/add-high-frequency-airspace-micro-timeline/`

## OpenSpec Change: `add-high-frequency-bearing-time-sweep`

- [x] Review the current timing-event retention, websocket/API serialization, and Timing-page buffer contract for the next spatial slice.
- [x] Extend backend timing events to retain optional inline `bearing_deg` at ingest time and serialize the widened shape on `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
- [x] Update the Timing-page client buffer to consume widened events without regressing the existing panels that share the same stream.
- [x] Add the `Bearing-time sweep heatmap` panel to `frontend/src/pages/TimingPage.jsx`.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic check covering empty state plus mixed bearing/no-bearing traffic.

### Review

- Plan verified against `openspec/changes/add-high-frequency-bearing-time-sweep/{proposal,design,tasks}.md` before implementation.
- Backend timing events now carry optional inline `bearing_deg`, captured after the aircraft record has been updated so the shared event buffer stays self-contained without a later live-state join.
- `/api/timing/events`, `/ws/timing`, and `/ws/timing-page` now serialize the widened event shape consistently for the Timing page and legacy timing consumers.
- The Timing page now includes a full-width `Bearing-time sweep heatmap` panel driven by the existing shared event buffer; only bearing-attributed events are plotted, while unattributed traffic still appears in the other timing panels.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - `node --input-type=module` synthetic heatmap-bucketing check confirmed `empty={attributedCount:0,occupiedCells:0,maxCount:0}` and `mixed={attributedCount:3,occupiedCells:2,maxCount:2}` for a shared-stream sample containing both bearing and no-bearing events.

- [x] `Item I` — Documentation scope correction for Phase `0A`
  - Update `.docs/development/polish-and-features-proposal.md` to state that the delivered result is shared filter helpers/predicates, not shared global filter state.
  - Update `.docs/development/combined-deficiency-report.md` to reflect that Coverage is intentionally not part of a shared aircraft-filter UI/state model.
  - Fix the `Item J` status-table mismatch in `.docs/development/deficiency-remediation.md` while touching the remediation docs, so the remaining-work list is internally consistent.
  - Verification gate:
    - Confirm the docs consistently describe `useAircraftFilter` as local state plus shared predicate helpers.
    - Confirm no remaining document claims say Coverage should share interactive filter state with Live/Map/SkyView.
    - Confirm `Item J` refers to loading/error-state adoption everywhere in the remediation doc.

- [x] `Item J1` — Adopt `useFetchState` on `SkyView`
  - Replace inline fetch/state handling in [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) for terrain, history, and tracks with [useFetchState.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/useFetchState.js) where appropriate.
  - Render [LoadingState.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/LoadingState.jsx) for loading, empty, and error states instead of ad hoc null-state handling.
  - Preserve existing polling behavior for track refreshes; only the state machine and user-visible status handling should change.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - SkyView shows a loading state before async overlay data arrives.
    - SkyView shows retryable error UI on failed fetches.
  - Disposition:
    - Waived by user on 2026-04-04 after clarifying this had already been reviewed and judged unnecessary because the current fetch path is fast enough.

- [x] `Item J2` — Adopt `useFetchState` on `CoveragePage`
  - Refactor historical/timelapse overlay fetches in [CoveragePage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/CoveragePage.jsx) to the same shared fetch-state pattern.
  - Keep current Three.js rendering logic intact; this is a fetch-state and UI-state cleanup, not a rendering redesign.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - Coverage historical overlays show loading and retryable error states.
    - No regression in existing timelapse/history rendering path once data is available.
  - Disposition:
    - Waived by user on 2026-04-04 for the same reason as `Item J1`; no code change required.

- [x] `Item K1` — Add active IID table to `ReceiverPage`
  - Extend [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the existing `/api/interrogators` data is presented as a table of active IID codes with counts and last-seen information if available from the API shape.
  - Keep the existing `InterrogatorTimeline` canvas; the table complements it rather than replacing it.
  - If last-seen data is missing from the current API, add the minimal backend support needed before finalizing the UI.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - Receiver page shows IID rows from live API data, not placeholder/static content.
    - Table values match `/api/interrogators` responses for the selected window.

- [x] `Item K2` — Capture and expose `msg_len` for timing events
  - Update [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) timing storage from `(ts, df)` to `(ts, df, msg_len)`.
  - Update [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) response serialization so clients receive message length with each timing event.
  - Update the timing plot in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) to consume the expanded event shape without breaking existing rendering.
  - Verification gate:
    - Backend starts cleanly.
    - `/api/timing/events` returns triples including message length.
    - Timing plot still renders with live polled data after the schema change.

- [x] `Item K3` — Implement real streamed transport for Item `16`
  - Replace the current HTTP polling transport for the message timing plot with a dedicated streamed channel, using WebSocket or SSE as originally intended.
  - Keep the payload aligned with `Item K2`: timing events must stream `ts`, `df`, and `msg_len`.
  - Preserve the current plot behavior while removing the fixed 200 ms HTTP polling loop from [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
  - Verification gate:
    - Backend starts cleanly with the new streamed timing endpoint.
    - The Receiver page timing plot receives live events over the stream rather than repeated HTTP fetches.
    - `npm run build` succeeds in `frontend/`.

## Review

- `Item I` verified on 2026-04-04.
  - Updated [polish-and-features-proposal.md](/home/keith/claude/adsb-dashboard/.docs/development/polish-and-features-proposal.md) to describe shared filter helpers/control patterns rather than shared cross-page filter state.
  - Updated [combined-deficiency-report.md](/home/keith/claude/adsb-dashboard/.docs/development/combined-deficiency-report.md) to reframe Phase `0A` as a documentation-scope issue and to state explicitly that Coverage should not be treated as part of a required shared aircraft-filter UI/state model.
  - Updated [deficiency-remediation.md](/home/keith/claude/adsb-dashboard/.docs/development/deficiency-remediation.md) so `Item J` now correctly refers to the Phase `0B` shared loading/error pattern instead of the already-completed SkyView history heatmap work.
- `Item J` waived on 2026-04-04.
  - User clarified this had already been addressed in a previous review and intentionally not implemented because the current fetch path is fast enough that explicit loading-state UI is unnecessary.
  - No code change made for [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) or [CoveragePage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/CoveragePage.jsx).
- `Item K1` verified on 2026-04-04.
  - Added per-IID activity metadata in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) and exposed `last_seen` plus `latest_icao` from [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py).
  - Extended the existing IID card in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) with an active-code table showing IID, reply count, last-seen age, and latest aircraft, with supporting styles in [ReceiverPage.module.css](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.module.css).
  - Follow-up fix: switched the IID card from one-shot `useFetch` to a 1 Hz polling loop so the chart and table refresh live without requiring a window change.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/interrogators.py` and `npm run build` in `frontend/`.
- `Item K2` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so timing events now store `(ts, df, msg_len)` on both native and fallback decode paths.
  - Updated [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) so `/api/timing/events` now returns `events: [[ts, df, msg_len], ...]`.
  - Updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the timing plot consumes the expanded event shape without changing current rendering behavior.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/timing.py` and `npm run build` in `frontend/`.
- `Item K3` verified on 2026-04-04.
  - Added a dedicated timing stream endpoint at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/timing`, sending timing events over WebSocket at roughly 10 Hz with heartbeat frames when idle.
  - Switched the live timing plot in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) from fixed-interval HTTP polling to a reconnecting WebSocket client using the streamed timing channel.
  - The HTTP endpoint in [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) remains available, but the receiver UI no longer depends on it for live delivery.
  - Verification completed with `python3 -m py_compile backend/main.py backend/timing.py backend/aircraft_state.py` and `npm run build` in `frontend/`.
- `HF1`–`HF6` pending.
  - Follow-on planning initiated on 2026-04-04 after confirming that the current timing plot is streamed but still does not faithfully match the Beast-timestamp-based proposal semantics.
- `HF0` verified on 2026-04-04.
  - Chosen V1 scope is: message timing, interrogator timing, burst-rate strip chart, pipeline latency oscilloscope, and DF cadence lanes.
  - Chosen V2 deferrals are: signal-floor shimmer, source-mix pulse monitor, micro-timeline, bearing-time sweep heatmap, waterfall, polar pulse bloom, and constellation drift.
  - Backend load rule is explicit: one shared recent-message buffer and one multiplexed high-frequency stream per page/client, not one producer or socket per widget.
  - Planning load target is explicit: design for roughly `3,000 msg/s` over a `5 s` recent window with `10 Hz` stream updates and client-side rAF rendering.
- `HF1` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so the high-frequency timing buffer now records Beast-relative `arrival_us` values with sequence ids, rather than wall-clock `time.time()` values.
  - Added unwrap/rebase handling for Beast timestamps and reset behavior across stream discontinuities so the recent timing window stays monotonic.
  - Preserved original stream provenance during decode so high-frequency timing only uses primary-stream Beast timestamps; MLAT marker frames and auxiliary MLAT-side streams are excluded from this timing domain to avoid mixing incompatible clocks.
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), and [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the live timing plot consumes `now_us` and `arrival_us` from the Beast-relative stream instead of wall-clock seconds.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF2` verified on 2026-04-04.
  - Added a dedicated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and exposed it through a new top-level `Timing` tab in [App.jsx](/home/keith/claude/adsb-dashboard/frontend/src/App.jsx).
  - Moved the existing high-frequency panels off [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) and onto the dedicated page by exporting and reusing the existing `InterrogatorCodes`, `InterrogatorTimeline`, and `MessageTimingPlot` panels.
  - Receiver now contains only the lower-frequency diagnostics and summary charts; the Timing page is the staging ground for the remaining V1 high-frequency panels.
  - Verification completed with `npm run build` in `frontend/`.
- `HF3` verified on 2026-04-04.
  - Updated the timing renderer in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so messages are drawn as bars whose width is derived from `msg_len`, rather than fixed-width ticks.
  - Added simple per-lane overlap stacking with a bounded number of vertical sub-slots; overflowed overlaps collapse into the final slot instead of silently overpainting.
  - The current implementation is a pragmatic first pass at overlap handling, not the final visual design: it distinguishes short/long messages and makes dense bursts legible without changing the transport or page structure.
  - Verification completed with `npm run build` in `frontend/`.
- `HF4` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so DF11 IID timing events now carry Beast-relative arrival timing alongside the existing wall-clock data used for longer-window counts.
  - Updated [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py) so `/api/interrogators/timeline` now returns `now_us` and `arrivals_us`, aligned to the current Beast timing epoch.
  - Updated the interrogator timing canvas in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so it renders against Beast-relative timing instead of wall-clock seconds, while preserving click-through to the map.
  - Intentional split: the IID count table still uses wall-clock `last_seen`/count windows, while the timing-lane view now uses Beast-relative timing for accuracy.
  - Follow-up correction: [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) `/ws/interrogators` had still been sorting streamed lanes by live activity count. That stale backend sort was removed so the stream contract is now stable IID-ascending order end-to-end.
  - Follow-up correction: [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) request-path helpers were iterating `_iid_events` directly while the decoder thread appended to it, which could raise `RuntimeError: deque mutated during iteration`. The helpers now snapshot shared timing/IID deques under `self._lock` before iterating.
  - Verification completed with `python3 -m py_compile backend/interrogators.py backend/aircraft_state.py` and `npm run build` in `frontend/`.
- `HF4A` verified on 2026-04-04.
  - Added timestamped short-retention latency traces in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so decode, lock-wait, snapshot, serialize, and broadcast timings can be rendered as moving waveforms rather than percentiles only.
  - Added a shared aggregate high-frequency websocket at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/highfreq`. It scans the recent message window once per tick and emits burst-rate bins, DF cadence family bins, and rebinned latency traces for the page.
  - Extended [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with the `Burst-rate strip chart`, `DF cadence lanes`, and `Pipeline latency oscilloscope`, all fed from the same shared websocket instead of one producer per panel.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/interrogators.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF5` verified on 2026-04-04.
  - Added a multiplexed Timing-page websocket at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/timing-page` carrying a typed envelope with:
    - incremental timing events
    - current interrogator lane state
    - shared aggregate panel state
  - Updated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) so the dedicated Timing page now opens one page-level websocket and passes its data into the existing panels instead of opening separate per-widget sockets.
  - Updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so `MessageTimingPlot` and `InterrogatorTimeline` can consume injected stream data when hosted on the Timing page, while keeping their existing standalone websocket behavior for compatibility.
  - Compatibility choice: left `/ws/timing`, `/ws/interrogators`, and `/ws/highfreq` in place for now so the transport refactor does not become a risky big-bang removal.
  - Verification completed with `python3 -m py_compile backend/main.py backend/aircraft_state.py backend/interrogators.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF6` accepted on 2026-04-04.
  - Code-level acceptance is complete:
    - `requestAnimationFrame` rendering is in place for the two scrolling timing canvases in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
    - Hidden-tab throttling is in place for the message timing plot.
    - The multiplexed Timing-page stream in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) has explicit 1 s heartbeat behavior and bounded incremental timing payloads.
    - Build/compile verification passed for the full backend/frontend set.
  - Startup smoke check is also complete:
    - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) starts successfully under `uvicorn`.
  - Live receiver backend validation completed on 2026-04-04 by overriding `BEAST_HOST` to `192.168.0.142`:
    - [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) returned dense real traffic with varying `arrival_us` deltas and mixed `msg_len` values of `7` and `14`, confirming the timing buffer is carrying true per-message timing rather than coarse quantized buckets.
    - The incremental timing API advanced cleanly from `since_seq=14503` to much higher sequence values on the follow-up pull, confirming that the live stream model is appending and advancing rather than resetting a static window.
    - [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py) returned populated DF11 timing lanes with many active IIDs, and those lanes were emitted in ascending IID order, matching the intended stable-order contract.
    - The long-window IID activity API also returned plausible live counts and recent `last_seen` values from the same receiver session.
  - Remaining acceptance item:
    - Completed by user review: current browser-side behavior is acceptable for now.
  - Follow-up runtime correction on 2026-04-04:
    - The scrolling plots were moving smoothly but newly received points were appearing at the right edge in visible 10 Hz chunks.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now uses a small `150 ms` render holdback for the scrolling message and interrogator timing canvases so newly arrived events are revealed smoothly across frames rather than all at once when each packet lands.
  - Follow-up runtime correction on 2026-04-04:
    - The remaining jerk came from packet-stepped aggregate panels and an overly small live-edge smoothing margin.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now uses a `400 ms` live-edge holdback for the scrolling message and interrogator timing canvases to better hide packet cadence and backend jitter at the right edge.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) no longer drives `Burst-rate strip chart` and `DF cadence lanes` from server-side aggregate snapshots. They are now derived on the client from the same incremental timing-event buffer as the main message plot.
  - Follow-up runtime correction on 2026-04-04:
    - The first client-side aggregation pass removed stepping but introduced shimmer because coarse bins were being recomputed every animation frame from a moving cutoff.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now freezes burst/cadence bin counts per timing snapshot and only interpolates the horizontal offset between snapshots, which should preserve smooth scrolling without rapid shape flicker.
  - Follow-up runtime correction on 2026-04-04:
    - Freezing on packet arrival removed shimmer but reintroduced websocket-rate stepping.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now rebuilds burst/cadence bins only when the displayed time crosses an actual bin boundary, then interpolates smoothly within the bin. That should avoid both shimmer and packet-step motion.
  - Follow-up runtime correction on 2026-04-04:
    - `Burst-rate` and `DF cadence` still looked like they were filling in from the right because the visible aggregate edge was too close to the latest bin.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now applies a one-bin display lag for those aggregate panels so the newest visible bin is already complete before it reaches the edge.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now keeps a stable default DF lane set for `Message timing` and uses a larger `650 ms` live-edge holdback so rows do not appear/disappear with short-window traffic changes and the right edge is less likely to show packet-gap fill-in.
  - Follow-up runtime correction on 2026-04-04:
    - Changing `Burst-rate` or `DF cadence` controls was incorrectly perturbing `Message timing` because the shared page websocket was being recreated on each control change.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now keeps one stable `Timing` page websocket and sends control updates over the existing connection instead of reconnecting.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now deduplicates timing events by `seq` when merging incremental packets, so duplicate history cannot create spurious red overflow bars if a reconnect or resend ever occurs.
  - Acceptance disposition:
    - User confirmed on 2026-04-04 that the page is acceptable for now.
    - Remaining smoothing/display imperfections are treated as future polish, not blocking defects.
- `HFV2` investigated on 2026-04-04.
  - Reviewed the deferred `HFV2` backlog in [tasks/todo.md](/home/keith/claude/adsb-dashboard/tasks/todo.md) against the original addendum in [polish-and-features-proposal.md](/home/keith/claude/adsb-dashboard/.docs/development/polish-and-features-proposal.md).
  - Chosen next implementation slice is `HFV2A`: enrich the shared recent-message stream with `signal_raw` and `source_class`, then add the `Signal-floor shimmer` and `Source-mix pulse monitor` panels to [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
  - Captured the implementation-ready proposal, design, specs, and task breakdown in [proposal.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/proposal.md), [design.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/design.md), and [tasks.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/tasks.md).
  - Deferred `22`, `23`, `25`, `26`, and `27` remain intentionally out of the first slice until ICAO/spatial attribution and richer visual grammar are explored separately.
- `HFV2A` implemented on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so the shared recent-message timing buffer now retains `signal_raw` and a compact `source_class` alongside `arrival_us`, `df`, and `msg_len`.
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) and [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) so the widened six-field event tuples flow through the existing multiplexed Timing-page stream and the direct timing endpoint without introducing extra sockets or per-widget producers.
  - Updated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with `Signal-Floor Shimmer` and `Source-Mix Pulse Monitor`, both rendered from the existing incremental event buffer and current page timing window.
  - Scope decision: optional per-message `icao` propagation stays deferred to the later aircraft-activity work, because the signal/source slice does not need it and widening the stream again is cheaper than mixing that concern into the first implementation.
  - Scope decision: the first `Source-Mix` pass tracks accepted primary-stream timing traffic classes only. Rejected/ignored-frame accounting remains a follow-up because it does not have a clean existing low-risk hook in the current hot path.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`, `npm run build` in `frontend/`, and a synthetic `uv run --directory backend python` smoke that exercised idle and busy Timing-page payload shapes from one shared envelope.

## OpenSpec Change: `add-high-frequency-bearing-time-sweep-heatmap`

- [x] Review the current timing-event ingest path, spatial publishability rules, and Timing-page buffer contract before widening the event shape again.
- [x] Extend backend timing events so each retained message can carry an inline ingest-time spatial snapshot with `bearing_deg` when the aircraft has a publishable receiver-relative position.
- [x] Update `/api/timing/events`, `/ws/timing`, and `/ws/timing-page` plus the Timing-page client buffer to preserve the widened event shape without adding any new transports.
- [x] Add a `Bearing-time sweep heatmap` panel to [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) using the shared incremental event buffer, absolute-time bins, and a clear empty state when spatially attributable events are absent.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic/backend runtime check that proves the widened event shape stays self-contained and the heatmap can distinguish populated versus empty bearing bins.

### Review

- Plan verified on 2026-04-04 against the existing high-frequency event pipeline in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), and the current Timing-page consumer in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
- Chosen `HFV2C` cut line:
  - widen the shared recent-message event model with ingest-time `bearing_deg`
  - add only proposal item `23`, `Bearing-time sweep heatmap`
  - keep `25`, `26`, and `27` deferred until the spatial event contract is proven under live load
- Spatial attribution rule for this slice:
  - snapshot receiver-relative bearing from the aircraft state at message ingest time
  - only publish those fields when the aircraft position is already publishable under the existing high-confidence rules
  - leave the fields empty for messages without trustworthy spatial attribution instead of backfilling later from mutable live state
- Implementation result on 2026-04-04:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now records `bearing_deg` inline in the recent timing-event buffer after decode/state updates, so position-bearing snapshots reflect message-time publishable state instead of a later live lookup.
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) and [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) now serialize the widened eight-field timing tuple consistently through `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now consumes inline `bearing_deg` and renders a full-width `Bearing-Time Sweep Heatmap` from the existing shared incremental event buffer.
  - Scope decision: this first spatial slice widens the event model with `bearing_deg` only. `range_nm` stays deferred because item `23` does not need it, and adding unused spatial fields would widen the hot-path payload without improving the shipped panel.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - Focused synthetic backend check is covered by the updated timing-event tests: one assertion keeps `bearing_deg` empty when the aircraft position is not publishable, and another confirms the value is retained once the publishability gate is satisfied.
- Follow-up display refinement on 2026-04-04:
  - Increased the `Bearing-Time Sweep Heatmap` to `10°` sectors by moving from `18` to `36` angular buckets in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
  - Added a second full-width `Raw Bearing Raster` panel in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) that plots exact per-message bearings on a tall `0.5°/px` canvas.
  - Added desktop-oriented raster controls for color mode (`strength`, `message type`, `source class`) plus an optional `phosphor` fade mode that dims older points as they scroll left.

## OpenSpec Change: `add-high-frequency-message-waterfall`

- [x] Review the current Timing-page high-frequency visuals and keep the waterfall scope distinct from the existing bearing heatmap and raw bearing raster.
- [x] Implement a fixed-slice `DF-family` message waterfall in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) using the shared incremental timing-event buffer and completed absolute time slices only.
- [x] Keep the panel on the existing shared Timing-page stream without widening the backend event model or adding a new transport.
- [x] Verify the frontend build and perform a focused check that visible rows only advance on completed slice boundaries.

### Review

- Plan verified on 2026-04-05 against the existing Timing-page panels in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and the narrowed OpenSpec artifacts under [openspec/changes/add-high-frequency-message-waterfall](/home/keith/claude/adsb-dashboard/openspec/changes/add-high-frequency-message-waterfall).
- Scope decision:
  - `HFV2D` is implemented as a `DF-family` waterfall first.
  - `bearing` is intentionally excluded because the page already has both a bucketed bearing heatmap and an exact-message bearing raster.
  - The first slice remains frontend-only and reuses the existing shared timing-event buffer.
- Implementation result on 2026-04-05:
  - Added [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) as a pure helper for fixed-slice `DF-family` aggregation from the shared timing-event buffer.
  - Updated [frontend/src/pages/TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with a full-width `Message Waterfall` panel, a `50/100 ms` slice control, and a row-stable render path that only rebuilds visible rows when the displayed completed-slice window advances.
  - Kept the panel distinct from the existing charts by using stacked slice rows with `DF-family` columns rather than another bearing plot or another lane-separated cadence chart.
- Verification:
  - `npm run build` in `frontend/`
  - `node --input-type=module -e "import { buildDfWaterfallRows } from './frontend/src/utils/timingWaterfall.js'; const events=[{arrival_us:1100000,df:17},{arrival_us:1120000,df:17},{arrival_us:1180000,df:11},{arrival_us:1210000,df:18},{arrival_us:1260000,df:18},{arrival_us:1310000,df:20}]; const base=buildDfWaterfallRows({events,renderNowUs:1299999,visibleWindowUs:300000,sliceUs:50000}); const next=buildDfWaterfallRows({events,renderNowUs:1300000,visibleWindowUs:300000,sliceUs:50000}); console.log(JSON.stringify({baseNewest:base.newestVisibleSliceIndex,nextNewest:next.newestVisibleSliceIndex,baseTopCounts:base.rows[0]?.counts,nextTopCounts:next.rows[0]?.counts}, null, 2));"`
  - The helper check confirmed that the newest visible row stayed on slice `23` just before the boundary and advanced to slice `24` only once the next slice was complete, matching the completed-slice display rule.
- Follow-up correction on 2026-04-05:
  - The first waterfall pass normalized cell brightness against the current visible-window peak, which made completed boxes change brightness as rows aged down the plot.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now uses a fixed log-scaled count-to-alpha transfer curve based on per-family event rate rather than a moving window max, so completed rows should keep stable intensity.
- Follow-up correction on 2026-04-05:
  - The `DF-family` version was still too coarse and the alternating row backgrounds were creating artificial periodic bands that read like real traffic structure.
  - [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) now buckets by concrete raw `DF` values (`17`, `18`, `11`, `20`, `21`, `4`, `5`, `16`, `0`, `other`) instead of broad families.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now removes the zebra row striping and adds a `20 ms` slice option so the waterfall carries more genuine texture from the data itself.

## OpenSpec Change: `fix-signal-dbfs-semantics`

- [x] Review the current signal path across Beast ingest, readsb ingest, timing-event serialization, snapshot/history APIs, and frontend consumers.
- [x] Define one canonical display contract for signal as readsb-style `dBFS`, while keeping any raw byte handling backend-internal only where it is still needed for storage or ingest.
- [x] Update backend public payloads and shared timing-event serialization so frontend-facing signal values are emitted in canonical `dBFS` semantics across Beast and readsb paths.
- [x] Update frontend signal-based displays, labels, and legends to consume canonical `dBFS` values instead of raw-byte assumptions.
- [x] Verify the backend/frontend changes with focused tests, `npm run build`, and a distribution sanity check against the documented readsb/graphs1090 expectations.

### Review

- Plan verified on 2026-04-05 against the current signal flow in [backend/aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), [backend/main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [backend/timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), [backend/db.py](/home/keith/claude/adsb-dashboard/backend/db.py), [frontend/src/pages/ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx), [frontend/src/pages/TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx), [frontend/src/components/AircraftTable.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftTable.jsx), [frontend/src/components/AircraftDetailPanel.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftDetailPanel.jsx), and [frontend/src/pages/SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx).
- Contract decision for this change:
  - public/frontend-facing signal values move to canonical `dBFS` semantics
  - raw Beast/readsb byte equivalents stay backend-internal where persistence or ingest still needs them
  - timing events should stop exposing `signal_raw` and instead expose display-grade `signal_dbfs`
- Compatibility constraint:
  - `coverage_samples` and minute aggregates can keep storing raw/internal values for now because existing history queries already convert those values server-side, but that raw form should no longer leak through public live APIs unless a dedicated debug need appears
- Implementation result on 2026-04-05:
  - Added [signal_utils.py](/home/keith/claude/adsb-dashboard/backend/signal_utils.py) so raw Beast bytes and canonical `dBFS` conversions are defined once and reused consistently.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so live aircraft snapshots now expose canonical `signal` in `dBFS`, retain explicit `signal_raw` only for internal/raw consumers, and store high-frequency timing events with `signal_dbfs` rather than raw bytes.
  - Updated [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [db.py](/home/keith/claude/adsb-dashboard/backend/db.py), and [history.py](/home/keith/claude/adsb-dashboard/backend/history.py) so live timing payloads, receiver scatter data, and SkyView history points all expose canonical display-grade signal semantics, while minute/coverage persistence keeps using raw values internally.
  - Added [frontend/src/utils/signal.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/signal.js) and updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx), [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx), [AircraftTable.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftTable.jsx), [AircraftDetailPanel.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftDetailPanel.jsx), and [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) so existing displays use one shared `dBFS` formatter/colour contract instead of local raw-byte assumptions.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/db.py backend/history.py backend/main.py backend/signal_utils.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py tests/test_db.py`
  - `npm run build` in `frontend/`
  - Added backend assertions in [test_timing_events.py](/home/keith/claude/adsb-dashboard/backend/tests/test_timing_events.py) and [test_db.py](/home/keith/claude/adsb-dashboard/backend/tests/test_db.py) covering canonical `dBFS` event/API shapes and representative raw-to-`dBFS` conversions.
- Distribution sanity check on 2026-04-05:
  - Queried the last 24 hours of persisted `minute_stats` in [adsb.db](/home/keith/claude/adsb-dashboard/backend/data/adsb.db) using the same raw-to-`dBFS` conversion now used by the live APIs.
  - Observed minute-average distribution: peak `-6.1 dBFS`, floor `-31.1 dBFS`, mean `-21.6 dBFS`, interquartile range `-24.3` to `-20.1 dBFS`.
  - Interpretation:
    - the mean and spread are now in the expected readsb-style negative-`dBFS` space rather than the old raw-byte/public-contract split
    - persisted minute averages are narrower than the user’s per-message readsb/graphs1090 reference distribution, so they are only a sanity check, not a perfect acceptance proxy for live per-message extremes

## Live Build Fix: `timingWaterfall` import

- [x] Reproduce the failing module path from [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and inspect the referenced utility path in [frontend/src/utils](/home/keith/claude/adsb-dashboard/frontend/src/utils).
- [x] Confirm whether the failure is caused by a bad import path or by the referenced file being absent from the tracked source tree used on the live Pi.
- [x] Make the minimal source change needed so the `Message Waterfall` helper is present in the repo/worktree used for deployment.
- [x] Verify with `npm run build` in `frontend/` and record the result.

## Cast Zeroconf Log Fix

- [x] Inspect the current Chromecast cache/discovery lifecycle and confirm whether the repeated zeroconf assertion is still possible.
- [x] Implement a clean cached-Chromecast shutdown path that disconnects the client before stopping discovery or dropping the cached browser.
- [x] Route every relevant cache-reset / rediscovery path through the same shutdown helper.
- [x] Add focused backend tests covering shutdown ordering and stale-cache cleanup semantics.
- [x] Verify the change with targeted backend tests and a final code review pass.

### Live Build Fix Review

- Plan verified on 2026-04-05 against [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx), [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js), and the current git worktree state.
- Root cause:
  - The import path in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) is correct.
  - The referenced module file existed locally at [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) but was untracked, so it would be missing from any environment updated only from tracked repository contents, including the live Pi build tree.
- Implementation result on 2026-04-05:
  - Kept the existing import contract and ensured [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) is the explicit home for the waterfall aggregation logic rather than duplicating that logic inline inside [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
  - Added a short file-level comment to [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) so the helper has an intentional tracked edit associated with this build-fix pass.
- Verification:
  - `npm run build` in `frontend/`
  - Result: Vite production build completed successfully on 2026-04-05, including the generated `TimingPage-*.js` bundle, so the `../utils/timingWaterfall` import now resolves correctly in the build tree.
- Follow-up correction on 2026-04-05:
  - The first implementation used the wrong Beast conversion model (`-(raw/2)`) and therefore produced live aircraft values that were far too strong compared with readsb.
  - Updated [signal_utils.py](/home/keith/claude/adsb-dashboard/backend/signal_utils.py) to use readsb’s actual Beast amplitude pipeline: normalize amplitude, square to power, then convert with `10 * log10(...)`.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so Beast-fed aircraft signals now use readsb-style rolling 8-sample power averaging for the live aircraft value instead of exposing the last raw byte through an incorrect direct mapping.
  - Updated [decode_api.c](/home/keith/claude/adsb-dashboard/backend/native/decode_api.c) and rebuilt `backend/native/libdecode.so` with `make` so the native decoder path uses the same Beast signal normalization as the Python path.
  - Added focused formula/inverse tests in [test_signal_utils.py](/home/keith/claude/adsb-dashboard/backend/tests/test_signal_utils.py) and reran the backend signal test set successfully.

### Cast Zeroconf Log Fix Review

- Plan verified on 2026-04-06 against [cast.py](/home/keith/claude/adsb-dashboard/backend/cast.py), [cast_api.py](/home/keith/claude/adsb-dashboard/backend/cast_api.py), and [test_cast.py](/home/keith/claude/adsb-dashboard/backend/tests/test_cast.py).
- Root cause:
  - the cached Chromecast path intentionally kept the discovery browser alive so `pychromecast` could do mDNS-based reconnects
  - later cache-reset and rediscovery paths stopped that browser without first disconnecting the cached Chromecast client
  - that left the client worker thread able to hit zeroconf after its event loop had already been stopped, which matches the repeated `AssertionError: Zeroconf instance loop must be running`
- Implementation result on 2026-04-06:
  - added `_clear_cached_chromecast()` in [cast.py](/home/keith/claude/adsb-dashboard/backend/cast.py) to enforce one shutdown order: disconnect cached client first, then stop discovery, then clear the cache
  - routed `reset_config_cache()`, stale-cache rediscovery in `_get_chromecast()`, and the stale-handle cleanup path after `quit_app()` failure through that same helper
  - added focused lifecycle tests in [test_cast.py](/home/keith/claude/adsb-dashboard/backend/tests/test_cast.py) asserting the disconnect-before-stop ordering and the rediscovery cleanup sequence
- Verification:
  - `uv run --directory backend pytest tests/test_cast.py`
  - Result: `5 passed` on 2026-04-06

## Radar FM: Iterative Second-Pass Frame Repair

- [x] Factor the live forward-model direction-selection logic into a reusable helper so the same solve policy can be applied before and after frame repair.
- [x] After the first intersection seed, rerun frame reference preparation against the seeded radar location and only adopt the repaired frame set when the follow-up intersection result is stronger.
- [x] Add focused regression coverage for the second-pass repair path and verify the sweep/forward-model test set still passes.

### Review

- Plan verified on 2026-04-08 against [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) and [test_forward_model.py](/home/keith/claude/adsb-dashboard/backend/tests/test_forward_model.py).
- Root cause:
  - The first-pass frame repair uses a provisional anchor location, so it can improve bad frames without fully matching the anchor ranking that becomes optimal once the intersection solver lands on a stronger candidate location.
  - Live results for `IID 7` showed repaired frames still had better alternative references when replayed at the chosen seed, which meant the initial repair pass was helpful but incomplete.
- Implementation result on 2026-04-08:
  - Added `_run_best_intersection_attempt()` in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so clockwise-preferred direction selection is shared between the initial solve and any rerun after frame repair.
  - Updated `run_full_pipeline()` in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) to perform a second frame-preparation pass using the first intersection seed as the anchor, rerun the intersection solver on the repaired frame set, and adopt that rerun only when support/quality metrics improve.
  - Extended the result diagnostics in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) with nested `frame_preparation.second_pass` details and a `second_pass_repair_applied` flag so live runs show whether the iterative repair path actually changed the solve.
  - Added a focused regression in [test_forward_model.py](/home/keith/claude/adsb-dashboard/backend/tests/test_forward_model.py) proving the rerun is adopted only when the second-pass repair yields the stronger intersection outcome.
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - Result: `40 passed in 0.10s` on 2026-04-08
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py`
  - Result: `62 passed in 0.06s` on 2026-04-08
  - `python3 -m py_compile backend/radar/forward_model.py`

## Radar FM: Marginal Frame Preparation

- [x] Replace the binary frame-reference keep/drop rule with a good/marginal/drop policy so borderline repaired frames can still contribute without dominating the solve.
- [x] Downweight marginal frames during forward-model preprocessing so retained borderline frames help support counts and coverage but carry less influence in clustering and replay.
- [x] Add focused regression coverage for marginal-frame downgrade behavior and rerun the sweep/forward-model regression set.

### Review

- Plan verified on 2026-04-08 against [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py), [models.py](/home/keith/claude/adsb-dashboard/backend/radar/models.py), and [test_forward_model.py](/home/keith/claude/adsb-dashboard/backend/tests/test_forward_model.py).
- Root cause:
  - After iterative frame repair, live runs were no longer publishing bad locations, but the frame-reference sigma gate was still binary: frames above the cutoff were dropped outright.
  - That left both `IID 7` and `IID 21` starved on later `pair_count`, `high_quality_frames`, and `cluster_member_count` gates even when some borderline frames were likely still usable.
- Implementation result on 2026-04-08:
  - Split the frame-reference sigma policy in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) into:
    - `good` when replay sigma is at or below `70°`
    - `marginal` when replay sigma is above `70°` but at or below `105°`
    - `dropped` only above `105°`
  - Added `_set_sweep_frame_quality()` in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so repaired or downgraded frames keep the same geometry but explicitly carry the adjusted quality state into later pipeline stages.
  - Updated `_prepare_frames_for_solving()` in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) to track `frames_good`, `frames_marginal`, and `frames_downgraded_to_marginal`, and to emit `downgraded` entries in `frame_reference_repairs` when a borderline frame is kept with reduced confidence.
  - Updated `_preprocess_scoring_frames()` in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) so marginal frames are still used but their observation weights are scaled down before intersection selection and replay scoring.
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - Result: `42 passed in 0.10s` on 2026-04-08
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py`
  - Result: `64 passed in 0.11s` on 2026-04-08
  - `python3 -m py_compile backend/radar/forward_model.py`

## Radar FM: Strong CCW Evidence Requirement

- [x] Tighten live direction selection so standalone `CCW` solutions are rejected unless they clear a much stronger support threshold than the normal intersection gates.
- [x] Keep `CW` as the default whenever both directions succeed.
- [x] Add a focused regression for weak standalone `CCW` rejection and rerun the sweep/forward-model regression set.

### Review

- Plan verified on 2026-04-08 against [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py), [test_forward_model.py](/home/keith/claude/adsb-dashboard/backend/tests/test_forward_model.py), and the latest live-validation findings for `IID 7`.
- Root cause:
  - The existing policy only preferred `CW` when both directions solved; a standalone `CCW` solution could still proceed as long as it passed the general quality gates.
  - Live results still showed occasional `CCW` candidates for `IID 7`, which conflicts with the operating assumption that almost all civilian radar sweeps should be clockwise unless there is unusually strong contrary evidence.
- Implementation result on 2026-04-08:
  - Added standalone `CCW` evidence gates in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py) requiring stronger pair count, high-quality-frame count, cluster-member count, and cluster dominance before a `CCW` solve is even treated as a valid live candidate.
  - Kept the existing “if both directions succeed, choose `CW`” behavior in [forward_model.py](/home/keith/claude/adsb-dashboard/backend/radar/forward_model.py).
  - Added [test_forward_model.py](/home/keith/claude/adsb-dashboard/backend/tests/test_forward_model.py) coverage proving a marginal standalone `CCW` solve is rejected at the intersection stage with explicit `ccw_gate_failures`.
- Verification:
  - `uv run --directory backend pytest tests/test_forward_model.py`
  - Result: `43 passed in 0.09s` on 2026-04-08
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_forward_model.py`
  - Result: `65 passed in 0.12s` on 2026-04-08
  - `python3 -m py_compile backend/radar/forward_model.py`

## Live Page: Hybrid Message Counter Fix

- [x] Trace the live-page `Messages / sec` and `Total Messages` cards back to the backend snapshot fields and confirm which ingest path owns those counters in hybrid mode.
- [x] Change hybrid-mode counting so the live page uses the Beast message stream for totals/rates instead of `aircraft.json["messages"]`.
- [x] Add focused backend tests covering the hybrid count path and the unchanged readsb-only delta path.

### Review

- Plan verified on 2026-04-08 against [StatsBar.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/StatsBar.jsx), [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), and [readsb_ingest.py](/home/keith/claude/adsb-dashboard/backend/readsb_ingest.py).
- Root cause:
  - The live page reads `snapshot.msg_per_sec` and `snapshot.total_messages` from [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py).
  - In `INGEST_MODE=hybrid`, the code was intentionally suppressing Beast-driven `_total` and rate updates and instead deriving non-MLAT totals from `aircraft.json["messages"]` in [readsb_ingest.py](/home/keith/claude/adsb-dashboard/backend/readsb_ingest.py).
  - On this deployment, that `aircraft.json` aggregate is materially lower than the real Beast message stream, so the live page underreported overall traffic even though messages were still being decoded.
- Implementation result on 2026-04-08:
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so Beast/MLAT ingest owns `_total` and live rate counters in both `beast` and `hybrid` modes.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so `update_from_json()` still tracks the readsb baseline in hybrid mode but no longer adds readsb deltas into `_total` or `_cur_sec_count`, preventing undercounting and avoiding double-counting.
  - Added [test_aircraft_state_counts.py](/home/keith/claude/adsb-dashboard/backend/tests/test_aircraft_state_counts.py) to prove:
    - hybrid mode totals follow Beast message ingestion and ignore `aircraft.json` totals
    - readsb-only mode still derives totals from `aircraft.json` deltas as before
- Verification:
  - `uv run --directory backend pytest tests/test_aircraft_state_counts.py`
  - Result: `2 passed in 0.36s` on 2026-04-08
  - `python3 -m py_compile backend/aircraft_state.py backend/tests/test_aircraft_state_counts.py`

## Radar Sync Visualisation Follow-up

- [x] Inspect and patch backend burst-sync timeline exposure so visualization includes all relevant burst-centre observations without changing sync-maintenance acceptance rules.
- [x] Update Burst Sync Alignment residual mode to plot live DF11 arrivals as signed residual dots with early/on-time/late color semantics.
- [x] Restore and wire a selectable legacy live DF alignment mode in Burst Sync Alignment.
- [x] Restore Position Verification timing colours (blue early, green on time, red late) based on beam-vs-arrival residual timing.
- [x] Run focused verification (`pytest` target + frontend build) and capture review notes.

### Review

- Plan verified on 2026-04-16 against `frontend/src/pages/RadarPage.jsx`, `frontend/src/hooks/useTimingEventStream.js`, `frontend/src/hooks/useTimingEventBuffer.js`, `backend/radar/sweep.py`, and `backend/radar/api.py`.
- Root cause:
  - Burst Sync Alignment was rendering raw DF11 only as neutral vertical ticks at zero residual, so the operator could not tell early/on-time/late behavior.
  - Position Verification had been switched to a neutral raw-point color and had lost explicit timing class semantics.
  - The burst-sync timeline endpoint only exposed the sync-driving burst subset (`_live_aligned_burst_obs`), which can underrepresent real burst-centre comparisons available for plotting.
  - The broad per-aircraft live alignment visualization had been replaced, removing useful operational context.
- Implementation result on 2026-04-16:
  - Added a broader burst visualisation buffer in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) (`_live_burst_timeline_obs`) and wired timeline recording for all burst-centre observations with usable geometry, while preserving sync maintenance on dominant-family observations only.
  - Extended burst-sync timeline payload entries with `sync_update_eligible` and switched timeline reads to the broader visualisation buffer in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py).
  - Updated endpoint docs in [api.py](/home/keith/claude/adsb-dashboard/backend/radar/api.py) to reflect broader observation exposure.
  - Added regression coverage in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) proving non-sync-driving observations are included in burst sync timeline responses.
  - Reworked [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx):
    - Burst Sync Alignment residual mode now renders live DF11 as residual dots (blue early, green on time, red late) on the residual axis.
    - Burst-centre observations remain primary and now distinguish sync-driving vs non-sync-driving points.
    - Added a mode selector and restored legacy per-aircraft live DF alignment view using `/api/radar/iids/{iid}/timeline`.
    - Restored Position Verification timing colours (blue/green/red) via beam-vs-arrival residual classification.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py`
  - Result: `41 passed in 0.10s` on 2026-04-16.
  - `npm run build` in `frontend/`
  - Result: Vite production build succeeded on 2026-04-16.

## 2026-04-16 — Radar hot-path performance rectification

Goal: eliminate message drops and radar queue saturation caused by expensive FM/sweep work running inline on the radar worker thread.

- [x] A+B. Per-IID FM mailbox in `RadarState`
  - Replaced inline `per_frame_solve_callback(iid, frame, period_s)` in `_finalize_live_frame()` with a non-blocking enqueue into `RadarState._fm_mailbox: dict[int, (frame, period_s)]`.
  - "Latest wins": newer frames overwrite unsolved older frames for the same IID, bounding backlog.
  - New API: `claim_pending_fm_frames()` / `wait_for_pending_fm_frames(timeout)`.
- [x] B. Dedicated FM worker thread in `main.py`
  - `_start_fm_worker()` drains the mailbox, at most one solve in flight per IID.
  - Wake via `threading.Event`; clean shutdown via `_fm_worker_stop`.
- [x] C. Pragmatic cap on `update_rotation_models()`
  - Per-IID tail-slice: `_ROTATION_ANALYSIS_MAX_EVENTS_PER_IID = 4000`.
  - Per-IID event-delta gate: `_ROTATION_ANALYSIS_MIN_DELTA = 40` skips reanalysis if established model has not accumulated meaningful new evidence.
  - Conservative — does not over-defer CHECK_MULTI; newly-seen IIDs always run.
- [x] D. Throttle `_update_multi_aircraft_sync_state` per IID
  - `_MULTI_SYNC_UPDATE_MIN_INTERVAL_S = 0.25`; observations keep accumulating in `_live_aligned_burst_obs` so throttled updates see the full window.
- [x] E. Frontend polling gate
  - `RadarPage.jsx`: legacy-timeline poll only runs when `alignmentMode === BURST_SYNC_VIEW_MODE_LEGACY`.
- [x] F. Cache `iid_fm_diagnostics` (3s TTL)
  - Avoids recomputing `build_sweep_frames` + `get_sweep_history` on every poll.

### Tests
- New regression tests in `test_radar_sweep.py`:
  - `test_finalize_live_frame_enqueues_fm_mailbox_instead_of_inline_callback`
  - `test_fm_mailbox_latest_frame_wins_for_same_iid`
  - `test_fm_mailbox_preserves_distinct_iids`
- Full suite: `291 passed in 1.49s`.

### Perf validation checklist (manual, against `/api/debug/perf`)
- [ ] `radar_fired_burst_phase_ms.fm_callback_avg` drops to near zero (was == `finalize_avg`).
- [ ] `radar_fired_burst_phase_ms.finalize_avg` drops substantially.
- [ ] `radar_worker_phase_ms.process_burst_avg` drops.
- [ ] `radar_rotation_ms.sweep_build_avg` reduced; stable across cycles with hot IIDs.
- [ ] `_radar_queue` depth falls, `_radar_drops` counter stable.
- [ ] `_msg_queue` depth falls, `_msg_drops` counter stable.
- [ ] FM solves still run (check `fm_last_run` timestamps under `/api/radar/iids/{iid}/fm-diagnostics`).
- [ ] Burst-sync residual UI still responsive (250 ms throttle imperceptible).
- [ ] Legacy timeline still renders when selected.

## 2026-04-17 Radar Sync Model Convergence Fix

- [x] Audit current live sync predictor, period-refinement slope basis, and localiser live-bearing path
- [x] Replace tuple predictor with one authoritative backend prediction helper exposing effective time, phase, bearing, waveform, propagation, and residual basis
- [x] Correct period refinement to fit residual slope against effective Beast time and apply a sign/gain update with explicit diagnostics
- [x] Add stricter fit-driving gates and reject-reason diagnostics separate from display classification
- [x] Route `aircraft_localiser.py` live bearing observations through the authoritative predictor
- [x] Expose convergence/update history, fit support, predictor consistency, and waveform-learning diagnostics to the burst-sync API/UI
- [x] Add focused backend regression tests for predictor consistency, slope correction, gating, and payload diagnostics
- [x] Run focused backend tests and frontend build verification

Plan confirmation: proceeding without further confirmation per task request. Scope is limited to radar sync-model correctness/diagnostics and shared prediction paths.

### Review

- Implementation:
  - Added `SyncPrediction` / `predict_sync_observation()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py) as the authoritative backend predictor for effective time, phase, raw/prop/waveform-corrected bearing, propagation delay, waveform correction, and predictor path tagging.
  - Changed multi-aircraft period refinement to fit corrected residuals against propagation-corrected Beast time, not wall-clock observation time, and documented the sign convention: positive residual slope decreases period because the model beam is lagging.
  - Increased period-refinement gain to a conservative but functional `0.25`, retained per-update/base ppm clamps, and exposed update term/direction/applied/gain/block reason diagnostics.
  - Added fit-driving gates separate from display classification: non-sync-driving observations, near-wrap residuals, large residuals, stale positions, poor ICAO quality, and zero-weight observations are marked with reject reasons and excluded from period fitting.
  - Routed `aircraft_localiser.py` live detection bearings through `predict_sync_observation()` and added a `RadarState.get_live_waveform_bins()` accessor so live localisation uses the same propagation/waveform predictor basis as diagnostics.
  - Added period/slope/update histories, fit support/reject summaries, predictor consistency flags, waveform-learning state, and raw/after-prop/after-waveform residual fields to the burst-sync timeline payload.
  - Updated `RadarPage.jsx` refinement diagnostics with a compact health block showing slope trend, update direction/gain/block reason, fit support/span, reject reasons, waveform-learning block state, recent updates, and predictor consistency.
  - Added backend regression tests for predictor prop/waveform application, period-refinement sign/slope basis, timeline gating diagnostics, and localiser predictor unification.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/aircraft_localiser.py backend/tests/test_radar_sweep.py backend/tests/test_aircraft_localiser_sync_predictor.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_aircraft_localiser_sync_predictor.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: focused tests `49 passed`; full backend `296 passed`; frontend build succeeded with the existing large-chunk warning.

## 2026-04-17 Radar Phase Anchor Rework

- [x] Inspect live sync state, burst-sync timeline/debug payloads, localiser predictor use, and Radar page diagnostics
- [x] Add explicit per-ICAO phase-anchor candidate scoring and replacement hysteresis
- [x] Solve absolute phase offset primarily from the selected anchor aircraft using Beast-relative effective timestamps and circular statistics
- [x] Keep period refinement multi-aircraft while limiting non-anchor aircraft to validation and small nudges
- [x] Expose summary, observation, and candidate-level phase-anchor diagnostics in backend payloads
- [x] Add Radar page anchor status, candidate, implied-offset, and validation diagnostics
- [x] Add/update focused backend tests for anchor selection, anchor-led offset solve, and validation/nudge behavior
- [x] Run backend/frontend verification and record results

Plan confirmation: proceeding without further confirmation per task request. Period solving remains the multi-aircraft long-baseline slope loop; absolute phase anchoring becomes a shorter-baseline selected-aircraft solve, with the rest of the population used only for validation, small correction, and anchor replacement diagnostics.

### Review

- Implementation:
  - Added circular mean/MAD helpers and explicit phase-anchor fields to `LiveSyncState` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py).
  - Added `_select_phase_anchor_aircraft()`, `_solve_phase_anchor_from_icao()`, and `_validate_phase_anchor_against_population()` in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py).
  - Changed `_update_multi_aircraft_sync_state()` so period slope fitting remains multi-aircraft, while absolute `phase_offset_deg` is derived from the selected anchor aircraft and only receives a capped validation nudge from non-anchor aircraft. If no suitable anchor exists, the previous mixed correction remains as fallback.
  - Decoupled anchor scoring from the old residual gate so a wrong absolute branch does not cause a coherent anchor aircraft to be rejected as a residual outlier.
  - Extended burst-sync timeline and sync-debug observations with `implied_phase_offset_deg`, `anchor_relative_phase_error_deg`, `phase_anchor_contributor`, and `phase_anchor_reject_reason`, and exposed ranked anchor candidates.
  - Added `PhaseAnchorPanel` to [RadarPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/RadarPage.jsx) with current anchor status, candidate table, implied-offset scatter, per-aircraft offset rows, and validation summary.
  - Added focused regression tests in [test_radar_sweep.py](/home/keith/claude/adsb-dashboard/backend/tests/test_radar_sweep.py) for wrong-branch recovery and capped population nudging.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/radar/api.py backend/radar/aircraft_localiser.py`
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_phase_anchor_selected_aircraft_recovers_wrong_absolute_branch tests/test_radar_sweep.py::test_phase_anchor_uses_population_only_as_small_validation_nudge tests/test_radar_sweep.py::test_period_refinement_uses_effective_time_slope_and_correct_sign`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: focused anchor/period tests passed, radar sweep/API tests passed (`87 passed`), full backend passed (`302 passed`), frontend build succeeded with the existing chunk-size warning.

## 2026-04-17 Radar Phase Anchor Rejection Fix

- [x] Re-check phase-anchor hard rejection gates after live report that suitable aircraft are rejected
- [x] Convert branch-contaminated quality signals from hard reject gates into score penalties
- [x] Add a regression for coherent candidates with poor old residual-quality memory
- [x] Run focused backend verification and frontend build if UI changes are needed

Plan confirmation: fix the anchor selection gate only. Do not change the period solver or UI unless diagnostics need field compatibility updates.

### Review

- Implementation:
  - Relaxed phase-anchor hard rejection in [sweep.py](/home/keith/claude/adsb-dashboard/backend/radar/sweep.py): poor per-ICAO residual quality memory, moderate phase spread, aging observations, and moderate position age are now warning/score penalties instead of hard rejects.
  - Kept hard rejects for structural failures: too few observations, pathological phase spread, fully stale positions, or fully stale observations.
  - Increased the anchor solve position-age allowance to match the selector, so candidates selected with acceptable but not ultra-fresh positions are not immediately discarded.
  - Updated the Radar page candidate table to show warning reasons for candidate rows rather than hiding why the score was reduced.
  - Added `test_phase_anchor_quality_memory_is_warning_not_hard_reject` to prevent old wrong-branch residual memory from blocking a coherent anchor candidate.
- Verification:
  - `python3 -m py_compile backend/radar/sweep.py backend/tests/test_radar_sweep.py`
  - `uv run --directory backend pytest tests/test_radar_sweep.py::test_phase_anchor_selected_aircraft_recovers_wrong_absolute_branch tests/test_radar_sweep.py::test_phase_anchor_quality_memory_is_warning_not_hard_reject tests/test_radar_sweep.py::test_phase_anchor_uses_population_only_as_small_validation_nudge`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py`
  - `uv run --directory backend pytest`
  - `cd frontend && npm run build`
  - Result: focused anchor tests passed, radar sweep/API tests passed (`88 passed`), full backend passed (`303 passed`), frontend build succeeded with the existing chunk-size warning.

## radar-core implementation

Source: `.docs/development/radar-core-design-brief.md`
Started: 2026-04-19

### Stage 0 — Protocol definition
- [x] Install Go 1.24.2 to ~/.local/go
- [x] Scaffold radar-core Go module (`radar-core/`)
- [x] `protocol/framing.go` — 4-byte length-prefix read/write, ErrFrameTooLarge
- [x] `protocol/messages.go` — all 10 message types with codec struct tags
- [x] `protocol/codec.go` — Encode/Decode via ugorji/go/codec (msgpack)
- [x] `protocol/protocol_test.go` — 14 Go tests, all pass
- [x] `backend/radar_core/protocol.py` — Python framing + encode/decode + constructors
- [x] `backend/tests/test_radar_core_protocol.py` — 26 Python tests, all pass
- [x] Stage 1 — Radar event tap (ingest, burst builder, shadow mode client)
- [x] Stage 2 — Rotation model parity
- [x] Stage 3 — Sync model parity
- [x] Stage 4 — Frame accumulation and mailbox
- [x] Stage 5 — Python consumption switch
- [x] Stage 6 — Remove Python operational path
