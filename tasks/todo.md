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
