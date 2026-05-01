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
