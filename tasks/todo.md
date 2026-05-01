## 2026-05-01 Burst Sync Residuals Recorder Semantics and Authority Consistency

- [x] Review current backend/frontend residual chart pipeline and identify where recorded events, recomputed projections, and authority/source fields are assembled.
- [x] Run best-effort impact scan for touched symbols/files and document blast radius before edits.
- [x] Update backend recorded residual event snapshots to store immutable scalar render/model metadata at event creation time, with operational vs diagnostic source separation and consistency checks.
- [x] Remove silent recomputed fallback from recorded mode and expose explicit recorded empty/unavailable state plus explicit recomputed projection modes/bases.
- [x] Update frontend chart semantics, controls, and labels to distinguish immutable recorded history from current-state projections and to consume canonical operational fields only.
- [x] Add backend tests for immutable recorded snapshots, authority/source consistency, and explicit projection metadata.
- [x] Add frontend/static tests for mode wording, basis labeling, and operational/diagnostic field separation.
- [x] Run focused verification (backend pytest, frontend tests/build) and add a review section with outcomes and remaining risks.

Plan confirmation:
- Scope limited to chart/data semantics and diagnostic consistency.
- No changes to period refinement math, Go refiner logic, phase math, readiness gates, or localisation logic.

### Review
- Backend:
  - Recorded burst residual events now snapshot scalar render/model metadata at creation time, including authority/source/phase/handoff fields and immutable display class labels.
  - Added recorded DF11 residual snapshots so recorded mode no longer depends on current-state DF11 recomputation.
  - Live snapshot/timeline payloads no longer silently substitute recomputed observations into recorded mode.
  - Added explicit recomputed projection datasets keyed by basis (`runtime_effective`, `compact_bootstrap`, `go_runtime_diagnostic`) while keeping recorded history separate.
  - Operational sync payload fields are now authority-driven; Go-only period values are exported under `go_diagnostic_*` fields instead of masquerading as operational sources.
  - Added consistency warnings/demotion for contradictory operational source combinations.
- Frontend:
  - Recorded mode is labelled `Recorded immutable event history` and shows an explicit unavailable/insufficient-recorded-data state instead of fallback wording.
  - Recomputed mode is labelled `Recomputed current projection` and consumes explicit basis options from the payload.
  - Burst/DF11 chart data now switches between recorded and recomputed datasets without reprojecting recorded history.
  - Diagnostic Go delta/source display now reads `go_diagnostic_*` fields only.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_ui_labels.py -q` -> `155 passed`.
  - `cd frontend && npm run build` -> passed.
- Remaining risks:
  - Manual live validation against a running feed is still needed to confirm the visual step-change behavior when refinement state changes mid-stream.
  - Recomputed `compact_bootstrap` / `go_runtime_diagnostic` views reuse the existing predictor with alternate period/phase snapshots; labels are explicit, but live operator validation should confirm those views match the intended diagnostic interpretation.
