## 2026-04-30 Stage 1 Radar Sync UI/Terminology Truthfulness

- [x] Review current Radar page/API sync payload fields and identify misleading operational vs diagnostic labels.
- [x] Run best-effort impact scan for touched symbols/files (no direct `gitnexus_impact` MCP tool available in this session).
- [x] Update Radar UI operational period triple to render one authority-consistent `Base | Δ | Effective` set only.
- [x] Add explicit authority badges/labels (`sync_authority`, `period_authority`, `effective_period_source`, `period_delta_source`) with state wording (bootstrap/refined/holdover/unavailable).
- [x] Move non-operational period refiner values into a clearly labeled diagnostic/shadow section with non-operational disclaimer.
- [x] Rename phase/status wording to anchor-relative semantics, add help text, and show `phase_basis`, `phase_is_absolute`, anchor ICAO/age/status.
- [x] Add display-safe phase status alias/mapping (preserve existing enum compatibility).
- [x] Update adjacent backend docstrings/comments for exposed sync/phase semantics (no behavior changes).
- [x] Add/update frontend and backend tests for period-triple authority consistency, diagnostic separation, and anchor-relative phase labeling.
- [x] Run focused verification (backend tests + frontend build) and perform manual Stage 0 payload sanity checks.

Plan confirmation:
- Scope limited to Stage 1 label/display/serialization clarity only.
- No sync/refinement/phase/frame/readiness/authority runtime behavior changes.
- API compatibility preserved by additive/alias fields and frontend mapping.

### Review
- Frontend:
  - Reworked sync summary to show an operational period triple only (`Base | Δ | Effective`) sourced from one authority path.
  - Added authority badges: `sync_authority`, `period_authority`, `effective_period_source`, `period_delta_source` and an operator-facing authority summary label.
  - Added `Diagnostic / shadow refiner` section for non-operational values; diagnostic Go delta is shown only there when it is non-operational.
  - Replaced phase wording with anchor-relative semantics and help text; added explicit `phase_basis`, `phase_is_absolute`, anchor ICAO/age, and anchor-relative trust label.
  - Added visible diagnostics fields in the radar diagnostics row for authority/source/basis/anchor/convergence context.
- Backend:
  - Added additive `sync_state.phase_status_display` alias mapping (`anchor_trusted`, `anchor_provisional`, `anchor_untrusted`, `unavailable`) while preserving legacy `phase_status`.
  - Updated adjacent sync serialization/anchor docstrings to state anchor-relative semantics and that geographic/absolute beam direction is unavailable.
- Tests:
  - Updated `backend/tests/test_radar_api.py` to assert additive alias behavior and legacy compatibility.
  - Added `backend/tests/test_radar_ui_labels.py` static wording checks for anchor-relative phase text.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_ui_labels.py -q` -> `50 passed`.
  - `cd frontend && npm run build` -> passed.
  - Manual Stage 0 payload sanity check performed against `tasks/radar_sync_baseline/sync_capture_iid7_smoke_20260430T191100Z.ndjson` and fail-case snapshot; required Stage 0 authority/source fields are present and consumed by UI logic.

## 2026-04-30 Stage 2 Canonical Operational Period Triple

- [x] Run impact scan for symbols/files touched by canonical period fields and sync snapshot serialization.
- [x] Add canonical period fields to sync state/API payload assembly (`base_period_s`, `period_delta_s`, `effective_period_s`, `period_authority`, `period_refinement_status`, `sync_authority`) without removing legacy fields.
- [x] Populate canonical fields conservatively from current operational authority (Python operational first; Go runtime only when actually operational; holdover/unavailable conservative labeling).
- [x] Enforce arithmetic invariant in snapshot builder: when all canonical numeric values exist, ensure `effective_period_s == base_period_s + period_delta_s` within tolerance; recompute safe delta or mark unavailable/diagnostic and warn.
- [x] Update Radar page operational period display to consume canonical fields only and show conservative unavailable/bootstrapping states without fallback to legacy/Go diagnostic delta.
- [x] Keep Stage 1 diagnostic/shadow period section separate and non-operational; ensure Go diagnostic delta does not drive operational triple while Python is operational.
- [x] Add/update backend tests for canonical field presence, conservative missing/holdover handling, invariant enforcement, and source separation.
- [x] Add/update frontend static tests to assert canonical field usage and authority labels in operational display.
- [x] Run focused verification (`pytest` for changed tests and frontend build/static tests), then perform manual payload/UI checks against existing capture/live-compatible endpoints.

Plan confirmation:
- Scope is Stage 2 only.
- No period math/refinement/phase/reference/burst/frame/handoff/readiness/authority behavior changes beyond additive canonical fields and their consumption in API/UI.

### Review (Stage 2)
- Backend: Added canonical fields to sync serialization (`base_period_s`, `period_delta_s`, `effective_period_s`, `period_authority`, `period_refinement_status`, `sync_authority`) while preserving legacy Stage 0 fields/sources.
- Backend: Canonical values are populated conservatively from operational source (`multi_aircraft_burst` -> `py_base/py_refined`, `go_frame_sync` -> `go_refined/go_runtime`, holdover/unavailable conservative labels).
- Backend: Enforced invariant `effective_period_s == base_period_s + period_delta_s` with `1e-9` tolerance; mismatches are corrected by recomputing delta and logged via warning+counter.
- Frontend: Operational period triple now reads canonical fields only; no fallback to legacy `period_s`/`period_base_s` or Go diagnostic delta.
- Frontend: Added explicit operational labels for `period_authority`, `sync_authority`, and `period_refinement_status`; diagnostic/shadow section remains separate.
- Tests: Added/updated backend and static UI tests for canonical field presence, conservative authority handling, invariant behavior, and operational-vs-diagnostic source separation.
- Verification: `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py tests/test_radar_ui_labels.py -q` passed (`145 passed`), and `cd frontend && npm run build` passed.
