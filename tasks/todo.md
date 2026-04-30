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
