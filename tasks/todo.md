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

## 2026-04-30 Stage 3 Formal Python→Go Authority Handoff

- [x] Run impact scan for handoff/authority symbols and current Go adoption path.
- [x] Add explicit handoff state model and gate payload fields to sync snapshot serialization.
- [x] Implement Python base-validity gate helper and Go-readiness gate helper with per-gate pass/fail/unknown reporting.
- [x] Replace silent Go sync adoption with explicit gated transitions:
- [x] Frame diagnostics mode (Go data accepted, no operational authority adoption).
- [x] Period authority adoption mode (gated; requires valid Python base + Go readiness).
- [x] Phase authority adoption mode (separate gating; anchor-relative/geographic basis + fresh/trusted phase state).
- [x] Add handoff transition tracking (`handoff_state`, `handoff_reason`, `last_handoff_transition_ts`) and non-spam structured logging.
- [x] Wire new handoff fields through API selected-IID and compact sync payloads.
- [x] Update Radar diagnostics panel to show handoff state/reason, authority split, and blocking gates.
- [x] Add/update backend tests for gate behavior, authority separation, transition updates/logging, and payload fields.
- [x] Add/update frontend static tests for handoff labels/blocking reason rendering and phase-authority truthfulness.
- [x] Run focused verification (`pytest` changed tests, frontend build) and document results.

Plan confirmation:
- Stage 3 only. No Stage 4+ implementation.
- No period-refinement math, phase math, burst classification, reference selection, or frame-generation algorithm changes except explicit authority gating/visibility.

### Review (Stage 3)
- Backend: Added explicit handoff metadata fields to live sync state (`handoff_state`, `handoff_reason`, `last_handoff_transition_ts`, `handoff_gate_failures`, `phase_authority`).
- Backend: Replaced implicit Go authority adoption with explicit diagnostic ingest + gate-driven handoff evaluation.
- Backend: Added centralized Python base-validity, Go readiness, and phase-authority gate evaluators with per-gate `passed`/`reason` entries and unknown/not-evaluated states.
- Backend: Added structured transition logging on handoff-state change only (`radar_sync_handoff_transition` with iid/old/new/reason).
- Backend/API: Sync snapshot payload now includes handoff state/reason/timestamp/gate failures and explicit phase authority, while preserving canonical period fields and legacy fields.
- Frontend: Radar diagnostics now shows handoff state, handoff reason, phase authority, and compact blocking gate list.
- Tests: Added/updated backend and UI tests for Stage 3 authority gating, handoff fields, and transition logging behavior.
- Verification:
- `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_ui_labels.py -q` -> `149 passed`
- `cd frontend && npm run build` -> passed

## 2026-04-30 Stage 4 Harden Go Refiner as Operational Engine (Preparation Only)

- [x] Audit and document residual sign convention across Go/Python paths feeding Go refinement.
- [x] Run best-effort impact scan for touched sync/refiner symbols/files (no direct `gitnexus_impact` MCP tool available in this session).
- [x] Add/standardize Go residual sign helper and boundary conversions; emit explicit `slope_sign_convention` only when true.
- [x] Replace Go count-tail residual retention with 120s + per-ICAO cap + global cap retention policy and diagnostics.
- [x] Tighten Go refinement observation eligibility (hard rejects, soft weights, family gating, stale position, suspicious ICAO handling).
- [x] Keep Go base-period authority constraints intact and enforce `effective_period_s = base_period_s + period_delta_s` diagnostics invariant.
- [x] Extend Go diagnostics payload/schema for fit/span/ICAO/slope/delta/reject/bounds/suspicion fields.
- [x] Wire new Go diagnostic fields through Python ingestion/API and keep operational triple authority unchanged.
- [x] Update RadarPage diagnostics rendering for additive Go diagnostic fields only.
- [x] Add/update Go tests for sign correctness, unwrap, retention, eligibility, suspicion, bounds, and invariants.
- [x] Add/update backend tests for diagnostic ingestion/exposure and operational-field non-regression.
- [x] Run focused verification (Go tests + backend pytest + frontend build) and replay/synthetic validation where available.

Plan confirmation:
- Stage 4 only; no Stage 5 authority flip or Python operational refiner demotion.
- Python DF/base period bootstrap and operational authority stay unchanged.

### Review (Stage 4)
- Go refiner hardening implemented in `sync.go`/`state.go`: explicit observed-minus-predicted sign contract, 120s window retention with per-ICAO/global caps, stale-position and family hard-rejects, soft outlier weight 0.25, suspicious ICAO quarantine, and bounded fit diagnostics.
- Go protocol + emitter updated with additive diagnostics for fit composition/span, slope EMA/std, proposed/applied delta, bounds/slew flags, suspicious ICAO state, and sign convention.
- Python ingest/snapshot serialization updated to consume and expose additive Go diagnostic fields while preserving canonical operational authority behavior from Stage 3.
- Radar page diagnostics now surfaces additional Go fit/slope/delta metadata in diagnostic pills; operational period triple remains canonical/authority-gated.
- Verification:
  - `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid ./protocol` -> passed.
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py -q` -> `146 passed`.
  - `cd frontend && npm run build` -> passed.

## 2026-05-01 Stage 4 Follow-up: Go Sync Epoch Reacquisition After Hard Residual Holdover

- [x] Run impact scan for sync epoch update and snapshot symbols.
- [x] Add hard-residual reject diagnostics (residual/predicted/observed, reject streak, accepted age, epoch age, current/candidate epoch).
- [x] Implement state-aware hard residual handling (trusted path rejects+holdover; holdover path accumulates reacquisition evidence).
- [x] Implement controlled epoch reacquisition gates and provisional state transition (no immediate phase authority).
- [x] Add bounded reset fallback based on conservative reject-count and no-accept-age thresholds.
- [x] Preserve period refinement history/delta across epoch reacquisition unless base period materially changes.
- [x] Wire diagnostics through debug/protocol snapshot payloads.
- [x] Add/extend Go tests for stale epoch rejects, reacquisition, authority non-promotion, period preservation, and trusted single-outlier behavior.
- [x] Run focused verification (`go test` for iid/protocol paths) and review results.

Plan confirmation:
- Scope is limited to Stage 4 follow-up reacquisition and diagnostics only.
- Operational authority gates remain strict; reacquisition cannot grant phase authority by itself.

### Review (Stage 4 follow-up)
- Added holdover hard-residual diagnostics and epoch-age visibility fields in Go sync state, debug snapshot, protocol IID_STATE, and snapshot payload map.
- Added holdover-aware hard-residual handling:
- Trusted/non-holdover path still hard-rejects and enters holdover.
- Holdover path records consecutive hard rejects and can trigger conservative controlled reacquisition when support/gates pass.
- Added bounded fallback gates (`consecutive` and `no accepted update age`) and kept thresholds conservative (`N=8`, `M=30s`) within the controlled reacquisition checks.
- Reacquisition re-anchors epoch/offset and marks provisional (`reacquired_provisional`) while forcing strict-gate flag false so phase authority is not granted by reacquisition alone.
- Preserved period refinement delta/history through epoch reacquisition; no reset of `PeriodDeltaS` unless existing base-period-change logic triggers elsewhere.
- Verification: `cd radar-core && GOCACHE=/tmp/go-build-cache go test ./iid ./protocol` -> passed.
