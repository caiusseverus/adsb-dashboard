## 2026-05-21 Stage 8 Kickoff

- [x] Run impact analysis for Stage 8 kickoff config toggle before edit.
- [x] Enable Stage 8 contamination gating by default via `RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED` while preserving rollback through env flag.
- [x] Run focused contamination/authority tests to confirm typed gate behavior and non-contamination paths remain stable.
- [x] Document Stage 8 kickoff outcome and any immediate follow-up tasks.

### Review
- Stage 8 runtime kickoff completed by setting `RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED` default to `True` in `backend/config.py`.
- Immediate rollback path remains intact: set `RADAR_SYNC_CONTAMINATION_DETECTION_ENABLED=false`.
- Updated disabled-path tests to explicitly monkeypatch flag false (no reliance on default).
- Verification:
  - `uv run --directory backend pytest tests/test_stage3r_handoff.py -q` -> 54 passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k contamination` -> 1 passed

## 2026-05-21 Pre-Stage-8 Structural Cleanup (Stage 3/6/7)

- [x] Audit and patch Stage 6 `phase_absolute_available` semantics in `LiveSyncState` serialization/normalization paths (no behavior policy change).
- [x] Remove operative `not_evaluated` gate reasons from Stage 3/10 gate outputs; replace with typed stub/typed unavailable reasons.
- [x] Ensure contamination pre-Stage-8 gate/report emits typed states/reasons (`single_family`/`insufficient_data` and stub reason), not `not_evaluated`.
- [x] Confirm Stage 7 population monitor config rollback path and typed propagation (`disabled`/`pass`/`fail`/`insufficient_data`) including demotion propagation fields.
- [x] Add/adjust tests for phase absolute availability semantics, typed gate outputs, contamination stub typing, population typed disabled/demotion propagation, terminology guard.
- [x] Run targeted tests: `test_radar_sweep.py`, `test_phase_semantics.py`, `test_sync_population_monitor.py`, `test_radar_api.py` relevant subsets.
- [x] Produce compliance delta summary and Stage 8 readiness statement.

### Review
- Stage 6R:
  - Tightened `phase_absolute_available` computation/normalization to explicit geographic-only semantics (`phase_basis=geographic`, `phase_is_absolute=true`, finite geographic offset).
  - Prevented legacy fallback promotion from finite offset alone in `_live_sync_state_to_dict`.
  - Preserved existing `phase_is_absolute` and `localisation_safe_phase` behavior paths (no policy threshold change).
- Stage 3R:
  - Removed operative `not_evaluated` emissions from gate outputs:
    - python gate `harmonic_ambiguity` now typed as `harmonic_ambiguity_unimplemented_stub`.
    - phase-readiness null-state placeholders now `insufficient_data`.
    - slope trend fallback reason now typed `insufficient_slope_history`.
  - Pre-Stage-8 contamination disabled path now typed stub:
    - gate reason `stage8_not_available_stub`,
    - `LiveSyncState.contamination_state` normalized to `insufficient_data`,
    - `LiveSyncState.contamination_reason` typed stub.
  - Snapshot Stage-3 summary now exposes `contamination_gate_reason` and avoids `not_evaluated`.
- Stage 7R:
  - Confirmed `RADAR_SYNC_POPULATION_MONITOR_ENABLED` config is read in resolver and drives typed states:
    - disabled -> `population_validation_state=disabled`, reason `monitor_disabled`.
    - enabled -> `pass` / `fail` / `insufficient_data`.
  - Confirmed population demotion propagation stays typed and intact:
    - `phase_anchor_status=population_demoted`,
    - `phase_trust_reason=population_demoted`,
    - `population_validation_reason=population_demoted` (or hold-active variant),
    - demotion hold hysteresis remains 10s.
- Terminology guard:
  - Added test guard that enforces phase semantics docs keep anchor-relative explicitly non-geographic/non-absolute wording.
- Targeted verification:
  - `uv run --directory backend pytest tests/test_stage3r_handoff.py -q` → 54 passed
  - `uv run --directory backend pytest tests/test_phase_semantics.py -q` → 58 passed
  - `uv run --directory backend pytest tests/test_sync_population_monitor.py -q` → 55 passed
  - `uv run --directory backend pytest tests/test_radar_api.py -q` → 63 passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q` → 275 passed


## 2026-05-21 Operator-Facing "Why Not Operational?" Panel

- [x] Run GitNexus impact analysis before frontend edits (`RadarPage`/related formatter paths).
- [x] Add pure helper formatter `formatOperationalBlocker(syncState)` with deterministic priority ordering and short explanation text.
- [x] Add utility tests for active, holdover, slope blocked, sync unusable, hysteresis pending, bootstrap/ICAO blocked, non-geographic phase, and unknown fallback.
- [x] Add "Why not operational?" panel in IID diagnostics with primary reason, compact supporting facts, and raw diagnostic expander.
- [x] Run frontend utility tests and production build.

### Review
- Implemented diagnostics-only frontend mapping in `frontend/src/utils/radarSync.js` via `formatOperationalBlocker(syncState)`; no backend or runtime gate/policy logic changed.
- Added coverage in `frontend/src/utils/radarSync.test.js` for all required formatter scenarios.
- Added an operator-facing panel in `frontend/src/pages/RadarPage.jsx` that shows:
  - one primary reason (ordered by requested precedence),
  - concise explanatory sentence,
  - compact reason-specific facts,
  - collapsible raw diagnostics payload for unknown/debug cases.
- Validation:
  - `node --test frontend/src/utils/radarSync.test.js` passed.
  - `cd frontend && npm run build` passed.

## 2026-05-21 Shadow-Freeze No-Benefit Decomposition Report Enhancement

- [x] Run GitNexus impact analysis for `tools/shadow_freeze_live_monitor_report.py` symbols before edits and confirm risk level.
- [x] Split no-benefit output into actionable per-IID causes using existing shadow diagnostics only (no behavior changes).
- [x] Add requested per-IID dwell/blocker/next-state/follow-up fields for shadow pass and shadow-safe opportunities.
- [x] Add ranked candidate list classification labels and include per-IID rationale fields.
- [x] Verify script output shape on existing capture rows artifact with no runtime policy/gate changes.

### Review
- GitNexus impact run completed for `_classify_iid` and `main` in `tools/shadow_freeze_live_monitor_report.py`; risk `LOW` (local tools-only blast radius).
- Report script now decomposes “no benefit” by explicit causes and exports requested per-IID diagnostics:
  actual/slope/pass/promote/safe/contaminated dwell, dominant blocker during pass, next state after pass, active/holdover/hard-reject follow-up flags, plus shadow-safe opportunity count/dwell/residual-fit/strict/holdover/hard-reject/next-state fields.
- Ranked candidate output now uses requested buckets (`strong future candidate`, `possible candidate`, `no benefit because other gates block`, `no benefit because too transient`, `no benefit because naturally recovers`, `unsafe/contaminated`, `insufficient data`) while preserving existing behavior logic.
- Verification used existing baseline rows input with report regeneration only; no runtime thresholds/tolerances/Stage-10/holdover/reacquire/slope/residual/localiser/geographic/chart behavior changed.

## 2026-05-20 Shadow Freeze / Delta-Stability Experiment

- [x] Implement offline shadow experiment script for variants A/B/C using fresh post-instrumentation capture rows only.
- [x] Emit required shadow fields (`shadow_freeze_variant`, `shadow_slope_*`, `shadow_would_promote_if_delta_frozen`, `shadow_blocker_if_delta_frozen`, `shadow_safety_flags`) without changing runtime behavior.
- [x] Run experiment on target IIDs (20,58,61,74,35,72 and operational comparators where present).
- [x] Produce safety checks for shadow-promotion rows vs actual active rows.
- [x] Classify each IID and provide one recommendation backed by shadow evidence.

### Review
- Added runtime diagnostics-only shadow monitor serialization for variants A/B/C plus safety class/flags and rolling 10m/30m counters in `backend/radar/sweep.py`.
- Added compact extraction coverage for all shadow fields in `tools/blocker_baseline_compact.py`.
- Added offline tools:
  - `tools/shadow_freeze_delta_stability_experiment.py`
  - `tools/shadow_freeze_live_monitor_report.py`
- Added targeted tests in `backend/tests/test_radar_sweep.py` and extended zero/false-preservation tests in `backend/tests/test_blocker_baseline_compact.py`.
- 30-minute live capture/report executed (`14400` rows), but the active backend process serving `127.0.0.1:8000` did not expose new shadow fields (`shadow_fields_present_ratio=0` across IIDs), so longitudinal classification from that capture is `insufficient data`.
- Attempted to restart backend from current workspace for live validation, but startup fails in this sandbox due radar-core unix-socket permissions (`listen unix /tmp/radar-core.sock: setsockopt: operation not permitted`), preventing in-sandbox end-to-end runtime verification of new live fields.

## 2026-05-20 Slope Target Movement Diagnostics Investigation

- [x] Run impact analysis for touched sync snapshot/event symbols before edits (GitNexus index lookup currently returning symbol-not-found/read-only index warnings; recorded in review).
- [x] Add diagnostics-only slope-target movement metrics to sync snapshot/event payload surfaces using existing retained in-memory history (no expensive scans, no gate behavior changes).
- [x] Extend compact baseline extractor rows with new movement metrics and movement reason fields.
- [x] Add offline shadow/cohort/timeline/classification analysis tool using captured compact rows (no runtime behavior change).
- [x] Run targeted backend tests covering compact extractor mapping, and execute offline analysis on latest blocker baseline capture.

### Review
- Added diagnostics-only 10s/30s/60s movement rate + absolute movement + moving booleans + reason for retained/proposed/applied deltas, sourced from retained in-memory update history.
- Added same diagnostics to per-IID convergence event snapshots and compact sync snapshot payload/root so movement can be correlated with `near_zero_window_short` and `regression_not_decreasing`.
- Updated compact extraction script to carry these fields into baseline row JSONL for downstream analysis.
- Added `tools/analyze_slope_target_movement.py` to run offline shadow tests, cohort comparisons, IID 20 timeline extraction, high-support slope-blocked IID classification, and one-action recommendation output.

## 2026-05-09 Transition Quarantine Counters In Sync Snapshot

- [x] Run GitNexus impact analysis for symbols to edit (`build_iid_sync_snapshot_payload`, `build_sync_mode_diagnostics`; `_build_go_diagnostic_fields` not indexed by name).
- [x] Expose transition-quarantine counters and metadata in lightweight sync-snapshot payload from existing compact per-IID diagnostics, without changing behavior paths.
- [x] Add `go_diagnostic_transition_quarantine_*` aliases in sync diagnostic display payload where Go diagnostic aliases are emitted.
- [x] Ensure reads are per-IID and O(1) by using existing compact diagnostic state, with no timeline/history scans.
- [x] Add/adjust tests for: snapshot field presence; fit-excluded counter increment path; hard-reject-suppressed stays zero unless suppression path exists; last reason/timestamp updates on trigger; no behavior/eligibility policy changes.
- [x] Run backend targeted tests, then focused pytest subset.
- [x] Re-run a 10-minute top-20 sync-snapshot capture and summarize latency, per-IID quarantine counters, quarantine rate/min, HOLDOVER/hard_residual_reject correlation, and go_operational_active dwell.

### Review
- Implemented transition-quarantine counters on `LiveSyncState` and wired serializer fallback from `go_sync` so lightweight snapshots expose both raw fields and `go_diagnostic_*` aliases.
- Kept behavior unchanged: no threshold/tolerance/policy/localiser/chart/semantic changes.
- `transition_quarantine_hard_reject_suppressed_count` remains zero unless an explicit suppression path is implemented.
- 10-minute top-20 capture completed successfully with no request failures and no observable latency regression in sampled endpoints.

## 2026-05-09 Stale Anchor-Relative Trust Guard

- [x] Trace source-of-truth for requested snapshot fields (`phase_status`, `phase_status_display`, anchor fields, population validation, burst/fit evidence counters/ages, Go diagnostic fit counters).
- [x] Implement freshness guard in sync serialization/readiness so stale retained anchor state is never emitted as current trusted (`anchor_trusted`) without fresh eligible sync-driving evidence.
- [x] Ensure retained diagnostics remain visible and explicitly marked retained/stale (`anchor_retained_stale`, `stale_phase_evidence`, `phase_anchor_retained_without_current_evidence`).
- [x] Ensure stale-evidence shape maps to concrete blocking gate (`go_readiness.go_evidence_fresh` and/or `phase_readiness.phase_evidence_fresh`), not `go_state_unclassified` / `go_readiness.unclassified_state`.
- [x] Add regression tests for screenshot-like stale shape and verify no Stage 10/threshold/tolerance/localiser/chart/geographic semantics changes.
- [x] Re-run focused tests and short live capture; report requested stale-lock counters and dwell metrics.

### Review
- Added serializer freshness signals (`phase_state_ts`, `phase_evidence_age_s`, `phase_evidence_fresh`, `current_fit_epoch_age_s`) and stale anchor display guard.
- Added phase-readiness gate `phase_evidence_fresh`; trusted gate now requires fresh evidence for anchor/geographic basis.
- Added Stage-10 non-active stale-shape remap from unclassified placeholder to concrete freshness blocker.
- Added regression tests for stale screenshot-like shape and unclassified stale mapping.
- Short live capture executed in an isolated runtime; no active IIDs were present (`SAMPLES 0`), so production-like stale-lock incidence could not be observed in this environment.

## 2026-05-09 Stale Anchor UI + Stage 10 Attribution Follow-up

- [x] Reproduce and extract captured `go_state_unclassified` / `go_readiness.unclassified_state` sample shapes from active-IID capture.
- [x] Update phase trust UI rendering to prefer `phase_status_display` and keep legacy `phase_status` only as fallback.
- [x] Ensure retained stale trust (`anchor_retained_stale`) is visually distinct from `anchor_trusted`.
- [x] Map captured non-active GO_REFINING unclassified shape to concrete Stage 10 gate attribution.
- [x] Clarify burst-row absence reason when evidence is fresh but burst-row display rows are absent.
- [ ] Run targeted/backend tests and replay + fresh 5–6 minute live validation; report requested metrics.

## 2026-05-10 Stage 10 Attribution Serialization Fix

- [x] Analyze provided capture for all rows where `go_operational_enabled=true`, `go_operational_active=false`, and `go_operational_blocking_gate=null`; group by requested attribution fields and extract top shapes with typed raw examples.
- [x] Run impact analysis on serializer symbol before edits; then patch Stage 10 attribution precedence in `backend/radar/sweep.py` so non-active enabled rows always serialize a concrete `go_operational_blocking_gate` and never use phase blockers as Go blockers.
- [x] Add/adjust regression tests in `backend/tests/test_radar_sweep.py` for precedence mapping, non-null blocker invariant, ready-like hysteresis mapping, stale-evidence mapping, and captured unclassified shapes.
- [x] Run targeted backend tests plus capture replay validation metrics; report counts for null blocker, unclassified labels, and phase blocker leakage into active Go blocker field.
- [x] Run fresh 3–6 minute live capture and report requested metrics including dwell by handoff state.

### Review
- Capture analysis confirmed the 3863 `go_operational_blocking_gate=None` rows are `sync-snapshot` records with `sync_state: null`, i.e. attribution fields absent at serialization time.
- Added compact-snapshot fallback serialization for `sync_state: null` + Go diagnostics so Stage 10 non-active blocker attribution is emitted with precedence and phase/Go blocker split preserved.
- Added regression tests for null-`sync_state` non-active attribution and ready-like hysteresis mapping.
- Targeted tests passed; live 3–6 minute capture could not be run in this sandbox because backend startup failed (`radar-core` unix socket `setsockopt: operation not permitted`).
- Corrected methodology now reports raw payload inspection and serializer replay separately, and excludes `sync_state: null` rows from Stage 10 attribution metrics.
- Replay of provided capture (`...live_r3...`) over `sync_state`-present rows: raw contained 54 `go_state_unclassified`/`go_readiness.unclassified_state`; serializer replay with patched code emitted 0 of each and 0 non-active null blocker.
- Fresh 3-minute multi-IID capture (`sync_capture_multi_iids_stage10_attr_postfix_live_20260510T210447Z.ndjson`) collected 513/513 `sync_state`-present rows. Raw backend output still had 58 unclassified rows (backend process had not yet reloaded patched code); serializer replay over those exact rows emitted 0 unclassified and 0 non-active null blocker.

## 2026-05-11 Compact Sync Snapshot Quality Diagnostics Propagation

- [x] Run impact analysis for touched symbols and classify break location across Go state, protocol transport, backend mapping, and compact snapshot serializers.
- [x] Patch propagation only (no logic changes): ensure quality-eval diagnostics and strict-gate subreason diagnostics are transported from Go IID_STATE into backend compact snapshot diagnostics.
- [x] Add explicit `quality_diagnostics_unavailable_reason` when quality-below-threshold diagnostics are absent.
- [x] Add/adjust strict-gate primary subreason export without changing strict gate behavior.
- [x] Add regression tests for mapping, compact snapshot exposure, unavailable-reason fallback, and strict-gate subreason presence.
- [~] Run targeted backend and radar-core tests proving no behavior/promotion-policy changes.
- [ ] Re-run compact validation capture (20-IID style, 5 minutes) and report requested breakdown metrics.

### Review
- Root cause confirmed: IID_STATE protocol transport omitted quality-eval diagnostic fields; backend IID_STATE mapping therefore could not populate compact snapshot quality diagnostics.
- Added transport + mapping for quality-eval fields, explicit quality diagnostics unavailable-reason fallback, and strict gate primary/subreason diagnostics (diagnostic-only).
- Targeted backend tests passed; full radar-core Go test run could not complete in this sandbox due toolchain/cache environment constraints.
- Live 5-minute capture could not be run because no backend process was reachable on `127.0.0.1:8000` in this sandbox session.

## 2026-05-15 Post-Cleanup Blocker Baseline (Current Build)

- [x] Capture fresh 10-minute sync-snapshot-only baseline from localhost:8000 for top 20-30 active IIDs (compact rows only).
- [x] Produce compact-row dataset and limited raw exemplars per blocker class with no behavior/policy changes.
- [x] Compute requested distributions/counters/evidence: handoff dwell, blocker gates, reasons, strict-gate primary reasons, quarantine, stale-anchor trust violations, unclassified/missing blocker recurrence, hard residual reject counters.
- [x] Compare best active IID versus typical blocked IID using the captured window.
- [x] Classify remaining blockers into expected conservative gating, real data/support limitation, likely bug, instrumentation ambiguity.
- [x] Add review summary and exactly one next action recommendation.

### Review
- Fresh 10-minute capture succeeded (`3000` compact sync-snapshot rows, `0` failures) for top 25 active IIDs.
- Handoff-state and handoff-reason fields are populated and show active conservative gating behavior (`GO_REFINING`, `GO_REFINED_READY`, `HOLDOVER`).
- Compact snapshot blocker-attribution/quality/strict/quarantine fields requested for this baseline were null for all rows in this runtime (`go_operational_blocking_gate`, strict-gate primary reason, quality-below-threshold status, holdover reason, quarantine counters), yielding `go_state_unclassified_or_missing_blocker_rows=3000`.
- Stale-anchor trust violation count stayed at `0` in this window.
- Single next action recommendation: restore compact snapshot propagation of Go blocker/strict/quality/quarantine fields, then rerun this same baseline protocol unchanged.

## 2026-05-15 Compact Blocker Baseline Extraction Fix

- [x] Compare raw sync-snapshot payload vs compact extraction for representative live rows and confirm mismatch source.
- [x] Fix compact extraction mapping to read blocker/quality/strict/quarantine fields from current `sync_state` paths with explicit alias fallbacks.
- [x] Remove lossy `or`-style fallback behavior for zero/false preservation via explicit `None`-aware selection.
- [x] Add extraction tests with representative fixture shapes, including zero/non-zero counters and false/empty-string values.
- [x] Re-run exact 10-minute compact baseline and verify acceptance checks on non-active blocker attribution and go-runtime active consistency.

### Review
- Root cause confirmed in extraction path, not runtime sync behavior: the baseline script read many fields from outdated top-level paths instead of `sync_state`/diagnostic aliases.
- Added `tools/blocker_baseline_compact.py` with `extract_compact_row()` and side-by-side raw-vs-compact evidence output.
- Added `backend/tests/test_blocker_baseline_compact.py` (3 tests passing) to lock field mapping and zero/false preservation.
- Final rerun (`blocker_baseline_compact_fixed_r2_20260515T230753Z_*`) produced consistent compact output:
  - non-active + sync_state-present rows with missing `go_operational_blocking_gate`: `0/2760`
  - `handoff_reason=go_runtime_operational` rows with `go_operational_active!=true`: `0/240`
  - stale evidence equivalent boolean surfaced (`True:720`, `False:2280`)

## 2026-05-17 Stage 10 Hysteresis Diagnostics Export + Baseline Rerun

- [x] Add read-only Stage 10 promotion-hysteresis diagnostics fields to sync snapshot serialization (`ready_streak`, threshold, reset details, soft-failure details, hysteresis decision).
- [x] Preserve new diagnostics in compact extraction rows and raw-vs-compact side-by-side mapping.
- [x] Add regression tests for pending hysteresis, promoted threshold shape, reset reason/gate export, soft-failure export, and zero/false value preservation.
- [x] Run targeted backend tests for new fields and compact extraction.
- [ ] Re-run 600s compact baseline capture and produce hysteresis distribution/classification report from newly exported diagnostics.

### Review
- Implemented read-only diagnostics fields on `LiveSyncState` and in Stage 10 handoff path without changing admission logic, thresholds, or gate semantics.
- Snapshot payload now exports:
  `go_operational_ready_streak`, `go_operational_promotion_threshold`,
  `go_operational_ready_streak_age_s`, `go_operational_last_ready_ts`,
  `go_operational_last_not_ready_ts`, `go_operational_streak_reset_reason`,
  `go_operational_streak_reset_gate`, `go_operational_streak_reset_handoff_reason`,
  `go_operational_soft_failure_active`, `go_operational_soft_failure_until_ts`,
  `go_operational_soft_failure_remaining_s`, `go_operational_soft_failure_reason`,
  `go_operational_hysteresis_decision`.
- Compact extractor and side-by-side mapping now preserve these fields.
- Tests passed:
  - `uv run --directory backend pytest tests/test_blocker_baseline_compact.py` (`5 passed`)
  - targeted hysteresis/snapshot subset in `test_radar_sweep.py` (`11 selected passed`).
- Remaining blocker in this Codex environment: localhost backend endpoint could not be reached for live 600s capture due runtime networking/process constraints, so rerun/report remains pending.

## 2026-05-17 GO_REFINING Hysteresis Attribution Ambiguity

- [x] Trace attribution path for `handoff_state=GO_REFINING` + `handoff_reason=go_ready_pending_hysteresis` and capture representative rows from `blocker_baseline_compact_stale_fix_r3_hystdiag_20260516T232042Z`.
- [x] Patch attribution logic so `go_ready_pending_hysteresis` is emitted only for explicit pending promotion hysteresis (`decision=pending`, threshold>0, streak<threshold, no earlier failed readiness gate).
- [x] Preserve proper Stage-10 pending hysteresis (`GO_REFINED_READY` + thresholded pending rows).
- [x] Add/update tests for: real pending hysteresis preserved; GO_REFINING threshold=0/decision=null not labeled hysteresis; GO_REFINING slope/sync-unusable concrete reasons.
- [x] Run targeted pytest validation for updated Stage-10 attribution suite.
- [ ] Re-run 600s compact baseline and produce before/after blocker redistribution metrics.

### Review
- Removed fallback remap of unclassified GO_REFINING rows to `go_ready_pending_hysteresis` in two serializer paths:
  `go_frame_sync` attribution in `_live_sync_state_to_dict`, and compact fallback sync-state synthesis in `get_live_sync_snapshot`.
- Added strict pending predicate for hysteresis attribution based on exported diagnostics (`go_operational_hysteresis_decision`, streak, threshold).
- Extended fallback sync-state synthesis to carry hysteresis diagnostic fields from `go_sync` when available.
- Updated Stage-10 regression expectations to enforce no fallback hysteresis labeling without true pending diagnostics.
- Targeted tests passed after update; live 600s rerun remains pending new capture artifact.

## 2026-05-17 Unclassified Attribution Follow-up

- [x] Analyse post-fix `go_readiness.unclassified_state` rows and extract representative typed row dumps.
- [x] Classify unclassified rows by concrete blocker heuristics vs genuinely unknown.
- [x] Update attribution logic to re-resolve concrete failed gates when `go_readiness.unclassified_state` is a placeholder.
- [x] Ensure placeholder `go_state_unclassified` is replaced by concrete reason when mapping to sync-usable / holdover / refinement / stale blockers.
- [x] Add regression tests for captured unclassified shapes (slope gate failure, holdover, insufficient history, pending hysteresis unchanged, active-row guard).
- [x] Run targeted pytest suite for Stage 10 attribution branches.
- [ ] Re-run 600s compact baseline and report before/after `unclassified_state` reduction.

### Review
- Root cause identified: serializer only promoted `handoff_gate_failures` into `blocking_gate` when `blocking_gate` was empty; rows with placeholder `go_readiness.unclassified_state` skipped concrete gate recovery.
- Patched `_live_sync_state_to_dict` to treat `go_readiness.unclassified_state` as a placeholder and re-evaluate concrete gate failures from `handoff_gate_failures`.
- Strengthened refinement status fallback so `go_sync.period_refinement_status in {insufficient_history, unavailable}` can override generic/derived stable status for attribution.
- Placeholder reason replacement now applies when a concrete gate is selected (`go_state_unclassified` -> concrete reason).
- New regression tests added and passing for slope/holdover/refinement placeholder shapes.

## 2026-05-18 Remaining GO_REFINING Unclassified Investigation + Attribution Mapping

- [x] Dump at least 10 representative `go_state_unclassified` / `go_readiness.unclassified_state` rows with raw-vs-compact side-by-side fields, including all requested gate/authority/refinement/fit/slope/period/population diagnostics.
- [x] Classify hidden blocker per row and identify attribution gap categories (missing gate tree, source/authority mismatch, hidden readiness fail, etc.).
- [x] Apply deterministic attribution-only mapping for unclassified GO rows; preserve all sync behavior, thresholds, tolerances, Stage 10 policy, localiser, chart, quarantine, holdover, strict epoch, residual gates, and geographic/absolute semantics.
- [x] Add regression tests for ready-looking GO_REFINING unclassified shape, insufficient_history variant, active-row null blocker invariants, and phase-blocker-not-Go-blocker guard.
- [x] Re-run targeted pytest and compact baseline analysis; report before/after unclassified counts, blocker redistribution, unknown remainder, and dwell metrics.

### Review
- Added deterministic attribution for two previously-unclassified non-active GO shapes:
  1) ready-like GO_REFINING where hysteresis has not started (`go_readiness_hysteresis` + `go_ready_hysteresis_not_started`),
  2) hidden quality/period/strict usability failures (`go_readiness.go_sync_state_usable` with concrete reason).
- Kept pending hysteresis semantics unchanged (`go_ready_pending_hysteresis` still requires explicit pending decision + thresholded streak).
- Added regression tests for threshold=0/decision=null ready-like shape, captured live unclassified variants, and hidden quality-gate shape.
- Captured investigation artifact: `tasks/radar_sync_baseline/go_unclassified_investigation_20260518.json` with 10 representative rows and replayed redistribution.
- Targeted tests passed; live 600s baseline rerun was not executed in this sandbox session, so before/after is reported via replay against captured baseline rows.

## 2026-05-19 Rolling Per-IID Sync Convergence Event History

- [x] Add rolling per-IID event history buffer with max-length cap and change-triggered event capture.
- [x] Include compact convergence snapshot fields per event (handoff/slope/fit/authority/holdover/residual/phase).
- [x] Expose IID debug endpoint: `GET /api/radar/iids/{iid}/sync-event-history` with `limit`, `event_type`, `since_s`.
- [x] Add backend tests for change recording coverage, buffer cap behavior, and endpoint filter/order semantics.
- [x] Run 10-15 minute live validation capture for IIDs `20,42,52,72` + one operational IID; summarize event patterns and recommendation.

### Review
- Implemented rolling sync-convergence event history recorder and API endpoint.
- Added targeted sweep/API tests for core event capture, holdover enter/exit, max-length cap, and endpoint filtering/order.
- Completed 600s live capture on IIDs 20/42/52/72/40 and produced per-IID event-pattern summary in terminal report.
