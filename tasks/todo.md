## 2026-04-28 Simple Sync Follow-Up

- [x] Review lessons and the prior radar sync cleanup before editing.
- [x] Remove stale Go sync-source residue and delete `_sync_source_is_go_compact()`.
- [x] Rename remaining Python simple-sync internals from `multi_sync` to `simple_sync` / `live_sync` where they now refer to the Python updater.
- [x] Update tests and debug/reset counters to the renamed simple-sync terminology.
- [x] Run focused verification for Go sync adoption, Stage 3 source rejection, and simple-sync throttling.

Plan confirmation:
- Keep this as a narrow cleanup pass. Do not change propagation delay or motion compensation.
- Remove all remaining `sweep_frame_go` references; keep compact Go snapshot behaviour if still required, but under non-stale naming.
- Rename only internals and debug/reset counters that now describe the Python simple sync updater.

### Review
- Implemented:
  - Deleted `_sync_source_is_go_compact()` and inlined the remaining compact-Go checks against the renamed non-Python source.
  - Renamed the stale source tag `sweep_frame_go` to `go_frame_sync` everywhere it was still used for compact Go-adopted sync state.
  - Renamed Python simple-sync updater internals from `multi_sync` to `simple_sync`, including `_SIMPLE_SYNC_FIT_WINDOW_*`, `_SIMPLE_SYNC_UPDATE_MIN_INTERVAL_S`, `_last_simple_sync_update_ts`, and the reset/debug counter key `simple_sync_throttle`.
  - Updated targeted tests to the new names and source tag.
- Not changed:
  - Propagation delay logic.
  - Motion-compensation logic.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_phase_refinement.py tests/test_aircraft_localiser_target_live.py -q` -> `186 passed`

## 2026-04-28 Radar Sync Cleanup Audit

- [x] Review lessons, current task history, and working-tree scope before editing.
- [x] Audit Python and Go for remaining radar sync/refinement surfaces against the intended architecture.
- [x] Write a short keep/delete inventory and use it to drive the cleanup.
- [x] Remove obsolete Python legacy/refined-sync diagnostics, compatibility fields, and dead config.
- [x] Simplify `LiveSyncState` and API payloads to only active predictor / Stage 3 / current frontend fields.
- [x] Remove frontend references to deleted sync diagnostics and compatibility fields.
- [x] Remove or rewrite tests that only preserve removed diagnostics/compatibility behavior.
- [x] Run focused backend tests, Go tests/build, and frontend build.
- [x] Document what was deleted, what was kept, and why.

Plan confirmation:
- Keep the functional path only: DF/IID burst accumulation -> base rotation analysis -> Python simple live sync -> `LiveSyncState` -> `predict_sync_observation()` -> Stage 3 trusted-phase gate -> localisation.
- Do not reintroduce Go sync authority, Python legacy sync selection, old waveform/refined-solver diagnostics, or fallback trust heuristics.
- Treat Go as keep-only for burst/frame/FM/radar-position support unless a remaining sync-facing field is still required by the compact functional path.
- Remove UI/API compatibility surfaces in the same pass when their backing backend fields are deleted.

Inventory before deletion:
- `KEEP_FUNCTIONAL`
  - Python: `RadarIID`, `RotationModel`, `BurstRecord`, `_analyse_iid_events()`, `_reinforce_radar_characteristics()`, `_fit_per_aircraft_slope()`, `_update_simple_live_sync_state()`, `predict_sync_observation()`, `_sync_state_has_trusted_absolute_phase()`, active `phase_status` / `phase_anchor_*` / `phase_validation_*` fields used by Stage 3 and current sync display.
  - Go: compact `SyncState`, `IIDState`, `BurstFired`, `IIDState`, `FRAME_READY`, FM state/result paths, frame/burst parsing and radar-position support.
- `KEEP_TEMPORARILY_FOR_FRONTEND`
  - Python/UI payloads around `burst_sync_timeline` that still drive the Radar page: core observation rows, `phase_anchor_candidates`, and currently-used fit / validation counts. These stay only if still read after the UI cleanup.
- `DELETE`
  - Python dead/obsolete surfaces: `WaveformBin`, `_live_waveform_bins`, `_serialise_waveform_bins`, any waveform-only predictor/localiser plumbing, `_compute_folded_phase_shape()`, `_classify_sync_error_mode()`, `dominant_error_mode`, `phase_shape_strength`, `cycle_to_cycle_repeatability`, `predictor_consistency`, `period_refine_mode`, `period_reacquire_*`, `sync_model`, `period_source`, `period_refinement_source`, `phase_source`, long-term-estimator placeholder fields, and tests asserting disabled waveform/refined behavior.
  - Go/Python compatibility leftovers: `go_timing_candidate`, `go_compact_timing_candidate`, `go_refined_payload_present`, `go_refined_payload_usable`, `_go_multi_sync_admission_by_iid`, `_compact_sync_debug_by_iid`, and UI/API surfaces that exist only to describe removed Go authority/refined sync states.
- `MOVE_TO_DIAGNOSTIC_ONLY`
  - None planned. Anything not required by the active predictor, Stage 3 gate, or still-needed Radar UI will be deleted rather than preserved as compatibility baggage.

### Review
- Audit outcome:
  - The active Python path was already the real authority path: base rotation analysis -> `_update_simple_live_sync_state()` -> `LiveSyncState` -> `predict_sync_observation()` -> Stage 3 trusted phase gate.
  - Most remaining cleanup was in Python/API/frontend compatibility and diagnostics, not active Go sync authority logic. Current `radar-core` still contains frame/FM functionality and generic burst evidence, but the old multi-sync solver / authority payload path described in the prompt is not present in the checked-out tree.
- Deleted:
  - Stale `LiveSyncState` fields tied to authority switching, candidate/authoritative dual-state bookkeeping, period reacquire/recovery bookkeeping, long-term branch estimators, and old model provenance placeholders.
  - Waveform/refined-solver compatibility payloads and debug exports, including `waveform_bins`, waveform application flags, phase-shape/error-mode summary fields, and per-ICAO offset compatibility surfaces.
  - Frontend Radar page references to candidate-vs-authoritative sync authority, long-term branch diagnostics, and Go per-ICAO offset compatibility tables.
  - Tests that only preserved removed waveform/error-mode/compatibility behavior.
- Kept:
  - Base period detection and the simple Python period-refinement path.
  - `phase_anchor_*`, `phase_validation_*`, and `phase_status` fields that still drive prediction trust and Stage 3 eligibility.
  - Compact sync diagnostics for reference-aircraft churn and reset/holdover state, because the current UI still uses them and they describe active compact behaviour rather than removed refined authority.
  - Go frame/FM functionality and generic burst evidence paths.
- Verification:
  - `uv run --directory backend pytest tests/test_aircraft_localiser_sync_predictor.py tests/test_radar_phase_refinement.py tests/test_radar_sweep.py tests/test_radar_api.py -q` -> `171 passed`
  - `cd frontend && npm run build` -> passed
  - `GOCACHE=/tmp/go-build go test ./...` under `radar-core/` -> most packages passed; `cmd/radar-core` still fails with pre-existing `declared and not used: assoc` in `main.go`

## 2026-04-24 Refined Sync Authority Promotion

- [x] Review lessons, current task history, and working-tree scope before editing.
- [x] Audit Go promotion logic for circular dependencies involving `AbsolutePhaseTrusted`.
- [x] Add explicit Go promotion blocker / validation status diagnostics and protocol fields.
- [x] Propagate blocker diagnostics through Python bridge and Radar UI.
- [x] Keep candidate, relative usable, refined authoritative, and absolute trusted concepts separate.
- [x] Add Go/backend/frontend regression coverage for promotion, blockers, partial disagreement, and UI propagation.
- [x] Run verification, rebuild `radar-core`, document results, and commit.

Plan confirmation:
- `AbsolutePhaseTrusted` must remain an output of refined authority, never an input to promotion.
- Promotion should be based on candidate validation, validator agreement/disagreement, branch ambiguity, slope gate, dominant-prior bounds, and candidate streak.
- Candidate-anchor diagnostics remain separate from applied authority and active residual basis.

### Review
- Audit outcome:
  - `AbsolutePhaseTrusted` was not directly used as a promotion prerequisite, but the recovery-mode exit gate was too opaque and could keep a validated refined candidate in `dominant_recovery` behind recovery-clean streak semantics.
  - Validation status was collapsed to `unavailable` on the Python/UI side whenever the score was weak, even when validator agree/reject counts had been evaluated.
- Implemented:
  - Recovery mode can now enter `refined_authoritative` from sustained refined-health streak directly; `AbsolutePhaseTrusted` remains only an output once refined authority is active and validation is strong.
  - Go now exports `candidate_validation_status` and `candidate_application_block_reason` with specific reasons such as `validator_disagreement_too_high`, `validator_agreement_insufficient`, `slope_gate_failed`, and `candidate_streak_not_met`.
  - Validation status now distinguishes genuine `validation_unavailable` from evaluated failures such as `validator_disagreement`, `insufficient_validator_agreement`, `branch_ambiguous`, and `weak_validation`.
  - The Radar UI shows the specific not-applied reason, agreement/rejection counts, and candidate promotion streak `N/6`.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(RefinedPromotionDoesNotDependOnAbsolutePhaseTrusted|PhaseValidationStatusDistinguishesUnavailableAndFailed|CandidateApplicationBlockReasons|RealisticPartialDisagreementCanPromote|CompactAuthorityPublishesCompactPhaseNotCandidate|AuthorityPromotionBlockedBySlope)' -count=1` -> passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol ./export -count=1` -> passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `177 passed`
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go build -o radar-core ./cmd/radar-core` -> passed
  - `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-24 Phase Anchor Residual Basis Follow-Up

- [x] Review current lessons and the prior phase-anchor task before changing code.
- [x] Trace Go `PhaseOffsetDeg` / `AuthoritativePhaseOffsetDeg` / `CandidatePhaseOffsetDeg` / `AnchorPhaseDeg` through the Python bridge and Radar UI.
- [x] Add explicit residual-basis labelling to burst residual plots.
- [x] Add an operator-controlled anchor-relative residual plot mode where the selected candidate anchor plots around zero.
- [x] Change Phase Anchor wording so candidate anchor diagnostics are not described as operational authority unless promoted.
- [x] Show single-ICAO validation blocking explicitly: candidate anchor selected, not applied, active residuals still compact-authority residuals.
- [x] Add focused backend/frontend verification and document results.

Plan confirmation:
- Keep this as a display/diagnostic separation fix; do not change the Go solver's authority promotion or validation rules.
- The active residual plot remains operational by default and is labelled by active authority mode (`compact_authoritative`, `dominant_recovery`, `refined_authoritative`, etc.).
- Add an explicit anchor-relative residual basis (`refined_candidate_anchor`) as a separate selectable mode.
- Treat selected phase anchors as candidate/diagnostic unless `active_authority_mode === "refined_authoritative"` and the authoritative anchor matches.
- Audit outcome will be documented in the review section after verification.

### Review
- Audit outcome:
  - Go `CandidatePhaseOffsetDeg` and `AuthoritativePhaseOffsetDeg` were already separate, and Stage 3 localisation is gated by `absolute_phase_trusted`.
  - The remaining bug was that `PhaseOffsetDeg` could publish the candidate phase while `ActiveAuthorityMode` still reported `compact_authoritative`, which made active residual plots ambiguous.
  - `AnchorPhaseDeg` / `AnchorICAO` remain diagnostic candidate-anchor exports; the UI now distinguishes candidate anchor from applied authoritative anchor.
- Implemented:
  - Go compact authority now publishes compact sync `PeriodS` / `PhaseEpochUS` / `PhaseOffsetDeg` while preserving candidate phase/anchor diagnostics in candidate fields and anchor diagnostics.
  - `absolute_phase_trusted` now requires `active_authority_mode == refined_authoritative`, so compact publication cannot be treated as trusted absolute phase even if a stale authoritative state exists.
  - Radar residual plots now show a `Residual basis` selector. Default is the active operational basis (`compact_authoritative`, `dominant_recovery`, or `refined_authoritative`); `refined_candidate_anchor` is available only when the selected ICAO is the candidate anchor and anchor-relative residuals exist.
  - DF11 dots are hidden in `refined_candidate_anchor` mode because they are active-authority residuals, not candidate-anchor residuals.
  - Phase Anchor panels now say `candidate anchor selected` / `not applied: insufficient validator ICAOs` when validation has not promoted the phase branch, and show active residuals as still compact-authority residuals.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(CompactAuthorityPublishesCompactPhaseNotCandidate|NormalRefinementPublishesAlignmentFromRefinedPeriod|ValidatorBackedPromotionInitializesAuthoritativeState|AuthorityPromotionBlockedBySlope)' -count=1` -> passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol ./export -count=1` -> passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go build -o radar-core ./cmd/radar-core` -> passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'go_sync_decodes_absolute_phase_trusted_and_dominant_prior_inconsistent or go_refined_timeline_includes_implied_phase_offsets_without_zero_fallback or update_go_multi_sync_state_overrides_python_multi_aircraft_burst or go_multi_sync_mode_diagnostics_report_dominant_recovery_fields'` -> `4 passed`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `177 passed`
  - `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-24 Phase Anchor Reference Frame Fix

- [x] Review current lessons and existing radar-sync task history before changing code.
- [x] Trace selected-anchor offset, active authority phase offset, and candidate/refined phase offset uses across Go export, backend payloads, and React rendering.
- [x] Fix Go `AnchorPhaseDeg` export so selected-anchor offset is the selected anchor's latest implied offset, while solver phase offset remains separate.
- [x] Fix `anchor Δ` so it is strictly the signed circular difference from the selected anchor's latest implied offset.
- [x] Ensure the selected anchor's latest row reports `anchor Δ = 0.00°`.
- [x] Make burst residual diagnostics anchor-relative when viewing the selected phase anchor, or explicitly label active-authority residual views.
- [x] Add focused Go regression coverage for selected-anchor relative offsets.
- [x] Run backend/frontend verification and document results here.

Plan confirmation:
- Do not change sync solving, authority promotion, or period refinement behavior for this display bug.
- Live behavior is Go-owned; fix `radar-core/iid/multisync.go` first, then keep Python/React as consumers/diagnostics.
- Treat `phase_anchor_offset_raw_deg` / `phase_anchor_offset_smoothed_deg` as solver state, not as the UI table's selected-anchor reference.
- Derive UI `anchor Δ` from observed `implied_phase_offset_deg` rows, choosing the latest selected-anchor observation as the reference.
- Keep active-authority residual fields available, but expose/label an anchor-relative residual frame when the selected aircraft is the phase anchor.

### Review
- Correction accepted: the first pass was incomplete because the live source is Go-owned. The fixed source of truth is now `radar-core/iid/multisync.go`.
- Root cause confirmed:
  - Go `selectAnchor()` returned the circular mean as both the solver phase offset and exported `AnchorPhaseDeg` (`ap`).
  - Python imported `ap` as the phase-anchor offset, so downstream `anchor_relative_phase_error_deg` compared rows against a mean/authority-adjacent phase rather than the selected anchor's latest implied offset.
  - The React table displayed that mixed frame, which made the selected anchor row show non-zero `anchor Δ`.
- Implemented:
  - Go now keeps solver `newOffset` as the circular mean, but exports `anchorPhaseDeg` as the latest implied offset for the selected anchor ICAO.
  - Added Go regression coverage proving exported anchor phase can differ from solver mean and that the selected anchor delta is zero by definition.
  - Python recomputes diagnostic `anchor_relative_phase_error_deg` from the latest selected-anchor implied row when rebuilding retained timeline payloads.
  - React Phase Anchor table/tooltips recompute `anchor Δ` from the latest selected-anchor row, and burst residual plots switch to selected-anchor-relative residuals when the selected ICAO is the active phase anchor. DF11 dots are hidden in that frame because they are active-authority residuals.
  - `radar-core/radar-core` was rebuilt locally from the patched Go source for backend-managed deployments.
- Frame audit:
  - Go `PhaseOffsetDeg`, `CandidatePhaseOffsetDeg`, and `AuthoritativePhaseOffsetDeg` remain active/candidate/authoritative solver offsets.
  - Go `AnchorPhaseDeg` is now selected-anchor latest implied offset for diagnostics/export.
  - Python/React no longer use active authority phase offset as the Phase Anchor table's anchor-relative reference.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'Test(SelectAnchorExportsLatestAnchorImpliedOffsetSeparatelyFromMean|AnchorDeltaCircularDiff|EvalCandidatePeriod_PreservesAnchorCandidates|MultiSyncSolver_ValidatorBackedPromotionInitializesAuthoritativeState)' -count=1` -> passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol ./export -count=1` -> passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'go_refined_timeline_includes_implied_phase_offsets_without_zero_fallback or df11_residual_dots_use_retained_residual_event_history'` -> `2 passed`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `177 passed`
  - `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-23 Burst Sync Display History Regression

- [x] Review current lessons and prior radar-sync task history before changing code.
- [x] Trace the current alignment-panel and sync-graph data paths across Go solver/export, Python bridge/API, selected-state assembly, and React rendering.
- [x] Confirm the exact 30-second regression root cause and document the window conflation.
- [x] Restore explicit separate horizons:
  - solver fit/update window: short `max(6 * period, 30s)`
  - display/history window: retained diagnostic horizon, independent of fit
  - authoritative-state memory: slow damped state, exported separately
- [x] Add retained Go sync diagnostic trend/history recording in the Python bridge without moving fitting logic out of Go.
- [x] Ensure alignment rows and sync graph are sourced from retained display history/evidence, not active fit state.
- [x] Expose fit/display/authoritative horizons in backend payloads and UI labels.
- [x] Fix selected-state/cache revision signatures so retained-history changes invalidate the sync section.
- [x] Add/update tests for retained alignment history, sync trend history, window separation, candidate vs authoritative field integrity, cache/revision correctness, and bounded projection.
- [x] Perform broader radar-pathway review for related issues and fix clear scoped problems in this change.
- [x] Run focused Go/backend/frontend verification, show diffs, and record results here.

Plan confirmation:
- Do not change the Go solver’s short fit window. It is correct for local fitting.
- Add diagnostic/display history as an additive retained source around the Go-owned sync payloads, with bounded deques and cheap per-request filtering.
- Keep Stage 3 ownership intact: Go remains the hot-path solver; Python records and projects diagnostics from Go exports.
- Prefer explicit horizons and null/missing states over silent fallback to a 30-second fit slice.

### Review
- Root cause confirmed:
  - `radar-core/iid/multisync.go` retained observations for 360s, but the solver rebuilds `recent` from `windowS := max(livePeriodS * 6, 30s)` and exported only that short-fit diagnostic view through `MULTI_SYNC_STATE`.
  - Python adopted Go multi-sync state into `LiveSyncState` but did not record Go-owned retained slope/period/update history, so Go sync payloads returned empty trend histories and the graph had no long-history source.
  - The selected-state sync cache signature did not include retained sync history counts/horizons, so history-only changes could be hidden behind an otherwise unchanged sync section.
- Implemented:
  - Go multi-sync export now carries explicit fit/display window diagnostics (`fit_window_s`, `display_window_s`, `fit_span_s`, `residual_slope_deg_per_s`) without changing the solver fit window.
  - Python records bounded retained Go sync trend history in the existing period/slope/update history buffers and exposes `sync_horizons` with fit, display, retained, and authoritative-state metadata.
  - Go-owned compact/refined sync snapshots now return retained trend histories instead of empty arrays.
  - Selected-state sync cache signatures now include `window_s`, horizon metadata, and retained history/observation counts.
  - Radar UI default display window is 300s and the alignment panel labels fit window, display window, and authoritative age explicitly.
- Broader review findings:
  - Candidate vs authoritative fields are still separated in payloads; operational `period_s`/`phase_offset_deg` remain the published Go fields, with candidate/authoritative fields diagnostic-only.
  - Compact vs refined source handling remains explicit through existing `sync_mode_diagnostics`; no Python fitting was reintroduced.
  - Projection remains bounded: retained histories are capped at 1500 rows and alignment projection still filters by requested display window.
- Verification:
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'go_sync_diagnostic_history_is_retained_beyond_fit_window or go_alignment_rows_use_retained_display_history_not_fit_window or update_go_multi_sync_state_overrides_python_multi_aircraft_burst or live_sync_snapshot_reuses_cached_payload_until_sync_inputs_change'` -> `4 passed`
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` -> passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `175 passed`
  - `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-23 Burst Sync Residual Plot Clip And Period Settlement Follow-Up

- [x] Re-open the task after operator feedback that the residual plot still displays only a subset and the refined period/slope still jitter.
- [x] Identify the remaining frontend render-domain clip separate from backend retained-history payloads.
- [x] Change the residual plot x-domain to anchor on retained residual payload timestamps, not the unrelated live timing websocket clock.
- [x] Make the refined Go sync model use the DF-derived dominant period as the stable hardware-period prior when available.
- [x] Keep short-window slope fitting available as diagnostics, but stop it from continuously moving the operational period around the stable dominant period.
- [x] Add focused tests for frontend-domain selection and Go dominant-period locking.
- [x] Run Go/backend/frontend verification and record results.

Plan confirmation:
- The display fix must show all retained residual rows returned by the backend window.
- The period fix must not widen the fit window or move solver work to Python.
- The refined model should solve phase/anchor against the stable dominant period, not repeatedly chase short-window slope noise.

### Review
- Root cause confirmed:
  - The residual plot was still computing its x-domain with `Math.max(timingView.nowUs, latestBurstUs)`. If the timing websocket clock was ahead of the retained residual payload, `windowStartUs` advanced and clipped older retained rows even though the axis showed the longer requested horizon.
  - The refined Go sync solver still let the short-window residual-slope fit move the operational period. That is the wrong authority model for a hardware-constant radar when the DF alignment path already has an established dominant period.
- Implemented:
  - `RadarPage.jsx` now anchors the residual plot domain to the latest retained residual payload timestamp (burst or backend DF11 residual), falling back to the timing clock only when no residual payload exists.
  - Backend DF11 residual dots are filtered to the same residual payload window as burst residuals.
  - `MultiSyncSolver` now selects the DF-derived dominant period as the active family prior whenever it is available, not only in recovery.
  - When a dominant period exists outside recovery, the solver locks the operational candidate/published period to that dominant period and keeps short-window slope as diagnostics.
  - Added `TestMultiSyncSolver_DominantPeriodLocksOperationalPeriod`.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(DominantPeriodLocksOperationalPeriod|UsesDominantPriorDuringRecovery|BasicFit|AuthoritativePeriodHasStrongInertia)' -count=1` -> passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` -> passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `175 passed`
  - `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-23 Refined Period Convergence Correction

- [x] Record operator correction: DF alignment period is an approximate prior influenced by spurious transmissions; refined sync must gently correct it over time.
- [x] Re-audit refined Go period path for places where it locks to DF or derives authority from the short fit window.
- [x] Replace the incorrect DF-period lock with a damped retained-window refinement path.
- [x] Extend residual-dot source retention so a 300s plot can actually contain 300s of data.
- [x] Add focused tests for DF-prior refinement, retained residual source horizon, and stable long-window period convergence.
- [x] Re-run Go/backend/frontend verification and document results.

Plan confirmation:
- The DF alignment period seeds the refined model but must not be treated as authoritative truth.
- Short fit/update windows remain short for local phase/eligibility work.
- Period correction must come from retained, motion/propagation-corrected evidence and be damped into candidate/authoritative state.
- UI residual history must be backed by retained source data, not just a longer visual axis.

### Review
- Correction accepted: the previous DF-period lock was wrong because it treated the Stage 1/DF dominant period as final truth. The refined model now uses DF as a seed/fallback, then prefers candidate/authoritative/trusted refined state once available.
- Period correction now uses a retained-window residual slope estimator across the Go observation retention horizon. It centers residuals per ICAO before fitting the common slope, so per-aircraft phase offsets do not dominate the hardware-period correction.
- The retained-window slope has a lower deadband than the old short-window slope gate, so small but persistent period errors can converge instead of being permanently ignored.
- Short fit window remains short and local; it still drives fit eligibility, phase/anchor construction, and diagnostics.
- The residual-dot source now has a separate 360s retained DF11 event buffer; the raw 60s `_iid_events` bootstrap buffer remains short. Go evidence export retention was also raised to 360s to match the diagnostic horizon.

Verification:
- `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -count=1` -> passed
- `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'df11_residual_dots_use_retained_residual_event_history or go_sync_diagnostic_history_is_retained_beyond_fit_window or go_alignment_rows_use_retained_display_history_not_fit_window'` -> `3 passed`
- `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol ./export -count=1` -> passed
- `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py tests/test_radar_core_protocol.py -q` -> `176 passed`
- `cd frontend && npm run build` -> passed; Vite reported the existing chunk-size warning.

## 2026-04-24 Robust Go Refined Multi-Aircraft Sync

- [x] Review lessons and current multisync / protocol / frontend paths before editing.
- [ ] Preserve the role split: DF alignment owns dominant period/family; Go multisync uses `DominantPeriodS` as the family authority and applies only bounded correction.
- [ ] Replace residual-gated period fitting with per-ICAO unwrapped retained residual-slope fitting, with diagnostics for slope-fit accepted/rejected vs phase-anchor rejected observations.
- [ ] Split period recovery from phase anchoring: run bounded period correction before anchor selection, and block anchor selection until the unwrapped slope gate is stable.
- [ ] Add authority promotion/demotion gates based on sustained retained residual slope health and expose slope window/gate/block diagnostics.
- [ ] Enforce dominant-prior bounds in normal refinement, recovery candidate publication, and authoritative state updates.
- [ ] Improve anchor scoring so support/persistence/validator agreement dominate over two-observation tight spreads.
- [ ] Populate per-aircraft anchor delta for Go-derived implied-offset rows and keep frontend rendering explicit when no anchor exists.
- [ ] Mark residual correction provenance, including current raw/propagation/motion/waveform status, and support degraded motion-guard correction.
- [ ] Add focused Go/backend/frontend tests for wrapped slope recovery, no-anchor period recovery, slope-blocked promotion, anchor delta output, dominant bounds, and anchor selection support.
- [ ] Run Go/backend/frontend verification, document results here, and commit the completed change.

Plan confirmation:
- Keep `rotation.go` focused on dominant-period/family detection; no family switching in multisync.
- Make period slope fitting use retained, propagation-corrected residual evidence that is unwrapped within each ICAO before fitting.
- Treat waveform/motion limitations as explicit diagnostics and conservative gains, not blockers for obvious bounded period recovery.
- Keep phase/anchor validation stricter than period recovery so near-wrap evidence can correct period without being promoted as absolute phase.

## 2026-04-25 Persistent Long-Term Sync Estimator (Refined Model Redesign)

Problem statement: the refined sync model behaves as a rolling 30s fitter — each
short fit window is effectively the entire memory of the model, so its
period/phase wobbles and recovery restarts every window even when evidence is
broadly consistent. Required change: separate short-term measurement from
long-term estimation, and have authority consume the long-term estimates rather
than the latest 30s candidate. **Non-goal**: do NOT add validator gates, branch
special cases, or threshold tuning to mask the wobble. The fix is structural.

References:
- solver entry: `radar-core/iid/multisync.go:627` `runFit`
- period stage: `radar-core/iid/multisync_period.go` (`fitPeriod`, applied period at `multisync.go:782`+)
- phase stage: `radar-core/iid/multisync_phase.go` (anchor + branch solving)
- authority: `radar-core/iid/multisync_authority.go`
- snapshot: `radar-core/iid/multisync_snapshot.go` + protobuf `protocol/`
- horizons: `backend/radar/sweep.py:2833` `_fit_window_s_for_period`, `:2841` `_sync_horizons_payload`, `_SYNC_DISPLAY_HISTORY_WINDOW_S`
- UI: `frontend/src/pages/RadarPage.jsx:2695` (Fit/Display window labels), `PhaseAnchorPanel`, sync history chart

### Plan

- [ ] Read recent lessons and confirm scope — do NOT touch validator gates, anchor scoring, or recovery thresholds in this task.

**Layer 1 — Short-term measurement (existing fit window, made noise-tolerant):**
- [ ] Inside `fitPeriod` and `solvePhaseBranch`, treat the current 30s fit as a *measurement only*: outputs are `local_period_measurement_s`, `local_residual_slope_deg_s`, `local_candidate_anchor`, `local_branch_offset_deg`, `local_validator_agreement`, `local_fit_quality`. No applied state mutation here beyond observation buffers and ICAO quality.
- [ ] Stop overwriting `AppliedPeriodS` directly with the latest fit slope-correction; the applied period now comes from layer 2.

**Layer 2 — Persistent long-term period estimator:**
- [ ] Add `MultiSyncSolver` fields: `LongTermPeriodEstimateS`, `LongTermPeriodEstimatorConfidence`, `LongTermPeriodEstimatorAgeS`, `ConsecutivePeriodConsistentWindows`, `LastPeriodUpdateDeltaS`, `LastLocalPeriodMeasurementS`. Seed `LongTermPeriodEstimateS` from `dominantPeriodS` (fallback: bootstrap/compact) on first valid window.
- [ ] Each window: nudge the estimator by a small fraction of the local measurement error (`delta = gain * (local - long_term)`), bounded around `dominantPeriodS` by `periodRefineMaxPPMFromDominant`. Gain shrinks with low local fit quality, grows with consistent windows. Never replace wholesale.
- [ ] Update `consecutive_period_consistent_windows`, decay confidence on contradictory wide-delta windows, never reset confidence from a single bad window.
- [ ] Trust promotion / `TrustedBasePeriodS` now derives from `LongTermPeriodEstimatorConfidence`, not raw streaks of fit-pool counts.

**Layer 3 — Persistent long-term phase / branch estimator:**
- [ ] New struct `BranchEstimate { OffsetDeg; SupportingICAOs map[uint32]struct{}; Confidence; LastSeenTS; ConsecutiveSupportedWindows; ContradictedWindows }`. Solver keeps `BranchTracks []BranchEstimate` (small bounded set, e.g. ≤4), plus `LongTermBranchAnchorICAO`, `LongTermBranchOffsetDeg`, `BranchEstimatorConfidence`.
- [ ] Each window's local candidate anchor/branch is matched to existing tracks by `globalBranchMatchDeg` proximity. Match → increment `ConsecutiveSupportedWindows`, refresh `LastSeenTS`, raise confidence with diminishing returns. No match → spawn a competing track (capped); contradiction (wide phase delta with high local quality) decays the matched track's confidence rather than killing it.
- [ ] Branch promotion to `refined_authoritative` requires accumulated `BranchEstimatorConfidence ≥ promotion_threshold` AND consistent windows count, not just current window validation strength.

**Layer 4 — Authority decision rewrite:**
- [ ] `updateAuthorityModePostFit` and `updateAuthoritativeState` consume `LongTermPeriodEstimateS` + `LongTermBranchOffsetDeg` (with current-window sanity checks: validator-not-strongly-disagreeing, slope-gate, dominant bound). Short-window candidate is no longer the applied authority unless we are explicitly bootstrapping (compact mode → first refined entry).
- [ ] Add `LastAuthorityPromotionBlockReason` values: `branch_confidence_insufficient`, `period_estimator_confidence_insufficient`, `branch_competitor_dominant`.

**Layer 5 — Display vs fit window separation (the chart bug):**
- [ ] Introduce explicit `display_window_s` (default 300s, settable) distinct from `fit_window_s`. In `runFit`, retain `MultiSyncSolver.obs` and slope/residual history sized by `display_window_s`; the solver fits against a subset trimmed to `fit_window_s`.
- [ ] `_sync_horizons_payload` already exposes both — fix any callsite that writes the fit window into the display field. Ensure `_SYNC_DISPLAY_HISTORY_WINDOW_S` is the retention horizon for slope/residual history rows shipped to the UI.
- [ ] Snapshot/protobuf: add `local_period_measurement_s`, `long_term_period_estimate_s`, `period_update_delta_s`, `period_estimator_confidence`, `period_estimator_age_s`, `consecutive_period_consistent_windows`, `local_candidate_anchor`, `long_term_branch_anchor`, `branch_confidence`, `branch_consistent_windows`, `branch_contradiction_windows`, `branch_competitor_count`, `branch_promotion_block_reason`. Plumb through `protocol/` Go ↔ Python.

**Layer 6 — UI:**
- [ ] Sync history chart honors `display_window_s` (300s) regardless of `fit_window_s`.
- [ ] Plot/label both: short-window fit residuals vs long-term applied residuals; local measured period vs long-term estimated period.
- [ ] PhaseAnchorPanel/multisync diagnostic panel surfaces local vs long-term anchor, branch confidence, and the new block reasons.

**Layer 7 — Tests (each must be failing-then-passing on the redesigned model):**
- [ ] A. `multisync_long_term_period_test.go`: feed noisy zero-mean local period measurements; assert `LongTermPeriodEstimateS` variance < input variance / 4 and converges toward truth.
- [ ] B. Same file: stable branch repeated across N windows → `BranchEstimatorConfidence` rises monotonically and eventually promotes to `refined_authoritative`.
- [ ] C. Single bad 30s window injected mid-stream → `LongTermBranchOffsetDeg` and `BranchTracks[0].Confidence` survive (no reset).
- [ ] D. Sustained contradictory branch (e.g. 6 windows agreeing on a different offset) → original track demotes, new track takes over.
- [ ] E. Backend `test_radar_sweep.py`: setting `fit_window_s` env/config knob does not change `display_window_s` retention.
- [ ] F. Backend test: with `display_window_s=300`, `fit_window_s=30`, snapshot history rows span ≥300s while solver `LastFitSpanS ≤ 30s`.

### Verification
- [ ] `go test ./iid -run 'TestMultiSync_LongTerm' -count=1`
- [ ] `go test ./iid ./cmd/radar-core ./protocol ./export -count=1`
- [ ] `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_core_protocol.py -q`
- [ ] `go build -o radar-core ./cmd/radar-core`
- [ ] `cd frontend && npm run build`
- [ ] Manually verify chart shows 300s history with fit_window_s=30s.

Plan confirmation points (please confirm before I start):
1. Layer separation as specified — local measurement state is read-only for layer 2/3; only layer 2/3 mutate applied/authoritative period and branch.
2. Long-term period estimator is a bounded EMA-style nudge, NOT a separate Kalman or independent free-running estimator (still anchored to dominant prior).
3. `BranchTracks` is a small bounded set (≤4), not unbounded history. OK?
4. Bootstrap exception: on first entry from compact → refined, the short-window candidate IS allowed to seed long-term state directly. After that, long-term wins.
5. Display window default 300s, exposed but not user-configurable in this task (UI control deferred).
6. Out of scope here: validator threshold tuning, anchor scoring changes, recovery threshold edits, waveform correction.
