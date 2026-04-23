## 2026-04-23 Burst Sync Long-Term Authoritative Estimator

- [x] Inspect the current Go refined solver flow and confirm where short-horizon candidate period/phase/anchor estimates are still published directly as live state.
- [x] Refactor the Go multi-sync solver to maintain explicit candidate versus authoritative sync state with strong long-term inertia for authoritative period and phase.
- [x] Make recovery entry relatively quick but authoritative promotion/demotion slow, validator-backed, and branch-aware.
- [x] Export additive candidate-versus-authoritative diagnostics through the Go protocol and Python payload shaping so the UI can distinguish tentative from trusted state.
- [x] Add focused Go/backend tests for stable authoritative period damping, weak-phase period freeze, wrong-branch non-promotion, recovery-then-slow-settlement, and no rapid state flapping.
- [x] Run verification and record exact commands/results in the review section.

### Review
- Root cause confirmed:
  - `radar-core/iid/multisync.go` still computed a fast local period/alignment estimate and then published it directly into `PeriodS`, `PhaseOffsetDeg`, and `AnchorICAO` in the same solver pass.
  - The existing `TrustedBasePeriodS` only damped the clamp reference. It did not create a separate slow authoritative state, so the published sync still behaved like a short-horizon tracker.
  - Anchor selection and recovery authority had hysteresis, but absolute phase/period promotion still lacked a validator-backed long-term settlement path.
- Implemented:
  - The Go solver now carries explicit candidate and authoritative state for period, phase, and anchor. The fast layer updates candidate state every run; the published state prefers the slow authoritative state when it exists.
  - Added validator-backed phase scoring using branch ambiguity, selected-anchor circular dispersion, and independent validator ICAO agreement/disagreement counts. Weak or ambiguous phase validation freezes authoritative period updates.
  - Authoritative period and phase now update with explicit low gains and capped per-update drift, and only after sustained candidate promotion streaks. Recovery can still move candidate state quickly, but authoritative settlement remains slow.
  - Added additive diagnostics for candidate versus authoritative state, promotion streaks, update gains, frozen-period state, branch ambiguity, circular dispersion, validator counts, and candidate/authoritative modes.
  - Exported those fields through `radar-core/protocol/messages.go`, `radar-core/cmd/radar-core/main.go`, and `backend/radar/sweep.py`.
- Audited files with no direct code change needed:
  - `backend/radar/api.py` already forwards the backend snapshot/debug payloads built in `backend/radar/sweep.py`.
  - `backend/radar_core/client.py` continues to pass through additive protocol fields without shape-specific changes.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(AuthoritativePeriodHasStrongInertia|WeakPhaseFreezesAuthoritativePeriod|WrongPhaseBranchNotPromotedQuickly|ValidatorBackedPromotionInitializesAuthoritativeState|RecoveryThenSlowSettlement|SelectAnchorKeepsCurrentOnSmallScoreDelta|SelectAnchorSwitchesWhenChallengerMateriallyBetter|UsesDominantPriorDuringRecovery|AuthorityModeHysteresis)|TestEvalCandidatePeriod_PreservesAnchorCandidates' -count=1` → passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'update_go_multi_sync_state_overrides_python_multi_aircraft_burst or go_multi_sync_mode_diagnostics_report_dominant_recovery_fields'` → `2 passed, 87 deselected`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py -q` → `147 passed`

## 2026-04-23 Burst Sync Panel Stable Ordering

- [x] Inspect the current frontend ordering of anchor candidates and implied-offset rows and confirm why they jump around.
- [x] Replace volatile score/time ordering with a more stable deterministic ordering that still keeps the selected anchor prominent.
- [x] Ensure the implied-offset panel is actually grouped per aircraft instead of resorting raw observations every refresh.
- [x] Run frontend verification and record the exact result.

### Review
- Root cause confirmed:
  - The anchor candidate table was effectively rendered in live backend score order, so small score changes could reshuffle the whole list on every poll.
  - The “per-aircraft implied offsets” table was not actually per aircraft. It rendered the last 20 observations in reverse time order, so rows constantly moved as new observations arrived.
- Implemented:
  - `frontend/src/pages/RadarPage.jsx` now sorts anchor candidates deterministically: selected anchor first, then by candidate status, then by ICAO.
  - The implied-offset table now collapses to the latest observation per ICAO and sorts rows stably with the selected anchor first, then candidate status, then ICAO.
- Verification:
  - `cd frontend && npm run build` → passed

## 2026-04-23 Burst Sync Anchor Stability

- [x] Inspect the Go refined anchor-selection path and confirm why selected anchors are churning.
- [x] Add explicit anchor hysteresis / lockout so close-score challengers do not replace a usable anchor every update.
- [x] Expose anchor-switch diagnostics if needed to explain why the solver kept or changed the anchor.
- [x] Add regression tests covering stable-anchor retention and anchor replacement only on materially better or clearly degraded evidence.
- [x] Run focused Go tests and any broader verification needed for the burst sync path.

### Review
- Root cause confirmed:
  - The prior source-authority hysteresis fixed compact vs refined mode switching, but Go anchor selection itself still had no hysteresis. `selectAnchor()` simply picked the highest-scoring aircraft on each run, so small score oscillations between otherwise-healthy candidates could churn the selected anchor even while the period family stayed stable.
  - The anchor selector is also used by candidate-period evaluation during wrong-period recovery. Any stickiness applied there would have been incorrect because those candidate evaluations are exploratory, not published state.
- Implemented:
  - `radar-core/iid/multisync.go` now applies anchor hysteresis only on the published alignment path. The solver keeps the current anchor through small score deltas and during an initial dwell window, and only switches when the current anchor is degraded or a challenger is materially better.
  - The solver now exports anchor-switch diagnostics (`anchor_switch_count`, `last_anchor_switch_ts`, `last_anchor_switch_reason`, `anchor_hold_updates`) through the Go snapshot/protocol path and the Python `LiveSyncState`/debug payloads.
  - Candidate-period evaluation still preserves candidate rows, but it no longer mutates anchor-switch state while exploring non-published candidate families.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(SelectAnchorKeepsCurrentOnSmallScoreDelta|SelectAnchorSwitchesWhenChallengerMateriallyBetter|UsesDominantPriorDuringRecovery|AuthorityModeHysteresis)|TestEvalCandidatePeriod_PreservesAnchorCandidates' -count=1` → passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'update_go_multi_sync_state_overrides_python_multi_aircraft_burst or go_multi_sync_mode_diagnostics_report_dominant_recovery_fields'` → `2 passed, 87 deselected`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py -q` → `147 passed`

## 2026-04-23 Burst Sync Authority Hysteresis And Refined Row Diagnostics

- [x] Trace the current compact/refined authority handoff end to end, including Go solver mode selection and Python adoption of compact `IID_STATE` vs Go multi-sync state
- [x] Refactor `radar-core/iid/multisync.go` to carry explicit authority mode with hysteresis/lockout so compact, dominant-recovery, and refined-authoritative decisions do not flap run-to-run
- [x] Export authority-mode diagnostics through Go protocol and Python normalisation, including switch count/reason/timestamp and any enter/exit streaks used for hysteresis
- [x] Fix the Go recovery candidate path so selected-anchor candidate metadata survives end to end instead of dropping the candidate list when a reacquire candidate wins
- [x] Audit and fix the refined row payload path so Go refined sync emits meaningful implied-offset / anchor-relative row fields, with explicit nulls where data is unavailable rather than misleading zeros
- [x] Update frontend burst-sync rendering to show authority mode clearly, preserve refined candidate rows, and never coerce missing implied offsets to 0
- [x] Add focused Go, backend, and frontend-compatible tests for authority hysteresis, Go refined candidate export, refined implied-offset row payloads, and bridge overwrite prevention
- [x] Run verification and capture the exact commands/results in the review below

Plan confirmation:
- Hot-path sync fitting and authority logic stay in Go. Python remains the bridge and diagnostics layer, but it must stop overwriting a Go refined state with compact sync traffic.
- The UI fix will prefer explicit degraded/null states over fabricated zero values so the panel stops implying a valid refined anchor row when the data is absent.

### Review
- Root causes confirmed:
  - Authority flapping was not just threshold noise in `multisync.go`. The Go solver had no explicit sticky authority mode, and Python’s `_adopt_go_frame_sync_locked()` still allowed compact `IID_STATE` updates to overwrite `go_multi_aircraft_burst`, so compact and refined ownership could bounce at the bridge even when Go multi-sync was present.
  - The selected-anchor / empty-candidates bug came from the Go recovery path: `runFit()` could publish the winning candidate’s anchor ICAO/score/phase but dropped the candidate list/count metadata when the reacquire search selected that candidate family.
  - The `0` implied offsets were a payload/rendering bug: the Go-refined timeline still used the compact row builder, which left `implied_phase_offset_deg` unset, and the frontend’s `Number(...)` coercion turned `null` into `0`.
- Implemented:
  - `radar-core/iid/multisync.go` now carries an explicit authority mode (`compact_authoritative`, `dominant_recovery`, `refined_authoritative`) with hysteresis streaks and switch diagnostics, and publishes those fields through `MultiSyncSnapshot`.
  - The Go recovery candidate path now preserves `anchorCandidateCount`, `anchorNoCandidateReason`, and `anchorCandidates` when a searched candidate family is published.
  - `radar-core/protocol/messages.go` and `radar-core/cmd/radar-core/main.go` now export authority-mode diagnostics and the preserved candidate metadata.
  - `backend/radar/sweep.py` now:
    - mirrors authority diagnostics into `LiveSyncState`
    - stops compact Go sync adoption from overwriting `go_multi_aircraft_burst`
    - marks Go multi-sync period authority as `base` unless authority mode is truly refined-authoritative
    - computes implied phase offsets / anchor-relative errors / candidate-linked row reasons for Go-refined timeline rows instead of leaving placeholder nulls
  - `frontend/src/pages/RadarPage.jsx` now shows authority mode/switch info and no longer coerces missing implied offsets to `0`.
- Audited files with no code change needed:
  - `backend/radar/api.py` already forwards the snapshot/state payloads built in `backend/radar/sweep.py`, so no direct code change was required there.
  - `backend/radar_core/client.py` forwards protocol payloads generically and did not need a shape-specific patch for these additive fields.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'Test(MultiSyncSolver_(UsesDominantPriorDuringRecovery|AuthorityModeHysteresis)|EvalCandidatePeriod_PreservesAnchorCandidates|MultiSyncSolver_HealthyCompactAssessmentStaysOutOfRecovery|MultiSyncSolver_RecoveryAdmissionRelaxesPositionAgeGate)' -count=1` → passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'go_iid_state_does_not_overwrite_go_multi_sync_state or go_refined_timeline_includes_implied_phase_offsets_without_zero_fallback or update_go_multi_sync_state_overrides_python_multi_aircraft_burst or go_multi_sync_mode_diagnostics_report_dominant_recovery_fields'` → `4 passed, 85 deselected`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py -q` → `147 passed`
  - `cd frontend && npm run build` → passed

## 2026-04-23 Dominant-Period-Led Refined Sync Recovery

- [x] Trace the current compact/bootstrap, dominant rotation, and refined multi-sync dependency chain in Go/Python and confirm where compact sync still seeds, clamps, and hard-gates recovery
- [x] Refactor `radar-core/iid/multisync.go` so the solver carries separate compact seed, dominant prior, trusted refined period, and explicit recovery-mode state, with dominant period as the primary family prior during recovery
- [x] Relax or bypass compact-sync mismatch gating during recovery so the refined fit pool and anchor search can rebuild from dominant-family observations instead of staying starved behind `compact_go_sync`
- [x] Add explicit failure detection that compares compact/refined sync against the dominant rotation period and enters recovery mode only when dominant divergence combines with other bad-sync signals
- [x] Export additive diagnostics through Go protocol emission and Python snapshot shaping for dominant period, active family prior, period delta to dominant, recovery mode, trigger reasons, compact gating bypass, and recovery-admitted observations
- [x] Update any API/debug payloads needed by the radar UI so operators can see whether the solver is following compact bootstrap, trusted refined state, or dominant-period-led recovery
- [x] Add focused Go and backend tests for wrong-compact/correct-dominant recovery, healthy compact normal mode, recovery admission, anchor recovery, and diagnostics payload semantics
- [x] Run verification, capture exact commands/results, and record a review summary below

Plan confirmation:
- The redesign stays in the Go hot path. Python remains a consumer/normaliser of exported solver state and diagnostics.
- Compact/sweep-frame sync remains a useful signal, but it stops being the authoritative recovery foundation when dominant rotation evidence is healthier.
- The change should be incremental and reviewable: explicit new state/diagnostics first, then admission/recovery logic, then payload/UI shaping, then focused verification.

### Review
- Root cause confirmed:
  - `radar-core/iid/multisync.go` still chose its live fit family from the current refined period if present, else compact sync, and captured bootstrap from compact sync unless refined state already existed. That meant the fit window, clamp base, and candidate search all inherited the compact family by default.
  - Recovery was expressed only as wrong-period reacquire inside the same solver, not as an explicit “dominant-family-led recovery mode”. The solver had no first-class dominant-period prior from the live DF alignment / rotation model.
  - Fit eligibility itself was not keyed on an explicit compact gate in Go, but the wrong compact family still starved the fit pool indirectly because residuals were scored against the wrong period family, which then tripped residual rejection and left anchors empty.
- Implemented:
  - `radar-core/iid/state.go` now exposes the current dominant rotation period to Stage 3 via `DominantPeriodSnapshot()`, and `radar-core/cmd/radar-core/main.go` passes that period into the Go multi-sync solver on each update.
  - `radar-core/iid/multisync.go` now chooses an explicit active family prior per run, tracks dominant prior vs compact period vs trusted refined base separately, and enters recovery mode when dominant-period divergence combines with bad-sync signals such as holdover, residual EMA, fit starvation, missing anchors, or repeated failures.
  - In recovery mode the solver now recentres on the dominant live DF period, uses it as the active family prior/clamp basis, widens admission enough to rebuild anchors, and counts observations admitted only because recovery relaxed the gates.
  - Protocol/export payloads now carry dominant prior, active family prior, compact-vs-dominant delta, refined-vs-dominant delta, compact unreliability, recovery mode, trigger reasons, compact-gating bypass, and relaxed-admission counts.
  - Python normalisation and sync-mode diagnostics now surface those fields through `LiveSyncState`, burst-sync diagnostics, and API/debug snapshots.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid -run 'TestMultiSyncSolver_(UsesDominantPriorDuringRecovery|HealthyCompactStaysOutOfRecovery|RecoveryRelaxesAdmission|BasicFit|DiagnosticsExposedInSnapshot)' -count=1` → passed
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol -count=1` → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q -k 'go_multi_sync_state or sync_mode_diagnostics_report_dominant_recovery_fields'` → `3 passed, 84 deselected`
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py -q` → `145 passed`

## 2026-04-23 Burst Sync Mode Clarity And Anchor Diagnostics

- [x] Trace the current burst sync alignment payload path end to end and document what actually drives the chart, anchor list, and existing status fields
- [x] Extend the Go export path and Python normalisation so compact/bootstrap sync and refined multi-aircraft sync are explicitly distinguishable in live payloads
- [x] Expose compact reference churn diagnostics, including current reference ICAO, previous reference ICAO, recent change indicators, and recent sync/reset transitions where available
- [x] Expose refined multi-sync diagnostics, including presence/usability, fit totals, fit-eligible counts, contributing ICAO counts, anchor candidate counts, admission diagnostics, and explicit no-anchor reasons
- [x] Update the burst sync panel UI so compact/bootstrap mode is clearly labelled, compact churn/reset events are visible, and the anchor section explains why it is empty when refined sync is absent or not admitted
- [x] Add or update focused tests for the new backend payload fields and compact/refined rendering semantics, run verification, and capture example compact/refined payloads in the review section below

Plan confirmation:
- The chart will keep using the real backend sync source; the fix is to export and present the source and diagnostics explicitly rather than guessing from chart motion.
- Hot-path authority remains in Go for compact sync and Go multi-sync. Python will only mirror, normalise, and explain the exported state.
- Thresholds will not be relaxed speculatively. Candidate/admission diagnostics will be exposed first so any gating issue is evidenced before tuning.

### Review
- Root cause confirmed:
  - The burst-sync chart can already be driven from Go-retained evidence while the phase-anchor panel still reads refined-only fields. In practice that let compact/bootstrap `sweep_frame_go` behaviour look like refined-anchor instability whenever refined sync was absent or thin.
  - Even when Go refined sync was active, Python still routed it through the compact Go diagnostics path, so the mode boundary was not explicit.
- Implemented:
  - Go multi-sync payloads now export fit totals, fit-eligible/rejected counts, contributing ICAO count, fit rejection reasons, anchor candidate count, no-candidate reason, and per-candidate rows.
  - Python now mirrors those refined diagnostics into `LiveSyncState`, tracks compact reference/epoch/reset transitions for Go sweep-frame sync, and exposes a unified `sync_mode_diagnostics` object in the live sync payload.
  - The Radar burst-sync panel now shows an explicit sync-source status section, compact reference churn/reset diagnostics, and a refined-anchor panel that explains inactivity instead of showing a blank list.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol` → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py -q` → `86 passed`
  - `cd frontend && npm run build` → passed

## 2026-04-23 Final-Period Trust And Eligibility Semantics Fixes

- [x] Inspect the remaining `finalPeriodS` vs `refinedPeriodS` trust-promotion and EMA leak in `radar-core/iid/multisync.go`, plus the current exported eligibility field meanings end to end
- [x] Fix the Go hot path so trusted-base promotion and any same-run trusted-base EMA updates always use the final published family/period, including reacquire candidate selection paths
- [x] Make Stage 3 export semantics explicit and consistent across `radar-core/cmd/radar-core/main.go`, `backend/radar/sweep.py`, `backend/radar/api.py`, and `backend/radar_core/client.py` so compact/bootstrap and authoritative refined eligibility are clearly separated
- [x] Add or update focused Go and backend tests for final-period trust promotion, trusted-base EMA updates, and explicit sync-eligibility semantics
- [x] Run focused verification, then record outcomes and any compatibility notes in the review section below

Plan confirmation:
- The fix will stay in the Go solver hot path for period-family authority. Python will only reshape and consume exported state; it will not retake authoritative refined-sync ownership.
- Exported eligibility semantics will be explicit: compact/bootstrap admission remains separate from authoritative refined-sync presence/usability, with any legacy alias either preserved as such or cleanly redefined everywhere.

### Review
- Root causes fixed:
  - `TrustedBasePeriodS` promotion and same-base EMA updates now consume the solver's published `finalPeriodS`, not the intermediate `refinedPeriodS`, so trusted base cannot drift toward a family that was not actually published.
  - The trusted-base update path is isolated in a small Go helper with a focused regression test for published-period EMA behavior.
  - Exported Stage 3 `sync_eligible` on burst/evidence/track objects now means authoritative refined-sync usability; compact/bootstrap admission remains explicitly available as `compact_sync_eligible`.
  - Python Go-payload normalisation now prefers explicit refined fields when present and only falls back to legacy compact-only `sync_eligible` semantics for older payloads.
- Audited files with no code changes needed:
  - `backend/radar/api.py` did not need a code change because it consumes the already-normalised snapshot/state objects from `backend/radar/sweep.py`.
  - `backend/radar_core/client.py` did not need a code change because it forwards payloads generically; the field-semantic fix lives in the Go producer and Python normaliser.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./iid ./cmd/radar-core ./protocol` (from `radar-core/`) → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_api.py tests/test_radar_core_client.py -q` → `144 passed`
- Compatibility note:
  - Mixed payloads that carry both the new refined fields and a stale legacy `sync_eligible` bit now resolve in favor of the explicit refined fields.

## 2026-04-23 Refined Burst Sync Regression Fixes

- [x] Inspect current Go refined multi-sync trust, reacquire, and admission gates; confirm root causes in code and payloads
- [x] Fix `radar-core/iid/multisync.go` so trust streak semantics are truly consecutive and reacquire publishes a period/phase/anchor fit from the same candidate family
- [x] Add Go-side admission diagnostics in `radar-core/cmd/radar-core/main.go` and expose them through emitted multi-sync state / snapshot payloads so blocked refined admission is visible from the API
- [x] Update Python/API sync snapshot assembly in `backend/radar/sweep.py`, `backend/radar/api.py`, and `backend/radar_core/client.py` so `sweep_frame_go` / `go_multi_aircraft_burst` sources produce bootstrap evidence or an explicit no-refined-data reason instead of silent blanks, with consistent source labelling
- [x] Adjust frontend alignment-panel handling only if needed so bootstrap/no-refined-data diagnostics render explicitly for Go-owned sync sources
- [x] Add or update focused Go, backend, and frontend-compatible tests for trust reset, reacquire family consistency, admission diagnostics, bootstrap alignment payloads, and source-label consistency
- [x] Run verification: Go tests, relevant backend tests, and frontend build/tests if touched; record results and review notes here

Plan confirmation:
- Root causes will be fixed in the Go hot path and exported as diagnostics; Python/UI will only reshape or explain Go-owned state, not reintroduce authoritative sync fitting into Python.
- Payloads will distinguish true refined multi-aircraft sync from bootstrap/compact Go sync explicitly, so the UI can show evidence or reasons without faking refined observations.

### Review
- Root causes fixed:
  - `TrustUpdateStreak` could survive weak/non-eligible updates because only reacquire / wrong-period paths reset it.
  - Reacquire could publish a candidate period with phase/anchor terms computed against the pre-search seed family.
  - Go refined multi-sync admission failures were opaque, and bootstrap `sweep_frame_go` payloads could surface empty alignment/timeline sections without any retained-evidence explanation.
  - Bootstrap Go sync adoption inherited `LiveSyncState.period_authoritative_source="refined"` from the dataclass default even when the source remained `sweep_frame_go`.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./...` → passed
  - `uv run --directory backend pytest tests/test_radar_api.py tests/test_radar_sweep.py -q` → `130 passed`
  - `cd frontend && npm run build` → passed
- Intentional non-goals left unchanged:
  - Waveform correction and motion-compensation parity remain richer on the Python authoritative multi-aircraft path.
  - Stage 3 ownership boundaries remain unchanged; Python still reshapes/explains Go-owned sync state rather than retaking authoritative hot-path fitting.

## 2026-04-23 Multi-Sync Consistency And Export Semantics Fixes

- [x] Inspect the remaining refined multi-sync ordering, trust-promotion, and export-semantic bugs in the current Go/Python paths and confirm the concrete failure modes in code
- [x] Refactor `radar-core/iid/multisync.go` so each solver run selects the final published period before any published alignment/anchor values are built, covering both normal refinement and reacquire candidate flows
- [x] Ensure `PeriodBaseS` published by the multi-sync snapshot reflects the effective base after same-run trust promotion rather than the pre-promotion base
- [x] Make exported sync eligibility semantics explicit and internally consistent across `radar-core/cmd/radar-core/main.go`, protocol/export payloads, and Python consumers
- [x] Add or update focused tests for normal refinement alignment consistency, reacquire alignment consistency, trust-promotion base diagnostics, and exported sync eligibility semantics
- [x] Run focused verification, capture results here, and note any compatibility tradeoffs

Plan confirmation:
- The solver will publish one coherent family per run: `PeriodS`, `PhaseEpochUS`, `PhaseOffsetDeg`, anchor ICAO, and anchor phase will all be derived from the same final published period.
- Exported objects will stop overloading one ambiguous sync-eligibility bit. Compact/bootstrap gating and authoritative refined-sync usability will be exposed with explicit names and preserved consistently through Go and Python.

### Review
- Root causes fixed:
  - `runFit()` no longer computes published alignment from `livePeriodS` before final period selection. It now chooses the final published period first, then builds one final alignment package from that period, or uses the selected reacquire candidate package directly.
  - `PeriodBaseS` now reflects the post-promotion effective base for the same solver run, so the snapshot stops lagging one cycle behind `TrustedBasePeriodS`.
  - Exported burst/evidence/track objects now expose explicit compact/bootstrap vs refined sync semantics: legacy `sync_eligible` is preserved as a compact-sync alias, and new `compact_sync_eligible`, `refined_sync_present`, and `refined_sync_usable` fields make the meaning explicit.
- Verification:
  - `env GOCACHE=/tmp/go-build GOMODCACHE=/tmp/go-mod-cache go test ./...` (from `radar-core/`) → passed
  - `uv run --directory backend pytest tests/test_radar_sweep.py tests/test_radar_core_protocol.py -q` → `111 passed`
  - `uv run --directory backend pytest tests/test_radar_api.py -q` → `47 passed`
- Compatibility notes:
  - `sync_eligible` remains on the wire and in exported snapshots as a backward-compatible alias for compact/bootstrap sync admission.
  - New explicit refined-sync fields are additive and are preferred by updated Python consumers.
