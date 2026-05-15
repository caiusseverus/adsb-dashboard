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
