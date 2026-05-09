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
