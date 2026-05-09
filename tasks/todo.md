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
