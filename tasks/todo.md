# Live DF11 refined-vs-base verification monitor

## Plan
- [x] Add diagnostics-only DF11 verification state, confidence, and blocker fields to live `sync_state` serialization from same-event base/refined DF11 rows.
- [x] Preserve all DF11 verification fields in compact sync snapshot extraction.
- [x] Add an offline report/simulation tool for live compact rows covering distributions, false-active cases, safe refined cases, and shadow enforcement simulation.
- [x] Run targeted tests for the new state/confidence/blocker logic and compact extraction.
- [x] Run a 30-60 minute live capture for active/top IIDs if localhost backend is reachable.
- [x] Run the live report on the capture and summarize results.
- [x] Run GitNexus change detection and document verification/recommendation.

## Constraints
- No live authority selection changes.
- No promotion policy changes.
- No threshold changes.
- No Stage 10 policy changes.
- No Stage 8 contamination policy changes.
- No holdover/reacquire policy changes.
- No slope gate changes.
- No residual gate changes.
- No localiser behavior changes.
- No geographic calibration changes.
- No chart rendering changes.
- Capture artifacts are analysis inputs only and must not be committed.

## Review
- Implemented diagnostics-only DF11 verification fields in `backend/radar/sweep_diagnostics.py`: state, confidence, and hypothetical blocker.
- Wired the same-event base/refined DF11 shadow comparison into compact live `sync_state` snapshots in `backend/radar/sweep.py`; this only serializes diagnostics and does not change authority selection.
- Updated `tools/blocker_baseline_compact.py` to retain the monitor fields from live snapshots.
- Added `tools/df11_verification_live_report.py` for per-IID distributions, false-active cases, safe refined cases, and offline shadow enforcement simulation.
- Added backend tests for the compact snapshot monitor path, helper classification, and compact extraction.
- Validation captures:
  - Initial port 8000 capture: `tasks/radar_sync_baseline/blocker_baseline_compact_df11_verify_live_20260523T234206Z_rows.jsonl`, 8610 rows, 0 fails; invalid for monitor behavior because the running backend did not have the new fields loaded.
  - Port 8001 updated backend before radar-core connection: `tasks/radar_sync_baseline/blocker_baseline_compact_df11_verify_live_p8001_20260524T001734Z_rows.jsonl`, 10800 rows, 0 fails; monitor fields emitted but all unavailable because radar-core was not connected and Python frame building was suppressed.
  - Final valid capture with updated backend plus radar-core: `tasks/radar_sync_baseline/blocker_baseline_compact_df11_verify_live_p8001_core_20260524T005449Z_rows.jsonl`, 10800 rows, 0 fails.
- Final report: `tasks/radar_sync_baseline/df11_verify_live_p8001_core_20260524T005449Z_report.json`.
- Final live result: 941 active Go-refined rows; shadow rule would retain 189 and suppress 752. It would avoid 582 rows of active refined where DF11 says refined is worse than base, with only 1 good row suppressed due insufficient data.
- Key IIDs in the final report:
  - IID 20: active refined dwell 315.0s; active refined bad dwell 185.0s; verification available 86.9%; refined_worse dwell 845.0s; base on-time median 13 vs refined 2; alignment delta p50 +1.50 and p95 +93.38.
  - IID 21: active refined dwell 55.0s; active refined bad dwell 25.0s; verification available 77.8%; refined_worse dwell 775.0s; alignment delta p50 +3.45 and p95 +88.04.
  - IID 33: active refined dwell 110.0s; active refined bad dwell 65.0s; verification available 74.7%; refined_worse dwell 520.0s; alignment delta p50 +0.11 and p95 +68.89.
  - IID 3: active refined dwell 365.0s; active refined bad dwell 240.0s despite some safe refined rows; verification available 94.7%; alignment delta p50 +0.93 and p95 +91.91.
- Recommendation: add a live DF11 verification gate before Go runtime promotion, or at minimum soft-degrade/suppress Go runtime authority when `df11_verification_state=refined_worse`. The captured data does not support accepting Go refined runtime based on internal readiness alone.
