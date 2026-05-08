# Compact Recomputed Go Sync Fit-Reject Relabel Analysis (2026-05-09)

Source capture:
- `tasks/radar_sync_baseline/sync_capture_all_daytime_high_traffic_lock_stability_20260508T201419Z.ndjson`

Scope:
- Diagnostic-only relabeling for compact recomputed rows.
- No eligibility/threshold/policy behavior changes.

## Post-change ordered primary reasons (recomputed rows, offline replay on capture)
Population:
- visible recomputed rows: 196,856
- sync_update_eligible=true: 75,622
- fit_eligible=false among sync_update_eligible: 37,745 (49.91% of sync_update_eligible; 19.17% of visible)

Primary reason counts (ordered):
- `residual_gate`: 25,496 (33.72% of sync_update_eligible; 67.55% of collapse)
- `near_wrap_residual`: 11,790 (15.59% of sync_update_eligible; 31.24% of collapse)
- `stale_position`: 345 (0.46% of sync_update_eligible; 0.91% of collapse)
- `missing_geometry`: 114 (0.15% of sync_update_eligible; 0.30% of collapse)

`compact_go_sync` is no longer required as the dominant label in this ordered scheme.

## Secondary flag counts (same collapse population)
- `stale_position`: 31,359 (41.47% of sync_update_eligible)
- `residual_gate_failed`: 25,496 (33.72%)
- `near_wrap_residual`: 11,790 (15.59%)
- `missing_geometry`: 459 (0.61%)
- `zero_weight`: 0
- `classification_rejected`: 37,745 (49.91%)

## Overlap matrix (stale_position, residual_gate_failed, near_wrap_residual)
Counts among collapse rows:
- `(True, True, False)`: 21,118
- `(True, False, True)`: 9,896
- `(False, True, False)`: 4,378
- `(False, False, True)`: 1,894
- `(True, False, False)`: 345
- `(False, False, False)`: 114

## Consistency checks
1. `sync_update_eligible=true` implies dominant-family equivalent:
   - compact emitter now exports `dominant_family` as sync-update eligibility equivalent for recomputed rows.
2. `fit_eligible=false` has exactly one primary reason:
   - yes, ordered single-string `fit_reject_reason`.
3. `zero_weight` measurement:
   - emitted and measurable on recomputed rows; count is 0 in this capture.
4. Residual threshold basis:
   - uses the same compact predicted/residual basis already used for plotted residual fields.
5. `stale_position` timestamp basis:
   - derived from observation `pos_age_s` (aircraft position age), not chart/event time.
6. Double-counting `residual_gate` and `zero_weight`:
   - primary reason is ordered single label; secondary flags may overlap by design.
7. Fit counters vs admitted rows:
   - no behavior changes; relabeling only.

