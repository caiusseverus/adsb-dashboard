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
