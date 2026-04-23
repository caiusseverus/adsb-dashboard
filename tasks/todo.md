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
