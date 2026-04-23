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
