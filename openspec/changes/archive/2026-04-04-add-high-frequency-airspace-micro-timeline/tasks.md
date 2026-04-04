## 1. Backend event model

- [x] 1.1 Extend the recent-message timing event model to include inline `icao` while preserving the current Beast-time semantics and bounded retention model.
- [x] 1.2 Update the Timing-page and direct timing endpoint serialization so widened events remain self-contained and compatible with the existing single-socket page architecture.
- [x] 1.3 Verify that `icao` is attached at message ingest time rather than reconstructed later from mutable live-aircraft state.

## 2. Timing page micro-timeline

- [x] 2.1 Extend the Timing-page client buffer to consume the widened high-frequency event shape with `icao`.
- [x] 2.2 Add a `Micro-timeline of airspace activity` panel to `frontend/src/pages/TimingPage.jsx` that renders recent per-aircraft ticks from the shared event buffer.
- [x] 2.3 Implement and tune a stable top-`N` row policy using a slower ranking cadence and sticky membership so the panel remains readable under bursty traffic.
- [x] 2.4 Choose and implement the first-pass visual semantics for row coloring and density without introducing new event fields beyond `icao`.

## 3. Verification

- [x] 3.1 Verify the touched backend high-frequency modules with focused syntax/tests.
- [x] 3.2 Run `npm run build` in `frontend/` and confirm the Timing page still builds and lays out sensibly with the additional panel.
- [x] 3.3 Perform a focused live or synthetic runtime check that covers idle state, multiple active aircraft, and row-stability behavior from one shared Timing-page stream.
