## 1. Signal contract definition

- [x] 1.1 Review the current signal handling across Beast ingest, readsb ingest, shared recent timing events, and frontend consumers.
- [x] 1.2 Define one canonical display-grade signal representation aligned to readsb-style dBFS semantics.
- [x] 1.3 Decide whether raw/internal signal values remain exposed anywhere for debugging or stay backend-only.

## 2. Backend alignment

- [x] 2.1 Update the backend ingest/state pipeline so display-facing signal values are produced consistently across Beast and readsb paths.
- [x] 2.2 Update shared recent timing-event serialization and any other affected APIs to expose the canonical signal semantics needed by current and planned displays.
- [x] 2.3 Add or update focused backend tests covering representative values, extrema, and event/API shapes.

## 3. Frontend consumer alignment

- [x] 3.1 Update existing frontend signal-based displays to consume the canonical semantics instead of raw-byte assumptions.
- [x] 3.2 Review labels, legends, and copy anywhere signal meaning is shown so they match the new dBFS-aligned contract.
- [x] 3.3 Confirm the high-frequency signal views and receiver-level signal displays now agree on the meaning of a stronger vs weaker signal.

## 4. Verification

- [x] 4.1 Verify the touched backend modules with focused syntax/tests.
- [x] 4.2 Run `npm run build` in `frontend/` and confirm the affected signal displays still build cleanly.
- [x] 4.3 Compare the resulting displayed distribution against readsb/graphs1090 expectations, including peak, weak-end floor, mean, and approximate interquartile spread.
