## 1. Backend spatial event model

- [x] 1.1 Extend the recent-message timing event model so each event can retain inline `bearing_deg` when the aircraft has a publishable position at message ingest time.
- [x] 1.2 Update the direct timing endpoint and both timing websockets so the widened event tuples remain self-contained and continue to reuse the existing shared transport model.
- [x] 1.3 Add or update focused backend tests covering the widened timing tuple shape and the publishable-position gating for spatial fields.

## 2. Timing page bearing-time panel

- [x] 2.1 Update the Timing-page client buffer to consume the widened timing event shape without regressing existing panels that share the same buffer.
- [x] 2.2 Add a `Bearing-time sweep heatmap` panel to `frontend/src/pages/TimingPage.jsx` that bins recent attributable messages by bearing sector over the visible timing window.
- [x] 2.3 Keep the panel on the shared Timing-page stream and render a clear empty state when no recent attributable messages are available.

## 3. Verification

- [x] 3.1 Verify the touched backend modules with focused syntax/tests.
- [x] 3.2 Run `npm run build` in `frontend/` and confirm the Timing page still builds with the new panel.
- [x] 3.3 Perform a focused synthetic or runtime check that proves the widened event shape differentiates attributable versus unattributable messages and that the heatmap reflects populated bearing bins.
