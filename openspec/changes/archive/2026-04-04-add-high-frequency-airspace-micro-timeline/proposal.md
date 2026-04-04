## Why

The signal/source `HFV2A` slice proved that the Timing page can absorb richer high-frequency views without abandoning the shared recent-message buffer and multiplexed page stream. The next useful step is to add aircraft identity to that same event model so operators can see which aircraft are dominating airtime in the last few seconds, not just that traffic is busy in aggregate.

## What Changes

- Extend the recent high-frequency event model with inline `icao` so aircraft identity is available in the same self-contained incremental stream used by the Timing page.
- Add a `Micro-timeline of airspace activity` panel to the existing `Timing` page, showing recent per-message activity by aircraft over a short rolling window.
- Implement a stable top-`N` row policy so the micro-timeline surfaces active talkers without reordering rows on every burst.
- Use the existing shared Timing-page websocket and event buffer rather than introducing a second transport or doing late joins against mutable aircraft state.
- Explicitly defer the spatial/visual family (`23`, `25`, `26`, `27`) until later work defines per-message spatial attribution and the longer-term advanced visualisation layout.

## Capabilities

### New Capabilities
- `high-frequency-aircraft-identity-stream`: The backend exposes recent high-frequency timing events with inline aircraft identity for use by aircraft-centric Timing-page views.
- `timing-page-airspace-micro-timeline`: The Timing page renders a stable recent-aircraft activity raster from the shared incremental event stream.

### Modified Capabilities

## Impact

- Backend hot-path timing-event retention and websocket serialization in `backend/aircraft_state.py`, `backend/main.py`, and `backend/timing.py`.
- Frontend high-frequency buffering, ranking, and canvas rendering in `frontend/src/pages/TimingPage.jsx`.
- Follow-on planning only: bearing/range-driven views and the more decorative waterfall/polar/constellation panels remain deferred.
