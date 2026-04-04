## Why

`HFV2A` proved that the Timing page can absorb richer per-message metadata, and `HFV2B` proved that aircraft-centric views can stay readable on the same shared event buffer. The next useful step is to add a bounded spatial snapshot to those same timing events so operators can see which azimuth sectors are active in the last few seconds without opening a second transport or reconstructing position later from mutable live state.

## What Changes

- Extend recent Timing-page events with inline receiver-relative `bearing_deg` when the aircraft has a publishable position at message ingest time.
- Add a `Bearing-time sweep heatmap` panel to the existing `Timing` page, showing recent activity density by bearing sector over the shared short rolling window.
- Keep the spatial attribution rule explicit: only messages with trustworthy publishable positions contribute to this panel; unattributable messages stay in the stream with empty spatial fields.
- Reuse the existing multiplexed Timing-page stream and client-side event buffer rather than introducing a dedicated spatial socket or late-joining against live aircraft snapshots.
- Explicitly defer the more decorative spatial/visual family (`25`, `26`, `27`) until the spatial event contract and load profile are proven by this first panel.

## Capabilities

### New Capabilities
- `high-frequency-spatial-attribution-stream`: The backend exposes recent high-frequency timing events with inline spatial snapshots for spatially attributable messages.
- `timing-page-bearing-time-sweep-heatmap`: The Timing page renders a recent bearing-time density panel from the shared incremental event stream.

## Impact

- Backend hot-path timing-event retention and websocket serialization in `backend/aircraft_state.py`, `backend/main.py`, and `backend/timing.py`.
- Frontend Timing-page buffering and canvas rendering in `frontend/src/pages/TimingPage.jsx`.
- Follow-on planning only: `Message waterfall`, `Polar pulse bloom`, and `Constellation drift view` remain deferred until this spatial event widening is validated.
