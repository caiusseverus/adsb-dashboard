## Why

The recent `HFV2` work proved that the dashboard can retain and stream recent per-message timing metadata, but the `Message waterfall` experiment did not justify its place on the `Timing` page. It duplicates information already available in the DF cadence lanes and the real-time message timing plot, while sacrificing the fine-grained exact-point detail that makes the high-speed data interesting in the first place.

The better direction is a dedicated large-format live message field: one page focused on exact message points, short persistence, and selectable geometries rather than buckets. That creates room for both spatial views and analytical cross-plots without overcrowding the `Timing` page, and it offers a clear home for richer high-speed visualisation work that does not fit the existing lane/strip-chart layout.

## What Changes

- Add a new top-level live visualisation page, separate from `Timing`, dedicated to exact-point high-speed message-field displays.
- Remove the shelved `Message waterfall` panel from the `Timing` page and treat waterfall-style displays as deferred exploration rather than active product scope.
- Expose the per-message metadata needed for a dedicated message-field page, including trustworthy spatial attribution and interrogator attribution where available.
- Support one full-width, tall primary plot with switchable geometry and axes rather than a cluster of overlapping smaller panels.
- Make the first plot modes:
  - `bearing × time` exact persistence view
  - `polar bearing × range`
  - `bearing × signal`
  - `range × signal`
- Support colour/filter controls that reuse the same point stream:
  - colour by `df`, `source`, `iid`, or `signal`
  - filter to all traffic or `DF11`-only where interrogator analysis is the focus
- Add persistence controls for short pattern visualisation, with an initial bounded range such as `1 s`, `3 s`, `5 s`, `10 s`, and `30 s`.
- Treat signal semantics as a prerequisite: align the displayed signal values with readsb-style dBFS behaviour before using signal as a primary axis or colour encoding on this new page.

## Capabilities

### New Capabilities
- `live-message-field-stream`: The backend exposes recent exact-point high-speed message events with the metadata needed for spatial and analytical message-field views.
- `message-field-page`: The frontend renders a dedicated page for large-format exact-point live message-field displays with selectable geometry, colour, filtering, and persistence controls.

### Modified Capabilities
- `timing-page-high-frequency-layout`: The Timing page removes the shelved waterfall experiment so it stays focused on timing and observability panels.

## Impact

- Backend recent-message event retention and page-stream serialization in `backend/aircraft_state.py`, `backend/main.py`, and related timing helpers.
- Frontend routing/navigation plus a new dedicated page component for the large-format live message field.
- Timing-page cleanup to remove the shelved waterfall panel.
- Follow-on planning only: decorative waterfall/pulse-bloom/constellation concepts remain deferred unless a distinct non-duplicative use emerges.
