## 1. Event model and transport

- [x] 1.1 Review the current recent-message timing-event contract and define the widened exact-point fields required for the dedicated page: `range_nm`, `iid`, and display-grade signal semantics.
- [x] 1.2 Align the signal values exposed to the frontend with readsb-style dBFS behaviour before treating signal as a primary axis or colour mode.
- [x] 1.3 Extend the retained exact-message event stream and page transport so recent events can carry optional `bearing_deg`, `range_nm`, `iid`, and trusted signal values without introducing per-widget transports.
- [x] 1.4 Add or update focused backend tests covering the widened exact-point event shape and the publishability/availability rules for optional spatial and interrogator fields.

## 2. Dedicated message-field page

- [x] 2.1 Add a new top-level routed page for the live message field, separate from `Timing`.
- [x] 2.2 Build one large-format exact-point plot component that supports selectable geometry (`polar` / `cartesian`) and plot modes (`bearing × time`, `polar bearing × range`, `bearing × signal`, `range × signal`).
- [x] 2.3 Add bounded persistence controls such as `1 s`, `3 s`, `5 s`, `10 s`, and `30 s`.
- [x] 2.4 Add compact colour/filter controls for `df`, `source`, `iid`, `signal`, and `DF11`-focused analysis without fragmenting the page into multiple overlapping plots.

## 3. Timing-page cleanup

- [x] 3.1 Remove the shelved waterfall panel from the `Timing` page so the page returns to its core timing/observability role.
- [x] 3.2 Update any related copy or documentation to reflect that the dedicated message-field page is now the home for exact-point experimental high-speed visualisation.

## 4. Verification

- [x] 4.1 Verify the touched backend modules with focused syntax/tests.
- [x] 4.2 Run `npm run build` in `frontend/` and confirm the new page and routing build cleanly.
- [x] 4.3 Perform a focused runtime or synthetic check that exact points persist until they age out of the selected window rather than being bucketed or rescaled.
- [x] 4.4 Compare the new signal display semantics against readsb expectations before signing off on any signal-axis or signal-colour mode.

## Future Consideration

- [ ] Run a short live-receiver tuning pass before archive sign-off to validate defaults and visual scaling against real DF11/DF17 traffic, especially point size, polar radius usage, default mode/persistence, and signal/bearing comfort.
