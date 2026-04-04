## Context

The current Timing page already carries incremental recent-message events over a shared `/ws/timing-page` stream and renders message timing, interrogator lanes, burst/cadence panels, and the new signal/source views from one client-side buffer. `HFV2A` widened each recent event with `signal_raw` and `source_class`, but it intentionally left `icao` out so the first slice could stay focused on signal/source observability.

The next explored phase is proposal item `22`, `Micro-timeline of airspace activity`. It is the smallest remaining `V2` expansion that materially increases operator value without forcing per-message spatial attribution. The codebase already resolves `icao` during decode before aircraft-state updates are applied, so this phase is mainly about deciding how to carry that identity into the recent-event stream and how to keep the panel stable enough to read under live traffic.

## Goals / Non-Goals

**Goals:**
- Reuse the current recent-message buffer and shared Timing-page transport.
- Add inline `icao` to each recent timing event so the client can build aircraft-centric views without extra joins.
- Render a short-window aircraft micro-timeline with stable row membership and ordering.
- Keep the first aircraft-centric panel on the existing Timing page.

**Non-Goals:**
- Add bearing/range or other per-message spatial fields in this phase.
- Introduce a second advanced-visualisation page.
- Reconstruct aircraft identity or ranking by querying mutable live aircraft state after the fact.
- Solve the later decorative/spatial panels in the same change.

## Decisions

### 1. Carry `icao` inline inside each recent high-frequency event

The recent-event stream should widen from the current six-field tuple to include `icao` directly. That keeps the client buffer self-contained, makes reconnect/dedup logic straightforward, and avoids fragile sidecar joins such as `seq -> icao` maps or live lookups against mutable aircraft state.

Alternative considered:
- Keep events lean and send a sidecar identity mapping.
  Rejected because it increases client complexity, makes resync behavior harder to reason about, and offers little benefit unless payload growth proves problematic in measurement.

### 2. Keep the micro-timeline on the existing `Timing` page

This panel belongs to the same “recent activity over the last few seconds” mental model as the rest of the Timing page. It reuses the same event buffer, window controls, and page socket. Moving it to a separate page now would split related observability surfaces before the high-frequency information architecture actually demands it.

Alternative considered:
- Start an advanced visualisation page now.
  Rejected because the information model only really changes once the spatial family (`23`, `26`, `27`) enters scope.

### 3. Use a two-timescale stable row policy

The panel should not sort rows by the live short window on every frame; that would make the display jump. Instead, the client should render events from the visible short window while computing row membership/order from a slower ranking window and refreshing that ranking only on a slower cadence. Existing rows should be sticky so aircraft do not disappear the moment traffic varies normally.

Alternative considered:
- Pure live-count ordering.
  Rejected because rows would thrash under normal traffic bursts.
- Pure ICAO ordering.
  Rejected because it is stable but weak for operator value.

### 4. Start with lightweight color semantics

The first pass should not widen the event model again just to support richer colouring. The cleanest initial semantic is either monochrome or `source_class` colouring because that field already exists on the event. Altitude or type-group colour can wait for a later refinement if the panel proves valuable.

Alternative considered:
- Colour by altitude band in the first pass.
  Rejected because altitude is not currently retained per event, and looking it up from mutable aircraft state would blur message-time truth.

## Risks / Trade-offs

- Adding `icao` increases per-event payload size -> Keep retention short and continue using the existing bounded event cap.
- Stable row policies can feel arbitrary if the thresholds are poorly chosen -> Make the ranking window, row count, and inactivity behavior explicit and easy to tune.
- The Timing page may become visually crowded -> Keep the micro-timeline integrated but treat layout density and mobile behavior as part of the acceptance criteria.
- Inline identity widens the backend contract again -> Keep the phase tightly scoped so this is the last required event-model widening before any spatial work is considered.

## Migration Plan

This is an additive change. Backend and frontend should deploy together so the Timing page can consume the widened event tuples immediately. Rollback is straightforward by removing the panel and ignoring the added `icao` field.

## Open Questions

- What exact defaults should the first pass use for visible window, ranking window, row count, and inactivity expiry?
- Should the first version colour rows by `source_class`, or is monochrome better until the row model is proven in live use?
- Does the existing page layout still work cleanly with one more dense canvas panel, or should the lower half of the Timing page be reorganized into a different grid?
