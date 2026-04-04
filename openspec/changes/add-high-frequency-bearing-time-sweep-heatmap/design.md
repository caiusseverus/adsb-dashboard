## Context

The current Timing page already renders per-message timing, interrogator lanes, aggregate burst/cadence panels, signal/source panels, and the aircraft micro-timeline from one shared incremental event buffer. That buffer now carries `arrival_us`, `df`, `msg_len`, `signal_raw`, `source_class`, and `icao`, but it still cannot support any spatial panel because the event stream has no inline spatial attribution.

The next slice should solve that once, minimally. Proposal item `23`, `Bearing-time sweep heatmap`, is the smallest remaining spatial view because it only needs receiver-relative bearing sectors over time. It does not yet require a new page, full coordinate history, or the heavier visual grammar implied by `25`, `26`, and `27`.

## Goals / Non-Goals

**Goals:**
- Keep the existing one-buffer, one-page-socket model.
- Add a self-contained ingest-time spatial snapshot to recent timing events.
- Render one first spatial panel on the current Timing page using the existing short rolling window.
- Preserve message-time truth by attaching spatial fields at ingest time, not by rejoining later against mutable live aircraft state.

**Non-Goals:**
- Add a second advanced visualisation page.
- Guarantee spatial attribution for every message.
- Implement the decorative spatial variants (`25`, `26`, `27`) in this change.
- Retain full lat/lon or long-lived per-message spatial histories.

## Decisions

### 1. Snapshot `bearing_deg` inline at message ingest time

Each retained timing event should optionally carry receiver-relative `bearing_deg`. The value is captured from the aircraft state at the time the message is processed, after any decode-side position update for that message has been applied.

Alternative considered:
- Reconstruct bearing/range later from live aircraft state.
  Rejected because reconnects and rolling-window rendering would no longer be self-contained, and the displayed position could drift away from the message time.

### 2. Publish spatial fields only when the aircraft already has a publishable position

This slice should reuse the existing publishability rules that already govern snapshot positions. If the aircraft does not currently have a trustworthy receiver-relative position, the timing event keeps empty spatial fields and the heatmap simply ignores it.

Alternative considered:
- Backfill approximate bearing from any last-known lat/lon regardless of confidence.
  Rejected because it would make the panel visually busy while weakening the meaning of each pixel.

### 3. Keep the first spatial panel on the existing Timing page

The bearing-time heatmap belongs with the other short-window observability panels. It shares the same event window and the same page socket. A separate page is only justified once the more decorative spatial family enters scope.

Alternative considered:
- Start the advanced visualisation page now.
  Rejected because the panel can fit the current page and the information architecture has not yet split cleanly enough to justify a second destination.

### 4. Use client-side bearing bins on absolute time boundaries

The backend should continue to stream incremental events, not 10 Hz spatial aggregates. The frontend already has the holdback/interpolation model needed to draw smooth absolute-time bins from the shared event buffer, and item `23` benefits from that same treatment.

Alternative considered:
- Server-side aggregated bearing snapshots.
  Rejected because it would duplicate client logic and reintroduce stepped motion in a panel that should scroll smoothly.

## Risks / Trade-offs

- Wider per-message tuples increase payload size.
  Mitigation: keep the fields compact, keep the retention window bounded, and reuse the current shared page stream.
- Many messages will have no spatial attribution during sparse or low-confidence periods.
  Mitigation: the panel must expose a clear empty/waiting state and treat unattributable messages as intentionally absent, not silently counted into a fake sector.
- The Timing page is already dense.
  Mitigation: add only one full-width spatial panel in this slice and avoid extra controls unless they are necessary for readability.

## Migration Plan

This is an additive change. Deploy backend and frontend together so the widened event tuples are consumed immediately. Rollback is straightforward by removing the panel and ignoring the optional spatial fields.

## Open Questions

- Is the first panel clearer as raw message count per sector, or should it optionally support distinct-aircraft count later?
- Does the current Timing page need a layout reshuffle after adding one more full-width dense panel, especially on smaller screens?
