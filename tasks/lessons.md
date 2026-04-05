# Lessons

## 2026-04-04

- When planning a new high-frequency panel, check it against every existing Timing-page visual first. Do not propose another bearing view if the page already has both bucketed and exact bearing displays; make the new panel's informational role distinct up front.
- For fixed-slice waterfall panels, do not normalize cell brightness against the current visible-window peak. That makes completed rows change brightness as stronger or weaker slices enter and leave the window; use a fixed transfer curve instead.
- For dense waterfall/raster panels, do not add alternating row backgrounds or other decorative striping. Operators will read that scaffolding as real periodic structure if the data itself is subtle.
- Before carrying a deficiency forward into implementation, check whether a later review or user decision already resolved it as "no change needed" and record that disposition in `tasks/todo.md` instead of assuming the remediation doc is still authoritative.
- Do not treat fast, low-value loading states as automatic wins. Verify whether the user actually wants the extra UI before planning or implementing it.
- When adding live receiver diagnostics, do not use a one-shot fetch hook for panels that are expected to update continuously. Match the page's existing polling model and verify that the UI refreshes without requiring a control change.
- For live-updating tables, prefer a stable domain sort when the user is scanning identifiers. Ranking by live counts can make rows jump and harms usability even if the data is technically correct.
- When a remediation item says an intended transport or architecture is missing, do not downgrade it to documentation-only without explicit confirmation. If the intended behavior is part of the feature contract, implement it or re-raise the tradeoff before closing the item.
- When moving a display onto the high-frequency page, verify its transport as well as its visual logic. A canvas panel can still feel wrong if it is being repainted from a coarse polling loop.
- For identifier-based lane views, do not rank and truncate by activity unless the design explicitly calls for that. If the operator is scanning codes, the default should be complete coverage in stable identifier order.
- When converting a live view from activity-ranked lanes to fixed identifier order, check every transport path too. A stale backend sort on the streaming endpoint can silently reintroduce unstable ordering even after the frontend is made fixed-order.
- Do not rely on “lock-free deque iteration” for shared live buffers in request handlers. If the decoder thread can append concurrently, snapshot the deque under the owning lock before iterating or filtering it.
- For high-frequency streamed canvases, smooth scroll alone is not enough. If packets arrive at 10 Hz, the right edge will still fill in chunks unless rendering uses a small holdback and reveals newly arrived events gradually across animation frames.
- Aggregate high-frequency panels have the same problem as per-message plots if they redraw only on packet arrival. Burst-rate and cadence bins need an interpolated timebase and frame-driven horizontal offset, otherwise they will visibly step even when the main scroll plots feel smooth.
- For the smoothest high-frequency overview panels, derive burst/cadence directly from the client-side incremental timing-event buffer when that buffer already exists. Server-side aggregate snapshots are fine for correctness, but they still look stepped compared with raw-event-driven rendering.
- Client-side aggregation alone is not sufficient for smooth overview plots. If bin membership is recomputed every frame from a moving cutoff, coarse bins will shimmer. For burst/cadence displays, freeze the bin counts for each timing snapshot and animate only the horizontal offset between snapshots.
- For binned high-frequency plots, do not freeze on packet arrival either. Rebuild counts only when the displayed time crosses a real bin boundary, then interpolate smoothly within that bin. That avoids both shimmer and websocket-rate stepping.
- If the right edge of a binned high-frequency plot still looks like it is filling in, the plot is too close to “now”. Add a one-bin display lag so the rightmost visible bin is already complete before it reaches the edge.
- For message-type timing lanes, do not derive the displayed lane set only from the active short window. Keep a stable default DF lane order and only append extra observed types, otherwise rows will appear and disappear during normal traffic variation.
- On a multiplexed live page, display-control changes must not recreate the shared websocket unless the stream identity really changed. Reconnecting on cosmetic control changes can reset incremental cursors and leak duplicate history into unrelated panels.
- Incremental timing buffers should deduplicate by sequence ID when merging, even if the transport is expected not to resend. That makes the UI robust against reconnects or config races.
- When matching an external decoder or protocol implementation, do not substitute a “close enough” linear transform for a documented signal formula. Trace the exact amplitude/power/log pipeline from the upstream implementation, including any smoothing window or epsilon floor, before changing public semantics.

## 2026-04-05

- On live filter controls, do not derive the selectable identifier list only from the currently visible short persistence window, and do not auto-reset the user's selected filter just because traffic pauses. Keep a sticky option cache so operator intent survives quiet periods.
- When rendering live canvas points with mixed colour formats, do not append hex alpha bytes blindly. If the palette can emit `hsl()` or named colours, apply opacity via canvas `globalAlpha` or a proper colour conversion so signal palettes do not collapse to one fallback colour.
- For instrument-style plots, do not place empty-state notes inside the plotting area when the axes themselves are still useful. Put the status note in surrounding chrome so the canvas remains readable as a frame of reference.
