# Deficiency Rectification Plan

Source inputs:
- `.docs/development/deficiency-remediation.md`
- `.docs/development/combined-deficiency-report.md`
- `.docs/development/polish-and-features-proposal.md`

Prepared: 2026-04-04
Updated: 2026-04-04

## AGENTS.md Contributor Guide Task

- [x] Inspect repository structure, manifests, and recent git history for contributor-facing conventions
- [x] Draft root `AGENTS.md` titled `Repository Guidelines` with concise, repo-specific instructions
- [x] Verify the guide against current commands/files and record the review result

### Review

- Verified repository structure against `README.md`, `frontend/package.json`, `backend/pyproject.toml`, `frontend/src/`, and `backend/tests/`.
- Verified commit guidance against recent `git log` subjects.
- Confirmed `AGENTS.md` is within the requested size target at `373` words and uses Markdown headings throughout.

## Current Assessment

- Items `A` through `H` were already remediated in code.
- The original remediation items are complete, but a follow-on high-frequency observability gap remains.
- The current timing display is streamed, but it still diverges materially from the proposal:
  - it uses wall-clock arrival time instead of Beast 12 MHz timestamps
  - it renders fixed-width ticks rather than message-length-scaled bars
  - it lives on the Receiver page instead of a dedicated high-frequency page
  - it does not yet handle overlap/stacking in a way that reflects true arrival density
- `Item J` is mislabeled in the execution table inside `.docs/development/deficiency-remediation.md`; the heading defines `J` as the shared loading/error-state rollout, but the status table currently labels `J` as the SkyView history heatmap issue. This must be corrected as part of the documentation pass.
- User clarification on 2026-04-04: `Item J` was previously reviewed and intentionally left as-is because the existing fetches are fast enough that added loading UI is not worth the churn. Treat it as a documented non-remediation, not active work.

## Execution Rules

- Rectify one deficiency at a time.
- Do not start the next item until the current item is verified.
- For each item, capture the verification result in the review section at the end of this file.
- Because the repo has no configured lint or test suite, verification will rely on targeted code inspection, focused runtime/API checks where applicable, and frontend build validation when UI code changes.

## Ordered Work

- [x] `HF0` — Scope the high-frequency page and backend load model
  - Review the addendum’s high-frequency proposals and separate them into:
    - `Core V1` displays that should ship on the first dedicated page
    - `Later V2` displays that depend on extra metadata or materially higher rendering cost
  - Recommended `Core V1` set for planning:
    - Message timing visualisation (`16`)
    - Interrogator timing lanes (`17`) with accurate timing
    - Burst-rate strip chart (`18`)
    - Pipeline latency oscilloscope (`19`)
    - DF cadence lanes (`24`)
  - Candidate `V2` items after the shared infrastructure is proven:
    - Signal-floor shimmer (`20`)
    - Source-mix pulse monitor (`21`)
    - Micro-timeline of airspace activity (`22`)
    - Bearing-time sweep heatmap (`23`)
    - Waterfall / pulse-bloom / constellation variants (`25`–`27`)
  - Define a single canonical recent-message event model for high-frequency displays, likely extending to:
    - `arrival_ts_us` derived from Beast timestamp
    - `df`
    - `msg_bits` or `msg_len`
    - `signal_raw`
    - `source_class`
    - optional `icao`
    - optional `iid`
  - Define backend load strategy for simultaneous display:
    - one shared in-memory recent-message buffer
    - one shared high-frequency stream per page/client, not one stream per widget
    - derive cheap aggregations client-side where possible
    - derive expensive aggregations server-side once per tick if they are shared across widgets
    - bounded retention and bounded per-client queues
  - Verification gate:
    - `tasks/todo.md` records the chosen V1 scope and explicit deferrals.
    - The plan includes a backend load budget and a rule for simultaneous widgets so we do not build each panel as an independent producer.
  - Chosen `Core V1` scope:
    - `16` Message timing visualisation, corrected to Beast-time fidelity
    - `17` Interrogator timing lanes, aligned to the same accurate timing model
    - `18` Burst-rate strip chart
    - `19` Pipeline latency oscilloscope
    - `24` DF cadence lanes
  - Explicit `V2` deferrals:
    - `20` Signal-floor shimmer
    - `21` Source-mix pulse monitor
    - `22` Micro-timeline of airspace activity
    - `23` Bearing-time sweep heatmap
    - `25` Message waterfall
    - `26` Polar pulse bloom
    - `27` Constellation drift view
  - Canonical recent-message event model for `V1` planning:
    - `arrival_ts_us` in Beast-relative microseconds
    - `df`
    - `msg_bits`
    - `signal_raw`
    - `source_class`
    - optional `icao`
    - optional `iid`
  - Backend load model:
    - One shared recent-message ring buffer, short retention only.
    - One shared high-frequency aggregation path, not one producer per panel.
    - One multiplexed high-frequency WebSocket per page/client, with typed messages for:
      - raw/incremental recent-message events
      - shared aggregate bins/traces needed by multiple widgets
      - low-rate status/heartbeat metadata
    - Per-client queue must stay bounded, matching the existing philosophy used by the main `/ws` snapshot path.
  - Planning load budget:
    - Design target: tolerate at least `3,000 msg/s` sustained for a `5 s` window on the dedicated page.
    - Raw retention budget: roughly `15,000` recent-message events in memory for V1, with room to raise if profiling justifies it.
    - Aggregation cadence target: `10 Hz` page stream updates, with `requestAnimationFrame` rendering client-side.
    - The backend should scan/aggregate the shared recent buffer at most once per tick for page-wide shared products; widgets must not each trigger their own buffer walk.
    - Keep the main `/ws` snapshot path and the new high-frequency path isolated so opening the high-frequency page does not bloat or slow normal clients.

- [x] `HF1` — Correct timing source to use Beast timestamp space
  - Stop using wall-clock `time.time()` as the plotted X-axis source for message timing.
  - Introduce a dedicated recent-message timing ring buffer keyed by Beast timestamp, not decode-time wall clock.
  - Handle timestamp unwrap/rollover explicitly so the last 2–5 seconds remain monotonic even across counter wrap or reconnect.
  - Define a canonical event shape for high-frequency displays, likely including:
    - `beast_ts_ticks`
    - `rel_us` or equivalent plot-ready relative microseconds
    - `df`
    - `msg_len`
    - optional metadata for later displays such as `iid`, `icao`, and source stream
  - Verification gate:
    - Consecutive events are monotonic in Beast time within the live window.
    - Plot timing no longer shows obvious wall-clock quantisation/jitter unrelated to message cadence.
    - Ring buffer behavior across reconnect/wrap is documented and tested with a focused synthetic case if practical.

- [x] `HF2` — Establish a dedicated high-frequency page
  - Add a new top-level page/tab for high-rate observability rather than embedding these panels inside [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
  - Move the message timing display and interrogator timing display onto that page, and design the page so additional V1 high-frequency panels can coexist without multiplying backend streams.
  - Give the page its own local controls for time window, lane mode, pause/freeze, and display density without cluttering Receiver.
  - Keep Receiver focused on lower-frequency diagnostics and summary charts.
  - Verification gate:
    - New page is accessible from the top navigation in [App.jsx](/home/keith/claude/adsb-dashboard/frontend/src/App.jsx).
    - Receiver no longer owns the high-frequency visualisations.
    - Existing receiver diagnostics remain functional.

- [x] `HF3` — Rebuild the message timing renderer to match proposal semantics
  - Render bars with width derived from `msg_len` so 56-bit and 112-bit messages are visually distinct.
  - Use Beast-time relative coordinates, not wall-clock interpolation.
  - Replace the current fixed-width tick rendering with a batching-oriented canvas pipeline designed for sustained high message rates.
  - Add overlap handling:
    - either per-lane stacking
    - or overlap shading/density accumulation
  - Verification gate:
    - Short and long messages are visibly different widths.
    - Dense bursts show meaningful overlap behavior rather than simply overpainting ticks.
    - Rendering remains smooth while the page is visible.

- [x] `HF4` — Move interrogator timing onto the high-frequency page and align its timing source
  - Relocate the current interrogator timing lanes from Receiver to the new page.
  - Replace the current wall-clock-ish timing source with the same Beast-relative timing model used by the message timing view so interrogator cadence is temporally accurate.
  - If needed, extend the recent-message event model or maintain a derived IID event stream keyed off the same Beast-relative clock rather than a separate wall-clock buffer.
  - Preserve row-click-to-map behavior.
  - Verification gate:
    - Interrogator lanes render correctly on the new page.
    - Click-through to map still works.
    - Timing model is documented and consistent with the page’s message timing display.

- [x] `HF4A` — Add Burst/Latency/Cadence V1 panels to the high-frequency page
  - Add `Burst-rate strip chart` from proposal item `18`, derived from the shared recent-message buffer using 10/20/50 ms bins.
  - Add `Pipeline latency oscilloscope` from proposal item `19`, using existing runtime timing stores plus any small additional retention needed for a scrolling view.
  - Add `DF cadence lanes` from proposal item `24` as a lower-cost aggregate complement to the full per-message timing view.
  - Make these views share the same transport/buffer foundation rather than each polling or streaming independently.
  - Chosen execution split:
    - `HF4A.1` backend: add timestamped short-retention performance traces so latency can be rendered as a moving waveform instead of percentiles only
    - `HF4A.2` backend: add one shared high-frequency aggregate feed that scans the recent message window once per tick and emits:
      - burst-rate bins
      - DF cadence bins by family
      - latency traces
    - `HF4A.3` frontend: add three new Timing-page panels consuming that shared aggregate feed
    - `HF4A.4` verification: confirm simultaneous display works from one aggregate producer rather than per-widget scans
  - Concrete data model for `HF4A`:
    - Recent message aggregates stay on the existing Beast-relative clock with a `window_us` and `now_us`
    - Burst-rate chart uses configurable `bin_ms` and one `counts` array for all messages
    - DF cadence lanes use the same window with a coarser `bin_ms` and per-family count arrays
    - Pipeline latency oscilloscope uses wall-clock `ts_s` samples because it reflects runtime pipeline behaviour, not RF arrival timing
  - Family grouping for `DF cadence lanes`:
    - `ADS-B` = `DF17`
    - `TIS-B` = `DF18`
    - `All-Call` = `DF11`
    - `Comm-B` = `DF20` + `DF21`
    - `Surveillance` = `DF4` + `DF5`
    - `ACAS` = `DF0` + `DF16`
    - `Other` = any remaining DF present in the recent window
  - Verification gate:
    - All selected V1 panels render on the new page from shared live data.
    - Simultaneous display of V1 panels does not require multiple independent backend scans of the same raw event history.

- [x] `HF5` — Tighten the streamed transport for high-frequency displays
  - Keep the dedicated streamed channel concept, but align it to the new canonical Beast-time event model.
  - Prefer a page-specific multiplexed WebSocket with typed messages over one socket per widget, unless profiling shows a simpler split is clearly cheaper.
  - Ensure the stream carries only incremental data and remains robust under idle periods, reconnects, and bursty traffic.
  - Chosen execution split:
    - `HF5.1` backend: add one `Timing` page websocket that carries typed payload sections for:
      - incremental timing events
      - current interrogator lanes
      - shared aggregate panels
    - `HF5.2` frontend: add one page-level stream hook in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and pass the unified data into the existing panels
    - `HF5.3` compatibility: keep `/ws/timing`, `/ws/interrogators`, and `/ws/highfreq` in place for now so the transport refactor does not force a risky big-bang removal
  - Payload contract for the page stream:
    - `type: "timing_page"`
    - `timing: { now_us, events }` using the existing incremental event shape
    - `interrogators: { now_us, window_s, lanes }`
    - `aggregates: { now_us, window_s, burst, cadence, latency }`
  - Transport rules:
    - timing events remain incremental per connection via `since_seq`
    - interrogator and aggregate sections are latest-state snapshots
    - idle heartbeats still send the typed envelope with empty `events`
  - Verification gate:
    - Stream reconnects cleanly without corrupting local buffers.
    - Idle heartbeat behavior is defined and does not cause visual artifacts.
    - Payload rate stays bounded under heavy traffic.

- [x] `HF6` — Verification and acceptance pass for the whole high-frequency page
  - Verify against known traffic patterns:
    - dense DF17 periods
    - DF11 interrogator rhythm
    - MLAT-heavy periods if available
  - Compare displayed temporal spacing against Beast timestamp deltas, not just visual intuition.
  - Confirm the page behaves sensibly when hidden, resized, or reopened.
  - Add a short review note here with observed behavior and any intentional scope cuts.
  - Verification gate:
    - Backend syntax/build checks pass.
    - Frontend build passes.
    - Manual runtime validation confirms the display is no longer obviously quantized and better reflects true message arrival timing.

- [ ] `HFV2` — Plan the remaining addendum high-frequency items
  - Purpose:
    - Capture the remaining addendum items as a deliberate `V2` phase rather than leaving them as loose ideas.
    - Do not treat this as implementation-ready scope yet; each item still needs proper exploration, event-model review, rendering design, and backend load assessment before coding.
  - Remaining addendum candidates:
    - `20` Signal-floor shimmer / RF health panel
    - `21` Source-mix pulse monitor
    - `22` Micro-timeline of airspace activity
    - `23` Bearing-time sweep heatmap
    - `25` Message waterfall
    - `26` Polar pulse bloom
    - `27` Constellation drift view

## OpenSpec Change: `add-high-frequency-airspace-micro-timeline`

- [x] Review the existing timing-event ingest, websocket/API serialization, and Timing-page buffer contracts.
- [x] Extend backend timing events to retain inline `icao` at ingest time and serialize the widened event shape on `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
- [x] Update the Timing-page client buffer to consume widened events without regressing existing panels that share the same stream.
- [x] Add the `Micro-timeline of airspace activity` panel with a stable top-`N` row policy, sticky membership, and first-pass source-based coloring.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic runtime check covering idle, multi-aircraft, and row-stability behavior.

### Review

- Plan verified against `openspec/changes/add-high-frequency-airspace-micro-timeline/{proposal,design,tasks}.md` and the two spec files before implementation.
- Backend event retention now stores inline `icao` in the recent-message buffer and serializes the widened tuple consistently through the direct API, `/ws/timing`, and `/ws/timing-page`.
- The Timing page now includes a full-width `Micro-timeline of airspace activity` panel backed by the existing shared timing buffer and a pure helper that applies slower ranking refreshes with sticky row membership.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - `node --input-type=module` synthetic helper check confirmed `idleRows: 0`, stable initial rows `AAAAAA/BBBBBB/CCCCCC`, and admission of new entrant `DDDDDD` without reordering existing rows.
  - Exploration work required before implementation:
    - Confirm which additional fields are needed in the recent-message event model:
      - `signal_raw`
      - `source_class`
      - `icao`
      - optional bearing/range or enough metadata to derive them cheaply
    - Decide which displays belong on the existing `Timing` page versus a separate advanced visualisation page.
    - Profile simultaneous-display load now that `V1` is working, so `V2` does not overrun the shared buffer/stream model.
    - For each candidate, define:
      - operator value
      - exact data requirements
      - rendering approach
      - expected refresh cadence
      - mobile/responsive impact
      - acceptance criteria
  - Recommended `V2` planning order:
    - `HFV2.1` signal/source/data-model exploration for items `20` and `21`
    - `HFV2.2` aircraft/bearing/spatial-data exploration for items `22`, `23`, `26`, and `27`
    - `HFV2.3` choose the first implementation subset based on value versus load
    - `HFV2.4` implement one item at a time with the same verify-before-next rule used for `V1`
  - Investigation result on 2026-04-04:
    - Proposed next implementation slice is `HFV2A` on the existing `Timing` page, centered on proposal items `20` and `21`.
    - Chosen first-slice scope:
      - widen the shared recent-message event model with per-message `signal_raw` and a compact `source_class`
      - add `Signal-floor shimmer / RF health panel`
      - add `Source-mix pulse monitor`
    - Reasoning:
      - these two panels add operator-facing value while reusing the current Beast-time ring buffer and multiplexed `/ws/timing-page` transport
      - they de-risk the `V2` data model with lighter backend changes than the bearing/range-dependent views
      - they keep the first `V2` slice within the existing load rule of one shared recent-message buffer and one page stream
    - Deferred beyond `HFV2A`:
      - `22` Micro-timeline of airspace activity after deciding whether per-message `icao` belongs in the same enriched event model
      - `23`, `26`, and `27` until per-message spatial attribution is defined and profiled
      - `25` until the enriched buffer is proven and a stronger visual grammar is chosen for the more decorative views
    - OpenSpec change created for this next-stage proposal:
      - `openspec/changes/plan-high-frequency-v2-visualizations/`
  - Follow-on exploration result on 2026-04-04:
    - Proposed next implementation slice after `HFV2A` is `HFV2B`: proposal item `22`, `Micro-timeline of airspace activity`.
    - Chosen `HFV2B` direction:
      - widen the shared recent-message event model with inline `icao`
      - keep the panel on the existing `Timing` page
      - use a stable top-`N` row policy driven by a slower ranking cadence rather than live per-frame sorting
      - start with lightweight color semantics, preferably `source_class`, instead of widening the event model again for altitude or spatial color
    - Reasoning:
      - item `22` is the smallest remaining `V2` step that materially improves operator readability
      - inline `icao` keeps the client event buffer self-contained and avoids fragile joins against mutable aircraft state
      - this de-risks aircraft-centric high-frequency views before the much larger spatial-attribute work needed by `23`, `26`, and `27`
    - OpenSpec change created for this follow-on proposal:
      - `openspec/changes/add-high-frequency-airspace-micro-timeline/`

## OpenSpec Change: `add-high-frequency-bearing-time-sweep`

- [x] Review the current timing-event retention, websocket/API serialization, and Timing-page buffer contract for the next spatial slice.
- [x] Extend backend timing events to retain optional inline `bearing_deg` at ingest time and serialize the widened shape on `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
- [x] Update the Timing-page client buffer to consume widened events without regressing the existing panels that share the same stream.
- [x] Add the `Bearing-time sweep heatmap` panel to `frontend/src/pages/TimingPage.jsx`.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic check covering empty state plus mixed bearing/no-bearing traffic.

### Review

- Plan verified against `openspec/changes/add-high-frequency-bearing-time-sweep/{proposal,design,tasks}.md` before implementation.
- Backend timing events now carry optional inline `bearing_deg`, captured after the aircraft record has been updated so the shared event buffer stays self-contained without a later live-state join.
- `/api/timing/events`, `/ws/timing`, and `/ws/timing-page` now serialize the widened event shape consistently for the Timing page and legacy timing consumers.
- The Timing page now includes a full-width `Bearing-time sweep heatmap` panel driven by the existing shared event buffer; only bearing-attributed events are plotted, while unattributed traffic still appears in the other timing panels.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - `node --input-type=module` synthetic heatmap-bucketing check confirmed `empty={attributedCount:0,occupiedCells:0,maxCount:0}` and `mixed={attributedCount:3,occupiedCells:2,maxCount:2}` for a shared-stream sample containing both bearing and no-bearing events.

- [x] `Item I` — Documentation scope correction for Phase `0A`
  - Update `.docs/development/polish-and-features-proposal.md` to state that the delivered result is shared filter helpers/predicates, not shared global filter state.
  - Update `.docs/development/combined-deficiency-report.md` to reflect that Coverage is intentionally not part of a shared aircraft-filter UI/state model.
  - Fix the `Item J` status-table mismatch in `.docs/development/deficiency-remediation.md` while touching the remediation docs, so the remaining-work list is internally consistent.
  - Verification gate:
    - Confirm the docs consistently describe `useAircraftFilter` as local state plus shared predicate helpers.
    - Confirm no remaining document claims say Coverage should share interactive filter state with Live/Map/SkyView.
    - Confirm `Item J` refers to loading/error-state adoption everywhere in the remediation doc.

- [x] `Item J1` — Adopt `useFetchState` on `SkyView`
  - Replace inline fetch/state handling in [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) for terrain, history, and tracks with [useFetchState.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/useFetchState.js) where appropriate.
  - Render [LoadingState.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/LoadingState.jsx) for loading, empty, and error states instead of ad hoc null-state handling.
  - Preserve existing polling behavior for track refreshes; only the state machine and user-visible status handling should change.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - SkyView shows a loading state before async overlay data arrives.
    - SkyView shows retryable error UI on failed fetches.
  - Disposition:
    - Waived by user on 2026-04-04 after clarifying this had already been reviewed and judged unnecessary because the current fetch path is fast enough.

- [x] `Item J2` — Adopt `useFetchState` on `CoveragePage`
  - Refactor historical/timelapse overlay fetches in [CoveragePage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/CoveragePage.jsx) to the same shared fetch-state pattern.
  - Keep current Three.js rendering logic intact; this is a fetch-state and UI-state cleanup, not a rendering redesign.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - Coverage historical overlays show loading and retryable error states.
    - No regression in existing timelapse/history rendering path once data is available.
  - Disposition:
    - Waived by user on 2026-04-04 for the same reason as `Item J1`; no code change required.

- [x] `Item K1` — Add active IID table to `ReceiverPage`
  - Extend [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the existing `/api/interrogators` data is presented as a table of active IID codes with counts and last-seen information if available from the API shape.
  - Keep the existing `InterrogatorTimeline` canvas; the table complements it rather than replacing it.
  - If last-seen data is missing from the current API, add the minimal backend support needed before finalizing the UI.
  - Verification gate:
    - `npm run build` succeeds in `frontend/`.
    - Receiver page shows IID rows from live API data, not placeholder/static content.
    - Table values match `/api/interrogators` responses for the selected window.

- [x] `Item K2` — Capture and expose `msg_len` for timing events
  - Update [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) timing storage from `(ts, df)` to `(ts, df, msg_len)`.
  - Update [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) response serialization so clients receive message length with each timing event.
  - Update the timing plot in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) to consume the expanded event shape without breaking existing rendering.
  - Verification gate:
    - Backend starts cleanly.
    - `/api/timing/events` returns triples including message length.
    - Timing plot still renders with live polled data after the schema change.

- [x] `Item K3` — Implement real streamed transport for Item `16`
  - Replace the current HTTP polling transport for the message timing plot with a dedicated streamed channel, using WebSocket or SSE as originally intended.
  - Keep the payload aligned with `Item K2`: timing events must stream `ts`, `df`, and `msg_len`.
  - Preserve the current plot behavior while removing the fixed 200 ms HTTP polling loop from [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
  - Verification gate:
    - Backend starts cleanly with the new streamed timing endpoint.
    - The Receiver page timing plot receives live events over the stream rather than repeated HTTP fetches.
    - `npm run build` succeeds in `frontend/`.

## Review

- `Item I` verified on 2026-04-04.
  - Updated [polish-and-features-proposal.md](/home/keith/claude/adsb-dashboard/.docs/development/polish-and-features-proposal.md) to describe shared filter helpers/control patterns rather than shared cross-page filter state.
  - Updated [combined-deficiency-report.md](/home/keith/claude/adsb-dashboard/.docs/development/combined-deficiency-report.md) to reframe Phase `0A` as a documentation-scope issue and to state explicitly that Coverage should not be treated as part of a required shared aircraft-filter UI/state model.
  - Updated [deficiency-remediation.md](/home/keith/claude/adsb-dashboard/.docs/development/deficiency-remediation.md) so `Item J` now correctly refers to the Phase `0B` shared loading/error pattern instead of the already-completed SkyView history heatmap work.
- `Item J` waived on 2026-04-04.
  - User clarified this had already been addressed in a previous review and intentionally not implemented because the current fetch path is fast enough that explicit loading-state UI is unnecessary.
  - No code change made for [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) or [CoveragePage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/CoveragePage.jsx).
- `Item K1` verified on 2026-04-04.
  - Added per-IID activity metadata in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) and exposed `last_seen` plus `latest_icao` from [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py).
  - Extended the existing IID card in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) with an active-code table showing IID, reply count, last-seen age, and latest aircraft, with supporting styles in [ReceiverPage.module.css](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.module.css).
  - Follow-up fix: switched the IID card from one-shot `useFetch` to a 1 Hz polling loop so the chart and table refresh live without requiring a window change.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/interrogators.py` and `npm run build` in `frontend/`.
- `Item K2` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so timing events now store `(ts, df, msg_len)` on both native and fallback decode paths.
  - Updated [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) so `/api/timing/events` now returns `events: [[ts, df, msg_len], ...]`.
  - Updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the timing plot consumes the expanded event shape without changing current rendering behavior.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/timing.py` and `npm run build` in `frontend/`.
- `Item K3` verified on 2026-04-04.
  - Added a dedicated timing stream endpoint at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/timing`, sending timing events over WebSocket at roughly 10 Hz with heartbeat frames when idle.
  - Switched the live timing plot in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) from fixed-interval HTTP polling to a reconnecting WebSocket client using the streamed timing channel.
  - The HTTP endpoint in [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) remains available, but the receiver UI no longer depends on it for live delivery.
  - Verification completed with `python3 -m py_compile backend/main.py backend/timing.py backend/aircraft_state.py` and `npm run build` in `frontend/`.
- `HF1`–`HF6` pending.
  - Follow-on planning initiated on 2026-04-04 after confirming that the current timing plot is streamed but still does not faithfully match the Beast-timestamp-based proposal semantics.
- `HF0` verified on 2026-04-04.
  - Chosen V1 scope is: message timing, interrogator timing, burst-rate strip chart, pipeline latency oscilloscope, and DF cadence lanes.
  - Chosen V2 deferrals are: signal-floor shimmer, source-mix pulse monitor, micro-timeline, bearing-time sweep heatmap, waterfall, polar pulse bloom, and constellation drift.
  - Backend load rule is explicit: one shared recent-message buffer and one multiplexed high-frequency stream per page/client, not one producer or socket per widget.
  - Planning load target is explicit: design for roughly `3,000 msg/s` over a `5 s` recent window with `10 Hz` stream updates and client-side rAF rendering.
- `HF1` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so the high-frequency timing buffer now records Beast-relative `arrival_us` values with sequence ids, rather than wall-clock `time.time()` values.
  - Added unwrap/rebase handling for Beast timestamps and reset behavior across stream discontinuities so the recent timing window stays monotonic.
  - Preserved original stream provenance during decode so high-frequency timing only uses primary-stream Beast timestamps; MLAT marker frames and auxiliary MLAT-side streams are excluded from this timing domain to avoid mixing incompatible clocks.
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), and [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so the live timing plot consumes `now_us` and `arrival_us` from the Beast-relative stream instead of wall-clock seconds.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF2` verified on 2026-04-04.
  - Added a dedicated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and exposed it through a new top-level `Timing` tab in [App.jsx](/home/keith/claude/adsb-dashboard/frontend/src/App.jsx).
  - Moved the existing high-frequency panels off [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) and onto the dedicated page by exporting and reusing the existing `InterrogatorCodes`, `InterrogatorTimeline`, and `MessageTimingPlot` panels.
  - Receiver now contains only the lower-frequency diagnostics and summary charts; the Timing page is the staging ground for the remaining V1 high-frequency panels.
  - Verification completed with `npm run build` in `frontend/`.
- `HF3` verified on 2026-04-04.
  - Updated the timing renderer in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so messages are drawn as bars whose width is derived from `msg_len`, rather than fixed-width ticks.
  - Added simple per-lane overlap stacking with a bounded number of vertical sub-slots; overflowed overlaps collapse into the final slot instead of silently overpainting.
  - The current implementation is a pragmatic first pass at overlap handling, not the final visual design: it distinguishes short/long messages and makes dense bursts legible without changing the transport or page structure.
  - Verification completed with `npm run build` in `frontend/`.
- `HF4` verified on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so DF11 IID timing events now carry Beast-relative arrival timing alongside the existing wall-clock data used for longer-window counts.
  - Updated [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py) so `/api/interrogators/timeline` now returns `now_us` and `arrivals_us`, aligned to the current Beast timing epoch.
  - Updated the interrogator timing canvas in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so it renders against Beast-relative timing instead of wall-clock seconds, while preserving click-through to the map.
  - Intentional split: the IID count table still uses wall-clock `last_seen`/count windows, while the timing-lane view now uses Beast-relative timing for accuracy.
  - Follow-up correction: [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) `/ws/interrogators` had still been sorting streamed lanes by live activity count. That stale backend sort was removed so the stream contract is now stable IID-ascending order end-to-end.
  - Follow-up correction: [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) request-path helpers were iterating `_iid_events` directly while the decoder thread appended to it, which could raise `RuntimeError: deque mutated during iteration`. The helpers now snapshot shared timing/IID deques under `self._lock` before iterating.
  - Verification completed with `python3 -m py_compile backend/interrogators.py backend/aircraft_state.py` and `npm run build` in `frontend/`.
- `HF4A` verified on 2026-04-04.
  - Added timestamped short-retention latency traces in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so decode, lock-wait, snapshot, serialize, and broadcast timings can be rendered as moving waveforms rather than percentiles only.
  - Added a shared aggregate high-frequency websocket at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/highfreq`. It scans the recent message window once per tick and emits burst-rate bins, DF cadence family bins, and rebinned latency traces for the page.
  - Extended [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with the `Burst-rate strip chart`, `DF cadence lanes`, and `Pipeline latency oscilloscope`, all fed from the same shared websocket instead of one producer per panel.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/interrogators.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF5` verified on 2026-04-04.
  - Added a multiplexed Timing-page websocket at [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) on `/ws/timing-page` carrying a typed envelope with:
    - incremental timing events
    - current interrogator lane state
    - shared aggregate panel state
  - Updated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) so the dedicated Timing page now opens one page-level websocket and passes its data into the existing panels instead of opening separate per-widget sockets.
  - Updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) so `MessageTimingPlot` and `InterrogatorTimeline` can consume injected stream data when hosted on the Timing page, while keeping their existing standalone websocket behavior for compatibility.
  - Compatibility choice: left `/ws/timing`, `/ws/interrogators`, and `/ws/highfreq` in place for now so the transport refactor does not become a risky big-bang removal.
  - Verification completed with `python3 -m py_compile backend/main.py backend/aircraft_state.py backend/interrogators.py backend/timing.py` and `npm run build` in `frontend/`.
- `HF6` accepted on 2026-04-04.
  - Code-level acceptance is complete:
    - `requestAnimationFrame` rendering is in place for the two scrolling timing canvases in [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx).
    - Hidden-tab throttling is in place for the message timing plot.
    - The multiplexed Timing-page stream in [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) has explicit 1 s heartbeat behavior and bounded incremental timing payloads.
    - Build/compile verification passed for the full backend/frontend set.
  - Startup smoke check is also complete:
    - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) starts successfully under `uvicorn`.
  - Live receiver backend validation completed on 2026-04-04 by overriding `BEAST_HOST` to `192.168.0.142`:
    - [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) returned dense real traffic with varying `arrival_us` deltas and mixed `msg_len` values of `7` and `14`, confirming the timing buffer is carrying true per-message timing rather than coarse quantized buckets.
    - The incremental timing API advanced cleanly from `since_seq=14503` to much higher sequence values on the follow-up pull, confirming that the live stream model is appending and advancing rather than resetting a static window.
    - [interrogators.py](/home/keith/claude/adsb-dashboard/backend/interrogators.py) returned populated DF11 timing lanes with many active IIDs, and those lanes were emitted in ascending IID order, matching the intended stable-order contract.
    - The long-window IID activity API also returned plausible live counts and recent `last_seen` values from the same receiver session.
  - Remaining acceptance item:
    - Completed by user review: current browser-side behavior is acceptable for now.
  - Follow-up runtime correction on 2026-04-04:
    - The scrolling plots were moving smoothly but newly received points were appearing at the right edge in visible 10 Hz chunks.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now uses a small `150 ms` render holdback for the scrolling message and interrogator timing canvases so newly arrived events are revealed smoothly across frames rather than all at once when each packet lands.
  - Follow-up runtime correction on 2026-04-04:
    - The remaining jerk came from packet-stepped aggregate panels and an overly small live-edge smoothing margin.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now uses a `400 ms` live-edge holdback for the scrolling message and interrogator timing canvases to better hide packet cadence and backend jitter at the right edge.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) no longer drives `Burst-rate strip chart` and `DF cadence lanes` from server-side aggregate snapshots. They are now derived on the client from the same incremental timing-event buffer as the main message plot.
  - Follow-up runtime correction on 2026-04-04:
    - The first client-side aggregation pass removed stepping but introduced shimmer because coarse bins were being recomputed every animation frame from a moving cutoff.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now freezes burst/cadence bin counts per timing snapshot and only interpolates the horizontal offset between snapshots, which should preserve smooth scrolling without rapid shape flicker.
  - Follow-up runtime correction on 2026-04-04:
    - Freezing on packet arrival removed shimmer but reintroduced websocket-rate stepping.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now rebuilds burst/cadence bins only when the displayed time crosses an actual bin boundary, then interpolates smoothly within the bin. That should avoid both shimmer and packet-step motion.
  - Follow-up runtime correction on 2026-04-04:
    - `Burst-rate` and `DF cadence` still looked like they were filling in from the right because the visible aggregate edge was too close to the latest bin.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now applies a one-bin display lag for those aggregate panels so the newest visible bin is already complete before it reaches the edge.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now keeps a stable default DF lane set for `Message timing` and uses a larger `650 ms` live-edge holdback so rows do not appear/disappear with short-window traffic changes and the right edge is less likely to show packet-gap fill-in.
  - Follow-up runtime correction on 2026-04-04:
    - Changing `Burst-rate` or `DF cadence` controls was incorrectly perturbing `Message timing` because the shared page websocket was being recreated on each control change.
    - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now keeps one stable `Timing` page websocket and sends control updates over the existing connection instead of reconnecting.
    - [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx) now deduplicates timing events by `seq` when merging incremental packets, so duplicate history cannot create spurious red overflow bars if a reconnect or resend ever occurs.
  - Acceptance disposition:
    - User confirmed on 2026-04-04 that the page is acceptable for now.
    - Remaining smoothing/display imperfections are treated as future polish, not blocking defects.
- `HFV2` investigated on 2026-04-04.
  - Reviewed the deferred `HFV2` backlog in [tasks/todo.md](/home/keith/claude/adsb-dashboard/tasks/todo.md) against the original addendum in [polish-and-features-proposal.md](/home/keith/claude/adsb-dashboard/.docs/development/polish-and-features-proposal.md).
  - Chosen next implementation slice is `HFV2A`: enrich the shared recent-message stream with `signal_raw` and `source_class`, then add the `Signal-floor shimmer` and `Source-mix pulse monitor` panels to [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
  - Captured the implementation-ready proposal, design, specs, and task breakdown in [proposal.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/proposal.md), [design.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/design.md), and [tasks.md](/home/keith/claude/adsb-dashboard/openspec/changes/plan-high-frequency-v2-visualizations/tasks.md).
  - Deferred `22`, `23`, `25`, `26`, and `27` remain intentionally out of the first slice until ICAO/spatial attribution and richer visual grammar are explored separately.
- `HFV2A` implemented on 2026-04-04.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so the shared recent-message timing buffer now retains `signal_raw` and a compact `source_class` alongside `arrival_us`, `df`, and `msg_len`.
  - Updated [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) and [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) so the widened six-field event tuples flow through the existing multiplexed Timing-page stream and the direct timing endpoint without introducing extra sockets or per-widget producers.
  - Updated [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with `Signal-Floor Shimmer` and `Source-Mix Pulse Monitor`, both rendered from the existing incremental event buffer and current page timing window.
  - Scope decision: optional per-message `icao` propagation stays deferred to the later aircraft-activity work, because the signal/source slice does not need it and widening the stream again is cheaper than mixing that concern into the first implementation.
  - Scope decision: the first `Source-Mix` pass tracks accepted primary-stream timing traffic classes only. Rejected/ignored-frame accounting remains a follow-up because it does not have a clean existing low-risk hook in the current hot path.
  - Verification completed with `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`, `npm run build` in `frontend/`, and a synthetic `uv run --directory backend python` smoke that exercised idle and busy Timing-page payload shapes from one shared envelope.

## OpenSpec Change: `add-high-frequency-bearing-time-sweep-heatmap`

- [x] Review the current timing-event ingest path, spatial publishability rules, and Timing-page buffer contract before widening the event shape again.
- [x] Extend backend timing events so each retained message can carry an inline ingest-time spatial snapshot with `bearing_deg` when the aircraft has a publishable receiver-relative position.
- [x] Update `/api/timing/events`, `/ws/timing`, and `/ws/timing-page` plus the Timing-page client buffer to preserve the widened event shape without adding any new transports.
- [x] Add a `Bearing-time sweep heatmap` panel to [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) using the shared incremental event buffer, absolute-time bins, and a clear empty state when spatially attributable events are absent.
- [x] Verify backend syntax/tests, frontend build, and a focused synthetic/backend runtime check that proves the widened event shape stays self-contained and the heatmap can distinguish populated versus empty bearing bins.

### Review

- Plan verified on 2026-04-04 against the existing high-frequency event pipeline in [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), and the current Timing-page consumer in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
- Chosen `HFV2C` cut line:
  - widen the shared recent-message event model with ingest-time `bearing_deg`
  - add only proposal item `23`, `Bearing-time sweep heatmap`
  - keep `25`, `26`, and `27` deferred until the spatial event contract is proven under live load
- Spatial attribution rule for this slice:
  - snapshot receiver-relative bearing from the aircraft state at message ingest time
  - only publish those fields when the aircraft position is already publishable under the existing high-confidence rules
  - leave the fields empty for messages without trustworthy spatial attribution instead of backfilling later from mutable live state
- Implementation result on 2026-04-04:
  - [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) now records `bearing_deg` inline in the recent timing-event buffer after decode/state updates, so position-bearing snapshots reflect message-time publishable state instead of a later live lookup.
  - [main.py](/home/keith/claude/adsb-dashboard/backend/main.py) and [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py) now serialize the widened eight-field timing tuple consistently through `/api/timing/events`, `/ws/timing`, and `/ws/timing-page`.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now consumes inline `bearing_deg` and renders a full-width `Bearing-Time Sweep Heatmap` from the existing shared incremental event buffer.
  - Scope decision: this first spatial slice widens the event model with `bearing_deg` only. `range_nm` stays deferred because item `23` does not need it, and adding unused spatial fields would widen the hot-path payload without improving the shipped panel.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/main.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py`
  - `npm run build` in `frontend/`
  - Focused synthetic backend check is covered by the updated timing-event tests: one assertion keeps `bearing_deg` empty when the aircraft position is not publishable, and another confirms the value is retained once the publishability gate is satisfied.
- Follow-up display refinement on 2026-04-04:
  - Increased the `Bearing-Time Sweep Heatmap` to `10°` sectors by moving from `18` to `36` angular buckets in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx).
  - Added a second full-width `Raw Bearing Raster` panel in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) that plots exact per-message bearings on a tall `0.5°/px` canvas.
  - Added desktop-oriented raster controls for color mode (`strength`, `message type`, `source class`) plus an optional `phosphor` fade mode that dims older points as they scroll left.

## OpenSpec Change: `add-high-frequency-message-waterfall`

- [x] Review the current Timing-page high-frequency visuals and keep the waterfall scope distinct from the existing bearing heatmap and raw bearing raster.
- [x] Implement a fixed-slice `DF-family` message waterfall in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) using the shared incremental timing-event buffer and completed absolute time slices only.
- [x] Keep the panel on the existing shared Timing-page stream without widening the backend event model or adding a new transport.
- [x] Verify the frontend build and perform a focused check that visible rows only advance on completed slice boundaries.

### Review

- Plan verified on 2026-04-05 against the existing Timing-page panels in [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) and the narrowed OpenSpec artifacts under [openspec/changes/add-high-frequency-message-waterfall](/home/keith/claude/adsb-dashboard/openspec/changes/add-high-frequency-message-waterfall).
- Scope decision:
  - `HFV2D` is implemented as a `DF-family` waterfall first.
  - `bearing` is intentionally excluded because the page already has both a bucketed bearing heatmap and an exact-message bearing raster.
  - The first slice remains frontend-only and reuses the existing shared timing-event buffer.
- Implementation result on 2026-04-05:
  - Added [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) as a pure helper for fixed-slice `DF-family` aggregation from the shared timing-event buffer.
  - Updated [frontend/src/pages/TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) with a full-width `Message Waterfall` panel, a `50/100 ms` slice control, and a row-stable render path that only rebuilds visible rows when the displayed completed-slice window advances.
  - Kept the panel distinct from the existing charts by using stacked slice rows with `DF-family` columns rather than another bearing plot or another lane-separated cadence chart.
- Verification:
  - `npm run build` in `frontend/`
  - `node --input-type=module -e "import { buildDfWaterfallRows } from './frontend/src/utils/timingWaterfall.js'; const events=[{arrival_us:1100000,df:17},{arrival_us:1120000,df:17},{arrival_us:1180000,df:11},{arrival_us:1210000,df:18},{arrival_us:1260000,df:18},{arrival_us:1310000,df:20}]; const base=buildDfWaterfallRows({events,renderNowUs:1299999,visibleWindowUs:300000,sliceUs:50000}); const next=buildDfWaterfallRows({events,renderNowUs:1300000,visibleWindowUs:300000,sliceUs:50000}); console.log(JSON.stringify({baseNewest:base.newestVisibleSliceIndex,nextNewest:next.newestVisibleSliceIndex,baseTopCounts:base.rows[0]?.counts,nextTopCounts:next.rows[0]?.counts}, null, 2));"`
  - The helper check confirmed that the newest visible row stayed on slice `23` just before the boundary and advanced to slice `24` only once the next slice was complete, matching the completed-slice display rule.
- Follow-up correction on 2026-04-05:
  - The first waterfall pass normalized cell brightness against the current visible-window peak, which made completed boxes change brightness as rows aged down the plot.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now uses a fixed log-scaled count-to-alpha transfer curve based on per-family event rate rather than a moving window max, so completed rows should keep stable intensity.
- Follow-up correction on 2026-04-05:
  - The `DF-family` version was still too coarse and the alternating row backgrounds were creating artificial periodic bands that read like real traffic structure.
  - [frontend/src/utils/timingWaterfall.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/timingWaterfall.js) now buckets by concrete raw `DF` values (`17`, `18`, `11`, `20`, `21`, `4`, `5`, `16`, `0`, `other`) instead of broad families.
  - [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx) now removes the zebra row striping and adds a `20 ms` slice option so the waterfall carries more genuine texture from the data itself.

## OpenSpec Change: `fix-signal-dbfs-semantics`

- [x] Review the current signal path across Beast ingest, readsb ingest, timing-event serialization, snapshot/history APIs, and frontend consumers.
- [x] Define one canonical display contract for signal as readsb-style `dBFS`, while keeping any raw byte handling backend-internal only where it is still needed for storage or ingest.
- [x] Update backend public payloads and shared timing-event serialization so frontend-facing signal values are emitted in canonical `dBFS` semantics across Beast and readsb paths.
- [x] Update frontend signal-based displays, labels, and legends to consume canonical `dBFS` values instead of raw-byte assumptions.
- [x] Verify the backend/frontend changes with focused tests, `npm run build`, and a distribution sanity check against the documented readsb/graphs1090 expectations.

### Review

- Plan verified on 2026-04-05 against the current signal flow in [backend/aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py), [backend/main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [backend/timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), [backend/db.py](/home/keith/claude/adsb-dashboard/backend/db.py), [frontend/src/pages/ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx), [frontend/src/pages/TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx), [frontend/src/components/AircraftTable.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftTable.jsx), [frontend/src/components/AircraftDetailPanel.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftDetailPanel.jsx), and [frontend/src/pages/SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx).
- Contract decision for this change:
  - public/frontend-facing signal values move to canonical `dBFS` semantics
  - raw Beast/readsb byte equivalents stay backend-internal where persistence or ingest still needs them
  - timing events should stop exposing `signal_raw` and instead expose display-grade `signal_dbfs`
- Compatibility constraint:
  - `coverage_samples` and minute aggregates can keep storing raw/internal values for now because existing history queries already convert those values server-side, but that raw form should no longer leak through public live APIs unless a dedicated debug need appears
- Implementation result on 2026-04-05:
  - Added [signal_utils.py](/home/keith/claude/adsb-dashboard/backend/signal_utils.py) so raw Beast bytes and canonical `dBFS` conversions are defined once and reused consistently.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so live aircraft snapshots now expose canonical `signal` in `dBFS`, retain explicit `signal_raw` only for internal/raw consumers, and store high-frequency timing events with `signal_dbfs` rather than raw bytes.
  - Updated [timing.py](/home/keith/claude/adsb-dashboard/backend/timing.py), [main.py](/home/keith/claude/adsb-dashboard/backend/main.py), [db.py](/home/keith/claude/adsb-dashboard/backend/db.py), and [history.py](/home/keith/claude/adsb-dashboard/backend/history.py) so live timing payloads, receiver scatter data, and SkyView history points all expose canonical display-grade signal semantics, while minute/coverage persistence keeps using raw values internally.
  - Added [frontend/src/utils/signal.js](/home/keith/claude/adsb-dashboard/frontend/src/utils/signal.js) and updated [ReceiverPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/ReceiverPage.jsx), [TimingPage.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/TimingPage.jsx), [AircraftTable.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftTable.jsx), [AircraftDetailPanel.jsx](/home/keith/claude/adsb-dashboard/frontend/src/components/AircraftDetailPanel.jsx), and [SkyView.jsx](/home/keith/claude/adsb-dashboard/frontend/src/pages/SkyView.jsx) so existing displays use one shared `dBFS` formatter/colour contract instead of local raw-byte assumptions.
- Verification:
  - `python3 -m py_compile backend/aircraft_state.py backend/db.py backend/history.py backend/main.py backend/signal_utils.py backend/timing.py`
  - `env UV_CACHE_DIR=/tmp/uv-cache uv run --directory backend pytest tests/test_timing_events.py tests/test_db.py`
  - `npm run build` in `frontend/`
  - Added backend assertions in [test_timing_events.py](/home/keith/claude/adsb-dashboard/backend/tests/test_timing_events.py) and [test_db.py](/home/keith/claude/adsb-dashboard/backend/tests/test_db.py) covering canonical `dBFS` event/API shapes and representative raw-to-`dBFS` conversions.
- Distribution sanity check on 2026-04-05:
  - Queried the last 24 hours of persisted `minute_stats` in [adsb.db](/home/keith/claude/adsb-dashboard/backend/data/adsb.db) using the same raw-to-`dBFS` conversion now used by the live APIs.
  - Observed minute-average distribution: peak `-6.1 dBFS`, floor `-31.1 dBFS`, mean `-21.6 dBFS`, interquartile range `-24.3` to `-20.1 dBFS`.
  - Interpretation:
    - the mean and spread are now in the expected readsb-style negative-`dBFS` space rather than the old raw-byte/public-contract split
    - persisted minute averages are narrower than the user’s per-message readsb/graphs1090 reference distribution, so they are only a sanity check, not a perfect acceptance proxy for live per-message extremes
- Follow-up correction on 2026-04-05:
  - The first implementation used the wrong Beast conversion model (`-(raw/2)`) and therefore produced live aircraft values that were far too strong compared with readsb.
  - Updated [signal_utils.py](/home/keith/claude/adsb-dashboard/backend/signal_utils.py) to use readsb’s actual Beast amplitude pipeline: normalize amplitude, square to power, then convert with `10 * log10(...)`.
  - Updated [aircraft_state.py](/home/keith/claude/adsb-dashboard/backend/aircraft_state.py) so Beast-fed aircraft signals now use readsb-style rolling 8-sample power averaging for the live aircraft value instead of exposing the last raw byte through an incorrect direct mapping.
  - Updated [decode_api.c](/home/keith/claude/adsb-dashboard/backend/native/decode_api.c) and rebuilt `backend/native/libdecode.so` with `make` so the native decoder path uses the same Beast signal normalization as the Python path.
  - Added focused formula/inverse tests in [test_signal_utils.py](/home/keith/claude/adsb-dashboard/backend/tests/test_signal_utils.py) and reran the backend signal test set successfully.
