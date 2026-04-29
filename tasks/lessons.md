# Lessons

## 2026-04-29

- When demoting a Go-derived model from authority to diagnostics, gate every operational export of that model, not just the first selector. If `RefreshReference()` ignores an unvalidated family but `FamilySnapshot()` still exposes it to frame admission, frames can stop while the period path looks fixed.
- When introducing a required base-period authority, update test helpers to seed that authority explicitly. Otherwise frame tests can pass by skipping reference setup instead of proving the frame path still works.
- When enabling Go as the frame generator, do not disable Python-side diagnostic/refinement observation recording. Performance ownership and frame ownership can move to Go, but Python burst-sync refinement still needs aligned burst observations unless an equivalent Go refinement feed exists.

## 2026-04-28

- After a structural extraction, verify the source file no longer defines local copies of the moved symbols. Do not stop at adding imports; confirm that imported helpers and dataclasses are not shadowed later in the file, and add an identity test when a module is meant to re-export shared classes.

## 2026-04-23

- For Go multi-sync phase work, do not treat an exported candidate anchor as operational authority. Keep `PhaseOffsetDeg` aligned with the active authority mode, expose candidate-anchor diagnostics separately, and label residual plots by their actual basis.
- When adding an output trust flag such as `AbsolutePhaseTrusted`, verify it is not accidentally coupled into the authority-entry path. Promotion gates should report their own blocker reason; the trust flag should be a consequence of promotion and validation, not a prerequisite.
- When fixing a displayed history regression, verify both backend payload span and frontend render-domain clipping. A plot can receive retained data but still hide it if its x-window is anchored to an unrelated live clock.
- When a live radar display is Go-owned, fix the Go source/export first instead of correcting only Python/React consumers. Python display post-processing can hide the bug while the runtime binary still exports the wrong diagnostic frame.
- For passive radar sync, treat the DF alignment period as the bootstrap prior, not the final answer. The refined model must slowly decontaminate that prior using motion-corrected residual evidence rather than locking to it or re-estimating from only a short noisy window.
- When extending an operator chart to a 300s display window, check every upstream source cap. A retained axis is not enough if the raw dot source is still age-pruned at 60s or exported by a shorter Go snapshot store.

## 2026-04-20

- When introducing backend-managed subprocess startup, do not hardcode a single install-path default for the worker binary. Use a robust default-resolution order (installed path first, repo-local build fallback) so local/dev and production/service environments both start cleanly without extra manual env edits.

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

- When a user says a feature family must progress stage-by-stage with proof before advancing, encode that rule immediately in the active task artifacts and remediation plan before making further fixes. Do not leave the new process expectation implicit.
- For operator-facing timing alignment views, do not default to modulo-phase plots just because they are compact. If the user needs to see repeated sweeps line up, normalize each row to its first observed pulse and show several full periods with grid spacing set by the derived period.
- For mixed-radar timing rows, do not classify an entire ICAO as `secondary` just because it contains some secondary-aligned bursts. Split the row into per-family point series so primary-aligned points remain primary, secondary-aligned points are shown separately on the same row, and residual points stay distinct.
- For operator timing rows, do not clutter the ICAO labels with family text when the colour coding already communicates the distinction. Keep the label focused on the identifier and count unless text adds information the plot cannot show.
- For passive-radar characteristics, do not let the top-level period/status be replaced wholesale by each short-window recompute. Keep a reinforced long-term primary/secondary model that accumulates support over time, and treat the current-window fit as a separate live observation.
- When exposing a passive-radar period/status, do not stop at the categorical label. Also expose a confidence/readiness measure based on accumulated qualifying evidence so operators can see whether a family is well established or still tentative.
- For passive-radar family ordering, do not make the smaller period primary by convention. Rank primary vs secondary by evidence strength, and require a clearly supported second family before promoting it out of the residuals.
- For operator-facing passive-radar labels, reserve `MULTI_RADAR` for fully established dual-family cases. If the secondary family is still tentative, keep the top-level label aligned with the trusted primary family instead of overstating ambiguity.
- For slow-turning radars, do not limit the Stage 1 alignment view to a short fixed span like 30 seconds. Show at least about a minute of history so the operator can actually see repeated rotations.
- If operator review shows that Stage 1 is still misclassifying obviously primary-aligned points, do not keep iterating on secondary-radar logic in parallel. Defer secondary detection entirely and simplify the stage back to primary-plus-residual until the primary family is reliable.
- For Stage 1 primary detection, do not rely only on per-ICAO interval summaries. If clutter bursts can distort the row median, add a centroid-sequence inlier pass so visually obvious repeated primary bursts are not left as residual.
- For instrument-like Stage 1 panels, do not rely on slow repeated full-panel polling once the interaction model is proven useful. Move the selected subject to a websocket/live feed, but keep an HTTP fallback so the view degrades gracefully if the socket stalls.
- On live filter controls, do not derive the selectable identifier list only from the currently visible short persistence window, and do not auto-reset the user's selected filter just because traffic pauses. Keep a sticky option cache so operator intent survives quiet periods.
- When rendering live canvas points with mixed colour formats, do not append hex alpha bytes blindly. If the palette can emit `hsl()` or named colours, apply opacity via canvas `globalAlpha` or a proper colour conversion so signal palettes do not collapse to one fallback colour.
- For instrument-style plots, do not place empty-state notes inside the plotting area when the axes themselves are still useful. Put the status note in surrounding chrome so the canvas remains readable as a frame of reference.
- For Stage 1 passive-radar primary detection, do not seed the dominant period from one median interval per ICAO and hope later evidence fixes it. Build candidates from repeated within-ICAO interval series, choose the strongest mutually agreeing cohort across ICAOs, and let sustained contradictory cohort evidence decay and replace a bad learned estimate.
- When working from a staged research/report plan, do not let a useful downstream display become the de facto goal of an earlier stage. For passive radar, Stage 2A should be judged on sweep correctness and dwell-centre refinement; beam azimuth only becomes a primary validated product once localisation exists.
- Do not keep operator-facing plots just because backend work exists for them. If a stage plot does not clearly answer an operator question without heavy clicking or interpretation, remove it from the page and keep the backend data path for later use instead.
- Before trusting passive-radar TDOA calibration, do not use the aircraft's current live position as a proxy for sweep-time geometry. Pull the aircraft position from historical track data at the burst time first, and only fall back to live state when history is unavailable.
- Do not let passive-radar calibration accumulation or solver output be "best effort". Calibration observations must be idempotent across rebuild passes, historical duplicate rows must be deduped on load, and the solver must reject boundary-hitting or high-residual fits instead of publishing absurd radar locations.
- For passive-radar sweep-time geometry, do not stop at a coarse sampled track store once the semantic bug is fixed. Keep timestamped ADS-B position history at accepted position-update cadence, interpolate between adjacent ADS-B samples, and reserve motion-vector extrapolation for only short gaps.
- For passive-radar localisation, do not treat raw co-sweep arrival `tdoa_us` as the solver target. The proposal’s hyperbola is defined only after subtracting the known receiver-to-aircraft path term, so the calibration payload or solver residual must include that correction explicitly.
- When a localisation equation fix is made, rerun it immediately on representative persisted datasets rather than assuming that a semantic correction alone will clear the stage. If the datasets still reject, record that the next blocker has shifted to observation quality or gating.
- For passive-radar calibration, do not assume broad sweep-cluster membership is enough to admit a pair. Put a hard physical cap on allowable burst-centroid TDOA first; tens of milliseconds are already too loose for this geometry and will swamp the solver with nonsense.
- When passive-radar localisation remains blocked after basic pair gating, do not keep tuning the solver in isolation. Look for a higher-grade observation subset first, such as near-coincident co-sweep aircraft pairs that imply a stronger directional constraint than the full broad pair pool.
- For passive-radar burst timing, do not stop at the raw centroid once per-reply dwell signal exists. Compute an internal refined burst-centre timestamp from reply timing and amplitude, but keep the plain centroid as the fallback for shallow bursts with too little structure.
- Before investing in long-term weighted accumulation for passive-radar localisation, verify that at least some individual sweeps can solve cleanly on their own. If per-sweep solves all fail, the real blocker is same-sweep geometry/timing quality, not the accumulation framework.
- On live point-field views, do not auto-scale inward directly from the currently visible point set. If points are intermittent or the persistence window is short, fast shrinkage makes the plot pulse and harms readability; expand quickly when needed, but contract slowly and verify the long-window retention against the actual client buffer cap.
- For live auto-zoom on sparse/intermittent plots, “slow contraction” still is not enough if it starts within a few seconds. Use an explicit long hold window before shrinking inward at all; otherwise the display still breathes during normal traffic gaps.
- For live scrolling exact-point plots, do not reset the render-time anchor directly to each newly received transport timestamp. Packet cadence jitter or fallback polling can make the viewport jump backward or lurch forward; keep a monotonic local render clock and only let transport updates pull it ahead.
- When one live timing plot needs monotonic scroll correction, check every sibling plot that shares the same `now_us` interpolation pattern. Leaving the old anchor-reset clock on adjacent panels creates inconsistent motion and guarantees repeat cleanup work.
- For signal-axis plots, keep the dBFS orientation physically intuitive and consistent across panels: `0 dBFS` is strongest and should sit at the top/high end of the axis unless there is a deliberate contrary reason.

## 2026-04-08

- For live-validation helpers that call slow backend operations, do not let one request timeout abort the entire validation session. Make timeouts configurable and convert per-run request failures into structured results so the operator still gets useful diagnostics for the other IIDs/runs.
- For SweepFrame construction, do not use strongest-signal burst selection as the primary way to decide which per-aircraft hit belongs in the frame. Prefer period-consistent burst assignment anchored on the chosen reference aircraft's burst, and treat multi-radar resolution as a later concern rather than baking in fragile signal-strength heuristics now.
- For passive-radar sweep direction, do not treat clockwise and counterclockwise as equally plausible defaults. Assume clockwise for almost all live cases, and require unusually strong standalone evidence before accepting a counterclockwise solution.
- When the user names a specific UI metric or page, verify that exact frontend/backend path before diagnosing related subsystems. Do not infer that a similarly named radar-specific counter is the one they mean.
- When a user states the deployment mode explicitly, verify that mode before proposing a root cause or patch. Do not assume `hybrid` when the receiver is actually running `beast` only.

## 2026-04-09

- When a user revises requirement language after an assessment, update the conclusions against the revised contract rather than preserving stricter superseded assumptions. Distinguish design incompatibility from mere lack of deployment-time verification.
- When a later phase is explicitly approved, do not leave that phase framed as a design-only investigation if the user has now decided the architectural direction. Update the task plan to reflect the chosen implementation path before continuing.
- When moving live radar work onto a new ingest path, preserve all side effects of the old path, not just the obvious event append. If the original path marked IIDs dirty for Stage 1 learning, the new predecoded worker path must do the same or downstream frame building will silently stop.
- When a radar UI shows a solved CEP in one panel and `No FM position` in another, verify whether the second panel is incorrectly keying off convergence history instead of the persisted current FM location. Do not assume the solver simply has not rerun.
- When adapting a live radar overlay from a delayed persistence plot, do not drive the beam from the delayed render clock. Keep the point persistence delay if needed, but anchor the beam phase to live Beast-time updates so it stays synchronised to the reference aircraft.
- When a live radar beam still jitters after phase alignment is corrected, check whether the beam clock is still coming from a polling helper or whether the reference frame selection is hopping between updates. Prefer the timing websocket clock and a stable reference-frame anchor over GET-driven re-anchoring.
- When a smooth radar beam is still phase-wrong, check whether phase zero is anchored to an old sweep-frame timestamp rather than the latest live DF11 pulse of the active reference ICAO. Use the live reference pulse for time anchoring whenever the display is meant to line up with current arrivals.

## 2026-04-10

- When a comparison panel is meant to explain solver behavior, do not populate it from solved outputs only. Show unsolved methods too, with explicit states like `collecting`, `blocked`, `inactive`, or `evidence_only`, otherwise the UI falsely suggests those methods are absent rather than merely not converged.
- When live decode timings suddenly implicate a native predecode phase, benchmark the isolated C/CFFI boundary before blaming the native decoder itself. Wall-clock time around a native call can be dominated by Python-side scheduling and background GIL contention rather than the C function's own runtime.
- When a user pushes back on backlog-based throttling for a CPU-bound path, treat that as a design correction, not a preference note. Do not keep leaning on skip/defer guards as the main fix once the requirement is to preserve processing under load; move the hot path itself out of Python/GIL contention.
- When moving a live history-based path into native code, do not store the history inside a state object that is deleted at burst expiry. Preserve long-lived per-entity history separately from short-lived pending-burst reply buffers, or the system can look fast while silently losing the evidence needed for later gating and selection.
- When a page-open regression persists after backend worker timings look healthy, do not keep hunting the ingest path by reflex. Check websocket payload cadence and heartbeat behavior; repeatedly rebuilding and resending large unchanged payloads can peg both backend serialisation and browser rendering while the core workers remain quiet.
- When adding a memoization hook to a React component with an early return path, re-check hook order immediately. Do not place the new hook below a conditional `return`, or production will fail with a minified hook-order error instead of a readable local warning.
- When a status endpoint only needs a count, do not load and dedupe thousands of evidence rows on the request path. Add a cheap aggregate query and keep full row materialisation limited to diagnostics/evidence views that actually render the rows.
- When a page interaction can switch subjects quickly, do not rely on "ignore stale response" flags alone. Abort stale fetches and avoid all-method fanout by default, otherwise the backend can keep doing obsolete CPU work after the UI has moved on.
- When a live panel filters a high-rate stream client-side, check whether the backend can apply the same filter before serialisation. Sending all events and discarding most of them in React still burns backend GIL time and can starve decode work.
- When a diagnostic solver is declared obsolete for the operator workflow, remove every UI trigger for it instead of only lazy-loading or hiding it behind a manual button. Keep backend debug endpoints separate from page behavior.
- When throttling passive-radar reference selection, do not replace repeated rescoring with unconditional reuse of the prior reference just because it is recent. Verify that SweepFrame construction still starts frames under live burst ordering; prefer one rescore per IID batch over stale-reference reuse that can suppress frame starts.

## 2026-04-16

- When two adjacent radar panels have different operational purposes, do not let both become driven by the same primary data primitive. Keep the alignment panel burst-sync-primary with raw live context, and keep position verification raw-live-primary with burst diagnostics secondary.

## 2026-04-17

- When building recovery logic for a wrong absolute phase branch, do not use residual-quality memory from that wrong branch as a hard eligibility gate. Use it as a score penalty or diagnostic only, otherwise every suitable recovery anchor can be rejected before it gets a chance to correct the branch.

## 2026-04-20

- After fixing a major retained-state bottleneck, re-audit adjacent operational caps in the same path (especially hard-coded per-ICAO history limits) before declaring scaling complete.
- When adding stage diagnostics (for example cluster counts), do not infer earlier-stage counts from later-stage artifacts like merge-event totals. Carry exact per-stage counts through the pipeline, and avoid leaving a simplistic early dedupe stage that can pre-empt richer identity logic.

## 2026-04-21

- When simplifying an operator panel, do not remove a secondary plot that still provides distinct operational value unless the user explicitly asked for that specific removal. In this case the residual-vs-range view should have been preserved.
- For folded live scatter plots, do not key rendered points by array index when the source window slides over time. Use stable per-observation keys and a static domain ordering, or the chart can visually "flow" like a time series even when the x-axis is phase-only.
- For a folded residual-by-phase/bearing plot, do not use a live rephased model coordinate for historical observations if the operator expects a visually fixed spatial domain. Use a stable per-observation bearing/rotation position for x, and reserve live model-phase overlays for explicitly diagnostic views.
- After changing a backend-managed native/Go worker protocol or snapshot shape, do not stop at source edits and unit tests. Verify that the actual runtime binary used by the service has been rebuilt or restarted, otherwise the Python side can still be talking to an old executable and the live behavior will contradict the patched code.
- When fixing a Go-owned UI data path, do not stop at the first retained-evidence fallback. Check the next retained source in the operational chain as well; an IID can lose raw burst evidence while still retaining accepted sweep frames, and the UI must either use that fallback or explain why it cannot.
