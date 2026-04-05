## Context

The dashboard now has two different kinds of high-speed views:

- `Timing` page observability panels, which compress recent traffic into lanes, strips, and aggregate diagnostics.
- Exact-message timing/spatial panels, which are more interesting when they preserve point-level detail instead of collapsing it into bins.

The waterfall experiment showed the boundary clearly. A bucketed image can be visually striking, but on this page it sits awkwardly between the existing `DF cadence` summary and the full exact-message timing plot. It adds another compressed view without creating enough new insight.

The better fit is a dedicated exact-point page. It should be large enough to make dense point clouds readable, flexible enough to switch between spatial and analytical geometries, and explicit that it is for fast transient structure rather than minute-scale summaries.

## Goals / Non-Goals

**Goals:**
- Create a dedicated page for exact-point high-speed message visualisation instead of continuing to crowd the `Timing` page.
- Reuse one exact-message recent-event model across multiple plot modes.
- Favour point fidelity and short persistence over buckets and server-side aggregation.
- Support both spatial and analytical views from the same underlying stream.
- Align signal semantics with readsb-style dBFS behaviour before relying on signal as a primary displayed variable.

**Non-Goals:**
- Keep iterating on the waterfall as an active shipped panel.
- Build a page full of separate near-duplicate plots.
- Retain long-lived histories or minute-scale archives on this page.
- Add every imaginable geometry or colour mode in the first slice.
- Replace the existing map/coverage pages for slower situational overview work.

## Decisions

### 1. Put the new display on its own page, not the Timing page

This work needs a full-width, deeper canvas and a different interaction model than the existing Timing cards. Keeping it on `Timing` would either letterbox the view or force the page into a jumble of overlapping concerns.

Alternative considered:
- Keep adding full-width exact-message panels to `Timing`.
  Rejected because the page is already dense and because the new display should be navigated as a primary visual, not squeezed in as another card.

### 2. Make one large exact-point plot the primary interaction, with selectable geometry/mode

The page should center on one main plot, not multiple separate panels that duplicate each other. The operator changes geometry, axes, colour, persistence, and filtering on the same underlying exact-point event field.

The first planned modes are:

- `bearing × time`
- `polar bearing × range`
- `bearing × signal`
- `range × signal`

Alternative considered:
- Ship a page with many small specialised charts.
  Rejected because it recreates the duplication problem that made the waterfall unconvincing.

### 3. Keep the display exact-point and persistence-based, not bucketed

The user feedback here is clear: the interesting part of this data is the exact transient detail. The page should render actual message points with persistence fade, not bucket them into cells or server-side density images.

Alternative considered:
- Use larger bucketed scatter/heatmap variants for simplicity.
  Rejected because that moves the page back toward the summary/aggregate territory already covered elsewhere.

### 4. Support both polar and Cartesian views from the same message field

Polar mode preserves real spatial relationships, which is important for bearing/range views. Cartesian mode gives equal visual weight across the plot and can make dense inner-radius structure easier to inspect. Both should be available as alternative projections of the same point stream.

Alternative considered:
- Pick one geometry globally.
  Rejected because spatial interpretation and analytical interpretation benefit from different projections.

### 5. Add `range_nm` and `iid` to the exact-message event contract, and treat signal semantics as a prerequisite

The current recent timing-event tuple already includes `bearing_deg`, `df`, `source_class`, `icao`, and raw signal bytes. To support the most valuable modes, the exact-point message-field stream should also carry:

- `range_nm` when the aircraft position is publishable at ingest time
- `iid` when the message carries interrogator attribution
- signal in a readsb-aligned dBFS-like representation suitable for display

Alternative considered:
- Build the page only with currently retained fields and keep signal raw bytes as-is.
  Rejected because the most compelling spatial and analytical modes need `range_nm`, and the user has already identified that the current signal values are not trustworthy enough for a serious axis/colour role.

### 6. Keep persistence short and bounded

Persistence should help reveal patterns, not turn the page into a smear. Start with short bounded options such as `1 s`, `3 s`, `5 s`, `10 s`, and `30 s`. Longer windows can be revisited only if density-aware fading proves necessary and useful.

Alternative considered:
- Allow up to `60 s` immediately.
  Deferred because it is likely too dense for the intended high-speed exact-point use and would complicate the rendering model before the shorter windows are validated.

### 7. Treat IID-focused views as a filter/colour mode, not a separate plot family

`IID spatial scatter` is not a separate product concept. It is the same message field with:

- a `DF11` filter
- `iid` colour encoding

Likewise, “airspace activity” is the same point field with different filters and colour mappings. The page should unify these instead of spawning more named plots.

Alternative considered:
- Build separate dedicated `IID` and `airspace activity` plots.
  Rejected because that duplicates controls and fragments the interaction model.

## Risks / Trade-offs

- Widening exact-message events further increases payload size.
  Mitigation: keep retention short, keep one dedicated page stream, and include optional fields only when publishable/available.
- Signal alignment may be more involved than expected.
  Mitigation: treat readsb-aligned signal semantics as an explicit prerequisite task, and avoid using signal as a primary mode until verified.
- A very flexible page can become control-heavy.
  Mitigation: keep one main plot, a small set of well-chosen modes, and avoid adding overlapping controls all at once.
- Polar mode can make inner-radius structure hard to inspect.
  Mitigation: offer Cartesian projection for equal-weight inspection of the same data.

## Migration Plan

This is a mixed additive/subtractive change. Add the new page and exact-point stream support, and remove the shelved waterfall from `Timing`. Existing timing and spatial pages remain intact. Rollback is straightforward: the new page can be removed and the Timing page can continue without the waterfall.

## Open Questions

- Should the first shipped page default to `bearing × time` or `polar bearing × range`?
- Do we need a small point-size control, or is colour/persistence enough in the first slice?
- Is `iid` best rendered as categorical colour only, or should there also be a per-`iid` filter control?
- Should `range × signal` and `bearing × signal` both ship in the first slice, or should one wait until signal alignment is validated on the page itself?
- Before archive sign-off, should a short live-receiver tuning pass adjust defaults and visual scaling based on real traffic, specifically point size, polar radius usage, default mode/persistence, and signal/bearing axis comfort under sustained DF11/DF17 activity?
