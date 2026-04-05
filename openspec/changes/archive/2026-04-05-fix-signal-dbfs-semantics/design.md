## Context

Signal is currently in an awkward middle state:

- internal high-frequency timing events retain `signal_raw`, a Beast-oriented byte-like value
- some UI code derives percentage-style or bucketed meaning directly from that raw value
- readsb-oriented displays and external operator references use dBFS semantics instead

The result is that signal visuals are not fully comparable either to each other or to readsb-derived expectations. That is tolerable for rough colouring, but it is not acceptable if signal is going to become a serious analytical axis or colour mode on new live exact-point pages.

This change should fix the semantics at the contract level. Once signal meaning is stable, downstream charts can become much simpler and more trustworthy.

## Goals / Non-Goals

**Goals:**
- Define one canonical signal value for display and analysis across the dashboard.
- Align that value with readsb-style dBFS behaviour closely enough that operators can compare the app with readsb/graphs1090 without mental translation.
- Update existing displays to consume the canonical value instead of inventing local transforms.
- Preserve enough backend flexibility that internal raw ingest values can still exist where needed.

**Non-Goals:**
- Redesign every signal-based chart at the same time.
- Promise bit-for-bit identity with every external tool if their smoothing or aggregation differs.
- Expand the scope to unrelated RF metrics such as noise floor estimation or calibration against absolute receiver hardware power.
- Build the dedicated message-field page in this change.

## Decisions

### 1. Separate internal raw ingest values from the canonical display signal

The system should be free to retain a raw ingest-oriented signal value internally if the decoder path naturally produces one, but frontend-facing APIs and shared recent-event contracts should expose a canonical display-grade signal representation instead of leaving each consumer to reinterpret the raw value.

Alternative considered:
- Keep exposing only raw values and fix each chart locally.
  Rejected because it guarantees drift and makes future signal-based plots harder to trust.

### 2. Align the canonical display signal to readsb-style dBFS semantics

The dashboard should speak the same signal language as readsb when presenting signal to operators: negative dBFS-like values with `0 dBFS` at full scale and weaker signals extending downward. This is already the mental model used by existing signal heatmap code and by the user's external validation references.

Alternative considered:
- Keep the Beast raw-byte scale as the public display contract.
  Rejected because it is not the operator's reference frame and has already proven confusing and misleading.

### 3. Fix the backend contract before extending new signal-heavy views

Signal alignment should land before the new live message-field page depends on signal for colour or axes. That keeps the new page simpler and prevents rework.

Alternative considered:
- Let the new page invent a better signal transform locally first.
  Rejected because the same signal issue already affects existing displays and should not be solved page-by-page.

### 4. Update shared timing events and existing receiver views together

The canonical signal semantics should flow through both:

- high-frequency recent-event views
- receiver/overview signal displays

This keeps the entire app coherent instead of leaving a split brain where one area uses raw-byte semantics and another uses dBFS.

Alternative considered:
- Fix only the high-frequency path.
  Rejected because the user explicitly noted this affects other displays too.

### 5. Validate the new semantics against expected distribution, not just code inspection

The user already provided concrete expectations from readsb/graphs1090:

- occasional peaks near `0 dB`
- weakest signals around `-42.5 dB`
- mean around `-25 dB`
- interquartile range roughly `-33 dB` to `-18 dB`

The change should therefore include an explicit comparison step against those kinds of observed ranges rather than stopping at “the transform looks plausible.”

Alternative considered:
- Accept the change if charts simply look less clustered.
  Rejected because this is a correctness issue, not merely an aesthetic one.

## Risks / Trade-offs

- Signal interpretation may differ between Beast and readsb ingest paths in subtle ways.
  Mitigation: document the canonical conversion clearly and validate both paths against the same display expectations.
- Changing the signal contract can shift the appearance of multiple existing plots at once.
  Mitigation: treat that as intended, verify the touched displays together, and update labels/copy where needed.
- Existing tests may not cover signal semantics deeply enough.
  Mitigation: add focused backend/frontend checks around representative values, ranges, and exposed event shapes.

## Migration Plan

This is a contract-fixing change. Update backend signal semantics first, then update frontend consumers so the canonical display value is used consistently. Rollback is straightforward but undesirable, because it would restore cross-page inconsistency and block signal-based follow-on work.

## Open Questions

- Should the canonical display contract expose `signal_dbfs` only, or both `signal_dbfs` and a retained raw/internal field for debugging?
- Are there any existing APIs consumed outside the frontend that would need a compatibility transition if signal semantics change?
- Does the app need explicit UI labeling changes anywhere to clarify the move from raw/percent-like signal interpretations to dBFS semantics?
