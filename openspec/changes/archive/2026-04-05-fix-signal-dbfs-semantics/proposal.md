## Why

The dashboard currently treats signal inconsistently across ingest paths and displays. Parts of the app still use the internal `signal_raw` byte convention, while readsb-derived displays and operator expectations are framed in dBFS-like values. User validation against readsb and graphs1090 indicates the current displayed signal spread is still too clustered and does not match the receiver's observed range or distribution.

This is not just a cosmetic issue for one chart. Signal semantics affect multiple existing displays and will directly gate planned work such as signal-coloured or signal-axis live message-field views. The right move is to fix signal as a shared data contract first, then let visualisations build on a trustworthy value.

## What Changes

- Define one canonical display-grade signal representation for the dashboard, aligned to readsb-style dBFS semantics.
- Audit the current Beast/raw-byte and readsb ingest paths so they produce consistent display-ready signal values.
- Update existing frontend displays that currently interpret raw bytes directly so they consume the canonical signal representation instead of re-deriving inconsistent local scales.
- Keep raw/internal ingest values only as implementation detail where needed; do not let each chart invent its own signal transform.
- Verify the resulting distribution and extrema against known readsb/graphs1090 expectations before considering the work complete.

## Capabilities

### New Capabilities
- `canonical-signal-display-semantics`: The system exposes a trusted display-grade signal value aligned to readsb-style dBFS behaviour for use across receiver and high-frequency views.

### Modified Capabilities
- `timing-page-signal-observability`: Timing-page signal views consume canonical display semantics rather than raw-byte interpretation.
- `receiver-signal-visuals`: Existing receiver-oriented signal displays consume the same canonical semantics so signal meaning is consistent across the app.

## Impact

- Backend ingest/state logic in `backend/aircraft_state.py` and any related readsb/beast parsing helpers.
- Frontend signal-based displays and formatting logic that currently interpret `signal_raw` directly.
- Follow-on work such as the dedicated live message-field page can depend on this change instead of embedding its own signal assumptions.
