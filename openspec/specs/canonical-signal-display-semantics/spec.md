## ADDED Requirements

### Requirement: Dashboard exposes canonical signal display semantics
The system SHALL expose a canonical display-grade signal value aligned to readsb-style dBFS semantics for use across receiver and high-frequency visualisations.

#### Scenario: Signal value is displayed in a frontend visual
- **WHEN** any frontend display consumes per-message or per-aircraft signal data
- **THEN** it uses the canonical signal semantics instead of inventing a local transform from raw ingest values

#### Scenario: Signal values arrive from different ingest paths
- **WHEN** the backend derives display signal values from Beast ingest and from readsb ingest
- **THEN** the resulting exposed display semantics remain consistent enough that charts and operators can compare them meaningfully

### Requirement: Canonical signal semantics match readsb-oriented operator expectations
The canonical display signal representation SHALL be validated against readsb-style dBFS expectations before signal-dependent displays are considered correct.

#### Scenario: Operator compares the dashboard against readsb or graphs1090
- **WHEN** the operator checks signal spread and extrema against readsb-oriented references
- **THEN** the dashboard's displayed signal range and distribution are no longer materially more clustered than the expected receiver behaviour
